package otlp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/receiver/otlp"
	"github.com/lesomnus/ward/signal"
)

const (
	protoType = "application/x-protobuf"
	jsonType  = "application/json"
)

// sink records what it is handed. A held sink keeps each call waiting until
// release, or until the call's context is done, which is how a test fills the
// queue: the worker waits in the sink, and nothing leaves the queue.
type sink struct {
	hold    chan struct{} // nil: calls return at once
	once    sync.Once
	entered chan struct{} // a value as each call starts

	mu        sync.Mutex
	got       []got
	cancelled int // calls given up because their context ended
}

type got struct {
	id    string // "logs/<test.id>" or "metrics/<test.id>"
	proto []byte // the batch, encoded again
	err   error  // the call's context's error
}

func newSink() *sink { return &sink{entered: make(chan struct{}, 64)} }

func newHeldSink() *sink {
	s := newSink()
	s.hold = make(chan struct{})
	return s
}

func (s *sink) release() {
	if s.hold != nil {
		s.once.Do(func() { close(s.hold) })
	}
}

func (s *sink) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	id := "logs/" + testID(ld.ResourceLogs().At(0).Resource().Attributes().AsRaw())
	b, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		return err
	}
	return s.consume(ctx, id, b)
}

func (s *sink) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	id := "metrics/" + testID(md.ResourceMetrics().At(0).Resource().Attributes().AsRaw())
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		return err
	}
	return s.consume(ctx, id, b)
}

func testID(attrs map[string]any) string {
	id, _ := attrs["test.id"].(string)
	return id
}

func (s *sink) consume(ctx context.Context, id string, b []byte) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	if s.hold != nil {
		select {
		case <-s.hold:
		case <-ctx.Done():
			s.mu.Lock()
			s.cancelled++
			s.mu.Unlock()
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, got{id: id, proto: b, err: ctx.Err()})
	return nil
}

func (s *sink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := []string{}
	for _, g := range s.got {
		ids = append(ids, g.id)
	}
	return ids
}

func (s *sink) all() []got {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

// waitFor waits until the sink got exactly ids, in that order.
func (s *sink) waitFor(t *testing.T, ids ...string) {
	t.Helper()
	eventually(t, func() bool { return len(s.ids()) >= len(ids) })
	if got := s.ids(); !slices.Equal(got, ids) {
		t.Fatalf("sink got %q, want %q", got, ids)
	}
}

// waitEntered waits until the worker is in the sink.
func (s *sink) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-s.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not call the sink")
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func config(withGRPC, withHTTP bool) conf.OTLP {
	c := conf.OTLP{MaxRequestBytes: 1 << 20, QueueSize: 16}
	if withGRPC {
		c.GRPC = &conf.Endpoint{Endpoint: "127.0.0.1:0"}
	}
	if withHTTP {
		c.HTTP = &conf.Endpoint{Endpoint: "127.0.0.1:0"}
	}
	return c
}

// start runs a Receiver until the test ends. The sink is released first, so
// stopping does not wait out its timeout.
func start(t *testing.T, cfg conf.OTLP, s *sink) *otlp.Receiver {
	t.Helper()
	r, err := otlp.New(cfg, s)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		s.release()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return")
		}
	})
	return r
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func logsReq(id string) plogotlp.ExportRequest {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("test.id", id)
	rl.Resource().Attributes().PutStr("host.name", "node-1")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.Body().SetStr("hello " + id)
	lr.Attributes().PutInt("diag.canary.seq", 7)
	return plogotlp.NewExportRequestFromLogs(ld)
}

func metricsReq(id string) pmetricotlp.ExportRequest {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("test.id", id)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("system.uptime")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(42.5)
	return pmetricotlp.NewExportRequestFromMetrics(md)
}

type marshaler interface {
	MarshalProto() ([]byte, error)
	MarshalJSON() ([]byte, error)
}

func encode(t *testing.T, m marshaler, contentType string) []byte {
	t.Helper()
	marshal := m.MarshalProto
	if strings.HasPrefix(contentType, jsonType) {
		marshal = m.MarshalJSON
	}
	b, err := marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// wire is how the sink encodes what it got, to compare it with what was sent.
func wire(t *testing.T, m any) []byte {
	t.Helper()
	var (
		b   []byte
		err error
	)
	switch m := m.(type) {
	case plogotlp.ExportRequest:
		b, err = (&plog.ProtoMarshaler{}).MarshalLogs(m.Logs())
	case pmetricotlp.ExportRequest:
		b, err = (&pmetric.ProtoMarshaler{}).MarshalMetrics(m.Metrics())
	}
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func gzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type answer struct {
	code   int
	header http.Header
	body   []byte
}

// post sends body to url with the media type and the Content-Encoding (none
// when empty).
func post(t *testing.T, c *http.Client, url, contentType, encoding string, body io.Reader) answer {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	return do(t, c, req)
}

func do(t *testing.T, c *http.Client, req *http.Request) answer {
	t.Helper()
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return answer{res.StatusCode, res.Header, b}
}

// sendHTTP posts an export request, protobuf and gzip, as the Collector's
// otlphttp exporter does by default.
func sendHTTP(t *testing.T, c *http.Client, r *otlp.Receiver, path string, m marshaler) answer {
	t.Helper()
	body := gzipped(t, encode(t, m, protoType))
	return post(t, c, "http://"+r.HTTPAddr()+path, protoType, "gzip", bytes.NewReader(body))
}

// refusal decodes the google.rpc.Status a refused OTLP/HTTP request is
// answered with.
func refusal(t *testing.T, a answer) *spb.Status {
	t.Helper()
	st := &spb.Status{}
	var err error
	switch a.header.Get("Content-Type") {
	case protoType:
		err = proto.Unmarshal(a.body, st)
	case jsonType:
		err = protojson.Unmarshal(a.body, st)
	default:
		t.Fatalf("Content-Type of a refusal is %q", a.header.Get("Content-Type"))
	}
	if err != nil {
		t.Fatalf("decode the Status of a %d: %v (%q)", a.code, err, a.body)
	}
	return st
}

func TestGRPC(t *testing.T) {
	s := newSink()
	r := start(t, config(true, false), s)
	if r.HTTPAddr() != "" {
		t.Errorf("HTTPAddr is %q without http configured", r.HTTPAddr())
	}

	conn := dial(t, r.GRPCAddr())
	ctx := context.Background()
	logs := plogotlp.NewGRPCClient(conn)
	metrics := pmetricotlp.NewGRPCClient(conn)

	// The Collector's otlp exporter compresses with gzip by default.
	l1, m2, l3 := logsReq("1"), metricsReq("2"), logsReq("3")
	if _, err := logs.Export(ctx, l1, grpc.UseCompressor(grpcgzip.Name)); err != nil {
		t.Fatalf("export logs: %v", err)
	}
	if _, err := metrics.Export(ctx, m2, grpc.UseCompressor(grpcgzip.Name)); err != nil {
		t.Fatalf("export metrics: %v", err)
	}
	if _, err := logs.Export(ctx, l3); err != nil {
		t.Fatalf("export logs uncompressed: %v", err)
	}

	s.waitFor(t, "logs/1", "metrics/2", "logs/3")
	for i, want := range [][]byte{wire(t, l1), wire(t, m2), wire(t, l3)} {
		if g := s.all()[i]; !bytes.Equal(g.proto, want) {
			t.Errorf("%s arrived changed", g.id)
		}
	}
	if st := r.Stats(); st.Accepted != 3 || st.RefusedBad+st.RefusedFull+st.RefusedTooLarge+st.SinkErrors != 0 {
		t.Errorf("stats: %+v", st)
	}
}

func TestHTTP(t *testing.T) {
	for _, tc := range []struct {
		name        string
		contentType string
		encoding    string
		answeredAs  string
	}{
		{"protobuf", protoType, "", protoType},
		{"protobuf gzip", protoType, "gzip", protoType},
		{"json", jsonType, "", jsonType},
		{"json gzip", jsonType, "gzip", jsonType},
		{"json with charset", jsonType + "; charset=utf-8", "GZIP", jsonType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink()
			r := start(t, config(false, true), s)
			if r.GRPCAddr() != "" {
				t.Errorf("GRPCAddr is %q without grpc configured", r.GRPCAddr())
			}

			l1, m2 := logsReq("1"), metricsReq("2")
			for _, x := range []struct {
				path string
				req  marshaler
			}{{"/v1/logs", l1}, {"/v1/metrics", m2}} {
				body := encode(t, x.req, tc.contentType)
				if tc.encoding != "" {
					body = gzipped(t, body)
				}
				a := post(t, http.DefaultClient, "http://"+r.HTTPAddr()+x.path, tc.contentType, tc.encoding, bytes.NewReader(body))
				if a.code != http.StatusOK {
					t.Fatalf("%s: %d %q", x.path, a.code, a.body)
				}

				// What the Collector's otlphttp exporter reads: an export
				// response, in the encoding it sent.
				resp := plogotlp.NewExportResponse()
				var err error
				switch ct := a.header.Get("Content-Type"); ct {
				case protoType:
					err = resp.UnmarshalProto(a.body)
				case jsonType:
					err = resp.UnmarshalJSON(a.body)
				default:
					t.Fatalf("%s: answered as %q", x.path, ct)
				}
				if err != nil {
					t.Errorf("%s: the answer is not an export response: %v (%q)", x.path, err, a.body)
				}
				if got := a.header.Get("Content-Type"); got != tc.answeredAs {
					t.Errorf("%s: answered as %q, want %q", x.path, got, tc.answeredAs)
				}
			}

			s.waitFor(t, "logs/1", "metrics/2")
			if g := s.all(); !bytes.Equal(g[0].proto, wire(t, l1)) || !bytes.Equal(g[1].proto, wire(t, m2)) {
				t.Error("the signals arrived changed")
			}
			if st := r.Stats(); st.Accepted != 2 {
				t.Errorf("stats: %+v", st)
			}
		})
	}
}

func TestQueueFull(t *testing.T) {
	// The worker takes the first request and waits in the sink; two more fill
	// the queue; the fourth finds no room.
	const size = 2

	t.Run("grpc", func(t *testing.T) {
		s := newHeldSink()
		cfg := config(true, false)
		cfg.QueueSize = size
		r := start(t, cfg, s)
		logs := plogotlp.NewGRPCClient(dial(t, r.GRPCAddr()))
		send := func(id string) error {
			_, err := logs.Export(context.Background(), logsReq(id), grpc.UseCompressor(grpcgzip.Name))
			return err
		}

		if err := send("1"); err != nil {
			t.Fatal(err)
		}
		s.waitEntered(t)
		for _, id := range []string{"2", "3"} {
			if err := send(id); err != nil {
				t.Fatalf("%s: %v", id, err)
			}
		}

		err := send("4")
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("a full queue answered %v, want Unavailable", err)
		}
		var delay time.Duration
		for _, d := range status.Convert(err).Details() {
			if ri, ok := d.(*errdetails.RetryInfo); ok {
				delay = ri.GetRetryDelay().AsDuration()
			}
		}
		if delay != time.Second {
			t.Errorf("RetryInfo says %v, want 1s", delay)
		}
		want := otlp.Stats{Queued: size, QueueSize: size, Accepted: 3, RefusedFull: 1}
		if st := r.Stats(); st != want {
			t.Errorf("stats: %+v, want %+v", st, want)
		}

		s.release()
		s.waitFor(t, "logs/1", "logs/2", "logs/3")

		// Room again.
		if err := send("5"); err != nil {
			t.Fatal(err)
		}
		s.waitFor(t, "logs/1", "logs/2", "logs/3", "logs/5")
	})

	t.Run("http", func(t *testing.T) {
		s := newHeldSink()
		cfg := config(false, true)
		cfg.QueueSize = size
		r := start(t, cfg, s)

		if a := sendHTTP(t, http.DefaultClient, r, "/v1/logs", logsReq("1")); a.code != http.StatusOK {
			t.Fatalf("%d %q", a.code, a.body)
		}
		s.waitEntered(t)
		if a := sendHTTP(t, http.DefaultClient, r, "/v1/metrics", metricsReq("2")); a.code != http.StatusOK {
			t.Fatalf("%d %q", a.code, a.body)
		}
		if a := sendHTTP(t, http.DefaultClient, r, "/v1/logs", logsReq("3")); a.code != http.StatusOK {
			t.Fatalf("%d %q", a.code, a.body)
		}

		a := sendHTTP(t, http.DefaultClient, r, "/v1/logs", logsReq("4"))
		if a.code != http.StatusServiceUnavailable || a.header.Get("Retry-After") != "1" {
			t.Fatalf("a full queue answered %d with Retry-After %q, want 503 and 1", a.code, a.header.Get("Retry-After"))
		}
		if st := refusal(t, a); codes.Code(st.GetCode()) != codes.Unavailable {
			t.Errorf("Status: %v", st)
		}
		want := otlp.Stats{Queued: size, QueueSize: size, Accepted: 3, RefusedFull: 1}
		if st := r.Stats(); st != want {
			t.Errorf("stats: %+v, want %+v", st, want)
		}

		s.release()
		s.waitFor(t, "logs/1", "metrics/2", "logs/3")
		if a := sendHTTP(t, http.DefaultClient, r, "/v1/logs", logsReq("5")); a.code != http.StatusOK {
			t.Fatalf("%d %q", a.code, a.body)
		}
		s.waitFor(t, "logs/1", "metrics/2", "logs/3", "logs/5")
	})
}

func TestTooLarge(t *testing.T) {
	const limit = 4096

	s := newSink()
	cfg := config(true, true)
	cfg.MaxRequestBytes = limit
	r := start(t, cfg, s)

	// Compressible: gzip makes it far smaller than the limit, and it inflates
	// past it.
	big := logsReq("big")
	big.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(strings.Repeat("x", 2*limit))
	body := encode(t, big, protoType)
	if len(body) <= limit || len(gzipped(t, body)) >= limit {
		t.Fatalf("the request is %d bytes, %d gzipped", len(body), len(gzipped(t, body)))
	}

	logs := plogotlp.NewGRPCClient(dial(t, r.GRPCAddr()))
	for _, opts := range [][]grpc.CallOption{nil, {grpc.UseCompressor(grpcgzip.Name)}} {
		if _, err := logs.Export(context.Background(), big, opts...); status.Code(err) != codes.ResourceExhausted {
			t.Errorf("grpc %v: %v, want ResourceExhausted", opts, err)
		}
	}

	url := "http://" + r.HTTPAddr() + "/v1/logs"
	for _, x := range []struct {
		name     string
		encoding string
		body     io.Reader
	}{
		{"as sent", "", bytes.NewReader(body)},
		// No Content-Length: refused while reading.
		{"as sent, chunked", "", io.MultiReader(bytes.NewReader(body))},
		{"inflated", "gzip", bytes.NewReader(gzipped(t, body))},
	} {
		a := post(t, http.DefaultClient, url, protoType, x.encoding, x.body)
		if a.code != http.StatusRequestEntityTooLarge {
			t.Errorf("http %s: %d, want 413", x.name, a.code)
			continue
		}
		if st := refusal(t, a); codes.Code(st.GetCode()) != codes.ResourceExhausted {
			t.Errorf("http %s: Status: %v", x.name, st)
		}
	}

	// Within the limit is taken.
	if _, err := logs.Export(context.Background(), logsReq("small")); err != nil {
		t.Fatal(err)
	}
	if a := sendHTTP(t, http.DefaultClient, r, "/v1/logs", logsReq("small")); a.code != http.StatusOK {
		t.Fatalf("%d %q", a.code, a.body)
	}
	s.waitFor(t, "logs/small", "logs/small")
	want := otlp.Stats{QueueSize: 16, Accepted: 2, RefusedTooLarge: 5}
	if st := r.Stats(); st != want {
		t.Errorf("stats: %+v, want %+v", st, want)
	}
}

func TestBad(t *testing.T) {
	s := newSink()
	r := start(t, config(true, true), s)
	base := "http://" + r.HTTPAddr()

	// A length-delimited field that claims more bytes than follow.
	malformed := []byte{0x0a, 0x05, 0x01}

	for _, x := range []struct {
		name        string
		method      string
		path        string
		contentType string
		encoding    string
		body        []byte
		code        int
	}{
		{"malformed protobuf", http.MethodPost, "/v1/logs", protoType, "", malformed, http.StatusBadRequest},
		{"malformed json", http.MethodPost, "/v1/metrics", jsonType, "", []byte(`{"resourceMetrics": [`), http.StatusBadRequest},
		{"broken gzip", http.MethodPost, "/v1/logs", protoType, "gzip", []byte("not gzip"), http.StatusBadRequest},
		{"media type", http.MethodPost, "/v1/logs", "text/plain", "", []byte("hello"), http.StatusUnsupportedMediaType},
		{"compression", http.MethodPost, "/v1/logs", protoType, "br", []byte{}, http.StatusUnsupportedMediaType},
		{"method", http.MethodGet, "/v1/logs", protoType, "", nil, http.StatusMethodNotAllowed},
		{"traces", http.MethodPost, "/v1/traces", protoType, "", []byte{}, http.StatusNotFound},
	} {
		req, err := http.NewRequest(x.method, base+x.path, bytes.NewReader(x.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", x.contentType)
		if x.encoding != "" {
			req.Header.Set("Content-Encoding", x.encoding)
		}
		a := do(t, http.DefaultClient, req)
		if a.code != x.code {
			t.Errorf("%s: %d %q, want %d", x.name, a.code, a.body, x.code)
			continue
		}
		if st := refusal(t, a); st.GetMessage() == "" {
			t.Errorf("%s: the Status says nothing", x.name)
		}
		switch x.code {
		case http.StatusMethodNotAllowed:
			if got := a.header.Get("Allow"); got != http.MethodPost {
				t.Errorf("%s: Allow is %q", x.name, got)
			}
		case http.StatusUnsupportedMediaType:
			if x.encoding != "" && a.header.Get("Accept-Encoding") != "gzip" {
				t.Errorf("%s: Accept-Encoding is %q", x.name, a.header.Get("Accept-Encoding"))
			}
		}
	}

	conn := dial(t, r.GRPCAddr())
	err := conn.Invoke(context.Background(), "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
		&malformed, new([]byte), grpc.ForceCodecV2(bytesCodec{}))
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("grpc malformed: %v, want InvalidArgument", err)
	}
	if _, err := ptraceotlp.NewGRPCClient(conn).Export(context.Background(), ptraceotlp.NewExportRequest()); status.Code(err) != codes.Unimplemented {
		t.Errorf("grpc traces: %v, want Unimplemented", err)
	}

	want := otlp.Stats{QueueSize: 16, RefusedBad: 9}
	if st := r.Stats(); st != want {
		t.Errorf("stats: %+v, want %+v", st, want)
	}
	if got := s.ids(); len(got) != 0 {
		t.Errorf("the sink got %q", got)
	}
}

// bytesCodec sends a *[]byte as it is: a request no gRPC client of OTLP sends.
type bytesCodec struct{}

func (bytesCodec) Name() string { return "proto" }

func (bytesCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(*v.(*[]byte))}, nil
}

func (bytesCodec) Unmarshal(data mem.BufferSlice, v any) error {
	*v.(*[]byte) = data.Materialize()
	return nil
}

func TestStopDrains(t *testing.T) {
	before := runtime.NumGoroutine()

	s := newHeldSink()
	cfg := config(true, true)
	cfg.QueueSize = 8
	r, err := otlp.New(cfg, s)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	conn := dial(t, r.GRPCAddr())
	logs := plogotlp.NewGRPCClient(conn)
	c := &http.Client{Transport: &http.Transport{}}

	if _, err := logs.Export(ctx, logsReq("1")); err != nil {
		t.Fatal(err)
	}
	s.waitEntered(t)
	if a := sendHTTP(t, c, r, "/v1/logs", logsReq("2")); a.code != http.StatusOK {
		t.Fatalf("%d %q", a.code, a.body)
	}
	if _, err := logs.Export(ctx, logsReq("3")); err != nil {
		t.Fatal(err)
	}
	if a := sendHTTP(t, c, r, "/v1/metrics", metricsReq("4")); a.code != http.StatusOK {
		t.Fatalf("%d %q", a.code, a.body)
	}

	cancel()

	// The listeners close, while the sink still holds the first request.
	refused := func(addr string) func() bool {
		return func() bool {
			c, err := net.Dial("tcp", addr)
			if err == nil {
				c.Close()
			}
			return err != nil
		}
	}
	eventually(t, refused(r.GRPCAddr()))
	eventually(t, refused(r.HTTPAddr()))
	select {
	case err := <-done:
		t.Fatalf("Run returned before the queue was handed over: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	s.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	if got, want := s.ids(), []string{"logs/1", "logs/2", "logs/3", "metrics/4"}; !slices.Equal(got, want) {
		t.Fatalf("sink got %q, want %q", got, want)
	}
	for _, g := range s.all() {
		if g.err != nil {
			t.Errorf("%s was handed over under a context that ended: %v", g.id, g.err)
		}
	}
	if err := r.Run(context.Background()); err == nil {
		t.Error("Run ran twice")
	}

	// Nothing the Receiver started outlives Run.
	conn.Close()
	c.CloseIdleConnections()
	eventually(t, func() bool { return runtime.NumGoroutine() <= before })
}

func TestStopTimeout(t *testing.T) {
	// A sink that does not take what is queued: Run gives up on it after the
	// timeout rather than wait.
	s := newHeldSink()
	r, err := otlp.New(config(true, false), s)
	if err != nil {
		t.Fatal(err)
	}
	otlp.SetStopTimeout(r, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	logs := plogotlp.NewGRPCClient(dial(t, r.GRPCAddr()))
	for _, id := range []string{"1", "2", "3"} {
		if _, err := logs.Export(ctx, logsReq(id)); err != nil {
			t.Fatal(err)
		}
	}
	s.waitEntered(t)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not give up on the sink")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelled != 1 || len(s.got) != 0 {
		t.Errorf("the sink was given up on %d times and took %d, want 1 and 0", s.cancelled, len(s.got))
	}
}

func TestNew(t *testing.T) {
	inUse, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inUse.Close()

	done, cancel := context.WithCancel(context.Background())
	cancel()
	for _, x := range []struct {
		name string
		cfg  func(*conf.OTLP)
		sink signal.Sink
	}{
		{"no sink", func(*conf.OTLP) {}, nil},
		{"no endpoint", func(c *conf.OTLP) { c.GRPC, c.HTTP = nil, nil }, newSink()},
		{"no queue", func(c *conf.OTLP) { c.QueueSize = 0 }, newSink()},
		{"no size", func(c *conf.OTLP) { c.MaxRequestBytes = 0 }, newSink()},
		{"address in use", func(c *conf.OTLP) { c.HTTP.Endpoint = inUse.Addr().String() }, newSink()},
	} {
		cfg := config(true, true)
		x.cfg(&cfg)
		if r, err := otlp.New(cfg, x.sink); err == nil {
			t.Errorf("%s: New did not refuse", x.name)
			r.Run(done)
		}
	}
}
