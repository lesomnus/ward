// Package otlp is Ward's OTLP receiver. It takes the logs and metrics each
// node's Collector sends, over gRPC, HTTP or both, and hands them to a
// signal.Sink in OTel's data model.
//
// A request is decoded, put on a bounded queue and answered at once; one
// worker hands the queue to the sink in the order the requests arrived. The
// Collector waits for the answer only, so how long Ward takes over a signal
// is not time the Collector's pipeline spends waiting.
//
// The answers are chosen by what the Collector's exporters do with them. A
// full queue is answered "try again" -- gRPC UNAVAILABLE, HTTP 503 with
// Retry-After -- which they retry, so the signal waits in the Collector's own
// sending queue rather than being dropped here. A request that cannot succeed
// however often it is sent -- larger than max_request_bytes, malformed, or not
// logs or metrics -- is answered with a code they do not retry, and the
// Collector drops it.
//
// Traces are not taken.
package otlp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesomnus/otx/log"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/signal"
)

// stopTimeout bounds how long Run takes to stop once its context is done:
// letting the requests in flight finish, then handing the sink what is queued.
// The sink is fast, so a full queue is handed over well within it; what is
// still queued when it runs out is lost.
const stopTimeout = 5 * time.Second

// Receiver is an OTLP receiver on the endpoints its configuration names. New
// listens; Run serves.
type Receiver struct {
	sink     signal.Sink
	maxBytes int
	queue    queue

	grpcSrv *grpc.Server // nil when gRPC is not configured
	grpcLis net.Listener
	httpSrv *http.Server // nil when HTTP is not configured
	httpLis net.Listener

	stopTimeout time.Duration
	ran         atomic.Bool

	accepted        atomic.Uint64
	refusedFull     atomic.Uint64
	refusedTooLarge atomic.Uint64
	refusedBad      atomic.Uint64
	sinkErrors      atomic.Uint64
}

// New listens on the endpoints cfg names, gRPC, HTTP or both, right away, so
// the addresses are known before Run (an endpoint may name port 0). It does not
// serve until Run. To let go of the listeners without serving, call Run with a
// context that is already done.
func New(cfg conf.OTLP, sink signal.Sink) (*Receiver, error) {
	switch {
	case sink == nil:
		return nil, errors.New("otlp: no sink")
	case cfg.GRPC == nil && cfg.HTTP == nil:
		return nil, errors.New("otlp: neither grpc nor http is configured")
	case cfg.MaxRequestBytes <= 0:
		return nil, fmt.Errorf("otlp: max_request_bytes must be positive, not %d", cfg.MaxRequestBytes)
	case cfg.QueueSize <= 0:
		return nil, fmt.Errorf("otlp: queue_size must be positive, not %d", cfg.QueueSize)
	}

	r := &Receiver{
		sink:        sink,
		maxBytes:    cfg.MaxRequestBytes,
		queue:       queue{ch: make(chan batch, cfg.QueueSize)},
		stopTimeout: stopTimeout,
	}
	if cfg.GRPC != nil {
		l, err := net.Listen("tcp", cfg.GRPC.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("otlp: grpc: %w", err)
		}
		r.grpcLis = l
		r.grpcSrv = r.newGRPC()
	}
	if cfg.HTTP != nil {
		l, err := net.Listen("tcp", cfg.HTTP.Endpoint)
		if err != nil {
			if r.grpcLis != nil {
				r.grpcLis.Close()
			}
			return nil, fmt.Errorf("otlp: http: %w", err)
		}
		r.httpLis = l
		r.httpSrv = r.newHTTP()
	}
	return r, nil
}

// GRPCAddr is the address gRPC listens on, or "" when it is not configured.
func (r *Receiver) GRPCAddr() string { return addr(r.grpcLis) }

// HTTPAddr is the address HTTP listens on, or "" when it is not configured.
func (r *Receiver) HTTPAddr() string { return addr(r.httpLis) }

func addr(l net.Listener) string {
	if l == nil {
		return ""
	}
	return l.Addr().String()
}

// Run serves until ctx is done, then stops and returns. It is called once.
//
// Stopping is bounded by a few seconds. The listeners close first and the
// requests in flight are answered; then the worker hands the sink what is
// still queued, under a context that is not cancelled with ctx but carries its
// values. What the sink has not taken when the time runs out is lost: the
// Collector was told it arrived. Run returns once the worker has returned, so a
// sink that ignores its context holds Run up.
//
// A transport that fails while serving stops the other, and Run returns its
// error.
func (r *Receiver) Run(ctx context.Context) error {
	if !r.ran.CompareAndSwap(false, true) {
		return errors.New("otlp: Run called twice")
	}

	// What the sink and the stop run under: ctx's values, not its end.
	base := context.WithoutCancel(ctx)
	sinkCtx, stopSink := context.WithCancel(base)
	defer stopSink()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		r.drain(sinkCtx)
	}()

	served := make(chan error, 2)
	serving := 0
	if r.grpcSrv != nil {
		serving++
		go func() {
			err := r.grpcSrv.Serve(r.grpcLis)
			if errors.Is(err, grpc.ErrServerStopped) {
				err = nil
			}
			if err != nil {
				err = fmt.Errorf("otlp: grpc: %w", err)
			}
			served <- err
		}()
	}
	if r.httpSrv != nil {
		serving++
		go func() {
			err := r.httpSrv.Serve(r.httpLis)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			if err != nil {
				err = fmt.Errorf("otlp: http: %w", err)
			}
			served <- err
		}()
	}

	// A transport stops by itself only when it fails.
	var errs []error
	select {
	case <-ctx.Done():
	case err := <-served:
		serving--
		errs = append(errs, err)
	}

	stopCtx, cancel := context.WithTimeout(base, r.stopTimeout)
	defer cancel()
	r.stopTransports(stopCtx)
	for ; serving > 0; serving-- {
		errs = append(errs, <-served)
	}

	// No handler is left to push now, except one HTTP cut off by the timeout,
	// which the closed queue refuses with "try again".
	r.queue.close()
	select {
	case <-drained:
	case <-stopCtx.Done():
		stopSink()
		<-drained
		if n := len(r.queue.ch); n > 0 {
			log.From(base).ErrorContext(base, "otlp: hand the queue to the sink before stopping",
				slog.Any("err", fmt.Errorf("%d requests still queued after %v", n, r.stopTimeout)))
		}
	}

	return errors.Join(errs...)
}

// stopTransports closes the listeners and waits for the requests in flight to
// be answered, until ctx is done; then it cuts the connections left.
func (r *Receiver) stopTransports(ctx context.Context) {
	var wg sync.WaitGroup
	if r.grpcSrv != nil {
		wg.Go(func() {
			stopped := make(chan struct{})
			go func() {
				r.grpcSrv.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-ctx.Done():
				// Waits for the handlers too (grpc.WaitForHandlers).
				r.grpcSrv.Stop()
				<-stopped
			}
		})
	}
	if r.httpSrv != nil {
		wg.Go(func() {
			if err := r.httpSrv.Shutdown(ctx); err != nil {
				r.httpSrv.Close()
			}
		})
	}
	wg.Wait()
}

// drain hands the sink the queue, in order, until the queue is closed and
// empty or ctx is done.
func (r *Receiver) drain(ctx context.Context) {
	for b := range r.queue.ch {
		err := b.to(ctx, r.sink)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// The Collector was told the request arrived; nothing is left to
			// do with it but say so.
			r.sinkErrors.Add(1)
			log.From(ctx).ErrorContext(ctx, "otlp: the sink did not take a request", slog.Any("err", err))
		}
	}
}

// take puts b on the queue, and says whether there was room.
func (r *Receiver) take(b batch) bool {
	if !r.queue.push(b) {
		r.refusedFull.Add(1)
		return false
	}
	r.accepted.Add(1)
	return true
}

// Stats is what a Receiver has done since New: how full its queue is, and how
// it answered the requests it got, over both transports.
type Stats struct {
	// Requests waiting for the sink, and how many the queue holds.
	Queued    int
	QueueSize int

	// Requests taken into the queue and answered as received.
	Accepted uint64

	// Requests refused because the queue had no room, or because Ward was
	// stopping. They were answered "try again", and the Collector sends them
	// again.
	RefusedFull uint64

	// Requests refused for being larger than max_request_bytes, as sent or
	// decompressed. The Collector drops them.
	RefusedTooLarge uint64

	// Requests refused for not being logs or metrics in OTLP: malformed, of a
	// media type or compression not taken, sent to a path or service not
	// served (traces). The Collector drops them.
	RefusedBad uint64

	// Accepted requests the sink returned an error for. They are lost.
	SinkErrors uint64
}

// Stats says what the Receiver has done so far. It is safe to call at any
// time, from any goroutine.
func (r *Receiver) Stats() Stats {
	return Stats{
		Queued:          len(r.queue.ch),
		QueueSize:       cap(r.queue.ch),
		Accepted:        r.accepted.Load(),
		RefusedFull:     r.refusedFull.Load(),
		RefusedTooLarge: r.refusedTooLarge.Load(),
		RefusedBad:      r.refusedBad.Load(),
		SinkErrors:      r.sinkErrors.Load(),
	}
}

// queue is the bounded queue between the transports and the sink. A push
// never waits: it takes the batch or says there is no room. A push after close
// says there is no room, rather than panic on the closed channel.
type queue struct {
	mu     sync.RWMutex
	closed bool
	ch     chan batch
}

func (q *queue) push(b batch) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return false
	}
	select {
	case q.ch <- b:
		return true
	default:
		return false
	}
}

// close ends the queue: what it holds is still drained, and a push is refused.
func (q *queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	close(q.ch)
}

// batch is what one request carried, decoded and waiting for the sink.
type batch interface {
	to(ctx context.Context, s signal.Sink) error
}

type logsBatch plog.Logs

func (b logsBatch) to(ctx context.Context, s signal.Sink) error {
	return s.ConsumeLogs(ctx, plog.Logs(b))
}

type metricsBatch pmetric.Metrics

func (b metricsBatch) to(ctx context.Context, s signal.Sink) error {
	return s.ConsumeMetrics(ctx, pmetric.Metrics(b))
}

// format is how a request's body is encoded. gRPC is always protobuf.
type format int

const (
	formatProto format = iota
	formatJSON
)

// decoder turns a request's body into the batch it carries.
type decoder func(body []byte, f format) (batch, error)

type unmarshaler interface {
	UnmarshalProto([]byte) error
	UnmarshalJSON([]byte) error
}

func unmarshal(m unmarshaler, body []byte, f format) error {
	if f == formatJSON {
		return m.UnmarshalJSON(body)
	}
	return m.UnmarshalProto(body)
}

func decodeLogs(body []byte, f format) (batch, error) {
	req := plogotlp.NewExportRequest()
	if err := unmarshal(req, body, f); err != nil {
		return nil, err
	}
	return logsBatch(req.Logs()), nil
}

func decodeMetrics(body []byte, f format) (batch, error) {
	req := pmetricotlp.NewExportRequest()
	if err := unmarshal(req, body, f); err != nil {
		return nil, err
	}
	return metricsBatch(req.Metrics()), nil
}
