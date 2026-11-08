package otlp

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	protoType = "application/x-protobuf"
	jsonType  = "application/json"
)

func (f format) contentType() string {
	if f == formatJSON {
		return jsonType
	}
	return protoType
}

// formatOf is the format a Content-Type names, and whether it names one this
// takes.
func formatOf(contentType string) (format, bool) {
	t, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return formatProto, false
	}
	switch t {
	case protoType:
		return formatProto, true
	case jsonType:
		return formatJSON, true
	}
	return formatProto, false
}

// newHTTP serves OTLP/HTTP's logs and metrics paths, /v1/logs and /v1/metrics,
// which is where the Collector's otlphttp exporter sends them.
func (r *Receiver) newHTTP() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/v1/logs", r.httpExport(decodeLogs))
	mux.Handle("/v1/metrics", r.httpExport(decodeMetrics))
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		r.refusedBad.Add(1)
		f, _ := formatOf(req.Header.Get("Content-Type"))
		writeStatus(w, f, http.StatusNotFound, codes.Unimplemented, "Ward takes logs and metrics only, at /v1/logs and /v1/metrics")
	})
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       time.Minute,
	}
}

// httpExport takes one OTLP/HTTP export request: protobuf or JSON, plain or
// gzip. The answer is in the request's encoding: an empty export response, or
// a google.rpc.Status saying why not.
//
// Only a full queue is answered 503, which the Collector's otlphttp exporter
// retries after Retry-After. Everything else refused is a 4xx it does not.
func (r *Receiver) httpExport(decode decoder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		f, ok := formatOf(req.Header.Get("Content-Type"))
		if req.Method != http.MethodPost {
			r.refusedBad.Add(1)
			w.Header().Set("Allow", http.MethodPost)
			writeStatus(w, f, http.StatusMethodNotAllowed, codes.Unimplemented, "only POST is served")
			return
		}
		if !ok {
			r.refusedBad.Add(1)
			writeStatus(w, f, http.StatusUnsupportedMediaType, codes.InvalidArgument,
				fmt.Sprintf("Content-Type is %s or %s, not %q", protoType, jsonType, req.Header.Get("Content-Type")))
			return
		}

		body, err := readBody(w, req, r.maxBytes)
		var unsupported unsupportedEncoding
		switch {
		case errors.Is(err, errTooLarge):
			r.refusedTooLarge.Add(1)
			writeStatus(w, f, http.StatusRequestEntityTooLarge, codes.ResourceExhausted,
				fmt.Sprintf("the request is larger than %d bytes", r.maxBytes))
			return
		case errors.As(err, &unsupported):
			r.refusedBad.Add(1)
			w.Header().Set("Accept-Encoding", "gzip")
			writeStatus(w, f, http.StatusUnsupportedMediaType, codes.InvalidArgument, err.Error())
			return
		case err != nil:
			r.refusedBad.Add(1)
			writeStatus(w, f, http.StatusBadRequest, codes.InvalidArgument, fmt.Sprintf("reading the body: %v", err))
			return
		}

		b, err := decode(body, f)
		if err != nil {
			r.refusedBad.Add(1)
			writeStatus(w, f, http.StatusBadRequest, codes.InvalidArgument, fmt.Sprintf("not an OTLP export request: %v", err))
			return
		}
		if !r.take(b) {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter/time.Second)))
			writeStatus(w, f, http.StatusServiceUnavailable, codes.Unavailable, "the queue is full; try again")
			return
		}

		w.Header().Set("Content-Type", f.contentType())
		w.WriteHeader(http.StatusOK)
		if f == formatJSON {
			// The empty export response; in protobuf it is no bytes at all.
			w.Write([]byte("{}"))
		}
	})
}

// errTooLarge is a body larger than max_request_bytes.
var errTooLarge = errors.New("too large")

type unsupportedEncoding string

func (e unsupportedEncoding) Error() string {
	return fmt.Sprintf("Content-Encoding is gzip or none, not %q", string(e))
}

// readBody reads req's body, decompressed. A body larger than limit bytes, as
// sent or decompressed, is errTooLarge, and is not read past the limit: a small
// gzip that inflates to gigabytes is refused at the limit.
func readBody(w http.ResponseWriter, req *http.Request, limit int) ([]byte, error) {
	gz := false
	switch enc := strings.ToLower(strings.TrimSpace(req.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
	case "gzip", "x-gzip":
		gz = true
	default:
		return nil, unsupportedEncoding(enc)
	}

	if req.ContentLength > int64(limit) {
		return nil, errTooLarge
	}
	var body io.Reader = http.MaxBytesReader(w, req.Body, int64(limit))
	if gz {
		zr, err := gzip.NewReader(body)
		if err != nil {
			return nil, tooLargeOr(err)
		}
		defer zr.Close()
		body = zr
	}

	b, err := io.ReadAll(io.LimitReader(body, int64(limit)+1))
	if err != nil {
		return nil, tooLargeOr(err)
	}
	if len(b) > limit {
		return nil, errTooLarge
	}
	return b, nil
}

func tooLargeOr(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return errTooLarge
	}
	return err
}

// writeStatus answers a request that was refused. OTLP/HTTP says why in a
// google.rpc.Status, encoded as the request was (protobuf when that is not
// known); the Collector puts its message in the error it logs.
func writeStatus(w http.ResponseWriter, f format, code int, c codes.Code, msg string) {
	st := status.New(c, msg).Proto()
	var b []byte
	if f == formatJSON {
		b, _ = protojson.Marshal(st)
	} else {
		b, _ = proto.Marshal(st)
	}
	w.Header().Set("Content-Type", f.contentType())
	w.WriteHeader(code)
	w.Write(b)
}
