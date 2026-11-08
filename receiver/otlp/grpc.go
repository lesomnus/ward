package otlp

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // the Collector's otlp exporter compresses with gzip by default
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// retryAfter is how long a Collector is asked to wait before it sends again a
// request refused for a full queue.
const retryAfter = time.Second

// errFull answers a request the queue had no room for. UNAVAILABLE is what the
// Collector's otlp exporter retries; RetryInfo says how soon, as HTTP's
// Retry-After does.
var errFull = func() error {
	st, err := status.New(codes.Unavailable, "the queue is full; try again").
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(retryAfter)})
	if err != nil {
		panic(err)
	}
	return st.Err()
}()

// newGRPC serves OTLP's logs and metrics services.
//
// They are registered from a service description of their own rather than with
// pdata's plogotlp.RegisterGRPCServer, so that the handler is handed the
// request's bytes and decodes them itself, with pdata. Behind gRPC's codec, a
// request that is too large or does not decode fails before any handler or
// interceptor runs -- a malformed one answered INTERNAL rather than
// INVALID_ARGUMENT -- and nothing here would see it to count it.
func (r *Receiver) newGRPC() *grpc.Server {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(r.maxBytes),
		grpc.ForceServerCodecV2(rawCodec{}),
		grpc.WaitForHandlers(true),
		grpc.UnknownServiceHandler(func(any, grpc.ServerStream) error {
			r.refusedBad.Add(1)
			return status.Error(codes.Unimplemented, "Ward takes logs and metrics only")
		}),
	)
	s.RegisterService(grpcService{
		name:   "opentelemetry.proto.collector.logs.v1.LogsService",
		proto:  "opentelemetry/proto/collector/logs/v1/logs_service.proto",
		decode: decodeLogs,
	}.desc(), r)
	s.RegisterService(grpcService{
		name:   "opentelemetry.proto.collector.metrics.v1.MetricsService",
		proto:  "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
		decode: decodeMetrics,
	}.desc(), r)
	return s
}

// grpcService is one of OTLP's export services: a single unary Export.
type grpcService struct {
	name   string
	proto  string
	decode decoder
}

func (s grpcService) desc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: s.name,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Export",
			Handler: func(srv any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				return srv.(*Receiver).export(dec, s.decode)
			},
		}},
		Metadata: s.proto,
	}
}

// export takes one gRPC export request. Its answer is an empty
// ExportLogsServiceResponse or ExportMetricsServiceResponse, which are the
// same zero bytes.
func (r *Receiver) export(dec func(any) error, decode decoder) (any, error) {
	var body rawMessage
	if err := dec(&body); err != nil {
		switch st := status.Convert(err); st.Code() {
		case codes.ResourceExhausted:
			// Larger than MaxRecvMsgSize, as sent or decompressed.
			r.refusedTooLarge.Add(1)
		case codes.Internal:
			// The compression is broken.
			r.refusedBad.Add(1)
			return nil, status.Error(codes.InvalidArgument, st.Message())
		}
		return nil, err
	}

	b, err := decode(body, formatProto)
	if err != nil {
		r.refusedBad.Add(1)
		return nil, status.Errorf(codes.InvalidArgument, "not an OTLP export request: %v", err)
	}
	if !r.take(b) {
		return nil, errFull
	}
	return &rawMessage{}, nil
}

// rawMessage is a message's bytes, not decoded.
type rawMessage []byte

// rawCodec carries a rawMessage as it is, both ways: decompressed and size
// checked by gRPC, decoded by the receiver.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(*rawMessage)
	if !ok {
		return nil, fmt.Errorf("otlp: cannot marshal %T", v)
	}
	return mem.BufferSlice{mem.SliceBuffer(*m)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(*rawMessage)
	if !ok {
		return fmt.Errorf("otlp: cannot unmarshal into %T", v)
	}
	*m = data.Materialize()
	return nil
}
