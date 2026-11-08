// Command emitter plays a service that sends signals to Ward. It sends logs
// and metrics with the OpenTelemetry Go SDK to its node's Collector, the way
// any service would, and changes what it sends when told to over HTTP: start
// and stop a repeating error, release a condition, go silent, hang, crash. The
// control endpoint is described in README.md.
//
// It is the service of the local environment (deploy/compose), and is not part
// of Ward. Nothing in it is particular to Ward but the attribute names it
// writes (`diag.condition.*`), which are a convention and not a protocol.
//
// # What it always sends
//
// A liveness counter -- a cumulative Sum, `emitter.frames` by default -- that
// is created with Add(0) at start and grows by one per iteration of the main
// loop. Its data points arriving say the process is alive, and its
// StartTimeUnixNano, with the resource's `service.instance.id`, says when this
// run of it started. Both change when the process restarts: the instance id is
// a new random one per start.
//
// # Configuration
//
// The exporters read the standard OpenTelemetry variables:
// OTEL_EXPORTER_OTLP_ENDPOINT (an http:// endpoint is a plain-text
// connection), OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES. The rest are
// flags, each with a variable that sets its default.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// scope is the instrumentation scope of everything the emitter sends.
const scope = "github.com/lesomnus/ward/example/emitter"

type config struct {
	// Where the HTTP control endpoint listens.
	control string

	// The name of the liveness counter.
	liveness string

	// How often the main loop runs: the liveness counter grows by one per run.
	tick time.Duration

	// How often metrics are exported. Short, so that a window of a few seconds
	// holds more than one data point; the SDK's own default is 60 s.
	exportInterval time.Duration

	// How often the batched logs are exported.
	logInterval time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.control, "control", envString("EMITTER_CONTROL", ":8080"), "address of the HTTP control endpoint (EMITTER_CONTROL)")
	flag.StringVar(&c.liveness, "liveness", envString("EMITTER_LIVENESS", "emitter.frames"), "name of the liveness counter (EMITTER_LIVENESS)")
	flag.DurationVar(&c.tick, "tick", envDuration("EMITTER_TICK", 100*time.Millisecond), "period of the main loop; the liveness counter grows by one per run (EMITTER_TICK)")
	flag.DurationVar(&c.exportInterval, "export-interval", envDuration("EMITTER_EXPORT_INTERVAL", time.Second), "metric export interval (EMITTER_EXPORT_INTERVAL)")
	flag.DurationVar(&c.logInterval, "log-interval", envDuration("EMITTER_LOG_INTERVAL", time.Second), "log batch export interval (EMITTER_LOG_INTERVAL)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, c); err != nil {
		slog.ErrorContext(ctx, "emitter stopped", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, c config) error {
	if c.tick <= 0 || c.exportInterval <= 0 || c.logInterval <= 0 {
		return errors.New("-tick, -export-interval and -log-interval are positive")
	}

	// A new instance id per start, as OpenTelemetry recommends: a restart is a
	// new instance, which is how Ward tells one run from the next beside the
	// counter's start time.
	instance := newInstanceID()
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.instance.id", instance),
	))
	if err != nil {
		return fmt.Errorf("resource: %w", err)
	}

	logs, err := otlploggrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("log exporter: %w", err)
	}
	// Cumulative, written out rather than left to the default or to
	// OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: a delta counter sends
	// no data point while nothing happens, and then "zero" and "no data" look
	// the same.
	metrics, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector))
	if err != nil {
		return fmt.Errorf("metric exporter: %w", err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logs, sdklog.WithExportInterval(c.logInterval))),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metrics, sdkmetric.WithInterval(c.exportInterval))),
	)
	defer func() {
		// Shutting the providers down is what exports the last batch. A crash
		// (POST /exit) skips this on purpose.
		last, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := errors.Join(lp.Shutdown(last), mp.Shutdown(last)); err != nil {
			slog.ErrorContext(last, "flush on shutdown", slog.Any("err", err))
		}
	}()

	service, _ := res.Set().Value("service.name")
	e, err := newEmitter(ctx, c, lp.Logger(scope), mp.Meter(scope), service.AsString(), instance)
	if err != nil {
		return err
	}

	l, err := net.Listen("tcp", c.control)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: e.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		srv.Shutdown(stop)
	}()
	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "control endpoint", slog.Any("err", err))
		}
	}()

	slog.InfoContext(ctx, "emitting",
		slog.String("service.name", e.service),
		slog.String("service.instance.id", instance),
		slog.String("liveness", c.liveness),
		slog.String("control", l.Addr().String()),
	)
	e.loop(ctx)
	return nil
}

// newInstanceID is a random UUID (version 4).
func newInstanceID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func envString(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return fallback
}

// envDuration is the duration a variable says, or fallback when it is not set.
// A variable that is set and is not a duration stops the program: running on a
// value nobody wrote would be worse.
func envDuration(name string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "emitter: %s: %v\n", name, err)
		os.Exit(2)
	}
	return d
}
