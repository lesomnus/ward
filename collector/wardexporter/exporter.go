// Package wardexporter is Ward as an OpenTelemetry Collector exporter (design
// §8.4): built into a Collector, it takes what the Collector's pipelines end
// with and evaluates it, and serves Ward's API, from inside the Collector.
//
// It hands Ward what it is given and returns at once, with no error, ever: a
// Collector can pass an exporter's error back to the apps that sent the
// signal. What Ward cannot take in is dropped and counted, and the canaries
// dropped with it are how Ward knows.
//
// A logs and a metrics exporter of the same name are one Ward.
package wardexporter

import (
	"context"
	"log/slog"
	"sync"

	"github.com/lesomnus/otx/log"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"

	"github.com/lesomnus/ward/cli"
)

var typ = component.MustNewType("ward")

// NewFactory answers with the factory of the `ward` exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(typ,
		func() component.Config { return &Config{} },
		exporter.WithLogs(createLogs, component.StabilityLevelDevelopment),
		exporter.WithMetrics(createMetrics, component.StabilityLevelDevelopment),
	)
}

func createLogs(_ context.Context, set exporter.Settings, c component.Config) (exporter.Logs, error) {
	return &wardExporter{w: acquire(set.ID, c.(*Config), set.Logger)}, nil
}

func createMetrics(_ context.Context, set exporter.Settings, c component.Config) (exporter.Metrics, error) {
	return &wardExporter{w: acquire(set.ID, c.(*Config), set.Logger)}, nil
}

// shared is the one Ward the exporters of a name are.
type shared struct {
	id     component.ID
	cfg    *Config
	logger *zap.Logger

	mu      sync.Mutex
	started int
	inst    *cli.Instance
}

var (
	mu    sync.Mutex
	wards = map[component.ID]*shared{}
)

func acquire(id component.ID, c *Config, logger *zap.Logger) *shared {
	mu.Lock()
	defer mu.Unlock()
	w, ok := wards[id]
	if !ok {
		w = &shared{id: id, cfg: c, logger: logger}
		wards[id] = w
	}
	return w
}

func (w *shared) start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.started++
	if w.inst != nil {
		return nil
	}
	c, err := w.cfg.load()
	if err != nil {
		w.started--
		return err
	}
	ctx = log.Into(context.WithoutCancel(ctx), slog.New(zapslog.NewHandler(w.logger.Core())))
	inst, err := cli.Start(ctx, c, w.cfg.QueueSize)
	if err != nil {
		w.started--
		return err
	}
	w.inst = inst
	return nil
}

func (w *shared) stop(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started == 0 {
		return nil
	}
	w.started--
	if w.started > 0 || w.inst == nil {
		return nil
	}
	err := w.inst.Shutdown(ctx)
	w.inst = nil
	mu.Lock()
	delete(wards, w.id)
	mu.Unlock()
	return err
}

func (w *shared) sink() *cli.Instance {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.inst
}

type wardExporter struct {
	w       *shared
	started bool
}

func (x *wardExporter) Start(ctx context.Context, _ component.Host) error {
	if err := x.w.start(ctx); err != nil {
		return err
	}
	x.started = true
	return nil
}

func (x *wardExporter) Shutdown(ctx context.Context) error {
	if !x.started {
		return nil
	}
	x.started = false
	return x.w.stop(ctx)
}

func (x *wardExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeLogs hands the logs to Ward. It never fails.
func (x *wardExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if i := x.w.sink(); i != nil {
		i.Sink.ConsumeLogs(ctx, ld)
	}
	return nil
}

// ConsumeMetrics hands the metrics to Ward. It never fails.
func (x *wardExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if i := x.w.sink(); i != nil {
		i.Sink.ConsumeMetrics(ctx, md)
	}
	return nil
}
