package cli

import (
	"context"
	"log/slog"
	"maps"
	"net"
	"slices"

	"github.com/lesomnus/otx/log"
	"github.com/lesomnus/xli"
	"golang.org/x/sync/errgroup"

	"github.com/lesomnus/payday/spin"

	"github.com/lesomnus/ward/api"
	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/engine"
	"github.com/lesomnus/ward/exporter"
	"github.com/lesomnus/ward/provider"
	"github.com/lesomnus/ward/receiver/otlp"
)

// Running is what `ward serve` runs besides the gRPC server: the evaluation
// loop, the providers that feed it definitions, the receiver that feeds it
// signals, and the canary that checks the nodes' paths.
type Running struct {
	Engine   *engine.Engine
	Registry *provider.Registry
	Receiver *otlp.Receiver
	Canary   *engine.Canary
	Exporter *exporter.Exporter
}

// Assemble builds what `ward serve` runs on s, and hands the API its services.
func Assemble(ctx context.Context, s *cmd.Server, w conf.Ward) (*Running, error) {
	store := &engine.Store{Ent: s.Ent, Drv: s.Drv, Sink: s.Sink}
	e := engine.New(w, store, engine.Options{Log: log.From(ctx)})

	var ps []provider.Provider
	for _, pc := range w.Providers {
		p, err := provider.New(pc)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	nodes := slices.Sorted(maps.Keys(w.Nodes))
	reg := provider.NewRegistry(nodes, ps, func(set *def.Set, warns []string) {
		e.Apply(set)
		for _, x := range warns {
			log.From(ctx).WarnContext(ctx, "definitions", slog.String("warning", x))
		}
	})

	rcv, err := otlp.New(*w.Receivers.OTLP, e.Norm)
	if err != nil {
		return nil, err
	}

	s.Backend.Set(e)
	svc := &api.Services{Engine: e, Providers: reg.Status, Receiver: rcv.Stats, Tick: w.Engine.Tick}
	s.Register = append(s.Register, svc.Register)

	return &Running{
		Engine: e, Registry: reg, Receiver: rcv, Canary: e.NewCanary(),
		Exporter: &exporter.Exporter{Engine: e, Receiver: rcv.Stats, Configs: w.Exporters, Log: log.From(ctx)},
	}, nil
}

// Run runs each part in g until ctx is done.
func (r *Running) Run(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error { return r.Registry.Run(ctx) })
	g.Go(func() error { return r.Engine.Run(ctx) })
	g.Go(func() error { return r.Receiver.Run(ctx) })
	g.Go(func() error { return r.Canary.Run(ctx) })
	g.Go(func() error { return r.Exporter.Run(ctx) })
}

// NewCmdServe is `ward serve`: bring the database up to date, then answer.
//
// The migrations always run first (Migrate): a server that answered on a
// database it disagrees with would fail per request, in whichever handler
// touches what moved.
func NewCmdServe(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "serve",
		Brief: "bring the database up to date, then answer requests",

		Handler: xli.OnRun(func(ctx context.Context, _ *xli.Command, next xli.Next) error {
			ctx, done, err := Telemetry(ctx, c)
			if err != nil {
				return err
			}
			defer done()

			s, err := cmd.Build(ctx, *c)
			if err != nil {
				return err
			}
			defer s.Close()

			w := c.Ward()
			if err := w.Validate(); err != nil {
				return err
			}

			if err := Migrate(ctx, s); err != nil {
				return err
			}

			r, err := Assemble(ctx, s, w)
			if err != nil {
				return err
			}

			l, err := net.Listen("tcp", c.Server.ListenAddr())
			if err != nil {
				return err
			}
			log.From(ctx).InfoContext(ctx, "grpc", slog.String("addr", l.Addr().String()))

			lease, err := c.Spin.Build(ctx, c.Db)
			if err != nil {
				return err
			}

			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error { return spin.Run(ctx, slices.Values(s.Spin), spin.WithLease(lease)) })
			g.Go(func() error { return s.Serve(ctx, *c, l) })
			r.Run(ctx, g)

			return g.Wait()
		}),
	}
}
