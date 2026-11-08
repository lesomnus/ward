package cli

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/lesomnus/ward/signal"
)

// Running is what Ward runs besides the gRPC server: the evaluation loop, the
// providers that feed it definitions, what feeds it signals, and the canary
// that checks the nodes' paths.
type Running struct {
	Engine   *engine.Engine
	Registry *provider.Registry
	Canary   *engine.Canary
	Exporter *exporter.Exporter

	// What signals arrive by: Ward's own OTLP receiver, or -- inside a
	// Collector -- the queue the Collector hands them to. One of the two.
	Receiver *otlp.Receiver
	Queue    *signal.Queue
}

// Assemble builds what Ward runs on s, and hands the API its services. With a
// receivers block Ward listens for OTLP itself; without one, Running.Queue
// takes what the program Ward runs in hands it, queue requests at most.
func Assemble(ctx context.Context, s *cmd.Server, w conf.Ward, queue int) (*Running, error) {
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

	r := &Running{Engine: e, Registry: reg, Canary: e.NewCanary()}
	var stats func() otlp.Stats
	if w.Receivers.OTLP != nil {
		rcv, err := otlp.New(*w.Receivers.OTLP, e.Norm)
		if err != nil {
			return nil, err
		}
		r.Receiver, stats = rcv, rcv.Stats
	} else {
		if queue <= 0 {
			return nil, errors.New("no receiver, and no queue to take signals in")
		}
		q := signal.NewQueue(e.Norm, queue)
		r.Queue = q
		stats = func() otlp.Stats {
			st := q.Stats()
			return otlp.Stats{Queued: st.Queued, QueueSize: st.Size, Accepted: st.Taken, RefusedFull: st.Dropped}
		}
	}
	r.Exporter = &exporter.Exporter{Engine: e, Receiver: stats, Configs: w.Exporters, Log: log.From(ctx)}

	s.Backend.Set(e)
	svc := &api.Services{Engine: e, Providers: reg.Status, Receiver: stats}
	s.Register = append(s.Register, svc.Register)
	return r, nil
}

// Run runs each part in g until ctx is done.
func (r *Running) Run(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error { return r.Registry.Run(ctx) })
	g.Go(func() error { return r.Engine.Run(ctx) })
	g.Go(func() error { return r.Canary.Run(ctx) })
	g.Go(func() error { return r.Exporter.Run(ctx) })
	if r.Receiver != nil {
		g.Go(func() error { return r.Receiver.Run(ctx) })
	}
	if r.Queue != nil {
		g.Go(func() error { return r.Queue.Run(ctx) })
	}
}

// serve runs the gRPC server on l, the sweeps under lease and everything in r,
// until ctx is done or one of them fails.
func serve(ctx context.Context, c *cmd.Config, s *cmd.Server, r *Running, l net.Listener, lease spin.Lease) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return spin.Run(ctx, slices.Values(s.Spin), spin.WithLease(lease)) })
	g.Go(func() error { return s.Serve(ctx, *c, l) })
	r.Run(ctx, g)
	return g.Wait()
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

			w := c.Ward()
			if err := w.Validate(); err != nil {
				return err
			}

			s, err := cmd.Build(ctx, *c)
			if err != nil {
				return err
			}
			defer s.Close()

			if err := Migrate(ctx, s); err != nil {
				return err
			}

			r, err := Assemble(ctx, s, w, 0)
			if err != nil {
				return err
			}

			lease, err := c.Spin.Build(ctx, c.Db)
			if err != nil {
				return err
			}
			l, err := net.Listen("tcp", c.Server.ListenAddr())
			if err != nil {
				return err
			}
			log.From(ctx).InfoContext(ctx, "grpc", slog.String("addr", l.Addr().String()))

			return serve(ctx, c, s, r, l, lease)
		}),
	}
}

// Instance is Ward run inside another program: everything `ward serve` runs
// but its OTLP receiver. The program hands it signals through Sink, which
// never waits and never fails (signal.Queue).
type Instance struct {
	Sink signal.Sink

	s      *cmd.Server
	cancel context.CancelFunc
	done   chan error
}

// Start brings Ward up inside another program: it checks c, migrates the
// database, and serves the API on c.Server, with no receiver of its own. queue
// is how many hand-overs Sink holds before it drops.
func Start(ctx context.Context, c cmd.Config, queue int) (*Instance, error) {
	w := c.Ward()
	if err := w.ValidateEmbedded(); err != nil {
		return nil, err
	}
	if queue <= 0 {
		return nil, fmt.Errorf("queue_size: positive, got %d", queue)
	}
	s, err := cmd.Build(ctx, c)
	if err != nil {
		return nil, err
	}
	if err := Migrate(ctx, s); err != nil {
		s.Close()
		return nil, err
	}
	r, err := Assemble(ctx, s, w, queue)
	if err != nil {
		s.Close()
		return nil, err
	}
	lease, err := c.Spin.Build(ctx, c.Db)
	if err != nil {
		s.Close()
		return nil, err
	}
	l, err := net.Listen("tcp", c.Server.ListenAddr())
	if err != nil {
		s.Close()
		return nil, err
	}
	log.From(ctx).InfoContext(ctx, "grpc", slog.String("addr", l.Addr().String()))

	run, cancel := context.WithCancel(context.WithoutCancel(ctx))
	i := &Instance{Sink: r.Queue, s: s, cancel: cancel, done: make(chan error, 1)}
	go func() { i.done <- serve(run, &c, s, r, l, lease) }()
	return i, nil
}

// Shutdown stops what Start started, waiting for it until ctx is done.
func (i *Instance) Shutdown(ctx context.Context) error {
	i.cancel()
	var err error
	select {
	case err = <-i.done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return errors.Join(err, i.s.Close())
}
