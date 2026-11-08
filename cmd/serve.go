package cmd

import (
	"context"
	"database/sql"
	"net"
	"time"

	"github.com/lesomnus/otx/log"
	"github.com/protobuf-orm/ent/dialect"
	entsql "github.com/protobuf-orm/ent/dialect/sql"
	"google.golang.org/grpc"

	"github.com/lesomnus/payday/access"
	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/grpcx"
	"github.com/lesomnus/payday/resource"
	"github.com/lesomnus/payday/trail"
	"github.com/lesomnus/payday/watch"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/engine"
	"github.com/lesomnus/ward/internal/ent"
	"github.com/lesomnus/ward/layer"
	"github.com/lesomnus/ward/server/bare"
	"github.com/lesomnus/ward/server/pd"
)

// Server is Ward built: the database it runs on, the stack a caller reaches,
// and the server the evaluation loop writes through.
type Server struct {
	Db      *sql.DB
	Ent     *ent.Client
	Drv     dialect.Driver
	Dialect string
	Watch   *watch.Watch

	// Walled is what a caller reaches: Gate, Audit, Seal, Ops, then the sink.
	Walled app.Server

	// Ungated is what works out who is calling. It has no wall and no gate, so
	// it is never handed to anything a caller can reach.
	Ungated app.Server

	// Sink is what the evaluation loop writes through: under every layer of
	// Ward's, so the Seal does not refuse it. The loop's writes are kept off the
	// trail by the mark it puts on its context (engine.UnlessMarked).
	Sink app.Server

	// Auth reads a credential when a caller sends one. Ward asks for none: every
	// method is public (Everyone), so a call without one is served.
	Auth   auth.Handler
	Policy gate.Policy

	Access *access.Writer
	Spin   []any
}

// Build opens the database and stacks the servers.
func Build(ctx context.Context, c Config) (*Server, error) {
	db, dialect, err := c.Db.Open(ctx)
	if err != nil {
		return nil, err
	}

	drv := entsql.OpenDB(dialect, db)
	client := ent.NewClient(ent.Driver(drv))

	b, err := c.Watch.Build(ctx, c.Db)
	if err != nil {
		db.Close()
		return nil, err
	}
	w := watch.New(b)

	content, err := c.Audit.Contents()
	if err != nil {
		db.Close()
		return nil, err
	}

	// The trail records every write a caller made, inside its transaction. The
	// evaluation loop's own writes are taken out of it (engine.UnlessMarked).
	rec := bare.Recorders{
		engine.UnlessMarked{Recorder: pd.Recorder(content)},
		pd.ResourceRecorder(w, c.Watch.Outbox, nil, resource.RecorderOptions{}),
		pd.AccessRecorder(),
	}
	opts := []bare.Option{bare.WithMinter(pd.Minter()), bare.WithRecorder(rec)}

	sink, err := pd.NewSink(client, opts...)
	if err != nil {
		db.Close()
		return nil, err
	}
	walled, err := pd.NewSink(client, append(opts, bare.WithScope(pd.Wall()))...)
	if err != nil {
		db.Close()
		return nil, err
	}

	// The last builder handles a call first: Gate, Audit, Seal, Ops, then the sink.
	stacked, err := app.Build(walled.WithWatch(w), layer.OpsBuild(), layer.SealBuild(), pd.AuditBuild(), pd.GateBuild())
	if err != nil {
		db.Close()
		return nil, err
	}
	ungated, err := app.Build(sink.WithWatch(w), pd.AuditBuild())
	if err != nil {
		db.Close()
		return nil, err
	}

	s := &Server{
		Db: db, Ent: client, Drv: drv, Dialect: dialect, Watch: w,
		Walled: stacked, Ungated: ungated, Sink: sink.WithWatch(w),
	}

	s.Access = &access.Writer{Sink: pd.AccessSink(client)}
	s.Spin = append(s.Spin, s.Access)
	if c.Watch.Outbox && b != nil {
		s.Spin = append(s.Spin, pd.Drain(client, b, c.Watch.Every()))
	}

	p, err := c.Audit.Policy()
	if err != nil {
		db.Close()
		return nil, err
	}
	if c.Spin.Lease != "" && c.Spin.Lease != config.LeaseNone {
		if p.Lease, err = c.Spin.Build(ctx, c.Db); err != nil {
			db.Close()
			return nil, err
		}
	}
	if p.On() {
		log.From(ctx).InfoContext(ctx, "trail: retention", "policy", p.String())
		s.Spin = append(s.Spin, trail.Sweep(pd.TrailStore(client), p))
	}

	return s, nil
}

func (s *Server) Close() error { return s.Db.Close() }

// Everyone serves every method without asking who is calling. Ward has no
// authentication yet; payday's default, auth.PublicDefault, answers only health
// and reflection and refuses everything else as Unauthenticated.
func Everyone(string) bool { return true }

// Grpc builds the server every call arrives at. Only Ward's own services are
// registered: payday's entities (tenant, holder, trail, outbox and the rest)
// are in the database and are not served.
func (s *Server) Grpc(ctx context.Context, c Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	h := s.Auth
	if h == nil {
		h = auth.Plain()
	}

	chain := grpcx.Serving(ctx, grpcx.WithDeadline(c.Server.CallTimeout())).
		WithUnary(auth.InterceptorUnary(h, Resolver(s.Ungated), Everyone)).
		WithStream(auth.InterceptorStream(h, Resolver(s.Ungated), Everyone)).
		WithUnary(grpcx.LimitUnary(c.Server.Limiter(), gate.ByTenant(), pd.Cost)).
		With(s.accessed().Interceptor()).
		With(gate.Interceptor(s.Policy)).
		With(s.Watch.Interceptor()).
		WithUnary(grpcx.ClosedUnary(c.Server.Closed()))

	os := append(opts, chain.ServerOptions()...)
	vs, err := c.Server.GrpcOptions()
	if err != nil {
		return nil, err
	}
	os = append(os, vs...)

	g := grpc.NewServer(os...)
	app.RegisterIssueServiceServer(g, s.Walled.Issue())

	return g, nil
}

func (s *Server) accessed() access.Config {
	return access.Config{
		Subjects: pd.Subjects,
		Refs:     pd.SubjectRefs,
		Address:  access.Peer,
		Writer:   s.Access,
	}
}

// Serve answers on l until ctx is done.
func (s *Server) Serve(ctx context.Context, c Config, l net.Listener) error {
	g, err := s.Grpc(ctx, c)
	if err != nil {
		return err
	}

	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		g.GracefulStop()
		close(stopped)
	}()

	err = g.Serve(l)
	if ctx.Err() == nil {
		return err
	}
	<-stopped

	last, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.Access.Flush(last); err != nil {
		log.From(ctx).ErrorContext(ctx, "access: the record waiting to be written was lost as the server stopped", "rows", s.Access.Pending(), "err", err)
	}

	return err
}
