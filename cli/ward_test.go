package cli_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/cli"
	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/engine"
)

type env struct {
	ctx context.Context
	s   *cmd.Server
	cl  app.IssueServiceClient
}

// setup is Ward as `ward serve` brings it up -- an empty database, migrated, and
// the gRPC server over it -- with a client that sends no credential.
func setup(t *testing.T) env {
	t.Helper()
	ctx := context.Background()

	var c cmd.Config
	c.Db.Driver = "sqlite3"
	c.Db.Dsn = "file:" + filepath.Join(t.TempDir(), "ward.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	c.Watch.Broker = "none"

	s, err := cmd.Build(ctx, c)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if err := cli.Migrate(ctx, s); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	g, err := s.Grpc(ctx, c)
	if err != nil {
		t.Fatalf("grpc: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///ward",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return env{ctx: ctx, s: s, cl: app.NewIssueServiceClient(conn)}
}

func (e env) trail(t *testing.T) int {
	t.Helper()
	n, err := e.s.Ent.Audit.Query().Count(e.ctx)
	if err != nil {
		t.Fatalf("trail: %v", err)
	}
	return n
}

func TestMigrateTwice(t *testing.T) {
	e := setup(t)
	if err := cli.Migrate(e.ctx, e.s); err != nil {
		t.Fatalf("a migrated database migrates again without change: %v", err)
	}
}

func TestEngineWritesAndCallerReads(t *testing.T) {
	e := setup(t)

	var id []byte
	err := engine.Tick(e.ctx, e.s.Drv, e.s.Sink, func(ctx context.Context, s app.Server) error {
		v, err := s.Issue().Add(ctx, app.IssueAddRequest_builder{
			Rule: "probe.rule", Source: "probe", Node: "n1", State: "active", Severity: "error",
		}.Build())
		if err != nil {
			return err
		}
		id = v.GetId()
		return nil
	})
	if err != nil {
		t.Fatalf("tick: %v", err)
	}

	all := true
	got, err := e.cl.Get(e.ctx, app.IssueGetRequest_builder{
		Ref:    app.IssueRef_builder{Id: id}.Build(),
		Select: app.IssueSelect_builder{All: &all}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("an anonymous caller reads the Issue: %v", err)
	}
	if got.GetRule() != "probe.rule" || got.GetState() != "active" || got.GetSeverity() != "error" {
		t.Errorf("read back %v", got)
	}

	active := "active"
	list, err := e.cl.List(e.ctx, app.IssueListRequest_builder{
		Filters: []*app.IssueFilter{app.IssueFilter_builder{State: &active}.Build()},
	}.Build())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if n := len(list.GetItems()); n != 1 {
		t.Errorf("listed %d active issues, want 1", n)
	}

	if n := e.trail(t); n != 0 {
		t.Errorf("the evaluation loop's write is on the trail: %d rows", n)
	}
}

func TestTrailKeepsUnmarkedWrites(t *testing.T) {
	e := setup(t)

	// The control for the filter: the same write without the loop's mark.
	if _, err := e.s.Sink.Issue().Add(e.ctx, app.IssueAddRequest_builder{
		Rule: "probe.control", Source: "probe", Node: "n1", State: "active", Severity: "error",
	}.Build()); err != nil {
		t.Fatalf("add: %v", err)
	}
	if n := e.trail(t); n != 1 {
		t.Errorf("an unmarked write left %d trail rows, want 1", n)
	}
}

func TestCallersCannotWrite(t *testing.T) {
	e := setup(t)

	_, err := e.cl.Add(e.ctx, app.IssueAddRequest_builder{Rule: "x", State: "active"}.Build())
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("Add: %v, want PermissionDenied from the Seal", err)
	}

	_, err = e.cl.Erase(e.ctx, app.IssueRef_builder{Id: make([]byte, 16)}.Build())
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("Erase: %v, want PermissionDenied from the Seal", err)
	}

	f := false
	_, err = e.cl.Patch(e.ctx, app.IssuePatchRequest_builder{Ref: app.IssueRef_builder{Id: make([]byte, 16)}.Build(), Acked: &f}.Build())
	if code := status.Code(err); code != codes.Unimplemented {
		t.Errorf("Patch: %v, want Unimplemented: closed at the transport", err)
	}
}
