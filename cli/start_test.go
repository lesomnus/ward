package cli_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/cli"
	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/conf"
)

const onceRules = `
version: 1
sources:
  app:
    liveness: none
rules:
  - id: app.error
    kind: log
    source: app
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 1}
    forget_after: 1h
    operator: {ack: none, latch: false}
`

// embedded is a configuration of Ward inside another program, on a database
// in dir, serving its API on the address answered.
func embedded(t *testing.T, dir string) (cmd.Config, string) {
	t.Helper()
	rules := filepath.Join(dir, "app.yaml")
	if err := os.WriteFile(rules, []byte(onceRules), 0o644); err != nil {
		t.Fatal(err)
	}
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := free.Addr().String()
	free.Close()

	var c cmd.Config
	c.Server.Addr = addr
	c.Db.Driver = "sqlite3"
	c.Db.Dsn = "file:" + filepath.Join(dir, "ward.db") + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	c.Watch.Broker = "none"
	c.Spin.Lease = "local"
	c.Operator = conf.Operator{MaxShelve: time.Hour}
	c.Engine = conf.Engine{
		Tick: 100 * time.Millisecond, EvalLag: conf.EvalLag{Log: 200 * time.Millisecond, Metric: 300 * time.Millisecond},
		RecoveryWindow: 10 * time.Second, Checkpoint: time.Second,
		Gate: conf.Gate{Hold: time.Second, UnobservableToStale: 5 * time.Second, Canary: conf.Canary{
			Interval: 100 * time.Millisecond, Timeout: time.Second, MaxRTT: 500 * time.Millisecond, CheckWindow: time.Second, MaxMissing: 1,
		}},
		Limits: conf.Limits{KeysPerRule: 10, NewIssuesPerRulePerMinute: 10},
	}
	c.Nodes = map[string]conf.Node{"n1": {Match: map[string]string{"host.name": "n1"}, Collector: "127.0.0.1:1"}}
	c.Providers = []conf.Provider{{Name: "app", Type: "file", Path: rules, PollInterval: time.Second, Debounce: 50 * time.Millisecond}}
	c.Store = conf.Store{Retention: time.Hour}
	return c, addr
}

// Ward inside another program: no receiver of its own, what the program hands
// Instance.Sink is evaluated, and the API answers on its address.
func TestStart(t *testing.T) {
	ctx := context.Background()
	c, addr := embedded(t, t.TempDir())

	c.Receivers.OTLP = &conf.OTLP{GRPC: &conf.Endpoint{Endpoint: "127.0.0.1:0"}, MaxRequestBytes: 1, QueueSize: 1}
	if _, err := cli.Start(ctx, c, 16); err == nil {
		t.Fatal("started with a receiver of its own inside another program")
	}
	c.Receivers.OTLP = nil

	i, err := cli.Start(ctx, c, 16)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := i.Shutdown(sctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	issues := app.NewIssueServiceClient(conn)

	waitIssue(t, issues, i, "app.error")
}

// appError hands Ward an ERROR record of the app on n1, as a Collector would.
func appError(t *testing.T, i *cli.Instance) {
	t.Helper()
	{
		ctx := context.Background()
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "app")
		rl.Resource().Attributes().PutStr("host.name", "n1")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(plog.SeverityNumberError)
		if err := i.Sink.ConsumeLogs(ctx, ld); err != nil {
			t.Fatalf("the sink failed its caller: %v", err)
		}
	}
}

// waitIssue sends app errors until the rule's Issue is open, and answers it.
func waitIssue(t *testing.T, issues app.IssueServiceClient, i *cli.Instance, rule string) *app.Issue {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for {
		// Until the provider's rules are in, a record matches nothing.
		appError(t, i)
		res, err := issues.List(ctx, app.IssueListRequest_builder{Filters: []*app.IssueFilter{app.IssueFilter_builder{Rule: &rule}.Build()}}.Build())
		if err == nil && len(res.GetItems()) > 0 {
			if s := res.GetItems()[0].GetState(); s != "active" {
				t.Fatalf("state %s", s)
			}
			return res.GetItems()[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no issue: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// What operators did is in the Store, and Ward carries on with it after a
// restart: an acknowledgement, a shelve, a Condition out of service.
func TestOperatorStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	c, addr := embedded(t, t.TempDir())
	c.Spin.Lease = "local"

	start := func() (*cli.Instance, *grpc.ClientConn) {
		i, err := cli.Start(ctx, c, 16)
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		return i, conn
	}
	stop := func(i *cli.Instance, conn *grpc.ClientConn) {
		conn.Close()
		sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := i.Shutdown(sctx); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}

	i, conn := start()
	issues := app.NewIssueServiceClient(conn)
	conds := app.NewConditionServiceClient(conn)
	is := waitIssue(t, issues, i, "app.error")
	if _, err := issues.Acknowledge(ctx, app.IssueAcknowledgeRequest_builder{Ref: app.IssueRef_builder{Id: is.GetId()}.Build()}.Build()); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ref := app.ConditionRef_builder{Rule: "app.error", Node: "n1"}.Build()
	if _, err := conds.Shelve(ctx, app.ConditionShelveRequest_builder{Ref: ref, Mode: "timed", Duration: durationpb.New(time.Hour)}.Build()); err != nil {
		t.Fatalf("shelve: %v", err)
	}
	if _, err := conds.RemoveFromService(ctx, app.ConditionRemoveFromServiceRequest_builder{Ref: ref}.Build()); err != nil {
		t.Fatalf("out of service: %v", err)
	}
	stop(i, conn)

	i, conn = start()
	defer stop(i, conn)
	issues = app.NewIssueServiceClient(conn)
	conds = app.NewConditionServiceClient(conn)
	all := true
	got, err := issues.Get(ctx, app.IssueGetRequest_builder{Ref: app.IssueRef_builder{Id: is.GetId()}.Build(), Select: app.IssueSelect_builder{All: &all}.Build()}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetAcked() || !got.GetShelved() || !got.GetOutOfService() || got.GetState() != "active" {
		t.Errorf("after the restart: acked %v shelved %v oos %v state %s", got.GetAcked(), got.GetShelved(), got.GetOutOfService(), got.GetState())
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		cv, err := conds.Get(ctx, app.ConditionGetRequest_builder{Ref: ref}.Build())
		if err == nil && cv.GetShelved() && cv.GetOutOfService() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the condition after the restart: %v %v", cv, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := issues.Acknowledge(ctx, app.IssueAcknowledgeRequest_builder{Ref: app.IssueRef_builder{Id: is.GetId()}.Build()}.Build()); err == nil {
		t.Error("acknowledged again after the restart")
	}
}
