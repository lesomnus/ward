package cli_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/cli"
	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/conf"
)

// collector plays a node's Collector: what is exported to it goes on to Ward
// with the node's host.name, as the diag pipeline sends it.
type collector struct {
	host string
	logs plogotlp.GRPCClient
	mets pmetricotlp.GRPCClient
}

type collectorLogs struct {
	plogotlp.UnimplementedGRPCServer
	c *collector
}

func (x *collectorLogs) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ld := req.Logs()
	for i := range ld.ResourceLogs().Len() {
		ld.ResourceLogs().At(i).Resource().Attributes().PutStr("host.name", x.c.host)
	}
	_, err := x.c.logs.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	return plogotlp.NewExportResponse(), err
}

type collectorMetrics struct {
	pmetricotlp.UnimplementedGRPCServer
	c *collector
}

func (x *collectorMetrics) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	md := req.Metrics()
	for i := range md.ResourceMetrics().Len() {
		md.ResourceMetrics().At(i).Resource().Attributes().PutStr("host.name", x.c.host)
	}
	_, err := x.c.mets.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
	return pmetricotlp.NewExportResponse(), err
}

// exported keeps what Ward exports of itself.
type exported struct {
	plogotlp.UnimplementedGRPCServer
	mu     sync.Mutex
	events []string
}

func (x *exported) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	rls := req.Logs().ResourceLogs()
	for i := range rls.Len() {
		sls := rls.At(i).ScopeLogs()
		for j := range sls.Len() {
			lrs := sls.At(j).LogRecords()
			for k := range lrs.Len() {
				ev, _ := lrs.At(k).Attributes().Get("diag.issue.event")
				reason, _ := lrs.At(k).Attributes().Get("diag.issue.reason")
				x.events = append(x.events, ev.AsString()+" "+reason.AsString())
			}
		}
	}
	return plogotlp.NewExportResponse(), nil
}

func (x *exported) seen() []string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return slices.Clone(x.events)
}

const appRules = `
version: 1
sources:
  app:
    liveness: {metric: app.frames, max_gap: 1s}
  ghost:
    liveness: {metric: ghost.beats, max_gap: 1s}
components:
  app:
    parent: ""
    members:
      - {rule: app.fail, node: n1, key: x}
      - {rule: app.down, node: n1}
  ghost:
    parent: ""
    members:
      - {rule: ghost.fail, node: n1, key: k}
rules:
  - id: app.down
    kind: metric
    source: app
    severity: error
    metric: {name: app.frames}
    key: []
    value: {of: points, window: 2s, across: sum}
    limit: {levels: [{name: down, below: 1, severity: error}], deadband: 0}
    activate: {for: 0s}
    resolve: {for: 1s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
  - id: ghost.fail
    kind: log
    source: ghost
    severity: error
    match: {attributes: {diag.condition.type: ghost.fail}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 1}
    resolve_after: 1s
    on_source_restart: close
    operator: {ack: none, latch: false}
  - id: app.fail
    kind: log
    source: app
    severity: error
    match: {attributes: {diag.condition.type: app.fail}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 2, within: 500ms}
    resolve_after: 1s
    on_source_restart: close
    operator: {ack: required, latch: false}
`

// TestServe is `ward serve` in one process: a node's Collector forwarding to
// Ward's OTLP receiver, the canary going round through it, a rule from a
// provider file, and the API answering. A condition the app reports opens an
// Issue, and silence with the app alive resolves it.
func TestServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()

	rules := filepath.Join(dir, "app.yaml")
	if err := os.WriteFile(rules, []byte(appRules), 0o644); err != nil {
		t.Fatal(err)
	}

	// The node's Collector, listening before Ward knows where it is.
	cl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	col := &collector{host: "n1"}

	// Where Ward exports its own records.
	xl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ex := &exported{}
	xg := grpc.NewServer()
	plogotlp.RegisterGRPCServer(xg, ex)
	go xg.Serve(xl)
	defer xg.Stop()

	var c cmd.Config
	c.Db.Driver = "sqlite3"
	c.Db.Dsn = "file:" + filepath.Join(dir, "ward.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	c.Watch.Broker = "none"
	c.Operator = conf.Operator{MaxShelve: time.Hour}
	c.Receivers = conf.Receivers{OTLP: &conf.OTLP{
		GRPC: &conf.Endpoint{Endpoint: "127.0.0.1:0"}, MaxRequestBytes: 1 << 22, QueueSize: 64,
	}}
	c.Engine = conf.Engine{
		Tick: 100 * time.Millisecond, EvalLag: conf.EvalLag{Log: 300 * time.Millisecond, Metric: 400 * time.Millisecond},
		RecoveryWindow: 10 * time.Second, Checkpoint: time.Second,
		Gate: conf.Gate{Hold: time.Second, UnobservableToStale: 5 * time.Second, Canary: conf.Canary{
			Interval: 100 * time.Millisecond, Timeout: time.Second, MaxRTT: 500 * time.Millisecond,
			CheckWindow: time.Second, MaxMissing: 1,
		}},
		Limits: conf.Limits{KeysPerRule: 10, NewIssuesPerRulePerMinute: 10},
	}
	c.Nodes = map[string]conf.Node{"n1": {Match: map[string]string{"host.name": "n1"}, Collector: cl.Addr().String()}}
	c.Providers = []conf.Provider{{Name: "app", Type: "file", Path: rules, PollInterval: time.Second, Debounce: 50 * time.Millisecond}}
	c.Store = conf.Store{Retention: time.Hour}
	c.Exporters = []conf.Exporter{{Type: "otlp", Endpoint: xl.Addr().String(), Insecure: true}}
	if err := c.Ward().Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}

	s, err := cmd.Build(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := cli.Migrate(ctx, s); err != nil {
		t.Fatal(err)
	}
	r, err := cli.Assemble(ctx, s, c.Ward(), 0)
	if err != nil {
		t.Fatal(err)
	}

	// The Collector forwards to Ward's receiver.
	wc, err := grpc.NewClient(r.Receiver.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer wc.Close()
	col.logs, col.mets = plogotlp.NewGRPCClient(wc), pmetricotlp.NewGRPCClient(wc)
	cg := grpc.NewServer()
	plogotlp.RegisterGRPCServer(cg, &collectorLogs{c: col})
	pmetricotlp.RegisterGRPCServer(cg, &collectorMetrics{c: col})
	go cg.Serve(cl)
	defer cg.Stop()

	g, err := s.Grpc(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go g.Serve(lis)
	defer g.Stop()
	conn, err := grpc.NewClient("passthrough:///ward",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	issues := app.NewIssueServiceClient(conn)
	nodes := app.NewNodeServiceClient(conn)

	eg, ectx := errgroup.WithContext(ctx)
	r.Run(ectx, eg)
	defer func() {
		cancel()
		if err := eg.Wait(); err != nil && ctx.Err() == nil {
			t.Errorf("serve: %v", err)
		}
	}()

	// The app: its liveness counter and the node's uptime, all along.
	ac, err := grpc.NewClient(cl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer ac.Close()
	appLogs, appMets := plogotlp.NewGRPCClient(ac), pmetricotlp.NewGRPCClient(ac)
	started := time.Now().Add(-time.Minute)
	booted := time.Now().Add(-time.Hour)
	go func() {
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		n := int64(0)
		for {
			select {
			case <-ectx.Done():
				return
			case <-tk.C:
			}
			n++
			now := time.Now()
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("service.name", "app")
			ms := rm.ScopeMetrics().AppendEmpty().Metrics()
			m := ms.AppendEmpty()
			m.SetName("app.frames")
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dp := sum.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(started))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
			dp.SetIntValue(n)
			up := ms.AppendEmpty()
			up.SetName("system.uptime")
			udp := up.SetEmptyGauge().DataPoints().AppendEmpty()
			udp.SetTimestamp(pcommon.NewTimestampFromTime(now))
			udp.SetDoubleValue(now.Sub(booted).Seconds())
			appMets.Export(ectx, pmetricotlp.NewExportRequestFromMetrics(md))
		}
	}()
	fail := func() {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "app")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(plog.SeverityNumberError)
		lr.Attributes().PutStr("diag.condition.type", "app.fail")
		lr.Attributes().PutStr("diag.condition.key", "x")
		if _, err := appLogs.Export(ctx, plogotlp.NewExportRequestFromLogs(ld)); err != nil {
			t.Errorf("app export: %v", err)
		}
	}

	waitFor := func(d time.Duration, what string, f func() (bool, string)) {
		t.Helper()
		deadline := time.Now().Add(d)
		last := ""
		for time.Now().Before(deadline) {
			ok, s := f()
			if ok {
				return
			}
			last = s
			time.Sleep(50 * time.Millisecond)
		}
		snap := r.Engine.Snapshot()
		for _, c := range snap.Conditions {
			t.Logf("condition %+v", c)
		}
		t.Logf("stats %+v", r.Engine.Norm.Stats())
		t.Logf("rules %d providers %+v", len(snap.Defs.Rules), r.Registry.Status())
		t.Fatalf("%s: not within %s; last: %s", what, d, last)
	}

	waitFor(15*time.Second, "n1 observable", func() (bool, string) {
		n, err := nodes.Get(ctx, app.NodeGetRequest_builder{Ref: app.NodeRef_builder{Name: "n1"}.Build()}.Build())
		if err != nil {
			return false, err.Error()
		}
		return n.GetObservable(), n.GetReason()
	})

	rule := "app.fail"
	issue := func() (*app.Issue, string) {
		res, err := issues.List(ctx, app.IssueListRequest_builder{Filters: []*app.IssueFilter{app.IssueFilter_builder{Rule: &rule}.Build()}}.Build())
		if err != nil {
			return nil, err.Error()
		}
		if len(res.GetItems()) == 0 {
			return nil, "none"
		}
		i := res.GetItems()[0]
		return i, i.GetState() + " " + i.GetCloseReason()
	}

	fail()
	time.Sleep(100 * time.Millisecond)
	fail()
	waitFor(10*time.Second, "issue opened", func() (bool, string) {
		i, s := issue()
		return i != nil && i.GetState() == "active", s
	})
	waitFor(15*time.Second, "issue resolved", func() (bool, string) {
		i, s := issue()
		return i != nil && i.GetState() == "closed" && i.GetCloseReason() == "resolved.silence", s
	})
	i, _ := issue()
	if !i.GetAttention() || i.GetCount() != 2 {
		t.Errorf("closed issue: attention %v count %d", i.GetAttention(), i.GetCount())
	}

	// The component tree says what is expected, normal or not seen at all.
	comps := app.NewComponentServiceClient(conn)
	conds := app.NewConditionServiceClient(conn)
	waitFor(5*time.Second, "the app's component", func() (bool, string) {
		c, err := comps.Get(ctx, app.ComponentGetRequest_builder{Ref: app.ComponentRef_builder{Name: "app"}.Build()}.Build())
		if err != nil {
			return false, err.Error()
		}
		var states []string
		ok := c.GetHealth() == "healthy" && slices.Equal(c.GetRules(), []string{"app.down", "app.fail"}) && len(c.GetConditions()) == 2
		for _, x := range c.GetConditions() {
			states = append(states, fmt.Sprintf("%s/%s=%s held=%v value=%v", x.GetRule(), x.GetKey(), x.GetState(), x.GetHeld(), x.GetValue()))
			if x.GetState() != "inactive" || x.GetHeld() || !x.GetDeclared() {
				ok = false
			}
			if x.GetRule() == "app.down" && (!x.HasValue() || x.GetValue() < 1) {
				ok = false
			}
		}
		return ok, c.GetHealth() + " " + fmt.Sprint(c.GetRules(), states)
	})
	ghost, err := comps.Get(ctx, app.ComponentGetRequest_builder{Ref: app.ComponentRef_builder{Name: "ghost"}.Build()}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if ghost.GetHealth() != "no_data" || len(ghost.GetConditions()) != 1 || ghost.GetConditions()[0].GetState() != "unobserved" {
		t.Errorf("a source that sends nothing: %s %v", ghost.GetHealth(), ghost.GetConditions())
	}
	if _, err := conds.Get(ctx, app.ConditionGetRequest_builder{Ref: app.ConditionRef_builder{Rule: "app.fail", Node: "n1", Key: "nobody"}.Build()}.Build()); status.Code(err) != codes.NotFound {
		t.Errorf("a condition nothing saw nor expects: %v, want NotFound", err)
	}

	waitFor(5*time.Second, "exported", func() (bool, string) {
		got := ex.seen()
		return slices.Equal(got, []string{"open ", "close resolved.silence"}), fmt.Sprint(got)
	})
}
