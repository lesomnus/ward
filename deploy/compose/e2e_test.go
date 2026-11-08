//go:build e2e

// The end-to-end scenarios run against the compose environment (deploy/README.md):
//
//	cd deploy/compose && docker compose up -d --build
//	go test -tags e2e -count=1 -v ./deploy/compose/
//
// They drive the emitters' control endpoints and docker compose, and read
// what Ward says through its gRPC API. They leave the environment running.
package compose_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	app "github.com/lesomnus/ward"
)

type e2e struct {
	t    *testing.T
	ctx  context.Context
	conn *grpc.ClientConn

	issues app.IssueServiceClient
	nodes  app.NodeServiceClient
	status app.StatusServiceClient
	srcs   app.SourceServiceClient
	conds  app.ConditionServiceClient
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func setup(t *testing.T) *e2e {
	conn, err := grpc.NewClient(env("WARD_API", "localhost:14319"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &e2e{
		t: t, ctx: context.Background(), conn: conn,
		issues: app.NewIssueServiceClient(conn),
		nodes:  app.NewNodeServiceClient(conn),
		status: app.NewStatusServiceClient(conn),
		srcs:   app.NewSourceServiceClient(conn),
		conds:  app.NewConditionServiceClient(conn),
	}
}

// ctl sends a command to an emitter: 1 and 2 are the emitters on edge-1 and
// edge-2, 3 the reporter.
func (e *e2e) ctl(emitter int, path string) {
	e.t.Helper()
	res, err := http.Post(fmt.Sprintf("http://localhost:1808%d%s", emitter, path), "", nil)
	if err != nil {
		e.t.Fatalf("emitter %d %s: %v", emitter, path, err)
	}
	res.Body.Close()
	if res.StatusCode/100 != 2 {
		e.t.Fatalf("emitter %d %s: %s", emitter, path, res.Status)
	}
}

func (e *e2e) compose(args ...string) {
	e.t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		e.t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// waitFor polls f until it answers true, or fails the test after d.
func (e *e2e) waitFor(d time.Duration, what string, f func() (bool, string)) {
	e.t.Helper()
	deadline := time.Now().Add(d)
	last := ""
	for time.Now().Before(deadline) {
		ok, s := f()
		if ok {
			e.t.Logf("%s: %s", what, s)
			return
		}
		last = s
		time.Sleep(500 * time.Millisecond)
	}
	e.t.Fatalf("%s: not within %s; last: %s", what, d, last)
}

// latest is the last-opened Issue of a rule on a node with a key.
func (e *e2e) latest(rule, node, key string) *app.Issue {
	res, err := e.issues.List(e.ctx, app.IssueListRequest_builder{
		Filters: []*app.IssueFilter{app.IssueFilter_builder{Rule: &rule, Node: &node}.Build()},
		Size:    500,
	}.Build())
	if err != nil {
		e.t.Fatalf("list issues: %v", err)
	}
	var out *app.Issue
	for _, i := range res.GetItems() {
		if i.GetKey() != key {
			continue
		}
		if out == nil || i.GetDateOpened().AsTime().After(out.GetDateOpened().AsTime()) {
			out = i
		}
	}
	return out
}

func show(i *app.Issue) string {
	if i == nil {
		return "no issue"
	}
	return fmt.Sprintf("%s %s/%s/%s %s %s level=%q count=%d restarts=%d attention=%v",
		i.GetState(), i.GetRule(), i.GetNode(), i.GetKey(), i.GetCloseReason(), i.GetSeverity(), i.GetLevel(), i.GetCount(), i.GetRestarts(), i.GetAttention())
}

func (e *e2e) issueIs(rule, node, key string, after time.Time, state, reason string) func() (bool, string) {
	return func() (bool, string) {
		i := e.latest(rule, node, key)
		if i == nil || i.GetDateOpened().AsTime().Before(after) {
			return false, show(i)
		}
		return i.GetState() == state && (reason == "" || i.GetCloseReason() == reason), show(i)
	}
}

func (e *e2e) observable(node string, want bool) func() (bool, string) {
	return func() (bool, string) {
		n, err := e.nodes.Get(e.ctx, app.NodeGetRequest_builder{Ref: app.NodeRef_builder{Name: node}.Build()}.Build())
		if err != nil {
			return false, err.Error()
		}
		return n.GetObservable() == want, fmt.Sprintf("observable=%v reason=%q rtt=%s missing=%d", n.GetObservable(), n.GetReason(), n.GetCanary().GetRtt().AsDuration(), n.GetCanary().GetMissing())
	}
}

func TestEndToEnd(t *testing.T) {
	e := setup(t)

	// Follow the change feed from the start, to see it carry what happens.
	var (
		mu   sync.Mutex
		feed []*app.IssueEvent
	)
	fctx, stop := context.WithCancel(e.ctx)
	defer stop()
	stream, err := app.NewIssueEventServiceClient(e.conn).Follow(fctx, app.IssueEventFollowRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	go func() {
		for {
			v, err := stream.Recv()
			if err != nil {
				return
			}
			mu.Lock()
			feed = append(feed, v)
			mu.Unlock()
		}
	}()

	t.Run("nodes become observable", func(t *testing.T) {
		e.t = t
		e.waitFor(90*time.Second, "edge-1", e.observable("edge-1", true))
		e.waitFor(30*time.Second, "edge-2", e.observable("edge-2", true))
	})

	t.Run("a repeating condition opens and resolves by silence", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.ctl(1, "/condition/repeat?type=emitter.overheat&key=left&every=300ms")
		e.waitFor(15*time.Second, "open", e.issueIs("emitter.overheat", "edge-1", "left", start, "active", ""))
		e.ctl(1, "/condition/stop?type=emitter.overheat&key=left")
		e.waitFor(30*time.Second, "resolved", e.issueIs("emitter.overheat", "edge-1", "left", start, "closed", "resolved.silence"))
	})

	t.Run("a one-shot condition is released explicitly, not by a string", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.ctl(1, "/condition/once?type=emitter.disconnected&key=cam")
		e.waitFor(15*time.Second, "open", e.issueIs("emitter.disconnected", "edge-1", "cam", start, "active", ""))

		e.ctl(1, "/condition/release?type=emitter.disconnected&key=cam&as=string")
		e.waitFor(15*time.Second, "type violation reported", func() (bool, string) {
			st, err := e.status.Get(e.ctx, app.StatusGetRequest_builder{}.Build())
			if err != nil {
				return false, err.Error()
			}
			for _, v := range st.GetViolations() {
				if v.GetAttribute() == "diag.condition.active" && v.GetGot() == "string" {
					return true, fmt.Sprintf("%s %s want %s got %s x%d", v.GetSource(), v.GetAttribute(), v.GetWant(), v.GetGot(), v.GetCount())
				}
			}
			return false, fmt.Sprint(st.GetViolations())
		})
		if i := e.latest("emitter.disconnected", "edge-1", "cam"); i.GetState() != "active" {
			t.Fatalf("a string \"false\" released it: %s", show(i))
		}

		e.ctl(1, "/condition/release?type=emitter.disconnected&key=cam")
		e.waitFor(15*time.Second, "released", e.issueIs("emitter.disconnected", "edge-1", "cam", start, "closed", "resolved.explicit"))

		i := e.latest("emitter.disconnected", "edge-1", "cam")
		if !i.GetAttention() {
			t.Fatalf("closed without an ack, and nobody needs to look: %s", show(i))
		}
		acked, err := e.issues.Acknowledge(e.ctx, app.IssueAcknowledgeRequest_builder{Ref: app.IssueRef_builder{Id: i.GetId()}.Build(), Comment: "replugged"}.Build())
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if acked.GetAttention() || !acked.GetAcked() {
			t.Fatalf("after ack: %s", show(acked))
		}
		if _, err := e.issues.Acknowledge(e.ctx, app.IssueAcknowledgeRequest_builder{Ref: app.IssueRef_builder{Id: i.GetId()}.Build()}.Build()); err == nil {
			t.Fatalf("acknowledged twice")
		}
	})

	t.Run("a gauge crosses levels", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.ctl(2, "/gauge?name=battery.charge&value=15&attr.battery.id=main")
		e.waitFor(30*time.Second, "low", func() (bool, string) {
			i := e.latest("emitter.battery.low", "edge-2", "")
			return i != nil && !i.GetDateOpened().AsTime().Before(start) && i.GetLevel() == "low", show(i)
		})
		e.ctl(2, "/gauge?name=battery.charge&value=5&attr.battery.id=main")
		e.waitFor(30*time.Second, "low_low", func() (bool, string) {
			i := e.latest("emitter.battery.low", "edge-2", "")
			return i.GetLevel() == "low_low" && i.GetSeverity() == "error", show(i)
		})
		e.ctl(2, "/gauge?name=battery.charge&value=80&attr.battery.id=main")
		// It latches: cleared, waiting for a reset.
		e.waitFor(40*time.Second, "cleared", e.issueIs("emitter.battery.low", "edge-2", "", start, "cleared", "resolved.condition"))
		i := e.latest("emitter.battery.low", "edge-2", "")
		if _, err := e.issues.Reset(e.ctx, app.IssueResetRequest_builder{Ref: app.IssueRef_builder{Id: i.GetId()}.Build(), Comment: "charged"}.Build()); err != nil {
			t.Fatalf("reset: %v", err)
		}
		e.waitFor(5*time.Second, "closed by the reset", e.issueIs("emitter.battery.low", "edge-2", "", start, "closed", "resolved.condition"))
	})

	t.Run("a discrete state", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.ctl(2, "/hw?id=disk0&state=degraded")
		e.waitFor(30*time.Second, "degraded", func() (bool, string) {
			i := e.latest("emitter.hardware", "edge-2", "disk0")
			return i != nil && !i.GetDateOpened().AsTime().Before(start) && i.GetLevel() == "degraded", show(i)
		})
		e.ctl(2, "/hw?id=disk0&state=ok")
		e.waitFor(40*time.Second, "ok", e.issueIs("emitter.hardware", "edge-2", "disk0", start, "closed", "resolved.condition"))
		e.ctl(2, "/hw/stop?id=disk0")
	})

	t.Run("a process that is down, then back", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.compose("stop", "emitter-2")
		e.waitFor(40*time.Second, "down", e.issueIs("emitter.down", "edge-2", "", start, "active", ""))
		e.compose("start", "emitter-2")
		e.waitFor(60*time.Second, "back", e.issueIs("emitter.down", "edge-2", "", start, "closed", "resolved.condition"))
	})

	t.Run("a crash loop", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		for range 2 {
			e.ctl(1, "/exit?code=3&after=100ms")
			time.Sleep(2 * time.Second)
			e.waitFor(30*time.Second, "back up", func() (bool, string) {
				res, err := http.Get("http://localhost:18081/")
				if err != nil {
					return false, err.Error()
				}
				res.Body.Close()
				return true, res.Status
			})
			time.Sleep(3 * time.Second)
		}
		e.waitFor(40*time.Second, "crash loop", e.issueIs("emitter.crash_loop", "edge-1", "", start, "active", ""))
		src, err := e.srcs.Get(e.ctx, app.SourceGetRequest_builder{Ref: app.SourceRef_builder{Name: "emitter"}.Build()}.Build())
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range src.GetNodes() {
			t.Logf("source emitter on %s: alive=%v started=%s restarted=%s instance=%s", n.GetNode(), n.GetAlive(), n.GetDateStarted().AsTime().Format(time.TimeOnly), n.GetDateRestarted().AsTime().Format(time.TimeOnly), n.GetInstance())
		}
	})

	t.Run("a node that cannot be observed", func(t *testing.T) {
		e.t = t
		start := time.Now().Add(-time.Second)
		e.compose("stop", "edge-2")
		e.waitFor(30*time.Second, "unobservable", e.observable("edge-2", false))
		e.waitFor(40*time.Second, "node issue", e.issueIs("node.unobservable", "edge-2", "", start, "active", ""))
		e.compose("start", "edge-2")
		e.waitFor(90*time.Second, "observable", e.observable("edge-2", true))
		e.waitFor(40*time.Second, "node issue resolved", e.issueIs("node.unobservable", "edge-2", "", start, "closed", "resolved.condition"))
	})

	t.Run("what Ward recorded", func(t *testing.T) {
		e.t = t
		res, err := e.issues.Search(e.ctx, app.IssueSearchRequest_builder{Size: 500}.Build())
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		for _, i := range res.GetItems() {
			t.Logf("%s opened %s", show(i), i.GetDateOpened().AsTime().Format(time.TimeOnly))
		}
	})

	t.Run("the change feed carried it", func(t *testing.T) {
		e.t = t
		mu.Lock()
		defer mu.Unlock()
		if len(feed) == 0 {
			t.Fatal("nothing on the feed")
		}
		var prev int64
		events := map[string]int{}
		for _, v := range feed {
			if v.GetCursor() <= prev {
				t.Fatalf("cursor %d after %d", v.GetCursor(), prev)
			}
			prev = v.GetCursor()
			events[v.GetEvent()]++
		}
		t.Logf("%d transitions on the feed: %v", len(feed), events)
		for _, ev := range []string{"open", "close", "ack", "clear", "reset"} {
			if events[ev] == 0 {
				t.Errorf("no %s on the feed", ev)
			}
		}
	})
}
