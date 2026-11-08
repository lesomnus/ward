package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/ward/def"
)

const s = time.Second

var t0 = time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

// fakeGate is the gate as a scenario says it: every node observable unless
// it is down.
type fakeGate struct {
	down  map[string]string
	since time.Time
}

func (g *fakeGate) Observable(node string, _ Path) (bool, string, time.Time) {
	if r, ok := g.down[node]; ok {
		return false, r, g.since
	}
	return true, "", g.since
}

type arrival struct {
	at time.Time
	x  any
}

// harness runs an Evaluator through a scenario, a tick a second.
type harness struct {
	t     *testing.T
	ev    *Evaluator
	gate  *fakeGate
	queue []arrival
	out   []Transition
	now   time.Time
}

const sources = `
version: 1
sources:
  det:
    liveness: {metric: det.frames, max_gap: 3s}
  job:
    liveness: none
rules: []
`

func newHarness(t *testing.T, rules string) *harness {
	t.Helper()
	var ds []def.Definitions
	for i, src := range []string{sources, rules} {
		f, err := def.Parse([]byte(src))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		ds = append(ds, def.Definitions{Provider: fmt.Sprint("p", i), Revision: "r", File: f})
	}
	set, _, err := def.Merge([]string{"n1", "n2"}, ds...)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	g := &fakeGate{down: map[string]string{}, since: t0.Add(-time.Hour)}
	ev := NewEvaluator(Config{
		LagLog: 2 * s, LagMetric: 3 * s, UnobservableToStale: 60 * s,
		KeysPerRule: 50, NewIssuesPerMinute: 5, MaxShelve: 8 * time.Hour,
		Nodes: []string{"n1", "n2"},
	}, g)
	ev.Apply(Compile(set), t0.Add(-time.Hour))
	h := &harness{t: t, ev: ev, gate: g, now: t0.Add(-time.Hour)}
	// Both nodes booted two hours ago, and say so every second.
	for _, n := range []string{"n1", "n2"} {
		h.uptime(n, t0.Add(-2*time.Hour), t0.Add(-time.Hour), t0.Add(time.Hour))
	}
	return h
}

// uptime has a node report its boot time every second over [from, to), each
// report arriving a second later.
func (h *harness) uptime(node string, boot, from, to time.Time) {
	for t := from; t.Before(to); t = t.Add(s) {
		h.arrive(t.Add(s), bootIn{node: node, boot: boot, t: t})
	}
}

// down takes a node's boot reports after t away: it is off.
func (h *harness) down(node string, t time.Time) {
	rest := h.queue[:0]
	for _, a := range h.queue {
		if b, ok := a.x.(bootIn); ok && b.node == node && b.t.After(t) {
			continue
		}
		rest = append(rest, a)
	}
	h.queue = rest
}

func (h *harness) arrive(at time.Time, xs ...any) {
	for _, x := range xs {
		h.queue = append(h.queue, arrival{at, x})
	}
}

func (h *harness) rule(id string) *rule {
	r := h.ev.defs.Rules[id]
	if r == nil {
		h.t.Fatalf("no rule %s", id)
	}
	return r
}

// signal is a record of rule matched at t, arriving 500ms later.
func (h *harness) signal(rule, node, key string, t time.Time) {
	h.arrive(t.Add(500*time.Millisecond), logIn{rule: h.rule(rule), node: node, key: key, t: t})
}

func (h *harness) release(rule, node, key string, t time.Time) {
	h.arrive(t.Add(500*time.Millisecond), logIn{rule: h.rule(rule), node: node, key: key, t: t, release: true})
}

// live sends a source's liveness counter every second over [from, to), from
// a process started at start; each point arrives a second later.
func (h *harness) live(node, source string, start, from, to time.Time) {
	for t := from; t.Before(to); t = t.Add(s) {
		h.arrive(t.Add(s), liveIn{node: node, source: source, start: start, t: t})
	}
}

func (h *harness) point(rule, node, key string, start, t time.Time, v float64) {
	h.arrive(t.Add(s), pointIn{rule: h.rule(rule), node: node, key: key, start: start, t: t, v: v, series: uint64(start.UnixNano()), group: 1})
}

// run ticks every second up to and including `to`.
func (h *harness) run(to time.Time) {
	for h.now.Before(to) {
		h.now = h.now.Add(s)
		var in []any
		rest := h.queue[:0]
		for _, a := range h.queue {
			if a.at.After(h.now) {
				rest = append(rest, a)
			} else {
				in = append(in, a.x)
			}
		}
		h.queue = rest
		h.ev.Step(h.now, in)
		ts, _ := h.ev.Take()
		h.out = append(h.out, ts...)
	}
}

func (h *harness) cond(rule, node, key string) *cond { return h.ev.conds[condKey{rule, node, key}] }

// events renders the transitions so far, one a line.
func (h *harness) events() string {
	var b strings.Builder
	for _, t := range h.out {
		i := t.Issue
		fmt.Fprintf(&b, "%s %s/%s/%s %s", t.Event, i.Rule, i.Node, i.Key, i.State)
		if i.CloseReason != "" {
			fmt.Fprintf(&b, " %s", i.CloseReason)
		}
		if i.Level != "" {
			fmt.Fprintf(&b, " level=%s", i.Level)
		}
		fmt.Fprintf(&b, " opened=%s", i.DateOpened.Sub(t0))
		if !i.DateClosed.IsZero() {
			fmt.Fprintf(&b, " closed=%s", i.DateClosed.Sub(t0))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (h *harness) expect(want ...string) {
	h.t.Helper()
	got := strings.Split(strings.TrimSpace(h.events()), "\n")
	if len(want) == 0 && h.events() == "" {
		return
	}
	if !slices.Equal(got, want) {
		h.t.Errorf("transitions:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

const logRules = `
version: 1
sources: {}
rules:
  - id: det.fail
    kind: log
    source: det
    severity: error
    match: {attributes: {diag.condition.type: det.fail}}
    release: {attributes: {diag.condition.type: det.fail, diag.condition.active: false}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 2, within: 5s}
    resolve_after: 10s
    on_source_restart: close
    operator: {ack: required, latch: false}
  - id: det.keep
    kind: log
    source: det
    severity: warning
    match: {attributes: {diag.condition.type: det.keep}}
    key: []
    activate: {count: 1}
    resolve_after: 10s
    on_source_restart: keep
    operator: {ack: none, latch: false}
  - id: det.once
    kind: log
    source: det
    severity: error
    match: {attributes: {diag.condition.type: det.once}}
    key: []
    activate: {count: 1}
    forget_after: 20s
    on_source_restart: close
    operator: {ack: none, latch: false}
  - id: job.err
    kind: log
    source: job
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 1}
    resolve_after: 5s
    operator: {ack: none, latch: false}
  - id: det.latch
    kind: log
    source: det
    severity: critical
    match: {attributes: {diag.condition.type: det.latch}}
    key: []
    activate: {count: 1}
    resolve_after: 5s
    on_source_restart: keep
    operator: {ack: required, latch: true}
`

// S1-b: twice within 5s opens; 10s of silence with the source alive resolves,
// closed at the last signal.
func TestRepeatResolvesBySilence(t *testing.T) {
	h := newHarness(t, logRules)
	start := t0.Add(-time.Hour)
	h.live("n1", "det", start, t0.Add(-10*s), t0.Add(40*s))
	for _, d := range []time.Duration{0, 3 * s, 4 * s} {
		h.signal("det.fail", "n1", "front", t0.Add(d))
	}
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.fail/n1/front active opened=3s",
		"close det.fail/n1/front closed resolved.silence opened=3s closed=4s",
	)
	if c := h.cond("det.fail", "n1", "front"); c == nil || c.state != StateInactive {
		t.Errorf("condition after: %+v", c)
	}
}

// One signal and no second within the window: pending, then nothing.
func TestPendingLapses(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(20*s))
	h.signal("det.fail", "n1", "front", t0)
	h.run(t0.Add(2 * s))
	if c := h.cond("det.fail", "n1", "front"); c == nil || c.state != StatePending {
		t.Fatalf("after one signal: %+v", c)
	}
	h.run(t0.Add(20 * s))
	h.expect()
	if c := h.cond("det.fail", "n1", "front"); c.state != StateInactive {
		t.Errorf("after the window: %s", c.state)
	}
}

// The source dies right after the last signal: not resolved, stale (design
// §7.4 row 6).
func TestSourceDiesIsStaleNotResolved(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(5*s))
	h.signal("det.fail", "n1", "front", t0)
	h.signal("det.fail", "n1", "front", t0.Add(s))
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.fail/n1/front active opened=1s",
		"update det.fail/n1/front stale opened=1s",
	)
}

// A restart after the last signal closes a close-on-restart rule's Issue.
func TestSourceRestartCloses(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(3*s))
	h.live("n1", "det", t0.Add(5*s), t0.Add(5*s), t0.Add(40*s))
	h.signal("det.fail", "n1", "front", t0)
	h.signal("det.fail", "n1", "front", t0.Add(s))
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.fail/n1/front active opened=1s",
		"close det.fail/n1/front closed interrupted.source_restart opened=1s closed=1s",
	)
}

// keep: restarts are counted and the Issue goes on.
func TestSourceRestartKeeps(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(3*s))
	h.live("n1", "det", t0.Add(4*s), t0.Add(4*s), t0.Add(8*s))
	h.live("n1", "det", t0.Add(9*s), t0.Add(9*s), t0.Add(60*s))
	for _, d := range []time.Duration{0, 2 * s, 6 * s, 11 * s} {
		h.signal("det.keep", "n1", "", t0.Add(d))
	}
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.keep/n1/ active opened=0s",
		"close det.keep/n1/ closed resolved.silence opened=0s closed=11s",
	)
	if r := h.out[1].Issue.Restarts; r != 2 {
		t.Errorf("restarts %d, want 2", r)
	}
}

// A reboot after the last signal: interrupted.reboot, not resolved.
func TestRebootInterrupts(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(2*s))
	h.signal("det.fail", "n1", "front", t0)
	h.signal("det.fail", "n1", "front", t0.Add(s))
	// Off at 2s, booted at 10s; its first report of it comes at 20s, after the
	// source's first point of its new run.
	h.down("n1", t0.Add(2*s))
	h.uptime("n1", t0.Add(10*s), t0.Add(19*s), t0.Add(time.Hour))
	h.live("n1", "det", t0.Add(15*s), t0.Add(15*s), t0.Add(40*s))
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.fail/n1/front active opened=1s",
		"update det.fail/n1/front stale opened=1s",
		"close det.fail/n1/front closed interrupted.reboot opened=1s closed=1s",
	)
}

// An unobservable node holds the judgement, and turns it stale after
// unobservable_to_stale; nothing resolves while it is.
func TestGateHolds(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(200*s))
	h.signal("det.fail", "n1", "front", t0)
	h.signal("det.fail", "n1", "front", t0.Add(s))
	h.gate.down["n1"] = GateMissing
	h.gate.since = t0
	h.run(t0.Add(100 * s))
	h.expect(
		"open det.fail/n1/front active opened=1s",
		"update det.fail/n1/front stale opened=1s",
	)
	if c := h.cond("det.fail", "n1", "front"); !c.held {
		t.Errorf("not held")
	}
	// Observable again: silence counts from then.
	delete(h.gate.down, "n1")
	h.gate.since = t0.Add(100 * s)
	h.run(t0.Add(111 * s))
	if len(h.out) != 2 {
		t.Fatalf("resolved before 10s of observed silence:\n%s", h.events())
	}
	h.run(t0.Add(120 * s))
	h.expect(
		"open det.fail/n1/front active opened=1s",
		"update det.fail/n1/front stale opened=1s",
		"close det.fail/n1/front closed resolved.silence opened=1s closed=1s",
	)
}

// forget_after: quiet, not resolved.
func TestForgetIsQuiet(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(40*s))
	h.signal("det.once", "n1", "", t0)
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.once/n1/ active opened=0s",
		"close det.once/n1/ closed quiet opened=0s closed=0s",
	)
}

// A source without liveness cannot be seen to be alive: quiet.
func TestNoLivenessIsQuiet(t *testing.T) {
	h := newHarness(t, logRules)
	h.signal("job.err", "n1", "", t0)
	h.run(t0.Add(20 * s))
	h.expect(
		"open job.err/n1/ active opened=0s",
		"close job.err/n1/ closed quiet opened=0s closed=0s",
	)
}

// A release closes at once, and is not counted as an occurrence.
func TestReleaseResolves(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(40*s))
	h.signal("det.fail", "n1", "rear", t0)
	h.signal("det.fail", "n1", "rear", t0.Add(s))
	h.release("det.fail", "n1", "rear", t0.Add(3*s))
	h.release("det.fail", "n1", "rear", t0.Add(4*s))
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.fail/n1/rear active opened=1s",
		"close det.fail/n1/rear closed resolved.explicit opened=1s closed=3s",
	)
	if n := h.out[1].Issue.Count; n != 2 {
		t.Errorf("count %d, want 2", n)
	}
}

// latch: cleared, then only a reset closes; a reset of an active one is
// refused; acknowledging twice is refused.
func TestLatchAndOperators(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(60*s))
	h.signal("det.latch", "n1", "", t0)
	h.run(t0.Add(3 * s))
	id := h.out[0].Issue.ID
	h.out = h.out[:0]
	ctx := context.Background()
	if _, err := h.ev.Reset(ctx, id, "", h.now); err == nil {
		t.Errorf("reset an active issue")
	}
	if _, _, err := h.ev.Acknowledge(ctx, id, "seen", h.now); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, _, err := h.ev.Acknowledge(ctx, id, "again", h.now); err == nil {
		t.Errorf("acknowledged twice")
	}
	h.run(t0.Add(20 * s))
	if _, err := h.ev.Reset(ctx, id, "fixed", h.now); err != nil {
		t.Fatalf("reset: %v", err)
	}
	ts, _ := h.ev.Take()
	h.out = append(h.out, ts...)
	h.expect(
		"ack det.latch/n1/ active opened=0s",
		"clear det.latch/n1/ cleared resolved.silence opened=0s",
		"reset det.latch/n1/ closed resolved.silence opened=0s closed=20s",
	)
	if i := h.out[2].Issue; i.Attention || !i.Acked {
		t.Errorf("after reset: attention %v acked %v", i.Attention, i.Acked)
	}
}

const metricRules = `
version: 1
sources: {}
rules:
  - id: det.down
    kind: metric
    source: det
    severity: error
    metric: {name: det.frames}
    key: []
    value: {of: points, window: 10s, across: sum}
    limit: {levels: [{name: down, below: 1, severity: error}], deadband: 0}
    activate: {for: 0s}
    resolve: {for: 5s}
    startup_grace: 5s
    operator: {ack: required, latch: false}
  - id: det.loop
    kind: metric
    source: det
    severity: error
    metric: {name: det.frames}
    key: []
    value: {of: resets, window: 60s, across: sum}
    limit: {levels: [{name: crash_loop, above: 1, severity: error}], deadband: 0}
    activate: {for: 0s}
    resolve: {for: 30s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
  - id: battery
    kind: metric
    nodes: [n1]
    severity: warning
    metric: {name: battery.charge}
    key: []
    value: {of: last, window: 10s, across: min}
    limit:
      levels:
        - {name: low_low, below: 10, severity: error}
        - {name: low, below: 20, severity: warning}
      deadband: 2
    activate: {for: 3s}
    resolve: {for: 3s}
    startup_grace: 0s
    operator: {ack: required, latch: false}
`

// S4: a source whose counter stops is down; back, it resolves.
func TestProcessDown(t *testing.T) {
	h := newHarness(t, metricRules)
	start := t0.Add(-time.Hour)
	for tt := t0.Add(-30 * s); tt.Before(t0); tt = tt.Add(s) {
		h.point("det.down", "n1", "", start, tt, 1)
		h.arrive(tt.Add(s), liveIn{node: "n1", source: "det", start: start, t: tt})
	}
	h.run(t0.Add(30 * s))
	restart := t0.Add(30 * s)
	for tt := restart; tt.Before(t0.Add(60 * s)); tt = tt.Add(s) {
		h.point("det.down", "n1", "", restart, tt, 1)
		h.arrive(tt.Add(s), liveIn{node: "n1", source: "det", start: restart, t: tt})
	}
	h.run(t0.Add(60 * s))
	h.expect(
		// The last point is at -1s; a 10s window holds none from H = 9s.
		"open det.down/n1/ active level=down opened=9s",
		// Back at 30s; judged after the 5s grace, normal for 5s.
		"close det.down/n1/ closed resolved.condition level=down opened=9s closed=40s",
	)
}

// Gauge levels with a deadband, escalation un-acknowledges.
func TestLevels(t *testing.T) {
	h := newHarness(t, metricRules)
	v := func(from, to time.Duration, x float64) {
		for d := from; d < to; d += s {
			h.point("battery", "n1", "", time.Time{}, t0.Add(d), x)
		}
	}
	v(-20*s, 10*s, 50)
	v(10*s, 20*s, 15) // low
	v(20*s, 30*s, 21) // within the deadband: still low
	v(30*s, 40*s, 5)  // low_low
	v(40*s, 60*s, 30) // normal
	h.run(t0.Add(20 * s))
	id := h.out[0].Issue.ID
	if _, _, err := h.ev.Acknowledge(context.Background(), id, "", h.now); err != nil {
		t.Fatal(err)
	}
	h.run(t0.Add(70 * s))
	h.expect(
		// low from 10s, for 3s.
		"open battery/n1/ active level=low opened=13s",
		"ack battery/n1/ active level=low opened=13s",
		// 21 is within the deadband of low; 5 at 30s is low_low, for 3s.
		"update battery/n1/ active level=low_low opened=13s",
		"close battery/n1/ closed resolved.condition level=low_low opened=13s closed=43s",
	)
	if h.out[2].Issue.Acked {
		t.Errorf("escalation kept the acknowledgement")
	}
}

const compositeRules = `
version: 1
sources: {}
rules:
  - id: a
    kind: log
    source: job
    severity: error
    match: {attributes: {t: a}}
    key: []
    activate: {count: 1}
    resolve_after: 5s
    operator: {ack: none, latch: false}
  - id: b
    kind: log
    source: job
    severity: error
    match: {attributes: {t: b}}
    key: []
    activate: {count: 1}
    resolve_after: 30s
    suppress_when: {rule: a}
    operator: {ack: none, latch: false}
  - id: both
    kind: composite
    scope: node
    severity: critical
    when: {all: [{rule: a}, {rule: b}]}
    activate: {for: 0s}
    resolve: {for: 0s}
    operator: {ack: none, latch: false}
`

func TestCompositeAndSuppression(t *testing.T) {
	h := newHarness(t, compositeRules)
	h.signal("a", "n1", "", t0)
	h.signal("b", "n1", "", t0)
	h.run(t0.Add(20 * s))
	h.expect(
		"open a/n1/ active opened=0s",
		"open b/n1/ active opened=0s",
		"update b/n1/ active opened=0s",
		"open both/n1/ active opened=1s",
		"close a/n1/ closed quiet opened=0s closed=0s",
		"update b/n1/ active opened=0s",
		"close both/n1/ closed resolved.condition opened=1s closed=7s",
	)
	if !h.out[2].Issue.Suppressed || h.out[5].Issue.Suppressed {
		t.Errorf("suppression: %v then %v", h.out[2].Issue.Suppressed, h.out[5].Issue.Suppressed)
	}
}

// Three values: unknown neither holds nor clears.
func TestThreeValued(t *testing.T) {
	h := newHarness(t, compositeRules)
	ev := h.ev
	set := func(rule, state string, held bool) {
		c := ev.conds[condKey{rule, "n1", ""}]
		if c == nil {
			c = ev.newCond(condKey{rule, "n1", ""}, ev.defs.Rules[rule])
		}
		c.state, c.held = state, held
	}
	both := ev.defs.Rules["both"].When
	for _, x := range []struct {
		a, b string
		ha   bool
		want tri
	}{
		{StateActive, StateActive, false, triTrue},
		{StateActive, StateInactive, false, triFalse},
		{StateStale, StateActive, false, triUnknown},
		{StateStale, StateInactive, false, triFalse},
		{StateActive, StateActive, true, triUnknown},
		{StatePending, StateCleared, false, triFalse},
	} {
		set("a", x.a, x.ha)
		set("b", x.b, false)
		if got := ev.eval(both, "n1"); got != x.want {
			t.Errorf("all(a=%s held=%v, b=%s) = %d, want %d", x.a, x.ha, x.b, got, x.want)
		}
	}
	not := &def.Expr{Not: &def.Expr{Rule: "a"}}
	set("a", StateStale, false)
	if got := ev.eval(not, "n1"); got != triUnknown {
		t.Errorf("not(unknown) = %d", got)
	}
}

const nodeRules = `
version: 1
sources: {}
rules:
  - id: node.unobservable
    kind: node
    nodes: [n1, n2]
    severity: error
    activate: {unobservable_for: 10s}
    resolve: {observable_for: 15s}
    operator: {ack: none, latch: false}
`

func TestNodeRule(t *testing.T) {
	h := newHarness(t, nodeRules)
	h.run(t0)
	h.gate.down["n2"] = GateUnreachable
	h.run(t0.Add(20 * s))
	delete(h.gate.down, "n2")
	h.run(t0.Add(40 * s))
	h.expect(
		// Unobservable from the tick at 1s, for 10s; observable from 21s, for 15s.
		"open node.unobservable/n2/ active level=log: unreachable opened=11s",
		"close node.unobservable/n2/ closed resolved.condition level=log: unreachable opened=11s closed=36s",
	)
}

// Keys beyond the limit open one flood Issue instead.
func TestKeyFlood(t *testing.T) {
	h := newHarness(t, logRules)
	h.ev.cfg.KeysPerRule = 3
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(90*s))
	for k := range 6 {
		h.signal("det.fail", "n1", fmt.Sprint("k", k), t0)
		h.signal("det.fail", "n1", fmt.Sprint("k", k), t0.Add(s))
	}
	h.run(t0.Add(5 * s))
	var keys []string
	for _, t := range h.out {
		if t.Event == EventOpen {
			keys = append(keys, t.Issue.Key)
		}
	}
	if want := []string{"*", "k0", "k1", "k2"}; !slices.Equal(slices.Sorted(slices.Values(keys)), want) {
		t.Errorf("opened %v, want %v", keys, want)
	}
}

// Ward restarted: the Collector sends the open Issue's signals again; they
// are not counted twice, and a closed Issue is not opened again.
func TestRestoreAndReplay(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(60*s))
	open := Issue{
		ID: IssueID("det.fail", "n1", "front", t0), Rule: "det.fail", Source: "det", Node: "n1", Key: "front",
		Severity: "error", State: StateActive, DateOpened: t0, DateSeen: t0.Add(2 * s), Count: 3,
	}
	h.ev.Restore([]Issue{open})
	closed := IssueID("det.fail", "n1", "rear", t0.Add(s))
	h.ev.Seen([][16]byte{closed})

	for _, d := range []time.Duration{0, s, 2 * s} {
		h.signal("det.fail", "n1", "front", t0.Add(d))
	}
	h.signal("det.fail", "n1", "rear", t0)
	h.signal("det.fail", "n1", "rear", t0.Add(s))
	h.run(t0.Add(30 * s))
	h.expect("close det.fail/n1/front closed resolved.silence opened=0s closed=2s")
	if n := h.out[0].Issue.Count; n != 3 {
		t.Errorf("count %d after the replay, want 3", n)
	}
}

// One restart is one reset; the run Ward saw first is not a reset at all.
func TestResetsCountRestartsOnly(t *testing.T) {
	h := newHarness(t, metricRules)
	first := t0.Add(-5 * s) // started after Ward did
	for tt := first; tt.Before(t0.Add(20 * s)); tt = tt.Add(s) {
		h.point("det.loop", "n1", "", first, tt, 1)
	}
	again := t0.Add(25 * s)
	for tt := again; tt.Before(t0.Add(40 * s)); tt = tt.Add(s) {
		h.point("det.loop", "n1", "", again, tt, 1)
	}
	h.run(t0.Add(40 * s))
	h.expect()
	third := t0.Add(45 * s)
	for tt := third; tt.Before(t0.Add(60 * s)); tt = tt.Add(s) {
		h.point("det.loop", "n1", "", third, tt, 1)
	}
	h.run(t0.Add(60 * s))
	h.expect("open det.loop/n1/ active level=crash_loop opened=45s")
}

// Shelving marks a Condition's Issues and nothing else: timed until it runs
// out, one_shot until the open Issue closes; out of service until put back.
func TestShelveAndOutOfService(t *testing.T) {
	h := newHarness(t, logRules)
	ctx := context.Background()
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(120*s))
	ref := ConditionRef{Rule: "det.keep", Node: "n1"}

	if err := h.ev.Shelve(ctx, ref, "one_shot", 0, "", h.now); err == nil {
		t.Error("one_shot with no open issue")
	}
	if err := h.ev.Shelve(ctx, ref, "timed", 9*time.Hour, "", h.now); err == nil {
		t.Error("shelved past operator.max_shelve")
	}
	if err := h.ev.Shelve(ctx, ConditionRef{Rule: "nope"}, "timed", time.Minute, "", h.now); err == nil {
		t.Error("shelved a rule that is not there")
	}

	h.run(t0)
	if err := h.ev.Shelve(ctx, ref, "timed", 5*s, "noisy", h.now); err != nil {
		t.Fatal(err)
	}
	h.signal("det.keep", "n1", "", t0.Add(s))
	h.run(t0.Add(3 * s))
	if !h.out[0].Issue.Shelved {
		t.Errorf("opened unshelved during a timed shelve: %+v", h.out[0].Issue)
	}
	h.run(t0.Add(8 * s)) // the shelve runs out: an update marks it
	last := h.out[len(h.out)-1]
	if last.Event != EventUpdate || last.Issue.Shelved {
		t.Errorf("after the shelve ran out: %s shelved=%v", last.Event, last.Issue.Shelved)
	}

	if err := h.ev.Shelve(ctx, ref, "one_shot", 0, "", h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.ev.SetOutOfService(ctx, ref, true, "sensor broken"); err != nil {
		t.Fatal(err)
	}
	if err := h.ev.SetOutOfService(ctx, ref, true, ""); err == nil {
		t.Error("out of service twice")
	}
	h.run(t0.Add(40 * s)) // closes by silence; the one_shot goes with it
	if o := h.ev.overrides[condKey{"det.keep", "n1", ""}]; o == nil || o.shelve != "" || !o.oos {
		t.Errorf("after the issue closed: %+v", o)
	}
	_, ovs := h.ev.Take()
	_ = ovs
	h.signal("det.keep", "n1", "", t0.Add(41*s))
	h.run(t0.Add(45 * s))
	opened := h.out[len(h.out)-1]
	if opened.Event != EventOpen || opened.Issue.Shelved || !opened.Issue.OutOfService {
		t.Errorf("the next issue: %s shelved=%v oos=%v", opened.Event, opened.Issue.Shelved, opened.Issue.OutOfService)
	}
	if err := h.ev.SetOutOfService(ctx, ref, false, "replaced"); err != nil {
		t.Fatal(err)
	}
	ts, _ := h.ev.Take()
	if len(ts) != 1 || ts[0].Issue.OutOfService {
		t.Errorf("back in service: %+v", ts)
	}
}

// A blip is not an outage: a node that is only waiting out the gate's hold
// after one bad canary does not count as unobservable to a node rule.
func TestNodeRuleIgnoresRecovery(t *testing.T) {
	h := newHarness(t, nodeRules)
	h.run(t0)
	h.gate.down["n1"] = GateSlow
	h.run(t0.Add(s))
	h.gate.down["n1"] = GateRecovering
	h.run(t0.Add(20 * s))
	delete(h.gate.down, "n1")
	h.run(t0.Add(30 * s))
	h.expect()
}

// A batch arrives out of order: the activation is the one event time says.
func TestOutOfOrderBatch(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(40*s))
	for _, d := range []time.Duration{4 * s, 3 * s, 0} {
		h.arrive(t0.Add(5*s), logIn{rule: h.rule("det.fail"), node: "n1", key: "front", t: t0.Add(d)})
	}
	h.run(t0.Add(30 * s))
	h.expect(
		"open det.fail/n1/front active opened=3s",
		"close det.fail/n1/front closed resolved.silence opened=3s closed=4s",
	)
}

// A signal that comes after its time was judged does not undo the verdict:
// it opens an Issue of its own, and is counted as late (design §7.5).
func TestLateSignal(t *testing.T) {
	h := newHarness(t, logRules)
	h.live("n1", "det", t0.Add(-time.Hour), t0.Add(-10*s), t0.Add(60*s))
	h.signal("det.keep", "n1", "", t0)
	h.run(t0.Add(20 * s))
	h.arrive(t0.Add(21*s), logIn{rule: h.rule("det.keep"), node: "n1", t: t0.Add(5 * s)})
	h.run(t0.Add(40 * s))
	h.expect(
		"open det.keep/n1/ active opened=0s",
		"close det.keep/n1/ closed resolved.silence opened=0s closed=0s",
		"open det.keep/n1/ active opened=5s",
		"close det.keep/n1/ closed resolved.silence opened=5s closed=5s",
	)
	if h.ev.Late() != 1 {
		t.Errorf("late %d, want 1", h.ev.Late())
	}
}

const rateRules = `
version: 1
sources: {}
rules:
  - id: msgs.slow
    kind: metric
    source: det
    severity: warning
    metric: {name: msgs}
    key: []
    value: {of: rate, window: 4s, across: sum}
    limit: {levels: [{name: slow, below: 8, severity: warning}], deadband: 1}
    activate: {for: 2s}
    resolve: {for: 2s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
  - id: temp.rising
    kind: metric
    nodes: [n1]
    severity: warning
    metric: {name: temp}
    key: []
    value: {of: change, window: 4s, across: max}
    limit: {levels: [{name: rising, above: 0.5, severity: warning}], deadband: 0.1}
    activate: {for: 0s}
    resolve: {for: 2s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
  - id: speed.off
    kind: metric
    nodes: [n1]
    severity: warning
    metric: {name: speed}
    key: []
    value: {of: last, window: 4s, across: avg}
    limit:
      relative_to: {metric: {name: speed.target}, value: {of: last, window: 4s, across: avg}}
      levels: [{name: fast, above: 100, severity: warning}, {name: slow, below: -100, severity: warning}]
      deadband: 10
    activate: {for: 0s}
    resolve: {for: 0s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
`

// rate is a counter's increase per second; change a gauge's; relative_to
// judges a value against another metric paired by Key.
func TestRateChangeRelative(t *testing.T) {
	h := newHarness(t, rateRules)
	start := t0.Add(-time.Hour)
	n := 0.0
	for d := -10 * s; d < 40*s; d += s {
		at := t0.Add(d)
		// 10 a second, then 2 a second from 10s, then 10 again from 25s.
		switch {
		case d >= 10*s && d < 25*s:
			n += 2
		default:
			n += 10
		}
		h.point("msgs.slow", "n1", "", start, at, n)
		// Flat, then rising 1 a second from 10s to 20s.
		temp := 20.0
		if d >= 10*s {
			temp = 20 + float64(min(d, 20*s)-10*s)/float64(s)
		}
		h.point("temp.rising", "n1", "", time.Time{}, at, temp)
		// 150 over its target from 15s to 25s.
		target, speed := 1000.0, 1000.0
		if d >= 15*s && d < 25*s {
			speed = 1150
		}
		h.point("speed.off", "n1", "", time.Time{}, at, speed)
		h.arrive(at.Add(s), pointIn{rule: h.rule("speed.off"), ref: true, node: "n1", t: at, v: target, series: 9, group: 9})
	}
	h.run(t0.Add(45 * s))
	got := map[string][]string{}
	for _, x := range h.out {
		got[x.Issue.Rule] = append(got[x.Issue.Rule], x.Event+" "+x.Issue.Level)
	}
	for rule, want := range map[string][]string{
		"msgs.slow":   {"open slow", "close slow"},
		"temp.rising": {"open rising", "close rising"},
		"speed.off":   {"open fast", "close fast"},
	} {
		if !slices.Equal(got[rule], want) {
			t.Errorf("%s: %v, want %v\n%s", rule, got[rule], want, h.events())
		}
	}
}

// Coming down from a level, the deadband holds at each milder level of the
// same direction on the way, and at none of the other direction.
func TestDeadbandComingDown(t *testing.T) {
	h := newHarness(t, metricRules)
	c := h.ev.newCond(condKey{"battery", "n1", ""}, h.rule("battery"))
	judge := func(cand string, v float64) string {
		c.cand = h.ev.levelOf(c.r, cand)
		if cand == "" {
			c.cand = judgement{}
		}
		return h.ev.judge(c, measurement{v: v, known: true}).level
	}
	for _, x := range []struct {
		from string
		v    float64
		want string
	}{
		{"low_low", 11, "low_low"}, // within low_low's deadband (10 + 2)
		{"low_low", 21, "low"},     // out of low_low, within low's (20 + 2)
		{"low_low", 23, ""},        // out of both
		{"low", 21, "low"},
		{"", 21, ""}, // never low: no deadband to stay in
		{"", 19, "low"},
	} {
		if got := judge(x.from, x.v); got != x.want {
			t.Errorf("from %q at %v: %q, want %q", x.from, x.v, got, x.want)
		}
	}

	r := newHarness(t, rateRules)
	d := r.ev.newCond(condKey{"speed.off", "n1", ""}, r.rule("speed.off"))
	d.cand = r.ev.levelOf(d.r, "fast")
	if got := r.ev.judge(d, measurement{v: -95, known: true}).level; got != "" {
		t.Errorf("from fast at -95: %q; slow is the other direction and was never entered", got)
	}
}

const expectedRules = `
version: 1
sources: {}
rules:
  - id: det.fail
    kind: log
    source: det
    severity: error
    match: {attributes: {diag.condition.type: det.fail}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 2, within: 5s}
    resolve_after: 10s
    on_source_restart: close
    operator: {ack: none, latch: false}
  - id: det.down
    kind: metric
    source: det
    severity: error
    metric: {name: det.frames}
    key: []
    value: {of: points, window: 10s, across: sum}
    limit: {levels: [{name: down, below: 1, severity: error}], deadband: 0}
    activate: {for: 0s}
    resolve: {for: 5s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
components:
  det:
    parent: ""
    members:
      - {rule: det.fail, node: n1, key: front}
      - {rule: det.fail, node: n2, key: rear}
      - {rule: det.down, node: n1}
      - {rule: det.fail}
`

// What a Component's members name is there before any signal of it, is
// normal only on evidence, and is never forgotten (design §4.3, §9).
func TestExpectedConditions(t *testing.T) {
	h := newHarness(t, expectedRules)
	state := func(rule, node, key string) string {
		c := h.cond(rule, node, key)
		if c == nil {
			return "none"
		}
		s := c.state
		if c.held {
			s += " held: " + c.holdReason
		}
		return s
	}
	for _, k := range [][3]string{{"det.fail", "n1", "front"}, {"det.fail", "n2", "rear"}, {"det.down", "n1", ""}} {
		if c := h.cond(k[0], k[1], k[2]); c == nil || c.state != StateUnobserved || !c.declared {
			t.Fatalf("%v before any signal: %s", k, state(k[0], k[1], k[2]))
		}
	}
	if len(h.ev.byRule["det.fail"]) != 2 {
		t.Errorf("a member that names no node and Key expects nothing: %d conditions", len(h.ev.byRule["det.fail"]))
	}
	if got := h.ev.eval(&def.Expr{Rule: "det.fail"}, "n2"); got != triUnknown {
		t.Errorf("an unobserved condition reads %d, want unknown", got)
	}

	start := t0.Add(-time.Hour)
	h.live("n1", "det", start, t0, t0.Add(30*s))
	for tt := t0; tt.Before(t0.Add(30 * s)); tt = tt.Add(s) {
		h.point("det.down", "n1", "", start, tt, 1)
	}
	h.run(t0.Add(15 * s))
	if s := state("det.fail", "n1", "front"); s != StateInactive {
		t.Errorf("front with its source alive: %s", s)
	}
	if s := state("det.fail", "n2", "rear"); s != StateUnobserved {
		t.Errorf("rear, nothing of its source on n2: %s", s)
	}
	if c := h.cond("det.down", "n1", ""); c.state != StateInactive || !c.hasValue || c.value < 5 {
		t.Errorf("det.down: %s value %v (%v)", c.state, c.value, c.hasValue)
	}

	h.run(t0.Add(40 * s)) // the source stopped at 30s
	if s := state("det.fail", "n1", "front"); s != StateInactive+" held: source not alive" {
		t.Errorf("front with its source gone: %s", s)
	}

	h.run(t0.Add(40 * time.Minute))
	if h.cond("det.fail", "n2", "rear") == nil || h.cond("det.fail", "n1", "front") == nil {
		t.Error("an expected condition was forgotten")
	}

	// No longer expected: the unobserved one goes; the other is an ordinary
	// condition again.
	f, err := def.Parse([]byte(strings.Replace(expectedRules, "      - {rule: det.fail, node: n2, key: rear}\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	g, _ := def.Parse([]byte(sources))
	set, _, err := def.Merge([]string{"n1", "n2"}, def.Definitions{Provider: "p0", File: g}, def.Definitions{Provider: "p1", File: f})
	if err != nil {
		t.Fatal(err)
	}
	h.ev.Apply(Compile(set), h.now)
	if h.cond("det.fail", "n2", "rear") != nil {
		t.Error("an unobserved condition stayed after nothing expected it")
	}
}

const stateRules = `
version: 1
sources: {}
rules:
  - id: box.state
    kind: metric
    source: det
    severity: warning
    metric: {name: box.status}
    key: [{attribute: box.id}]
    value: {of: last, window: 5s, across: max}
    state: {from: {attribute: box.state}, normal: [created, running], abnormal: {exited: error}, other: warning}
    activate: {for: 0s}
    resolve: {for: 0s}
    startup_grace: 0s
    operator: {ack: none, latch: false}
`

// A Condition says since when what it reads has been so -- a change between
// normal states too, which opens no Issue (design §10.2).
func TestValueChanged(t *testing.T) {
	h := newHarness(t, stateRules)
	states := []string{"created", "running", "exited"}
	at := func(d time.Duration) string {
		switch {
		case d < 10*s:
			return "created"
		case d < 20*s:
			return "running"
		}
		return "exited"
	}
	for d := time.Duration(0); d < 30*s; d += s {
		for i, st := range states {
			v := 0.0
			if st == at(d) {
				v = 1
			}
			h.arrive(t0.Add(d+s), pointIn{rule: h.rule("box.state"), node: "n1", key: "b1", t: t0.Add(d), v: v, series: uint64(i + 1), group: uint64(i + 1), state: st})
		}
	}
	c := func() *cond { return h.cond("box.state", "n1", "b1") }

	h.run(t0.Add(8 * s))
	if c().reading != "created" || c().readingSince.After(t0.Add(time.Second)) {
		t.Errorf("created: %q since %s", c().reading, c().readingSince.Sub(t0))
	}
	h.run(t0.Add(18 * s))
	if c().reading != "running" || c().readingSince != t0.Add(10*s) || c().issue != nil {
		t.Errorf("running: %q since %s, issue %v", c().reading, c().readingSince.Sub(t0), c().issue != nil)
	}
	if v := h.ev.view(c().k); v.ValueState != "running" || v.DateValueChanged != t0.Add(10*s) || v.Source != "det" {
		t.Errorf("view: %q since %s, source %q", v.ValueState, v.DateValueChanged.Sub(t0), v.Source)
	}
	h.run(t0.Add(28 * s))
	if c().reading != "exited" || c().readingSince != t0.Add(20*s) || c().issue == nil {
		t.Errorf("exited: %q since %s, issue %v", c().reading, c().readingSince.Sub(t0), c().issue != nil)
	}
}
