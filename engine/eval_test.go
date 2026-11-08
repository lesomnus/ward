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
