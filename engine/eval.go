package engine

import (
	"cmp"
	"context"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/ward/def"
)

// FloodKey is the Key of the Condition a rule opens instead of one per Key
// when it has more Keys, or opens Issues faster, than the limits allow.
const FloodKey = "*"

// Config is what the evaluation reads of the static configuration.
type Config struct {
	LagLog, LagMetric   time.Duration
	UnobservableToStale time.Duration
	KeysPerRule         int
	NewIssuesPerMinute  int
	MaxShelve           time.Duration
	Nodes               []string
}

// GateView is what the evaluation reads of the node observation gate.
type GateView interface {
	// Observable answers whether a node's path is observable now, why not,
	// and since when it has been so.
	Observable(node string, p Path) (bool, string, time.Time)
}

// Evaluator is Ward's judgement (design §7): every Condition, its open Issue,
// and the evidence they are judged on. It does no I/O and reads no clock --
// time, signals and the gate are its input, the transitions its output -- so
// a scenario is a test of it.
//
// One goroutine uses it: the loop.
type Evaluator struct {
	cfg  Config
	defs *Definitions
	gate GateView

	conds  map[condKey]*cond
	byRule map[string]map[condKey]*cond
	issues map[[16]byte]*cond

	live  map[srcKey]*runs
	boots map[string]*boots

	overrides map[condKey]*override

	// Issues the Store already has, which the same signals must not open again.
	seen map[[16]byte]bool

	// When each rule's Issues opened on each node, for the flood limit.
	opens map[condKey][]time.Time

	// Issues restored from the Store whose rule is not in effect yet.
	orphans []Issue

	now  time.Time
	out  []Transition
	ovs  []OverrideChange
	late int64
}

type override struct {
	shelve  string // "", OverrideShelveTimed or OverrideShelveOneShot
	until   time.Time
	oos     bool
	comment string
}

// tri is a three-valued truth: a Condition is active, it is not, or nobody can
// say now.
type tri int8

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

// judgement is a metric value judged: a level, or normal.
type judgement struct {
	level    string
	severity string
	rank     int // 0 is normal; more is more severe
}

type mpoint struct {
	t, start      time.Time
	v             float64
	series, group uint64
	instance      string
	state         string
}

type cond struct {
	k condKey
	r *rule

	state   string
	changed time.Time
	issue   *Issue

	// log
	pending    []time.Time // signals in no Issue yet, in order
	recent     []time.Time // the open Issue's last signals, to split it at a restart
	countedRun time.Time   // keep: the last restart counted
	replayedTo time.Time   // restored: signals up to here were counted before

	lastSignal time.Time

	held       bool
	holdReason string
	heldSince  time.Time

	// metric
	pts, refPts []mpoint
	groups      map[uint64]*runs
	first       time.Time // the first data point's time
	cur, cand   judgement
	candSince   time.Time

	// node, composite
	truth      tri
	truthSince time.Time

	// flood
	floodSeen time.Time

	suppressed bool

	// A Component's member names it: Ward keeps it before any signal of it,
	// and does not forget it.
	declared bool

	// What a metric rule judged last, and the time it is of.
	value      float64
	hasValue   bool
	valueState string
	evaluated  time.Time

	// What it read -- the state a state rule reads, the level a limit judges,
	// normal included -- and since when.
	reading      string
	read         bool
	readingSince time.Time
}

// NewEvaluator answers with an evaluator that has no definitions yet.
func NewEvaluator(cfg Config, gate GateView) *Evaluator {
	return &Evaluator{
		cfg:       cfg,
		defs:      Compile(nil),
		gate:      gate,
		conds:     map[condKey]*cond{},
		byRule:    map[string]map[condKey]*cond{},
		issues:    map[[16]byte]*cond{},
		live:      map[srcKey]*runs{},
		boots:     map[string]*boots{},
		overrides: map[condKey]*override{},
		seen:      map[[16]byte]bool{},
		opens:     map[condKey][]time.Time{},
	}
}

// Take answers with the transitions and override changes made since the last
// Take.
func (e *Evaluator) Take() ([]Transition, []OverrideChange) {
	out, ovs := e.out, e.ovs
	e.out, e.ovs = nil, nil
	return out, ovs
}

// Late is how many signals arrived after their time was judged.
func (e *Evaluator) Late() int64 { return e.late }

// Definitions answers with the definitions in effect.
func (e *Evaluator) Definitions() *Definitions { return e.defs }

func (e *Evaluator) newCond(k condKey, r *rule) *cond {
	c := &cond{k: k, r: r, state: StateInactive, changed: e.now}
	e.conds[k] = c
	m, ok := e.byRule[k.rule]
	if !ok {
		m = map[condKey]*cond{}
		e.byRule[k.rule] = m
	}
	m[k] = c
	return c
}

func (e *Evaluator) dropCond(c *cond) {
	delete(e.conds, c.k)
	if m := e.byRule[c.k.rule]; m != nil {
		delete(m, c.k)
		if len(m) == 0 {
			delete(e.byRule, c.k.rule)
		}
	}
}

func (e *Evaluator) condsOf(rule string) []*cond {
	m := e.byRule[rule]
	ks := slices.SortedFunc(maps.Keys(m), func(a, b condKey) int {
		return cmp.Or(cmp.Compare(a.node, b.node), cmp.Compare(a.key, b.key))
	})
	out := make([]*cond, len(ks))
	for i, k := range ks {
		out[i] = m[k]
	}
	return out
}

func (e *Evaluator) bootsOf(node string) *boots {
	b, ok := e.boots[node]
	if !ok {
		b = &boots{}
		e.boots[node] = b
	}
	return b
}

func (e *Evaluator) liveOf(node, source string) *runs {
	k := srcKey{node, source}
	r, ok := e.live[k]
	if !ok {
		r = &runs{}
		e.live[k] = r
	}
	return r
}

// Apply puts definitions in effect. A rule that is gone, or whose signals,
// activation, resolution or Key changed, closes its Issues as rule_changed and
// starts over; one whose severity changed carries it into its open Issues.
func (e *Evaluator) Apply(d *Definitions, now time.Time) {
	e.now = now
	old := e.defs
	e.defs = d

	for _, id := range slices.Sorted(maps.Keys(old.Rules)) {
		or, nr := old.Rules[id], d.Rules[id]
		if nr == nil || nr.sig != or.sig {
			for _, c := range e.condsOf(id) {
				if c.issue != nil {
					e.close(c, ReasonRuleChanged, now)
				}
				e.dropCond(c)
			}
			continue
		}
		for _, c := range e.condsOf(id) {
			c.r = nr
			i := c.issue
			if i == nil {
				continue
			}
			changed := false
			if nr.Kind != def.KindMetric && i.Severity != nr.Severity {
				i.Severity = nr.Severity
				changed = true
			}
			if i.AckRequired != nr.ackRequired {
				i.AckRequired = nr.ackRequired
				changed = true
			}
			if changed {
				e.emit(c, EventUpdate, now, "", nil)
			}
		}
	}

	for _, r := range d.Order {
		switch r.Kind {
		case def.KindNode:
			for _, n := range r.Nodes {
				if k := (condKey{r.ID, n, ""}); e.conds[k] == nil {
					e.newCond(k, r)
				}
			}
		case def.KindComposite:
			nodes := e.cfg.Nodes
			if r.Scope == "global" {
				nodes = []string{""}
			}
			for _, n := range nodes {
				if k := (condKey{r.ID, n, ""}); e.conds[k] == nil {
					c := e.newCond(k, r)
					c.truth = triUnknown
				}
			}
		}
	}

	e.declare()

	orphans := e.orphans
	e.orphans = nil
	e.Restore(orphans)
}

// declare keeps the Conditions the Components expect -- a member that names
// its rule, node and Key (design §9) -- and lets go of those they no longer do.
func (e *Evaluator) declare() {
	want := map[condKey]*rule{}
	if s := e.defs.Set; s != nil {
		for _, name := range slices.Sorted(maps.Keys(s.Components)) {
			for _, m := range s.Components[name].Members {
				r := e.defs.Rules[m.Rule]
				if r == nil || m.Node == "" || (r.Kind != def.KindLog && r.Kind != def.KindMetric) {
					continue
				}
				key := ""
				switch {
				case m.Key != nil:
					key = *m.Key
				case len(r.Key) > 0:
					continue // any Key: it matches, it expects nothing
				}
				want[condKey{r.ID, m.Node, key}] = r
			}
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(e.conds), compareKeys) {
		c := e.conds[k]
		if !c.declared || want[k] != nil {
			continue
		}
		c.declared = false
		if c.state == StateUnobserved {
			e.dropCond(c)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(want), compareKeys) {
		c := e.conds[k]
		if c == nil {
			c = e.newCond(k, want[k])
			c.state = StateUnobserved
		}
		c.declared = true
	}
}

// Restore takes in the Issues the Store had open when Ward stopped. They go on
// being judged; the signals a Collector sends again of them are not counted
// twice. One whose rule is not in effect waits until it is.
func (e *Evaluator) Restore(open []Issue) {
	for _, x := range open {
		r := e.defs.Rules[x.Rule]
		if r == nil {
			e.orphans = append(e.orphans, x)
			continue
		}
		k := condKey{x.Rule, x.Node, x.Key}
		c := e.conds[k]
		if c == nil {
			c = e.newCond(k, r)
		}
		if c.issue != nil {
			continue
		}
		i := x
		i.AckRequired = r.ackRequired
		c.issue = &i
		c.state = i.State
		c.changed = e.now
		e.issues[i.ID] = c

		switch r.Kind {
		case def.KindLog:
			c.recent = []time.Time{i.DateSeen}
			c.replayedTo = i.DateSeen
			c.countedRun = i.DateSeen
			c.lastSignal = i.DateSeen
		case def.KindMetric:
			c.cur = e.levelOf(r, i.Level)
			c.cand = c.cur
			c.candSince = e.now
			c.countedRun = i.DateSeen
			c.lastSignal = i.DateSeen
		default:
			c.truth = triTrue
			c.truthSince = e.now
		}
	}
}

// Seen takes in identifiers of Issues the Store has, so that signals sent
// again do not open them a second time.
func (e *Evaluator) Seen(ids [][16]byte) {
	for _, id := range ids {
		e.seen[id] = true
	}
}

// Overrides takes in the operators' Overrides the Store has.
func (e *Evaluator) Overrides(ovs []OverrideChange) {
	for _, o := range ovs {
		k := condKey{o.Rule, o.Node, o.Key}
		x := e.overrides[k]
		if x == nil {
			x = &override{}
			e.overrides[k] = x
		}
		switch o.Kind {
		case OverrideOutOfService:
			x.oos = true
		default:
			x.shelve = o.Kind
			x.until = o.Until
			x.comment = o.Comment
		}
	}
}

func (e *Evaluator) levelOf(r *rule, level string) judgement {
	if r.Limit != nil {
		for i, l := range r.Limit.Levels {
			if l.Name == level {
				return judgement{level: l.Name, severity: l.Severity, rank: len(r.Limit.Levels) - i}
			}
		}
	}
	if r.State != nil && level != "" {
		sev, ok := r.State.Abnormal[level]
		if !ok {
			sev = r.State.Other
		}
		return judgement{level: level, severity: sev, rank: def.SeverityRank(sev)}
	}
	return judgement{level: level, severity: r.Severity, rank: 1}
}

// Step is one tick: it takes in what arrived, then judges every Condition at
// now.
func (e *Evaluator) Step(now time.Time, in []any) {
	e.now = now
	hLog := now.Add(-e.cfg.LagLog)
	hMetric := now.Add(-e.cfg.LagMetric)

	// Evidence before records: a record is judged against what was known by
	// its time.
	var logs []logIn
	for _, x := range in {
		switch x := x.(type) {
		case bootIn:
			e.bootsOf(x.node).observe(x.boot, x.t)
		case liveIn:
			e.liveOf(x.node, x.source).observe(x.start, x.instance, x.t)
		case pointIn:
			e.point(x)
		case logIn:
			logs = append(logs, x)
		}
	}
	slices.SortStableFunc(logs, func(a, b logIn) int { return a.t.Compare(b.t) })
	for _, x := range logs {
		e.log(x, hLog)
	}

	// Boot boundaries, then restarts, then everything else (design §7.6).
	for _, id := range slices.Sorted(maps.Keys(e.byRule)) {
		for _, c := range e.condsOf(id) {
			e.boundaries(c)
		}
	}

	for _, r := range e.defs.Order {
		for _, c := range e.condsOf(r.ID) {
			if c.k.key == FloodKey {
				e.floodTick(c)
				continue
			}
			switch r.Kind {
			case def.KindLog:
				e.logTick(c, hLog, hMetric)
			case def.KindMetric:
				e.metricTick(c, hMetric)
			case def.KindNode:
				e.nodeTick(c)
			case def.KindComposite:
				e.compositeTick(c)
			}
		}
		if r.SuppressWhen != nil {
			for _, c := range e.condsOf(r.ID) {
				s := e.eval(r.SuppressWhen, c.k.node) == triTrue
				if s == c.suppressed {
					continue
				}
				c.suppressed = s
				if c.issue != nil {
					c.issue.Suppressed = s
					e.emit(c, EventUpdate, now, "", nil)
				}
			}
		}
	}

	// Timed shelves that ran out.
	for _, k := range slices.SortedFunc(maps.Keys(e.overrides), compareKeys) {
		o := e.overrides[k]
		if o.shelve == OverrideShelveTimed && !o.until.After(now) {
			e.unshelve(k, "", nil)
		}
	}

	e.forget()
	e.prune(hMetric)
}

func compareKeys(a, b condKey) int {
	return cmp.Or(cmp.Compare(a.rule, b.rule), cmp.Compare(a.node, b.node), cmp.Compare(a.key, b.key))
}

// current reports whether the rule a signal was matched by is the one in
// effect.
func (e *Evaluator) current(r *rule) *rule {
	x := e.defs.Rules[r.ID]
	if x == nil || x.sig != r.sig {
		return nil
	}
	return x
}

// admit reports whether a new Key may have a Condition of its own, and opens
// the rule's flood Condition when it may not.
func (e *Evaluator) admit(r *rule, node string) bool {
	if e.cfg.KeysPerRule <= 0 {
		return true
	}
	n := 0
	for k := range e.byRule[r.ID] {
		if k.node == node && k.key != FloodKey {
			n++
		}
	}
	if n < e.cfg.KeysPerRule {
		return true
	}
	e.flood(r, node)
	return false
}

func (e *Evaluator) flood(r *rule, node string) {
	k := condKey{r.ID, node, FloodKey}
	c := e.conds[k]
	if c == nil {
		c = e.newCond(k, r)
	}
	c.floodSeen = e.now
	if c.issue == nil {
		e.open(c, e.now, e.now, 1, r.Severity, "key_flood", true)
		return
	}
	c.issue.Count++
	c.issue.DateSeen = e.now
	c.issue.dirty = true
}

func (e *Evaluator) floodTick(c *cond) {
	if c.issue != nil && c.issue.State != StateCleared && e.now.Sub(c.floodSeen) >= time.Minute {
		e.end(c, ReasonCondition, e.now)
	}
}

// open opens an Issue on c. It answers false when it did not: the Store has
// this Issue already, or the rule is opening them faster than the limit.
func (e *Evaluator) open(c *cond, opened, seen time.Time, count int64, severity, level string, flood bool) bool {
	id := IssueID(c.k.rule, c.k.node, c.k.key, opened)
	if e.seen[id] {
		// Sent again after Ward restarted; the Store has it.
		return false
	}
	if !flood && e.cfg.NewIssuesPerMinute > 0 {
		fk := condKey{c.k.rule, c.k.node, ""}
		ts := e.opens[fk]
		i := 0
		for i < len(ts) && e.now.Sub(ts[i]) >= time.Minute {
			i++
		}
		ts = ts[i:]
		if len(ts) >= e.cfg.NewIssuesPerMinute {
			e.opens[fk] = ts
			e.flood(c.r, c.k.node)
			return false
		}
		e.opens[fk] = append(ts, e.now)
	}

	i := &Issue{
		ID: id, Rule: c.k.rule, Source: c.r.Source, Node: c.k.node, Key: c.k.key,
		Severity: severity, Level: level, State: StateActive,
		DateOpened: opened, DateSeen: seen, Count: count,
		AckRequired: c.r.ackRequired, Suppressed: c.suppressed,
	}
	if o := e.overrides[c.k]; o != nil {
		i.Shelved = o.shelve != ""
		i.OutOfService = o.oos
	}
	c.issue = i
	c.state = StateActive
	c.changed = opened
	c.countedRun = opened
	e.issues[id] = c
	e.seen[id] = true
	e.emit(c, EventOpen, opened, "", nil)
	return true
}

// reactivate brings a cleared Issue of a latching rule back to active: the
// condition came back before an operator reset it.
func (e *Evaluator) reactivate(c *cond, at time.Time) {
	i := c.issue
	i.State = StateActive
	i.DateCleared = time.Time{}
	i.CloseReason = ""
	i.DateSeen = later(i.DateSeen, at)
	c.state = StateActive
	c.changed = at
	e.emit(c, EventUpdate, at, "", nil)
}

// end ends the Issue's interval: closes it, or clears it when the rule
// latches -- then only an operator's reset closes it.
func (e *Evaluator) end(c *cond, reason string, at time.Time) {
	i := c.issue
	if i == nil || i.State == StateCleared {
		return
	}
	if c.r.latch {
		i.State = StateCleared
		i.DateCleared = at
		i.CloseReason = reason
		c.state = StateCleared
		c.changed = at
		c.held = false
		e.emit(c, EventClear, at, "", nil)
		return
	}
	e.close(c, reason, at)
}

// close closes the Issue for good.
func (e *Evaluator) close(c *cond, reason string, at time.Time) {
	i := c.issue
	if i == nil {
		return
	}
	i.State = StateClosed
	i.CloseReason = reason
	if i.DateClosed.IsZero() {
		i.DateClosed = at
	}
	e.emit(c, EventClose, at, "", nil)
	delete(e.issues, i.ID)
	c.issue = nil
	c.state = StateInactive
	c.changed = at
	c.recent = nil
	c.held = false
	c.replayedTo = time.Time{}
	if o := e.overrides[c.k]; o != nil && o.shelve == OverrideShelveOneShot {
		e.unshelve(c.k, "", nil)
	}
}

func (e *Evaluator) setState(c *cond, s string, at time.Time) {
	if c.issue == nil || c.issue.State == s {
		return
	}
	c.issue.State = s
	c.state = s
	c.changed = at
	e.emit(c, EventUpdate, at, "", nil)
}

func (e *Evaluator) emit(c *cond, event string, at time.Time, comment string, ctx context.Context) {
	i := c.issue
	i.attend()
	i.dirty = false
	e.out = append(e.out, Transition{Event: event, Date: at, Comment: comment, Issue: *i, Ctx: ctx})
}

func (e *Evaluator) hold(c *cond, reason string, stale bool) {
	if !c.held {
		c.held = true
		c.heldSince = e.now
	}
	c.holdReason = reason
	if stale && c.issue != nil && c.issue.State == StateActive && e.now.Sub(c.heldSince) >= e.cfg.UnobservableToStale {
		e.setState(c, StateStale, e.now)
	}
}

func (e *Evaluator) unhold(c *cond) {
	c.held = false
	c.holdReason = ""
}

// -- log rules --

func (e *Evaluator) log(x logIn, hLog time.Time) {
	r := e.current(x.rule)
	if r == nil {
		return
	}
	k := condKey{r.ID, x.node, x.key}
	c := e.conds[k]
	if c == nil {
		if x.release || !e.admit(r, x.node) {
			return
		}
		c = e.newCond(k, r)
	}
	if x.t.Before(hLog) {
		e.late++
	}
	if x.release {
		e.release(c, x.t)
		return
	}
	e.signal(c, x.t)
}

func (e *Evaluator) signal(c *cond, t time.Time) {
	c.lastSignal = later(c.lastSignal, t)
	if i := c.issue; i != nil && (i.State == StateActive || i.State == StateStale) {
		if !c.replayedTo.IsZero() && !t.After(c.replayedTo) {
			// Counted before Ward restarted; the Collector sent it again.
			return
		}
		if b, ok := e.boots[c.k.node].after(i.DateSeen); ok && !b.After(t) {
			e.split(c, b, ReasonReboot)
		} else if c.r.OnSourceRestart == "close" && e.restartBy(c, t) {
			e.end(c, ReasonSourceRestart, i.DateSeen)
		} else {
			i.Count++
			i.dirty = true
			i.DateSeen = later(i.DateSeen, t)
			c.recent = appendRecent(c.recent, t)
			if i.State == StateStale {
				e.setState(c, StateActive, t)
			}
			return
		}
	}
	c.pending = insertTime(c.pending, t)
	e.activate(c)
}

// restartBy reports whether the source restarted after the open Issue's last
// signal and by t.
func (e *Evaluator) restartBy(c *cond, t time.Time) bool {
	for _, r := range e.live[srcKey{c.k.node, c.r.Source}].restarts(c.issue.DateSeen, e.boots[c.k.node]) {
		if !r.began.After(t) {
			return true
		}
	}
	return false
}

func (e *Evaluator) activate(c *cond) {
	a := c.r.Activate
	n := a.Count
	at := -1
	for i := n - 1; i < len(c.pending); i++ {
		if n == 1 || c.pending[i].Sub(c.pending[i-n+1]) <= a.Within {
			at = i
			break
		}
	}
	if at < 0 {
		if c.issue == nil && len(c.pending) > 0 && c.state != StatePending {
			c.state = StatePending
			c.changed = c.pending[len(c.pending)-1]
		}
		return
	}
	times := slices.Clone(c.pending[at-n+1:])
	c.pending = nil
	// The signal that met the activation opens it; the ones after it in the
	// same batch belong to it as well.
	opened, seen := times[n-1], times[len(times)-1]

	if i := c.issue; i != nil && i.State == StateCleared {
		i.Count += int64(len(times))
		c.recent = times
		e.reactivate(c, seen)
		return
	}
	if e.open(c, opened, seen, int64(len(times)), c.r.Severity, "", false) {
		c.recent = times
		return
	}
	if c.issue == nil {
		c.state = StateInactive
	}
}

func (e *Evaluator) release(c *cond, t time.Time) {
	i := 0
	for i < len(c.pending) && !c.pending[i].After(t) {
		i++
	}
	c.pending = c.pending[i:]
	if is := c.issue; is != nil && (is.State == StateActive || is.State == StateStale) && !t.Before(is.DateSeen) {
		e.end(c, ReasonExplicit, t)
	}
	if c.issue == nil && len(c.pending) == 0 {
		c.state = StateInactive
	}
}

// split ends an open Issue at a boundary its signals straddle -- a restart or a
// boot Ward learned of after it had counted signals from both sides -- and
// judges the signals after it anew.
func (e *Evaluator) split(c *cond, at time.Time, reason string) {
	i := c.issue
	var after []time.Time
	seen := i.DateOpened
	for _, t := range c.recent {
		if t.Before(at) {
			seen = later(seen, t)
		} else {
			after = append(after, t)
		}
	}
	i.DateSeen = seen
	i.Count = max(1, i.Count-int64(len(after)))
	e.end(c, reason, seen)
	for _, t := range after {
		c.pending = insertTime(c.pending, t)
	}
	if len(c.pending) > 0 {
		e.activate(c)
	}
}

// boundaries ends the open Issues of log and metric rules the node's boot or
// the source's restart interrupted, and counts the restarts an Issue that
// keeps going lives through.
func (e *Evaluator) boundaries(c *cond) {
	i := c.issue
	if i == nil || c.k.key == FloodKey || (i.State != StateActive && i.State != StateStale) {
		return
	}
	if c.r.Kind != def.KindLog && c.r.Kind != def.KindMetric {
		return
	}
	b := e.boots[c.k.node]
	if boot, ok := b.after(i.DateOpened); ok && !boot.After(e.now) {
		switch {
		case boot.After(i.DateSeen):
			e.end(c, ReasonReboot, i.DateSeen)
		case c.r.Kind == def.KindLog:
			e.split(c, boot, ReasonReboot)
		default:
			e.end(c, ReasonReboot, boot)
			c.cur, c.cand = judgement{}, judgement{}
		}
		return
	}
	src := e.defs.Set.Sources[c.r.Source]
	if src == nil || src.Liveness.None {
		return
	}
	rs := e.live[srcKey{c.k.node, c.r.Source}].restarts(i.DateOpened, b)
	if len(rs) == 0 {
		return
	}
	if c.r.Kind == def.KindLog && c.r.OnSourceRestart == "close" {
		if r := rs[0]; r.began.After(i.DateSeen) {
			e.end(c, ReasonSourceRestart, i.DateSeen)
		} else {
			e.split(c, r.began, ReasonSourceRestart)
		}
		return
	}
	for _, r := range rs {
		if r.began.After(c.countedRun) {
			i.Restarts++
			i.dirty = true
			c.countedRun = r.began
		}
	}
}

func (e *Evaluator) logTick(c *cond, hLog, hMetric time.Time) {
	if len(c.pending) > 0 {
		from := hLog.Add(-c.r.Activate.Within)
		i := 0
		for i < len(c.pending) && c.pending[i].Before(from) {
			i++
		}
		c.pending = c.pending[i:]
		if len(c.pending) == 0 && c.issue == nil {
			c.state = StateInactive
			c.changed = e.now
		}
	}

	i := c.issue
	if i == nil || i.State == StateCleared {
		e.idle(c, hMetric)
		return
	}
	d := c.r.ForgetAfter
	if c.r.ResolveAfter != nil {
		d = c.r.ResolveAfter
	}
	if hLog.Before(i.DateSeen.Add(*d)) {
		e.unhold(c)
		return
	}

	// The decision table (design §7.4), top to bottom.
	node := c.k.node
	if !e.boots[node].known() {
		e.hold(c, "boot time unknown", false)
		return
	}
	okL, whyL, sinceL := e.gate.Observable(node, PathLog)
	okM, whyM, sinceM := e.gate.Observable(node, PathMetric)
	switch {
	case !okL:
		e.hold(c, "log path: "+whyL, true)
		return
	case !okM:
		e.hold(c, "metric path: "+whyM, true)
		return
	}
	// Silence counts only while the node was observable.
	base := later(i.DateSeen, later(sinceL, sinceM))
	if hLog.Before(base.Add(*d)) {
		e.unhold(c)
		return
	}
	if c.r.ForgetAfter != nil {
		e.end(c, ReasonQuiet, i.DateSeen)
		return
	}
	src := e.defs.Set.Sources[c.r.Source]
	if src == nil || src.Liveness.None {
		e.end(c, ReasonQuiet, i.DateSeen)
		return
	}
	rs := e.live[srcKey{node, c.r.Source}]
	r := rs.at(i.DateSeen)
	if c.r.OnSourceRestart == "keep" {
		r = rs.latest()
	}
	if r != nil && !r.last.Before(base.Add(*d)) {
		e.end(c, ReasonSilence, i.DateSeen)
		return
	}
	var last time.Time
	if l := rs.latest(); l != nil {
		last = l.last
	}
	if hMetric.Sub(last) > src.Liveness.MaxGap {
		e.setState(c, StateStale, e.now)
		c.held = true
		c.holdReason = "liveness counter silent"
		return
	}
	// The evidence has not arrived yet; next tick.
	e.unhold(c)
}

// idle judges a log Condition with nothing open. Quiet is normal only while
// the source is seen alive on the node: quiet from a dead source says nothing
// (design §4.3). One that is expected is unobserved until something of its
// source -- its liveness counter, or a record of it -- has been seen there.
func (e *Evaluator) idle(c *cond, hMetric time.Time) {
	if c.issue != nil || c.state == StatePending {
		e.unhold(c)
		return
	}
	var src *def.Source
	if e.defs.Set != nil {
		src = e.defs.Set.Sources[c.r.Source]
	}
	l := e.live[srcKey{c.k.node, c.r.Source}].latest()
	if c.state == StateUnobserved {
		if l == nil {
			return
		}
		c.state = StateInactive
		c.changed = e.now
	}
	if src == nil || src.Liveness.None {
		e.unhold(c)
		return
	}
	if l == nil || hMetric.Sub(l.last) > src.Liveness.MaxGap {
		e.hold(c, "source not alive", false)
		return
	}
	e.unhold(c)
}

// -- metric rules --

func (e *Evaluator) point(x pointIn) {
	r := e.current(x.rule)
	if r == nil {
		return
	}
	k := condKey{r.ID, x.node, x.key}
	c := e.conds[k]
	if c == nil {
		if !e.admit(r, x.node) {
			return
		}
		c = e.newCond(k, r)
		c.candSince = e.now
	}
	p := mpoint{t: x.t, start: x.start, v: x.v, series: x.series, group: x.group, instance: x.instance, state: x.state}
	if c.first.IsZero() || x.t.Before(c.first) {
		c.first = x.t
	}
	if x.ref {
		c.refPts = append(c.refPts, p)
		return
	}
	c.pts = append(c.pts, p)
	if c.groups == nil {
		c.groups = map[uint64]*runs{}
	}
	g, ok := c.groups[x.group]
	if !ok {
		g = &runs{}
		c.groups[x.group] = g
	}
	g.observe(x.start, x.instance, x.t)
	c.lastSignal = later(c.lastSignal, x.t)
	if i := c.issue; i != nil && (i.State == StateActive || i.State == StateStale) {
		i.Count++
		i.dirty = true
	}
}

func (e *Evaluator) metricTick(c *cond, h time.Time) {
	r := c.r
	node := c.k.node
	if c.issue == nil && c.first.IsZero() {
		// Expected, and no data point of it yet: nothing to judge.
		c.state = StateUnobserved
		return
	}
	if c.state == StateUnobserved {
		c.state = StateInactive
		c.changed = e.now
	}
	b := e.boots[node]
	okM, whyM, sinceM := e.gate.Observable(node, PathMetric)

	if g := *r.StartupGrace; g > 0 {
		from := b.latest()
		if r.Source != "" {
			if l := e.live[srcKey{node, r.Source}].latest(); l != nil {
				from = later(from, l.began)
			}
		}
		if !from.IsZero() && h.Before(from.Add(g)) {
			e.hold(c, "startup grace", false)
			return
		}
	}

	m := e.measure(c, r.Value, c.pts, h, okM, whyM, sinceM)
	if m.known && r.Limit != nil && r.Limit.RelativeTo != nil {
		rt := r.Limit.RelativeTo
		if rt.Constant != nil {
			m.v -= *rt.Constant
		} else {
			ref := e.measure(c, rt.Value, c.refPts, h, okM, whyM, sinceM)
			if !ref.known {
				m = ref
				m.why = "reference: " + ref.why
			} else {
				m.v -= ref.v
			}
		}
	}
	if m.known {
		c.value, c.valueState, c.evaluated = m.v, m.state, h
		c.hasValue = r.State == nil || r.State.FromValue
	}
	i := c.issue
	if !m.known {
		if m.noData && i != nil && i.State == StateActive {
			e.setState(c, StateStale, e.now)
		}
		e.hold(c, m.why, true)
		return
	}
	e.unhold(c)

	j := e.judge(c, m)
	reading := j.level
	if r.State != nil {
		reading = m.state
	}
	if !c.read || reading != c.reading {
		c.reading, c.read, c.readingSince = reading, true, h
	}
	if j.level != c.cand.level || c.candSince.IsZero() {
		c.cand = j
		c.candSince = h
	}
	if i != nil && i.State == StateStale {
		e.setState(c, StateActive, e.now)
	}
	if c.cand.rank > 0 && i != nil && i.State != StateCleared {
		i.DateSeen = later(i.DateSeen, h)
		i.dirty = true
	}

	switch {
	case c.cand.rank > c.cur.rank || (c.cand.rank > 0 && c.cand.rank == c.cur.rank && c.cand.level != c.cur.level):
		if h.Sub(c.candSince) < *r.Activate.For {
			return
		}
		prev := c.cur
		c.cur = c.cand
		switch {
		case i == nil:
			if !e.open(c, h, h, 1, c.cur.severity, c.cur.level, false) {
				c.cur = prev
			}
		case i.State == StateCleared:
			i.Level, i.Severity = c.cur.level, c.cur.severity
			e.reactivate(c, h)
		default:
			i.Level, i.Severity = c.cur.level, c.cur.severity
			if c.cur.rank > prev.rank && i.Acked {
				// More severe than what was acknowledged (design §7.7).
				i.Acked = false
				i.DateAcked = time.Time{}
			}
			e.emit(c, EventUpdate, h, "", nil)
		}

	case c.cand.rank < c.cur.rank:
		if h.Sub(c.candSince) < *r.Resolve.For {
			return
		}
		// Resolving takes what silence does: a known boot and an observable
		// node (design §7.4, rows 1-3).
		if !b.known() {
			e.hold(c, "boot time unknown", false)
			return
		}
		if !okM {
			e.hold(c, "metric path: "+whyM, true)
			return
		}
		c.cur = c.cand
		switch {
		case i == nil || i.State == StateCleared:
		case c.cur.rank == 0:
			e.end(c, ReasonCondition, h)
		default:
			i.Level, i.Severity = c.cur.level, c.cur.severity
			e.emit(c, EventUpdate, h, "", nil)
		}
	}
}

type measurement struct {
	v      float64
	state  string
	known  bool
	noData bool
	why    string
}

// measure reduces a Condition's series to one value at h (design §6.5).
func (e *Evaluator) measure(c *cond, val *def.Value, pts []mpoint, h time.Time, gateOK bool, gateWhy string, gateSince time.Time) measurement {
	from := h.Add(-val.Window)
	in := func(p mpoint) bool { return p.t.After(from) && !p.t.After(h) }

	series := map[uint64][]mpoint{}
	var order []uint64
	for _, p := range pts {
		if !in(p) {
			continue
		}
		if _, ok := series[p.series]; !ok {
			order = append(order, p.series)
		}
		series[p.series] = append(series[p.series], p)
	}
	for _, ps := range series {
		slices.SortFunc(ps, func(a, b mpoint) int { return a.t.Compare(b.t) })
	}

	var vs []float64
	switch val.Of {
	case "points":
		// Absence is judged only over a window Ward could watch whole.
		if !gateOK {
			return measurement{why: "metric path: " + gateWhy}
		}
		if from.Before(gateSince) {
			return measurement{why: "window not observed whole yet"}
		}
		// Nothing is absent before the series was first seen: Ward had just
		// started, or the source had not.
		if h.Before(c.first) {
			return measurement{why: "series not seen yet"}
		}
		groups := map[uint64]float64{}
		for _, id := range order {
			ps := series[id]
			groups[ps[0].group] += float64(len(ps))
		}
		if len(groups) == 0 {
			return measurement{v: 0, known: true}
		}
		for _, g := range slices.Sorted(maps.Keys(groups)) {
			vs = append(vs, groups[g])
		}

	case "resets":
		b := e.boots[c.k.node]
		for _, g := range slices.Sorted(maps.Keys(c.groups)) {
			vs = append(vs, float64(c.groups[g].resets(from, h, b)))
		}
		if len(vs) == 0 {
			return measurement{v: 0, known: true}
		}

	case "rate", "change":
		for _, id := range order {
			ps := series[id]
			if len(ps) < 2 {
				continue
			}
			first, last := ps[0], ps[len(ps)-1]
			span := last.t.Sub(first.t)
			if span < val.Window/2 {
				continue
			}
			if val.Of == "rate" {
				// Per second of what the points span: the window holds
				// points one export interval short of its length, and
				// dividing by the window would read every rate low by that.
				inc := last.v - first.v
				if inc < 0 {
					inc = last.v
				}
				vs = append(vs, inc/span.Seconds())
			} else {
				vs = append(vs, (last.v-first.v)/span.Seconds())
			}
		}
		if len(vs) == 0 {
			return measurement{why: "insufficient data", noData: len(order) == 0}
		}

	default: // last, avg, min, max
		if c.r.State != nil && c.r.State.FromAttribute != "" {
			return e.stateOf(c, series, order)
		}
		for _, id := range order {
			ps := series[id]
			switch val.Of {
			case "last":
				vs = append(vs, ps[len(ps)-1].v)
			case "avg":
				s := 0.0
				for _, p := range ps {
					s += p.v
				}
				vs = append(vs, s/float64(len(ps)))
			case "min":
				vs = append(vs, slices.MinFunc(ps, func(a, b mpoint) int { return cmp.Compare(a.v, b.v) }).v)
			case "max":
				vs = append(vs, slices.MaxFunc(ps, func(a, b mpoint) int { return cmp.Compare(a.v, b.v) }).v)
			}
		}
		if len(vs) == 0 {
			return measurement{why: "no data", noData: true}
		}
	}

	m := measurement{v: across(val.Across, vs), known: true}
	if c.r.State != nil {
		m.state = formatState(m.v)
	}
	return m
}

// stateOf reads a discrete state from the attribute of the series whose last
// value is not zero; of several, the most severe.
func (e *Evaluator) stateOf(c *cond, series map[uint64][]mpoint, order []uint64) measurement {
	if len(order) == 0 {
		return measurement{why: "no data", noData: true}
	}
	var (
		best  judgement
		state string
		found bool
	)
	for _, id := range order {
		ps := series[id]
		p := ps[len(ps)-1]
		if p.v == 0 {
			continue
		}
		j := e.judgeState(c.r, p.state)
		if !found || j.rank > best.rank {
			best, state, found = j, p.state, true
		}
	}
	if !found {
		return measurement{why: "no series is in a state"}
	}
	return measurement{state: state, known: true}
}

func across(how string, vs []float64) float64 {
	switch how {
	case "sum":
		s := 0.0
		for _, v := range vs {
			s += v
		}
		return s
	case "avg":
		s := 0.0
		for _, v := range vs {
			s += v
		}
		return s / float64(len(vs))
	case "min":
		return slices.Min(vs)
	case "max":
		return slices.Max(vs)
	}
	return vs[0]
}

// formatState writes a state code the way a rule names it: 2, not 2.0.
func formatState(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func (e *Evaluator) judge(c *cond, m measurement) judgement {
	r := c.r
	if r.State != nil {
		return e.judgeState(r, m.state)
	}
	ls := r.Limit.Levels
	db := 0.0
	if r.Limit.Deadband != nil {
		db = *r.Limit.Deadband
	}
	v := m.v
	at := slices.IndexFunc(ls, func(l def.Level) bool { return l.Name == c.cand.level })
	for idx, l := range ls {
		holds := (l.Above != nil && v > *l.Above) || (l.Below != nil && v < *l.Below)
		// Leaving a level takes coming back past its bound by the deadband --
		// and so does each milder level of the same direction it comes down
		// through: from low_low, a value just above low's bound is still low.
		if !holds && at >= 0 && idx >= at && (l.Above != nil) == (ls[at].Above != nil) {
			holds = (l.Above != nil && v > *l.Above-db) || (l.Below != nil && v < *l.Below+db)
		}
		if holds {
			return judgement{level: l.Name, severity: l.Severity, rank: len(ls) - idx}
		}
	}
	return judgement{}
}

func (e *Evaluator) judgeState(r *rule, s string) judgement {
	if slices.Contains(r.State.Normal, s) {
		return judgement{}
	}
	sev, ok := r.State.Abnormal[s]
	if !ok {
		sev = r.State.Other
	}
	return judgement{level: s, severity: sev, rank: def.SeverityRank(sev)}
}

// -- node rules --

func (e *Evaluator) nodeTick(c *cond) {
	okL, whyL, _ := e.gate.Observable(c.k.node, PathLog)
	okM, whyM, _ := e.gate.Observable(c.k.node, PathMetric)
	truth, reason := triFalse, ""
	switch {
	case okL && okM:
	case whyL == GateStarting || whyM == GateStarting:
		truth = triUnknown
	case (okL || whyL == GateRecovering) && (okM || whyM == GateRecovering):
		// Fine again, and waiting out gate.hold before silence counts. A node
		// rule is about how long the node was failing, so a blip -- one slow
		// canary -- does not become an outage as long as the hold.
	case !okL:
		truth, reason = triTrue, PathLog.String()+": "+whyL
	default:
		truth, reason = triTrue, PathMetric.String()+": "+whyM
	}
	e.judgeTruth(c, truth, c.r.Activate.UnobservableFor, c.r.Resolve.ObservableFor, reason, false)
}

// -- composite rules --

func (e *Evaluator) compositeTick(c *cond) {
	truth := e.eval(c.r.When, c.k.node)
	e.judgeTruth(c, truth, *c.r.Activate.For, *c.r.Resolve.For, "", true)
}

// judgeTruth is the state machine of a Condition that is a truth over time: a
// node being unobservable, an expression holding.
func (e *Evaluator) judgeTruth(c *cond, truth tri, activate, resolve time.Duration, level string, stale bool) {
	if truth != c.truth || c.truthSince.IsZero() {
		c.truth = truth
		c.truthSince = e.now
	}
	i := c.issue
	switch truth {
	case triUnknown:
		e.hold(c, "unknown", stale)
	case triTrue:
		e.unhold(c)
		if i == nil || i.State == StateCleared {
			if e.now.Sub(c.truthSince) < activate {
				return
			}
			if i == nil {
				e.open(c, e.now, e.now, 1, c.r.Severity, level, false)
			} else {
				i.Level = level
				e.reactivate(c, e.now)
			}
			return
		}
		i.DateSeen = e.now
		i.dirty = true
		switch {
		case i.State == StateStale:
			i.Level = level
			e.setState(c, StateActive, e.now)
		case i.Level != level:
			i.Level = level
			e.emit(c, EventUpdate, e.now, "", nil)
		}
	case triFalse:
		e.unhold(c)
		if i == nil || i.State == StateCleared {
			return
		}
		if e.now.Sub(c.truthSince) >= resolve {
			e.end(c, ReasonCondition, e.now)
		}
	}
}

// eval is an expression over Conditions in three values (design §6.11). A node
// of "" reads every node.
func (e *Evaluator) eval(x *def.Expr, node string) tri {
	switch {
	case x.Rule != "":
		unknown := false
		for _, c := range e.byRule[x.Rule] {
			if node != "" && c.k.node != node {
				continue
			}
			if x.HasKey && c.k.key != x.Key {
				continue
			}
			switch truthOf(c) {
			case triTrue:
				return triTrue
			case triUnknown:
				unknown = true
			}
		}
		if unknown {
			return triUnknown
		}
		return triFalse
	case x.Not != nil:
		switch e.eval(x.Not, node) {
		case triTrue:
			return triFalse
		case triFalse:
			return triTrue
		}
		return triUnknown
	case len(x.All) > 0:
		out := triTrue
		for _, y := range x.All {
			switch e.eval(y, node) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case len(x.Any) > 0:
		out := triFalse
		for _, y := range x.Any {
			switch e.eval(y, node) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	}
	return triUnknown
}

func truthOf(c *cond) tri {
	if c.held {
		return triUnknown
	}
	switch c.state {
	case StateActive:
		return triTrue
	case StateStale, StateUnobserved:
		return triUnknown
	}
	return triFalse
}

// forget drops Conditions of log and metric rules that have nothing open and
// have been quiet for their resolution time or ten minutes, whichever is
// longer (design §6.9).
func (e *Evaluator) forget() {
	for _, k := range slices.SortedFunc(maps.Keys(e.conds), compareKeys) {
		c := e.conds[k]
		if c.issue != nil || len(c.pending) > 0 || c.cur.rank > 0 || c.declared {
			continue
		}
		if c.r.Kind != def.KindLog && c.r.Kind != def.KindMetric {
			continue
		}
		keep := 10 * time.Minute
		for _, d := range []*time.Duration{c.r.ResolveAfter, c.r.ForgetAfter} {
			if d != nil {
				keep = max(keep, *d)
			}
		}
		if c.r.Resolve != nil && c.r.Resolve.For != nil {
			keep = max(keep, *c.r.Resolve.For)
		}
		keep = max(keep, c.r.window)
		last := later(c.lastSignal, c.changed)
		if e.now.Sub(last) > keep {
			e.dropCond(c)
		}
	}
}

// prune forgets data points and runs no window reaches any more.
func (e *Evaluator) prune(h time.Time) {
	for _, c := range e.conds {
		if c.r.Kind != def.KindMetric {
			continue
		}
		from := h.Add(-c.r.window - time.Minute)
		c.pts = dropPoints(c.pts, from)
		c.refPts = dropPoints(c.refPts, from)
		for g, rs := range c.groups {
			rs.prune(from)
			if l := rs.latest(); l != nil && l.last.Before(from) {
				delete(c.groups, g)
			}
		}
	}
	for _, rs := range e.live {
		rs.prune(h.Add(-time.Hour))
	}
}

func dropPoints(ps []mpoint, from time.Time) []mpoint {
	out := ps[:0]
	for _, p := range ps {
		if !p.t.Before(from) {
			out = append(out, p)
		}
	}
	return out
}

func insertTime(ts []time.Time, t time.Time) []time.Time {
	i, _ := slices.BinarySearchFunc(ts, t, func(a, b time.Time) int { return a.Compare(b) })
	return slices.Insert(ts, i, t)
}

// The signals of an open Issue kept for splitting it.
const recentMax = 1024

func appendRecent(ts []time.Time, t time.Time) []time.Time {
	ts = insertTime(ts, t)
	if len(ts) > recentMax {
		ts = ts[len(ts)-recentMax:]
	}
	return ts
}

// -- operators --

// Acknowledge acknowledges an open Issue. It answers false when the Issue is
// not open: the Store has it, if anything does.
func (e *Evaluator) Acknowledge(ctx context.Context, id [16]byte, comment string, now time.Time) (Issue, bool, error) {
	c, ok := e.issues[id]
	if !ok {
		return Issue{}, false, nil
	}
	i := c.issue
	if i.Acked {
		return Issue{}, true, status.Error(codes.FailedPrecondition, "already acknowledged")
	}
	i.Acked = true
	i.DateAcked = now
	e.emit(c, EventAck, now, comment, ctx)
	return *i, true, nil
}

// Comment notes something on an open Issue.
func (e *Evaluator) Comment(ctx context.Context, id [16]byte, comment string, now time.Time) (Issue, bool) {
	c, ok := e.issues[id]
	if !ok {
		return Issue{}, false
	}
	e.emit(c, EventComment, now, comment, ctx)
	return *c.issue, true
}

// Reset closes a cleared Issue of a latching rule.
func (e *Evaluator) Reset(ctx context.Context, id [16]byte, comment string, now time.Time) (Issue, error) {
	c, ok := e.issues[id]
	if !ok {
		return Issue{}, status.Error(codes.FailedPrecondition, "not waiting for a reset: only a cleared Issue is")
	}
	i := c.issue
	if i.State != StateCleared {
		return Issue{}, status.Errorf(codes.FailedPrecondition, "%s, not cleared: the condition has not ended", i.State)
	}
	i.State = StateClosed
	i.DateClosed = now
	i.DateReset = now
	e.emit(c, EventReset, now, comment, ctx)
	out := *i
	delete(e.issues, id)
	c.issue = nil
	c.state = StateInactive
	c.changed = now
	if o := e.overrides[c.k]; o != nil && o.shelve == OverrideShelveOneShot {
		e.unshelve(c.k, "", nil)
	}
	return out, nil
}

// ConditionRef names a Condition for an operator.
type ConditionRef struct{ Rule, Node, Key string }

func (e *Evaluator) checkRef(ref ConditionRef) (condKey, *rule, error) {
	r := e.defs.Rules[ref.Rule]
	if r == nil {
		return condKey{}, nil, status.Errorf(codes.NotFound, "no rule %q", ref.Rule)
	}
	if ref.Node != "" && !slices.Contains(e.cfg.Nodes, ref.Node) {
		return condKey{}, nil, status.Errorf(codes.NotFound, "no node %q", ref.Node)
	}
	return condKey{ref.Rule, ref.Node, ref.Key}, r, nil
}

// Shelve shelves a Condition: timed until now+d, or one_shot until its open
// Issue closes. Its Issues are marked; nothing else changes.
func (e *Evaluator) Shelve(ctx context.Context, ref ConditionRef, mode string, d time.Duration, comment string, now time.Time) error {
	k, _, err := e.checkRef(ref)
	if err != nil {
		return err
	}
	o := &override{}
	if x := e.overrides[k]; x != nil {
		o = x
	}
	ch := OverrideChange{Rule: k.rule, Node: k.node, Key: k.key, Comment: comment, Ctx: ctx}
	switch mode {
	case "timed":
		if d <= 0 {
			return status.Error(codes.InvalidArgument, "duration: a timed shelve needs one")
		}
		if d > e.cfg.MaxShelve {
			return status.Errorf(codes.InvalidArgument, "duration: at most %s (operator.max_shelve)", e.cfg.MaxShelve)
		}
		o.shelve, o.until = OverrideShelveTimed, now.Add(d)
		ch.Kind, ch.Until = OverrideShelveTimed, o.until
	case "one_shot":
		c := e.conds[k]
		if c == nil || c.issue == nil {
			return status.Error(codes.FailedPrecondition, "one_shot shelves the open Issue, and the Condition has none")
		}
		o.shelve, o.until = OverrideShelveOneShot, time.Time{}
		ch.Kind = OverrideShelveOneShot
	default:
		return status.Errorf(codes.InvalidArgument, "mode: %q is timed or one_shot", mode)
	}
	o.comment = comment
	e.overrides[k] = o
	e.ovs = append(e.ovs, ch)
	e.mark(k, func(i *Issue) bool {
		changed := !i.Shelved
		i.Shelved = true
		return changed
	}, comment, ctx)
	return nil
}

// Unshelve takes a Condition's shelve away.
func (e *Evaluator) Unshelve(ctx context.Context, ref ConditionRef, comment string) error {
	k, _, err := e.checkRef(ref)
	if err != nil {
		return err
	}
	if o := e.overrides[k]; o == nil || o.shelve == "" {
		return status.Error(codes.FailedPrecondition, "not shelved")
	}
	e.unshelve(k, comment, ctx)
	return nil
}

func (e *Evaluator) unshelve(k condKey, comment string, ctx context.Context) {
	o := e.overrides[k]
	if o == nil || o.shelve == "" {
		return
	}
	e.ovs = append(e.ovs, OverrideChange{Delete: true, Rule: k.rule, Node: k.node, Key: k.key, Kind: o.shelve, Comment: comment, Ctx: ctx})
	o.shelve, o.until = "", time.Time{}
	if !o.oos {
		delete(e.overrides, k)
	}
	e.mark(k, func(i *Issue) bool {
		changed := i.Shelved
		i.Shelved = false
		return changed
	}, comment, ctx)
}

// SetOutOfService takes a Condition out of service, or puts it back.
func (e *Evaluator) SetOutOfService(ctx context.Context, ref ConditionRef, oos bool, comment string) error {
	k, _, err := e.checkRef(ref)
	if err != nil {
		return err
	}
	o := e.overrides[k]
	if (o != nil && o.oos) == oos {
		if oos {
			return status.Error(codes.FailedPrecondition, "already out of service")
		}
		return status.Error(codes.FailedPrecondition, "not out of service")
	}
	if o == nil {
		o = &override{}
		e.overrides[k] = o
	}
	o.oos = oos
	e.ovs = append(e.ovs, OverrideChange{Delete: !oos, Rule: k.rule, Node: k.node, Key: k.key, Kind: OverrideOutOfService, Comment: comment, Ctx: ctx})
	if !oos && o.shelve == "" {
		delete(e.overrides, k)
	}
	e.mark(k, func(i *Issue) bool {
		changed := i.OutOfService != oos
		i.OutOfService = oos
		return changed
	}, comment, ctx)
	return nil
}

// mark changes the open Issue of a Condition, if it has one.
func (e *Evaluator) mark(k condKey, f func(*Issue) bool, comment string, ctx context.Context) {
	c := e.conds[k]
	if c == nil || c.issue == nil {
		return
	}
	if f(c.issue) {
		e.emit(c, EventUpdate, e.now, comment, ctx)
	}
}
