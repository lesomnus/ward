package engine

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/ward/def"
)

// Snapshot is the engine's state as the API reads it: made by the loop at the
// end of every tick, and not changed after.
type Snapshot struct {
	Time       time.Time
	Defs       *Definitions
	Conditions []ConditionView
	Nodes      []NodeView
	Sources    []SourceView
	Late       int64
}

// ConditionView is one Condition as ConditionService reports it.
type ConditionView struct {
	Rule, Node, Key        string
	State, Level, Severity string

	Held       bool
	HoldReason string

	Suppressed, Shelved, OutOfService bool
	DateUnshelve                      time.Time

	DateChanged time.Time

	// The open Issue; nil when there is none.
	Issue *Issue
}

// NodeView is one node as NodeService reports it.
type NodeView struct {
	Name       string
	Gate       NodeGate
	DateBooted time.Time
}

// SourceView is one source as SourceService reports it.
type SourceView struct {
	Name, Provider, Revision string
	Liveness                 def.Liveness
	Nodes                    []SourceNodeView
}

// SourceNodeView is what is known of a source on one node.
type SourceNodeView struct {
	Node                                 string
	DateSignaled, DateAlive, DateStarted time.Time
	DateRestarted                        time.Time
	Instance                             string
	Alive                                bool
}

// Snapshot answers with the state as of the last tick.
func (e *Engine) Snapshot() *Snapshot { return e.snap.Load() }

func (e *Engine) publish(now time.Time) {
	ev := e.ev
	s := &Snapshot{Time: now, Defs: ev.defs, Late: ev.late}

	for _, k := range slices.SortedFunc(maps.Keys(ev.conds), compareKeys) {
		s.Conditions = append(s.Conditions, ev.view(k))
	}

	for _, n := range ev.cfg.Nodes {
		v := NodeView{Name: n, Gate: e.Gate.Report(n)}
		b := ev.boots[n]
		v.DateBooted = b.latest()
		// The canary fails, yet the node's own data -- its uptime, scraped
		// after the failure began -- still arrives: what is
		// broken is the way to its Collector, not the node. It stays
		// unobservable -- nothing says its pipeline delivers in full -- but
		// the reason says which (ward#29).
		if !v.Gate.Observable && b != nil && b.seen.After(v.Gate.Since) &&
			strings.Contains(v.Gate.Reason, GateUnreachable) {
			v.Gate.Reason = "canary path: " + v.Gate.Reason
		}
		s.Nodes = append(s.Nodes, v)
	}

	if ev.defs.Set != nil {
		hMetric := now.Add(-e.cfg.Engine.EvalLag.Metric)
		for _, name := range slices.Sorted(maps.Keys(ev.defs.Set.Sources)) {
			src := ev.defs.Set.Sources[name]
			v := SourceView{Name: name, Provider: src.Provider, Revision: src.Revision, Liveness: src.Liveness}
			for _, n := range ev.cfg.Nodes {
				signaled, instance := e.Norm.SourceSeen(n, name)
				rs := ev.live[srcKey{n, name}]
				l := rs.latest()
				if signaled.IsZero() && l == nil {
					continue
				}
				sn := SourceNodeView{Node: n, DateSignaled: signaled, Instance: instance}
				if l != nil {
					sn.DateAlive = l.last
					sn.DateStarted = l.began
					sn.Alive = hMetric.Sub(l.last) <= src.Liveness.MaxGap
					if len(rs.list) > 1 {
						sn.DateRestarted = l.began
					}
					if l.instance != "" {
						sn.Instance = l.instance
					}
				}
				v.Nodes = append(v.Nodes, sn)
			}
			s.Sources = append(s.Sources, v)
		}
	}
	e.snap.Store(s)
}

// view is a Condition as the API reports it. A Condition the engine holds
// nothing of yet -- no signal has come for it -- is inactive, with whatever
// operators set on it.
func (ev *Evaluator) view(k condKey) ConditionView {
	v := ConditionView{Rule: k.rule, Node: k.node, Key: k.key, State: StateInactive}
	if r := ev.defs.Rules[k.rule]; r != nil {
		v.Severity = r.Severity
	}
	if o := ev.overrides[k]; o != nil {
		v.Shelved = o.shelve != ""
		v.DateUnshelve = o.until
		v.OutOfService = o.oos
	}
	c := ev.conds[k]
	if c == nil {
		return v
	}
	v.State = c.state
	v.Held = c.held
	v.HoldReason = c.holdReason
	v.Suppressed = c.suppressed
	v.DateChanged = c.changed
	if c.cur.rank > 0 {
		v.Level, v.Severity = c.cur.level, c.cur.severity
	}
	if i := c.issue; i != nil {
		x := *i
		v.Issue = &x
		v.Level, v.Severity = i.Level, i.Severity
	}
	return v
}
