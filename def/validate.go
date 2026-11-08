package def

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"
)

// Set is what Ward evaluates: every provider's definitions, merged and checked
// together.
type Set struct {
	Sources    map[string]*Source
	Rules      map[string]*Rule
	Components map[string]*Component

	// Order is every rule id, a rule before any rule whose `when` or
	// `suppress_when` reads it, so that one pass of a tick sees what it reads
	// already evaluated.
	Order []string

	// Revisions is the revision in effect per provider.
	Revisions map[string]string
}

// Definitions is one provider's supply.
type Definitions struct {
	Provider string
	Revision string
	File     *File
}

// Merge checks the providers' definitions together and answers with the Set
// they make. nodes are the node names of the static configuration. Warnings
// are things that are allowed but probably not meant.
func Merge(nodes []string, ds ...Definitions) (*Set, []string, error) {
	s := &Set{
		Sources:    map[string]*Source{},
		Rules:      map[string]*Rule{},
		Components: map[string]*Component{},
		Revisions:  map[string]string{},
	}
	var (
		errs  []error
		warns []string
	)
	fail := func(provider, at, why string) {
		errs = append(errs, fmt.Errorf("provider %s: %s: %s", provider, at, why))
	}

	for _, d := range ds {
		s.Revisions[d.Provider] = d.Revision
		if d.File == nil {
			continue
		}
		for _, name := range sortedKeys(d.File.Sources) {
			src := d.File.Sources[name]
			if prev, ok := s.Sources[name]; ok {
				fail(d.Provider, "sources."+name, fmt.Sprintf("also defined by provider %s", prev.Provider))
				continue
			}
			src.Provider, src.Revision = d.Provider, d.Revision
			s.Sources[name] = src
		}
		for i, r := range d.File.Rules {
			at := fmt.Sprintf("rules[%d]", i)
			if r.ID == "" {
				fail(d.Provider, at+".id", "missing")
				continue
			}
			if prev, ok := s.Rules[r.ID]; ok {
				fail(d.Provider, at, fmt.Sprintf("id %q also defined by provider %s", r.ID, prev.Provider))
				continue
			}
			r.Provider, r.Revision = d.Provider, d.Revision
			s.Rules[r.ID] = r
		}
		for _, name := range sortedKeys(d.File.Components) {
			c := d.File.Components[name]
			if prev, ok := s.Components[name]; ok {
				fail(d.Provider, "components."+name, fmt.Sprintf("also defined by provider %s", prev.Provider))
				continue
			}
			c.Provider = d.Provider
			s.Components[name] = c
		}
	}

	for _, name := range sortedKeys(s.Sources) {
		src := s.Sources[name]
		at := "sources." + name + ".liveness"
		if !src.Liveness.None {
			if src.Liveness.Metric == "" {
				fail(src.Provider, at, "`none`, or {metric, max_gap}: metric is missing")
			}
			if src.Liveness.MaxGap <= 0 {
				fail(src.Provider, at+".max_gap", "missing")
			}
		}
	}

	for _, id := range sortedKeys(s.Rules) {
		r := s.Rules[id]
		for _, e := range validateRule(r, s, nodes) {
			fail(r.Provider, "rules."+id, e)
		}
		warns = append(warns, warnRule(r, s)...)
	}

	order, cyc := orderRules(s.Rules)
	if cyc != "" {
		errs = append(errs, fmt.Errorf("rules: %s", cyc))
	}
	s.Order = order

	for _, name := range sortedKeys(s.Components) {
		c := s.Components[name]
		at := "components." + name
		if c.Parent != "" {
			if _, ok := s.Components[c.Parent]; !ok {
				fail(c.Provider, at+".parent", fmt.Sprintf("no component %q", c.Parent))
			}
		}
		for i, m := range c.Members {
			mat := fmt.Sprintf("%s.members[%d]", at, i)
			if m.Rule == "" {
				fail(c.Provider, mat+".rule", "missing")
			} else if _, ok := s.Rules[m.Rule]; !ok {
				fail(c.Provider, mat+".rule", fmt.Sprintf("no rule %q", m.Rule))
			}
			if m.Node != "" && !slices.Contains(nodes, m.Node) {
				fail(c.Provider, mat+".node", fmt.Sprintf("no node %q", m.Node))
			}
		}
	}
	if cyc := componentCycle(s.Components); cyc != "" {
		errs = append(errs, fmt.Errorf("components: %s", cyc))
	}

	if err := errors.Join(errs...); err != nil {
		return nil, warns, err
	}
	return s, warns, nil
}

func validateRule(r *Rule, s *Set, nodes []string) []string {
	var es []string
	bad := func(format string, a ...any) { es = append(es, fmt.Sprintf(format, a...)) }
	absent := func(name string, present bool) {
		if present {
			bad("%s: a %s rule has none", name, r.Kind)
		}
	}

	if SeverityRank(r.Severity) == 0 {
		bad("severity: %q is not one of info, warning, error, critical", r.Severity)
	}
	if r.Operator == nil {
		bad("operator: missing; {ack: required|none, latch: true|false}")
	} else {
		if r.Operator.Ack != "required" && r.Operator.Ack != "none" {
			bad("operator.ack: %q is required or none", r.Operator.Ack)
		}
		if r.Operator.Latch == nil {
			bad("operator.latch: missing")
		}
	}
	checkExpr := func(name string, e *Expr) {
		for _, ref := range e.Rules() {
			if ref == r.ID {
				bad("%s: reads the rule itself", name)
			} else if _, ok := s.Rules[ref]; !ok {
				bad("%s: no rule %q", name, ref)
			}
		}
	}
	if r.SuppressWhen != nil {
		checkExpr("suppress_when", r.SuppressWhen)
	}
	src := func() *Source {
		if r.Source == "" {
			bad("source: missing")
			return nil
		}
		x, ok := s.Sources[r.Source]
		if !ok {
			bad("source: no source %q", r.Source)
		}
		return x
	}
	checkNodes := func() {
		if len(r.Nodes) == 0 {
			bad("nodes: missing")
		}
		for _, n := range r.Nodes {
			if !slices.Contains(nodes, n) {
				bad("nodes: no node %q", n)
			}
		}
	}

	switch r.Kind {
	case KindLog:
		x := src()
		absent("nodes", len(r.Nodes) > 0)
		if r.Match.Empty() {
			bad("match: needs at least one condition")
		} else {
			es = append(es, checkMatch("match", r.Match)...)
		}
		if r.Key == nil {
			bad("key: missing; write `key: []` for one Condition per rule and node")
		}
		if r.Activate == nil {
			bad("activate: missing; {count: N, within: D}")
		} else {
			a := r.Activate
			if a.Count < 1 {
				bad("activate.count: at least 1")
			}
			if a.Count > 1 && a.Within <= 0 {
				bad("activate.within: needed when count is more than 1")
			}
			if a.Count == 1 && a.Within != 0 {
				bad("activate.within: not written when count is 1")
			}
			if a.For != nil || a.UnobservableFor != 0 {
				bad("activate: a log rule takes count and within")
			}
		}
		switch {
		case r.ResolveAfter != nil && r.ForgetAfter != nil:
			bad("resolve_after and forget_after: exactly one")
		case r.ResolveAfter == nil && r.ForgetAfter == nil:
			bad("resolve_after or forget_after: exactly one is needed")
		case r.ResolveAfter != nil:
			if *r.ResolveAfter <= 0 {
				bad("resolve_after: positive")
			}
			if r.Activate != nil && *r.ResolveAfter <= r.Activate.Within {
				bad("resolve_after: longer than activate.within")
			}
		case r.ForgetAfter != nil:
			if *r.ForgetAfter <= 0 {
				bad("forget_after: positive")
			}
		}
		if r.Release != nil {
			if r.Release.Empty() {
				bad("release: needs at least one condition")
			} else {
				es = append(es, checkMatch("release", r.Release)...)
			}
		}
		if x != nil && !x.Liveness.None && r.OnSourceRestart == "" {
			bad("on_source_restart: needed, since source %s has a liveness counter; close or keep", r.Source)
		}
		if r.OnSourceRestart != "" && r.OnSourceRestart != "close" && r.OnSourceRestart != "keep" {
			bad("on_source_restart: %q is close or keep", r.OnSourceRestart)
		}
		absent("metric", r.Metric != nil)
		absent("value", r.Value != nil)
		absent("limit", r.Limit != nil)
		absent("state", r.State != nil)
		absent("resolve", r.Resolve != nil)
		absent("startup_grace", r.StartupGrace != nil)
		absent("scope", r.Scope != "")
		absent("when", r.When != nil)

	case KindMetric:
		switch {
		case r.Source != "" && len(r.Nodes) > 0:
			bad("source and nodes: exactly one")
		case r.Source != "":
			src()
		case len(r.Nodes) > 0:
			checkNodes()
		default:
			bad("source or nodes: exactly one is needed")
		}
		if r.Metric == nil || r.Metric.Name == "" {
			bad("metric.name: missing")
		} else {
			es = append(es, checkAttrPreds("metric.match", r.Metric.Match)...)
		}
		if r.Key == nil {
			bad("key: missing; write `key: []` for one Condition per rule and node")
		}
		es = append(es, checkValue("value", r.Value)...)
		switch {
		case r.Limit != nil && r.State != nil:
			bad("limit and state: exactly one")
		case r.Limit != nil:
			es = append(es, checkLimit(r.Limit)...)
		case r.State != nil:
			es = append(es, checkState(r.State)...)
			if r.Value != nil && r.Value.Of != "last" {
				bad("value.of: a state is read from the last value; write last")
			}
		default:
			bad("limit or state: exactly one is needed")
		}
		if r.Activate == nil || r.Activate.For == nil {
			bad("activate.for: missing")
		} else if *r.Activate.For < 0 {
			bad("activate.for: negative")
		}
		if r.Activate != nil && (r.Activate.Count != 0 || r.Activate.Within != 0 || r.Activate.UnobservableFor != 0) {
			bad("activate: a metric rule takes for")
		}
		if r.Resolve == nil || r.Resolve.For == nil {
			bad("resolve.for: missing")
		} else if *r.Resolve.For < 0 {
			bad("resolve.for: negative")
		}
		if r.StartupGrace == nil {
			bad("startup_grace: missing")
		} else if *r.StartupGrace < 0 {
			bad("startup_grace: negative")
		}
		absent("match", r.Match != nil)
		absent("resolve_after", r.ResolveAfter != nil)
		absent("forget_after", r.ForgetAfter != nil)
		absent("release", r.Release != nil)
		absent("on_source_restart", r.OnSourceRestart != "")
		absent("scope", r.Scope != "")
		absent("when", r.When != nil)

	case KindNode:
		checkNodes()
		absent("source", r.Source != "")
		if r.Activate == nil || r.Activate.UnobservableFor <= 0 {
			bad("activate.unobservable_for: missing")
		}
		if r.Resolve == nil || r.Resolve.ObservableFor <= 0 {
			bad("resolve.observable_for: missing")
		}
		absent("key", r.Key != nil)
		absent("match", r.Match != nil)
		absent("metric", r.Metric != nil)
		absent("value", r.Value != nil)
		absent("when", r.When != nil)

	case KindComposite:
		if r.Scope != "node" && r.Scope != "global" {
			bad("scope: %q is node or global", r.Scope)
		}
		if r.When == nil {
			bad("when: missing")
		} else {
			checkExpr("when", r.When)
		}
		if r.Activate == nil || r.Activate.For == nil {
			bad("activate.for: missing")
		}
		if r.Resolve == nil || r.Resolve.For == nil {
			bad("resolve.for: missing")
		}
		absent("source", r.Source != "")
		absent("nodes", len(r.Nodes) > 0)
		absent("key", r.Key != nil)
		absent("match", r.Match != nil)
		absent("metric", r.Metric != nil)

	case "":
		bad("kind: missing; log, metric, node or composite")
	default:
		bad("kind: %q is log, metric, node or composite", r.Kind)
	}
	return es
}

func checkMatch(at string, m *Match) []string {
	var es []string
	if m.Severity != nil {
		p := *m.Severity
		names := []any{p.Value}
		if p.Op == "in" {
			names = p.Value.([]any)
		}
		if p.Op == "exists" {
			es = append(es, at+".severity: exists does not apply to a severity")
		}
		for _, n := range names {
			if s, ok := n.(string); !ok || LogSeverity(s) == 0 {
				es = append(es, fmt.Sprintf("%s.severity: %v is not one of trace, debug, info, warn, error, fatal", at, n))
			}
		}
	}
	if m.EventName != nil {
		switch m.EventName.Op {
		case "eq", "in", "exists":
		default:
			es = append(es, fmt.Sprintf("%s.event_name: %s does not apply to a name", at, m.EventName.Op))
		}
	}
	es = append(es, checkAttrPreds(at+".attributes", m.Attrs)...)
	es = append(es, checkAttrPreds(at+".resource", m.Resource)...)
	return es
}

func checkAttrPreds(at string, ps map[string]Pred) []string {
	var es []string
	for _, k := range sortedKeys(ps) {
		p := ps[k]
		switch p.Op {
		case "gt", "gte", "lt", "lte":
			switch p.Value.(type) {
			case int64, float64:
			default:
				es = append(es, fmt.Sprintf("%s.%s: %s compares numbers; %v is not one", at, k, p.Op, p.Value))
			}
		}
	}
	return es
}

var valueOf = []string{"last", "avg", "min", "max", "rate", "change", "points", "resets"}

func checkValue(at string, v *Value) []string {
	if v == nil {
		return []string{at + ": missing; {of, window, across}"}
	}
	var es []string
	if !slices.Contains(valueOf, v.Of) {
		es = append(es, fmt.Sprintf("%s.of: %q is one of last, avg, min, max, rate, change, points, resets", at, v.Of))
	}
	if v.Window <= 0 {
		es = append(es, at+".window: missing")
	}
	if !slices.Contains([]string{"sum", "avg", "min", "max"}, v.Across) {
		es = append(es, fmt.Sprintf("%s.across: %q is sum, avg, min or max", at, v.Across))
	}
	return es
}

func checkLimit(l *Limit) []string {
	var es []string
	if len(l.Levels) == 0 {
		es = append(es, "limit.levels: at least one")
	}
	seen := map[string]bool{}
	var aboves, belows []float64
	for i, lv := range l.Levels {
		at := fmt.Sprintf("limit.levels[%d]", i)
		if lv.Name == "" {
			es = append(es, at+".name: missing")
		} else if seen[lv.Name] {
			es = append(es, fmt.Sprintf("%s.name: %q twice", at, lv.Name))
		}
		seen[lv.Name] = true
		switch {
		case lv.Above != nil && lv.Below != nil:
			es = append(es, at+": above or below, not both")
		case lv.Above != nil:
			aboves = append(aboves, *lv.Above)
		case lv.Below != nil:
			belows = append(belows, *lv.Below)
		default:
			es = append(es, at+": above or below is needed")
		}
		if SeverityRank(lv.Severity) == 0 {
			es = append(es, fmt.Sprintf("%s.severity: %q is not one of info, warning, error, critical", at, lv.Severity))
		}
	}
	// Most severe first: an above further up, a below further down.
	for i := 1; i < len(aboves); i++ {
		if aboves[i] >= aboves[i-1] {
			es = append(es, "limit.levels: the above bounds are written most severe (highest) first")
			break
		}
	}
	for i := 1; i < len(belows); i++ {
		if belows[i] <= belows[i-1] {
			es = append(es, "limit.levels: the below bounds are written most severe (lowest) first")
			break
		}
	}
	if l.Deadband == nil {
		es = append(es, "limit.deadband: missing; write 0 for none")
	} else if *l.Deadband < 0 {
		es = append(es, "limit.deadband: negative")
	}
	if r := l.RelativeTo; r != nil && r.Constant == nil {
		if r.Metric == nil || r.Metric.Name == "" {
			es = append(es, "limit.relative_to.metric.name: missing")
		}
		es = append(es, checkValue("limit.relative_to.value", r.Value)...)
	}
	return es
}

func checkState(s *State) []string {
	var es []string
	if !s.FromValue && s.FromAttribute == "" {
		es = append(es, "state.from: missing; `value` or {attribute: NAME}")
	}
	if len(s.Normal) == 0 {
		es = append(es, "state.normal: at least one")
	}
	for _, k := range sortedKeys(s.Abnormal) {
		if SeverityRank(s.Abnormal[k]) == 0 {
			es = append(es, fmt.Sprintf("state.abnormal.%s: %q is not a severity", k, s.Abnormal[k]))
		}
		if slices.Contains(s.Normal, k) {
			es = append(es, fmt.Sprintf("state.abnormal.%s: also normal", k))
		}
	}
	if SeverityRank(s.Other) == 0 {
		es = append(es, fmt.Sprintf("state.other: %q is not a severity", s.Other))
	}
	return es
}

func warnRule(r *Rule, s *Set) []string {
	var ws []string
	if r.Kind == KindLog && r.OnSourceRestart != "" {
		if x, ok := s.Sources[r.Source]; ok && x.Liveness.None {
			ws = append(ws, fmt.Sprintf("rules.%s.on_source_restart: source %s has no liveness counter, so a restart is never seen", r.ID, r.Source))
		}
	}
	if r.Kind == KindLog && r.ResolveAfter != nil {
		if x, ok := s.Sources[r.Source]; ok && x.Liveness.None {
			ws = append(ws, fmt.Sprintf("rules.%s.resolve_after: source %s has no liveness counter, so silence closes as quiet, never resolved", r.ID, r.Source))
		}
	}
	return ws
}

// orderRules answers rule ids with every rule after the rules it reads, or the
// cycle that makes that impossible.
func orderRules(rules map[string]*Rule) ([]string, string) {
	deps := map[string][]string{}
	for id, r := range rules {
		var ds []string
		if r.When != nil {
			ds = append(ds, r.When.Rules()...)
		}
		if r.SuppressWhen != nil {
			ds = append(ds, r.SuppressWhen.Rules()...)
		}
		deps[id] = ds
	}
	var (
		order []string
		state = map[string]int{} // 1 visiting, 2 done
		cycle string
	)
	var visit func(id string, path []string) bool
	visit = func(id string, path []string) bool {
		switch state[id] {
		case 1:
			cycle = fmt.Sprintf("%v reads itself", append(path, id))
			return false
		case 2:
			return true
		}
		state[id] = 1
		for _, d := range deps[id] {
			if _, ok := rules[d]; !ok {
				continue
			}
			if !visit(d, append(path, id)) {
				return false
			}
		}
		state[id] = 2
		order = append(order, id)
		return true
	}
	for _, id := range sortedKeys(rules) {
		if !visit(id, nil) {
			return sortedKeys(rules), cycle
		}
	}
	return order, ""
}

func componentCycle(cs map[string]*Component) string {
	for _, name := range sortedKeys(cs) {
		seen := map[string]bool{}
		for at := name; at != ""; {
			if seen[at] {
				return fmt.Sprintf("%s is its own ancestor", name)
			}
			seen[at] = true
			c, ok := cs[at]
			if !ok {
				break
			}
			at = c.Parent
		}
	}
	return ""
}

// LogSeverity is a severity name's bucket of OTel's SeverityNumber: trace 1,
// debug 2, info 3, warn 4, error 5, fatal 6; 0 for none.
func LogSeverity(name string) int {
	switch name {
	case "trace":
		return 1
	case "debug":
		return 2
	case "info":
		return 3
	case "warn":
		return 4
	case "error":
		return 5
	case "fatal":
		return 6
	}
	return 0
}

// SeverityBucket is the bucket of an OTel SeverityNumber, 1..6; 0 when unset.
func SeverityBucket(n int32) int {
	if n <= 0 {
		return 0
	}
	if n > 24 {
		return 6
	}
	return int((n-1)/4) + 1
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

var _ = time.Second
