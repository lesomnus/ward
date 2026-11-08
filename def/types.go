// Package def is what a provider supplies -- sources, rules and components --
// parsed and checked. A Set is every provider's definitions merged and checked
// together, which is what Ward evaluates.
//
// Every field a definition needs is written out; nothing is filled in. A field
// that only narrows (a match condition, a release) may be left out, and then
// narrows nothing.
package def

import (
	"time"
)

// Severity of an Issue, mildest first.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityError    = "error"
	SeverityCritical = "critical"
)

// Severities in order, mildest first.
var Severities = []string{SeverityInfo, SeverityWarning, SeverityError, SeverityCritical}

// SeverityRank orders severities; 0 is not one.
func SeverityRank(s string) int {
	for i, v := range Severities {
		if v == s {
			return i + 1
		}
	}
	return 0
}

// Kinds of rule.
const (
	KindLog       = "log"
	KindMetric    = "metric"
	KindNode      = "node"
	KindComposite = "composite"
)

// File is one provider's supply, as its file says it.
type File struct {
	Version    int                   `yaml:"version"`
	Sources    map[string]*Source    `yaml:"sources"`
	Rules      []*Rule               `yaml:"-"`
	Components map[string]*Component `yaml:"components"`
}

// Source is a service that sends signals, by its OTel service.name.
type Source struct {
	Name string `yaml:"-"`

	Liveness Liveness `yaml:"liveness"`

	// Set when merged: who defined it.
	Provider string `yaml:"-"`
	Revision string `yaml:"-"`
}

// Liveness is `none`, or the cumulative counter whose data points say the
// source is alive and whose StartTimeUnixNano says when it started.
type Liveness struct {
	None   bool
	Metric string
	MaxGap time.Duration
}

// Rule turns signals into the state of Conditions. Which fields it has
// depends on its kind; Validate says which.
type Rule struct {
	ID       string `yaml:"id"`
	Kind     string `yaml:"kind"`
	Severity string `yaml:"severity"`

	// log and metric: the service the signals come from.
	Source string `yaml:"source"`

	// node, and a metric rule over host metrics: the nodes it is about.
	Nodes []string `yaml:"nodes"`

	// log
	Match           *Match         `yaml:"match"`
	Key             []KeyPart      `yaml:"key"`
	Activate        *Activate      `yaml:"activate"`
	ResolveAfter    *time.Duration `yaml:"resolve_after"`
	ForgetAfter     *time.Duration `yaml:"forget_after"`
	Release         *Match         `yaml:"release"`
	OnSourceRestart string         `yaml:"on_source_restart"`

	// metric
	Metric       *MetricSelect  `yaml:"metric"`
	Value        *Value         `yaml:"value"`
	Limit        *Limit         `yaml:"limit"`
	State        *State         `yaml:"state"`
	Resolve      *Resolve       `yaml:"resolve"`
	StartupGrace *time.Duration `yaml:"startup_grace"`

	// composite
	Scope string `yaml:"scope"`
	When  *Expr  `yaml:"when"`

	// every kind
	Operator     *Operator `yaml:"operator"`
	SuppressWhen *Expr     `yaml:"suppress_when"`

	// Set when parsed and merged.
	Raw      map[string]any `yaml:"-"`
	Provider string         `yaml:"-"`
	Revision string         `yaml:"-"`
}

// Match narrows the records a log rule reads. Every condition given has to
// hold; one left out narrows nothing.
type Match struct {
	// By severity name: trace, debug, info, warn, error, fatal.
	Severity  *Pred           `yaml:"severity"`
	EventName *Pred           `yaml:"event_name"`
	Attrs     map[string]Pred `yaml:"attributes"`
	Resource  map[string]Pred `yaml:"resource"`
}

// Empty reports whether m narrows nothing.
func (m *Match) Empty() bool {
	return m == nil || (m.Severity == nil && m.EventName == nil && len(m.Attrs) == 0 && len(m.Resource) == 0)
}

// Pred is a condition on one value: an operator and its operand.
//
//	eq      the value itself, type included (written as a bare scalar)
//	in      one of a list
//	exists  present (true) or absent (false)
//	gt gte lt lte  ordered: numbers, or severity names in Match.Severity
type Pred struct {
	Op    string
	Value any
}

// KeyPart is one attribute a Key is made of.
type KeyPart struct {
	Attribute string
	Resource  string
}

// Activate is when a Condition becomes active. Which fields apply depends on
// the rule's kind.
type Activate struct {
	// log: N records within D.
	Count  int           `yaml:"count"`
	Within time.Duration `yaml:"within"`

	// metric, composite: how long it has to hold.
	For *time.Duration `yaml:"for"`

	// node
	UnobservableFor time.Duration `yaml:"unobservable_for"`
}

// Resolve is when an active Condition of a metric, node or composite rule
// resolves.
type Resolve struct {
	For           *time.Duration `yaml:"for"`
	ObservableFor time.Duration  `yaml:"observable_for"`
}

// MetricSelect is the series a metric rule reads.
type MetricSelect struct {
	Name  string          `yaml:"name"`
	Match map[string]Pred `yaml:"match"`
}

// Value is how a series is reduced to one number per Key.
type Value struct {
	// last, avg, min, max, rate, change, points or resets.
	Of     string        `yaml:"of"`
	Window time.Duration `yaml:"window"`

	// How the series of one Key combine: sum, avg, min or max.
	Across string `yaml:"across"`
}

// Limit judges a value by levels.
type Limit struct {
	// Most severe first. The first that holds is the current level.
	Levels []Level `yaml:"levels"`

	// How far back past a level's bound the value has to come to leave it.
	Deadband *float64 `yaml:"deadband"`

	// What the value is judged against, when not zero: a constant, or another
	// metric paired by Key. A deviation alarm.
	RelativeTo *RelativeTo `yaml:"relative_to"`
}

// Level is one band of a Limit.
type Level struct {
	Name     string   `yaml:"name"`
	Above    *float64 `yaml:"above"`
	Below    *float64 `yaml:"below"`
	Severity string   `yaml:"severity"`
}

// RelativeTo is a constant or another metric.
type RelativeTo struct {
	Constant *float64
	Metric   *MetricSelect
	Value    *Value
}

// State judges a discrete state.
type State struct {
	// Where the state is read: the value itself, or the attribute of the series
	// whose value is not zero.
	FromValue     bool
	FromAttribute string

	Normal   []string          `yaml:"normal"`
	Abnormal map[string]string `yaml:"abnormal"`
	Other    string            `yaml:"other"`
}

// Operator is how an operator handles a rule's Issues.
type Operator struct {
	// required or none.
	Ack string `yaml:"ack"`

	// Whether a cleared Issue waits for a reset.
	Latch *bool `yaml:"latch"`
}

// Expr is a three-valued expression over Conditions. Exactly one of All, Any,
// Not and Rule is set.
type Expr struct {
	All  []*Expr
	Any  []*Expr
	Not  *Expr
	Rule string

	// With Rule: one Key of it rather than every Condition of it.
	Key    string
	HasKey bool
}

// Rules an expression reads.
func (e *Expr) Rules() []string {
	if e == nil {
		return nil
	}
	var out []string
	switch {
	case e.Rule != "":
		out = append(out, e.Rule)
	case e.Not != nil:
		out = append(out, e.Not.Rules()...)
	default:
		for _, x := range append(append([]*Expr{}, e.All...), e.Any...) {
			out = append(out, x.Rules()...)
		}
	}
	return out
}

// Component is a node of the component tree: a name, its parent, and the
// Conditions it holds.
type Component struct {
	Name    string   `yaml:"-"`
	Parent  string   `yaml:"parent"`
	Members []Member `yaml:"members"`

	Provider string `yaml:"-"`
}

// Member says which Conditions belong to a Component: those of Rule, on Node
// if given, with Key if given.
type Member struct {
	Rule string  `yaml:"rule"`
	Node string  `yaml:"node"`
	Key  *string `yaml:"key"`
}
