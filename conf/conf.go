// Package conf is Ward's static configuration: what ward.yaml says beside
// payday's blocks. It is read once, at start, and changing it needs a restart;
// sources and rules come from providers instead and change while Ward runs.
//
// Every value is written out. Validate refuses a value that is missing rather
// than fill one in, because a default is a guess about a deployment that only
// the deployment can make.
package conf

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Ward is everything ward.yaml says beside payday's blocks.
type Ward struct {
	Operator  Operator        `yaml:"operator"`
	Receivers Receivers       `yaml:"receivers"`
	Engine    Engine          `yaml:"engine"`
	Nodes     map[string]Node `yaml:"nodes"`
	Providers []Provider      `yaml:"providers"`
	Store     Store           `yaml:"store"`
	Exporters []Exporter      `yaml:"exporters"`
}

// Operator bounds what an operator may do.
type Operator struct {
	// The longest a timed shelve may last.
	MaxShelve time.Duration `yaml:"max_shelve"`
}

// Receivers are how signals arrive. v1 has one: OTLP.
type Receivers struct {
	OTLP *OTLP `yaml:"otlp"`
}

// OTLP receives what each node's Collector sends, over gRPC, HTTP, or both.
type OTLP struct {
	GRPC *Endpoint `yaml:"grpc"`
	HTTP *Endpoint `yaml:"http"`

	// A request larger than this is refused.
	MaxRequestBytes int `yaml:"max_request_bytes"`

	// Requests waiting to be evaluated. A full queue answers "try again", so
	// the Collector's own queue holds the signal rather than Ward dropping it.
	QueueSize int `yaml:"queue_size"`
}

type Endpoint struct {
	Endpoint string `yaml:"endpoint"`
}

// Engine is how the evaluation loop runs.
type Engine struct {
	// How often the loop evaluates.
	Tick time.Duration `yaml:"tick"`

	// How long a verdict that rests on silence waits for a signal that may
	// still be on its way.
	EvalLag EvalLag `yaml:"eval_lag"`

	// How long Ward may be down and still pick up where it left off: as long
	// as each Collector's diag exporter keeps retrying (its max_elapsed_time).
	RecoveryWindow time.Duration `yaml:"recovery_window"`

	// How often the open Issues' running values and the time Ward was last
	// alive are written.
	Checkpoint time.Duration `yaml:"checkpoint"`

	Gate   Gate   `yaml:"gate"`
	Limits Limits `yaml:"limits"`
}

type EvalLag struct {
	Log    time.Duration `yaml:"log"`
	Metric time.Duration `yaml:"metric"`
}

// Gate decides per node whether what its Collector sends is known to arrive.
type Gate struct {
	// How long the canary has to keep passing before a node that failed is
	// observable again.
	Hold time.Duration `yaml:"hold"`

	// How long a node may be unobservable before its active Conditions are
	// stale rather than held.
	UnobservableToStale time.Duration `yaml:"unobservable_to_stale"`

	Canary Canary `yaml:"canary"`
}

// Canary is the record Ward sends through each node's Collector and expects
// back.
type Canary struct {
	Interval    time.Duration `yaml:"interval"`
	Timeout     time.Duration `yaml:"timeout"`
	MaxRTT      time.Duration `yaml:"max_rtt"`
	CheckWindow time.Duration `yaml:"check_window"`
	MaxMissing  int           `yaml:"max_missing"`
}

// Limits keep one misbehaving source from flooding Ward.
type Limits struct {
	KeysPerRule               int `yaml:"keys_per_rule"`
	NewIssuesPerRulePerMinute int `yaml:"new_issues_per_rule_per_minute"`
}

// Node is a Collector that sends to Ward, and the host it runs on.
type Node struct {
	// Resource attributes a signal from this node carries, usually host.name.
	// Every one has to match.
	Match map[string]string `yaml:"match"`

	// The node Collector's OTLP gRPC endpoint, host:port, where the canary is
	// sent.
	Collector string `yaml:"collector"`
}

// Provider is where sources and rules come from.
type Provider struct {
	Name string `yaml:"name"`

	// file.
	Type string `yaml:"type"`

	// For a file provider: the file, how often its content is checked even
	// without an event, and how long to wait after an event before reading.
	Path         string        `yaml:"path"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Debounce     time.Duration `yaml:"debounce"`
}

// Store is how long Ward keeps its history.
type Store struct {
	// IssueEvents and closed Issues older than this are removed, except an
	// Issue that still needs attention.
	Retention time.Duration `yaml:"retention"`
}

// Exporter sends Ward's own records out as OTel signals.
type Exporter struct {
	// otlp.
	Type string `yaml:"type"`

	// host:port of an OTLP gRPC endpoint.
	Endpoint string `yaml:"endpoint"`

	// Whether the connection is plain text.
	Insecure bool `yaml:"insecure"`
}

// Validate says everything that is missing or wrong, all at once.
func (w Ward) Validate() error { return w.validate(true) }

// ValidateEmbedded is Validate for Ward run inside another program -- a
// Collector, whose own receivers deliver the signals. There Ward has no
// receiver of its own, and a receivers block is refused rather than ignored.
func (w Ward) ValidateEmbedded() error { return w.validate(false) }

func (w Ward) validate(receives bool) error {
	var v validator

	v.positive("operator.max_shelve", w.Operator.MaxShelve)

	if o := w.Receivers.OTLP; !receives {
		if o != nil {
			v.fail("receivers", "not used inside a Collector, whose receivers deliver the signals; leave it out")
		}
	} else if o == nil {
		v.missing("receivers.otlp")
	} else {
		if o.GRPC == nil && o.HTTP == nil {
			v.fail("receivers.otlp", "needs grpc, http or both")
		}
		if o.GRPC != nil {
			v.text("receivers.otlp.grpc.endpoint", o.GRPC.Endpoint)
		}
		if o.HTTP != nil {
			v.text("receivers.otlp.http.endpoint", o.HTTP.Endpoint)
		}
		v.count("receivers.otlp.max_request_bytes", o.MaxRequestBytes)
		v.count("receivers.otlp.queue_size", o.QueueSize)
	}

	e := w.Engine
	v.positive("engine.tick", e.Tick)
	v.positive("engine.eval_lag.log", e.EvalLag.Log)
	v.positive("engine.eval_lag.metric", e.EvalLag.Metric)
	v.positive("engine.recovery_window", e.RecoveryWindow)
	v.positive("engine.checkpoint", e.Checkpoint)
	v.positive("engine.gate.hold", e.Gate.Hold)
	v.positive("engine.gate.unobservable_to_stale", e.Gate.UnobservableToStale)
	c := e.Gate.Canary
	v.positive("engine.gate.canary.interval", c.Interval)
	v.positive("engine.gate.canary.timeout", c.Timeout)
	v.positive("engine.gate.canary.max_rtt", c.MaxRTT)
	v.positive("engine.gate.canary.check_window", c.CheckWindow)
	if c.MaxMissing < 0 {
		v.fail("engine.gate.canary.max_missing", "is negative")
	}
	if c.CheckWindow > 0 && c.Interval > 0 && c.CheckWindow < c.Interval {
		v.fail("engine.gate.canary.check_window", "is shorter than one canary interval")
	}
	v.count("engine.limits.keys_per_rule", e.Limits.KeysPerRule)
	v.count("engine.limits.new_issues_per_rule_per_minute", e.Limits.NewIssuesPerRulePerMinute)

	if len(w.Nodes) == 0 {
		v.missing("nodes")
	}
	for _, name := range slices.Sorted(mapsKeys(w.Nodes)) {
		n := w.Nodes[name]
		if len(n.Match) == 0 {
			v.missing("nodes." + name + ".match")
		}
		v.text("nodes."+name+".collector", n.Collector)
	}

	if len(w.Providers) == 0 {
		v.missing("providers")
	}
	seen := map[string]bool{}
	for i, p := range w.Providers {
		at := fmt.Sprintf("providers[%d]", i)
		v.text(at+".name", p.Name)
		if seen[p.Name] {
			v.fail(at+".name", fmt.Sprintf("%q is named twice", p.Name))
		}
		seen[p.Name] = true
		switch p.Type {
		case "file":
			v.text(at+".path", p.Path)
			v.positive(at+".poll_interval", p.PollInterval)
			v.positive(at+".debounce", p.Debounce)
		case "":
			v.missing(at + ".type")
		default:
			v.fail(at+".type", fmt.Sprintf("%q is not a provider type; file is", p.Type))
		}
	}

	v.positive("store.retention", w.Store.Retention)

	for i, x := range w.Exporters {
		at := fmt.Sprintf("exporters[%d]", i)
		switch x.Type {
		case "otlp":
			v.text(at+".endpoint", x.Endpoint)
		case "":
			v.missing(at + ".type")
		default:
			v.fail(at+".type", fmt.Sprintf("%q is not an exporter type; otlp is", x.Type))
		}
	}

	return v.err()
}

type validator struct{ errs []error }

func (v *validator) fail(at, why string) { v.errs = append(v.errs, fmt.Errorf("%s: %s", at, why)) }
func (v *validator) missing(at string)   { v.fail(at, "missing") }

func (v *validator) text(at, s string) {
	if strings.TrimSpace(s) == "" {
		v.missing(at)
	}
}

func (v *validator) positive(at string, d time.Duration) {
	if d == 0 {
		v.missing(at)
	} else if d < 0 {
		v.fail(at, "is negative")
	}
}

func (v *validator) count(at string, n int) {
	if n == 0 {
		v.missing(at)
	} else if n < 0 {
		v.fail(at, "is negative")
	}
}

func (v *validator) err() error { return errors.Join(v.errs...) }

func mapsKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}
