package engine

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/lesomnus/ward/def"
)

// rule is a definition, with what the engine reads of it worked out once.
type rule struct {
	*def.Rule

	// What a change of the rule has to close its Issues over: everything but
	// its severity and how operators handle it (design §6.10).
	sig string

	ackRequired bool
	latch       bool

	// The longest window its metric values are computed over.
	window time.Duration
}

// Definitions are a def.Set, compiled for the engine: indexed by what a signal
// is looked up by.
type Definitions struct {
	Set *def.Set

	Rules map[string]*rule
	Order []*rule

	logBySource      map[string][]*rule
	metricByName     map[string][]*rule
	refByName        map[string][]*rule
	livenessBySource map[string]string
	livenessByMetric map[string][]string

	// The attributes the rules of a source compare, with the type they compare
	// them as: an attribute that arrives as another type is reported.
	typed map[string]map[string]string
}

// Compile prepares a Set for the engine.
func Compile(s *def.Set) *Definitions {
	d := &Definitions{
		Set:              s,
		Rules:            map[string]*rule{},
		logBySource:      map[string][]*rule{},
		metricByName:     map[string][]*rule{},
		refByName:        map[string][]*rule{},
		livenessBySource: map[string]string{},
		livenessByMetric: map[string][]string{},
		typed:            map[string]map[string]string{},
	}
	if s == nil {
		return d
	}
	for _, name := range slices.Sorted(maps.Keys(s.Sources)) {
		src := s.Sources[name]
		if src.Liveness.None {
			continue
		}
		d.livenessBySource[name] = src.Liveness.Metric
		d.livenessByMetric[src.Liveness.Metric] = append(d.livenessByMetric[src.Liveness.Metric], name)
	}
	for _, id := range s.Order {
		r := s.Rules[id]
		c := &rule{Rule: r, sig: signature(r)}
		if r.Operator != nil {
			c.ackRequired = r.Operator.Ack == "required"
			c.latch = r.Operator.Latch != nil && *r.Operator.Latch
		}
		d.Rules[id] = c
		d.Order = append(d.Order, c)

		switch r.Kind {
		case def.KindLog:
			d.logBySource[r.Source] = append(d.logBySource[r.Source], c)
			for _, m := range []*def.Match{r.Match, r.Release} {
				if m == nil {
					continue
				}
				for k, p := range m.Attrs {
					d.expect(r.Source, k, p)
				}
			}
		case def.KindMetric:
			d.metricByName[r.Metric.Name] = append(d.metricByName[r.Metric.Name], c)
			c.window = r.Value.Window
			if rt := r.Limit; rt != nil && rt.RelativeTo != nil && rt.RelativeTo.Metric != nil {
				d.refByName[rt.RelativeTo.Metric.Name] = append(d.refByName[rt.RelativeTo.Metric.Name], c)
				c.window = max(c.window, rt.RelativeTo.Value.Window)
			}
		}
	}
	return d
}

func (d *Definitions) expect(source, attr string, p def.Pred) {
	want := ""
	switch p.Op {
	case "eq":
		want = typeName(p.Value)
	case "in":
		if vs, ok := p.Value.([]any); ok && len(vs) > 0 {
			want = typeName(vs[0])
		}
	case "gt", "gte", "lt", "lte":
		want = "number"
	}
	if want == "" {
		return
	}
	m, ok := d.typed[source]
	if !ok {
		m = map[string]string{}
		d.typed[source] = m
	}
	m[attr] = want
}

// typeName is the name of an attribute value's type as Ward reports it.
func typeName(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "double"
	case []any:
		return "slice"
	case map[string]any:
		return "map"
	case []byte:
		return "bytes"
	case nil:
		return "empty"
	}
	return fmt.Sprintf("%T", v)
}

func signature(r *def.Rule) string {
	raw := maps.Clone(r.Raw)
	delete(raw, "severity")
	delete(raw, "operator")
	delete(raw, "suppress_when")
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Sprintf("%#v", raw)
	}
	return string(b)
}
