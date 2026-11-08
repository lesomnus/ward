package def

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type fileYAML struct {
	Version    int                   `yaml:"version"`
	Sources    map[string]*Source    `yaml:"sources"`
	Rules      []yaml.Node           `yaml:"rules"`
	Components map[string]*Component `yaml:"components"`
}

// Parse reads a provider file. A field it does not know is refused, so a typo
// is an error rather than a rule that silently means something else.
func Parse(b []byte) (*File, error) {
	var raw fileYAML
	if err := strict(b, &raw); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("empty: a provider file says at least `version: 1`, `sources` and `rules`")
		}
		return nil, err
	}

	var errs []error
	if raw.Version != 1 {
		errs = append(errs, fmt.Errorf("version: %d is not one this Ward reads; 1 is", raw.Version))
	}
	if raw.Sources == nil {
		errs = append(errs, errors.New("sources: missing; write `sources: {}` for none"))
	}
	if raw.Rules == nil {
		errs = append(errs, errors.New("rules: missing; write `rules: []` for none"))
	}

	f := &File{Version: raw.Version, Sources: raw.Sources, Components: raw.Components}
	for name, s := range f.Sources {
		if s == nil {
			errs = append(errs, fmt.Errorf("sources.%s: empty", name))
			continue
		}
		s.Name = name
	}
	for name, c := range f.Components {
		if c == nil {
			f.Components[name] = &Component{Name: name}
			continue
		}
		c.Name = name
	}

	for i := range raw.Rules {
		n := &raw.Rules[i]
		b, err := yaml.Marshal(n)
		if err != nil {
			errs = append(errs, fmt.Errorf("rules[%d]: %w", i, err))
			continue
		}
		var r Rule
		if err := strict(b, &r); err != nil {
			errs = append(errs, fmt.Errorf("rules[%d]: %w", i, err))
			continue
		}
		if err := n.Decode(&r.Raw); err != nil {
			errs = append(errs, fmt.Errorf("rules[%d]: %w", i, err))
			continue
		}
		f.Rules = append(f.Rules, &r)
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return f, nil
}

func strict(b []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	return dec.Decode(v)
}

// keys refuses a mapping that holds a key not in allowed.
func keys(n *yaml.Node, what string, allowed ...string) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: %s is a mapping", n.Line, what)
	}
	for i := 0; i < len(n.Content); i += 2 {
		k := n.Content[i].Value
		if !slices.Contains(allowed, k) {
			return fmt.Errorf("line %d: %s has no field %q; it has %s", n.Content[i].Line, what, k, strings.Join(allowed, ", "))
		}
	}
	return nil
}

func field(n *yaml.Node, k string) *yaml.Node {
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == k {
			return n.Content[i+1]
		}
	}
	return nil
}

// scalar decodes a scalar to the type it was written as: string, int64,
// float64 or bool.
func scalar(n *yaml.Node) (any, error) {
	if n.Kind != yaml.ScalarNode {
		return nil, fmt.Errorf("line %d: a single value", n.Line)
	}
	var v any
	if err := n.Decode(&v); err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64, float64, bool, string:
		return x, nil
	case nil:
		return nil, fmt.Errorf("line %d: a value, not null", n.Line)
	default:
		return nil, fmt.Errorf("line %d: %T is not a value an attribute holds", n.Line, v)
	}
}

func (l *Liveness) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		if n.Value != "none" {
			return fmt.Errorf("line %d: liveness is `none` or {metric, max_gap}", n.Line)
		}
		*l = Liveness{None: true}
		return nil
	}
	if err := keys(n, "liveness", "metric", "max_gap"); err != nil {
		return err
	}
	var v struct {
		Metric string        `yaml:"metric"`
		MaxGap time.Duration `yaml:"max_gap"`
	}
	if err := n.Decode(&v); err != nil {
		return err
	}
	*l = Liveness{Metric: v.Metric, MaxGap: v.MaxGap}
	return nil
}

func (p *Pred) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		v, err := scalar(n)
		if err != nil {
			return err
		}
		*p = Pred{Op: "eq", Value: v}
		return nil
	}
	if err := keys(n, "condition", "in", "exists", "gt", "gte", "lt", "lte"); err != nil {
		return err
	}
	if len(n.Content) != 2 {
		return fmt.Errorf("line %d: a condition is one of in, exists, gt, gte, lt, lte", n.Line)
	}
	op, v := n.Content[0].Value, n.Content[1]
	switch op {
	case "in":
		if v.Kind != yaml.SequenceNode || len(v.Content) == 0 {
			return fmt.Errorf("line %d: in is a list of values", v.Line)
		}
		var vs []any
		for _, x := range v.Content {
			s, err := scalar(x)
			if err != nil {
				return err
			}
			vs = append(vs, s)
		}
		*p = Pred{Op: op, Value: vs}
	case "exists":
		var b bool
		if err := v.Decode(&b); err != nil {
			return fmt.Errorf("line %d: exists is true or false", v.Line)
		}
		*p = Pred{Op: op, Value: b}
	default:
		s, err := scalar(v)
		if err != nil {
			return err
		}
		*p = Pred{Op: op, Value: s}
	}
	return nil
}

func (k *KeyPart) UnmarshalYAML(n *yaml.Node) error {
	if err := keys(n, "key part", "attribute", "resource"); err != nil {
		return err
	}
	if len(n.Content) != 2 || n.Content[1].Value == "" {
		return fmt.Errorf("line %d: a key part is {attribute: NAME} or {resource: NAME}", n.Line)
	}
	if n.Content[0].Value == "attribute" {
		*k = KeyPart{Attribute: n.Content[1].Value}
	} else {
		*k = KeyPart{Resource: n.Content[1].Value}
	}
	return nil
}

func (r *RelativeTo) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		var f float64
		if err := n.Decode(&f); err != nil {
			return fmt.Errorf("line %d: relative_to is a number or {metric, value}", n.Line)
		}
		*r = RelativeTo{Constant: &f}
		return nil
	}
	if err := keys(n, "relative_to", "metric", "value"); err != nil {
		return err
	}
	var v struct {
		Metric *MetricSelect `yaml:"metric"`
		Value  *Value        `yaml:"value"`
	}
	if err := n.Decode(&v); err != nil {
		return err
	}
	*r = RelativeTo{Metric: v.Metric, Value: v.Value}
	return nil
}

func (s *State) UnmarshalYAML(n *yaml.Node) error {
	if err := keys(n, "state", "from", "normal", "abnormal", "other"); err != nil {
		return err
	}
	var v struct {
		Normal   []string          `yaml:"normal"`
		Abnormal map[string]string `yaml:"abnormal"`
		Other    string            `yaml:"other"`
	}
	if err := n.Decode(&v); err != nil {
		return err
	}
	*s = State{Normal: v.Normal, Abnormal: v.Abnormal, Other: v.Other}
	from := field(n, "from")
	switch {
	case from == nil:
		// Validate says it is missing.
	case from.Kind == yaml.ScalarNode && from.Value == "value":
		s.FromValue = true
	case from.Kind == yaml.MappingNode:
		if err := keys(from, "state.from", "attribute"); err != nil {
			return err
		}
		a := field(from, "attribute")
		if a == nil || a.Value == "" {
			return fmt.Errorf("line %d: state.from is `value` or {attribute: NAME}", from.Line)
		}
		s.FromAttribute = a.Value
	default:
		return fmt.Errorf("line %d: state.from is `value` or {attribute: NAME}", from.Line)
	}
	return nil
}

func (e *Expr) UnmarshalYAML(n *yaml.Node) error {
	if err := keys(n, "expression", "all", "any", "not", "rule", "key"); err != nil {
		return err
	}
	var out Expr
	set := 0
	for i := 0; i < len(n.Content); i += 2 {
		k, v := n.Content[i].Value, n.Content[i+1]
		switch k {
		case "all", "any":
			set++
			if v.Kind != yaml.SequenceNode || len(v.Content) == 0 {
				return fmt.Errorf("line %d: %s is a list of expressions", v.Line, k)
			}
			for _, x := range v.Content {
				var sub Expr
				if err := sub.UnmarshalYAML(x); err != nil {
					return err
				}
				if k == "all" {
					out.All = append(out.All, &sub)
				} else {
					out.Any = append(out.Any, &sub)
				}
			}
		case "not":
			set++
			var sub Expr
			if err := sub.UnmarshalYAML(v); err != nil {
				return err
			}
			out.Not = &sub
		case "rule":
			set++
			out.Rule = v.Value
		case "key":
			out.Key, out.HasKey = v.Value, true
		}
	}
	if set != 1 {
		return fmt.Errorf("line %d: an expression is exactly one of all, any, not, rule", n.Line)
	}
	if out.HasKey && out.Rule == "" {
		return fmt.Errorf("line %d: key goes with rule", n.Line)
	}
	*e = out
	return nil
}
