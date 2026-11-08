package def_test

import (
	"strings"
	"testing"

	"github.com/lesomnus/ward/def"
)

const base = `
version: 1
sources:
  detector:
    liveness: {metric: detector.frames, max_gap: 3s}
  report-job:
    liveness: none
rules: []
`

const rules = `
version: 1
sources: {}
rules:
  - id: detector.inference_failed
    kind: log
    source: detector
    severity: error
    match: {attributes: {diag.condition.type: detector.inference_failed}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 3, within: 200ms}
    resolve_after: 300ms
    release: {attributes: {diag.condition.type: detector.inference_failed, diag.condition.active: false}}
    on_source_restart: close
    operator: {ack: required, latch: false}

  - id: report-job.error
    kind: log
    source: report-job
    severity: error
    match: {severity: {gte: error}, attributes: {diag.condition.type: {exists: false}}}
    key: []
    activate: {count: 1}
    forget_after: 10m
    operator: {ack: none, latch: false}

  - id: detector.down
    kind: metric
    source: detector
    severity: error
    metric: {name: detector.frames}
    key: []
    value: {of: points, window: 10s, across: sum}
    limit: {levels: [{name: down, below: 1, severity: error}], deadband: 0}
    activate: {for: 0s}
    resolve: {for: 30s}
    startup_grace: 30s
    operator: {ack: required, latch: false}

  - id: battery.low
    kind: metric
    nodes: [n1]
    severity: warning
    metric: {name: battery.charge, match: {battery.id: main}}
    key: []
    value: {of: last, window: 10s, across: min}
    limit:
      levels:
        - {name: low_low, below: 10, severity: error}
        - {name: low, below: 20, severity: warning}
      deadband: 2
    activate: {for: 30s}
    resolve: {for: 60s}
    startup_grace: 30s
    operator: {ack: required, latch: true}

  - id: hw
    kind: metric
    source: detector
    severity: warning
    metric: {name: hw.status}
    key: [{attribute: hw.id}]
    value: {of: last, window: 30s, across: max}
    state: {from: {attribute: hw.state}, normal: [ok], abnormal: {failed: error}, other: warning}
    activate: {for: 0s}
    resolve: {for: 10s}
    startup_grace: 0s
    operator: {ack: none, latch: false}

  - id: node.unobservable
    kind: node
    nodes: [n1, n2]
    severity: error
    activate: {unobservable_for: 10s}
    resolve: {observable_for: 15s}
    operator: {ack: none, latch: false}

  - id: perception.degraded
    kind: composite
    severity: error
    scope: node
    when:
      all:
        - {rule: battery.low}
        - any: [{rule: detector.inference_failed}, {rule: detector.down}]
    activate: {for: 5s}
    resolve: {for: 10s}
    operator: {ack: required, latch: false}
    suppress_when: {rule: node.unobservable}
`

func parse(t *testing.T, name, src string) def.Definitions {
	t.Helper()
	f, err := def.Parse([]byte(src))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return def.Definitions{Provider: name, Revision: "r1", File: f}
}

func TestDesignExamplesMerge(t *testing.T) {
	s, warns, err := def.Merge([]string{"n1", "n2"}, parse(t, "base", base), parse(t, "rules", rules))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(s.Rules) != 7 || len(s.Sources) != 2 {
		t.Fatalf("got %d rules, %d sources", len(s.Rules), len(s.Sources))
	}
	pos := map[string]int{}
	for i, id := range s.Order {
		pos[id] = i
	}
	if pos["perception.degraded"] < pos["battery.low"] || pos["perception.degraded"] < pos["node.unobservable"] {
		t.Errorf("order puts a composite before what it reads: %v", s.Order)
	}
	if len(warns) != 0 {
		t.Errorf("warnings: %v", warns)
	}
}

func TestRefusals(t *testing.T) {
	cases := map[string]struct{ src, want string }{
		"unknown field":     {`{version: 1, sources: {}, rules: [{id: a, kind: log, sourse: x}]}`, "field sourse not found"},
		"missing sources":   {`{version: 1, rules: []}`, "sources: missing"},
		"unknown condition": {`{version: 1, sources: {}, rules: [{id: a, kind: log, match: {attributes: {x: {like: y}}}}]}`, "no field \"like\""},
	}
	for name, c := range cases {
		_, err := def.Parse([]byte(c.src))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want it to say %q", name, err, c.want)
		}
	}
}

func TestMergeRefusals(t *testing.T) {
	bad := `
version: 1
sources: {detector: {liveness: {metric: m, max_gap: 3s}}}
rules:
  - id: a
    kind: log
    source: detector
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 2}
    resolve_after: 1s
    operator: {ack: maybe}
  - id: b
    kind: composite
    severity: error
    scope: node
    when: {rule: c}
    activate: {for: 1s}
    resolve: {for: 1s}
    operator: {ack: none, latch: false}
  - id: c
    kind: composite
    severity: error
    scope: node
    when: {rule: b}
    activate: {for: 1s}
    resolve: {for: 1s}
    operator: {ack: none, latch: false}
`
	_, _, err := def.Merge(nil, parse(t, "p", bad))
	if err == nil {
		t.Fatal("merged")
	}
	for _, want := range []string{
		"activate.within: needed",
		"on_source_restart: needed",
		"operator.ack",
		"operator.latch: missing",
		"reads itself",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not say %q:\n%v", want, err)
		}
	}
}

func TestMatchHolds(t *testing.T) {
	f, err := def.Parse([]byte(rules))
	if err != nil {
		t.Fatal(err)
	}
	m := f.Rules[1].Match // severity >= error, no diag.condition.type
	attrs := map[string]any{"error.type": "timeout"}
	rec := def.Record{
		SeverityNumber: 17,
		Attrs:          func(k string) (any, bool) { v, ok := attrs[k]; return v, ok },
		Resource:       func(string) (any, bool) { return nil, false },
	}
	if !m.Holds(rec) {
		t.Error("an ERROR record without a type does not hold")
	}
	rec.SeverityNumber = 13
	if m.Holds(rec) {
		t.Error("a WARN record holds a gte error")
	}
	attrs["diag.condition.type"] = "x"
	rec.SeverityNumber = 21
	if m.Holds(rec) {
		t.Error("a record with a type holds exists: false")
	}

	rel := f.Rules[0].Release
	attrs = map[string]any{"diag.condition.type": "detector.inference_failed", "diag.condition.active": "false"}
	if rel.Holds(rec) {
		t.Error(`"false" the string holds false the bool`)
	}
	attrs["diag.condition.active"] = false
	if !rel.Holds(rec) {
		t.Error("the release does not hold")
	}
}

// A member that names a Condition its rule cannot have is refused: it would be
// expected and never observed.
func TestMemberMustBeObservable(t *testing.T) {
	merge := func(components string) error {
		f, err := def.Parse([]byte(base))
		if err != nil {
			t.Fatal(err)
		}
		g, err := def.Parse([]byte(rules + components))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		_, _, err = def.Merge([]string{"n1", "n2"}, def.Definitions{Provider: "base", File: f}, def.Definitions{Provider: "rules", File: g})
		return err
	}
	if err := merge(`
components:
  ok:
    parent: ""
    members:
      - {rule: detector.inference_failed, node: n1, key: front}
      - {rule: detector.down, node: n2}
      - {rule: battery.low, node: n1}
`); err != nil {
		t.Errorf("observable members refused: %v", err)
	}
	for _, bad := range []string{
		"      - {rule: detector.down, node: n1, key: front}\n", // a rule with key: []
		"      - {rule: battery.low, node: n2}\n",               // battery.low reads n1 only
	} {
		if err := merge("\ncomponents:\n  bad:\n    parent: \"\"\n    members:\n" + bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
