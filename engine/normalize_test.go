package engine

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/signal"
)

func normalizer(t *testing.T, rules string) (*Normalizer, *Gate) {
	t.Helper()
	h := newHarness(t, rules)
	g := NewGate(conf.Gate{Hold: s, Canary: conf.Canary{Interval: 100 * time.Millisecond, MaxRTT: s, CheckWindow: s}}, []string{"n1", "n2"})
	n := NewNormalizer(func() time.Time { return t0 }, map[string]map[string]string{
		"n1": {"host.name": "n1"},
		"n2": {"host.name": "n2"},
	}, "run-1", g, time.Minute)
	n.Use(h.ev.defs)
	return n, g
}

type rec struct {
	host, service string
	at            time.Time
	sev           plog.SeverityNumber
	attrs         map[string]any
	body          string
}

func logs(rs ...rec) plog.Logs {
	ld := plog.NewLogs()
	for _, r := range rs {
		rl := ld.ResourceLogs().AppendEmpty()
		if r.host != "" {
			rl.Resource().Attributes().PutStr("host.name", r.host)
		}
		rl.Resource().Attributes().PutStr("service.name", r.service)
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(r.at))
		lr.SetSeverityNumber(r.sev)
		lr.Body().SetStr(r.body)
		lr.Attributes().FromRaw(r.attrs)
	}
	return ld
}

func signals(in []any) (n int, released int) {
	for _, x := range in {
		if l, ok := x.(logIn); ok {
			if l.release {
				released++
			} else {
				n++
			}
		}
	}
	return
}

// The same record sent again is one signal, however many times it comes;
// two records at the same time that differ are two (design §7.5).
func TestNormalizerDuplicates(t *testing.T) {
	n, _ := normalizer(t, logRules)
	fail := rec{host: "n1", service: "det", at: t0, sev: plog.SeverityNumberError, attrs: map[string]any{"diag.condition.type": "det.fail", "diag.condition.key": "front"}}
	other := fail
	other.body = "different"
	for range 5 {
		n.ConsumeLogs(context.Background(), logs(fail))
	}
	n.ConsumeLogs(context.Background(), logs(other))
	if got, _ := signals(n.Drain()); got != 2 {
		t.Errorf("%d signals of five copies and a different record, want 2", got)
	}
	if d := n.Stats().Duplicates; d != 4 {
		t.Errorf("%d duplicates, want 4", d)
	}
}

// What no rule reads is counted by source and condition type; an attribute of
// the wrong type is reported; a resource no node matches is reported.
func TestNormalizerReports(t *testing.T) {
	n, _ := normalizer(t, logRules)
	n.ConsumeLogs(context.Background(), logs(
		rec{host: "n1", service: "det", at: t0, sev: plog.SeverityNumberError, attrs: map[string]any{"diag.condition.type": "nobody.reads"}},
		rec{host: "n1", service: "det", at: t0, sev: plog.SeverityNumberInfo, attrs: map[string]any{"diag.condition.type": "det.fail", "diag.condition.active": "false"}},
		rec{host: "elsewhere", service: "det", at: t0, sev: plog.SeverityNumberError, attrs: map[string]any{"diag.condition.type": "det.fail"}},
	))
	in := n.Drain()
	st := n.Stats()
	if st.Unmatched[UnmatchedKey{"det", "nobody.reads"}] != 1 {
		t.Errorf("unmatched: %v", st.Unmatched)
	}
	if st.Violations[ViolationKey{"det", "diag.condition.active", "bool", "string"}] != 1 {
		t.Errorf("violations: %v", st.Violations)
	}
	if _, ok := st.UnknownHosts["elsewhere"]; !ok {
		t.Errorf("unknown hosts: %v", st.UnknownHosts)
	}
	// The string "false" is no release; the record still matches det.fail.
	if got, rel := signals(in); got != 1 || rel != 0 {
		t.Errorf("%d signals and %d releases, want 1 and 0", got, rel)
	}
}

// A record two rules match is a signal of each.
func TestNormalizerTwoRules(t *testing.T) {
	n, _ := normalizer(t, `
version: 1
sources: {}
rules:
  - id: a
    kind: log
    source: job
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 1}
    forget_after: 1m
    operator: {ack: none, latch: false}
  - id: b
    kind: log
    source: job
    severity: warning
    match: {attributes: {error.type: timeout}}
    key: [{attribute: server.address}]
    activate: {count: 1}
    forget_after: 1m
    operator: {ack: none, latch: false}
`)
	n.ConsumeLogs(context.Background(), logs(rec{host: "n2", service: "job", at: t0, sev: plog.SeverityNumberError, attrs: map[string]any{"error.type": "timeout", "server.address": "db:5432"}}))
	keys := map[string]string{}
	for _, x := range n.Drain() {
		l := x.(logIn)
		keys[l.rule.ID] = l.key
	}
	if len(keys) != 2 || keys["a"] != "" || keys["b"] != "db:5432" {
		t.Errorf("matched %v", keys)
	}
}

// A canary of this run comes back to the gate; one of an earlier run of Ward
// is ignored.
func TestNormalizerCanary(t *testing.T) {
	n, g := normalizer(t, logRules)
	g.Sent("n1", 7, t0)
	canary := func(run string) plog.Logs {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", signal.Canary)
		a := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes()
		a.PutStr(signal.CanaryRun, run)
		a.PutInt(signal.CanarySeq, 7)
		a.PutStr(signal.CanaryNode, "n1")
		return ld
	}
	n.ConsumeLogs(context.Background(), canary("run-0"))
	if s := g.nodes["n1"].find(7); s.back[PathLog] {
		t.Fatal("a canary of another run counted")
	}
	n.ConsumeLogs(context.Background(), canary("run-1"))
	if s := g.nodes["n1"].find(7); !s.back[PathLog] {
		t.Error("the canary did not come back")
	}
	if len(n.Drain()) != 0 {
		t.Error("a canary is not a signal")
	}
}

var _ = def.KindLog
