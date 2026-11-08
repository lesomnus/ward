package engine

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/signal"
)

// Normalizer turns what a receiver hands over -- OTel's data model -- into what
// the evaluation reads: the records a rule matched, by rule, node and Key, and
// the data points that are evidence. It decides each signal's node, takes out
// duplicates, recognizes Ward's own canaries, and counts what no rule reads.
//
// It is the engine's signal.Sink. A receiver's goroutine calls it; the loop
// drains what it took in once a tick.
type Normalizer struct {
	now   func() time.Time
	nodes []nodeMatch
	run   string
	gate  *Gate

	// How long a record is remembered for taking out its duplicates: a
	// Collector retries for up to the recovery window.
	dedupeFor time.Duration

	defs atomic.Pointer[Definitions]

	mu      sync.Mutex
	inbox   []any
	dedupe  map[uint64]time.Time
	pruned  time.Time
	stats   Stats
	sources map[srcKey]*sourceSeen
}

type nodeMatch struct {
	name  string
	match map[string]string
}

type srcKey struct{ node, source string }

type sourceSeen struct {
	signaled time.Time
	instance string
}

// Stats are the Normalizer's counts, for StatusService.
type Stats struct {
	Unmatched    map[UnmatchedKey]int64
	Violations   map[ViolationKey]int64
	UnknownHosts map[string]time.Time
	Duplicates   int64
}

type UnmatchedKey struct{ Source, ConditionType string }

type ViolationKey struct{ Source, Attribute, Want, Got string }

// What the Normalizer hands the evaluation.
type (
	// logIn is a record a log rule matched, or that its release matched.
	logIn struct {
		rule     *rule
		node     string
		key      string
		t        time.Time
		release  bool
		instance string
	}

	// pointIn is a data point a metric rule reads, as its own series or as the
	// series its value is judged relative to.
	pointIn struct {
		rule     *rule
		ref      bool
		node     string
		key      string
		series   uint64 // node, attributes and start
		group    uint64 // node and attributes
		t, start time.Time
		instance string
		v        float64
		state    string
	}

	// liveIn is a data point of a source's liveness counter.
	liveIn struct {
		node, source string
		t, start     time.Time
		instance     string
	}

	// bootIn is a node's boot time, from a system.uptime data point at t.
	bootIn struct {
		node    string
		boot, t time.Time
	}
)

// NewNormalizer answers with a Normalizer for the static configuration's
// nodes. run is this run's identifier, which Ward's own canaries carry.
func NewNormalizer(now func() time.Time, nodes map[string]map[string]string, run string, gate *Gate, dedupeFor time.Duration) *Normalizer {
	n := &Normalizer{
		now:       now,
		run:       run,
		gate:      gate,
		dedupeFor: dedupeFor,
		dedupe:    map[uint64]time.Time{},
		sources:   map[srcKey]*sourceSeen{},
		stats: Stats{
			Unmatched:    map[UnmatchedKey]int64{},
			Violations:   map[ViolationKey]int64{},
			UnknownHosts: map[string]time.Time{},
		},
	}
	for _, name := range slices.Sorted(maps.Keys(nodes)) {
		n.nodes = append(n.nodes, nodeMatch{name: name, match: nodes[name]})
	}
	n.defs.Store(Compile(nil))
	return n
}

// Use makes d what signals are matched against from now on.
func (n *Normalizer) Use(d *Definitions) { n.defs.Store(d) }

// Drain answers with what came in since the last Drain.
func (n *Normalizer) Drain() []any {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := n.inbox
	n.inbox = nil

	now := n.now()
	if now.Sub(n.pruned) > time.Minute {
		for h, t := range n.dedupe {
			if now.Sub(t) > n.dedupeFor {
				delete(n.dedupe, h)
			}
		}
		for host, t := range n.stats.UnknownHosts {
			if now.Sub(t) > time.Hour {
				delete(n.stats.UnknownHosts, host)
			}
		}
		n.pruned = now
	}
	return out
}

// Stats answers with a copy of the counts.
func (n *Normalizer) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Stats{
		Unmatched:    maps.Clone(n.stats.Unmatched),
		Violations:   maps.Clone(n.stats.Violations),
		UnknownHosts: maps.Clone(n.stats.UnknownHosts),
		Duplicates:   n.stats.Duplicates,
	}
}

// SourceSeen answers when a node last sent a signal of a source, and which
// instance sent it.
func (n *Normalizer) SourceSeen(node, source string) (time.Time, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.sources[srcKey{node, source}]
	if !ok {
		return time.Time{}, ""
	}
	return s.signaled, s.instance
}

// Signaled answers when any node last sent a signal of the source.
func (n *Normalizer) Signaled(source string) time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	var t time.Time
	for k, s := range n.sources {
		if k.source == source {
			t = later(t, s.signaled)
		}
	}
	return t
}

func (n *Normalizer) nodeOf(res pcommon.Map) (string, bool) {
	for _, m := range n.nodes {
		ok := true
		for k, want := range m.match {
			v, has := res.Get(k)
			if !has || v.AsString() != want {
				ok = false
				break
			}
		}
		if ok {
			return m.name, true
		}
	}
	return "", false
}

// unknownHost records a resource that no node's match holds for.
func (n *Normalizer) unknownHost(res pcommon.Map) {
	host := "(no host.name)"
	if v, ok := res.Get("host.name"); ok {
		host = v.AsString()
	}
	if len(n.stats.UnknownHosts) < 256 {
		n.stats.UnknownHosts[host] = n.now()
	}
}

func (n *Normalizer) seen(node, source, instance string, t time.Time) {
	k := srcKey{node, source}
	s, ok := n.sources[k]
	if !ok {
		s = &sourceSeen{}
		n.sources[k] = s
	}
	if t.After(s.signaled) {
		s.signaled = t
		s.instance = instance
	}
}

// ConsumeLogs implements signal.Sink.
func (n *Normalizer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	d := n.defs.Load()
	now := n.now()

	n.mu.Lock()
	defer n.mu.Unlock()

	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		rl := rls.At(i)
		res := rl.Resource().Attributes()
		source := str(res, "service.name")
		instance := str(res, "service.instance.id")

		if source == signal.Canary {
			n.canaryLogs(rl)
			continue
		}
		node, ok := n.nodeOf(res)
		if !ok {
			n.unknownHost(res)
			continue
		}
		rules := d.logBySource[source]
		typed := d.typed[source]
		resLookup := lookup(res)

		sls := rl.ScopeLogs()
		for j := range sls.Len() {
			lrs := sls.At(j).LogRecords()
			for k := range lrs.Len() {
				lr := lrs.At(k)
				t := eventTime(lr.Timestamp(), lr.ObservedTimestamp(), now)
				n.seen(node, source, instance, t)

				attrs := lr.Attributes()
				for name, want := range typed {
					if v, ok := attrs.Get(name); ok {
						if got := valueType(v); !typeFits(want, got) {
							n.stats.Violations[ViolationKey{source, name, want, got}]++
						}
					}
				}

				rec := def.Record{
					SeverityNumber: int32(lr.SeverityNumber()),
					EventName:      lr.EventName(),
					Attrs:          lookup(attrs),
					Resource:       resLookup,
				}
				var hits []logIn
				for _, r := range rules {
					release := r.Release != nil && r.Release.Holds(rec)
					if !release && !r.Match.Holds(rec) {
						continue
					}
					hits = append(hits, logIn{
						rule: r, node: node, key: keyFrom(r.Key, attrs, res),
						t: t, release: release, instance: instance,
					})
				}
				if len(hits) == 0 {
					n.stats.Unmatched[UnmatchedKey{source, str(attrs, "diag.condition.type")}]++
					continue
				}
				h := logHash(node, res, lr)
				if _, dup := n.dedupe[h]; dup {
					n.stats.Duplicates++
					continue
				}
				n.dedupe[h] = now
				for _, x := range hits {
					n.inbox = append(n.inbox, x)
				}
			}
		}
	}
	return nil
}

func (n *Normalizer) canaryLogs(rl plog.ResourceLogs) {
	sls := rl.ScopeLogs()
	for j := range sls.Len() {
		lrs := sls.At(j).LogRecords()
		for k := range lrs.Len() {
			attrs := lrs.At(k).Attributes()
			if str(attrs, signal.CanaryRun) != n.run {
				continue
			}
			seq, ok := attrs.Get(signal.CanarySeq)
			if !ok {
				continue
			}
			n.gate.Returned(str(attrs, signal.CanaryNode), PathLog, seq.Int())
		}
	}
}

// ConsumeMetrics implements signal.Sink.
func (n *Normalizer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	d := n.defs.Load()
	now := n.now()

	n.mu.Lock()
	defer n.mu.Unlock()

	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		res := rm.Resource().Attributes()
		source := str(res, "service.name")
		instance := str(res, "service.instance.id")

		sms := rm.ScopeMetrics()
		if source == signal.Canary {
			n.canaryMetrics(sms)
			continue
		}
		node, ok := n.nodeOf(res)
		if !ok {
			n.unknownHost(res)
			continue
		}

		for j := range sms.Len() {
			ms := sms.At(j).Metrics()
			for k := range ms.Len() {
				m := ms.At(k)
				var (
					dps        pmetric.NumberDataPointSlice
					cumulative bool
					kind       string
				)
				switch m.Type() {
				case pmetric.MetricTypeSum:
					dps = m.Sum().DataPoints()
					cumulative = m.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
					kind = "cumulative sum"
					if !cumulative {
						kind = "delta sum"
					}
				case pmetric.MetricTypeGauge:
					dps = m.Gauge().DataPoints()
					kind = "gauge"
				default:
					continue
				}
				name := m.Name()

				for p := range dps.Len() {
					dp := dps.At(p)
					t := eventTime(dp.Timestamp(), 0, now)
					if source != "" {
						n.seen(node, source, instance, t)
					}
				}

				if name == "system.uptime" {
					for p := range dps.Len() {
						dp := dps.At(p)
						t := eventTime(dp.Timestamp(), 0, now)
						up := number(dp)
						if up < 0 || math.IsNaN(up) {
							continue
						}
						n.inbox = append(n.inbox, bootIn{node: node, t: t, boot: t.Add(-time.Duration(up * float64(time.Second))).Truncate(time.Second)})
					}
				}

				for _, s := range d.livenessByMetric[name] {
					if s != source {
						continue
					}
					if kind != "cumulative sum" {
						n.stats.Violations[ViolationKey{source, name, "cumulative sum", kind}]++
						continue
					}
					for p := range dps.Len() {
						dp := dps.At(p)
						n.inbox = append(n.inbox, liveIn{
							node: node, source: source, instance: instance,
							t: eventTime(dp.Timestamp(), 0, now), start: unixTime(dp.StartTimestamp()),
						})
					}
				}

				n.points(d.metricByName[name], false, node, source, instance, res, dps, now)
				n.points(d.refByName[name], true, node, source, instance, res, dps, now)
			}
		}
	}
	return nil
}

func (n *Normalizer) points(rules []*rule, ref bool, node, source, instance string, res pcommon.Map, dps pmetric.NumberDataPointSlice, now time.Time) {
	for _, r := range rules {
		if r.Source != "" && r.Source != source {
			continue
		}
		if len(r.Nodes) > 0 && !slices.Contains(r.Nodes, node) {
			continue
		}
		sel := r.Metric
		if ref {
			sel = r.Limit.RelativeTo.Metric
		}
		for p := range dps.Len() {
			dp := dps.At(p)
			attrs := dp.Attributes()
			if !def.HoldsAttrs(sel.Match, lookup(attrs)) {
				continue
			}
			x := pointIn{
				rule: r, ref: ref, node: node, key: keyFrom(r.Key, attrs, res),
				t: eventTime(dp.Timestamp(), 0, now), start: unixTime(dp.StartTimestamp()),
				instance: instance, v: number(dp),
			}
			x.group = attrHash(node, attrs, 0)
			x.series = attrHash(node, attrs, dp.StartTimestamp())
			if r.State != nil && r.State.FromAttribute != "" {
				x.state = str(attrs, r.State.FromAttribute)
			}
			n.inbox = append(n.inbox, x)
		}
	}
}

func (n *Normalizer) canaryMetrics(sms pmetric.ScopeMetricsSlice) {
	for j := range sms.Len() {
		ms := sms.At(j).Metrics()
		for k := range ms.Len() {
			m := ms.At(k)
			var dps pmetric.NumberDataPointSlice
			switch m.Type() {
			case pmetric.MetricTypeSum:
				dps = m.Sum().DataPoints()
			case pmetric.MetricTypeGauge:
				dps = m.Gauge().DataPoints()
			default:
				continue
			}
			for p := range dps.Len() {
				attrs := dps.At(p).Attributes()
				if str(attrs, signal.CanaryRun) != n.run {
					continue
				}
				seq, ok := attrs.Get(signal.CanarySeq)
				if !ok {
					continue
				}
				n.gate.Returned(str(attrs, signal.CanaryNode), PathMetric, seq.Int())
			}
		}
	}
}

// eventTime is when a record says it happened: its timestamp, the time it was
// observed when it has none, and now when it has neither.
func eventTime(ts, observed pcommon.Timestamp, now time.Time) time.Time {
	switch {
	case ts != 0:
		return ts.AsTime()
	case observed != 0:
		return observed.AsTime()
	}
	return now
}

func unixTime(ts pcommon.Timestamp) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	return ts.AsTime()
}

func number(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

func str(m pcommon.Map, k string) string {
	if v, ok := m.Get(k); ok {
		return v.AsString()
	}
	return ""
}

// lookup reads a map's values as def's predicates compare them.
func lookup(m pcommon.Map) def.Lookup {
	return func(k string) (any, bool) {
		v, ok := m.Get(k)
		if !ok {
			return nil, false
		}
		return value(v), true
	}
}

func value(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeSlice:
		return v.Slice().AsRaw()
	case pcommon.ValueTypeMap:
		return v.Map().AsRaw()
	}
	return nil
}

func valueType(v pcommon.Value) string { return typeName(value(v)) }

func typeFits(want, got string) bool {
	if want == "number" {
		return got == "int" || got == "double"
	}
	return want == got
}

func keyFrom(parts []def.KeyPart, attrs, res pcommon.Map) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		if parts[0].Resource != "" {
			return str(res, parts[0].Resource)
		}
		return str(attrs, parts[0].Attribute)
	}
	vs := make([]string, len(parts))
	for i, p := range parts {
		if p.Resource != "" {
			vs[i] = str(res, p.Resource)
		} else {
			vs[i] = str(attrs, p.Attribute)
		}
	}
	return keyOf(vs)
}

// logHash is what makes two records the same signal: everything of them
// (design §7.5).
func logHash(node string, res pcommon.Map, lr plog.LogRecord) uint64 {
	h := fnv.New64a()
	var b [8]byte
	h.Write([]byte(node))
	h.Write([]byte{0})
	writeMap(h, res)
	binary.BigEndian.PutUint64(b[:], uint64(lr.Timestamp()))
	h.Write(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(lr.ObservedTimestamp()))
	h.Write(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(lr.SeverityNumber()))
	h.Write(b[:])
	h.Write([]byte(lr.SeverityText()))
	h.Write([]byte{0})
	h.Write([]byte(lr.EventName()))
	h.Write([]byte{0})
	h.Write([]byte(lr.Body().AsString()))
	h.Write([]byte{0})
	writeMap(h, lr.Attributes())
	return h.Sum64()
}

func attrHash(node string, attrs pcommon.Map, start pcommon.Timestamp) uint64 {
	h := fnv.New64a()
	h.Write([]byte(node))
	h.Write([]byte{0})
	writeMap(h, attrs)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(start))
	h.Write(b[:])
	return h.Sum64()
}

type writer interface{ Write([]byte) (int, error) }

func writeMap(h writer, m pcommon.Map) {
	ks := make([]string, 0, m.Len())
	for k := range m.All() {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		v, _ := m.Get(k)
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(strconv.Itoa(int(v.Type()))))
		h.Write([]byte(v.AsString()))
		h.Write([]byte{0})
	}
}
