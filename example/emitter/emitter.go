package main

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
)

// Attributes of the `diag.*` convention (design §5.2). Ward gives them no
// special meaning; its rules name them like any other attribute.
const (
	attrConditionType   = "diag.condition.type"
	attrConditionKey    = "diag.condition.key"
	attrConditionActive = "diag.condition.active"
)

// hwStates are the values of `hw.state` in OpenTelemetry's hardware semantic
// conventions. hw.status has one series per state: 1 on the current one, 0 on
// the rest.
var hwStates = []string{"ok", "degraded", "failed", "needs_cleaning", "predicted_failure"}

// Emitter is the service being played: what it sends, and what it has been
// told to send.
type Emitter struct {
	// ctx is the program's: what the emitter sends outlives the HTTP request
	// that asked for it.
	ctx context.Context

	cfg      config
	service  string
	instance string
	started  time.Time

	logger   log.Logger
	meter    metric.Meter
	frames   metric.Int64Counter
	messages metric.Int64Counter

	// silent drops every log record; the metrics go on.
	silent atomic.Bool

	// hung stops the main loop's work: the liveness counter and the message
	// counters stop growing. The SDK still exports their cumulative values, so
	// data points keep arriving, flat.
	hung atomic.Bool

	mu         sync.Mutex
	conditions map[condition]*repeater
	errors     *repeater
	streams    map[string]*stream
	gauges     map[string]*gauge
	hardware   map[string]hardware
	exiting    bool
}

// condition is what a `diag.*` record is about.
type condition struct{ typ, key string }

func (c condition) String() string {
	if c.key == "" {
		return c.typ
	}
	return c.typ + "/" + c.key
}

// repeater sends a record every so often until it is stopped.
type repeater struct {
	every  time.Duration
	attrs  []attribute.KeyValue
	body   string
	since  time.Time
	sent   atomic.Int64
	cancel context.CancelFunc
}

// stream is a message counter growing at a rate.
type stream struct {
	rate   float64
	cancel context.CancelFunc
}

// gauge is a gauge by name, and the value of each of its series.
type gauge struct {
	series map[attribute.Distinct]gaugeSeries
}

type gaugeSeries struct {
	attrs attribute.Set
	value float64
}

// hardware is one device of hw.status.
type hardware struct {
	typ   string
	state string
}

func newEmitter(ctx context.Context, c config, logger log.Logger, meter metric.Meter, service, instance string) (*Emitter, error) {
	e := &Emitter{
		ctx:        ctx,
		cfg:        c,
		service:    service,
		instance:   instance,
		started:    time.Now(),
		logger:     logger,
		meter:      meter,
		conditions: map[condition]*repeater{},
		streams:    map[string]*stream{},
		gauges:     map[string]*gauge{},
		hardware:   map[string]hardware{},
	}

	var err error
	e.frames, err = meter.Int64Counter(c.liveness,
		metric.WithDescription("Runs of the main loop. Its data points say the process is alive; its start time says since when."))
	if err != nil {
		return nil, err
	}
	e.messages, err = meter.Int64Counter("emitter.messages",
		metric.WithDescription("Messages sent, per stream."))
	if err != nil {
		return nil, err
	}
	_, err = meter.Int64ObservableGauge("hw.status",
		metric.WithDescription("Operational status of a device: 1 on the series of its current hw.state, 0 on the others."),
		metric.WithInt64Callback(e.observeHardware))
	if err != nil {
		return nil, err
	}

	return e, nil
}

// loop is the main loop: one more on the liveness counter per run, until ctx
// is done.
func (e *Emitter) loop(ctx context.Context) {
	// The series exists from the start, so "nothing yet" reads as zero rather
	// than as no data.
	e.frames.Add(ctx, 0)

	t := time.NewTicker(e.cfg.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if e.hung.Load() {
			continue
		}
		e.frames.Add(ctx, 1)
	}
}

// emit sends one log record now, and reports whether it was sent: nothing is
// while the emitter is silent.
func (e *Emitter) emit(severity log.Severity, body string, attrs ...attribute.KeyValue) bool {
	if e.silent.Load() {
		return false
	}

	now := time.Now()
	var r log.Record
	r.SetTimestamp(now)
	r.SetObservedTimestamp(now)
	r.SetSeverity(severity)
	r.SetSeverityText(severityText(severity))
	r.SetBody(attribute.StringValue(body))
	r.AddAttributes(attrs...)
	e.logger.Emit(e.ctx, r)
	return true
}

func severityText(s log.Severity) string {
	switch s {
	case log.SeverityError:
		return "ERROR"
	case log.SeverityInfo:
		return "INFO"
	}
	return s.String()
}

// diagAttrs are the attributes of a record about c. A condition without a key
// carries no key attribute, which a rule keyed on it reads as "".
func diagAttrs(c condition, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(attrConditionType, c.typ)}
	if c.key != "" {
		attrs = append(attrs, attribute.String(attrConditionKey, c.key))
	}
	return append(attrs, extra...)
}

// repeat starts r, replacing *slot, and answers with what it replaced.
func (e *Emitter) repeat(slot **repeater, r *repeater) *repeater {
	ctx, cancel := context.WithCancel(e.ctx)
	r.cancel = cancel
	r.since = time.Now()

	prev := *slot
	if prev != nil {
		prev.cancel()
	}
	*slot = r

	go func() {
		t := time.NewTicker(r.every)
		defer t.Stop()
		for {
			if e.emit(log.SeverityError, r.body, r.attrs...) {
				r.sent.Add(1)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return prev
}

// setRate makes the stream's counter grow by rate per second; 0 keeps the
// series and stops it growing.
func (e *Emitter) setRate(name string, rate float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if s, ok := e.streams[name]; ok {
		s.cancel()
	}
	ctx, cancel := context.WithCancel(e.ctx)
	e.streams[name] = &stream{rate: rate, cancel: cancel}

	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("stream", name)))
	e.messages.Add(ctx, 0, attrs)
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		last := time.Now()
		var owed float64
		for {
			var now time.Time
			select {
			case <-ctx.Done():
				return
			case now = <-t.C:
			}
			elapsed := now.Sub(last)
			last = now
			if e.hung.Load() {
				continue
			}
			owed += rate * elapsed.Seconds()
			if n := int64(owed); n > 0 {
				owed -= float64(n)
				e.messages.Add(ctx, n, attrs)
			}
		}
	}()
}

// setGauge sets one series of the gauge called name, making the gauge the
// first time it is named.
func (e *Emitter) setGauge(name string, value float64, attrs attribute.Set) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	g, ok := e.gauges[name]
	if !ok {
		g = &gauge{series: map[attribute.Distinct]gaugeSeries{}}
		_, err := e.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			e.mu.Lock()
			defer e.mu.Unlock()
			for _, s := range g.series {
				o.Observe(s.value, metric.WithAttributeSet(s.attrs))
			}
			return nil
		}))
		if err != nil {
			return err
		}
		e.gauges[name] = g
	}
	g.series[attrs.Equivalent()] = gaugeSeries{attrs: attrs, value: value}
	return nil
}

// stopGauge stops every series of the gauge called name: from the next export
// on it has no data points, which is a gauge that went away rather than one
// that reads zero.
func (e *Emitter) stopGauge(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	g, ok := e.gauges[name]
	if !ok || len(g.series) == 0 {
		return false
	}
	clear(g.series)
	return true
}

func (e *Emitter) observeHardware(_ context.Context, o metric.Int64Observer) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for id, h := range e.hardware {
		states := hwStates
		if !slices.Contains(states, h.state) {
			// A state the convention does not name, which a rule's `other`
			// is for.
			states = append(slices.Clone(states), h.state)
		}
		for _, s := range states {
			var v int64
			if s == h.state {
				v = 1
			}
			o.Observe(v, metric.WithAttributes(
				attribute.String("hw.id", id),
				attribute.String("hw.type", h.typ),
				attribute.String("hw.state", s),
			))
		}
	}
	return nil
}

// exit ends the process with code after the delay, the way a crash does: the
// batches waiting in the SDK are not exported.
func (e *Emitter) exit(code int, after time.Duration) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.exiting {
		return false
	}
	e.exiting = true

	time.AfterFunc(after, func() {
		slog.Warn("exiting, as told", slog.Int("code", code))
		os.Exit(code)
	})
	return true
}

// Status is what GET / answers with.
type Status struct {
	Service    string                    `json:"service"`
	Instance   string                    `json:"instance"`
	Started    time.Time                 `json:"started"`
	Liveness   string                    `json:"liveness"`
	Silent     bool                      `json:"silent"`
	Hung       bool                      `json:"hung"`
	Exiting    bool                      `json:"exiting"`
	Conditions []RepeatStatus            `json:"conditions"`
	Errors     *RepeatStatus             `json:"errors"`
	Streams    map[string]float64        `json:"streams"`
	Gauges     map[string][]GaugeStatus  `json:"gauges"`
	Hardware   map[string]HardwareStatus `json:"hardware"`
}

type RepeatStatus struct {
	Type  string `json:"type,omitempty"`
	Key   string `json:"key,omitempty"`
	Body  string `json:"body"`
	Every string `json:"every"`
	Since string `json:"since"`
	Sent  int64  `json:"sent"`
}

type GaugeStatus struct {
	Attributes map[string]string `json:"attributes"`
	Value      float64           `json:"value"`
}

type HardwareStatus struct {
	Type  string `json:"type"`
	State string `json:"state"`
}

func (e *Emitter) status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := Status{
		Service:  e.service,
		Instance: e.instance,
		Started:  e.started,
		Liveness: e.cfg.liveness,
		Silent:   e.silent.Load(),
		Hung:     e.hung.Load(),
		Exiting:  e.exiting,
		Streams:  map[string]float64{},
		Gauges:   map[string][]GaugeStatus{},
		Hardware: map[string]HardwareStatus{},
	}
	for _, c := range slices.SortedFunc(maps.Keys(e.conditions), func(a, b condition) int {
		return cmp.Or(cmp.Compare(a.typ, b.typ), cmp.Compare(a.key, b.key))
	}) {
		r := repeatStatus(e.conditions[c])
		r.Type, r.Key = c.typ, c.key
		s.Conditions = append(s.Conditions, r)
	}
	if e.errors != nil {
		r := repeatStatus(e.errors)
		s.Errors = &r
	}
	for name, st := range e.streams {
		s.Streams[name] = st.rate
	}
	for name, g := range e.gauges {
		for _, x := range g.series {
			attrs := map[string]string{}
			for _, kv := range x.attrs.ToSlice() {
				attrs[string(kv.Key)] = kv.Value.Emit()
			}
			s.Gauges[name] = append(s.Gauges[name], GaugeStatus{Attributes: attrs, Value: x.value})
		}
	}
	for id, h := range e.hardware {
		s.Hardware[id] = HardwareStatus{Type: h.typ, State: h.state}
	}
	return s
}

func repeatStatus(r *repeater) RepeatStatus {
	return RepeatStatus{
		Body:  r.body,
		Every: r.every.String(),
		Since: time.Since(r.since).Truncate(time.Second).String(),
		Sent:  r.sent.Load(),
	}
}
