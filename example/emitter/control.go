package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

// routes is the control endpoint. Every command is a POST with its arguments
// in the query, and answers with one line saying what it did; GET / answers
// with everything the emitter is doing, as JSON. README.md lists them.
func (e *Emitter) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", e.handleStatus)

	mux.HandleFunc("POST /condition/repeat", e.handleConditionRepeat)
	mux.HandleFunc("POST /condition/stop", e.handleConditionStop)
	mux.HandleFunc("POST /condition/once", e.handleConditionOnce)
	mux.HandleFunc("POST /condition/release", e.handleConditionRelease)

	mux.HandleFunc("POST /error/repeat", e.handleErrorRepeat)
	mux.HandleFunc("POST /error/stop", e.handleErrorStop)
	mux.HandleFunc("POST /error/once", e.handleErrorOnce)

	mux.HandleFunc("POST /silence", e.toggle(&e.silent, true, "silent: no log record is sent; the metrics go on"))
	mux.HandleFunc("POST /silence/stop", e.toggle(&e.silent, false, "logging again"))
	mux.HandleFunc("POST /hang", e.toggle(&e.hung, true, "hung: the counters stop growing; their flat values are still exported"))
	mux.HandleFunc("POST /hang/stop", e.toggle(&e.hung, false, "running again"))
	mux.HandleFunc("POST /exit", e.handleExit)

	mux.HandleFunc("POST /gauge", e.handleGauge)
	mux.HandleFunc("POST /gauge/stop", e.handleGaugeStop)
	mux.HandleFunc("POST /messages", e.handleMessages)
	mux.HandleFunc("POST /hw", e.handleHardware)
	mux.HandleFunc("POST /hw/stop", e.handleHardwareStop)

	return mux
}

func (e *Emitter) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(e.status())
}

// done answers a command, and says it on stderr too, so that the container's
// log reads as the story of what was done to it.
func done(w http.ResponseWriter, r *http.Request, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	slog.Info(msg, slog.String("command", r.URL.Path), slog.String("query", r.URL.RawQuery))
	fmt.Fprintln(w, msg)
}

func fail(w http.ResponseWriter, code int, format string, a ...any) {
	http.Error(w, fmt.Sprintf(format, a...), code)
}

// conditionOf reads `type` (required) and `key`.
func conditionOf(q url.Values) (condition, error) {
	c := condition{typ: q.Get("type"), key: q.Get("key")}
	if c.typ == "" {
		return c, fmt.Errorf("type is required: the value of %s", attrConditionType)
	}
	return c, nil
}

// every reads `every`: a duration such as 500ms or 2s, or a bare number of
// milliseconds.
func every(q url.Values, fallback time.Duration) (time.Duration, error) {
	v := q.Get("every")
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		ms, err2 := strconv.Atoi(v)
		if err2 != nil {
			return 0, fmt.Errorf("every: %q is a duration (500ms) or milliseconds (500)", v)
		}
		d = time.Duration(ms) * time.Millisecond
	}
	if d < 10*time.Millisecond {
		return 0, fmt.Errorf("every: %s is shorter than 10ms", d)
	}
	return d, nil
}

func (e *Emitter) handleConditionRepeat(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := conditionOf(q)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	d, err := every(q, time.Second)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}

	rep := &repeater{every: d, attrs: diagAttrs(c), body: bodyOr(q, c.String()+": still failing")}
	e.mu.Lock()
	slot := e.conditions[c]
	e.repeat(&slot, rep)
	e.conditions[c] = slot
	e.mu.Unlock()

	done(w, r, "repeating ERROR %s=%s %s=%q every %s", attrConditionType, c.typ, attrConditionKey, c.key, d)
}

func (e *Emitter) handleConditionStop(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	e.mu.Lock()
	defer e.mu.Unlock()

	if q.Get("type") == "" {
		n := len(e.conditions)
		for c, rep := range e.conditions {
			rep.cancel()
			delete(e.conditions, c)
		}
		done(w, r, "stopped %d repeating conditions", n)
		return
	}

	c, _ := conditionOf(q)
	rep, ok := e.conditions[c]
	if !ok {
		fail(w, http.StatusNotFound, "%s %q is not repeating", c.typ, c.key)
		return
	}
	rep.cancel()
	delete(e.conditions, c)
	done(w, r, "stopped %s %q after %d records", c.typ, c.key, rep.sent.Load())
}

func (e *Emitter) handleConditionOnce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := conditionOf(q)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	if !e.emit(log.SeverityError, bodyOr(q, c.String()+": failed"), diagAttrs(c)...) {
		fail(w, http.StatusConflict, "silent: nothing sent")
		return
	}
	done(w, r, "sent one ERROR %s=%s %s=%q", attrConditionType, c.typ, attrConditionKey, c.key)
}

// handleConditionRelease sends the record that says a condition is over: INFO,
// the same type and key, and `diag.condition.active` false. With as=string the
// false is the string "false" -- a record of the wrong type, which a rule
// that asks for the bool must not take for a release.
func (e *Emitter) handleConditionRelease(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := conditionOf(q)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}

	active := attribute.Bool(attrConditionActive, false)
	switch q.Get("as") {
	case "", "bool":
	case "string":
		active = attribute.String(attrConditionActive, "false")
	default:
		fail(w, http.StatusBadRequest, "as: %q is bool or string", q.Get("as"))
		return
	}

	if !e.emit(log.SeverityInfo, bodyOr(q, c.String()+": recovered"), diagAttrs(c, active)...) {
		fail(w, http.StatusConflict, "silent: nothing sent")
		return
	}
	done(w, r, "sent INFO %s=%s %s=%q %s=%s (%s)", attrConditionType, c.typ, attrConditionKey, c.key, attrConditionActive, active.Value.Emit(), active.Value.Type())
}

// errorAttrs are what a plain error carries: nothing, or the standard
// `error.type` and `server.address` when asked.
func errorAttrs(q url.Values) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if v := q.Get("error_type"); v != "" {
		attrs = append(attrs, attribute.String("error.type", v))
	}
	if v := q.Get("server_address"); v != "" {
		attrs = append(attrs, attribute.String("server.address", v))
	}
	return attrs
}

func describe(attrs []attribute.KeyValue) string {
	if len(attrs) == 0 {
		return "no attributes"
	}
	var b strings.Builder
	for i, kv := range attrs {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s=%s", kv.Key, kv.Value.Emit())
	}
	return b.String()
}

func (e *Emitter) handleErrorRepeat(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	d, err := every(q, time.Second)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	attrs := errorAttrs(q)
	rep := &repeater{every: d, attrs: attrs, body: bodyOr(q, "operation failed")}

	e.mu.Lock()
	e.repeat(&e.errors, rep)
	e.mu.Unlock()

	done(w, r, "repeating a plain ERROR (%s) every %s", describe(attrs), d)
}

func (e *Emitter) handleErrorStop(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.errors == nil {
		fail(w, http.StatusNotFound, "no plain error is repeating")
		return
	}
	e.errors.cancel()
	n := e.errors.sent.Load()
	e.errors = nil
	done(w, r, "stopped the plain ERROR after %d records", n)
}

func (e *Emitter) handleErrorOnce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	attrs := errorAttrs(q)
	if !e.emit(log.SeverityError, bodyOr(q, "operation failed"), attrs...) {
		fail(w, http.StatusConflict, "silent: nothing sent")
		return
	}
	done(w, r, "sent one plain ERROR (%s)", describe(attrs))
}

func (e *Emitter) toggle(flag interface{ Store(bool) }, on bool, msg string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flag.Store(on)
		done(w, r, "%s", msg)
	}
}

func (e *Emitter) handleExit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	code := 1
	if v := q.Get("code"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 125 {
			fail(w, http.StatusBadRequest, "code: %q is 0 to 125", v)
			return
		}
		code = n
	}
	after := time.Second
	if v := q.Get("after"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			fail(w, http.StatusBadRequest, "after: %q is a duration", v)
			return
		}
		after = d
	}
	if !e.exit(code, after) {
		fail(w, http.StatusConflict, "already exiting")
		return
	}
	done(w, r, "exiting with code %d in %s, without flushing", code, after)
}

// attrsOf reads the attributes a metric series is told to carry:
// attr.NAME=VALUE, each a string.
func attrsOf(q url.Values) attribute.Set {
	var kvs []attribute.KeyValue
	for k, vs := range q {
		if name, ok := strings.CutPrefix(k, "attr."); ok && name != "" && len(vs) > 0 {
			kvs = append(kvs, attribute.String(name, vs[0]))
		}
	}
	return attribute.NewSet(kvs...)
}

func (e *Emitter) handleGauge(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	if name == "" {
		fail(w, http.StatusBadRequest, "name is required, e.g. battery.charge")
		return
	}
	v, err := strconv.ParseFloat(q.Get("value"), 64)
	if err != nil {
		fail(w, http.StatusBadRequest, "value: %q is a number", q.Get("value"))
		return
	}
	attrs := attrsOf(q)
	if err := e.setGauge(name, v, attrs); err != nil {
		fail(w, http.StatusBadRequest, "%s: %v", name, err)
		return
	}
	done(w, r, "gauge %s{%s} = %g", name, attrs.Encoded(attribute.DefaultEncoder()), v)
}

func (e *Emitter) handleGaugeStop(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if !e.stopGauge(name) {
		fail(w, http.StatusNotFound, "gauge %q has no series", name)
		return
	}
	done(w, r, "gauge %s stopped: no data points from now on", name)
}

func (e *Emitter) handleMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("stream")
	if name == "" {
		fail(w, http.StatusBadRequest, "stream is required")
		return
	}
	rate, err := strconv.ParseFloat(q.Get("rate"), 64)
	if err != nil || rate < 0 {
		fail(w, http.StatusBadRequest, "rate: %q is messages per second, 0 or more", q.Get("rate"))
		return
	}
	e.setRate(name, rate)
	done(w, r, "emitter.messages{stream=%s} growing by %g per second", name, rate)
}

func (e *Emitter) handleHardware(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, state := q.Get("id"), q.Get("state")
	if id == "" || state == "" {
		fail(w, http.StatusBadRequest, "id and state are required, e.g. id=disk0&state=degraded")
		return
	}
	typ := q.Get("type")
	if typ == "" {
		typ = "physical_disk"
	}

	e.mu.Lock()
	e.hardware[id] = hardware{typ: typ, state: state}
	e.mu.Unlock()
	done(w, r, "hw.status{hw.id=%s, hw.type=%s}: 1 on hw.state=%s", id, typ, state)
}

func (e *Emitter) handleHardwareStop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	e.mu.Lock()
	defer e.mu.Unlock()
	if id == "" {
		clear(e.hardware)
		done(w, r, "hw.status stopped for every device")
		return
	}
	if _, ok := e.hardware[id]; !ok {
		fail(w, http.StatusNotFound, "no device %q", id)
		return
	}
	delete(e.hardware, id)
	done(w, r, "hw.status stopped for %s", id)
}

func bodyOr(q url.Values, fallback string) string {
	if v := q.Get("msg"); v != "" {
		return v
	}
	return fallback
}
