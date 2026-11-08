// Package exporter sends what Ward records as OTel signals of its own (design
// §8.6, §8.7): each Issue transition it commits as a log record, and its own
// state as metrics, over OTLP to the configured endpoints. It is optional;
// Ward never reads them back. They carry service.name=ward, which a node's
// diag pipeline does not send to Ward again.
package exporter

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"
	"uuid"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/engine"
	"github.com/lesomnus/ward/receiver/otlp"
)

// Interval is how often Ward's metrics are exported.
const Interval = 10 * time.Second

// Exporter exports to every configured endpoint.
type Exporter struct {
	Engine   *engine.Engine
	Receiver func() otlp.Stats
	Configs  []conf.Exporter
	Log      *slog.Logger
}

type client struct {
	endpoint string
	logs     plogotlp.GRPCClient
	mets     pmetricotlp.GRPCClient
}

// Run exports until ctx is done: the transitions committed from when it
// starts, and the metrics every Interval.
func (x *Exporter) Run(ctx context.Context) error {
	if len(x.Configs) == 0 {
		<-ctx.Done()
		return nil
	}
	if x.Log == nil {
		x.Log = slog.Default()
	}
	var cs []client
	for _, c := range x.Configs {
		creds := credentials.NewTLS(&tls.Config{})
		if c.Insecure {
			creds = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(c.Endpoint, grpc.WithTransportCredentials(creds))
		if err != nil {
			return fmt.Errorf("exporter %s: %w", c.Endpoint, err)
		}
		defer conn.Close()
		cs = append(cs, client{endpoint: c.Endpoint, logs: plogotlp.NewGRPCClient(conn), mets: pmetricotlp.NewGRPCClient(conn)})
	}

	select {
	case <-x.Engine.Ready():
	case <-ctx.Done():
		return nil
	}
	from, err := x.Engine.Store.LastCursor(ctx)
	if err != nil {
		return err
	}

	go func() {
		t := time.NewTicker(Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			req := pmetricotlp.NewExportRequestFromMetrics(x.metrics(time.Now()))
			for _, c := range cs {
				if _, err := c.mets.Export(ctx, req); err != nil && ctx.Err() == nil {
					x.Log.WarnContext(ctx, "export metrics", slog.String("endpoint", c.endpoint), slog.Any("err", err))
				}
			}
		}
	}()

	return x.Engine.Follow(ctx, from, func(v *app.IssueEvent) error {
		req := plogotlp.NewExportRequestFromLogs(x.record(v))
		for _, c := range cs {
			if _, err := c.logs.Export(ctx, req); err != nil && ctx.Err() == nil {
				// Not retried: the Store has it, and Follow can replay it.
				x.Log.WarnContext(ctx, "export an issue transition", slog.String("endpoint", c.endpoint), slog.Int64("cursor", v.GetCursor()), slog.Any("err", err))
			}
		}
		return nil
	})
}

func (x *Exporter) resource(r pcommon.Resource) {
	r.Attributes().PutStr("service.name", "ward")
	r.Attributes().PutStr("service.instance.id", x.Engine.RunID)
}

// record is a transition as a log record (design §8.6).
func (x *Exporter) record(v *app.IssueEvent) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	x.resource(rl.Resource())
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/lesomnus/ward")
	lr := sl.LogRecords().AppendEmpty()

	lr.SetTimestamp(pcommon.NewTimestampFromTime(v.GetDateOccurred().AsTime()))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	switch v.GetEvent() {
	case engine.EventOpen, engine.EventUpdate:
		lr.SetSeverityNumber(plog.SeverityNumberError)
		lr.SetSeverityText("ERROR")
	default:
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
	}
	lr.SetEventName("diag.issue." + v.GetEvent())
	lr.Body().SetStr(fmt.Sprintf("[%s] %s @ %s (key=%s)", v.GetEvent(), v.GetRule(), v.GetNode(), v.GetKey()))

	a := lr.Attributes()
	a.PutStr("diag.issue.id", uuid.UUID(v.GetIssueId()).String())
	a.PutInt("diag.issue.cursor", v.GetCursor())
	a.PutStr("diag.issue.event", v.GetEvent())
	a.PutStr("diag.issue.reason", v.GetCloseReason())
	a.PutStr("diag.issue.state", v.GetState())
	a.PutStr("diag.rule.id", v.GetRule())
	a.PutStr("diag.issue.source", v.GetSource())
	a.PutStr("diag.issue.node", v.GetNode())
	a.PutStr("diag.issue.key", v.GetKey())
	a.PutStr("diag.issue.severity", v.GetSeverity())
	a.PutStr("diag.issue.level", v.GetLevel())
	a.PutInt("diag.issue.date_opened", v.GetDateOpened().AsTime().UnixNano())
	a.PutInt("diag.issue.date_seen", v.GetDateSeen().AsTime().UnixNano())
	a.PutInt("diag.issue.count", v.GetCount())
	a.PutInt("diag.issue.restarts", v.GetRestarts())
	a.PutBool("diag.issue.attention", v.GetAttention())
	if v.GetComment() != "" {
		a.PutStr("diag.issue.comment", v.GetComment())
	}
	return ld
}

// metrics are Ward's own (design §8.7).
func (x *Exporter) metrics(now time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	x.resource(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/lesomnus/ward")
	ms := sm.Metrics()
	at := pcommon.NewTimestampFromTime(now)
	start := pcommon.NewTimestampFromTime(x.Engine.Started)

	counter := func(name, unit string) pmetric.NumberDataPointSlice {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		return s.DataPoints()
	}
	gauge := func(name, unit string) pmetric.NumberDataPointSlice {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		return m.SetEmptyGauge().DataPoints()
	}
	point := func(dps pmetric.NumberDataPointSlice, v int64, attrs ...string) {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(at)
		dp.SetIntValue(v)
		for i := 0; i+1 < len(attrs); i += 2 {
			dp.Attributes().PutStr(attrs[i], attrs[i+1])
		}
	}

	if x.Receiver != nil {
		r := x.Receiver()
		reqs := counter("diag.receiver.requests", "{request}")
		point(reqs, int64(r.Accepted), "receiver", "otlp", "result", "accepted")
		point(reqs, int64(r.RefusedFull), "receiver", "otlp", "result", "refused_full")
		point(reqs, int64(r.RefusedTooLarge), "receiver", "otlp", "result", "refused_too_large")
		point(reqs, int64(r.RefusedBad), "receiver", "otlp", "result", "refused_bad")
		point(gauge("diag.receiver.queue.size", "{request}"), int64(r.Queued), "receiver", "otlp")
	}
	point(counter("diag.tick.overrun", "{tick}"), x.Engine.Overruns())
	point(gauge("diag.tick.duration", "us"), x.Engine.LastTick().Microseconds())

	snap := x.Engine.Snapshot()
	point(counter("diag.signals.late", "{signal}"), snap.Late)

	gate := gauge("diag.gate", "1")
	rtt := gauge("diag.canary.rtt", "us")
	for _, n := range snap.Nodes {
		v := int64(0)
		if n.Gate.Observable {
			v = 1
		}
		point(gate, v, "node", n.Name)
		point(rtt, n.Gate.RTT.Microseconds(), "node", n.Name)
	}

	states := map[string]int64{}
	for _, c := range snap.Conditions {
		states[c.State]++
	}
	conds := gauge("diag.conditions", "{condition}")
	for _, s := range []string{engine.StateInactive, engine.StatePending, engine.StateActive, engine.StateStale, engine.StateCleared} {
		point(conds, states[s], "state", s)
	}
	return md
}
