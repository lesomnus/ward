package engine

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/signal"
)

// Canary sends, every interval, one log record and one data point to each
// node's Collector, the way the node's services send theirs (design §7.2).
// What comes back through the node's diag pipeline, and how the Collector
// answered, is the evidence the gate judges by.
//
// Each is one export, tried once: a canary that needs a retry is the
// evidence.
type Canary struct {
	cfg   conf.Canary
	nodes map[string]string
	run   string
	gate  *Gate
	now   func() time.Time
	log   *slog.Logger
}

// NewCanary answers with the canary of an Engine.
func (e *Engine) NewCanary() *Canary {
	nodes := map[string]string{}
	for n, x := range e.cfg.Nodes {
		nodes[n] = x.Collector
	}
	return &Canary{cfg: e.cfg.Engine.Gate.Canary, nodes: nodes, run: e.RunID, gate: e.Gate, now: e.now, log: e.log}
}

type canaryClient struct {
	node string
	conn *grpc.ClientConn
	logs plogotlp.GRPCClient
	mets pmetricotlp.GRPCClient
}

// Run sends canaries until ctx is done.
func (c *Canary) Run(ctx context.Context) error {
	var clients []canaryClient
	for _, n := range slices.Sorted(maps.Keys(c.nodes)) {
		conn, err := grpc.NewClient(c.nodes[n], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		clients = append(clients, canaryClient{node: n, conn: conn, logs: plogotlp.NewGRPCClient(conn), mets: pmetricotlp.NewGRPCClient(conn)})
	}

	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	var (
		wg  sync.WaitGroup
		seq int64
	)
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		seq++
		at := c.now()
		for _, cl := range clients {
			c.gate.Sent(cl.node, seq, at)
			wg.Add(2)
			go func(cl canaryClient, seq int64) {
				defer wg.Done()
				c.send(ctx, cl, PathLog, seq, at)
			}(cl, seq)
			go func(cl canaryClient, seq int64) {
				defer wg.Done()
				c.send(ctx, cl, PathMetric, seq, at)
			}(cl, seq)
		}
	}
}

func (c *Canary) send(ctx context.Context, cl canaryClient, p Path, seq int64, at time.Time) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	start := time.Now()
	var err error
	if p == PathLog {
		_, err = cl.logs.Export(ctx, c.logs(cl.node, seq, at))
	} else {
		_, err = cl.mets.Export(ctx, c.metrics(cl.node, seq, at))
	}
	rtt := time.Since(start)
	if ctx.Err() != nil && err != nil && errorsIsCanceled(ctx) {
		return // Ward is stopping
	}

	reason, detail := "", ""
	if err != nil {
		reason, detail = classify(err)
	}
	c.gate.Answered(cl.node, p, seq, c.now(), rtt, reason, detail)
}

func errorsIsCanceled(ctx context.Context) bool { return context.Cause(ctx) == context.Canceled }

// classify tells a Collector that could not be reached from one that
// answered with an error.
func classify(err error) (string, string) {
	s, _ := status.FromError(err)
	switch s.Code() {
	case codes.DeadlineExceeded:
		return GateUnreachable, "timeout"
	case codes.Unavailable:
		m := s.Message()
		if strings.Contains(m, "connection") || strings.Contains(m, "dial") || strings.Contains(m, "no such host") {
			return GateUnreachable, m
		}
	}
	return GateError, s.Code().String()
}

func (c *Canary) attrs(m pcommon.Map, node string, seq int64) {
	m.PutStr(signal.CanaryRun, c.run)
	m.PutInt(signal.CanarySeq, seq)
	m.PutStr(signal.CanaryNode, node)
}

func (c *Canary) logs(node string, seq int64, at time.Time) plogotlp.ExportRequest {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", signal.Canary)
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(at))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(at))
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetEventName(signal.CanaryEvent)
	c.attrs(lr.Attributes(), node, seq)
	return plogotlp.NewExportRequestFromLogs(ld)
}

func (c *Canary) metrics(node string, seq int64, at time.Time) pmetricotlp.ExportRequest {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", signal.Canary)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(signal.CanaryMetric)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(at))
	dp.SetIntValue(seq)
	c.attrs(dp.Attributes(), node, seq)
	return pmetricotlp.NewExportRequestFromMetrics(md)
}
