// Package signal is the boundary between how signals arrive and what Ward does
// with them. A receiver turns what it received into OTel's data model and
// hands it to a Sink; the evaluation loop is one.
package signal

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Sink takes what a receiver received, in OTel's data model. It returns
// quickly: it takes the data in and evaluates it later, on the loop's tick.
// The data is the Sink's after the call; a receiver does not touch it again.
type Sink interface {
	ConsumeLogs(ctx context.Context, ld plog.Logs) error
	ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Canary is the service.name of the records Ward sends through each node's
// Collector to check that what the node sends arrives.
const Canary = "ward-canary"

// Attributes of a canary record.
const (
	CanaryRun  = "diag.canary.run"
	CanarySeq  = "diag.canary.seq"
	CanaryNode = "diag.canary.node"
)

// Canary names of the log record and the metric a canary is made of.
const (
	CanaryEvent  = "diag.canary"
	CanaryMetric = "diag.canary"
)

// Queue is a Sink that never makes its caller wait or fail: what it is handed
// goes into a bounded queue, which Run hands on to the next Sink in order. When
// the queue is full, what it is handed is dropped and counted; the canaries
// lost with it leave a gap Ward's node gate sees.
//
// It is for running Ward where an error would travel back to whoever sent the
// signal -- inside a Collector, where an exporter's error reaches the apps.
type Queue struct {
	next    Sink
	ch      chan any
	dropped atomic.Uint64
	taken   atomic.Uint64
}

// NewQueue answers with a Queue of size requests in front of next.
func NewQueue(next Sink, size int) *Queue {
	return &Queue{next: next, ch: make(chan any, size)}
}

func (q *Queue) put(x any) {
	select {
	case q.ch <- x:
		q.taken.Add(1)
	default:
		q.dropped.Add(1)
	}
}

// ConsumeLogs implements Sink. It returns nil, always.
func (q *Queue) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	q.put(ld)
	return nil
}

// ConsumeMetrics implements Sink. It returns nil, always.
func (q *Queue) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	q.put(md)
	return nil
}

// Run hands what is queued to the next Sink until ctx is done.
func (q *Queue) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case x := <-q.ch:
			switch x := x.(type) {
			case plog.Logs:
				q.next.ConsumeLogs(ctx, x)
			case pmetric.Metrics:
				q.next.ConsumeMetrics(ctx, x)
			}
		}
	}
}

// QueueStats are a Queue's counts.
type QueueStats struct {
	Queued, Size   int
	Taken, Dropped uint64
}

// Stats answers with the Queue's counts.
func (q *Queue) Stats() QueueStats {
	return QueueStats{Queued: len(q.ch), Size: cap(q.ch), Taken: q.taken.Load(), Dropped: q.dropped.Load()}
}
