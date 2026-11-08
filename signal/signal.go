// Package signal is the boundary between how signals arrive and what Ward does
// with them. A receiver turns what it received into OTel's data model and
// hands it to a Sink; the evaluation loop is one.
package signal

import (
	"context"

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
	CanaryRun = "diag.canary.run"
	CanarySeq = "diag.canary.seq"
)
