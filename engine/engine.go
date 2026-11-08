// Package engine is where Ward's evaluation loop lives. For now it holds what
// the loop's writes stand on: the mark that says a write is the loop's own, the
// recorder filter that keeps those writes off the trail, and the transaction a
// tick commits in.
//
// The loop is the only thing that writes Ward's results. It writes through the
// sink (under the Seal layer, which refuses the same writes to callers), one
// transaction per tick, and its writes are not put on payday's trail: an Issue
// transition is already recorded as an IssueEvent, and recording every tick
// twice is most of the table for nothing. A write made for a caller -- an
// operator acknowledging an Issue -- carries the caller's context instead, and
// is on the trail under the RPC that asked for it.
package engine

import (
	"context"

	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/protoc-gen-orm-ent/runtime/enttx"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/server/bare"
)

type markKey struct{}

// Mark says the writes made with ctx are the evaluation loop's own.
func Mark(ctx context.Context) context.Context {
	return context.WithValue(ctx, markKey{}, true)
}

// Marked reports whether ctx carries the evaluation loop's mark.
func Marked(ctx context.Context) bool {
	v, _ := ctx.Value(markKey{}).(bool)
	return v
}

// UnlessMarked is the trail's recorder with the evaluation loop's writes taken
// out. Every other write goes to the recorder it wraps.
//
// Answering nil means "nothing to record" and the write carries on; this is
// payday's way of keeping a class of machine writes off the trail (payday
// docs/guide/server.md, "Changing what the trail records").
type UnlessMarked struct{ bare.Recorder }

func (r UnlessMarked) Record(ctx context.Context, s bare.Server, c bare.Change) error {
	if Marked(ctx) {
		return nil
	}
	return r.Recorder.Record(ctx, s, c)
}

// Tick runs f as one evaluation tick: a transaction begun on drv, the sink
// rebound onto it, and the loop's mark on the context. f's writes commit
// together or not at all.
func Tick(ctx context.Context, drv dialect.Driver, sink app.Server, f func(ctx context.Context, s app.Server) error) error {
	ctx = Mark(ctx)

	txd, tx, err := dialect.BeginTx(ctx, drv)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	s, err := enttx.Rebind(sink, txd)
	if err != nil {
		return err
	}
	if err := f(ctx, s); err != nil {
		return err
	}

	return tx.Commit()
}
