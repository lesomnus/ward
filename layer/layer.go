// Package layer holds Ward's layers of the payday stack:
//
//	Gate → Audit → Seal → Ops → Sink
//
// Seal refuses the writes only the evaluation loop makes. Ops is where Ward's
// own RPCs are implemented. The evaluation loop writes through the sink, under
// both, so the Seal never sees it.
package layer

import (
	"context"

	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/protoc-gen-orm-ent/runtime/enttx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	app "github.com/lesomnus/ward"
)

// Seal refuses callers the generated writes of what only the evaluation loop
// writes. Patch and Apply are already closed at the transport by payday; Add
// and Erase are refused here, because the generated Gate lets them through for
// a global entity (ward#22).
type Seal struct{ app.Overlay }

func NewSeal(next app.Server) Seal { return Seal{app.NewOverlay(next)} }

func SealBuild() app.Builder {
	return app.BuilderFunc(func(next app.Server) (app.Server, error) { return NewSeal(next), nil })
}

func (s Seal) WithDriver(drv dialect.Driver) (app.Server, error) {
	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		return nil, err
	}
	return NewSeal(next), nil
}

var errWrittenByWard = status.Error(codes.PermissionDenied, "written by Ward's evaluation loop, not by callers")

type sealIssue struct {
	Seal
	app.IssueServiceServer
}

func (s Seal) Issue() app.IssueServiceServer { return sealIssue{s, s.Next().Issue()} }

func (sealIssue) Add(context.Context, *app.IssueAddRequest) (*app.Issue, error) {
	return nil, errWrittenByWard
}

func (sealIssue) Erase(context.Context, *app.IssueRef) (*app.IssueEraseResponse, error) {
	return nil, errWrittenByWard
}

type sealIssueEvent struct {
	Seal
	app.IssueEventServiceServer
}

func (s Seal) IssueEvent() app.IssueEventServiceServer {
	return sealIssueEvent{s, s.Next().IssueEvent()}
}

func (sealIssueEvent) Add(context.Context, *app.IssueEventAddRequest) (*app.IssueEvent, error) {
	return nil, errWrittenByWard
}

func (sealIssueEvent) Erase(context.Context, *app.IssueEventRef) (*app.IssueEventEraseResponse, error) {
	return nil, errWrittenByWard
}

type sealOverride struct {
	Seal
	app.OverrideServiceServer
}

func (s Seal) Override() app.OverrideServiceServer { return sealOverride{s, s.Next().Override()} }

func (sealOverride) Add(context.Context, *app.OverrideAddRequest) (*app.Override, error) {
	return nil, errWrittenByWard
}

func (sealOverride) Erase(context.Context, *app.OverrideRef) (*app.OverrideEraseResponse, error) {
	return nil, errWrittenByWard
}

type sealCheckpoint struct {
	Seal
	app.CheckpointServiceServer
}

func (s Seal) Checkpoint() app.CheckpointServiceServer {
	return sealCheckpoint{s, s.Next().Checkpoint()}
}

func (sealCheckpoint) Add(context.Context, *app.CheckpointAddRequest) (*app.Checkpoint, error) {
	return nil, errWrittenByWard
}

func (sealCheckpoint) Erase(context.Context, *app.CheckpointRef) (*app.CheckpointEraseResponse, error) {
	return nil, errWrittenByWard
}

// Ops is where Ward's own RPCs are implemented: those declared in
// proto/ext/ward/*_svc.ext.proto. It has none yet.
type Ops struct{ app.Overlay }

func NewOps(next app.Server) Ops { return Ops{app.NewOverlay(next)} }

func OpsBuild() app.Builder {
	return app.BuilderFunc(func(next app.Server) (app.Server, error) { return NewOps(next), nil })
}

func (s Ops) WithDriver(drv dialect.Driver) (app.Server, error) {
	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		return nil, err
	}
	return NewOps(next), nil
}

var (
	_ app.Server               = Seal{}
	_ enttx.Binder[app.Server] = Seal{}
	_ app.Server               = Ops{}
	_ enttx.Binder[app.Server] = Ops{}
)
