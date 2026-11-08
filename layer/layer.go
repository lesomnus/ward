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
	"sync/atomic"

	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/protoc-gen-orm-ent/runtime/enttx"
	"google.golang.org/grpc"
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
// proto/ext/ward/*_svc.ext.proto. They are answered by the evaluation loop and
// the Store, which serve builds after the stack: Ops reads them through a
// Backend that serve fills in.
type Ops struct {
	app.Overlay
	b *Backend
}

func NewOps(next app.Server, b *Backend) Ops { return Ops{app.NewOverlay(next), b} }

func OpsBuild(b *Backend) app.Builder {
	return app.BuilderFunc(func(next app.Server) (app.Server, error) { return NewOps(next, b), nil })
}

func (s Ops) WithDriver(drv dialect.Driver) (app.Server, error) {
	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		return nil, err
	}
	return NewOps(next, s.b), nil
}

// Operations are what Ops hands its RPCs to.
type Operations interface {
	Acknowledge(ctx context.Context, id [16]byte, comment string) (*app.Issue, error)
	Comment(ctx context.Context, id [16]byte, comment string) (*app.IssueEvent, error)
	Reset(ctx context.Context, id [16]byte, comment string) (*app.Issue, error)
	Search(ctx context.Context, req *app.IssueSearchRequest) (*app.IssueListResponse, error)
	Follow(ctx context.Context, after int64, send func(*app.IssueEvent) error) error
}

// Backend holds the Operations once there are some.
type Backend struct{ p atomic.Pointer[Operations] }

// Set makes o what Ops answers with.
func (b *Backend) Set(o Operations) { b.p.Store(&o) }

func (b *Backend) get() (Operations, error) {
	if b == nil {
		return nil, status.Error(codes.Unavailable, "Ward is starting")
	}
	o := b.p.Load()
	if o == nil {
		return nil, status.Error(codes.Unavailable, "Ward is starting")
	}
	return *o, nil
}

func issueID(ref *app.IssueRef) ([16]byte, error) {
	id := ref.GetId()
	if len(id) != 16 {
		return [16]byte{}, status.Error(codes.InvalidArgument, "ref.id: an Issue is named by its 16-byte id")
	}
	return [16]byte(id), nil
}

type opsIssue struct {
	Ops
	app.IssueServiceServer
}

func (s Ops) Issue() app.IssueServiceServer { return opsIssue{s, s.Next().Issue()} }

func (s opsIssue) Search(ctx context.Context, req *app.IssueSearchRequest) (*app.IssueListResponse, error) {
	o, err := s.b.get()
	if err != nil {
		return nil, err
	}
	return o.Search(ctx, req)
}

func (s opsIssue) Acknowledge(ctx context.Context, req *app.IssueAcknowledgeRequest) (*app.Issue, error) {
	o, err := s.b.get()
	if err != nil {
		return nil, err
	}
	id, err := issueID(req.GetRef())
	if err != nil {
		return nil, err
	}
	return o.Acknowledge(ctx, id, req.GetComment())
}

func (s opsIssue) Comment(ctx context.Context, req *app.IssueCommentRequest) (*app.IssueEvent, error) {
	o, err := s.b.get()
	if err != nil {
		return nil, err
	}
	id, err := issueID(req.GetRef())
	if err != nil {
		return nil, err
	}
	if req.GetComment() == "" {
		return nil, status.Error(codes.InvalidArgument, "comment: empty")
	}
	return o.Comment(ctx, id, req.GetComment())
}

func (s opsIssue) Reset(ctx context.Context, req *app.IssueResetRequest) (*app.Issue, error) {
	o, err := s.b.get()
	if err != nil {
		return nil, err
	}
	id, err := issueID(req.GetRef())
	if err != nil {
		return nil, err
	}
	return o.Reset(ctx, id, req.GetComment())
}

type opsIssueEvent struct {
	Ops
	app.IssueEventServiceServer
}

func (s Ops) IssueEvent() app.IssueEventServiceServer {
	return opsIssueEvent{s, s.Next().IssueEvent()}
}

func (s opsIssueEvent) Follow(req *app.IssueEventFollowRequest, out grpc.ServerStreamingServer[app.IssueEvent]) error {
	o, err := s.b.get()
	if err != nil {
		return err
	}
	return o.Follow(out.Context(), req.GetAfter(), out.Send)
}

var (
	_ app.Server               = Seal{}
	_ enttx.Binder[app.Server] = Seal{}
	_ app.Server               = Ops{}
	_ enttx.Binder[app.Server] = Ops{}
)
