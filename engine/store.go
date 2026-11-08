package engine

import (
	"context"
	"fmt"
	"maps"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/protobuf-orm/ent/dialect"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/internal/ent"
	"github.com/lesomnus/ward/internal/ent/issue"
	"github.com/lesomnus/ward/internal/ent/issueevent"
)

// Store is where the loop keeps what outlives a run of Ward (design §8.6):
// Issues and their transitions, the operators' Overrides, and when Ward was
// last alive. The loop is its only writer. It writes through the sink, one
// transaction a tick; it reads with the ent client.
type Store struct {
	Ent  *ent.Client
	Drv  dialect.Driver
	Sink app.Server

	// The version of every Issue row the loop may patch: a generated Patch
	// takes the row's date_updated.
	versions map[[16]byte]*timestamppb.Timestamp

	cursor int64

	checkpoint  []byte
	checkpointV *timestamppb.Timestamp

	overrides map[overrideKey][]byte
}

type overrideKey struct {
	rule, node, key string
	shelve          bool
}

// Loaded is what a Ward that starts reads back.
type Loaded struct {
	Open      []Issue
	DateAlive time.Time
	Run       string
	Overrides []OverrideChange

	// Issues closed lately, which a Collector's retries may open again.
	Seen [][16]byte
}

// Load reads what the loop needs to carry on. recent is how far back closed
// Issues are remembered, so that signals sent again do not reopen them.
func (s *Store) Load(ctx context.Context, recent time.Duration) (Loaded, error) {
	var out Loaded
	s.versions = map[[16]byte]*timestamppb.Timestamp{}
	s.overrides = map[overrideKey][]byte{}

	open, err := s.Ent.Issue.Query().Where(issue.StateNEQ(StateClosed)).All(ctx)
	if err != nil {
		return out, fmt.Errorf("open issues: %w", err)
	}
	for _, v := range open {
		i := fromPb(v.Proto())
		out.Open = append(out.Open, i)
		s.versions[i.ID] = timestamppb.New(v.DateUpdated)
	}

	last, err := s.Ent.IssueEvent.Query().Order(ent.Desc(issueevent.FieldCursor)).First(ctx)
	switch {
	case ent.IsNotFound(err):
	case err != nil:
		return out, fmt.Errorf("last cursor: %w", err)
	default:
		s.cursor = last.Cursor
	}

	cp, err := s.Ent.Checkpoint.Query().First(ctx)
	switch {
	case ent.IsNotFound(err):
	case err != nil:
		return out, fmt.Errorf("checkpoint: %w", err)
	default:
		s.checkpoint = cp.Id[:]
		s.checkpointV = timestamppb.New(cp.DateUpdated)
		out.DateAlive = cp.DateAlive
		out.Run = cp.Run
	}

	if !out.DateAlive.IsZero() {
		closed, err := s.Ent.Issue.Query().
			Where(issue.StateEQ(StateClosed), issue.DateClosedGTE(out.DateAlive.Add(-recent))).
			All(ctx)
		if err != nil {
			return out, fmt.Errorf("recent issues: %w", err)
		}
		for _, v := range closed {
			out.Seen = append(out.Seen, [16]byte(v.Id))
		}
	}

	ovs, err := s.Ent.Override.Query().All(ctx)
	if err != nil {
		return out, fmt.Errorf("overrides: %w", err)
	}
	for _, v := range ovs {
		k := overrideKey{v.Rule, v.Node, v.Key, v.Kind != OverrideOutOfService}
		s.overrides[k] = v.Id[:]
		o := OverrideChange{Rule: v.Rule, Node: v.Node, Key: v.Key, Kind: v.Kind, Comment: v.Comment}
		if v.DateUnshelve != nil {
			o.Until = *v.DateUnshelve
		}
		out.Overrides = append(out.Overrides, o)
	}
	return out, nil
}

// Write is one tick's writes.
type Write struct {
	Transitions []Transition
	Overrides   []OverrideChange

	// Open Issues whose count, date_seen or restarts moved.
	Dirty []Issue

	// When set, Ward was alive then.
	Alive time.Time
	Run   string
}

func (w Write) empty() bool {
	return len(w.Transitions) == 0 && len(w.Overrides) == 0 && len(w.Dirty) == 0 && w.Alive.IsZero()
}

// Commit writes a tick in one transaction. Transitions get their cursors in
// order. Nothing is kept of a transaction that failed: the loop tries the same
// writes again next tick.
func (s *Store) Commit(ctx context.Context, w Write) ([]*app.IssueEvent, error) {
	if w.empty() {
		return nil, nil
	}
	versions := map[[16]byte]*timestamppb.Timestamp{}
	version := func(id [16]byte) *timestamppb.Timestamp {
		if v, ok := versions[id]; ok {
			return v
		}
		return s.versions[id]
	}
	cursor := s.cursor
	overrides := maps.Clone(s.overrides)
	var (
		events      []*app.IssueEvent
		checkpoint  = s.checkpoint
		checkpointV = s.checkpointV
	)

	err := Tick(ctx, s.Drv, s.Sink, func(ctx context.Context, x app.Server) error {
		for _, t := range w.Transitions {
			wctx := ctx
			if t.Ctx != nil {
				// The operator's request: its write is on the trail under its
				// RPC. Not cancelled with it -- the tick commits as a whole.
				wctx = context.WithoutCancel(t.Ctx)
			}
			i := t.Issue
			switch {
			case t.Event == EventComment:
			case t.Event == EventOpen && version(i.ID) == nil:
				v, err := x.Issue().Add(wctx, addReq(i))
				if status.Code(err) == codes.AlreadyExists {
					v, err = s.patchFresh(wctx, x, i)
				}
				if err != nil {
					return fmt.Errorf("add issue %s: %w", uuid.UUID(i.ID), err)
				}
				versions[i.ID] = v.GetDateUpdated()
			default:
				ver := version(i.ID)
				var (
					v   *app.Issue
					err error
				)
				if ver == nil {
					v, err = s.patchFresh(wctx, x, i)
				} else {
					v, err = x.Issue().Patch(wctx, patchReq(i, ver))
				}
				if err != nil {
					return fmt.Errorf("patch issue %s: %w", uuid.UUID(i.ID), err)
				}
				versions[i.ID] = v.GetDateUpdated()
			}

			cursor++
			e, err := x.IssueEvent().Add(wctx, eventReq(cursor, t))
			if err != nil {
				return fmt.Errorf("add issue event: %w", err)
			}
			events = append(events, e)
		}

		for _, d := range w.Dirty {
			ver := version(d.ID)
			if ver == nil {
				continue
			}
			v, err := x.Issue().Patch(ctx, app.IssuePatchRequest_builder{
				Ref:         issueRef(d.ID),
				DateUpdated: ver,
				DateSeen:    ts(d.DateSeen),
				Count:       &d.Count,
				Restarts:    &d.Restarts,
			}.Build())
			if err != nil {
				return fmt.Errorf("patch issue %s: %w", uuid.UUID(d.ID), err)
			}
			versions[d.ID] = v.GetDateUpdated()
		}

		for _, o := range w.Overrides {
			wctx := ctx
			if o.Ctx != nil {
				wctx = context.WithoutCancel(o.Ctx)
			}
			k := overrideKey{o.Rule, o.Node, o.Key, o.Kind != OverrideOutOfService}
			if id, ok := overrides[k]; ok {
				if _, err := x.Override().Erase(wctx, app.OverrideRef_builder{Id: id}.Build()); err != nil && status.Code(err) != codes.NotFound {
					return fmt.Errorf("erase override: %w", err)
				}
				delete(overrides, k)
			}
			if o.Delete {
				continue
			}
			v, err := x.Override().Add(wctx, app.OverrideAddRequest_builder{
				Rule: o.Rule, Node: o.Node, Key: o.Key, Kind: o.Kind,
				DateUnshelve: tsOrNil(o.Until), Comment: o.Comment,
			}.Build())
			if err != nil {
				return fmt.Errorf("add override: %w", err)
			}
			overrides[k] = v.GetId()
		}

		if !w.Alive.IsZero() {
			if checkpoint == nil {
				v, err := x.Checkpoint().Add(ctx, app.CheckpointAddRequest_builder{DateAlive: ts(w.Alive), Run: w.Run}.Build())
				if err != nil {
					return fmt.Errorf("add checkpoint: %w", err)
				}
				checkpoint, checkpointV = v.GetId(), v.GetDateUpdated()
			} else {
				v, err := x.Checkpoint().Patch(ctx, app.CheckpointPatchRequest_builder{
					Ref:         app.CheckpointRef_builder{Id: checkpoint}.Build(),
					DateUpdated: checkpointV,
					DateAlive:   ts(w.Alive),
					Run:         &w.Run,
				}.Build())
				if err != nil {
					return fmt.Errorf("patch checkpoint: %w", err)
				}
				checkpointV = v.GetDateUpdated()
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Committed: what the transaction wrote is now so.
	for id, v := range versions {
		s.versions[id] = v
	}
	for _, t := range w.Transitions {
		if t.Issue.State == StateClosed {
			delete(s.versions, t.Issue.ID)
		}
	}
	s.cursor = cursor
	s.overrides = overrides
	s.checkpoint, s.checkpointV = checkpoint, checkpointV
	return events, nil
}

// patchFresh patches an Issue whose version the loop does not have: one
// closed before, or one the Store had when it was opened again.
func (s *Store) patchFresh(ctx context.Context, x app.Server, i Issue) (*app.Issue, error) {
	all := true
	v, err := x.Issue().Get(ctx, app.IssueGetRequest_builder{
		Ref:    issueRef(i.ID),
		Select: app.IssueSelect_builder{All: &all}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	return x.Issue().Patch(ctx, patchReq(i, v.GetDateUpdated()))
}

// Closed reads an Issue that is not open, for an operator's request.
func (s *Store) Closed(ctx context.Context, id [16]byte) (Issue, error) {
	v, err := s.Ent.Issue.Get(ctx, uuid.UUID(id))
	if err != nil {
		if ent.IsNotFound(err) {
			return Issue{}, status.Error(codes.NotFound, "Issue not found")
		}
		return Issue{}, err
	}
	return fromPb(v.Proto()), nil
}

// Retain erases closed Issues that closed before the cutoff and that nobody
// has to look at any more, and the transitions of Issues that are gone.
func (s *Store) Retain(ctx context.Context, cutoff time.Time) (int, int, error) {
	n, err := s.Ent.Issue.Delete().
		Where(issue.StateEQ(StateClosed), issue.DateClosedLT(cutoff), issue.AttentionEQ(false)).
		Exec(ctx)
	if err != nil {
		return 0, 0, err
	}
	kept, err := s.Ent.Issue.Query().Ids(ctx)
	if err != nil {
		return n, 0, err
	}
	ids := make([][]byte, len(kept))
	for i, id := range kept {
		ids[i] = id[:]
	}
	m, err := s.Ent.IssueEvent.Delete().
		Where(issueevent.DateCreatedLT(cutoff), issueevent.IssueIdNotIn(ids...)).
		Exec(ctx)
	return n, m, err
}

// Events reads the transitions after a cursor, in order.
func (s *Store) Events(ctx context.Context, after int64, limit int) ([]*app.IssueEvent, error) {
	vs, err := s.Ent.IssueEvent.Query().
		Where(issueevent.CursorGT(after)).
		Order(ent.Asc(issueevent.FieldCursor)).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*app.IssueEvent, len(vs))
	for i, v := range vs {
		out[i] = v.Proto()
	}
	return out, nil
}

// FirstCursor is the oldest transition the Store still has; 0 if none.
func (s *Store) FirstCursor(ctx context.Context) (int64, error) {
	v, err := s.Ent.IssueEvent.Query().Order(ent.Asc(issueevent.FieldCursor)).First(ctx)
	if ent.IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v.Cursor, nil
}

func issueRef(id [16]byte) *app.IssueRef {
	return app.IssueRef_builder{Id: id[:]}.Build()
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func tsOrNil(t time.Time) *timestamppb.Timestamp { return ts(t) }

func addReq(i Issue) *app.IssueAddRequest {
	return app.IssueAddRequest_builder{
		Id: i.ID[:], Rule: i.Rule, Source: i.Source, Node: i.Node, Key: i.Key,
		Severity: i.Severity, State: i.State, Level: i.Level,
		DateOpened: ts(i.DateOpened), DateSeen: ts(i.DateSeen),
		DateCleared: ts(i.DateCleared), DateClosed: ts(i.DateClosed),
		CloseReason: i.CloseReason, Count: i.Count, Restarts: i.Restarts,
		Attention: i.Attention, Acked: i.Acked,
		DateAcked: ts(i.DateAcked), DateReset: ts(i.DateReset),
		Suppressed: i.Suppressed, Shelved: i.Shelved, OutOfService: i.OutOfService,
	}.Build()
}

func patchReq(i Issue, version *timestamppb.Timestamp) *app.IssuePatchRequest {
	b := app.IssuePatchRequest_builder{
		Ref: issueRef(i.ID), DateUpdated: version,
		Severity: &i.Severity, State: &i.State, Level: &i.Level,
		DateSeen: ts(i.DateSeen), CloseReason: &i.CloseReason,
		Count: &i.Count, Restarts: &i.Restarts,
		Attention: &i.Attention, Acked: &i.Acked,
		Suppressed: &i.Suppressed, Shelved: &i.Shelved, OutOfService: &i.OutOfService,
	}
	null := true
	set := func(t time.Time, v **timestamppb.Timestamp, n **bool) {
		if t.IsZero() {
			*n = &null
		} else {
			*v = ts(t)
		}
	}
	set(i.DateCleared, &b.DateCleared, &b.DateClearedNull)
	set(i.DateClosed, &b.DateClosed, &b.DateClosedNull)
	set(i.DateAcked, &b.DateAcked, &b.DateAckedNull)
	set(i.DateReset, &b.DateReset, &b.DateResetNull)
	return b.Build()
}

func eventReq(cursor int64, t Transition) *app.IssueEventAddRequest {
	i := t.Issue
	return app.IssueEventAddRequest_builder{
		Cursor: cursor, IssueId: i.ID[:], Event: t.Event,
		DateOccurred: ts(t.Date), Comment: t.Comment,
		Rule: i.Rule, Source: i.Source, Node: i.Node, Key: i.Key,
		Severity: i.Severity, Level: i.Level, State: i.State,
		DateOpened: ts(i.DateOpened), DateSeen: ts(i.DateSeen), DateClosed: ts(i.DateClosed),
		CloseReason: i.CloseReason, Count: i.Count, Restarts: i.Restarts,
		Attention: i.Attention, Acked: i.Acked,
		Suppressed: i.Suppressed, Shelved: i.Shelved, OutOfService: i.OutOfService,
	}.Build()
}

func fromPb(v *app.Issue) Issue {
	t := func(x *timestamppb.Timestamp) time.Time {
		if x == nil {
			return time.Time{}
		}
		return x.AsTime()
	}
	return Issue{
		ID: [16]byte(v.GetId()), Rule: v.GetRule(), Source: v.GetSource(), Node: v.GetNode(), Key: v.GetKey(),
		Severity: v.GetSeverity(), Level: v.GetLevel(), State: v.GetState(),
		DateOpened: t(v.GetDateOpened()), DateSeen: t(v.GetDateSeen()),
		DateCleared: t(v.GetDateCleared()), DateClosed: t(v.GetDateClosed()),
		CloseReason: v.GetCloseReason(), Count: v.GetCount(), Restarts: v.GetRestarts(),
		Attention: v.GetAttention(), Acked: v.GetAcked(),
		DateAcked: t(v.GetDateAcked()), DateReset: t(v.GetDateReset()),
		Suppressed: v.GetSuppressed(), Shelved: v.GetShelved(), OutOfService: v.GetOutOfService(),
	}
}

// Search finds Issues by when they opened and closed (IssueService/Search),
// in the order they opened. after is the last Issue of the previous page.
func (s *Store) Search(ctx context.Context, req *app.IssueSearchRequest) (*app.IssueListResponse, error) {
	q := s.Ent.Issue.Query()
	if req.HasDateOpenedFrom() {
		q.Where(issue.DateOpenedGTE(req.GetDateOpenedFrom().AsTime()))
	}
	if req.HasDateOpenedTo() {
		q.Where(issue.DateOpenedLT(req.GetDateOpenedTo().AsTime()))
	}
	if req.HasDateClosedFrom() {
		q.Where(issue.DateClosedGTE(req.GetDateClosedFrom().AsTime()))
	}
	if req.HasDateClosedTo() {
		q.Where(issue.DateClosedLT(req.GetDateClosedTo().AsTime()))
	}
	if st := req.GetState(); st != "" {
		q.Where(issue.StateEQ(st))
	}
	if a := req.GetAfter(); a != "" {
		at, id, err := parseAfter(a)
		if err != nil {
			return nil, err
		}
		q.Where(issue.Or(
			issue.DateOpenedGT(at),
			issue.And(issue.DateOpenedEQ(at), issue.IdGT(uuid.UUID(id))),
		))
	}
	size := int(req.GetSize())
	switch {
	case size <= 0:
		size = 50
	case size > 500:
		size = 500
	}
	vs, err := q.Order(ent.Asc(issue.FieldDateOpened), ent.Asc(issue.FieldId)).Limit(size + 1).All(ctx)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(vs) > size {
		vs = vs[:size]
		last := vs[len(vs)-1]
		next = fmt.Sprintf("%d.%x", last.DateOpened.UnixNano(), last.Id[:])
	}
	items := make([]*app.Issue, len(vs))
	for i, v := range vs {
		items[i] = v.Proto()
	}
	return app.IssueListResponse_builder{Items: items, Next: next}.Build(), nil
}

func parseAfter(a string) (time.Time, [16]byte, error) {
	var (
		ns int64
		id [16]byte
		b  []byte
	)
	if _, err := fmt.Sscanf(a, "%d.%x", &ns, &b); err != nil || len(b) != 16 {
		return time.Time{}, id, status.Error(codes.InvalidArgument, "after: not a page this answered")
	}
	copy(id[:], b)
	return time.Unix(0, ns), id, nil
}

// LastCursor is the newest transition the Store has; 0 if none.
func (s *Store) LastCursor(ctx context.Context) (int64, error) {
	v, err := s.Ent.IssueEvent.Query().Order(ent.Desc(issueevent.FieldCursor)).First(ctx)
	if ent.IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v.Cursor, nil
}
