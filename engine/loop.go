package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
)

// Engine is the evaluation loop (design §8.5). Every tick it drains what the
// Normalizer took in, judges every Condition, applies what operators asked
// for, and commits the tick's transitions in one transaction. It is the only
// writer of Ward's results.
type Engine struct {
	cfg conf.Ward
	now func() time.Time
	log *slog.Logger

	RunID   string
	Started time.Time

	Gate  *Gate
	Norm  *Normalizer
	Store *Store
	Feed  *Feed

	ev *Evaluator

	defs atomic.Pointer[Definitions]
	cmds chan *command

	snap atomic.Pointer[Snapshot]

	pending        Write
	waiting        []*command
	lastCheckpoint time.Time
	lastRetain     time.Time
	ops            atomic.Uint64

	overruns atomic.Int64
	ready    chan struct{}
}

// Options are what a test changes of an Engine.
type Options struct {
	Now func() time.Time
	Log *slog.Logger
}

// New answers with an Engine for the static configuration, writing to store.
func New(c conf.Ward, store *Store, o Options) *Engine {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	var b [8]byte
	rand.Read(b[:])

	nodes := slices.Sorted(maps.Keys(c.Nodes))
	matches := map[string]map[string]string{}
	for n, x := range c.Nodes {
		matches[n] = x.Match
	}

	e := &Engine{
		cfg:     c,
		now:     o.Now,
		log:     o.Log,
		RunID:   hex.EncodeToString(b[:]),
		Started: o.Now(),
		Store:   store,
		Feed:    &Feed{ch: make(chan struct{})},
		cmds:    make(chan *command, 64),
		ready:   make(chan struct{}),
	}
	e.Gate = NewGate(c.Engine.Gate, nodes)
	e.Norm = NewNormalizer(o.Now, matches, e.RunID, e.Gate, c.Engine.RecoveryWindow+time.Minute)
	e.ev = NewEvaluator(Config{
		LagLog:              c.Engine.EvalLag.Log,
		LagMetric:           c.Engine.EvalLag.Metric,
		UnobservableToStale: c.Engine.Gate.UnobservableToStale,
		KeysPerRule:         c.Engine.Limits.KeysPerRule,
		NewIssuesPerMinute:  c.Engine.Limits.NewIssuesPerRulePerMinute,
		MaxShelve:           c.Operator.MaxShelve,
		Nodes:               nodes,
	}, e.Gate)
	e.snap.Store(&Snapshot{})
	return e
}

// Apply puts a merged definition Set in effect from the next tick. The
// provider registry calls it.
func (e *Engine) Apply(s *def.Set) {
	d := Compile(s)
	e.defs.Store(d)
	e.Norm.Use(d)
}

// Ready is closed once the Engine has recovered what the Store had and runs.
func (e *Engine) Ready() <-chan struct{} { return e.ready }

// Overruns is how many ticks were skipped because one took longer than a
// tick.
func (e *Engine) Overruns() int64 { return e.overruns.Load() }

// Late is how many signals arrived after their time had been judged.
func (e *Engine) Late() int64 { return e.snap.Load().Late }

// Run recovers from the Store, then runs the loop until ctx is done.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.recover(ctx); err != nil {
		return err
	}
	close(e.ready)

	t := time.NewTicker(e.cfg.Engine.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			e.pending.Alive = e.now()
			e.pending.Run = e.RunID
			e.pending.Dirty = e.ev.dirty()
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_, err := e.Store.Commit(cctx, e.pending)
			cancel()
			for _, c := range e.waiting {
				c.reply <- reply{err: status.Error(codes.Unavailable, "Ward is stopping")}
			}
			return err
		case <-t.C:
			start := e.now()
			e.tick(ctx, start)
			if d := e.now().Sub(start); d > e.cfg.Engine.Tick {
				e.overruns.Add(int64(d / e.cfg.Engine.Tick))
			}
		}
	}
}

// recover reads back what the Store had (design §8.6, "재시작 복구").
func (e *Engine) recover(ctx context.Context) error {
	now := e.now()
	l, err := e.Store.Load(ctx, e.cfg.Engine.RecoveryWindow+time.Minute)
	if err != nil {
		return err
	}

	if !l.DateAlive.IsZero() && now.Sub(l.DateAlive) > e.cfg.Engine.RecoveryWindow && len(l.Open) > 0 {
		// Down longer than a Collector keeps retrying: what was sent meanwhile
		// is gone, so these cannot be followed on.
		e.log.WarnContext(ctx, "down longer than the recovery window; closing open issues",
			slog.Time("date_alive", l.DateAlive), slog.Int("issues", len(l.Open)))
		var w Write
		for _, i := range l.Open {
			i.State = StateClosed
			i.CloseReason = ReasonDiagGap
			i.DateClosed = i.DateSeen
			i.attend()
			w.Transitions = append(w.Transitions, Transition{Event: EventClose, Date: now, Issue: i})
		}
		evs, err := e.Store.Commit(ctx, w)
		if err != nil {
			return err
		}
		e.Feed.Publish(evs)
		l.Open = nil
	}

	e.ev.Seen(l.Seen)
	e.ev.Overrides(l.Overrides)
	if d := e.defs.Load(); d != nil {
		e.ev.Apply(d, now)
	}
	e.ev.now = now
	e.ev.Restore(l.Open)
	e.lastCheckpoint = now
	e.lastRetain = time.Time{}
	e.publish(now)
	return nil
}

func (e *Engine) tick(ctx context.Context, now time.Time) {
	if d := e.defs.Load(); d != nil && d != e.ev.defs {
		e.ev.Apply(d, now)
	}

	in := e.Norm.Drain()
	e.Gate.Update(now, now.Add(-e.cfg.Engine.EvalLag.Log), now.Add(-e.cfg.Engine.EvalLag.Metric))
	e.ev.Step(now, in)

	// What operators asked for since the last tick, in this tick's transaction.
	var cmds []*command
	for len(e.cmds) > 0 {
		c := <-e.cmds
		if c.ctx.Err() != nil {
			continue
		}
		c.run(e, now)
		cmds = append(cmds, c)
	}

	ts, ovs := e.ev.Take()
	e.pending.Transitions = append(e.pending.Transitions, ts...)
	e.pending.Overrides = append(e.pending.Overrides, ovs...)
	for _, c := range cmds {
		e.pending.Transitions = append(e.pending.Transitions, c.extra...)
		if c.err == nil {
			e.waiting = append(e.waiting, c)
		} else {
			c.reply <- reply{err: c.err}
		}
	}
	if now.Sub(e.lastCheckpoint) >= e.cfg.Engine.Checkpoint {
		e.pending.Alive = now
		e.pending.Run = e.RunID
		e.pending.Dirty = e.ev.dirty()
	}

	evs, err := e.Store.Commit(ctx, e.pending)
	if err != nil {
		e.log.ErrorContext(ctx, "commit a tick; trying again next tick", slog.Any("err", err))
	} else {
		if !e.pending.Alive.IsZero() {
			e.lastCheckpoint = now
		}
		e.answer(ctx, evs)
		e.pending = Write{}
		e.Feed.Publish(evs)
	}

	if now.Sub(e.lastRetain) >= time.Hour {
		e.lastRetain = now
		n, m, err := e.Store.Retain(ctx, now.Add(-e.cfg.Store.Retention))
		if err != nil {
			e.log.ErrorContext(ctx, "erase what is past retention", slog.Any("err", err))
		} else if n+m > 0 {
			e.log.InfoContext(ctx, "erased what is past retention", slog.Int("issues", n), slog.Int("events", m))
		}
	}

	e.publish(now)
}

// answer tells the operators whose requests just committed.
func (e *Engine) answer(ctx context.Context, evs []*app.IssueEvent) {
	byOp := map[uint64]*app.IssueEvent{}
	for i, t := range e.pending.Transitions {
		if t.Ctx == nil || i >= len(evs) {
			continue
		}
		if op, ok := t.Ctx.Value(opKey{}).(uint64); ok {
			byOp[op] = evs[i]
		}
	}
	for _, c := range e.waiting {
		r := reply{event: byOp[c.id], value: c.value}
		if c.issue != nil {
			v, err := e.Store.Ent.Issue.Get(ctx, uuid.UUID(*c.issue))
			if err != nil {
				r.err = err
			} else {
				r.value = v.Proto()
			}
		}
		c.reply <- r
	}
	e.waiting = nil
}

// dirty answers with the open Issues whose count, date_seen or restarts moved
// since they were last written, and marks them written.
func (ev *Evaluator) dirty() []Issue {
	var out []Issue
	for _, id := range slices.SortedFunc(maps.Keys(ev.issues), func(a, b [16]byte) int { return slices.Compare(a[:], b[:]) }) {
		c := ev.issues[id]
		if c.issue != nil && c.issue.dirty {
			out = append(out, *c.issue)
			c.issue.dirty = false
		}
	}
	return out
}

// -- operators --

type opKey struct{}

type command struct {
	id  uint64
	ctx context.Context
	do  func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error)

	value any
	issue *[16]byte
	extra []Transition
	err   error
	reply chan reply
}

type reply struct {
	value any
	event *app.IssueEvent
	err   error
}

func (c *command) run(e *Engine, now time.Time) {
	c.value, c.issue, c.extra, c.err = c.do(e, c.ctx, now)
}

// do hands an operator's request to the loop and waits until the tick that
// applies it has committed.
func (e *Engine) do(ctx context.Context, f func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error)) (reply, error) {
	select {
	case <-e.ready:
	default:
		return reply{}, status.Error(codes.Unavailable, "Ward is starting")
	}
	id := e.ops.Add(1)
	c := &command{id: id, ctx: context.WithValue(ctx, opKey{}, id), do: f, reply: make(chan reply, 1)}
	select {
	case e.cmds <- c:
	case <-ctx.Done():
		return reply{}, ctx.Err()
	}
	select {
	case r := <-c.reply:
		return r, r.err
	case <-ctx.Done():
		return reply{}, ctx.Err()
	}
}

// Acknowledge acknowledges an Issue, open or closed.
func (e *Engine) Acknowledge(ctx context.Context, id [16]byte, comment string) (*app.Issue, error) {
	r, err := e.do(ctx, func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error) {
		_, found, err := e.ev.Acknowledge(ctx, id, comment, now)
		if err != nil || found {
			return nil, &id, nil, err
		}
		i, err := e.Store.Closed(ctx, id)
		if err != nil {
			return nil, nil, nil, err
		}
		if i.Acked {
			return nil, nil, nil, status.Error(codes.FailedPrecondition, "already acknowledged")
		}
		// Closed: acknowledged, nobody has to look at it any more.
		i.Acked = true
		i.DateAcked = now
		i.Attention = false
		return nil, &id, []Transition{{Event: EventAck, Date: now, Comment: comment, Issue: i, Ctx: ctx}}, nil
	})
	if err != nil {
		return nil, err
	}
	return r.value.(*app.Issue), nil
}

// Comment notes something on an Issue, open or closed.
func (e *Engine) Comment(ctx context.Context, id [16]byte, comment string) (*app.IssueEvent, error) {
	r, err := e.do(ctx, func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error) {
		if _, found := e.ev.Comment(ctx, id, comment, now); found {
			return nil, nil, nil, nil
		}
		i, err := e.Store.Closed(ctx, id)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, nil, []Transition{{Event: EventComment, Date: now, Comment: comment, Issue: i, Ctx: ctx}}, nil
	})
	if err != nil {
		return nil, err
	}
	if r.event == nil {
		return nil, status.Error(codes.Internal, "the comment was committed without its event")
	}
	return r.event, nil
}

// Reset closes a cleared Issue of a latching rule.
func (e *Engine) Reset(ctx context.Context, id [16]byte, comment string) (*app.Issue, error) {
	r, err := e.do(ctx, func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error) {
		_, err := e.ev.Reset(ctx, id, comment, now)
		return nil, &id, nil, err
	})
	if err != nil {
		return nil, err
	}
	return r.value.(*app.Issue), nil
}

// Shelve shelves a Condition.
func (e *Engine) Shelve(ctx context.Context, ref ConditionRef, mode string, d time.Duration, comment string) (ConditionView, error) {
	return e.condition(ctx, ref, func(ev *Evaluator, ctx context.Context, now time.Time) error {
		return ev.Shelve(ctx, ref, mode, d, comment, now)
	})
}

// Unshelve takes a Condition's shelve away.
func (e *Engine) Unshelve(ctx context.Context, ref ConditionRef, comment string) (ConditionView, error) {
	return e.condition(ctx, ref, func(ev *Evaluator, ctx context.Context, _ time.Time) error {
		return ev.Unshelve(ctx, ref, comment)
	})
}

// SetOutOfService takes a Condition out of service or puts it back.
func (e *Engine) SetOutOfService(ctx context.Context, ref ConditionRef, oos bool, comment string) (ConditionView, error) {
	return e.condition(ctx, ref, func(ev *Evaluator, ctx context.Context, _ time.Time) error {
		return ev.SetOutOfService(ctx, ref, oos, comment)
	})
}

func (e *Engine) condition(ctx context.Context, ref ConditionRef, f func(*Evaluator, context.Context, time.Time) error) (ConditionView, error) {
	r, err := e.do(ctx, func(e *Engine, ctx context.Context, now time.Time) (any, *[16]byte, []Transition, error) {
		if err := f(e.ev, ctx, now); err != nil {
			return nil, nil, nil, err
		}
		return e.ev.view(condKey{ref.Rule, ref.Node, ref.Key}), nil, nil, nil
	})
	if err != nil {
		return ConditionView{}, err
	}
	return r.value.(ConditionView), nil
}

// -- the change feed --

// Feed tells followers of the change feed that transitions were committed.
type Feed struct {
	mu     sync.Mutex
	cursor int64
	ch     chan struct{}
}

// Publish says that evs were committed.
func (f *Feed) Publish(evs []*app.IssueEvent) {
	if len(evs) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursor = evs[len(evs)-1].GetCursor()
	close(f.ch)
	f.ch = make(chan struct{})
}

// Changed answers with a channel that is closed at the next commit.
func (f *Feed) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ch
}

// Follow sends every transition after `after`, in order, then each one as it
// is committed, until ctx is done or send fails (design §10.3).
func (e *Engine) Follow(ctx context.Context, after int64, send func(*app.IssueEvent) error) error {
	if after > 0 {
		first, err := e.Store.FirstCursor(ctx)
		if err != nil {
			return err
		}
		if first > 0 && after+1 < first {
			return status.Errorf(codes.OutOfRange, "cursor %d is past retention; the oldest kept is %d", after, first)
		}
	}
	for {
		changed := e.Feed.Changed()
		evs, err := e.Store.Events(ctx, after, 256)
		if err != nil {
			return err
		}
		for _, v := range evs {
			if err := send(v); err != nil {
				return err
			}
			after = v.GetCursor()
		}
		if len(evs) == 256 {
			continue
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil
		}
	}
}

// Search finds Issues by when they opened and closed.
func (e *Engine) Search(ctx context.Context, req *app.IssueSearchRequest) (*app.IssueListResponse, error) {
	return e.Store.Search(ctx, req)
}
