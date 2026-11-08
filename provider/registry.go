package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/otx/log"

	"github.com/lesomnus/ward/def"
)

// Status is what ProviderService reports of one provider.
type Status struct {
	Name, Type, Location string

	// Revision is the revision in effect, and Applied when it took effect.
	// Both are empty while nothing of the provider is in effect.
	Revision string
	Applied  time.Time

	// Errors say why the provider's last supply was refused; the revision in
	// effect stays. Refused is when, and RefusedRevision what was refused: it
	// is empty when there was nothing that could be read. All three are empty
	// when the last supply was accepted.
	Errors          []string
	Refused         time.Time
	RefusedRevision string

	// Warnings are about the provider's definitions in effect: things that
	// are allowed but probably not meant.
	Warnings []string
}

// Registry runs the providers and puts what they supply in effect.
//
// It keeps each provider's last accepted supply. A new one is merged with
// what every other provider has in effect: if def.Merge passes, the merged
// Set takes effect as a whole and is handed to apply; if not, the supply is
// refused, the provider's previous revision stays in effect, and the errors
// go in its Status. A supply that could not be read is refused the same way.
//
// A refused supply is tried again whenever what is in effect changes, so a
// change that had to wait for another -- a source removed while another
// provider's rule still reads it -- goes in once the other has, without
// being written again. What is in effect is what the providers supply, as
// soon as that agrees.
//
// Nothing is applied until every provider has sent its first supply. Then
// all of them are tried together, so that providers that refer to each
// other go in together; if that does not merge, they are tried one at a
// time, in the order they were given, until no more go in. A provider whose
// first supply is broken is left out -- nothing of it is applied -- and its
// errors are reported; the rest still apply, and Ward starts either way.
type Registry struct {
	nodes     []string
	providers []Provider
	apply     func(*def.Set, []string)
	index     map[string]int

	// Per provider, in the order given, owned by Run's goroutine: what is in
	// effect, and what was supplied but is not -- refused, or not tried yet.
	// nil for none.
	accepted []*def.Definitions
	pending  []*def.Definitions

	mu     sync.Mutex
	status []Status
	set    *def.Set
}

// NewRegistry makes a registry of providers. Their names are unique, as the
// static configuration's are; a name given twice panics. nodes are the node
// names of the static configuration, which def.Merge checks references
// against. apply is called with every merged Set that takes effect, and its
// warnings, from Run's goroutine; Run waits for it. It may be nil.
func NewRegistry(nodes []string, providers []Provider, apply func(*def.Set, []string)) *Registry {
	if apply == nil {
		apply = func(*def.Set, []string) {}
	}
	r := &Registry{
		nodes:     slices.Clone(nodes),
		providers: slices.Clone(providers),
		apply:     apply,
		index:     map[string]int{},
		accepted:  make([]*def.Definitions, len(providers)),
		pending:   make([]*def.Definitions, len(providers)),
		status:    make([]Status, len(providers)),
	}
	for i, p := range providers {
		if _, ok := r.index[p.Name()]; ok {
			panic(fmt.Sprintf("provider: %q is registered twice", p.Name()))
		}
		r.index[p.Name()] = i
		r.status[i] = Status{Name: p.Name(), Type: p.Type(), Location: p.Location()}
	}
	return r
}

// Status is every provider's, in the order they were given. It is safe to
// call at any time, from any goroutine.
func (r *Registry) Status() []Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Status, len(r.status))
	for i, s := range r.status {
		s.Errors = slices.Clone(s.Errors)
		s.Warnings = slices.Clone(s.Warnings)
		out[i] = s
	}
	return out
}

// Set is the Set in effect: the last one handed to apply, nil before the
// first. Nothing writes to a Set once it is in effect.
func (r *Registry) Set() *def.Set {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.set
}

// Run runs every provider until ctx is done, then returns nil once they have
// all stopped. It is called once.
//
// The first apply happens once every provider has sent its first update, even
// when none of them could be accepted: the Set is empty then. A provider
// that stops while ctx is not done keeps what it has in effect, and its
// Status says it stopped.
func (r *Registry) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	updates := make(chan Update)
	for _, p := range r.providers {
		wg.Go(func() { run(ctx, p, updates) })
	}

	started := make([]bool, len(r.providers))
	for n := 0; n < len(r.providers); {
		select {
		case <-ctx.Done():
			return nil
		case u := <-updates:
			i, _ := r.take(ctx, u)
			if i >= 0 && !started[i] {
				started[i] = true
				n++
			}
		}
	}
	r.settle(ctx, true)

	for {
		select {
		case <-ctx.Done():
			return nil
		case u := <-updates:
			if _, try := r.take(ctx, u); try {
				r.settle(ctx, false)
			}
		}
	}
}

// run runs p, and tells the registry if it stops before ctx is done.
func run(ctx context.Context, p Provider, out chan<- Update) {
	err := p.Run(ctx, out)
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = errors.New("returned while it was still wanted")
	}
	select {
	case out <- Update{Provider: p.Name(), Err: fmt.Errorf("stopped: %w", err)}:
	case <-ctx.Done():
	}
}

// take records an update, and says whose it is (-1 when it is nobody's) and
// whether it is a supply to try.
func (r *Registry) take(ctx context.Context, u Update) (int, bool) {
	i, ok := r.index[u.Provider]
	if !ok {
		log.From(ctx).WarnContext(ctx, "provider: an update from a provider that is not registered; ignored",
			slog.String("provider", u.Provider))
		return -1, false
	}

	// The latest supply is the one that counts; one refused before it is
	// not tried again.
	r.pending[i] = nil
	switch {
	case u.Err != nil:
		r.refuse(ctx, i, u.Revision, u.Err)
	case u.File == nil:
		r.refuse(ctx, i, u.Revision, errors.New("supplied nothing"))
	case u.Revision != "" && r.accepted[i] != nil && r.accepted[i].Revision == u.Revision:
		// What is in effect already, read again after a refusal: the refusal
		// is over, and nothing changes.
		r.restore(ctx, i)
	default:
		r.pending[i] = &def.Definitions{Provider: u.Provider, Revision: u.Revision, File: u.File}
		return i, true
	}
	return i, false
}

// settle puts in effect what of the pending supplies merges with what is in
// effect, and records why the rest do not. At start (first), the Set is
// applied even when nothing went in.
func (r *Registry) settle(ctx context.Context, first bool) {
	var (
		set   *def.Set
		warns []string
		in    []int
	)
	try := func(is ...int) error {
		s, w, err := r.merge(is...)
		if err != nil {
			return err
		}
		for _, i := range is {
			r.accepted[i], r.pending[i] = r.pending[i], nil
		}
		set, warns, in = s, w, append(in, is...)
		return nil
	}

	if is := r.waiting(); len(is) > 1 {
		_ = try(is...)
	}
	// One at a time until a whole round puts nothing in. The errors of that
	// round are each supply's against what ended up in effect, since nothing
	// went in during it.
	for {
		progress := false
		refused := map[int]error{}
		for _, i := range r.waiting() {
			if err := try(i); err != nil {
				refused[i] = err
			} else {
				progress = true
			}
		}
		if progress {
			continue
		}
		for _, i := range slices.Sorted(maps.Keys(refused)) {
			r.refuse(ctx, i, r.pending[i].Revision, refused[i])
		}
		break
	}

	if len(in) == 0 {
		if !first {
			return
		}
		// Nothing is in effect, so the Set is empty.
		var err error
		if set, warns, err = r.merge(); err != nil {
			log.From(ctx).ErrorContext(ctx, "provider: an empty Set does not merge", slog.Any("err", err))
			return
		}
	}

	now := time.Now()
	byOwner := owners(set, warns)
	r.mu.Lock()
	for _, i := range in {
		s := &r.status[i]
		s.Revision, s.Applied = r.accepted[i].Revision, now
		s.Errors, s.Refused, s.RefusedRevision = nil, time.Time{}, ""
	}
	for i, p := range r.providers {
		r.status[i].Warnings = byOwner[p.Name()]
	}
	r.set = set
	r.mu.Unlock()

	slices.Sort(in)
	for _, i := range in {
		log.From(ctx).InfoContext(ctx, "provider: applied",
			slog.String("provider", r.providers[i].Name()), slog.String("revision", r.accepted[i].Revision))
	}
	r.apply(set, warns)
}

// waiting is the providers with a pending supply, in the order given.
func (r *Registry) waiting() []int {
	var is []int
	for i, d := range r.pending {
		if d != nil {
			is = append(is, i)
		}
	}
	return is
}

// merge merges what is in effect with the pending supplies of is in place of
// those providers' own. The pending ones go last, so that a name defined
// twice is blamed on them: def.Merge refuses the second definition it meets.
func (r *Registry) merge(is ...int) (*def.Set, []string, error) {
	var ds []def.Definitions
	for i, d := range r.accepted {
		if d != nil && !slices.Contains(is, i) {
			ds = append(ds, own(*d))
		}
	}
	for _, i := range is {
		ds = append(ds, own(*r.pending[i]))
	}
	return def.Merge(r.nodes, ds...)
}

// refuse records why provider i's last supply is not in effect.
func (r *Registry) refuse(ctx context.Context, i int, revision string, err error) {
	errs := messages(err)

	r.mu.Lock()
	s := &r.status[i]
	again := !s.Refused.IsZero() && s.RefusedRevision == revision && slices.Equal(s.Errors, errs)
	if !again {
		s.Errors, s.Refused, s.RefusedRevision = errs, time.Now(), revision
	}
	in := s.Revision
	r.mu.Unlock()

	if again {
		return
	}
	log.From(ctx).WarnContext(ctx, "provider: refused; the revision in effect stays",
		slog.String("provider", r.providers[i].Name()), slog.String("revision", revision),
		slog.String("in_effect", in), slog.Any("err", err))
}

// restore records that provider i supplies what is in effect again.
func (r *Registry) restore(ctx context.Context, i int) {
	r.mu.Lock()
	s := &r.status[i]
	was := len(s.Errors) > 0
	s.Errors, s.Refused, s.RefusedRevision = nil, time.Time{}, ""
	in := s.Revision
	r.mu.Unlock()

	if was {
		log.From(ctx).InfoContext(ctx, "provider: supplies the revision in effect again",
			slog.String("provider", r.providers[i].Name()), slog.String("revision", in))
	}
}

// own copies the sources, rules and components of d for one merge.
//
// def.Merge writes who defined each into them, and a Set in effect holds
// them: the evaluation loop and the API read a rule's provider while the
// registry merges the next change. Merged as they are, an unchanged
// provider's would be written again under those readers -- with the same
// values, but a write all the same -- so each merge writes into copies of its
// own. What they point to is shared, and nothing writes to it.
func own(d def.Definitions) def.Definitions {
	f := d.File
	if f == nil {
		return d
	}

	c := &def.File{Version: f.Version}
	if f.Sources != nil {
		c.Sources = make(map[string]*def.Source, len(f.Sources))
		for k, v := range f.Sources {
			if v != nil {
				x := *v
				v = &x
			}
			c.Sources[k] = v
		}
	}
	if f.Rules != nil {
		c.Rules = make([]*def.Rule, 0, len(f.Rules))
		for _, v := range f.Rules {
			if v != nil {
				x := *v
				v = &x
			}
			c.Rules = append(c.Rules, v)
		}
	}
	if f.Components != nil {
		c.Components = make(map[string]*def.Component, len(f.Components))
		for k, v := range f.Components {
			if v != nil {
				x := *v
				v = &x
			}
			c.Components[k] = v
		}
	}

	d.File = c
	return d
}

// owners sorts a Set's warnings by the provider whose definitions each is
// about. def.Merge words a warning after what it is about --
// `rules.<id>.<field>: ...` -- and that is looked up in the Set. A warning
// about nothing found there is every provider's.
func owners(set *def.Set, warns []string) map[string][]string {
	out := map[string][]string{}
	for _, w := range warns {
		if p, ok := owner(set, w); ok {
			out[p] = append(out[p], w)
			continue
		}
		for p := range set.Revisions {
			out[p] = append(out[p], w)
		}
	}
	return out
}

func owner(set *def.Set, w string) (string, bool) {
	// Ids hold dots, so the longest that fits is the one meant.
	longest, who := -1, ""
	fits := func(prefix, name, provider string) {
		at := prefix + name
		if len(name) > longest && strings.HasPrefix(w, at) && len(w) > len(at) && (w[len(at)] == '.' || w[len(at)] == ':') {
			longest, who = len(name), provider
		}
	}
	for id, x := range set.Rules {
		fits("rules.", id, x.Provider)
	}
	for name, x := range set.Sources {
		fits("sources.", name, x.Provider)
	}
	for name, x := range set.Components {
		fits("components.", name, x.Provider)
	}
	return who, longest >= 0
}

// messages are err's parts, one for each error def.Parse or def.Merge joined.
func messages(err error) []string {
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		var ms []string
		for _, e := range j.Unwrap() {
			ms = append(ms, messages(e)...)
		}
		return ms
	}
	return []string{err.Error()}
}
