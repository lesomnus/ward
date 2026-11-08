// Package api is Ward's services that no schema states: the ones that answer
// from what the evaluation loop holds in memory (design §10.2) -- Conditions,
// Sources, Rules, Providers, Nodes, Ward's own status and the Component tree.
// Issues and their transitions are payday's services, under layer.Ops.
package api

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	app "github.com/lesomnus/ward"
	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/engine"
	"github.com/lesomnus/ward/provider"
	"github.com/lesomnus/ward/receiver/otlp"
)

// Services answer from the evaluation loop, the provider registry and the
// receiver.
type Services struct {
	Engine    *engine.Engine
	Providers func() []provider.Status
	Receiver  func() otlp.Stats
}

// Register registers every service on g.
func (s *Services) Register(g *grpc.Server) {
	app.RegisterConditionServiceServer(g, conditions{s: s})
	app.RegisterSourceServiceServer(g, sources{s: s})
	app.RegisterRuleServiceServer(g, rules{s: s})
	app.RegisterProviderServiceServer(g, providers{s: s})
	app.RegisterNodeServiceServer(g, nodes{s: s})
	app.RegisterStatusServiceServer(g, statuses{s: s})
	app.RegisterComponentServiceServer(g, components{s: s})
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// page cuts a sorted list after the item named `after`, to `size` items.
func page[T any](items []T, name func(T) string, size int32, after string) ([]T, string) {
	if after != "" {
		i := sort.Search(len(items), func(i int) bool { return name(items[i]) > after })
		items = items[i:]
	}
	n := int(size)
	switch {
	case n <= 0:
		n = 100
	case n > 1000:
		n = 1000
	}
	if len(items) <= n {
		return items, ""
	}
	return items[:n], name(items[n-1])
}

// -- ConditionService --

type conditions struct {
	app.UnimplementedConditionServiceServer
	s *Services
}

func condName(v engine.ConditionView) string { return v.Rule + "\x00" + v.Node + "\x00" + v.Key }

func toCondition(v engine.ConditionView) *app.Condition {
	b := app.Condition_builder{
		Rule: v.Rule, Node: v.Node, Key: v.Key,
		State: v.State, Level: v.Level, Severity: v.Severity,
		Held: v.Held, HoldReason: v.HoldReason,
		Suppressed: v.Suppressed, Shelved: v.Shelved, OutOfService: v.OutOfService,
		DateUnshelve: ts(v.DateUnshelve), DateChanged: ts(v.DateChanged),
	}
	if v.Issue != nil {
		b.IssueId = v.Issue.ID[:]
	}
	return b.Build()
}

func condRef(r *app.ConditionRef) (engine.ConditionRef, error) {
	if r.GetRule() == "" {
		return engine.ConditionRef{}, status.Error(codes.InvalidArgument, "ref.rule: missing")
	}
	return engine.ConditionRef{Rule: r.GetRule(), Node: r.GetNode(), Key: r.GetKey()}, nil
}

func (x conditions) Get(_ context.Context, req *app.ConditionGetRequest) (*app.Condition, error) {
	ref, err := condRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	snap := x.s.Engine.Snapshot()
	for _, v := range snap.Conditions {
		if v.Rule == ref.Rule && v.Node == ref.Node && v.Key == ref.Key {
			return toCondition(v), nil
		}
	}
	if snap.Defs == nil || snap.Defs.Rules[ref.Rule] == nil {
		return nil, status.Errorf(codes.NotFound, "no rule %q", ref.Rule)
	}
	// Nothing has come for it: inactive.
	return app.Condition_builder{Rule: ref.Rule, Node: ref.Node, Key: ref.Key, State: engine.StateInactive, Severity: snap.Defs.Rules[ref.Rule].Severity}.Build(), nil
}

func (x conditions) List(_ context.Context, req *app.ConditionListRequest) (*app.ConditionListResponse, error) {
	var out []engine.ConditionView
	for _, v := range x.s.Engine.Snapshot().Conditions {
		if len(req.GetFilters()) > 0 && !slices.ContainsFunc(req.GetFilters(), func(f *app.ConditionFilter) bool {
			return (f.GetRule() == "" || f.GetRule() == v.Rule) &&
				(f.GetNode() == "" || f.GetNode() == v.Node) &&
				(f.GetState() == "" || f.GetState() == v.State)
		}) {
			continue
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b engine.ConditionView) int { return cmp.Compare(condName(a), condName(b)) })
	items, next := page(out, condName, req.GetSize(), req.GetAfter())
	vs := make([]*app.Condition, len(items))
	for i, v := range items {
		vs[i] = toCondition(v)
	}
	return app.ConditionListResponse_builder{Items: vs, Next: next}.Build(), nil
}

func (x conditions) Shelve(ctx context.Context, req *app.ConditionShelveRequest) (*app.Condition, error) {
	ref, err := condRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	v, err := x.s.Engine.Shelve(ctx, ref, req.GetMode(), req.GetDuration().AsDuration(), req.GetComment())
	if err != nil {
		return nil, err
	}
	return toCondition(v), nil
}

func (x conditions) Unshelve(ctx context.Context, req *app.ConditionUnshelveRequest) (*app.Condition, error) {
	ref, err := condRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	v, err := x.s.Engine.Unshelve(ctx, ref, req.GetComment())
	if err != nil {
		return nil, err
	}
	return toCondition(v), nil
}

func (x conditions) RemoveFromService(ctx context.Context, req *app.ConditionRemoveFromServiceRequest) (*app.Condition, error) {
	ref, err := condRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	v, err := x.s.Engine.SetOutOfService(ctx, ref, true, req.GetComment())
	if err != nil {
		return nil, err
	}
	return toCondition(v), nil
}

func (x conditions) PlaceInService(ctx context.Context, req *app.ConditionPlaceInServiceRequest) (*app.Condition, error) {
	ref, err := condRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	v, err := x.s.Engine.SetOutOfService(ctx, ref, false, req.GetComment())
	if err != nil {
		return nil, err
	}
	return toCondition(v), nil
}

// -- SourceService --

type sources struct {
	app.UnimplementedSourceServiceServer
	s *Services
}

func toSource(v engine.SourceView) *app.Source {
	var ns []*app.SourceNode
	for _, n := range v.Nodes {
		ns = append(ns, app.SourceNode_builder{
			Node: n.Node, DateSignaled: ts(n.DateSignaled), DateAlive: ts(n.DateAlive),
			DateStarted: ts(n.DateStarted), Instance: n.Instance, Alive: n.Alive,
			DateRestarted: ts(n.DateRestarted),
		}.Build())
	}
	b := app.Source_builder{Name: v.Name, Provider: v.Provider, Revision: v.Revision, Nodes: ns}
	if !v.Liveness.None {
		b.LivenessMetric = v.Liveness.Metric
		b.MaxGap = durationpb.New(v.Liveness.MaxGap)
	}
	return b.Build()
}

func (x sources) Get(_ context.Context, req *app.SourceGetRequest) (*app.Source, error) {
	for _, v := range x.s.Engine.Snapshot().Sources {
		if v.Name == req.GetRef().GetName() {
			return toSource(v), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "no source %q", req.GetRef().GetName())
}

func (x sources) List(_ context.Context, req *app.SourceListRequest) (*app.SourceListResponse, error) {
	var out []engine.SourceView
	for _, v := range x.s.Engine.Snapshot().Sources {
		if len(req.GetFilters()) > 0 && !slices.ContainsFunc(req.GetFilters(), func(f *app.SourceFilter) bool {
			return f.GetProvider() == "" || f.GetProvider() == v.Provider
		}) {
			continue
		}
		out = append(out, v)
	}
	items, next := page(out, func(v engine.SourceView) string { return v.Name }, req.GetSize(), req.GetAfter())
	vs := make([]*app.Source, len(items))
	for i, v := range items {
		vs[i] = toSource(v)
	}
	return app.SourceListResponse_builder{Items: vs, Next: next}.Build(), nil
}

// -- RuleService --

type rules struct {
	app.UnimplementedRuleServiceServer
	s *Services
}

func toRule(r *def.Rule) *app.Rule {
	b, _ := yaml.Marshal(r.Raw)
	return app.Rule_builder{
		Id: r.ID, Kind: r.Kind, Provider: r.Provider, Revision: r.Revision,
		Severity: r.Severity, Definition: string(b),
	}.Build()
}

func (x rules) all() []*def.Rule {
	d := x.s.Engine.Snapshot().Defs
	if d == nil || d.Set == nil {
		return nil
	}
	out := slices.Collect(maps.Values(d.Set.Rules))
	slices.SortFunc(out, func(a, b *def.Rule) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func (x rules) Get(_ context.Context, req *app.RuleGetRequest) (*app.Rule, error) {
	for _, r := range x.all() {
		if r.ID == req.GetRef().GetId() {
			return toRule(r), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "no rule %q", req.GetRef().GetId())
}

func (x rules) List(_ context.Context, req *app.RuleListRequest) (*app.RuleListResponse, error) {
	var out []*def.Rule
	for _, r := range x.all() {
		if len(req.GetFilters()) > 0 && !slices.ContainsFunc(req.GetFilters(), func(f *app.RuleFilter) bool {
			return (f.GetKind() == "" || f.GetKind() == r.Kind) && (f.GetProvider() == "" || f.GetProvider() == r.Provider)
		}) {
			continue
		}
		out = append(out, r)
	}
	items, next := page(out, func(r *def.Rule) string { return r.ID }, req.GetSize(), req.GetAfter())
	vs := make([]*app.Rule, len(items))
	for i, r := range items {
		vs[i] = toRule(r)
	}
	return app.RuleListResponse_builder{Items: vs, Next: next}.Build(), nil
}

// -- ProviderService --

type providers struct {
	app.UnimplementedProviderServiceServer
	s *Services
}

func toProvider(p provider.Status) *app.Provider {
	return app.Provider_builder{
		Name: p.Name, Type: p.Type, Location: p.Location,
		Revision: p.Revision, DateApplied: ts(p.Applied),
		Errors: p.Errors, Warnings: p.Warnings, DateRefused: ts(p.Refused),
	}.Build()
}

func (x providers) all() []provider.Status {
	if x.s.Providers == nil {
		return nil
	}
	out := x.s.Providers()
	slices.SortFunc(out, func(a, b provider.Status) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

func (x providers) Get(_ context.Context, req *app.ProviderGetRequest) (*app.Provider, error) {
	for _, p := range x.all() {
		if p.Name == req.GetRef().GetName() {
			return toProvider(p), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "no provider %q", req.GetRef().GetName())
}

func (x providers) List(_ context.Context, req *app.ProviderListRequest) (*app.ProviderListResponse, error) {
	items, next := page(x.all(), func(p provider.Status) string { return p.Name }, req.GetSize(), req.GetAfter())
	vs := make([]*app.Provider, len(items))
	for i, p := range items {
		vs[i] = toProvider(p)
	}
	return app.ProviderListResponse_builder{Items: vs, Next: next}.Build(), nil
}

// -- NodeService --

type nodes struct {
	app.UnimplementedNodeServiceServer
	s *Services
}

func toNode(n engine.NodeView) *app.Node {
	var gaps []*app.Gap
	for _, g := range n.Gate.Gaps {
		gaps = append(gaps, app.Gap_builder{DateFrom: ts(g.From), DateTo: ts(g.To), Reason: g.Reason}.Build())
	}
	return app.Node_builder{
		Name: n.Name, Observable: n.Gate.Observable, Reason: n.Gate.Reason,
		DateBooted: ts(n.DateBooted),
		Canary: app.Canary_builder{
			Rtt: durationpb.New(n.Gate.RTT), DateFailed: ts(n.Gate.DateFailed),
			Error: n.Gate.Error, Sent: n.Gate.Sent, Missing: n.Gate.Missing,
		}.Build(),
		Gaps: gaps,
	}.Build()
}

func (x nodes) Get(_ context.Context, req *app.NodeGetRequest) (*app.Node, error) {
	for _, n := range x.s.Engine.Snapshot().Nodes {
		if n.Name == req.GetRef().GetName() {
			return toNode(n), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "no node %q", req.GetRef().GetName())
}

func (x nodes) List(_ context.Context, req *app.NodeListRequest) (*app.NodeListResponse, error) {
	items, next := page(x.s.Engine.Snapshot().Nodes, func(n engine.NodeView) string { return n.Name }, req.GetSize(), req.GetAfter())
	vs := make([]*app.Node, len(items))
	for i, n := range items {
		vs[i] = toNode(n)
	}
	return app.NodeListResponse_builder{Items: vs, Next: next}.Build(), nil
}

// -- StatusService --

type statuses struct {
	app.UnimplementedStatusServiceServer
	s *Services
}

// A source with nothing from it for this long is reported silent: its
// services may not be sending OTLP at all (design §6.9).
const silentAfter = time.Minute

func (x statuses) Get(context.Context, *app.StatusGetRequest) (*app.Status, error) {
	e := x.s.Engine
	snap := e.Snapshot()
	st := e.Norm.Stats()

	b := app.Status_builder{
		Run: e.RunID, DateStarted: ts(e.Started), Tick: durationpb.New(e.LastTick()),
		Overruns: e.Overruns(), Late: snap.Late,
	}
	if x.s.Receiver != nil {
		r := x.s.Receiver()
		b.Queued, b.QueueSize = int64(r.Queued), int64(r.QueueSize)
	}
	for _, k := range slices.SortedFunc(maps.Keys(st.Unmatched), func(a, b engine.UnmatchedKey) int {
		return cmp.Or(cmp.Compare(a.Source, b.Source), cmp.Compare(a.ConditionType, b.ConditionType))
	}) {
		b.Unmatched = append(b.Unmatched, app.Unmatched_builder{Source: k.Source, ConditionType: k.ConditionType, Count: st.Unmatched[k]}.Build())
	}
	for _, k := range slices.SortedFunc(maps.Keys(st.Violations), func(a, b engine.ViolationKey) int {
		return cmp.Or(cmp.Compare(a.Source, b.Source), cmp.Compare(a.Attribute, b.Attribute), cmp.Compare(a.Got, b.Got))
	}) {
		b.Violations = append(b.Violations, app.TypeViolation_builder{Source: k.Source, Attribute: k.Attribute, Want: k.Want, Got: k.Got, Count: st.Violations[k]}.Build())
	}
	b.UnknownHosts = slices.Sorted(maps.Keys(st.UnknownHosts))
	if snap.Time.Sub(e.Started) > silentAfter {
		for _, src := range snap.Sources {
			if snap.Time.Sub(e.Norm.Signaled(src.Name)) > silentAfter {
				b.SilentSources = append(b.SilentSources, src.Name)
			}
		}
	}
	return b.Build(), nil
}

// -- ComponentService --

type components struct {
	app.UnimplementedComponentServiceServer
	s *Services
}

// Health of a Component, best first (design §9).
const (
	HealthHealthy   = "healthy"
	HealthNoData    = "no_data"
	HealthDegraded  = "degraded"
	HealthUnknown   = "unknown"
	HealthUnhealthy = "unhealthy"
)

var healthRank = map[string]int{HealthHealthy: 0, HealthNoData: 1, HealthDegraded: 2, HealthUnknown: 3, HealthUnhealthy: 4}

// health works out every Component's health and its counts of the states of
// what it holds, its children's included.
func (x components) health() map[string]*app.Component {
	snap := x.s.Engine.Snapshot()
	out := map[string]*app.Component{}
	if snap.Defs == nil || snap.Defs.Set == nil {
		return out
	}
	cs := snap.Defs.Set.Components
	observable := map[string]bool{}
	for _, n := range snap.Nodes {
		observable[n.Name] = n.Gate.Observable
	}
	alive := map[string]bool{}
	for _, src := range snap.Sources {
		for _, n := range src.Nodes {
			alive[src.Name+"\x00"+n.Node] = n.Alive
		}
	}

	own := map[string]string{}
	counts := map[string]map[string]int64{}
	for _, name := range slices.Sorted(maps.Keys(cs)) {
		c := cs[name]
		h := HealthNoData
		evidence := false
		cnt := map[string]int64{}
		worse := func(x string) {
			if healthRank[x] > healthRank[h] {
				h = x
			}
		}
		for _, v := range snap.Conditions {
			if !slices.ContainsFunc(c.Members, func(m def.Member) bool {
				return m.Rule == v.Rule && (m.Node == "" || m.Node == v.Node) && (m.Key == nil || *m.Key == v.Key)
			}) {
				continue
			}
			cnt[v.State]++
			if v.Node != "" && !observable[v.Node] {
				worse(HealthUnknown)
			}
			r := snap.Defs.Rules[v.Rule]
			if r != nil && r.Source != "" && v.Node != "" {
				if alive[r.Source+"\x00"+v.Node] {
					evidence = true
				}
			} else {
				evidence = true
			}
			switch v.State {
			case engine.StateActive:
				switch v.Severity {
				case def.SeverityCritical, def.SeverityError:
					worse(HealthUnhealthy)
				case def.SeverityWarning:
					worse(HealthDegraded)
				}
			case engine.StateStale:
				worse(HealthUnknown)
			}
		}
		if evidence && h == HealthNoData {
			h = HealthHealthy
		}
		own[name] = h
		counts[name] = cnt
	}

	// A parent is as bad as the worst of what is under it.
	var total func(name string, seen map[string]bool) (string, map[string]int64)
	total = func(name string, seen map[string]bool) (string, map[string]int64) {
		h := own[name]
		cnt := maps.Clone(counts[name])
		if seen[name] {
			return h, cnt
		}
		seen[name] = true
		for _, child := range slices.Sorted(maps.Keys(cs)) {
			if cs[child].Parent != name {
				continue
			}
			ch, cc := total(child, seen)
			if healthRank[ch] > healthRank[h] {
				h = ch
			}
			for k, v := range cc {
				cnt[k] += v
			}
		}
		return h, cnt
	}
	for name, c := range cs {
		h, cnt := total(name, map[string]bool{})
		if cnt == nil {
			cnt = map[string]int64{}
		}
		out[name] = app.Component_builder{Name: name, Parent: c.Parent, Health: h, Counts: cnt}.Build()
	}
	return out
}

func (x components) Get(_ context.Context, req *app.ComponentGetRequest) (*app.Component, error) {
	if c, ok := x.health()[req.GetRef().GetName()]; ok {
		return c, nil
	}
	return nil, status.Errorf(codes.NotFound, "no component %q", req.GetRef().GetName())
}

func (x components) List(_ context.Context, req *app.ComponentListRequest) (*app.ComponentListResponse, error) {
	all := x.health()
	var out []*app.Component
	for _, name := range slices.Sorted(maps.Keys(all)) {
		c := all[name]
		if len(req.GetFilters()) > 0 && !slices.ContainsFunc(req.GetFilters(), func(f *app.ComponentFilter) bool {
			return (f.GetParent() == "" || f.GetParent() == c.GetParent()) && (f.GetHealth() == "" || f.GetHealth() == c.GetHealth())
		}) {
			continue
		}
		out = append(out, c)
	}
	items, next := page(out, func(c *app.Component) string { return c.GetName() }, req.GetSize(), req.GetAfter())
	return app.ComponentListResponse_builder{Items: items, Next: next}.Build(), nil
}
