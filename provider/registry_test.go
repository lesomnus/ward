package provider_test

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/provider"
)

type applied struct {
	set   *def.Set
	warns []string
}

type harness struct {
	*provider.Registry
	dir     string
	applied chan applied
}

// file is a provider file: its provider's name and what it says; "" is a
// file that is not there.
type file struct{ name, content string }

// start runs a registry of file providers, in the order given, until the
// test ends. extra are providers of other kinds, after the files.
func start(t *testing.T, files []file, extra ...provider.Provider) *harness {
	t.Helper()
	h := &harness{dir: t.TempDir(), applied: make(chan applied, 64)}

	var ps []provider.Provider
	for _, f := range files {
		path := h.path(f.name)
		if f.content != "" {
			overwrite(t, path, f.content)
		}
		p, err := provider.New(conf.Provider{Name: f.name, Type: "file", Path: path, PollInterval: time.Hour, Debounce: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		ps = append(ps, p)
	}
	ps = append(ps, extra...)
	h.Registry = provider.NewRegistry([]string{"n1"}, ps, func(s *def.Set, warns []string) {
		h.applied <- applied{s, warns}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return once its context was done")
		}
	})
	return h
}

func (h *harness) path(name string) string { return filepath.Join(h.dir, name+".yaml") }

// next is the next Set applied.
func (h *harness) next(t *testing.T) applied {
	t.Helper()
	select {
	case a := <-h.applied:
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("nothing applied")
		return applied{}
	}
}

// quiet fails if anything is applied within d.
func (h *harness) quiet(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case a := <-h.applied:
		t.Fatalf("applied %v", a.set.Revisions)
	case <-time.After(d):
	}
}

func (h *harness) status(t *testing.T, name string) provider.Status {
	t.Helper()
	for _, s := range h.Status() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no provider %s", name)
	return provider.Status{}
}

// until waits for the status of a provider to come to what ok says.
func (h *harness) until(t *testing.T, name string, ok func(provider.Status) bool) provider.Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := h.status(t, name)
		if ok(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("status of %s did not come: %+v", name, s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func refused(s provider.Status) bool { return len(s.Errors) > 0 }

func says(errs []string, want string) bool {
	return slices.ContainsFunc(errs, func(e string) bool { return strings.Contains(e, want) })
}

// wantInEffect fails unless content is the revision of name in effect, with
// nothing refused since.
func wantInEffect(t *testing.T, s provider.Status, content string) {
	t.Helper()
	if want := provider.Revision([]byte(content)); s.Revision != want {
		t.Errorf("%s: revision %q in effect, want %q", s.Name, s.Revision, want)
	}
	if s.Applied.IsZero() {
		t.Errorf("%s: no time applied", s.Name)
	}
	if len(s.Errors) > 0 || !s.Refused.IsZero() || s.RefusedRevision != "" {
		t.Errorf("%s: refused: %+v", s.Name, s)
	}
}

func TestRegistryStartsWithoutBrokenProviders(t *testing.T) {
	noOperator := strings.Replace(reportRules, "    operator: {ack: none, latch: false}\n", "", 1)
	alsoDetector := "version: 1\nsources: {detector: {liveness: none}}\nrules: []\n"

	// rules reads base, which comes after it: the order given is not the
	// order things have to go in.
	h := start(t, []file{
		{"rules", detectorRules},
		{"base", base},
		{"broken", broken},
		{"missing", ""},
		{"no-operator", noOperator},
		{"also-detector", alsoDetector},
	})

	a := h.next(t)
	if got := slices.Sorted(maps.Keys(a.set.Revisions)); !slices.Equal(got, []string{"base", "rules"}) {
		t.Fatalf("applied %v", got)
	}
	if _, ok := a.set.Rules["detector.inference_failed"]; !ok {
		t.Error("rules is not applied")
	}
	if h.Set() != a.set {
		t.Error("Set is not what was applied")
	}
	wantInEffect(t, h.status(t, "rules"), detectorRules)
	wantInEffect(t, h.status(t, "base"), base)

	for name, want := range map[string]string{
		"broken":        "yaml",
		"missing":       "no such file",
		"no-operator":   "operator: missing",
		"also-detector": "also defined by provider base",
	} {
		s := h.status(t, name)
		if s.Revision != "" || !s.Applied.IsZero() {
			t.Errorf("%s: in effect: %+v", name, s)
		}
		if !says(s.Errors, want) || s.Refused.IsZero() {
			t.Errorf("%s: errors do not say %q: %q", name, want, s.Errors)
		}
	}
	if s := h.status(t, "broken"); s.RefusedRevision != provider.Revision([]byte(broken)) {
		t.Errorf("broken: refused revision %q", s.RefusedRevision)
	}
	if s := h.status(t, "missing"); s.RefusedRevision != "" {
		t.Errorf("missing: refused revision %q", s.RefusedRevision)
	}
	h.quiet(t, 100*time.Millisecond)

	// Mended, a provider left out goes in.
	replace(t, h.path("missing"), reportRules)
	a = h.next(t)
	if _, ok := a.set.Rules["report-job.error"]; !ok {
		t.Error("missing is not applied once written")
	}
	wantInEffect(t, h.status(t, "missing"), reportRules)
}

// Neither of two providers that read each other's sources merges alone; they
// go in together.
func TestRegistryStartsProvidersThatReadEachOther(t *testing.T) {
	a := strings.Replace(detectorRules, "sources: {}", "sources: {report-job: {liveness: none}}", 1)
	b := strings.Replace(reportRules, "sources: {}", "sources: {detector: {liveness: {metric: detector.frames, max_gap: 3s}}}", 1)
	h := start(t, []file{{"a", a}, {"b", b}})

	set := h.next(t).set
	if len(set.Rules) != 2 || len(set.Sources) != 2 {
		t.Fatalf("applied %d rules and %d sources", len(set.Rules), len(set.Sources))
	}
	wantInEffect(t, h.status(t, "a"), a)
	wantInEffect(t, h.status(t, "b"), b)
}

func TestRegistryKeepsRevisionOfBrokenFile(t *testing.T) {
	h := start(t, []file{{"base", base}, {"rules", detectorRules}})
	first := h.next(t).set

	replace(t, h.path("rules"), broken)
	s := h.until(t, "rules", refused)
	if s.Revision != provider.Revision([]byte(detectorRules)) || s.RefusedRevision != provider.Revision([]byte(broken)) {
		t.Errorf("revision %q in effect, %q refused", s.Revision, s.RefusedRevision)
	}
	h.quiet(t, 100*time.Millisecond)
	if h.Set() != first {
		t.Error("the Set in effect changed")
	}

	// What is in effect, written again: the refusal is over, and nothing
	// changes.
	replace(t, h.path("rules"), detectorRules)
	s = h.until(t, "rules", func(s provider.Status) bool { return !refused(s) })
	wantInEffect(t, s, detectorRules)
	h.quiet(t, 100*time.Millisecond)

	replace(t, h.path("rules"), reportRules)
	set := h.next(t).set
	if _, ok := set.Rules["report-job.error"]; !ok || len(set.Rules) != 1 {
		t.Errorf("rules in effect: %v", slices.Sorted(maps.Keys(set.Rules)))
	}
	wantInEffect(t, h.status(t, "rules"), reportRules)
	if h.Set() != set {
		t.Error("Set is not what was applied")
	}
}

// A source another provider's rule reads cannot be removed. Once the rule is
// gone, the removal goes in without being written again.
func TestRegistryRefusesRemovingSourceInUse(t *testing.T) {
	h := start(t, []file{{"base", base}, {"rules", detectorRules}})
	first := h.next(t).set

	// The Set in effect is read while the registry merges what comes next.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = h.Status()
			if s := h.Set(); s != nil {
				for _, r := range s.Rules {
					_, _ = r.Provider, r.Revision
				}
				for _, x := range s.Sources {
					_, _ = x.Provider, x.Revision
				}
			}
		}
	})
	defer func() { close(stop); wg.Wait() }()

	replace(t, h.path("base"), baseWithoutDetector)
	s := h.until(t, "base", refused)
	if !says(s.Errors, `no source "detector"`) {
		t.Errorf("errors: %q", s.Errors)
	}
	wantInEffect(t, h.status(t, "rules"), detectorRules)
	h.quiet(t, 100*time.Millisecond)
	if _, ok := h.Set().Sources["detector"]; !ok || h.Set() != first {
		t.Error("detector was removed")
	}

	replace(t, h.path("rules"), noRules)
	set := h.next(t).set
	if len(set.Rules) != 0 {
		t.Errorf("rules in effect: %v", slices.Sorted(maps.Keys(set.Rules)))
	}
	if _, ok := set.Sources["detector"]; ok {
		t.Error("detector is still in effect")
	}
	wantInEffect(t, h.status(t, "base"), baseWithoutDetector)
	wantInEffect(t, h.status(t, "rules"), noRules)
	h.quiet(t, 100*time.Millisecond)
}

func TestRegistryWarnings(t *testing.T) {
	h := start(t, []file{{"base", base}, {"rules", reportRules}})
	a := h.next(t)
	if len(a.warns) != 2 {
		t.Fatalf("warnings: %q", a.warns)
	}
	if s := h.status(t, "rules"); !slices.Equal(s.Warnings, a.warns) {
		t.Errorf("rules warns %q", s.Warnings)
	}
	if s := h.status(t, "base"); len(s.Warnings) != 0 {
		t.Errorf("base warns %q, about rules of another", s.Warnings)
	}

	replace(t, h.path("rules"), detectorRules)
	if a := h.next(t); len(a.warns) != 0 {
		t.Fatalf("warnings: %q", a.warns)
	}
	if s := h.status(t, "rules"); len(s.Warnings) != 0 {
		t.Errorf("rules warns %q", s.Warnings)
	}
}

// stops is a provider that cannot run.
type stops struct{}

func (stops) Name() string     { return "stops" }
func (stops) Type() string     { return "test" }
func (stops) Location() string { return "nowhere" }
func (stops) Run(context.Context, chan<- provider.Update) error {
	return errors.New("cannot")
}

func TestRegistryProviderStops(t *testing.T) {
	h := start(t, []file{{"base", base}}, stops{})
	a := h.next(t)
	if got := slices.Sorted(maps.Keys(a.set.Revisions)); !slices.Equal(got, []string{"base"}) {
		t.Fatalf("applied %v", got)
	}
	if s := h.status(t, "stops"); !slices.Equal(s.Errors, []string{"stopped: cannot"}) {
		t.Errorf("errors: %q", s.Errors)
	}
}

// With nothing that can be accepted, an empty Set is applied all the same:
// the start is over.
func TestRegistryStartsWithNothing(t *testing.T) {
	h := start(t, []file{{"missing", ""}})
	a := h.next(t)
	if a.set == nil || len(a.set.Rules) != 0 || len(a.set.Sources) != 0 {
		t.Fatalf("applied %+v", a.set)
	}
	if h.Set() != a.set {
		t.Error("Set is not what was applied")
	}
}

func TestRegistryBeforeRun(t *testing.T) {
	f := newFile(t, filepath.Join(t.TempDir(), "base.yaml"), time.Hour, time.Second)
	r := provider.NewRegistry(nil, []provider.Provider{f}, nil)
	if r.Set() != nil {
		t.Error("a Set before Run")
	}
	want := []provider.Status{{Name: "p", Type: "file", Location: f.Location()}}
	if got := r.Status(); !slices.EqualFunc(got, want, func(a, b provider.Status) bool {
		return a.Name == b.Name && a.Type == b.Type && a.Location == b.Location && a.Revision == "" && len(a.Errors) == 0
	}) {
		t.Errorf("status %+v", got)
	}
}
