package provider_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/provider"
)

// base supplies the sources.
const base = `
version: 1
sources:
  detector:
    liveness: {metric: detector.frames, max_gap: 3s}
  report-job:
    liveness: none
rules: []
`

// baseWithoutDetector is base with detector removed.
const baseWithoutDetector = `
version: 1
sources:
  report-job:
    liveness: none
rules: []
`

// detectorRules reads base's detector.
const detectorRules = `
version: 1
sources: {}
rules:
  - id: detector.inference_failed
    kind: log
    source: detector
    severity: error
    match: {attributes: {diag.condition.type: detector.inference_failed}}
    key: [{attribute: diag.condition.key}]
    activate: {count: 3, within: 200ms}
    resolve_after: 300ms
    on_source_restart: close
    operator: {ack: required, latch: false}
`

// reportRules reads base's report-job, in a way def.Merge warns about.
const reportRules = `
version: 1
sources: {}
rules:
  - id: report-job.error
    kind: log
    source: report-job
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 1}
    resolve_after: 10m
    on_source_restart: keep
    operator: {ack: none, latch: false}
`

const noRules = `
version: 1
sources: {}
rules: []
`

// broken does not parse.
const broken = `
version: 1
sources: {detector: {liveness: none}
rules: []
`

func newFile(t *testing.T, path string, poll, debounce time.Duration) *provider.File {
	t.Helper()
	f, err := provider.NewFile(conf.Provider{Name: "p", Type: "file", Path: path, PollInterval: poll, Debounce: debounce})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// runFile runs f until the test ends, and answers what it sends.
func runFile(t *testing.T, f *provider.File) <-chan provider.Update {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan provider.Update)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, out) }()
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
	return out
}

func next(t *testing.T, out <-chan provider.Update) provider.Update {
	t.Helper()
	select {
	case u := <-out:
		return u
	case <-time.After(5 * time.Second):
		t.Fatal("no update")
		return provider.Update{}
	}
}

// quiet fails if anything is sent within d.
func quiet(t *testing.T, out <-chan provider.Update, d time.Duration) {
	t.Helper()
	select {
	case u := <-out:
		t.Fatalf("sent revision %q (err: %v)", u.Revision, u.Err)
	case <-time.After(d):
	}
}

// wantContent fails unless u supplies content.
func wantContent(t *testing.T, u provider.Update, content string) {
	t.Helper()
	if u.Err != nil {
		t.Fatalf("err: %v", u.Err)
	}
	if u.File == nil {
		t.Fatal("no file")
	}
	if want := provider.Revision([]byte(content)); u.Revision != want {
		t.Fatalf("revision %q, want %q", u.Revision, want)
	}
}

// replace writes content beside path and renames it over, as an editor or a
// template renderer does.
func replace(t *testing.T, path, content string) {
	t.Helper()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		t.Fatal(err)
	}
}

// overwrite writes content into path in place.
func overwrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFileSendsAtStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, path, base)

	u := next(t, runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond)))
	wantContent(t, u, base)
	if u.Provider != "p" {
		t.Errorf("provider %q", u.Provider)
	}
	if len(u.File.Sources) != 2 {
		t.Errorf("%d sources", len(u.File.Sources))
	}
	if got := provider.Revision([]byte(base)); len(got) != 12 {
		t.Errorf("revision %q is not 12 hex digits", got)
	}
}

// A file replaced by renaming another over it is a new inode; the watch on
// the directory sees it. The poll interval is too long to be what does.
func TestFileReplacedByRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, path, base)
	out := runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond))
	wantContent(t, next(t, out), base)

	replace(t, path, baseWithoutDetector)
	wantContent(t, next(t, out), baseWithoutDetector)

	replace(t, path, base)
	wantContent(t, next(t, out), base)
}

// Kubernetes swaps a mounted ConfigMap through a link the file resolves
// through, and the file's own name sees no event.
func TestFileSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{"..v1": base, "..v2": baseWithoutDetector} {
		if err := os.Mkdir(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
		overwrite(t, filepath.Join(dir, name, "base.yaml"), content)
	}
	if err := os.Symlink("..v1", filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "base.yaml")
	if err := os.Symlink(filepath.Join("..data", "base.yaml"), path); err != nil {
		t.Fatal(err)
	}

	out := runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond))
	wantContent(t, next(t, out), base)

	if err := os.Symlink("..v2", filepath.Join(dir, "..data_tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	wantContent(t, next(t, out), baseWithoutDetector)
}

// The file is written where no event reaches the watched directory: through
// a link to another directory. Only the poll can see it; the debounce is too
// long for an event to be what does.
func TestFilePolls(t *testing.T) {
	elsewhere := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, elsewhere, base)
	path := filepath.Join(t.TempDir(), "base.yaml")
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}

	out := runFile(t, newFile(t, path, 50*time.Millisecond, time.Hour))
	wantContent(t, next(t, out), base)

	replace(t, elsewhere, baseWithoutDetector)
	wantContent(t, next(t, out), baseWithoutDetector)
}

func TestFileUnchangedSendsNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, path, base)
	out := runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond))
	wantContent(t, next(t, out), base)

	replace(t, path, base)

	// In place, and without truncating it first: at no moment does it read as
	// anything but what it held.
	fh, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteAt([]byte(base), 0); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatal(err)
	}
	quiet(t, out, 300*time.Millisecond)

	// Still watching: a change is sent, and only the change.
	replace(t, path, baseWithoutDetector)
	wantContent(t, next(t, out), baseWithoutDetector)
	quiet(t, out, 100*time.Millisecond)
}

// A burst of writes shorter than Debounce is read once, Debounce after it
// began. The writes are in place, as a writer that does not rename does.
func TestFileDebounce(t *testing.T) {
	const debounce = 300 * time.Millisecond
	path := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, path, base)
	out := runFile(t, newFile(t, path, time.Hour, debounce))
	wantContent(t, next(t, out), base)

	began := time.Now()
	last := ""
	for i := range 5 {
		last = base + strings.Repeat("\n# padding", i+1)
		overwrite(t, path, last)
	}
	if took := time.Since(began); took >= debounce {
		// The read may have come in the middle, as it is meant to.
		t.Skipf("the burst took %v, no shorter than the debounce; the machine is too busy to tell", took)
	}
	u := next(t, out)
	if waited := time.Since(began); waited < debounce {
		t.Errorf("read %v after the first write, before the debounce", waited)
	}
	wantContent(t, u, last)
	quiet(t, out, 2*debounce)
}

// A broken file is sent as an error, with the revision of what was read; the
// file mended is sent again.
func TestFileBroken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "base.yaml")
	overwrite(t, path, base)
	out := runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond))
	wantContent(t, next(t, out), base)

	replace(t, path, broken)
	u := next(t, out)
	if u.Err == nil || u.File != nil {
		t.Fatalf("broken file sent as %+v", u)
	}
	if want := provider.Revision([]byte(broken)); u.Revision != want {
		t.Errorf("revision %q, want %q", u.Revision, want)
	}

	// The same content as before it broke is sent again: the error is over.
	replace(t, path, base)
	wantContent(t, next(t, out), base)
}

// A file missing at start is an error, not a stop; once written it is sent.
func TestFileMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "base.yaml")
	out := runFile(t, newFile(t, path, time.Hour, 20*time.Millisecond))

	u := next(t, out)
	if !errors.Is(u.Err, fs.ErrNotExist) || u.Revision != "" {
		t.Fatalf("missing file sent as %+v", u)
	}

	replace(t, path, base)
	wantContent(t, next(t, out), base)

	// Removed: an error again, once.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if u := next(t, out); !errors.Is(u.Err, fs.ErrNotExist) {
		t.Fatalf("removed file sent as %+v", u)
	}
	quiet(t, out, 100*time.Millisecond)
}

// The directory is not there at start, so there is nothing to watch; the
// poll finds the file once it is.
func TestFileMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "providers")
	path := filepath.Join(dir, "base.yaml")
	out := runFile(t, newFile(t, path, 50*time.Millisecond, 20*time.Millisecond))
	if u := next(t, out); !errors.Is(u.Err, fs.ErrNotExist) {
		t.Fatalf("missing directory sent as %+v", u)
	}

	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	replace(t, path, base)
	wantContent(t, next(t, out), base)
}

func TestNewFileRefuses(t *testing.T) {
	_, err := provider.NewFile(conf.Provider{Name: "p", Type: "file"})
	if err == nil {
		t.Fatal("made a file provider of nothing")
	}
	for _, want := range []string{"path: missing", "poll_interval: missing", "debounce: missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not say %q:\n%v", want, err)
		}
	}

	p, err := provider.New(conf.Provider{Name: "p", Type: "http"})
	if err == nil || p != nil {
		t.Errorf("made a provider of an unknown type: %v, %v", p, err)
	}
	p, err = provider.New(conf.Provider{Name: "p", Type: "file"})
	if err == nil || p != nil {
		t.Errorf("made a file provider of nothing: %v, %v", p, err)
	}
}
