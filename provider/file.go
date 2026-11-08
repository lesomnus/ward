package provider

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/lesomnus/otx/log"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
)

// File supplies what one file says.
//
// A writer is expected to replace the file rather than write it in place --
// write a temporary file beside it, then rename it over -- as editors and
// template renderers do. A file written in place is read too, but it may be
// caught half written; that is refused like any broken file, and the file is
// read again once the writing is done.
type File struct {
	name     string
	path     string
	poll     time.Duration
	debounce time.Duration
}

// NewFile makes the file provider c configures. It refuses a configuration
// that is missing a value, as conf.Ward.Validate does.
func NewFile(c conf.Provider) (*File, error) {
	var errs []error
	bad := func(at, why string) { errs = append(errs, fmt.Errorf("provider %s: %s: %s", c.Name, at, why)) }
	if strings.TrimSpace(c.Name) == "" {
		bad("name", "missing")
	}
	if c.Type != "file" {
		bad("type", fmt.Sprintf("%q is not file", c.Type))
	}
	if strings.TrimSpace(c.Path) == "" {
		bad("path", "missing")
	}
	if c.PollInterval <= 0 {
		bad("poll_interval", "missing")
	}
	if c.Debounce <= 0 {
		bad("debounce", "missing")
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return &File{
		name:     c.Name,
		path:     filepath.Clean(c.Path),
		poll:     c.PollInterval,
		debounce: c.Debounce,
	}, nil
}

func (f *File) Name() string     { return f.name }
func (f *File) Type() string     { return "file" }
func (f *File) Location() string { return f.path }

// Run sends what the file says, then again each time that changes, until ctx
// is done.
//
// It watches the directory that holds the file rather than the file: a watch
// on a file stays with the file it was put on, so a file replaced by renaming
// another over it, or swapped through a symbolic link, is never heard of. Any
// event in the directory counts, not only one named for the file: Kubernetes
// swaps a mounted ConfigMap by renaming a link the file resolves through
// (`..data`), and the file's own name sees nothing. After an event Run waits
// Debounce, so that the rest of a write arrives, then reads once; what
// happens during the wait is covered by that read.
//
// Every PollInterval it reads the file anyway. That covers what no event is
// sent for -- a bind-mounted file, a network filesystem, events that
// overflowed the kernel's queue -- and the same tick puts the watch back when
// it was lost (the directory removed, or not there yet) or could not be set
// up at all.
//
// Content is compared by its hash: what reads as the content last sent sends
// nothing, however it was rewritten. A file that cannot be read, or does not
// parse, is sent as an Update with Err, once until it changes.
func (f *File) Run(ctx context.Context, out chan<- Update) error {
	// Watch before the first read, so that nothing written after that read
	// goes unheard.
	w := &dirWatch{dir: filepath.Dir(f.path), provider: f.name}
	defer w.close()
	w.ensure(ctx)

	// What was sent last: the content's sum, or why the file could not be read.
	last := ""
	send := func() bool {
		u, ok := f.read(&last)
		if !ok {
			return true
		}
		select {
		case out <- u:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if !send() {
		return nil
	}

	tick := time.NewTicker(f.poll)
	defer tick.Stop()

	// The debounce: the read an event asked for, nil while none is waiting.
	var wait <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return nil

		case _, ok := <-w.events():
			if !ok {
				w.lost(ctx)
				continue
			}
			if wait == nil {
				wait = time.After(f.debounce)
			}

		case err, ok := <-w.errors():
			if !ok {
				w.lost(ctx)
				continue
			}
			// An overflow is events lost; whatever else it is, a read costs
			// little and finds out.
			log.From(ctx).WarnContext(ctx, "provider: the directory watch failed; reading the file again",
				slog.String("provider", f.name), slog.String("dir", w.dir), slog.Any("err", err))
			if wait == nil {
				wait = time.After(f.debounce)
			}

		case <-wait:
			wait = nil
			if !send() {
				return nil
			}

		case <-tick.C:
			w.ensure(ctx)
			if !send() {
				return nil
			}
		}
	}
}

// read reads the file and answers the Update to send, or false when it reads
// as what was last sent. last is updated to what this read found.
func (f *File) read(last *string) (Update, bool) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		seen := "error: " + err.Error()
		if seen == *last {
			return Update{}, false
		}
		*last = seen
		return Update{Provider: f.name, Err: err}, true
	}

	sum := sha256.Sum256(b)
	seen := "content: " + string(sum[:])
	if seen == *last {
		return Update{}, false
	}
	*last = seen

	u := Update{Provider: f.name, Revision: revision(sum)}
	u.File, u.Err = def.Parse(b)
	return u, true
}

// dirWatch is the watch on the directory that holds the file, while there is
// one. Without it the file is still read every poll interval.
type dirWatch struct {
	dir      string
	provider string

	w *fsnotify.Watcher

	// Why the directory could not be watched, until it can: a failure is
	// logged once, not at every tick.
	failed string
}

func (d *dirWatch) events() <-chan fsnotify.Event {
	if d.w == nil {
		return nil
	}
	return d.w.Events
}

func (d *dirWatch) errors() <-chan error {
	if d.w == nil {
		return nil
	}
	return d.w.Errors
}

// ensure watches the directory if it is not watched: not yet, or not any more.
func (d *dirWatch) ensure(ctx context.Context) {
	err := d.add()
	switch {
	case err != nil:
		if err.Error() == d.failed {
			return
		}
		d.failed = err.Error()
		log.From(ctx).WarnContext(ctx, "provider: cannot watch the directory; the file is read every poll_interval",
			slog.String("provider", d.provider), slog.String("dir", d.dir), slog.Any("err", err))

	case d.failed != "":
		d.failed = ""
		log.From(ctx).InfoContext(ctx, "provider: watching the directory",
			slog.String("provider", d.provider), slog.String("dir", d.dir))
	}
}

func (d *dirWatch) add() error {
	if d.w == nil {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		d.w = w
	}
	// A directory that was removed or unmounted drops out of the list.
	if slices.Contains(d.w.WatchList(), d.dir) {
		return nil
	}
	return d.w.Add(d.dir)
}

// lost lets go of a watcher whose channels were closed under it; the next
// tick makes a new one.
func (d *dirWatch) lost(ctx context.Context) {
	d.close()
	d.failed = "stopped"
	log.From(ctx).WarnContext(ctx, "provider: the directory watch stopped; the file is read every poll_interval",
		slog.String("provider", d.provider), slog.String("dir", d.dir))
}

func (d *dirWatch) close() {
	if d.w != nil {
		d.w.Close()
		d.w = nil
	}
}
