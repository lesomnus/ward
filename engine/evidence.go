package engine

import (
	"slices"
	"time"
)

// run is one run of a process as a cumulative counter tells it: the counter's
// StartTimeUnixNano and the resource's service.instance.id, and the event times
// of the first and the last data point seen of it.
type run struct {
	start    time.Time
	instance string

	// When it began: the counter's start, or its first data point when the
	// counter says no start.
	began time.Time

	first, last time.Time
}

// runs are the runs of one process, ordered by when they began.
type runs struct{ list []*run }

// observe takes in a data point of the counter and answers with the run it is
// of.
func (rs *runs) observe(start time.Time, instance string, t time.Time) *run {
	for _, r := range rs.list {
		if r.start.Equal(start) && r.instance == instance {
			if t.Before(r.first) {
				r.first = t
			}
			if t.After(r.last) {
				r.last = t
			}
			return r
		}
	}
	r := &run{start: start, instance: instance, first: t, last: t, began: start}
	if start.IsZero() || start.After(t) {
		r.began = t
	}
	i, _ := slices.BinarySearchFunc(rs.list, r.began, func(x *run, t time.Time) int { return x.began.Compare(t) })
	rs.list = slices.Insert(rs.list, i, r)
	return r
}

// prune forgets runs whose last point is before t, keeping the latest.
func (rs *runs) prune(t time.Time) {
	if len(rs.list) < 2 {
		return
	}
	n := 0
	for _, r := range rs.list[:len(rs.list)-1] {
		if r.last.Before(t) {
			n++
			continue
		}
		break
	}
	rs.list = rs.list[n:]
}

// latest is the run that began last.
func (rs *runs) latest() *run {
	if rs == nil || len(rs.list) == 0 {
		return nil
	}
	return rs.list[len(rs.list)-1]
}

// at is the run that was going at t: the last that began by then.
func (rs *runs) at(t time.Time) *run {
	if rs == nil {
		return nil
	}
	var out *run
	for _, r := range rs.list {
		if r.began.After(t) {
			break
		}
		out = r
	}
	return out
}

// restarts answers with the runs that began after t because the process
// restarted -- not because its node booted. A run Ward saw nothing before
// counts when it began after t: Ward may have been down when it restarted, and
// t is what Ward knew before.
//
// A run is not answered until the node's boot time is known as of when it
// began: until then, a boot may yet explain it (design §7.4, row 1).
func (rs *runs) restarts(t time.Time, b *boots) []*run {
	if rs == nil {
		return nil
	}
	var out []*run
	prev := t
	for _, r := range rs.list {
		if !r.began.After(t) {
			prev = later(prev, r.last)
			continue
		}
		if !b.knownAt(r.began) {
			break
		}
		if !b.between(prev, r.began) {
			out = append(out, r)
		}
		prev = r.last
	}
	return out
}

// boots are the boot times of a node, from hostmetrics' system.uptime.
type boots struct {
	list []time.Time

	// The event time of the latest system.uptime data point: what is known
	// of the node's boots is known up to here.
	seen time.Time
}

// Boot times computed from two scrapes differ by how the uptime was rounded;
// a difference this small is the same boot.
const bootJitter = 10 * time.Second

// observe takes in a boot time computed from one system.uptime data point,
// and the point's time. It answers true for a boot it did not know.
func (b *boots) observe(t, at time.Time) bool {
	b.seen = later(b.seen, at)
	if len(b.list) == 0 {
		b.list = append(b.list, t)
		return true
	}
	last := b.list[len(b.list)-1]
	if t.Sub(last) <= bootJitter {
		// The same boot, or a point from before it arriving late.
		return false
	}
	b.list = append(b.list, t)
	if len(b.list) > 16 {
		b.list = b.list[len(b.list)-16:]
	}
	return true
}

func (b *boots) known() bool { return b != nil && len(b.list) > 0 }

// knownAt reports whether the node's boots are known up to t.
func (b *boots) knownAt(t time.Time) bool { return b.known() && !b.seen.Before(t) }

// latest is the last boot; zero if none is known.
func (b *boots) latest() time.Time {
	if !b.known() {
		return time.Time{}
	}
	return b.list[len(b.list)-1]
}

// after answers with the first boot after t.
func (b *boots) after(t time.Time) (time.Time, bool) {
	if b == nil {
		return time.Time{}, false
	}
	for _, x := range b.list {
		if x.After(t) {
			return x, true
		}
	}
	return time.Time{}, false
}

// between reports whether the node booted in (a, c].
func (b *boots) between(a, c time.Time) bool {
	x, ok := b.after(a)
	return ok && !x.After(c)
}
