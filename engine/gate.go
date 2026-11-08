package engine

import (
	"slices"
	"sync"
	"time"

	"github.com/lesomnus/ward/conf"
)

// Path is the way a node's signals take to Ward: its logs or its metrics. Each
// is watched by a canary of its own.
type Path int

const (
	PathLog Path = iota
	PathMetric
)

func (p Path) String() string {
	if p == PathLog {
		return "log"
	}
	return "metric"
}

// Reasons a node is not observable.
const (
	GateStarting    = "starting"    // not enough canaries yet to say
	GateUnreachable = "unreachable" // the Collector did not answer
	GateError       = "error"       // the Collector answered with an error
	GateSlow        = "slow"        // the Collector answered slower than max_rtt
	GateMissing     = "missing"     // canaries did not come back
	GateRecovering  = "recovering"  // fine again, for less than hold
)

// Gate is the node observation gate (design §7.2): whether everything a node
// sent up to a time has arrived, judged by the canaries Ward sends through the
// node's Collector and the ones that come back.
//
// The canary sender, the receiver's goroutine and the loop all touch it; it
// locks for itself.
type Gate struct {
	cfg conf.Gate

	mu    sync.Mutex
	nodes map[string]*gateNode
}

type gateNode struct {
	sends []*canarySend

	// Per path.
	lastViolation [2]time.Time
	lastReason    [2]string
	ok            [2]bool
	since         [2]time.Time // since when ok, or since when not
	reason        [2]string

	// For NodeService.
	rtt        time.Duration
	dateFailed time.Time
	err        string
	sent       int64
	missing    [2]int64
	gaps       []GateGap
}

type canarySend struct {
	seq    int64
	at     time.Time
	answer [2]bool
	back   [2]bool
}

// GateGap is an interval during which a node was not observable.
type GateGap struct {
	From, To time.Time
	Reason   string
}

// NewGate answers with a gate over the given nodes.
func NewGate(cfg conf.Gate, nodes []string) *Gate {
	g := &Gate{cfg: cfg, nodes: map[string]*gateNode{}}
	for _, n := range nodes {
		g.nodes[n] = &gateNode{reason: [2]string{GateStarting, GateStarting}}
	}
	return g
}

// Sent records a canary sent to a node.
func (g *Gate) Sent(node string, seq int64, at time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[node]
	if !ok {
		return
	}
	n.sends = append(n.sends, &canarySend{seq: seq, at: at})
	n.sent++
}

// Answered records how the node's Collector answered a canary's export on a
// path. reason is empty for an OK; GateUnreachable when the export did not
// reach it, or GateError with what it said.
func (g *Gate) Answered(node string, p Path, seq int64, at time.Time, rtt time.Duration, reason, detail string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[node]
	if !ok {
		return
	}
	if s := n.find(seq); s != nil {
		s.answer[p] = true
	}
	n.rtt = rtt
	if reason == "" && rtt > g.cfg.Canary.MaxRTT {
		reason = GateSlow
	}
	if reason != "" {
		n.lastViolation[p] = at
		n.lastReason[p] = reason
		n.dateFailed = at
		n.err = reason
		if detail != "" {
			n.err = reason + ": " + detail
		}
	}
}

// Returned records a canary that came back through a node's pipeline.
func (g *Gate) Returned(node string, p Path, seq int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[node]
	if !ok {
		return
	}
	if s := n.find(seq); s != nil {
		s.back[p] = true
	}
}

func (n *gateNode) find(seq int64) *canarySend {
	i, ok := slices.BinarySearchFunc(n.sends, seq, func(s *canarySend, seq int64) int {
		switch {
		case s.seq < seq:
			return -1
		case s.seq > seq:
			return 1
		}
		return 0
	})
	if !ok {
		return nil
	}
	return n.sends[i]
}

// Update judges every node's paths at now, the logs up to hLog and the metrics
// up to hMetric. The loop calls it once a tick, before the evaluation reads it.
func (g *Gate) Update(now, hLog, hMetric time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()

	hold := g.cfg.Hold
	for _, n := range g.nodes {
		for p, h := range [2]time.Time{hLog, hMetric} {
			reason := ""

			// Canaries sent within the check window before H that did not come
			// back. One whose export failed is not expected back, and its
			// failure is a violation of its own.
			missing := int64(0)
			from := h.Add(-g.cfg.Canary.CheckWindow)
			for _, s := range n.sends {
				if s.at.Before(from) || s.at.After(h) {
					continue
				}
				if !s.back[p] {
					missing++
				}
			}
			n.missing[p] = missing
			if missing > int64(g.cfg.Canary.MaxMissing) {
				n.lastViolation[p] = now
				n.lastReason[p] = GateMissing
			}

			switch {
			case len(n.sends) == 0 || n.sends[0].at.After(now.Add(-hold)):
				reason = GateStarting
			case !n.lastViolation[p].IsZero() && now.Sub(n.lastViolation[p]) < hold:
				reason = n.lastReason[p]
				if now.Sub(n.lastViolation[p]) > g.cfg.Canary.Interval*2 && n.lastReason[p] != GateMissing {
					reason = GateRecovering
				}
			}

			ok := reason == ""
			if ok != n.ok[p] || n.since[p].IsZero() {
				if !ok && n.ok[p] {
					n.gaps = append(n.gaps, GateGap{From: now, Reason: reason})
				}
				if ok && len(n.gaps) > 0 && n.gaps[len(n.gaps)-1].To.IsZero() {
					n.gaps[len(n.gaps)-1].To = now
				}
				if len(n.gaps) > 32 {
					n.gaps = n.gaps[len(n.gaps)-32:]
				}
				n.ok[p] = ok
				n.since[p] = now
			}
			n.reason[p] = reason
		}

		// Forget sends nothing looks at any more.
		keep := later(now.Add(-hold), hMetric.Add(-g.cfg.Canary.CheckWindow)).Add(-g.cfg.Canary.CheckWindow)
		i := 0
		for i < len(n.sends) && n.sends[i].at.Before(keep) {
			i++
		}
		n.sends = n.sends[i:]
	}
}

// Observable answers whether a node's path is observable now, why not, and
// since when it has been so.
func (g *Gate) Observable(node string, p Path) (bool, string, time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[node]
	if !ok {
		return false, GateStarting, time.Time{}
	}
	return n.ok[p], n.reason[p], n.since[p]
}

// NodeGate is what NodeService reports of a node's gate.
type NodeGate struct {
	Observable bool
	Reason     string
	RTT        time.Duration
	DateFailed time.Time
	Error      string
	Sent       int64
	Missing    int64
	Gaps       []GateGap
}

// Report answers with what NodeService says of a node.
func (g *Gate) Report(node string) NodeGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[node]
	if !ok {
		return NodeGate{Reason: GateStarting}
	}
	r := NodeGate{
		Observable: n.ok[PathLog] && n.ok[PathMetric],
		RTT:        n.rtt,
		DateFailed: n.dateFailed,
		Error:      n.err,
		Sent:       n.sent,
		Missing:    n.missing[PathLog] + n.missing[PathMetric],
		Gaps:       slices.Clone(n.gaps),
	}
	switch {
	case !n.ok[PathLog]:
		r.Reason = PathLog.String() + ": " + n.reason[PathLog]
	case !n.ok[PathMetric]:
		r.Reason = PathMetric.String() + ": " + n.reason[PathMetric]
	}
	return r
}
