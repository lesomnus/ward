package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/ward/server/pd"
)

// States of a Condition, and of an Issue.
const (
	// A Condition's only: Ward expects it (a Component's member names it),
	// and nothing of it has been seen yet.
	StateUnobserved = "unobserved"

	StateInactive = "inactive"
	StatePending  = "pending"
	StateActive   = "active"
	StateStale    = "stale"
	StateCleared  = "cleared"

	// An Issue's only: the interval is over.
	StateClosed = "closed"
)

// Why an Issue closed. resolved.* says the problem ended; the others say Ward
// stopped being able to follow it, which is not the same thing.
const (
	ReasonSilence       = "resolved.silence"
	ReasonExplicit      = "resolved.explicit"
	ReasonCondition     = "resolved.condition"
	ReasonQuiet         = "quiet"
	ReasonSourceRestart = "interrupted.source_restart"
	ReasonReboot        = "interrupted.reboot"
	ReasonDiagGap       = "interrupted.diag_gap"
	ReasonRuleChanged   = "rule_changed"
)

// Transitions of an Issue: the evaluation's, then an operator's.
const (
	EventOpen    = "open"
	EventUpdate  = "update"
	EventClear   = "clear"
	EventClose   = "close"
	EventAck     = "ack"
	EventComment = "comment"
	EventReset   = "reset"
)

// Kinds of an Override.
const (
	OverrideShelveTimed   = "shelve.timed"
	OverrideShelveOneShot = "shelve.one_shot"
	OverrideOutOfService  = "out_of_service"
)

// Issue is one interval during which a Condition was active, as the engine
// holds it. A zero time is a date that is not set.
type Issue struct {
	ID [16]byte

	Rule, Source, Node, Key string

	Severity, Level, State string

	DateOpened, DateSeen time.Time
	DateCleared          time.Time
	DateClosed           time.Time
	CloseReason          string

	Count, Restarts int64

	Attention, Acked     bool
	DateAcked, DateReset time.Time
	Suppressed, Shelved  bool
	OutOfService         bool

	// From the rule: whether it asks for an acknowledgement.
	AckRequired bool

	// Whether count, date_seen or restarts moved since the Store last had
	// them. They move with every signal, which is not a transition; a
	// checkpoint writes them.
	dirty bool
}

// open reports whether the Issue's interval has not ended for good: active,
// stale, or cleared and waiting for a reset.
func (i *Issue) open() bool { return i.State != StateClosed }

// attend works out whether somebody should look at the Issue: it is not closed,
// or it asks for an acknowledgement it has not had.
func (i *Issue) attend() {
	i.Attention = i.State != StateClosed || (i.AckRequired && !i.Acked)
}

// Transition is one change of an Issue, with the Issue as it is after it.
type Transition struct {
	Event   string
	Date    time.Time
	Comment string
	Issue   Issue

	// Ctx is the operator's request, for an operator's transition: its write
	// goes on the trail under that RPC. Nil for the loop's own.
	Ctx context.Context
}

// OverrideChange is an operator's change to how a Condition is handled: set
// (or replace) the Override of a kind, or take it away.
type OverrideChange struct {
	Delete  bool
	Rule    string
	Node    string
	Key     string
	Kind    string
	Until   time.Time
	Comment string
	Ctx     context.Context
}

// condKey names a Condition.
type condKey struct{ rule, node, key string }

// IssueID is the identifier of the Issue a Condition opens at a time: a hash of
// the four, laid out as payday's identifier of the Issue domain, so that the
// same signals open the same Issue after Ward restarts and the Store refuses a
// second one.
func IssueID(rule, node, key string, opened time.Time) [16]byte {
	h := sha256.New()
	for _, s := range []string{rule, node, key, strconv.FormatInt(opened.UnixNano(), 10)} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)

	var id [16]byte
	copy(id[:], sum)
	// payday's layout: a millisecond timestamp, version 8, the variant, and the
	// domain at byte 9 (pdid's package comment).
	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(opened.UnixMilli()))
	copy(id[0:6], ms[2:8])
	id[6] = 0x80 | (id[6] & 0x0F)
	id[8] = 0x80 | (id[8] & 0x3F)
	id[9] = byte(pd.IssueDomain)
	return id
}

// keyOf joins the values of a Key's parts.
func keyOf(parts []string) string { return strings.Join(parts, "/") }

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
