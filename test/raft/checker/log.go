package main

// The run log: the ONLY thing the checkers read.
//
// Every harness (difffuzz, crash, kill, flatness) writes the same file so that
// one set of checkers judges all of them, and so that a failed run can be
// judged again, later, by a newer checker. That is the pgless lesson in §13.6:
// a kill run whose evidence is console output cannot be re-judged.
//
// FORMAT: newline-delimited JSON, one event per line, append-only, one file per
// run. Unknown fields are kept (RawExtra) and unknown kinds are an ERROR, not a
// skip: a checker that silently ignores what it does not understand reports
// "no violations" on a log it never read.
//
// ORDERING: `seq` is a per-writer monotone counter, `ts` is the writer's wall
// clock (RFC3339 nanos). Checkers reason with `seq` within one writer and never
// compare wall clocks across writers — the whole point of §7.4 is that only the
// planner's clock is authoritative.
//
// WHO WRITES WHAT: a client-side harness writes what it SENT and what it was
// ANSWERED (push_ok, push_unanswered, delivery, ack_ok …). A node-side collector
// writes what the broker observed (timer_fired, partition_deleted, digest). A
// check that needs both says so in its documentation.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Kind is the event kind. Every kind a checker needs is declared here; a log
// line with a kind that is not in this list fails the load (see LoadLog).
type Kind string

const (
	// Client-side, write path.
	KindPushOK         Kind = "push_ok"         // a push the broker acknowledged (per-item status ok)
	KindPushDuplicate  Kind = "push_duplicate"  // a push the broker answered "duplicate"
	KindPushUnanswered Kind = "push_unanswered" // sent, no usable answer (timeout, 5xx, connection reset)
	KindPushRejected   Kind = "push_rejected"   // the broker refused it (4xx): must NOT be delivered
	// Client-side, read path.
	KindDelivery Kind = "delivery" // one message handed to a consumer
	KindAckOK    Kind = "ack_ok"   // an ack the broker acknowledged
	KindAckFail  Kind = "ack_fail" // an ack the broker refused or that got no answer
	// Transactions, KV, timers, streams.
	KindTxnCommitted Kind = "txn_committed" // a transaction bundle the broker acknowledged
	KindTxnFailed    Kind = "txn_failed"    // a bundle that failed or got no answer
	KindKVOp         Kind = "kv_op"         // one KV operation with its answer
	KindTimerSet     Kind = "timer_set"     // a timer schedule (with its generation)
	KindTimerFired   Kind = "timer_fired"   // observed firing (node-side or via the fired queue)
	KindTimerCancel  Kind = "timer_cancel"  // a cancel the broker acknowledged
	KindStreamState  Kind = "stream_state"  // a streams state read
	// Topology and admin.
	KindPartitionDeleted Kind = "partition_deleted" // a partition/queue delete the broker acknowledged
	KindDLQRow           Kind = "dlq_row"           // a DLQ row as read back
	KindNodeEvent        Kind = "node_event"        // kill, restart, leader change, fault armed (context, never judged alone)
	KindDigest           Kind = "digest"            // a node's state digest at an applied index (§12.9)
	KindNote             Kind = "note"              // free text from the harness; never judged
)

var knownKinds = map[Kind]bool{
	KindPushOK: true, KindPushDuplicate: true, KindPushUnanswered: true, KindPushRejected: true,
	KindDelivery: true, KindAckOK: true, KindAckFail: true,
	KindTxnCommitted: true, KindTxnFailed: true, KindKVOp: true,
	KindTimerSet: true, KindTimerFired: true, KindTimerCancel: true, KindStreamState: true,
	KindPartitionDeleted: true, KindDLQRow: true, KindNodeEvent: true, KindDigest: true, KindNote: true,
}

// Event is one line of the log. Fields are optional by kind; each check names
// the fields it requires and reports a missing one as an ERROR (a check that
// cannot read its input has not passed).
type Event struct {
	Seq  int64  `json:"seq"`
	TS   string `json:"ts"`
	Kind Kind   `json:"kind"`

	// Who produced the line.
	Writer string `json:"writer,omitempty"` // "difffuzz", "kill", "collector@node-1", …
	Node   string `json:"node,omitempty"`   // broker node id, when the line is node-side

	// Message identity. TxnID + PayloadHash is the pair §13.7 names: the id
	// survives redelivery, the hash proves the bytes did not change.
	Queue       string `json:"queue,omitempty"`
	Partition   string `json:"partition,omitempty"`
	PartitionID string `json:"partitionId,omitempty"`
	TxnID       string `json:"txnId,omitempty"`
	MessageID   string `json:"messageId,omitempty"`
	PayloadHash string `json:"payloadHash,omitempty"` // lowercase hex, any algorithm, ONE per run
	Group       string `json:"group,omitempty"`
	LeaseID     string `json:"leaseId,omitempty"`
	Offset      *int64 `json:"offset,omitempty"`
	Attempt     int    `json:"attempt,omitempty"`

	// Acks and transactions.
	Status  string   `json:"status,omitempty"`  // completed | failed | dlq | ok | error …
	TxnRefs []string `json:"txnRefs,omitempty"` // the members of a bundle (txn ids, kv keys, timer ids)

	// KV.
	KVNamespace string          `json:"kvNs,omitempty"`
	KVKey       string          `json:"kvKey,omitempty"`
	KVOp        string          `json:"kvOp,omitempty"` // put | get | delete | incr | cas
	KVExpect    json.RawMessage `json:"kvExpect,omitempty"`
	KVValue     json.RawMessage `json:"kvValue,omitempty"`
	KVVersion   *int64          `json:"kvVersion,omitempty"`
	KVOK        *bool           `json:"kvOk,omitempty"`

	// Timers and streams.
	TimerID    string `json:"timerId,omitempty"`
	Generation *int64 `json:"generation,omitempty"`
	StreamName string `json:"stream,omitempty"`
	Processed  *int64 `json:"processed,omitempty"`

	// Nodes.
	Event      string `json:"event,omitempty"` // kill9 | sigstop | restart | leader | fault …
	AppliedIdx *int64 `json:"appliedIndex,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Text       string `json:"text,omitempty"`

	// Line number in the file, for error messages. Not serialized.
	Line int `json:"-"`
}

// Log is a whole run.
type Log struct {
	Path   string
	Events []Event
}

// LoadLog reads a run log. It is strict on purpose:
//   - a malformed line is an error, never skipped;
//   - an unknown kind is an error, because a checker must not pass a log whose
//     events it does not understand.
func LoadLog(path string) (*Log, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadLog(path, f)
}

// ReadLog is LoadLog over any reader (used by the tests and by `-` for stdin).
func ReadLog(path string, r io.Reader) (*Log, error) {
	l := &Log{Path: path}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20) // a payload hash line is small; a note may not be
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(text), &ev); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		if !knownKinds[ev.Kind] {
			return nil, fmt.Errorf("%s:%d: unknown event kind %q (log.go declares the catalogue)", path, line, ev.Kind)
		}
		ev.Line = line
		l.Events = append(l.Events, ev)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return l, nil
}

// Writer appends events to a run log. Harnesses use it so that every log in
// test/raft/ has the same shape; it is deliberately tiny and synchronous.
type Writer struct {
	w    io.Writer
	name string
	seq  int64
}

func NewWriter(w io.Writer, name string) *Writer { return &Writer{w: w, name: name} }

// Write stamps seq and writer and appends one line.
func (w *Writer) Write(ev Event) error {
	w.seq++
	ev.Seq = w.seq
	if ev.Writer == "" {
		ev.Writer = w.name
	}
	if !knownKinds[ev.Kind] {
		return fmt.Errorf("refusing to write unknown event kind %q", ev.Kind)
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w.w, "%s\n", b)
	return err
}

// Counts summarizes a log, so a run can be sanity-checked before it is judged
// ("0 deliveries" is a broken harness, not a passing run).
func (l *Log) Counts() map[Kind]int {
	out := map[Kind]int{}
	for _, e := range l.Events {
		out[e.Kind]++
	}
	return out
}

func (l *Log) CountsString() string {
	c := l.Counts()
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, c[Kind(k)]))
	}
	return strings.Join(parts, " ")
}
