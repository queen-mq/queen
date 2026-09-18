package main

// The checker run log (test/raft/checker §13.7): difffuzz emits ONE JSONL file
// per side so the checker can judge each broker's behaviour on its own —
// at-least-once delivery and payload-hash — over the same sequence the
// differential run drove. This is the phase-1 substitute for the final-view
// comparison, which the raft1 router of WP-1.7a does not serve (every
// /api/v1/resources, /api/v1/messages and /api/v1/dlq route answers 503
// raft_phase1_unsupported until WP-2.6): the run log records what each side
// DELIVERED and ACKED, which the pop and ack answers DO carry.
//
// The format is checker/log.go's, byte for byte (the two are separate Go
// modules — README.md: "one module each, no vendoring" — so this cannot import
// that Event type; it mirrors the JSON tags and pins them with a test that a
// checker binary loads what this writes). Only the kinds the checker's
// implemented checks read are emitted: push_ok, push_duplicate, push_rejected,
// delivery, ack_ok, ack_fail, dlq_row, note.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// logEvent mirrors checker/log.go's Event, with only the fields difffuzz sets.
type logEvent struct {
	Seq         int64  `json:"seq"`
	TS          string `json:"ts"`
	Kind        string `json:"kind"`
	Writer      string `json:"writer,omitempty"`
	Queue       string `json:"queue,omitempty"`
	Partition   string `json:"partition,omitempty"`
	PartitionID string `json:"partitionId,omitempty"`
	TxnID       string `json:"txnId,omitempty"`
	MessageID   string `json:"messageId,omitempty"`
	PayloadHash string `json:"payloadHash,omitempty"`
	Group       string `json:"group,omitempty"`
	LeaseID     string `json:"leaseId,omitempty"`
	Offset      *int64 `json:"offset,omitempty"`
	Attempt     int    `json:"attempt,omitempty"`
	Status      string `json:"status,omitempty"`
	Text        string `json:"text,omitempty"`
}

// knownLogKinds is checker/log.go's catalogue of the kinds difffuzz writes. The
// checker rejects a kind it does not know, so a typo here would fail the load —
// which is exactly what TestCheckerLogKindsAreKnown asserts against.
var knownLogKinds = map[string]bool{
	"push_ok": true, "push_duplicate": true, "push_rejected": true,
	"delivery": true, "ack_ok": true, "ack_fail": true, "dlq_row": true, "note": true,
}

// RunLog is one side's checker log. Nil is a no-op sink, so every call site can
// stay unconditional whether -logdir was given or not.
type RunLog struct {
	w    io.Writer
	name string
	seq  int64
}

func NewRunLog(w io.Writer, name string) *RunLog {
	if w == nil {
		return nil
	}
	return &RunLog{w: w, name: name}
}

func (l *RunLog) emit(ev logEvent) {
	if l == nil {
		return
	}
	l.seq++
	ev.Seq = l.seq
	ev.TS = time.Now().UTC().Format(time.RFC3339Nano)
	ev.Writer = l.name
	if !knownLogKinds[ev.Kind] {
		// A programming error, not run data: fail loud so it is caught by a test
		// rather than by the checker rejecting the log after a 2000-seed campaign.
		panic(fmt.Sprintf("difffuzz: refusing to write unknown checker-log kind %q", ev.Kind))
	}
	b, err := json.Marshal(ev)
	if err != nil {
		panic(fmt.Sprintf("difffuzz: marshal checker-log event: %v", err))
	}
	_, _ = l.w.Write(append(b, '\n'))
}

// Push records the broker's per-item verdict. status is the per-item wire status
// ("queued"/"created" => push_ok, "duplicate" => push_duplicate); a rejected
// push (HTTP 4xx) is recorded with pushRejected instead.
func (l *RunLog) push(queue, partition, txn, msgID, payloadHash, status string) {
	kind := "push_ok"
	if strings.EqualFold(status, "duplicate") {
		kind = "push_duplicate"
	}
	l.emit(logEvent{Kind: kind, Queue: queue, Partition: partition, TxnID: txn, MessageID: msgID, PayloadHash: payloadHash})
}

func (l *RunLog) pushRejected(queue, partition, txn string) {
	l.emit(logEvent{Kind: "push_rejected", Queue: queue, Partition: partition, TxnID: txn})
}

func (l *RunLog) delivery(queue, partitionID, txn, msgID, group, leaseID, payloadHash string, offset *int64, attempt int) {
	l.emit(logEvent{
		Kind: "delivery", Queue: queue, PartitionID: partitionID, TxnID: txn, MessageID: msgID,
		Group: group, LeaseID: leaseID, PayloadHash: payloadHash, Offset: offset, Attempt: attempt,
	})
}

func (l *RunLog) ack(queue, partitionID, txn, group, status string, ok bool) {
	kind := "ack_ok"
	if !ok {
		kind = "ack_fail"
	}
	l.emit(logEvent{Kind: kind, Queue: queue, PartitionID: partitionID, TxnID: txn, Group: group, Status: status})
}

func (l *RunLog) dlqRow(queue, partitionID, txn, group, payloadHash string) {
	l.emit(logEvent{Kind: "dlq_row", Queue: queue, PartitionID: partitionID, TxnID: txn, Group: group, PayloadHash: payloadHash})
}

// drainComplete is the scope note the checker's at-least-once reads: after it,
// every acknowledged push to (group, queue) at or before this seq must have been
// delivered to group.
func (l *RunLog) drainComplete(group, queue string) {
	l.emit(logEvent{Kind: "note", Text: "drain-complete", Group: group, Queue: queue})
}

func (l *RunLog) note(text string) { l.emit(logEvent{Kind: "note", Text: text}) }

// payloadHashOf hashes a payload for the checker: sha256 over its CANONICAL JSON,
// so whitespace and key order between what was pushed and what was delivered do
// not read as a mismatch while a changed value still does. A payload delivered
// as a JSON STRING that itself parses as JSON (some wire shapes wrap `data` that
// way) is unwrapped once, so a push object and its delivery match regardless of
// which shape the broker chose. A payload that is not JSON is hashed as its raw
// bytes.
func payloadHashOf(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(canonicalPayload(raw)))
	return hex.EncodeToString(sum[:])
}

func canonicalPayload(raw json.RawMessage) string {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(raw) // not JSON: hash the bytes as they are
	}
	// Unwrap a string that is itself JSON (a wrapped `data` field).
	if s, ok := v.(string); ok {
		inner := json.NewDecoder(strings.NewReader(s))
		inner.UseNumber()
		var iv any
		if err := inner.Decode(&iv); err == nil {
			if _, isStr := iv.(string); !isStr {
				return Canonical(iv)
			}
		}
	}
	return Canonical(v)
}
