package main

// The checks of PLAN_RAFT.md §13.7.
//
// Three states, never two: PASS, FAIL and SKIP. SKIP is what keeps this honest
// — a check whose input the log does not contain (no drain note, no digests, no
// timer events) reports SKIP with the reason, and the report prints it next to
// the passes. A checker that quietly returns "no violations" on a log it could
// not judge is how a broken harness looks green for a month.
//
// Each check states: what it reads, what it proves, and what it CANNOT prove
// from a client-side log alone.

import (
	"fmt"
	"sort"
	"strings"
)

type State string

const (
	Pass State = "PASS"
	Fail State = "FAIL"
	Skip State = "SKIP"
)

// Violation is one counter-example. It always names a line of the log, because
// a violation without a line is an opinion.
type Violation struct {
	Line   int    `json:"line"`
	Seq    int64  `json:"seq"`
	What   string `json:"what"`
	Detail string `json:"detail"`
}

func (v Violation) String() string {
	return fmt.Sprintf("line %d (seq %d): %s — %s", v.Line, v.Seq, v.What, v.Detail)
}

// Result is one check's verdict.
type Result struct {
	ID         string      `json:"id"`
	Title      string      `json:"title"`
	State      State       `json:"state"`
	Reason     string      `json:"reason,omitempty"`   // why it was skipped
	Evidence   string      `json:"evidence,omitempty"` // what it actually judged: counts
	Violations []Violation `json:"violations,omitempty"`
}

// Check is one entry of the catalogue.
type Check struct {
	ID          string
	Title       string // the §13.7 bullet, quoted
	Implemented bool
	Note        string // for a stub: what it still owes, and what it will need
	Run         func(*Log) Result
}

// Checks is the catalogue, in the order §13.7 lists them.
var Checks = []Check{
	{
		ID:          "delivery-at-least-once",
		Title:       "every acknowledged push delivered at least once (transaction id + payload hash)",
		Implemented: true,
		Run:         checkAtLeastOnce,
	},
	{
		ID:          "payload-hash",
		Title:       "no payload mismatch",
		Implemented: true,
		Run:         checkPayloadHash,
	},
	{
		ID:    "offsets-monotone",
		Title: "delivered offsets monotone per partition within a lease",
		Note: "reads delivery events with offset+leaseId; needs the offset field on the pop answer " +
			"(present on the log engine, absent on some postgres answers — the harness must record it or the check SKIPs)",
	},
	{
		ID:    "txn-atomic",
		Title: "transaction bundles all or nothing",
		Note: "reads txn_committed/txn_failed with txnRefs, then requires every member visible (delivery, kv_op get, timer) " +
			"for a committed bundle and none for a failed one; a failed-with-no-answer bundle is UNKNOWN and must be resolved " +
			"by reading the final views, not by assuming",
	},
	{
		ID:    "kv-linearizable",
		Title: "KV compare-and-set histories linearizable (porcupine model)",
		Note: "needs an invocation/response pair per kv_op (the log carries one line today) and the porcupine dependency; " +
			"the model is: per (ns,key) a register with version, cas succeeds iff expect == current",
	},
	{
		ID:    "timer-once",
		Title: "each timer schedule generation fired exactly once",
		Note: "reads timer_set/timer_cancel/timer_fired keyed by (timerId, generation); the fired events must come from a " +
			"node-side collector or the fired queue, so a client-only log SKIPs",
	},
	{
		ID:    "streams-count",
		Title: "streams state equal to the processed message count",
		Note:  "reads stream_state.processed against the deliveries acked completed for that stream's queue",
	},
	{
		ID:    "no-delivery-after-delete",
		Title: "nothing delivered after its partition was deleted",
		Note: "reads partition_deleted then any delivery for that partitionId with a later seq FROM THE SAME WRITER " +
			"(cross-writer ordering needs the leader-stamped now, §7.4)",
	},
	{
		ID:    "dlq-payload",
		Title: "DLQ rows carry the payload that was pushed",
		Note:  "reads dlq_row.payloadHash against the push_ok hash for the same txnId; the read-back is the harness's job",
	},
}

func CheckByID(id string) (Check, bool) {
	for _, c := range Checks {
		if c.ID == id {
			return c, true
		}
	}
	return Check{}, false
}

func CheckIDs() []string {
	out := make([]string, 0, len(Checks))
	for _, c := range Checks {
		out = append(out, c.ID)
	}
	return out
}

// RunChecks runs the named checks (all implemented ones when ids is empty).
func RunChecks(l *Log, ids []string) ([]Result, error) {
	var selected []Check
	if len(ids) == 0 {
		for _, c := range Checks {
			if c.Implemented {
				selected = append(selected, c)
			}
		}
	} else {
		for _, id := range ids {
			c, ok := CheckByID(id)
			if !ok {
				return nil, fmt.Errorf("unknown check %q (known: %s)", id, strings.Join(CheckIDs(), " "))
			}
			if !c.Implemented {
				return nil, fmt.Errorf("check %q is a documented stub (checks.go): %s", id, c.Note)
			}
			selected = append(selected, c)
		}
	}
	out := make([]Result, 0, len(selected))
	for _, c := range selected {
		r := c.Run(l)
		r.ID, r.Title = c.ID, c.Title
		out = append(out, r)
	}
	return out, nil
}

// ------------------------------------------------------------------- check 1

// checkAtLeastOnce: every acknowledged push must have been delivered at least
// once to every consumer group that drained its queue.
//
// SCOPE, and why it is explicit: a push is only required to have been delivered
// if the run claims to have drained the queue. The harness states that with a
// note line:
//
//	{"kind":"note","text":"drain-complete","group":"g0","queue":"q1"}
//
// written after it has popped that (group, queue) to empty. Without such a note
// the check SKIPs: "still in the queue" and "lost" look identical in a log that
// stopped early, and calling that a violation would train everybody to ignore
// this checker.
//
// A push_duplicate is NOT required to be delivered (its original is). A
// push_unanswered is not required either — it may or may not have happened,
// which is the whole reason D6 exists; it is counted and reported as context.
func checkAtLeastOnce(l *Log) Result {
	type scope struct{ group, queue string }
	drained := map[scope]int64{} // scope -> seq of the drain note
	for _, e := range l.Events {
		if e.Kind == KindNote && e.Text == "drain-complete" && e.Group != "" && e.Queue != "" {
			drained[scope{e.Group, e.Queue}] = e.Seq
		}
	}
	if len(drained) == 0 {
		return Result{State: Skip, Reason: `no {"kind":"note","text":"drain-complete","group":…,"queue":…} line: ` +
			`the log does not claim any (group, queue) was drained, so "undelivered" cannot be told from "still queued"`}
	}

	// txnId -> the acknowledged push (first one wins; a duplicate refers to it).
	pushed := map[string]Event{}
	unanswered := 0
	for _, e := range l.Events {
		switch e.Kind {
		case KindPushOK:
			if _, seen := pushed[e.TxnID]; !seen {
				pushed[e.TxnID] = e
			}
		case KindPushUnanswered:
			unanswered++
		}
	}
	// (txnId, group) -> delivered
	delivered := map[string]bool{}
	for _, e := range l.Events {
		if e.Kind == KindDelivery {
			delivered[e.TxnID+"\x00"+e.Group] = true
		}
	}

	var viol []Violation
	judged := 0
	for sc, drainSeq := range drained {
		for txn, p := range pushed {
			if p.Queue != sc.queue || p.Seq > drainSeq {
				continue
			}
			judged++
			if !delivered[txn+"\x00"+sc.group] {
				viol = append(viol, Violation{
					Line: p.Line, Seq: p.Seq, What: "acknowledged push never delivered",
					Detail: fmt.Sprintf("txn=%s queue=%s partition=%s group=%s (push acknowledged at seq %d, %s drained at seq %d)",
						txn, p.Queue, p.Partition, sc.group, p.Seq, sc.group, drainSeq),
				})
			}
		}
	}
	sortViolations(viol)
	res := Result{
		State: Pass,
		Evidence: fmt.Sprintf("%d (push, group) pairs judged over %d drained scope(s), %d acknowledged pushes, %d unanswered pushes (not judged here)",
			judged, len(drained), len(pushed), unanswered),
		Violations: viol,
	}
	if judged == 0 {
		return Result{State: Skip, Reason: "drain notes name no (group, queue) that any acknowledged push belongs to", Evidence: res.Evidence}
	}
	if len(viol) > 0 {
		res.State = Fail
	}
	return res
}

// ------------------------------------------------------------------- check 2

// checkPayloadHash: the bytes delivered are the bytes pushed.
//
// It judges three things, all from (txnId, payloadHash):
//  1. a delivery whose hash differs from the push's hash — payload corruption
//     or a crossed message;
//  2. a delivery of a transaction id that was REJECTED (4xx) — a refused write
//     that came back;
//  3. a delivery of a transaction id the log never mentions — a phantom; a
//     delivery of an UNANSWERED push is not a phantom (D6) and is counted
//     separately.
//
// DLQ rows are judged the same way, which is the §13.7 bullet "DLQ rows carry
// the payload that was pushed" for the hash half; the row read-back itself is
// the dlq-payload check.
func checkPayloadHash(l *Log) Result {
	hash := map[string]string{}   // txnId -> pushed hash
	rejected := map[string]bool{} // txnId -> refused
	unanswered := map[string]bool{}
	for _, e := range l.Events {
		switch e.Kind {
		case KindPushOK, KindPushDuplicate:
			if e.TxnID == "" {
				continue
			}
			if h, seen := hash[e.TxnID]; seen && e.PayloadHash != "" && h != "" && h != e.PayloadHash {
				// Two different payloads under one transaction id: the harness
				// asked the broker a question the checker cannot judge.
				hash[e.TxnID] = "" // "ambiguous": deliveries of it are not judged
				continue
			}
			if e.PayloadHash != "" {
				hash[e.TxnID] = e.PayloadHash
			}
		case KindPushRejected:
			rejected[e.TxnID] = true
		case KindPushUnanswered:
			unanswered[e.TxnID] = true
		}
	}

	var viol []Violation
	judged, phantomUnanswered, noHash := 0, 0, 0
	for _, e := range l.Events {
		if e.Kind != KindDelivery && e.Kind != KindDLQRow {
			continue
		}
		if rejected[e.TxnID] {
			viol = append(viol, Violation{
				Line: e.Line, Seq: e.Seq, What: "delivered a push the broker had refused",
				Detail: fmt.Sprintf("txn=%s queue=%s group=%s kind=%s", e.TxnID, e.Queue, e.Group, e.Kind),
			})
			continue
		}
		want, known := hash[e.TxnID]
		if !known {
			if unanswered[e.TxnID] {
				phantomUnanswered++
				continue
			}
			viol = append(viol, Violation{
				Line: e.Line, Seq: e.Seq, What: "delivered a transaction id that was never pushed",
				Detail: fmt.Sprintf("txn=%s queue=%s group=%s kind=%s", e.TxnID, e.Queue, e.Group, e.Kind),
			})
			continue
		}
		if want == "" || e.PayloadHash == "" {
			noHash++
			continue
		}
		judged++
		if want != e.PayloadHash {
			viol = append(viol, Violation{
				Line: e.Line, Seq: e.Seq, What: "payload mismatch",
				Detail: fmt.Sprintf("txn=%s queue=%s group=%s pushed=%s delivered=%s", e.TxnID, e.Queue, e.Group, want, e.PayloadHash),
			})
		}
	}
	sortViolations(viol)
	if judged == 0 && len(viol) == 0 {
		return Result{State: Skip, Reason: "no delivery carries a payloadHash that can be matched to a push (record payloadHash on both sides)",
			Evidence: fmt.Sprintf("%d deliveries without a comparable hash", noHash)}
	}
	res := Result{
		State: Pass,
		Evidence: fmt.Sprintf("%d deliveries/DLQ rows hashed and matched, %d skipped for a missing hash, %d deliveries of an unanswered push (allowed, D6)",
			judged, noHash, phantomUnanswered),
		Violations: viol,
	}
	if len(viol) > 0 {
		res.State = Fail
	}
	return res
}

func sortViolations(v []Violation) {
	sort.SliceStable(v, func(i, j int) bool { return v[i].Line < v[j].Line })
}
