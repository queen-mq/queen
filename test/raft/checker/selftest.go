package main

// The offline smoke test, shared by `-selftest` and `go test`.
//
// Every case builds a small log IN MEMORY and asserts the verdict, including
// the negative cases: a checker is only worth running if a planted violation
// turns it red. The four cases here are the four ways this checker could lie:
//
//   1. it passes a log it could not read            -> unknown kind must ERROR
//   2. it passes a log it did not judge             -> SKIP with a reason
//   3. it misses a lost message                     -> FAIL naming the txn
//   4. it misses corrupted bytes                    -> FAIL naming the hashes

import (
	"bytes"
	"fmt"
	"strings"
)

func SelfTest() error {
	for _, c := range []struct {
		name string
		fn   func() error
	}{
		{"log round-trips through the writer", checkWriterRoundTrip},
		{"an unknown event kind is an error", checkUnknownKindRefused},
		{"a log with no drain note is skipped, not passed", checkSkipWithoutDrain},
		{"a drained run with every message delivered passes", checkAtLeastOnceGreen},
		{"a lost message is caught", checkAtLeastOnceRed},
		{"a corrupted payload is caught", checkPayloadHashRed},
		{"a phantom delivery is caught", checkPhantomRed},
	} {
		if err := c.fn(); err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
	}
	return nil
}

// buildLog writes events through the real Writer and reads them back through
// the real loader, so the selftest exercises the format, not a struct literal.
func buildLog(events ...Event) (*Log, error) {
	var buf bytes.Buffer
	w := NewWriter(&buf, "selftest")
	for _, e := range events {
		if err := w.Write(e); err != nil {
			return nil, err
		}
	}
	return ReadLog("<selftest>", &buf)
}

func drainedRun(withHashMismatch, dropDelivery, phantom bool) []Event {
	evs := []Event{
		{Kind: KindPushOK, Queue: "q", Partition: "p0", TxnID: "t1", PayloadHash: "aa"},
		{Kind: KindPushOK, Queue: "q", Partition: "p0", TxnID: "t2", PayloadHash: "bb"},
		{Kind: KindPushDuplicate, Queue: "q", Partition: "p0", TxnID: "t2", PayloadHash: "bb"},
		{Kind: KindDelivery, Queue: "q", Partition: "p0", TxnID: "t1", PayloadHash: "aa", Group: "g0"},
	}
	if !dropDelivery {
		h := "bb"
		if withHashMismatch {
			h = "cc"
		}
		evs = append(evs, Event{Kind: KindDelivery, Queue: "q", Partition: "p0", TxnID: "t2", PayloadHash: h, Group: "g0"})
	}
	if phantom {
		evs = append(evs, Event{Kind: KindDelivery, Queue: "q", Partition: "p0", TxnID: "never-pushed", PayloadHash: "dd", Group: "g0"})
	}
	return append(evs, Event{Kind: KindNote, Text: "drain-complete", Group: "g0", Queue: "q"})
}

func checkWriterRoundTrip() error {
	l, err := buildLog(drainedRun(false, false, false)...)
	if err != nil {
		return err
	}
	if len(l.Events) != 6 {
		return fmt.Errorf("wrote 6 events, read back %d", len(l.Events))
	}
	if l.Events[0].Seq != 1 || l.Events[5].Seq != 6 {
		return fmt.Errorf("seq is not 1..6: %d..%d", l.Events[0].Seq, l.Events[5].Seq)
	}
	if l.Events[0].Writer != "selftest" {
		return fmt.Errorf("writer was not stamped: %q", l.Events[0].Writer)
	}
	if !strings.Contains(l.CountsString(), "push_ok=2") {
		return fmt.Errorf("counts do not summarize the log: %s", l.CountsString())
	}
	return nil
}

func checkUnknownKindRefused() error {
	_, err := ReadLog("<inline>", strings.NewReader(`{"kind":"push_ok","txnId":"t"}`+"\n"+`{"kind":"teleport","txnId":"t"}`+"\n"))
	if err == nil {
		return fmt.Errorf("an unknown kind was accepted")
	}
	if !strings.Contains(err.Error(), "teleport") {
		return fmt.Errorf("the error does not name the kind: %v", err)
	}
	if w := NewWriter(&bytes.Buffer{}, "x"); w.Write(Event{Kind: "teleport"}) == nil {
		return fmt.Errorf("the writer accepted an unknown kind")
	}
	return nil
}

func checkSkipWithoutDrain() error {
	evs := drainedRun(false, false, false)
	evs = evs[:len(evs)-1] // drop the drain note
	l, err := buildLog(evs...)
	if err != nil {
		return err
	}
	r, err := one(l, "delivery-at-least-once")
	if err != nil {
		return err
	}
	if r.State != Skip {
		return fmt.Errorf("state is %s, want SKIP", r.State)
	}
	if r.Reason == "" {
		return fmt.Errorf("a SKIP with no reason is a silent pass")
	}
	return nil
}

func checkAtLeastOnceGreen() error {
	l, err := buildLog(drainedRun(false, false, false)...)
	if err != nil {
		return err
	}
	for _, id := range []string{"delivery-at-least-once", "payload-hash"} {
		r, err := one(l, id)
		if err != nil {
			return err
		}
		if r.State != Pass {
			return fmt.Errorf("%s is %s (%s) on a clean log: %v", id, r.State, r.Reason, r.Violations)
		}
		if r.Evidence == "" {
			return fmt.Errorf("%s passed without saying what it judged", id)
		}
	}
	return nil
}

func checkAtLeastOnceRed() error {
	l, err := buildLog(drainedRun(false, true, false)...)
	if err != nil {
		return err
	}
	r, err := one(l, "delivery-at-least-once")
	if err != nil {
		return err
	}
	if r.State != Fail {
		return fmt.Errorf("a lost message left the check %s", r.State)
	}
	if len(r.Violations) != 1 || !strings.Contains(r.Violations[0].Detail, "t2") {
		return fmt.Errorf("the violation does not name the lost txn: %+v", r.Violations)
	}
	return nil
}

func checkPayloadHashRed() error {
	l, err := buildLog(drainedRun(true, false, false)...)
	if err != nil {
		return err
	}
	r, err := one(l, "payload-hash")
	if err != nil {
		return err
	}
	if r.State != Fail {
		return fmt.Errorf("a corrupted payload left the check %s", r.State)
	}
	if !strings.Contains(r.Violations[0].Detail, "pushed=bb") || !strings.Contains(r.Violations[0].Detail, "delivered=cc") {
		return fmt.Errorf("the violation does not quote both hashes: %+v", r.Violations[0])
	}
	return nil
}

func checkPhantomRed() error {
	l, err := buildLog(drainedRun(false, false, true)...)
	if err != nil {
		return err
	}
	r, err := one(l, "payload-hash")
	if err != nil {
		return err
	}
	if r.State != Fail {
		return fmt.Errorf("a phantom delivery left the check %s", r.State)
	}
	if !strings.Contains(r.Violations[0].What, "never pushed") {
		return fmt.Errorf("the violation is not the phantom one: %+v", r.Violations[0])
	}
	return nil
}

func one(l *Log, id string) (Result, error) {
	rs, err := RunChecks(l, []string{id})
	if err != nil {
		return Result{}, err
	}
	if len(rs) != 1 {
		return Result{}, fmt.Errorf("check %q produced %d results", id, len(rs))
	}
	return rs[0], nil
}
