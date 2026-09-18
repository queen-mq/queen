package main

// `GOWORK=off go test ./...` runs the same cases as `-selftest`, one per test,
// plus the catalogue invariants that only make sense from a test binary.

import (
	"strings"
	"testing"
)

func TestWriterRoundTrip(t *testing.T) {
	if err := checkWriterRoundTrip(); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownKindRefused(t *testing.T) {
	if err := checkUnknownKindRefused(); err != nil {
		t.Fatal(err)
	}
}

func TestSkipWithoutDrain(t *testing.T) {
	if err := checkSkipWithoutDrain(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanRunPasses(t *testing.T) {
	if err := checkAtLeastOnceGreen(); err != nil {
		t.Fatal(err)
	}
}

func TestLostMessageCaught(t *testing.T) {
	if err := checkAtLeastOnceRed(); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptedPayloadCaught(t *testing.T) {
	if err := checkPayloadHashRed(); err != nil {
		t.Fatal(err)
	}
}

func TestPhantomDeliveryCaught(t *testing.T) {
	if err := checkPhantomRed(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogueCoversSection137(t *testing.T) {
	// The nine bullets of §13.7, by id. Every one must be in the catalogue,
	// implemented or as a stub with a note saying what it owes.
	want := []string{
		"delivery-at-least-once", "offsets-monotone", "payload-hash", "txn-atomic",
		"kv-linearizable", "timer-once", "streams-count", "no-delivery-after-delete", "dlq-payload",
	}
	for _, id := range want {
		c, ok := CheckByID(id)
		if !ok {
			t.Errorf("check %q from §13.7 is missing from the catalogue", id)
			continue
		}
		if !c.Implemented && c.Note == "" {
			t.Errorf("stub %q has no note: -list-checks would print a blank line", id)
		}
		if c.Implemented && c.Run == nil {
			t.Errorf("check %q claims to be implemented and has no body", id)
		}
	}
	if len(Checks) != len(want) {
		t.Errorf("the catalogue has %d checks, §13.7 lists %d", len(Checks), len(want))
	}
}

func TestSelectingAStubIsAnError(t *testing.T) {
	l, err := buildLog(drainedRun(false, false, false)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunChecks(l, []string{"kv-linearizable"}); err == nil || !strings.Contains(err.Error(), "stub") {
		t.Fatalf("selecting a stub gave %v, want an error naming it a stub", err)
	}
	if _, err := RunChecks(l, []string{"no-such-check"}); err == nil {
		t.Fatal("an unknown check id was accepted")
	}
}
