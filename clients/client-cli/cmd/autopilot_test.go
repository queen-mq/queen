package cmd

import (
	"strings"
	"testing"
)

// Pop autopilot, queenctl side.
//
// The SDK's one rule is that an explicit user value is sacred, and a CLI is
// where that rule is easiest to break: a flag has a DEFAULT, and a default is
// not a user value. Before autopilot the two were indistinguishable on the wire
// — an omitted `partitions` meant 1, which is exactly what the flag's default
// said — so `if maxParts > 1` was a correct gate. It stopped being one the
// moment an omitted `partitions` started meaning "broker, you choose": that
// gate would have handed a typed `--max-partitions 1` to the autopilot to
// widen, silently turning a single-partition claim into a sweep.
//
// So the gate is now Flags().Changed, and these tests pin both halves of it
// against the real cobra tree and the fake broker in conflation_test.go.

// --- --max-partitions: typed pins, untouched delegates ----------------------

func TestPopPinsTypedMaxPartitionsEvenAtOne(t *testing.T) {
	// `pop` pins the batch too (--limit defaults to 1), so with the sweep width
	// typed as well there is nothing left for autopilot to decide: the request
	// is the byte-identical legacy one, no autopilot flag, and the broker
	// applies its own default of one partition. THAT is the pin — a request
	// that never delegated anything cannot be widened.
	fb := newFakeBroker(t, popBody("ap-pop-pin", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "ap-pop-pin", "--cg", "workers",
		"--max-partitions", "1", "--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := fb.firstPop(t).Query
	if _, present := q["autopilot"]; present {
		t.Fatalf("autopilot must not be sent when both knobs are pinned (raw: %v)", q.Encode())
	}
	if got, present := q["partitions"], q.Has("partitions"); present && got[0] != "1" {
		t.Fatalf("partitions = %q, want absent or \"1\" (raw: %v)", got[0], q.Encode())
	}
}

func TestPopSendsTypedMaxPartitionsOfOneWhenTheBatchIsDelegated(t *testing.T) {
	// Same typed --max-partitions 1, but with --limit 0 the batch is nobody's
	// decision, so autopilot engages for it -- and now the pin HAS to travel,
	// because an omitted partitions would be the broker's to choose.
	fb := newFakeBroker(t, popBody("ap-pop-pin-explicit", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "ap-pop-pin-explicit", "--cg", "workers",
		"--max-partitions", "1", "--limit", "0", "--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := fb.firstPop(t).Query
	if got := q.Get("partitions"); got != "1" {
		t.Fatalf("partitions = %q, want \"1\": a typed --max-partitions 1 must pin the claim (raw: %v)", got, q.Encode())
	}
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("autopilot = %q, want \"true\" (raw: %v)", got, q.Encode())
	}
	if _, present := q["batch"]; present {
		t.Fatalf("batch must not be sent when --limit is 0 and --batch untyped (raw: %v)", q.Encode())
	}
}

func TestPopLeavesMaxPartitionsToTheBrokerWhenUntouched(t *testing.T) {
	fb := newFakeBroker(t, popBody("ap-pop-auto", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "ap-pop-auto", "--cg", "workers",
		"--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := fb.firstPop(t).Query
	if _, present := q["partitions"]; present {
		t.Fatalf("partitions must not be sent when the flag was not typed (raw: %v)", q.Encode())
	}
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("autopilot = %q, want \"true\" (raw: %v)", got, q.Encode())
	}
	// pop's --limit defaults to 1 and always pins the batch: that is this
	// command's own semantics and autopilot does not touch it.
	if got := q.Get("batch"); got != "1" {
		t.Fatalf("batch = %q, want \"1\" (--limit still pins it) (raw: %v)", got, q.Encode())
	}
}

func TestPopPassesTypedMaxPartitionsAboveOne(t *testing.T) {
	fb := newFakeBroker(t, popBody("ap-pop-wide", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "ap-pop-wide", "--cg", "workers",
		"--max-partitions", "16", "--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := fb.firstPop(t).Query
	if got := q.Get("partitions"); got != "16" {
		t.Fatalf("partitions = %q, want \"16\" (raw: %v)", got, q.Encode())
	}
	// Both knobs pinned (batch via --limit) leaves autopilot nothing to do, so
	// the request is the one queenctl sent before this feature existed.
	if _, present := q["autopilot"]; present {
		t.Fatalf("autopilot must not be sent when both knobs are pinned (raw: %v)", q.Encode())
	}
}

func TestTailPinsTypedMaxPartitionsEvenAtOne(t *testing.T) {
	// `tail` runs the consume loop, a DIFFERENT param builder from the one
	// `pop` uses. A pop-only test would pass with a tail that dropped the pin.
	fb := newFakeBroker(t, popBody("ap-tail-pin", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "ap-tail-pin", "--cg", "workers",
		"--max-partitions", "1", "-n", "1", "--timeout", "2s")
	stdout, _ := stop()
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if !strings.Contains(stdout, `"transactionId"`) {
		t.Fatalf("tail printed no message; stdout=%q", stdout)
	}

	q := fb.firstPop(t).Query
	if got := q.Get("partitions"); got != "1" {
		t.Fatalf("partitions = %q, want \"1\": a typed --max-partitions 1 must pin the claim (raw: %v)", got, q.Encode())
	}
}

func TestTailLeavesBothKnobsToTheBrokerWhenUntouched(t *testing.T) {
	// tail's --batch defaults to 0 (unset) and --max-partitions is untyped, so
	// this is the fully delegated shape: autopilot=true and neither knob.
	fb := newFakeBroker(t, popBody("ap-tail-auto", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "ap-tail-auto", "--cg", "workers",
		"-n", "1", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("tail: %v", err)
	}

	q := fb.firstPop(t).Query
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("autopilot = %q, want \"true\" (raw: %v)", got, q.Encode())
	}
	for _, absent := range []string{"partitions", "batch"} {
		if _, present := q[absent]; present {
			t.Fatalf("%s must not be sent when the flag was not typed (raw: %v)", absent, q.Encode())
		}
	}
}

func TestTailPinsTypedBatch(t *testing.T) {
	fb := newFakeBroker(t, popBody("ap-tail-batch", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "ap-tail-batch", "--cg", "workers",
		"--batch", "25", "-n", "1", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("tail: %v", err)
	}

	q := fb.firstPop(t).Query
	if got := q.Get("batch"); got != "25" {
		t.Fatalf("batch = %q, want \"25\" (raw: %v)", got, q.Encode())
	}
	// Batch pinned, sweep width still the broker's.
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("autopilot = %q, want \"true\" (raw: %v)", got, q.Encode())
	}
	if _, present := q["partitions"]; present {
		t.Fatalf("partitions must not be sent when the flag was not typed (raw: %v)", q.Encode())
	}
}

// --- the harness itself -----------------------------------------------------

func TestTypedFlagDoesNotLeakIntoTheNextRun(t *testing.T) {
	// pflag records "was it typed" on the flag object, and the cobra tree is a
	// package singleton: without the reset in resetConflationFlags, the run
	// below would inherit the previous run's --max-partitions and pin a claim
	// nobody asked to pin. This test exists because that failure is invisible —
	// the second run simply sends a parameter, and every assertion about it
	// passes for the wrong reason.
	fb := newFakeBroker(t, popBody("ap-leak-1", ""), popBody("ap-leak-2", ""))
	stop := captureStdio(t)

	if err := runCLI(t, fb.url(), "pop", "ap-leak-1", "--cg", "workers",
		"--max-partitions", "8", "--wait=false", "--timeout", "2s"); err != nil {
		t.Fatalf("first pop: %v", err)
	}
	if err := runCLI(t, fb.url(), "pop", "ap-leak-2", "--cg", "workers",
		"--wait=false", "--timeout", "2s"); err != nil {
		t.Fatalf("second pop: %v", err)
	}
	stop()

	pops := fb.pops()
	if len(pops) != 2 {
		t.Fatalf("expected 2 pops, got %d", len(pops))
	}
	if got := pops[0].Query.Get("partitions"); got != "8" {
		t.Fatalf("first pop partitions = %q, want \"8\"", got)
	}
	if _, present := pops[1].Query["partitions"]; present {
		t.Fatalf("the typed flag leaked into the next run (raw: %v)", pops[1].Query.Encode())
	}
}
