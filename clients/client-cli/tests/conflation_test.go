package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Conflation through queenctl, against a live broker (PLAN_CONFLATION.md §4 the
// client-cli row, §7.2 "client-cli/tests/", §7.3 E2E-3 and E2E-4).
//
// The unit suite in ../cmd pins the wire and the degrade-loudly error against a
// fake broker. What it cannot pin is that the flag queenctl sends is the flag
// the SQL reads, that the policy really is per GROUP and not per call, and that
// the depth numbers an operator reads mean what §5.3 says they mean. Those need
// the real thing.

// cflGroup keeps consumer-group names unique per run, so a re-run against a
// shared broker never inherits a group that was already registered with the
// opposite policy — which is exactly the state these tests are about.
func cflGroup(t *testing.T, label string) string {
	t.Helper()
	return fmt.Sprintf("ct-cfl-%s-%d", label, time.Now().UnixNano()%1_000_000)
}

func cflPayloads(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = map[string]any{"i": i}
	}
	return items
}

// TestConflation_DeliversOnlyTheNewest is the feature in one assertion: a
// hundred pending messages in a partition, one delivery, and it is the newest.
func TestConflation_DeliversOnlyTheNewest(t *testing.T) {
	q := uniqueQueue(t, "cfl-newest")
	createQueue(t, q)
	pushNDJSON(t, q, "p0", cflPayloads(100))

	got := popN(t, q, 100, "--cg", cflGroup(t, "newest"), "--from-mode", "all",
		"--conflation", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("conflating pop returned %d messages, want exactly 1", len(got))
	}
	if got[0].Data["i"] != float64(99) {
		t.Errorf("delivered i=%v, want the tail i=99", got[0].Data["i"])
	}
}

// TestConflation_MixedGroupsOnOneQueue is E2E-3: the policy is per consumer
// group, so an auditing reader on the same queue still sees everything. This is
// the property that makes conflation adoptable at all — if it were per queue,
// turning it on would silently blind every other reader.
func TestConflation_MixedGroupsOnOneQueue(t *testing.T) {
	q := uniqueQueue(t, "cfl-mixed")
	createQueue(t, q)
	pushNDJSON(t, q, "p0", cflPayloads(20))

	workers := popN(t, q, 50, "--cg", cflGroup(t, "workers"), "--from-mode", "all",
		"--conflation", "--timeout", "5s")
	if len(workers) != 1 {
		t.Fatalf("conflating group got %d messages, want 1", len(workers))
	}
	if workers[0].Data["i"] != float64(19) {
		t.Errorf("conflating group got i=%v, want 19", workers[0].Data["i"])
	}

	audit := popN(t, q, 50, "--cg", cflGroup(t, "audit"), "--from-mode", "all",
		"--auto-ack", "--timeout", "5s")
	if len(audit) != 20 {
		t.Errorf("non-conflating group got %d messages, want all 20", len(audit))
	}
}

// TestConflation_StoredPolicySurvivesAConsumerThatForgetsTheFlag pins §1.1: the
// declaration is persisted on first registration and the STORED value wins from
// then on. An operator who runs the same command without --conflation the second
// time does not silently un-conflate the group for everyone else.
func TestConflation_StoredPolicySurvivesAConsumerThatForgetsTheFlag(t *testing.T) {
	q := uniqueQueue(t, "cfl-sticky")
	cg := cflGroup(t, "sticky")
	createQueue(t, q)
	pushNDJSON(t, q, "p0", cflPayloads(5))

	first := popN(t, q, 50, "--cg", cg, "--from-mode", "all", "--conflation", "--timeout", "5s")
	if len(first) != 1 {
		t.Fatalf("first (registering) pop got %d, want 1", len(first))
	}
	// Release the lease so the next pop of the same group can claim the
	// partition; --auto-ack is refused alongside conflation by design (§3.3).
	runOK(t, "ack", first[0].TransactionID,
		"--partition-id", first[0].PartitionID,
		"--lease-id", first[0].LeaseID,
		"--cg", cg)

	pushNDJSON(t, q, "p0", cflPayloads(5))

	second := popN(t, q, 50, "--cg", cg, "--from-mode", "all", "--timeout", "5s")
	if len(second) != 1 {
		t.Errorf("a pop without the flag on a conflating group got %d messages, want 1 "+
			"(the stored policy wins)", len(second))
	}
}

// TestConflation_DeclarationConflictWarnsAndKeepsWorking is E2E-4 from the
// queenctl side. The group was registered WITHOUT conflation, so a later
// --conflation loses: the broker answers conflationConflict, keeps serving the
// full batch, and queenctl says so once. Failing here instead would take down
// exactly the half of a rolling fleet that is already correct (§3.3 Q3).
func TestConflation_DeclarationConflictWarnsAndKeepsWorking(t *testing.T) {
	q := uniqueQueue(t, "cfl-conflict")
	cg := cflGroup(t, "conflict")
	createQueue(t, q)
	pushNDJSON(t, q, "p0", cflPayloads(5))

	plain := popN(t, q, 50, "--cg", cg, "--from-mode", "all", "--auto-ack", "--timeout", "5s")
	if len(plain) != 5 {
		t.Fatalf("registering pop got %d, want all 5", len(plain))
	}

	pushNDJSON(t, q, "p0", cflPayloads(5))

	stdout, stderr, code := run("pop", q, "-n", "50", "-o", "ndjson",
		"--cg", cg, "--from-mode", "all", "--conflation", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("a declaration conflict must not fail the pop: exit %d\nstdout: %s\nstderr: %s",
			code, stdout, stderr)
	}
	if got := len(parseNDJSONMessages(t, stdout)); got != 5 {
		t.Errorf("conflicting pop delivered %d messages, want all 5 (stored policy is non-conflating)", got)
	}
	low := strings.ToLower(stderr)
	if !strings.Contains(low, "conflation.conflict") || !strings.Contains(low, cg) {
		t.Errorf("queenctl swallowed the declaration conflict for %s; stderr was:\n%s", cg, stderr)
	}
}

// TestConflation_DepthSeparatesLogDepthFromWorkDepth pins §5.3, the number that
// decides whether somebody gets paged. `pending` counts log positions still to
// retire; `effectivePending` counts handler runs still owed. For a conflating
// group they diverge by orders of magnitude and only the second one is an
// incident signal.
func TestConflation_DepthSeparatesLogDepthFromWorkDepth(t *testing.T) {
	q := uniqueQueue(t, "cfl-depth")
	cg := cflGroup(t, "depth")
	createQueue(t, q)
	for p := 0; p < 3; p++ {
		pushNDJSON(t, q, fmt.Sprintf("p%d", p), cflPayloads(50))
	}

	// Registers the group as conflating; the lease is left open on purpose so
	// the cursor does not move and the backlog stays measurable.
	if got := popN(t, q, 100, "--cg", cg, "--from-mode", "all", "--conflation",
		"--max-partitions", "3", "--timeout", "5s"); len(got) == 0 {
		t.Fatal("registering pop delivered nothing")
	}

	var raw map[string]any
	runJSON(t, &raw, "queue", "depth", q, "--group", cg, "-o", "json")
	if raw["conflation"] != true {
		t.Errorf("depth does not report the group as conflating: %v", raw["conflation"])
	}
	if raw["pending"] != float64(150) {
		t.Errorf("pending (log depth) = %v, want 150", raw["pending"])
	}
	if raw["partitionsPending"] != float64(3) {
		t.Errorf("partitionsPending = %v, want 3", raw["partitionsPending"])
	}
	if raw["effectivePending"] != float64(3) {
		t.Errorf("effectivePending (work depth) = %v, want 3 — one handler run per partition",
			raw["effectivePending"])
	}

	// The summary view has to carry it too, or the operator reading a terminal
	// sees only the scary number.
	var row map[string]any
	runJSON(t, &row, "queue", "depth", q, "--group", cg, "-o", "table")
	if row["effective"] != float64(3) {
		t.Errorf("depth summary effective = %v, want 3", row["effective"])
	}
	if row["partitionsPending"] != float64(3) {
		t.Errorf("depth summary partitionsPending = %v, want 3", row["partitionsPending"])
	}
	if row["conflation"] != true {
		t.Errorf("depth summary lost the conflation flag: %v", row)
	}
}

// TestConflation_RefusedWithAutoAck and _WithoutConsumerGroup pin the two
// combinations §3.3 rejects outright. Both are consumer bugs whose silent form
// is unfixable in production — auto-ack commits at delivery, which turns the
// whole point of conflation ("the newest state is definitely processed") into
// at-most-once, and queue mode has no group identity to hang a policy on.
func TestConflation_RefusedWithAutoAck(t *testing.T) {
	q := uniqueQueue(t, "cfl-autoack")
	createQueue(t, q)

	stdout, stderr, code := run("pop", q, "--cg", cflGroup(t, "autoack"),
		"--conflation", "--auto-ack", "--wait=false", "--timeout", "2s")
	if code == 0 || code == 4 {
		t.Fatalf("conflation + --auto-ack must be refused, got exit %d\nstdout: %s", code, stdout)
	}
	if !strings.Contains(strings.ToLower(stderr), "autoack") {
		t.Errorf("refusal does not name the reason:\n%s", stderr)
	}
}

func TestConflation_RefusedWithoutConsumerGroup(t *testing.T) {
	q := uniqueQueue(t, "cfl-nogroup")
	createQueue(t, q)

	stdout, stderr, code := run("pop", q, "--conflation", "--wait=false", "--timeout", "2s")
	if code == 0 || code == 4 {
		t.Fatalf("conflation without --cg must be refused, got exit %d\nstdout: %s", code, stdout)
	}
	if !strings.Contains(strings.ToLower(stderr), "consumergroup") {
		t.Errorf("refusal does not name the reason:\n%s", stderr)
	}
}

// TestConflation_TailStreamsOnlyTheNewest is the same guarantee through the
// OTHER param builder. `tail` runs the consume loop, whose query string is
// assembled by different code than `pop` uses (§4) — the standing hazard in
// every SDK in this repo.
func TestConflation_TailStreamsOnlyTheNewest(t *testing.T) {
	q := uniqueQueue(t, "cfl-tail")
	cg := cflGroup(t, "tail")
	createQueue(t, q)
	pushNDJSON(t, q, "p0", cflPayloads(30))

	stdout, stderr, code := run("tail", q, "--cg", cg, "--conflation",
		"--from", "all", "-n", "1", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("tail --conflation: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	msgs := parseNDJSONMessages(t, stdout)
	if len(msgs) != 1 {
		t.Fatalf("tail emitted %d messages, want 1", len(msgs))
	}
	if msgs[0].Data["i"] != float64(29) {
		t.Errorf("tail emitted i=%v, want the tail i=29", msgs[0].Data["i"])
	}
}
