package verify

import (
	"os"
	"strings"
	"testing"
)

// log builds a stage log body from "prop,seq" pairs.
func log(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func run(t *testing.T, body string, produced map[int]int64, ackErr, baseSeq int64) StageResult {
	t.Helper()
	r, err := parse(strings.NewReader(body), "t_g.log", "A", produced, ackErr, baseSeq)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return r
}

func TestCleanStreamPasses(t *testing.T) {
	r := run(t, log("1,1", "1,2", "1,3", "2,1", "2,2"), nil, 0, 1)
	if !r.Pass || r.Gaps != 0 || r.Dups != 0 || r.Viols != 0 {
		t.Fatalf("clean stream should PASS clean, got %+v", r)
	}
	if r.Msgs != 5 || r.Unique != 5 || r.Props != 2 {
		t.Fatalf("counts wrong: %+v", r)
	}
}

func TestGapBelowFrontierFails(t *testing.T) {
	// seq 2 never arrives but 3 does: 2 was lost, not in flight.
	r := run(t, log("1,1", "1,3"), nil, 0, 1)
	if r.Gaps != 1 || !r.Fail {
		t.Fatalf("expected 1 gap + FAIL, got %+v", r)
	}
}

func TestMissingTailIsInflightNotGap(t *testing.T) {
	// Producer assigned up to 5, only 1..3 delivered: the tail is in flight.
	r := run(t, log("1,1", "1,2", "1,3"), map[int]int64{1: 5}, 0, 1)
	if r.Gaps != 0 || !r.Pass {
		t.Fatalf("tail must not be a gap, got %+v", r)
	}
	if r.Inflight != 2 {
		t.Fatalf("expected inflight=2, got %d", r.Inflight)
	}
}

func TestDuplicateCountedNotFatal(t *testing.T) {
	r := run(t, log("1,1", "1,2", "1,2", "1,3"), nil, 0, 1)
	if r.Dups != 1 {
		t.Fatalf("expected 1 dup, got %+v", r)
	}
	if r.Unique != 3 || !r.Pass {
		t.Fatalf("dups alone must not fail the stage: %+v", r)
	}
}

func TestReorderFailsWhenAcksWereClean(t *testing.T) {
	r := run(t, log("1,1", "1,3", "1,2"), nil, 0, 1)
	// 1,3 then 1,2 = first occurrence of a lower seq after a higher one.
	if r.Viols != 1 {
		t.Fatalf("expected 1 violation, got %+v", r)
	}
	if !r.Fail {
		t.Fatalf("reorder with ackErr=0 must FAIL, got %+v", r)
	}
}

func TestReorderExcusedWhenAcksFailed(t *testing.T) {
	r := run(t, log("1,1", "1,3", "1,2"), nil, 7, 1)
	if r.Viols != 1 {
		t.Fatalf("expected 1 violation, got %+v", r)
	}
	if r.Fail {
		t.Fatalf("reorder with ackErr>0 is excusable as redelivery, got %+v", r)
	}
}

func TestRedeliveryDedupedBeforeOrderCheck(t *testing.T) {
	// A plain at-least-once redelivery of an already-seen seq must NOT be read
	// as an ordering violation — that is what "dedup by first occurrence" buys.
	r := run(t, log("1,1", "1,2", "1,3", "1,2"), nil, 0, 1)
	if r.Viols != 0 {
		t.Fatalf("redelivery must not count as a violation, got %+v", r)
	}
	if r.Dups != 1 || !r.Pass {
		t.Fatalf("expected 1 dup and PASS, got %+v", r)
	}
}

// TestClampPreventsGapCancellation is the soundness regression test for the
// per-property clamp. Property 1 carries a seq BELOW baseSeq (a warm-up seq 0
// verified against a meta that defaults baseSeq=1), which yields a negative
// shortfall term. Property 2 has a genuine loss. Without the clamp the negative
// term cancels the real gap and the stage flips FAIL -> PASS.
func TestClampPreventsGapCancellation(t *testing.T) {
	body := log(
		"1,0", "1,1", "1,2", // spans [0,2] but baseSeq=1 => shortfall -1
		"2,1", "2,3", // genuine loss of seq 2 => +1
	)
	r := run(t, body, nil, 0, 1)
	if r.Gaps != 1 {
		t.Fatalf("real gap must survive the negative term: expected gaps=1, got %+v", r)
	}
	if !r.Fail {
		t.Fatalf("stage with a real loss must FAIL, got %+v", r)
	}
}

func TestBaseSeqZeroAcceptsWarmupSeq(t *testing.T) {
	r := run(t, log("1,0", "1,1", "1,2"), nil, 0, 0)
	if r.Gaps != 0 || !r.Pass {
		t.Fatalf("warm-up stream with baseSeq=0 should be clean, got %+v", r)
	}
}

func TestMalformedLinesIgnored(t *testing.T) {
	r := run(t, log("1,1", "garbage", ",5", "2,x", "1,2"), nil, 0, 1)
	if r.Msgs != 2 || r.Unique != 2 || !r.Pass {
		t.Fatalf("malformed lines must be skipped, got %+v", r)
	}
}

// TestEmptyStreamIsFailNotPass guards the soundness hole found on 2026-08-02:
// a broken pipeline writes no lines, an empty log has no gaps and no
// violations, and the run would otherwise be reported as a flawless PASS.
func TestEmptyStreamIsFailNotPass(t *testing.T) {
	dir := t.TempDir()
	stages := []Stage{
		{Topic: "cm-avail", Group: "cm-db", Flow: "A"},
		{Topic: "cm-prices", Group: "cm-cal", Flow: "B"},
	}
	// One healthy stream, one that received nothing.
	if err := os.WriteFile(dir+"/cm-avail_cm-db.log", []byte("1,1\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/cm-prices_cm-cal.log", nil, 0o644); err != nil {
		t.Fatal(err)
	}

	rep, err := Run(dir, stages, 0, true)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Pass {
		t.Fatal("a run with an empty stream must not PASS")
	}
	if len(rep.EmptyStreams) != 1 || rep.EmptyStreams[0] != "cm-prices_cm-cal" {
		t.Fatalf("empty stream not reported: %+v", rep.EmptyStreams)
	}
	if rep.Gaps != 0 {
		t.Fatalf("the empty stream must fail on emptiness, not by inventing gaps: %d", rep.Gaps)
	}
}
