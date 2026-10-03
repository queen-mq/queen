package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// StampSeq messages are valid JSON with ts, src and seq in front; ParseSeq reads them back; ParseStamp still works
// on them (the reader's e2e path); Transform keeps the head and appends "by".
func TestStampSeqParseTransform(t *testing.T) {
	p := NewPayloadPool(1, 256, 10, 7)
	ms := p.StampSeq(1759300000123456, 10, 990)
	if len(ms) != 10 {
		t.Fatalf("%d messages", len(ms))
	}
	for j, m := range ms {
		var ev map[string]any
		if err := json.Unmarshal(m, &ev); err != nil {
			t.Fatalf("message %d not JSON: %v: %s", j, err, m)
		}
		if ev["seq"].(float64) != float64(990+j) || ev["src"].(float64) != 7 || ev["ts"].(float64) != 1759300000123456 {
			t.Fatalf("message %d head: %v", j, ev)
		}
		ts, src, seq, ok := ParseSeq(m)
		if !ok || ts != 1759300000123456 || src != 7 || seq != int64(990+j) {
			t.Fatalf("ParseSeq(%d) = %d %d %d %v", j, ts, src, seq, ok)
		}
		if ts2, src2, ok2 := ParseStamp(m); !ok2 || ts2 != ts || src2 != src {
			t.Fatalf("ParseStamp on a txn message: %d %d %v", ts2, src2, ok2)
		}
		x := Transform(m, 1234)
		var ev2 map[string]any
		if err := json.Unmarshal(x, &ev2); err != nil {
			t.Fatalf("transformed not JSON: %v: %s", err, x)
		}
		if ev2["by"].(float64) != 1234 || ev2["seq"].(float64) != float64(990+j) || len(ev2) != len(ev)+1 {
			t.Fatalf("transform: %v", ev2)
		}
		if _, _, seq2, ok := ParseSeq(x); !ok || seq2 != seq {
			t.Fatalf("ParseSeq after Transform: %d %v", seq2, ok)
		}
	}
	if _, _, _, ok := ParseSeq(p.Stamp(5, 1)[0]); ok {
		t.Fatal("ParseSeq accepted a message without seq")
	}
	if _, _, _, ok := ParseSeq(p.Warm()); ok {
		t.Fatal("ParseSeq accepted a warm message")
	}
	if got := string(Transform([]byte(`{}`), 3)); got != `{"by":3}` {
		t.Fatalf("Transform({}) = %s", got)
	}
}

// The ledger round-trips through its file, and the verdict classifies every case: exactly once, a duplicate, a
// re-read of the same position, missing, still pending, in out AND pending, extra, ambiguous.
func TestLedgerAndVerdict(t *testing.T) {
	dir := t.TempDir()
	l0 := &IdLedger{}
	l0.Reserve(10)            // seqs 0..9
	l0.Set(0, 8, IdConfirmed) // 0..7 confirmed
	l0.Set(8, 1, IdFailed)    // 8 failed; 9 never answered
	l1 := &IdLedger{}
	l1.Reserve(3)
	l1.Set(0, 3, IdConfirmed)
	if c, f, u, err := l0.WriteFile(filepath.Join(dir, "p0.ids"), 0); err != nil || c != 8 || f != 1 || u != 1 {
		t.Fatalf("write p0: %d %d %d %v", c, f, u, err)
	}
	if _, _, _, err := l1.WriteFile(filepath.Join(dir, "p1.ids"), 1); err != nil {
		t.Fatal(err)
	}
	e, err := LoadIds(dir)
	if err != nil || e.Files != 2 || len(e.Srcs[0]) != 10 || e.Srcs[0][8] != IdFailed || e.Srcs[0][9] != IdLaunched {
		t.Fatalf("load: %+v %v", e, err)
	}
	pool := NewPayloadPool(1, 64, 1, 0)
	msg := func(src int, seq int64) []byte {
		p := NewPayloadPool(1, 64, 1, src)
		_ = pool
		return Transform(p.StampSeq(1, 1, seq)[0], 0)
	}
	ta := NewTally(e)
	for s := int64(0); s < 5; s++ { // src 0: 0..4 once
		ta.Out(msg(0, s), PosHash("out", "0", string(rune('a'+s))))
	}
	ta.Out(msg(0, 1), PosHash("out", "0", "b"))  // re-read of seq 1 (same position)
	ta.Out(msg(0, 2), PosHash("out", "9", "zz")) // duplicate of seq 2
	ta.Pending(msg(0, 5))                        // 5 still in in
	ta.Pending(msg(0, 4))                        // 4 in out AND pending
	ta.Out(msg(0, 8), PosHash("x"))              // ambiguous, present
	ta.Out(msg(0, 12), PosHash("y"))             // beyond the ledger: extra
	ta.Out(msg(1, 0), PosHash("z0"))
	ta.Out(msg(1, 2), PosHash("z2")) // src 1 seq 1 missing
	ta.Out(pool.Warm(), PosHash("w"))
	v := ta.Verdict("test")
	want := Verdict{Produced: 11, Ambiguous: 2, OutUnique: 9, Duplicates: 1, DupRecords: 1, Missing: 4, PendingIn: 1,
		InAndOut: 1, Extra: 1, AmbigInOut: 1, Rereads: 1, Warm: 1}
	// missing: src0 6,7 + src1 1 = 3 ... plus src0 seq 3? no: 0..4 are in out. src0 confirmed 0..7: out 0..4, pending 5,
	// missing 6, 7; src1: missing 1 => 3 missing.
	want.Missing = 3
	if v.Produced != want.Produced || v.Ambiguous != want.Ambiguous || v.OutUnique != want.OutUnique ||
		v.Duplicates != want.Duplicates || v.DupRecords != want.DupRecords || v.Missing != want.Missing ||
		v.PendingIn != want.PendingIn || v.InAndOut != want.InAndOut || v.Extra != want.Extra ||
		v.AmbigInOut != want.AmbigInOut || v.Rereads != want.Rereads || v.Warm != want.Warm || v.Pass {
		t.Fatalf("verdict %+v\nwant     %+v", *v, want)
	}
	// a clean run passes
	tb := NewTally(e)
	for s := int64(0); s < 8; s++ {
		tb.Out(msg(0, s), PosHash("p", string(rune('a'+s))))
	}
	for s := int64(0); s < 3; s++ {
		tb.Out(msg(1, s), PosHash("q", string(rune('a'+s))))
	}
	if v := tb.Verdict("test"); !v.Pass || v.Missing != 0 || v.Duplicates != 0 || v.OutUnique != 11 {
		t.Fatalf("clean verdict %+v", *v)
	}
}

// In -txn mode the pacer stamps seqs, every launched message gets one, and the ledger records each unit's fate.
func TestTxnRunLedger(t *testing.T) {
	c := testConfig(t, "-rate", "2000", "-batch", "5", "-max-inflight", "1000", "-ramp", "0", "-duration", "500ms", "-report", "1h")
	r, _ := NewRun(c, "test")
	out := filepath.Join(t.TempDir(), "p.ids")
	r.EnableTxn(out)
	seen := map[int64]bool{}
	var mu sync.Mutex
	r.Produce(context.Background(), time.Now(), func(u *Unit) {
		mu.Lock()
		defer mu.Unlock()
		for j, m := range u.Payloads {
			_, _, seq, ok := ParseSeq(m)
			if !ok || seq != u.FirstSeq+int64(j) || seen[seq] {
				t.Errorf("unit %d message %d: seq %d ok=%v", u.Seq, j, seq, ok)
			}
			seen[seq] = true
		}
		for i := 0; i < u.N; i++ {
			var err error
			if u.Seq == 2 {
				err = context.DeadlineExceeded
			}
			u.MsgDone(err)
		}
	})
	r.WaitInflight(2 * time.Second)
	r.StopReporter(time.Now())
	r.Finish(time.Now())
	e, err := LoadIds(filepath.Dir(out))
	if err != nil {
		t.Fatal(err)
	}
	st := e.Srcs[0]
	if int64(len(st)) != r.offeredMsgs.Load()-r.shedMsgs.Load() {
		t.Fatalf("ledger has %d seqs for %d launched messages", len(st), r.offeredMsgs.Load()-r.shedMsgs.Load())
	}
	nf := 0
	for _, f := range st {
		if f == IdFailed {
			nf++
		} else if f != IdConfirmed {
			t.Fatalf("fate %d", f)
		}
	}
	if nf != 5 {
		t.Fatalf("%d failed seqs, want 5 (one unit)", nf)
	}
	_ = os.Remove(out)
}
