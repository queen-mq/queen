// When to come back: the runner claims a quiet partition when its earliest
// open window closes, and the engine says when that is. Mirror of the
// next_close tests in clients/client-rust/src/streams/engine.rs.
package operators

import (
	"context"
	"testing"
)

func countFn(acc, _ interface{}) (interface{}, error) {
	n, _ := acc.(float64)
	return n + 1, nil
}

func wantClose(t *testing.T, got *int64, want int64) {
	t.Helper()
	if got == nil {
		t.Fatalf("next close: got none, want %d", want)
	}
	if *got != want {
		t.Fatalf("next close: got %d, want %d", *got, want)
	}
}

func wantNoClose(t *testing.T, got *int64) {
	t.Helper()
	if got != nil {
		t.Fatalf("next close: got %d, want none", *got)
	}
}

func TestNextCloseIsTheEarliestOpenWindowEndPlusGrace(t *testing.T) {
	w := NewWindowTumblingOperator(10)
	var envs []Envelope
	for _, ts := range []int64{5_000, 15_000} {
		out, err := w.Apply(context.Background(), Envelope{
			Msg: map[string]interface{}{"createdAt": tsIso(ts)}, Key: "k", Value: 1.0,
		})
		if err != nil {
			t.Fatal(err)
		}
		envs = append(envs, out...)
	}
	op := ReduceOperator{Fn: countFn, Initial: float64(0)}
	state := map[string]interface{}{}

	// First window: end 10 s + 2 s grace.
	res := op.Run(envs, state, 9_000, w.OperatorTag(), 2_000)
	if len(res.Emits) != 0 {
		t.Fatalf("nothing is ripe at 9 s, got %d emits", len(res.Emits))
	}
	wantClose(t, res.NextCloseMs, 12_000)

	// The first closed: the second is next.
	res = op.Run(envs, state, 12_000, w.OperatorTag(), 2_000)
	if len(res.Emits) != 1 {
		t.Fatalf("expected the first window to close, got %d emits", len(res.Emits))
	}
	wantClose(t, res.NextCloseMs, 22_000)

	// Both closed: nothing to come back for.
	res = op.Run(envs, state, 22_000, w.OperatorTag(), 2_000)
	if len(res.Emits) != 2 {
		t.Fatalf("expected both windows to close, got %d emits", len(res.Emits))
	}
	wantNoClose(t, res.NextCloseMs)
}

func TestASeededWindowLeftUntouchedStillCountsForTheNextClose(t *testing.T) {
	op := ReduceOperator{Fn: countFn, Initial: float64(0)}
	state := map[string]interface{}{
		StateKeyFor("tumb:10", "1970-01-01T00:00:10.000Z", "k"): map[string]interface{}{
			"acc": map[string]interface{}{"count": 1.0}, "windowStart": 10_000.0, "windowEnd": 20_000.0,
		},
	}
	res := op.Run(nil, state, 15_000, "tumb:10", 0)
	if len(res.Emits) != 0 || len(res.StateOps) != 0 {
		t.Fatalf("an open, untouched window neither emits nor writes: %+v", res)
	}
	wantClose(t, res.NextCloseMs, 20_000)
}

func TestASessionsNextCloseIsItsLastEventPlusGapAndGrace(t *testing.T) {
	// Not at 0 ms: a session whose last event is at 0 counts as no session.
	const base = int64(1_000_000)
	w := NewWindowSessionOperator(30, WithGracePeriod(1))
	initial := func() interface{} { return float64(0) }
	envs := []Envelope{
		{Key: "u1", EventTimeMs: base, Value: 1.0},
		{Key: "u1", EventTimeMs: base + 10_000, Value: 1.0},
	}
	res := w.RunSession(envs, map[string]interface{}{}, countFn, initial, base+11_000)
	if len(res.Emits) != 0 {
		t.Fatalf("the session is open, got %d emits", len(res.Emits))
	}
	// Last event + 30 s gap + 1 s grace.
	wantClose(t, res.NextCloseMs, base+41_000)

	// The idle close of the stored session: nothing left open after it.
	state := map[string]interface{}{
		w.OpenSessionStateKey("u1"): map[string]interface{}{
			"acc": 2.0, "sessionStart": float64(base), "lastEventTime": float64(base + 10_000),
		},
	}
	res = w.RunSession(nil, state, countFn, initial, base+40_999)
	if len(res.Emits) != 0 {
		t.Fatalf("still inside the grace, got %d emits", len(res.Emits))
	}
	wantClose(t, res.NextCloseMs, base+41_000)

	res = w.RunSession(nil, state, countFn, initial, base+41_000)
	if len(res.Emits) != 1 || res.Emits[0].Value != 2.0 {
		t.Fatalf("expected the session to close with count 2, got %+v", res.Emits)
	}
	wantNoClose(t, res.NextCloseMs)
}
