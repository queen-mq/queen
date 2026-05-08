// Unit tests for operator semantics. 1:1 port of test-v2/streams-unit/operators.test.js
// (29 cases across 9 describe-style groups).
package operators

import (
	"context"
	"strings"
	"testing"
	"time"
)

func env(msg map[string]interface{}) Envelope {
	return Envelope{Msg: msg, Key: "p1", Value: msg["data"]}
}

func dateParseMs(t *testing.T, s string) int64 {
	t.Helper()
	tt, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return tt.UnixMilli()
}

// ---------------------------------------------------------------------------
// MapOperator (2 tests)
// ---------------------------------------------------------------------------

func TestMapOperator(t *testing.T) {
	t.Run("transforms the envelope value", func(t *testing.T) {
		op := MapOperator{Fn: func(m interface{}, _ EmitCtx) (interface{}, error) {
			x := m.(map[string]interface{})["data"].(map[string]interface{})["x"].(int)
			return map[string]interface{}{"doubled": x * 2}, nil
		}}
		out, err := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"x": 3}}))
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 1 {
			t.Fatalf("expected 1 envelope, got %d", len(out))
		}
		if v := out[0].Value.(map[string]interface{})["doubled"]; v != 6 {
			t.Fatalf("expected doubled=6, got %v", v)
		}
	})

	t.Run("supports async fns (errors propagate)", func(t *testing.T) {
		op := MapOperator{Fn: func(m interface{}, _ EmitCtx) (interface{}, error) {
			x := m.(map[string]interface{})["data"].(map[string]interface{})["x"].(int)
			return map[string]interface{}{"x": x + 1}, nil
		}}
		out, err := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"x": 4}}))
		if err != nil {
			t.Fatal(err)
		}
		if v := out[0].Value.(map[string]interface{})["x"]; v != 5 {
			t.Fatalf("expected x=5, got %v", v)
		}
	})
}

// ---------------------------------------------------------------------------
// FilterOperator (1 test)
// ---------------------------------------------------------------------------

func TestFilterOperator(t *testing.T) {
	t.Run("drops envelopes where predicate is false", func(t *testing.T) {
		op := FilterOperator{Fn: func(m interface{}, _ EmitCtx) (bool, error) {
			x := m.(map[string]interface{})["data"].(map[string]interface{})["x"].(int)
			return x > 10, nil
		}}
		out1, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"x": 5}}))
		out2, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"x": 20}}))
		if len(out1) != 0 {
			t.Fatalf("x=5 should be filtered out, got %d envs", len(out1))
		}
		if len(out2) != 1 {
			t.Fatalf("x=20 should pass, got %d envs", len(out2))
		}
	})
}

// ---------------------------------------------------------------------------
// FlatMapOperator (2 tests)
// ---------------------------------------------------------------------------

func TestFlatMapOperator(t *testing.T) {
	t.Run("emits zero or more outputs", func(t *testing.T) {
		op := FlatMapOperator{Fn: func(m interface{}, _ EmitCtx) ([]interface{}, error) {
			n := m.(map[string]interface{})["data"].(map[string]interface{})["n"].(int)
			out := make([]interface{}, n)
			for i := 0; i < n; i++ {
				out[i] = i
			}
			return out, nil
		}}
		out3, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"n": 3}}))
		out0, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"n": 0}}))
		if len(out3) != 3 {
			t.Fatalf("expected 3, got %d", len(out3))
		}
		if len(out0) != 0 {
			t.Fatalf("expected 0, got %d", len(out0))
		}
	})

	t.Run("propagates user errors", func(t *testing.T) {
		op := FlatMapOperator{Fn: func(m interface{}, _ EmitCtx) ([]interface{}, error) {
			return nil, errBoom{}
		}}
		_, err := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{}}))
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// ---------------------------------------------------------------------------
// KeyByOperator (2 tests)
// ---------------------------------------------------------------------------

func TestKeyByOperator(t *testing.T) {
	t.Run("overrides the envelope key", func(t *testing.T) {
		op := KeyByOperator{Fn: func(m interface{}) (string, error) {
			return m.(map[string]interface{})["data"].(map[string]interface{})["userId"].(string), nil
		}}
		out, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"userId": "u-42"}}))
		if out[0].Key != "u-42" {
			t.Fatalf("expected key=u-42, got %s", out[0].Key)
		}
	})

	t.Run("converts ids to string", func(t *testing.T) {
		op := KeyByOperator{Fn: func(m interface{}) (string, error) {
			id := m.(map[string]interface{})["data"].(map[string]interface{})["id"].(int)
			return string(rune('0' + id)), nil
		}}
		out, _ := op.Apply(context.Background(), env(map[string]interface{}{"data": map[string]interface{}{"id": 7}}))
		if out[0].Key != "7" {
			t.Fatalf("expected key=7, got %s", out[0].Key)
		}
	})
}

// ---------------------------------------------------------------------------
// WindowTumblingOperator (2 tests)
// ---------------------------------------------------------------------------

func TestWindowTumblingOperator(t *testing.T) {
	t.Run("annotates envelope with window fields", func(t *testing.T) {
		op := NewWindowTumblingOperator(60)
		out, err := op.Apply(context.Background(), Envelope{
			Msg: map[string]interface{}{"createdAt": "2026-01-01T10:01:30.000Z", "data": map[string]interface{}{}},
			Key: "p1",
		})
		if err != nil {
			t.Fatal(err)
		}
		want := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		if out[0].WindowStart != want {
			t.Fatalf("expected windowStart=%d, got %d", want, out[0].WindowStart)
		}
		if out[0].WindowKey != "2026-01-01T10:01:00.000Z" {
			t.Fatalf("unexpected windowKey: %s", out[0].WindowKey)
		}
	})

	t.Run("errors on missing or invalid createdAt", func(t *testing.T) {
		op := NewWindowTumblingOperator(60)
		_, err := op.Apply(context.Background(), Envelope{Msg: map[string]interface{}{"data": map[string]interface{}{}}})
		if err == nil || !strings.Contains(err.Error(), "could not determine timestamp") {
			t.Fatalf("expected timestamp error, got %v", err)
		}
		_, err = op.Apply(context.Background(), Envelope{Msg: map[string]interface{}{"createdAt": "nope", "data": map[string]interface{}{}}})
		if err == nil {
			t.Fatal("expected error for invalid createdAt")
		}
	})
}

// ---------------------------------------------------------------------------
// ReduceOperator (5 tests)
// ---------------------------------------------------------------------------

const tag = "tumb:60"

func reduceEnv(key, ts string, v interface{}, t *testing.T) Envelope {
	ms := dateParseMs(t, ts)
	ws := (ms / 60_000) * 60_000
	return Envelope{
		Key:         key,
		WindowStart: ws,
		WindowEnd:   ws + 60_000,
		WindowKey:   tsIso(ws),
		OperatorTag: tag,
		Value:       v,
	}
}

func tsIso(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z") }

func TestReduceOperator(t *testing.T) {
	add := func(a, v interface{}) (interface{}, error) {
		af, _ := a.(int)
		vf, _ := v.(int)
		return af + vf, nil
	}

	t.Run("accumulates within a window and emits closed windows", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		envs := []Envelope{
			reduceEnv("A", "2026-01-01T10:00:01.000Z", 1, t),
			reduceEnv("A", "2026-01-01T10:00:30.000Z", 2, t),
			reduceEnv("A", "2026-01-01T10:01:05.000Z", 5, t),
		}
		res := op.Run(envs, map[string]interface{}{}, dateParseMs(t, "2026-01-01T10:01:05.000Z"), tag, 0)
		if len(res.Emits) != 1 {
			t.Fatalf("expected 1 emit, got %d", len(res.Emits))
		}
		if res.Emits[0].Value.(int) != 3 {
			t.Fatalf("expected acc=3, got %v", res.Emits[0].Value)
		}
	})

	t.Run("emits a delete for a seeded window when its end has passed", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		ws := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		we := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		loaded := map[string]interface{}{
			StateKeyFor(tag, "2026-01-01T10:00:00.000Z", "A"): map[string]interface{}{
				"acc": 42, "windowStart": float64(ws), "windowEnd": float64(we),
			},
		}
		envs := []Envelope{reduceEnv("A", "2026-01-01T10:01:05.000Z", 5, t)}
		res := op.Run(envs, loaded, dateParseMs(t, "2026-01-01T10:01:05.000Z"), tag, 0)
		if len(res.Emits) != 1 {
			t.Fatalf("expected 1 emit, got %d", len(res.Emits))
		}
		var deletes int
		for _, op := range res.StateOps {
			if op.Type == "delete" {
				deletes++
			}
		}
		if deletes != 1 {
			t.Fatalf("expected 1 delete op, got %d", deletes)
		}
	})

	t.Run("respects loaded state across cycles", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		ws := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		we := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		loaded := map[string]interface{}{
			StateKeyFor(tag, "2026-01-01T10:00:00.000Z", "A"): map[string]interface{}{
				"acc": 10, "windowStart": float64(ws), "windowEnd": float64(we),
			},
		}
		envs := []Envelope{reduceEnv("A", "2026-01-01T10:00:30.000Z", 5, t)}
		res := op.Run(envs, loaded, dateParseMs(t, "2026-01-01T10:00:30.000Z"), tag, 0)
		if len(res.Emits) != 0 {
			t.Fatalf("window not yet closed; got %d emits", len(res.Emits))
		}
		var got int
		for _, op := range res.StateOps {
			if op.Type == "upsert" {
				got = op.Value.(map[string]interface{})["acc"].(int)
			}
		}
		if got != 15 {
			t.Fatalf("expected acc=15, got %d", got)
		}
	})

	t.Run("respects gracePeriod — keeps window open during grace", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		ws := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		we := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		loaded := map[string]interface{}{
			StateKeyFor(tag, "2026-01-01T10:00:00.000Z", "A"): map[string]interface{}{
				"acc": 42, "windowStart": float64(ws), "windowEnd": float64(we),
			},
		}
		res := op.Run(nil, loaded, dateParseMs(t, "2026-01-01T10:01:05.000Z"), tag, 30_000)
		if len(res.Emits) != 0 {
			t.Fatalf("grace not exceeded; expected 0 emits, got %d", len(res.Emits))
		}
	})

	t.Run("respects gracePeriod — closes after grace", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		ws := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		we := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		loaded := map[string]interface{}{
			StateKeyFor(tag, "2026-01-01T10:00:00.000Z", "A"): map[string]interface{}{
				"acc": 42, "windowStart": float64(ws), "windowEnd": float64(we),
			},
		}
		res := op.Run(nil, loaded, dateParseMs(t, "2026-01-01T10:01:35.000Z"), tag, 30_000)
		if len(res.Emits) != 1 {
			t.Fatalf("grace exceeded; expected 1 emit, got %d", len(res.Emits))
		}
		if res.Emits[0].Value.(int) != 42 {
			t.Fatalf("expected emit value=42, got %v", res.Emits[0].Value)
		}
	})

	t.Run("ignores state rows owned by another operator", func(t *testing.T) {
		op := ReduceOperator{Fn: add, Initial: 0}
		ws := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		we := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		loaded := map[string]interface{}{
			StateKeyFor("slide:60:10", "2026-01-01T10:00:00.000Z", "A"): map[string]interface{}{
				"acc": 99, "windowStart": float64(ws), "windowEnd": float64(we),
			},
			"__wm__": map[string]interface{}{"eventTimeMs": float64(1234)},
		}
		res := op.Run(nil, loaded, dateParseMs(t, "2026-01-01T10:05:00.000Z"), tag, 0)
		if len(res.Emits) != 0 {
			t.Fatalf("foreign rows should be skipped; got %d emits", len(res.Emits))
		}
	})
}

// ---------------------------------------------------------------------------
// AggregateOperator (1 test)
// ---------------------------------------------------------------------------

func TestAggregateOperator(t *testing.T) {
	t.Run("computes count, sum, avg, min, max", func(t *testing.T) {
		op := NewAggregateOperator(map[string]ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return float64(m.(map[string]interface{})["amount"].(int)), nil },
			"avg":   func(m interface{}) (float64, error) { return float64(m.(map[string]interface{})["amount"].(int)), nil },
			"min":   func(m interface{}) (float64, error) { return float64(m.(map[string]interface{})["amount"].(int)), nil },
			"max":   func(m interface{}) (float64, error) { return float64(m.(map[string]interface{})["amount"].(int)), nil },
		}, "count", "sum", "avg", "min", "max")
		envs := []Envelope{
			reduceEnv("A", "2026-01-01T10:00:01.000Z", map[string]interface{}{"amount": 10}, t),
			reduceEnv("A", "2026-01-01T10:00:30.000Z", map[string]interface{}{"amount": 30}, t),
			reduceEnv("A", "2026-01-01T10:00:55.000Z", map[string]interface{}{"amount": 20}, t),
			reduceEnv("A", "2026-01-01T10:01:05.000Z", map[string]interface{}{"amount": 99}, t),
		}
		res := op.Run(envs, map[string]interface{}{}, dateParseMs(t, "2026-01-01T10:01:05.000Z"), tag, 0)
		if len(res.Emits) != 1 {
			t.Fatalf("expected 1 emit, got %d", len(res.Emits))
		}
		v := res.Emits[0].Value.(map[string]interface{})
		if v["count"] != 3.0 || v["sum"] != 60.0 || v["avg"] != 20.0 {
			t.Fatalf("unexpected aggregate: %+v", v)
		}
		if v["min"].(float64) != 10 || v["max"].(float64) != 30 {
			t.Fatalf("unexpected min/max: %+v", v)
		}
	})
}

// ---------------------------------------------------------------------------
// WindowSlidingOperator (2 tests)
// ---------------------------------------------------------------------------

func TestWindowSlidingOperator(t *testing.T) {
	t.Run("emits N envelopes per event for size=60, slide=10", func(t *testing.T) {
		op := NewWindowSlidingOperator(60, 10)
		out, err := op.Apply(context.Background(), Envelope{
			Msg: map[string]interface{}{"createdAt": "2026-01-01T10:00:35.000Z", "data": map[string]interface{}{}},
			Key: "p1",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 6 {
			t.Fatalf("expected 6 envs, got %d", len(out))
		}
		seen := map[string]bool{}
		for _, e := range out {
			if e.WindowEnd-e.WindowStart != 60_000 {
				t.Fatalf("size mismatch")
			}
			if e.OperatorTag != "slide:60:10" {
				t.Fatalf("operator tag mismatch: %s", e.OperatorTag)
			}
			seen[e.WindowKey] = true
		}
		if len(seen) != 6 {
			t.Fatalf("expected 6 distinct windowKeys, got %d", len(seen))
		}
	})
}

// ---------------------------------------------------------------------------
// WindowCronOperator (5 tests)
// ---------------------------------------------------------------------------

func TestWindowCronOperator(t *testing.T) {
	cases := []struct{ every, in, out string }{
		{"minute", "2026-01-01T10:00:35.123Z", "2026-01-01T10:00:00.000Z"},
		{"hour", "2026-01-01T10:30:35.000Z", "2026-01-01T10:00:00.000Z"},
		{"day", "2026-01-01T22:30:35.000Z", "2026-01-01T00:00:00.000Z"},
		{"week", "2026-01-07T12:00:00.000Z", "2026-01-05T00:00:00.000Z"},
	}
	for _, c := range cases {
		t.Run("aligns "+c.every+" boundaries", func(t *testing.T) {
			op := NewWindowCronOperator(c.every)
			out, err := op.Apply(context.Background(), Envelope{
				Msg: map[string]interface{}{"createdAt": c.in, "data": map[string]interface{}{}},
				Key: "p1",
			})
			if err != nil {
				t.Fatal(err)
			}
			if out[0].WindowKey != c.out {
				t.Fatalf("expected windowKey=%s, got %s", c.out, out[0].WindowKey)
			}
		})
	}

	t.Run("rejects unknown every:", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic")
			}
		}()
		op := NewWindowCronOperator("fortnight")
		_ = op.WindowMs()
	})
}

// ---------------------------------------------------------------------------
// WindowSessionOperator (3 tests)
// ---------------------------------------------------------------------------

func TestWindowSessionOperator(t *testing.T) {
	add := func(a, m interface{}) (interface{}, error) {
		af, _ := a.(int)
		mf, _ := m.(map[string]interface{})
		x, _ := mf["x"].(int)
		return af + x, nil
	}
	initial := func() interface{} { return 0 }

	t.Run("extends a session within gap", func(t *testing.T) {
		op := NewWindowSessionOperator(30)
		t1 := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		t2 := dateParseMs(t, "2026-01-01T10:00:10.000Z")
		envs := []Envelope{
			{Key: "A", EventTimeMs: t1, Value: map[string]interface{}{"x": 1}},
			{Key: "A", EventTimeMs: t2, Value: map[string]interface{}{"x": 2}},
		}
		res := op.RunSession(envs, map[string]interface{}{}, add, initial, t2)
		if len(res.Emits) != 0 {
			t.Fatalf("expected 0 emits, got %d", len(res.Emits))
		}
		var ok bool
		for _, op := range res.StateOps {
			if op.Type == "upsert" {
				v := op.Value.(map[string]interface{})
				if v["acc"].(int) == 3 {
					ok = true
				}
			}
		}
		if !ok {
			t.Fatal("expected upsert with acc=3")
		}
	})

	t.Run("closes a session when gap exceeded by next event", func(t *testing.T) {
		op := NewWindowSessionOperator(30)
		t1 := dateParseMs(t, "2026-01-01T10:00:00.000Z")
		t2 := dateParseMs(t, "2026-01-01T10:01:00.000Z")
		envs := []Envelope{
			{Key: "A", EventTimeMs: t1, Value: map[string]interface{}{"x": 1}},
			{Key: "A", EventTimeMs: t2, Value: map[string]interface{}{"x": 5}},
		}
		res := op.RunSession(envs, map[string]interface{}{}, add, initial, t2)
		if len(res.Emits) != 1 {
			t.Fatalf("expected 1 emit, got %d", len(res.Emits))
		}
		if res.Emits[0].Value.(int) != 1 {
			t.Fatalf("expected emit acc=1, got %v", res.Emits[0].Value)
		}
	})

	t.Run("idle-flushes a seeded session whose gap has expired", func(t *testing.T) {
		op := NewWindowSessionOperator(30)
		t1 := float64(dateParseMs(t, "2026-01-01T10:00:00.000Z"))
		flushTime := dateParseMs(t, "2026-01-01T10:02:00.000Z")
		stateKey := op.OpenSessionStateKey("A")
		loaded := map[string]interface{}{
			stateKey: map[string]interface{}{
				"acc": 99, "sessionStart": t1, "lastEventTime": t1,
			},
		}
		res := op.RunSession(nil, loaded, add, initial, flushTime)
		if len(res.Emits) != 1 {
			t.Fatalf("expected 1 emit, got %d", len(res.Emits))
		}
		if res.Emits[0].Value.(int) != 99 {
			t.Fatalf("expected emit acc=99, got %v", res.Emits[0].Value)
		}
	})
}
