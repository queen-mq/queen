// Tests for the fluent Stream builder + chain compilation.
// 1:1 port of test-v2/streams-unit/configHash.test.js (9 cases).
package streams

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/smartpricing/queen/clients/client-go/streams/operators"
)

type fakeQueue struct{ name string }

func (f fakeQueue) Name() string { return f.name }

type fakeSource struct{ name string }

func (f *fakeSource) Name() string                             { return f.name }
func (f *fakeSource) Batch(int) Source                         { return f }
func (f *fakeSource) Wait(bool) Source                         { return f }
func (f *fakeSource) TimeoutMillis(int) Source                 { return f }
func (f *fakeSource) Group(string) Source                      { return f }
func (f *fakeSource) Partitions(int) Source                    { return f }
func (f *fakeSource) SubscriptionMode(string) Source           { return f }
func (f *fakeSource) SubscriptionFrom(string) Source           { return f }
func (f *fakeSource) Pop(_ context.Context) ([]Message, error) { return nil, nil }

func newFake(name string) *fakeSource { return &fakeSource{name: name} }

// ---------------------------------------------------------------------------
// Stream chain validation — 1:1 port of "Stream chain validation" describe
// ---------------------------------------------------------------------------

func TestStreamChainValidation(t *testing.T) {
	t.Run("compiles a simple stateless chain", func(t *testing.T) {
		s := From(newFake("orders")).
			Map(func(m interface{}, _ EmitCtx) (interface{}, error) {
				return m, nil
			}).
			Filter(func(m interface{}, _ EmitCtx) (bool, error) { return true, nil }).
			To(fakeQueue{name: "orders.filtered"})
		c, err := s.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		if len(c.Stages.Pre) != 2 {
			t.Fatalf("expected 2 pre stages, got %d", len(c.Stages.Pre))
		}
		if c.Stages.Sink == nil || c.Stages.Sink.Kind() != "sink" {
			t.Fatal("expected sink stage")
		}
	})

	t.Run("compiles a windowed-aggregation chain", func(t *testing.T) {
		s := From(newFake("orders")).
			WindowTumbling(60).
			Aggregate(map[string]ExtractorFn{"sum": func(m interface{}) (float64, error) { return 0, nil }}).
			To(fakeQueue{name: "orders.totals"})
		c, err := s.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		if c.Stages.Window == nil {
			t.Fatal("expected window")
		}
		if c.Stages.Reducer == nil {
			t.Fatal("expected reducer")
		}
	})

	t.Run("rejects reduce without a window", func(t *testing.T) {
		s := From(newFake("x")).Reduce(func(a, m interface{}) (interface{}, error) { return 0, nil }, 0)
		_, err := s.Compile()
		if err == nil || !strings.Contains(err.Error(), "requires a preceding window") {
			t.Fatalf("expected 'requires a preceding window' error, got %v", err)
		}
	})

	t.Run("rejects double window", func(t *testing.T) {
		s := From(newFake("x")).WindowTumbling(10).WindowTumbling(30)
		_, err := s.Compile()
		if err == nil || !strings.Contains(err.Error(), "only one window operator") {
			t.Fatalf("expected only-one-window error, got %v", err)
		}
	})

	t.Run("rejects sink not at the end", func(t *testing.T) {
		s := From(newFake("x")).
			To(fakeQueue{name: "y"}).
			Map(func(m interface{}, _ EmitCtx) (interface{}, error) { return m, nil })
		_, err := s.Compile()
		if err == nil || !strings.Contains(err.Error(), "must be the last") {
			t.Fatalf("expected 'must be the last' error, got %v", err)
		}
	})

	t.Run("produces stable config_hash for identical chain shapes", func(t *testing.T) {
		a := From(newFake("q")).
			WindowTumbling(60).
			Aggregate(map[string]ExtractorFn{"sum": func(m interface{}) (float64, error) { return 0, nil }}, "sum").
			To(fakeQueue{name: "out"})
		b := From(newFake("q")).
			WindowTumbling(60).
			Aggregate(map[string]ExtractorFn{"sum": func(m interface{}) (float64, error) { return 0, nil }}, "sum").
			To(fakeQueue{name: "out"})
		ca, _ := a.Compile()
		cb, _ := b.Compile()
		if ca.ConfigHash != cb.ConfigHash {
			t.Fatalf("hashes diverge: %s vs %s", ca.ConfigHash, cb.ConfigHash)
		}
	})

	t.Run("config_hash differs when sink queue changes", func(t *testing.T) {
		a := From(newFake("q")).
			Map(func(m interface{}, _ EmitCtx) (interface{}, error) { return m, nil }).
			To(fakeQueue{name: "out-a"})
		b := From(newFake("q")).
			Map(func(m interface{}, _ EmitCtx) (interface{}, error) { return m, nil }).
			To(fakeQueue{name: "out-b"})
		ca, _ := a.Compile()
		cb, _ := b.Compile()
		if ca.ConfigHash == cb.ConfigHash {
			t.Fatalf("hashes should differ but both are %s", ca.ConfigHash)
		}
	})

	_ = strings.Contains // keep import alive across the table-driven cases below

	t.Run("config_hash differs when aggregate field set changes", func(t *testing.T) {
		a := From(newFake("q")).
			WindowTumbling(60).
			Aggregate(map[string]ExtractorFn{"sum": func(m interface{}) (float64, error) { return 0, nil }}, "sum").
			To(fakeQueue{name: "out"})
		b := From(newFake("q")).
			WindowTumbling(60).
			Aggregate(map[string]ExtractorFn{
				"sum":   func(m interface{}) (float64, error) { return 0, nil },
				"count": func(m interface{}) (float64, error) { return 1, nil },
			}, "sum", "count").
			To(fakeQueue{name: "out"})
		ca, _ := a.Compile()
		cb, _ := b.Compile()
		if ca.ConfigHash == cb.ConfigHash {
			t.Fatalf("hashes should differ but both are %s", ca.ConfigHash)
		}
	})
}

// ---------------------------------------------------------------------------
// Window operator validation tests
// ---------------------------------------------------------------------------

func TestWindowSlidingRejectsBadConfig(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for size mod slide non-zero")
		}
		if !strings.Contains(toString(r), "must be an integer multiple") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()
	operators.NewWindowSlidingOperator(60, 7)
}

func TestWindowCronRejectsUnknownEvery(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for unknown every")
		}
		if !strings.Contains(toString(r), "every must be") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()
	op := operators.NewWindowCronOperator("fortnight")
	// Trigger validation via WindowMs() (lazy in this Go port).
	_ = op.WindowMs()
}

func toString(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	case error:
		return s.Error()
	}
	return ""
}

// ---------------------------------------------------------------------------
// ToPartitioned — builder surface for the sink partition resolver
// ---------------------------------------------------------------------------

// jsSinkChainHash is the config_hash the JS client produces for
// From('orders').map(...).to('orders.out') — i.e. sha256 of
// [{"kind":"map"},{"kind":"sink","queue":"orders.out"}]. JS emits those exact
// bytes whether or not .to() was given a { partition } opt, so Go must too:
// cross-language resume compares this digest at registration.
const jsSinkChainHash = "32d0607d270acd6c63677e4c7a8c788941e85d491fd13737552464a2bf2d3aef"

func TestToPartitioned(t *testing.T) {
	identity := func(m interface{}, _ EmitCtx) (interface{}, error) { return m, nil }

	t.Run("wires a string resolver onto the sink operator", func(t *testing.T) {
		c, err := From(newFake("orders")).
			ToPartitioned(fakeQueue{name: "orders.out"}, "fixed").
			Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		sink, ok := c.Stages.Sink.(operators.SinkOperator)
		if !ok {
			t.Fatalf("expected a SinkOperator, got %T", c.Stages.Sink)
		}
		items := sink.BuildPushItems([]operators.SinkEntry{{Value: map[string]interface{}{}}}, "src")
		if items[0].Partition != "fixed" {
			t.Fatalf("expected fixed, got %q", items[0].Partition)
		}
	})

	t.Run("wires a func resolver onto the sink operator", func(t *testing.T) {
		c, err := From(newFake("orders")).
			ToPartitioned(fakeQueue{name: "orders.out"}, func(v interface{}) string {
				return v.(map[string]interface{})["tenantId"].(string)
			}).
			Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		sink := c.Stages.Sink.(operators.SinkOperator)
		items := sink.BuildPushItems([]operators.SinkEntry{
			{Value: map[string]interface{}{"tenantId": "acme"}},
		}, "src")
		if items[0].Partition != "acme" {
			t.Fatalf("expected acme, got %q", items[0].Partition)
		}
	})

	t.Run("To still defaults to the source partition", func(t *testing.T) {
		c, err := From(newFake("orders")).To(fakeQueue{name: "orders.out"}).Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		sink := c.Stages.Sink.(operators.SinkOperator)
		items := sink.BuildPushItems([]operators.SinkEntry{{Value: map[string]interface{}{}}}, "src")
		if items[0].Partition != "src" {
			t.Fatalf("expected src, got %q", items[0].Partition)
		}
	})

	t.Run("config_hash matches JS and is identical across all resolver forms", func(t *testing.T) {
		plain, _ := From(newFake("orders")).Map(identity).
			To(fakeQueue{name: "orders.out"}).Compile()
		str, _ := From(newFake("orders")).Map(identity).
			ToPartitioned(fakeQueue{name: "orders.out"}, "fixed").Compile()
		fn, _ := From(newFake("orders")).Map(identity).
			ToPartitioned(fakeQueue{name: "orders.out"}, func(v interface{}) string { return "p" }).Compile()

		for name, got := range map[string]string{
			"To":                    plain.ConfigHash,
			"ToPartitioned(string)": str.ConfigHash,
			"ToPartitioned(func)":   fn.ConfigHash,
		} {
			if got != jsSinkChainHash {
				t.Fatalf("%s hash %s, want the JS digest %s", name, got, jsSinkChainHash)
			}
		}
	})

	t.Run("rejects a resolver that is neither string nor func", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic on a bad resolver type")
			}
			if !strings.Contains(fmt.Sprint(r), "must be a string or func") {
				t.Fatalf("unexpected panic message: %v", r)
			}
		}()
		From(newFake("orders")).ToPartitioned(fakeQueue{name: "orders.out"}, 42)
	})
}
