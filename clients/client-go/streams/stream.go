// Package streams — fluent streaming SDK for Queen, mirror of the JS
// Stream and Python queen.streams.Stream.
//
// Build a pipeline by chaining operators on Stream and finalize with .Run():
//
//	queen, _ := queen.New(queen.Config{URL: "http://localhost:6632"})
//	stream, err := streams.From(queen.Queue("orders")).
//	    Filter(func(m interface{}, ctx streams.EmitCtx) (bool, error) { ... }).
//	    Map(func(m interface{}, ctx streams.EmitCtx) (interface{}, error) { ... }).
//	    To(queen.Queue("orders.enriched")).
//	    Run(ctx, streams.RunOptions{QueryID: "examples.enrich", URL: "http://localhost:6632"})
//
// The chain is immutable — every combinator returns a new Stream.
package streams

import (
	"context"
	"fmt"

	"github.com/smartpricing/queen/client-go/streams/operators"
	"github.com/smartpricing/queen/client-go/streams/runtime"
	"github.com/smartpricing/queen/client-go/streams/util"
)

// Re-exports for ergonomics.
type (
	Envelope     = operators.Envelope
	EmitCtx      = operators.EmitCtx
	GateContext  = operators.GateContext
	GateFn       = operators.GateFn
	MapFn        = operators.MapFn
	FilterFn     = operators.FilterFn
	FlatMapFn    = operators.FlatMapFn
	KeyFn        = operators.KeyFn
	ReduceFn     = operators.ReduceFn
	ExtractorFn  = operators.ExtractorFn
	EventTimeFn  = operators.EventTimeFn
	ForeachFn    = operators.ForeachFn
	WindowOption = operators.WindowOption
	Source       = runtime.Source
	Message      = runtime.Message
	RunOptions   = runtime.RunOptions
	Metrics      = runtime.Metrics
	Runner       = runtime.Runner
)

// Window option re-exports.
var (
	WithGracePeriod     = operators.WithGracePeriod
	WithIdleFlushMs     = operators.WithIdleFlushMs
	WithEventTime       = operators.WithEventTime
	WithAllowedLateness = operators.WithAllowedLateness
	WithOnLate          = operators.WithOnLate
)

// Stream is an immutable fluent builder.
type Stream struct {
	source    Source
	operators []util.Operator
}

// From builds a Stream sourced from a Queen queue (anything implementing
// the runtime.Source interface — the real *queen.QueueBuilder satisfies it
// via the streamadapter helper, see streams_adapter.go).
func From(src Source) *Stream {
	return &Stream{source: src}
}

func (s *Stream) extend(op util.Operator) *Stream {
	return &Stream{source: s.source, operators: append(append([]util.Operator{}, s.operators...), op)}
}

// Map appends a stateless transformation.
func (s *Stream) Map(fn MapFn) *Stream {
	return s.extend(operators.MapOperator{Fn: fn})
}

// Filter drops envelopes where predicate is false.
func (s *Stream) Filter(fn FilterFn) *Stream {
	return s.extend(operators.FilterOperator{Fn: fn})
}

// FlatMap emits zero or more values per input.
func (s *Stream) FlatMap(fn FlatMapFn) *Stream {
	return s.extend(operators.FlatMapOperator{Fn: fn})
}

// KeyBy overrides the implicit partition key.
func (s *Stream) KeyBy(fn KeyFn) *Stream {
	return s.extend(operators.KeyByOperator{Fn: fn})
}

// WindowTumbling adds a fixed-size, non-overlapping window.
func (s *Stream) WindowTumbling(seconds float64, opts ...WindowOption) *Stream {
	return s.extend(operators.NewWindowTumblingOperator(seconds, opts...))
}

// WindowSliding adds an overlapping fixed-size window that hops every slide
// seconds.
func (s *Stream) WindowSliding(size, slide float64, opts ...WindowOption) *Stream {
	return s.extend(operators.NewWindowSlidingOperator(size, slide, opts...))
}

// WindowSession adds a per-key activity-based window.
func (s *Stream) WindowSession(gap float64, opts ...WindowOption) *Stream {
	return s.extend(operators.NewWindowSessionOperator(gap, opts...))
}

// WindowCron adds a wall-clock-aligned tumbling window.
func (s *Stream) WindowCron(every string, opts ...WindowOption) *Stream {
	return s.extend(operators.NewWindowCronOperator(every, opts...))
}

// Reduce folds windowed values into an accumulator.
func (s *Stream) Reduce(fn ReduceFn, initial interface{}) *Stream {
	return s.extend(operators.ReduceOperator{Fn: fn, Initial: initial})
}

// Aggregate is sugar over Reduce with named extractors.
//
// fieldOrder controls the deterministic config_hash. Pass the same key order
// the JS / Python clients use (insertion order of the JS object literal).
func (s *Stream) Aggregate(extractors map[string]ExtractorFn, fieldOrder ...string) *Stream {
	return s.extend(operators.NewAggregateOperator(extractors, fieldOrder...))
}

// Gate adds an ALLOW/DENY decision for rate limiting / flow control.
func (s *Stream) Gate(fn GateFn) *Stream {
	return s.extend(operators.GateOperator{Fn: fn})
}

// To sends the final emits to a Queen queue.
func (s *Stream) To(sink Sink) *Stream {
	return s.extend(operators.SinkOperator{QueueName: sink.Name()})
}

// Foreach runs a side effect for each emit (at-least-once).
func (s *Stream) Foreach(fn ForeachFn) *Stream {
	return s.extend(operators.ForeachOperator{Fn: fn})
}

// Sink is the minimal interface accepted by .To().
type Sink interface {
	Name() string
}

// Run compiles the chain, registers the query, and starts the Runner.
func (s *Stream) Run(ctx context.Context, opts RunOptions) (*Runner, error) {
	if opts.QueryID == "" {
		return nil, fmt.Errorf("Run requires QueryID")
	}
	compiled, err := s.compile()
	if err != nil {
		return nil, err
	}
	r := runtime.NewRunner(compiled, opts)
	if err := r.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Compile exposes the compiled chain (mainly for tests).
func (s *Stream) Compile() (*runtime.CompiledStream, error) { return s.compile() }

func (s *Stream) compile() (*runtime.CompiledStream, error) {
	var sink util.Operator
	sinkIdx := len(s.operators)
	for i, op := range s.operators {
		if op.Kind() == "sink" || op.Kind() == "foreach" {
			sink = op
			sinkIdx = i
			break
		}
	}
	if sink != nil && sinkIdx != len(s.operators)-1 {
		return nil, fmt.Errorf("sink operators (.To / .Foreach) must be the last in the chain")
	}
	upstream := s.operators
	if sink != nil {
		upstream = s.operators[:sinkIdx]
	}

	stages := runtime.Stages{}
	phase := "pre"
	for _, op := range upstream {
		switch k := op.Kind(); k {
		case "map", "filter", "flatMap":
			if so, ok := op.(runtime.StatelessOp); ok {
				if phase == "pre" || phase == "keyed" || phase == "window" {
					stages.Pre = append(stages.Pre, so)
				} else {
					stages.Post = append(stages.Post, so)
				}
			}
		case "keyBy":
			if stages.KeyBy != nil {
				return nil, fmt.Errorf("only one .KeyBy() per stream")
			}
			if phase == "reducer" || phase == "gate" {
				return nil, fmt.Errorf(".KeyBy() must come before window/reduce/gate")
			}
			ko, _ := op.(operators.KeyByOperator)
			stages.KeyBy = &ko
			if phase == "pre" {
				phase = "keyed"
			}
		case "window":
			if stages.Window != nil {
				return nil, fmt.Errorf("only one window operator per stream")
			}
			if phase == "reducer" {
				return nil, fmt.Errorf("window must come before reduce/aggregate")
			}
			if stages.Gate != nil {
				return nil, fmt.Errorf("window+reduce is incompatible with .Gate() in the same stream")
			}
			wo, _ := op.(runtime.WindowOp)
			stages.Window = wo
			phase = "window"
		case "reduce", "aggregate":
			if stages.Reducer != nil {
				return nil, fmt.Errorf("only one reduce/aggregate per stream")
			}
			if stages.Window == nil {
				return nil, fmt.Errorf("reduce/aggregate requires a preceding window operator")
			}
			if stages.Gate != nil {
				return nil, fmt.Errorf("reduce/aggregate is incompatible with .Gate() in the same stream")
			}
			switch r := op.(type) {
			case operators.ReduceOperator:
				stages.Reducer = &r
			case *operators.AggregateOperator:
				stages.Reducer = &r.ReduceOperator
			}
			phase = "reducer"
		case "gate":
			if stages.Gate != nil {
				return nil, fmt.Errorf("only one .Gate() per stream")
			}
			if stages.Window != nil || stages.Reducer != nil {
				return nil, fmt.Errorf(".Gate() is incompatible with windowing/reduce in the same stream")
			}
			go_, _ := op.(operators.GateOperator)
			stages.Gate = &go_
			phase = "gate"
		default:
			return nil, fmt.Errorf("unsupported operator: %s", k)
		}
	}
	if sink != nil {
		if so, ok := sink.(runtime.SinkLike); ok {
			stages.Sink = so
		}
	}

	hash := util.ConfigHashOf(s.operators)
	return &runtime.CompiledStream{
		Source:     s.source,
		Stages:     stages,
		Operators:  append([]util.Operator(nil), s.operators...),
		ConfigHash: hash,
	}, nil
}
