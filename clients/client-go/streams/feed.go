package streams

// Feed streams: a Stream sourced from the tenant feed (JS Stream.fromFeed):
// every push to the tenant's queues (or the ones named), in log order, read
// from followers. The chain runs here, its state is kept in this process and
// snapshotted to KV, and every commit pushes the outputs and moves the
// position in one transaction. Same behaviour and same KV formats as the JS
// SDK, so a stream can move between the SDKs.
//
//	h, err := streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}}).
//	    Where("data.country", "=", "IT").
//	    Filter(func(m interface{}, _ streams.EmitCtx) (bool, error) { ... }).
//	    To(q.Queue("orders-it")).
//	    RunFeed(ctx, streams.FeedRunOptions{QueryID: "orders-it"})

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/operators"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/util"
)

// FeedClient is the client a feed stream runs on: *queen.Queen.
type FeedClient interface {
	FeedBroker() feedrt.Broker
}

// FeedSource is FromFeed's options (JS Stream.fromFeed(queen, options)).
type FeedSource struct {
	// Queues: names or patterns (`orders*`); nil: every queue.
	Queues []string
	// Partitions: partition names; nil: every partition.
	Partitions []string
	// From: where a NEW query starts: "now" (default), "earliest", or a position.
	From interface{}
	// Since: a new query starts at the messages created since (a time.Time,
	// epoch ms, an ISO time, or a duration back from now such as "1d").
	Since interface{}
	// FollowerURLs: nodes to read from first.
	FollowerURLs []string
	Kinds        []string
	MaxRecords   int
	MaxBytes     int
}

type feedSource struct {
	client FeedClient
	opts   FeedSource
}

// Re-exports of the feed's types.
type (
	FeedRunOptions   = feedrt.RunOptions
	FeedHandle       = feedrt.Handle
	FeedMetrics      = feedrt.Metrics
	FeedStepState    = feedrt.StepState
	FeedView         = feedrt.FeedView
	FeedCompiled     = feedrt.Compiled
	ShardSpec        = feedrt.ShardSpec
	PatternOptions   = operators.PatternOptions
	JoinOptions      = operators.JoinOptions
	JoinSidesOptions = operators.JoinSidesOptions
	GatherOptions    = operators.GatherOptions
	TableOptions     = operators.TableOptions
	Part             = operators.Part
	StepTime         = operators.StepTime
	RouteFn          = operators.RouteFn
	Target           = feedrt.Target
	AggregateField   = operators.AggregateField
	EventTimeValueFn = operators.EventTimeValueFn
)

// Undefined is JavaScript's undefined: answered by a function, it means
// "nothing" (as undefined does in JS); a map field holding it is left out.
var Undefined = feedrt.Undefined

// NullInitial is .Reduce(fn, NullInitial): JS .reduce(fn, null) (a nil initial is .reduce(fn)).
var NullInitial = operators.NullInitial

// Queues is parts named by queue (a join's sides, a gather's steps).
var Queues = operators.Queues

// Feed-only window options.
var (
	WithIdle                = operators.WithIdle
	WithIdleSpan            = operators.WithIdleSpan
	WithAllowedLatenessSpan = operators.WithAllowedLatenessSpan
	WithEventTimeOf         = operators.WithEventTimeOf
	WithEventTimeOff        = operators.WithEventTimeOff
)

// IsUndefined reports whether v is Undefined.
func IsUndefined(v interface{}) bool { return jsv.IsUndefined(v) }

// Int is a pointer to n, for the options whose zero is a value (CommitEveryMillis: streams.Int(0)).
func Int(n int) *int { return &n }

// Bool is a pointer to b (Exclusive: streams.Bool(false)).
func Bool(b bool) *bool { return &b }

// FromFeed builds a Stream sourced from the tenant feed.
func FromFeed(q FeedClient, src FeedSource) *Stream {
	s := &Stream{feed: &feedSource{client: q, opts: src}}
	if q == nil {
		s.err = errors.New("Stream.fromFeed(queen, options): pass the Queen client")
	}
	return s
}

// Where keeps only the source messages where `field op value` holds. On a
// feed it is sent to the broker (before any stateful step); on a queue source
// it is evaluated here. Fields: queue, partition, transactionId, offset,
// position, data, data.<path>. Ops: = != < <= > >= in exists prefix !prefix.
func (s *Stream) Where(field, op string, value ...interface{}) *Stream {
	cond, err := feed.WhereCondition(field, op, value...)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(operators.WhereOperator{Cond: cond})
}

// Pattern: a Start message, then a Then message for the same entity within
// Within. Feed sources only.
func (s *Stream) Pattern(o PatternOptions) *Stream {
	op, err := operators.NewPatternOperator(o)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// Join: a Left and a Right message for the same entity, in either order,
// within Within (JS join({ left, right })). Feed sources only.
func (s *Stream) Join(o JoinOptions) *Stream {
	op, err := operators.NewJoinOperator(o)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// JoinSides: one message per side, any number of sides (JS join({ sides })). Feed sources only.
func (s *Stream) JoinSides(o JoinSidesOptions) *Stream {
	op, err := operators.NewJoinSidesOperator(o)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// Gather: the parts of one run, until all are in or the deadline passes. Feed sources only.
func (s *Stream) Gather(o GatherOptions) *Stream {
	op, err := operators.NewGatherOperator(o)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// JoinTable: every message joined with the latest row of a table queue. Feed sources only.
func (s *Stream) JoinTable(o TableOptions) *Stream {
	op, err := operators.NewTableOperator(o)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// Name names the stateful step before it, for live reads (handle.State(name), View).
func (s *Stream) Name(name string) *Stream {
	op, err := operators.NewNameOperator(name)
	if err != nil {
		return s.fail(err)
	}
	return s.extend(op)
}

// AggregateFields is Aggregate with extractors in order, answering any value
// (as JS's: a non-number counts 1 for count, 0 for sum, nothing for min, max, avg).
func (s *Stream) AggregateFields(fields ...AggregateField) *Stream {
	if len(fields) == 0 {
		return s.fail(errors.New("aggregate requires at least one extractor"))
	}
	names := make([]string, len(fields))
	ex := map[string]operators.ExtractorFn{}
	for i, f := range fields {
		names[i] = f.Name
		fn := f.Fn
		ex[f.Name] = func(v interface{}) (float64, error) {
			x, err := fn(v)
			if err != nil {
				return 0, err
			}
			n, _ := jsv.Num(x)
			return n, nil
		}
	}
	op := operators.NewAggregateOperator(ex, names...)
	op.Values = append([]AggregateField(nil), fields...)
	return s.extend(op)
}

// ToAll sends every record to each of the queues (feed sources).
func (s *Stream) ToAll(sinks ...Sink) *Stream {
	if len(sinks) == 0 {
		return s.fail(errors.New(".to([]): name at least one queue"))
	}
	var queues []string
	seen := map[string]bool{}
	for _, k := range sinks {
		if !seen[k.Name()] {
			seen[k.Name()] = true
			queues = append(queues, k.Name())
		}
	}
	op := operators.NewSinkOperator("", nil)
	op.Queues = queues
	if len(queues) == 1 {
		op.QueueName = queues[0]
	}
	return s.extend(op)
}

// ToFunc sends each record where fn says: a queue name, a Target, a Sink, a
// list of those, or nil/Undefined for nowhere (feed sources).
func (s *Stream) ToFunc(fn RouteFn) *Stream {
	if fn == nil {
		return s.fail(errors.New(".to(): pass a function"))
	}
	op := operators.NewSinkOperator("", nil)
	op.Route = fn
	return s.extend(op)
}

// ToOptions are a sink's options: the partition, a fixed name or a function
// of the value (a PartitionFn, or func(value) (interface{}, error)).
type ToOptions struct {
	Partition interface{}
}

// ToWith is .to(target, opts) with any target: a Sink, a queue name, a list
// of either, or a RouteFn.
func (s *Stream) ToWith(target interface{}, opts ToOptions) *Stream {
	op := operators.NewSinkOperator("", nil)
	switch t := target.(type) {
	case RouteFn:
		op.Route = t
	case func(value, msg interface{}) (interface{}, error):
		op.Route = t
	case Sink:
		op.QueueName = t.Name()
	case string:
		if t == "" {
			return s.fail(fmt.Errorf(".to(): a queue name is a non-empty string, got \"\""))
		}
		op.QueueName = t
	case []Sink:
		if len(t) == 0 {
			return s.fail(errors.New(".to([]): name at least one queue"))
		}
		for _, k := range t {
			op.Queues = appendUniq(op.Queues, k.Name())
		}
	case []string:
		if len(t) == 0 {
			return s.fail(errors.New(".to([]): name at least one queue"))
		}
		for _, k := range t {
			op.Queues = appendUniq(op.Queues, k)
		}
	default:
		return s.fail(errors.New("Could not extract queue name from sink target. Pass either a queen.queue(\"name\") builder or { queueName: \"name\" }."))
	}
	if op.Queues != nil && len(op.Queues) == 1 {
		op.QueueName = op.Queues[0]
	}
	switch p := opts.Partition.(type) {
	case nil:
	case string:
		op.PartitionResolver = p
	case operators.PartitionFn:
		op.PartitionResolver = p
	case func(value interface{}) (interface{}, error):
		op.PartitionValueFn = p
	default:
		return s.fail(fmt.Errorf("sink partition must be a string or a function, got %T", opts.Partition))
	}
	return s.extend(op)
}

func appendUniq(l []string, v string) []string {
	for _, x := range l {
		if x == v {
			return l
		}
	}
	return append(l, v)
}

// feedOnly are the operators that run on a feed source only.
func feedOnly(kind string) bool {
	switch kind {
	case "pattern", "collect", "table", "name":
		return true
	}
	return false
}

// queueOnlyCheck refuses on a queue source what runs on a feed only.
func (s *Stream) queueOnlyCheck() error {
	for _, op := range s.operators {
		if feedOnly(op.Kind()) {
			return errors.New(".pattern(), .join(), .gather(), .joinTable() and .name() run on Stream.fromFeed only: they need the clock every queue shares")
		}
		if w, ok := op.(interface{ HasIdle() bool }); ok && w.HasIdle() {
			return errors.New("a window's idle runs on Stream.fromFeed only (on a queue source, idleFlushMs closes windows on quiet partitions)")
		}
	}
	for _, op := range s.operators {
		if sk, ok := op.(operators.SinkOperator); ok && sk.Routes() {
			return errors.New(".to() with several queues or a function runs on Stream.fromFeed only; a queue source sinks to one queue")
		}
	}
	return nil
}

// RunFeed starts a stream on the feed: one runner, or the shards of it in
// this process (Shards). It returns once the runner is active, or a standby
// waiting for the lock.
func (s *Stream) RunFeed(ctx context.Context, opts FeedRunOptions) (FeedHandle, error) {
	if opts.QueryID == "" {
		return nil, errors.New("run({ queryId }) is required")
	}
	if s.feed == nil {
		return nil, errors.New("RunFeed runs a stream built by streams.FromFeed; a queue source runs with Run")
	}
	compiled, err := s.CompileFeed()
	if err != nil {
		return nil, err
	}
	b := s.feed.client.FeedBroker()
	if opts.Shards != 0 && opts.Shards != 1 {
		g, err := feedrt.NewShardGroup(b, compiled, opts, opts.Shards)
		if err != nil {
			return nil, err
		}
		if err := g.Start(ctx); err != nil {
			return nil, err
		}
		return g, nil
	}
	r, err := feedrt.NewRunner(b, compiled, opts)
	if err != nil {
		return nil, err
	}
	if err := r.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// ViewOptions are View's: Shards (0: read the stream's layout, else 1) and TimeoutMillis (0: 5000).
type ViewOptions struct {
	Shards        int
	TimeoutMillis int
}

// View asks a running feed stream about its live state, from any process.
func View(q FeedClient, queryID string, opts ViewOptions) (*FeedView, error) {
	if q == nil {
		return nil, errors.New("Stream.view(queen, queryId): pass the Queen client (it asks over ephemeral queues)")
	}
	return feedrt.NewView(q.FeedBroker(), queryID, opts.Shards, opts.TimeoutMillis)
}

// ---- compile ---------------------------------------------------------------------

func opStepName(op util.Operator) string {
	switch o := op.(type) {
	case *operators.PatternOperator:
		return o.StepName
	case *operators.CollectOperator:
		return o.StepName
	case *operators.TableOperator:
		return o.StepName
	}
	return ""
}

type planStep struct {
	step   feedrt.Step
	window operators.FeedWindowOp
	// at is the place in the chain of the operator the step's config comes from.
	at int
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// CompileFeed compiles a chain on a feed into steps (JS _compileFeed):
// stateless operators and the stateful steps in chain order (a window and
// its reduce/aggregate are one step); a where before the first stateful step
// goes to the broker. ChainHash is made from the stateful steps and the
// keyBys around them.
func (s *Stream) CompileFeed() (*feedrt.Compiled, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.feed == nil {
		return nil, errors.New("CompileFeed compiles a stream built by streams.FromFeed")
	}
	ops := s.operators
	sinkIdx := -1
	for i, op := range ops {
		if op.Kind() == "sink" || op.Kind() == "foreach" {
			sinkIdx = i
			break
		}
	}
	if sinkIdx != -1 && sinkIdx != len(ops)-1 {
		return nil, fmt.Errorf("sink operators (.to / .foreach) must be the last in the chain; found %s at position %d of %d", ops[sinkIdx].Kind(), sinkIdx, len(ops))
	}
	upstream := ops
	var sink util.Operator
	if sinkIdx != -1 {
		upstream = ops[:sinkIdx]
		sink = ops[sinkIdx]
	}
	var steps []*planStep
	var pushdown []feed.Condition
	var pending []feedrt.Op
	var window operators.FeedWindowOp
	windowAt := -1
	stateful := 0
	flush := func() {
		if len(pending) > 0 {
			steps = append(steps, &planStep{step: feedrt.Step{Kind: "ops", Ops: pending}})
		}
		pending = nil
	}
	named := func(op util.Operator) string {
		if n := opStepName(op); n != "" {
			return n
		}
		return fmt.Sprintf("step%d", stateful)
	}
	for i, op := range upstream {
		kind := op.Kind()
		if window != nil && kind != "reduce" && kind != "aggregate" {
			return nil, errors.New("Stream.fromFeed: a window needs .reduce() or .aggregate() right after it")
		}
		switch kind {
		case "filter":
			// Before any stateful step a where tests the source message: the broker does it.
			if w, ok := op.(operators.WhereOperator); ok {
				if stateful == 0 {
					pushdown = append(pushdown, w.Cond)
				} else {
					c := w.Cond
					pending = append(pending, feedrt.Op{Kind: "where", Where: &c})
				}
				continue
			}
			pending = append(pending, statelessOp(op))
		case "map", "flatMap", "keyBy":
			pending = append(pending, statelessOp(op))
		case "window":
			flush()
			w, ok := op.(operators.FeedWindowOp)
			if !ok {
				return nil, fmt.Errorf("unsupported operator: %s", kind)
			}
			window, windowAt = w, i
		case "reduce", "aggregate":
			if window == nil {
				return nil, errors.New("reduce/aggregate requires a preceding window operator")
			}
			stateful++
			fw := window.FeedWindow()
			if fw.Err != nil {
				return nil, fw.Err
			}
			red := reducerSpec(op)
			steps = append(steps, &planStep{step: feedrt.Step{Kind: "window", Name: fmt.Sprintf("step%d", stateful),
				Window: windowSpec(fw), Reducer: red, Config: fw.Config, ReducerConfig: describe(op)}, window: window, at: windowAt})
			window, windowAt = nil, -1
		case "pattern":
			flush()
			stateful++
			p := op.(*operators.PatternOperator)
			steps = append(steps, &planStep{step: feedrt.Step{Kind: "pattern", Name: named(op), Pattern: patternSpec(p), Config: p.FeedConfig}, at: i})
		case "collect":
			flush()
			stateful++
			c := op.(*operators.CollectOperator)
			steps = append(steps, &planStep{step: feedrt.Step{Kind: "collect", Name: named(op), Collect: collectSpec(c), Config: c.FeedConfig}, at: i})
		case "table":
			flush()
			stateful++
			t := op.(*operators.TableOperator)
			steps = append(steps, &planStep{step: feedrt.Step{Kind: "table", Name: named(op), Table: tableSpec(t), Config: t.FeedConfig}, at: i})
		case "name":
			var last *planStep
			for i := len(steps) - 1; i >= 0; i-- {
				if steps[i].step.Kind != "ops" {
					last = steps[i]
					break
				}
			}
			if last == nil || len(pending) > 0 {
				return nil, errors.New(".name() names the stateful step right before it")
			}
			last.step.Name = op.(*operators.NameOperator).StepName
		case "gate":
			return nil, errors.New("Stream.fromFeed: .gate() runs on queue sources only — it holds a partition's lease, and a feed has none")
		default:
			return nil, fmt.Errorf("unsupported operator: %s", kind)
		}
	}
	if window != nil {
		return nil, errors.New("Stream.fromFeed: a window needs .reduce() or .aggregate() after it")
	}
	flush()
	seen := map[string]bool{}
	for _, st := range steps {
		if st.step.Kind == "ops" {
			continue
		}
		if seen[st.step.Name] {
			return nil, fmt.Errorf("Stream.fromFeed: two stateful steps are named %s", jsv.MustMarshal(st.step.Name))
		}
		seen[st.step.Name] = true
	}
	// The clock each stateful step runs on: a step that takes the event time
	// of the step before says so in the hash.
	inheriting := map[int]bool{}
	onEventTime := false
	for _, st := range steps {
		if st.step.Kind == "ops" || st.step.Kind == "table" {
			continue
		}
		var t feedrt.TimeSpec
		switch st.step.Kind {
		case "window":
			t = st.step.Window.Time
		case "pattern":
			t = st.step.Pattern.Time
		case "collect":
			t = st.step.Collect.Time
		}
		if t.EventTime != nil {
			onEventTime = true
		} else if onEventTime && !t.EventTimeOff {
			inheriting[st.at] = true
		} else {
			onEventTime = false
		}
	}
	shape := make([]interface{}, 0, len(upstream))
	for i, op := range upstream {
		switch op.Kind() {
		case "map", "filter", "flatMap":
			continue
		}
		d := describe(op)
		if inheriting[i] {
			d = append(append(jsv.Ordered(nil), d...), jsv.Pair{K: "clock", V: "event"})
		}
		shape = append(shape, d)
	}
	hashJSON, err := jsv.Marshal(shape)
	if err != nil {
		return nil, err
	}
	plan := make([]feedrt.Step, len(steps))
	for i, st := range steps {
		plan[i] = st.step
	}
	out := &feedrt.Compiled{
		Source: feedrt.Source{Queues: s.feed.opts.Queues, Partitions: s.feed.opts.Partitions, From: s.feed.opts.From,
			Since: s.feed.opts.Since, FollowerURLs: s.feed.opts.FollowerURLs, Kinds: s.feed.opts.Kinds,
			MaxRecords: s.feed.opts.MaxRecords, MaxBytes: s.feed.opts.MaxBytes},
		Steps:     plan,
		Pushdown:  pushdown,
		ChainHash: sha256Hex(hashJSON)[:16],
	}
	if sink != nil {
		out.Sink = sinkSpec(sink)
	}
	return out, nil
}

// describe is configHash's { kind: op.kind, ...op.config }, in JS insertion order.
func describe(op util.Operator) jsv.Ordered {
	switch o := op.(type) {
	case operators.FeedWindowOp:
		return o.FeedWindow().Config
	case *operators.PatternOperator:
		return o.FeedConfig
	case *operators.CollectOperator:
		return o.FeedConfig
	case *operators.TableOperator:
		return o.FeedConfig
	case *operators.NameOperator:
		return jsv.Ordered{{K: "kind", V: "name"}, {K: "name", V: o.StepName}}
	case *operators.AggregateOperator:
		fields := make([]interface{}, len(o.Fields))
		for i, f := range o.Fields {
			fields[i] = f
		}
		return jsv.Ordered{{K: "kind", V: "aggregate"}, {K: "fields", V: fields}}
	case operators.ReduceOperator:
		return jsv.Ordered{{K: "kind", V: "reduce"}, {K: "hasInitial", V: o.Initial != nil}}
	case *operators.ReduceOperator:
		return jsv.Ordered{{K: "kind", V: "reduce"}, {K: "hasInitial", V: o.Initial != nil}}
	}
	return jsv.Ordered{{K: "kind", V: op.Kind()}}
}

func ctxOf(ctx interface{}) EmitCtx {
	if m, ok := ctx.(map[string]interface{}); ok {
		return EmitCtx(m)
	}
	return nil
}

func statelessOp(op util.Operator) feedrt.Op {
	switch o := op.(type) {
	case operators.MapOperator:
		fn := o.Fn
		return feedrt.Op{Kind: "map", Map: func(msg, ctx interface{}) (interface{}, error) { return fn(msg, ctxOf(ctx)) }}
	case operators.FilterOperator:
		fn := o.Fn
		return feedrt.Op{Kind: "filter", Filter: func(msg, ctx interface{}) (bool, error) { return fn(msg, ctxOf(ctx)) }}
	case operators.FlatMapOperator:
		fn := o.Fn
		return feedrt.Op{Kind: "flatMap", FlatMap: func(msg, ctx interface{}) ([]interface{}, error) { return fn(msg, ctxOf(ctx)) }}
	case operators.KeyByOperator:
		fn := o.Fn
		return feedrt.Op{Kind: "keyBy", KeyBy: func(msg interface{}) (string, error) { return fn(msg) }}
	}
	return feedrt.Op{Kind: op.Kind()}
}

func timeSpec(t operators.FeedTime) feedrt.TimeSpec {
	ts := feedrt.TimeSpec{EventTimeOff: t.EventTimeOff, LatenessMs: t.LatenessMs, OnLate: t.OnLate, IdleMs: t.IdleMs}
	if t.EventTime != nil {
		ts.EventTime = t.EventTime
	}
	return ts
}

func windowSpec(f operators.FeedWindow) *feedrt.WindowSpec {
	w := &feedrt.WindowSpec{Kind: f.Kind, WindowMs: f.WindowMs, SlideMs: f.SlideMs, SizeMs: f.SizeMs,
		WindowsPerEvent: f.WindowsPerEvent, GapMs: f.GapMs, Every: f.Every, GraceMs: f.GraceMs,
		Time: feedrt.TimeSpec{EventTimeOff: f.EventTimeOff, LatenessMs: f.LatenessMs, OnLate: f.OnLate, IdleMs: f.IdleMs}}
	if f.EventTime != nil {
		w.Time.EventTime = f.EventTime
	}
	return w
}

func reducerSpec(op util.Operator) *feedrt.ReducerSpec {
	switch o := op.(type) {
	case *operators.AggregateOperator:
		return aggregateSpec(o)
	case operators.ReduceOperator:
		return &feedrt.ReducerSpec{Fn: o.Fn, Initial: initialOf(o.Initial)}
	case *operators.ReduceOperator:
		return &feedrt.ReducerSpec{Fn: o.Fn, Initial: initialOf(o.Initial)}
	}
	return nil
}

func initialOf(v interface{}) interface{} {
	switch v {
	case nil:
		return feedrt.Undefined
	case operators.NullInitial:
		return nil
	}
	return jsv.Normalize(v)
}

// aggregateSpec is AggregateOperator.js's fold, on any values.
func aggregateSpec(a *operators.AggregateOperator) *feedrt.ReducerSpec {
	initial := map[string]interface{}{}
	for _, f := range a.Values {
		switch f.Name {
		case "min", "max":
			initial[f.Name] = nil
		case "avg":
			initial["__avg_sum"], initial["__avg_count"], initial["avg"] = 0.0, 0.0, 0.0
		default:
			initial[f.Name] = 0.0
		}
	}
	or0 := func(v interface{}) float64 {
		// JS (x || 0) on a number.
		f, ok := jsv.Num(v)
		if !ok || math.IsNaN(f) {
			return 0
		}
		return f
	}
	fields := a.Values
	fn := func(acc, msg interface{}) (interface{}, error) {
		next := map[string]interface{}{}
		if m, ok := acc.(map[string]interface{}); ok {
			for k, v := range m {
				next[k] = v
			}
		}
		for _, f := range fields {
			v, err := f.Fn(msg)
			if err != nil {
				return nil, err
			}
			n, isNum := jsv.Num(v)
			switch f.Name {
			case "count":
				add := 1.0
				if isNum {
					add = n
				}
				next["count"] = or0(next["count"]) + add
			case "sum":
				add := 0.0
				if isNum {
					add = n
				}
				next["sum"] = or0(next["sum"]) + add
			case "min", "max":
				if isNum {
					cur := next[f.Name]
					if jsv.IsNullish(cur) {
						next[f.Name] = n
					} else if f.Name == "min" {
						next[f.Name] = math.Min(or0(cur), n)
					} else {
						next[f.Name] = math.Max(or0(cur), n)
					}
				}
			case "avg":
				if isNum {
					next["__avg_sum"] = or0(next["__avg_sum"]) + n
					next["__avg_count"] = or0(next["__avg_count"]) + 1
					c := or0(next["__avg_count"])
					if c > 0 {
						next["avg"] = or0(next["__avg_sum"]) / c
					} else {
						next["avg"] = 0.0
					}
				}
			default:
				add := 0.0
				if isNum {
					add = n
				}
				next[f.Name] = or0(next[f.Name]) + add
			}
		}
		return next, nil
	}
	return &feedrt.ReducerSpec{Fn: fn, Initial: initial, IsAggregate: true}
}

func patternSpec(p *operators.PatternOperator) *feedrt.PatternSpec {
	return &feedrt.PatternSpec{Kind: p.PatternKind, WithinMs: p.WithinMs, Key: p.Key, Keep: p.Keep, OnTimeout: p.OnTimeout,
		OnRepeat: p.OnRepeat, Start: p.Start, Then: p.Then, OnMatch: p.OnMatch, Left: p.Left, Right: p.Right,
		Select: p.Select, Time: timeSpec(p.Time)}
}

func collectSpec(c *operators.CollectOperator) *feedrt.CollectSpec {
	var parts []feedrt.Part
	if c.Parts != nil {
		parts = make([]feedrt.Part, len(c.Parts))
		for i, p := range c.Parts {
			parts[i] = feedrt.Part{Name: p.Name, Pred: p.Pred}
		}
	}
	return &feedrt.CollectSpec{Kind: c.CollectKind, WithinMs: c.WithinMs, Key: c.Key, Keep: c.Keep, OnTimeout: c.OnTimeout,
		OnRepeat: c.OnRepeat, Parts: parts, Count: c.Count, OnComplete: c.OnComplete, Remember: c.Remember, Time: timeSpec(c.Time)}
}

func tableSpec(t *operators.TableOperator) *feedrt.TableSpec {
	return &feedrt.TableSpec{Queues: t.Queues, On: t.On, Key: t.KeyFn, Value: t.ValueFn, Select: t.SelectFn,
		Missing: t.Missing, Initial: t.Initial}
}

func sinkSpec(op util.Operator) *feedrt.SinkSpec {
	switch o := op.(type) {
	case operators.ForeachOperator:
		fn := o.Fn
		return &feedrt.SinkSpec{Kind: "foreach", Foreach: func(value, ctx interface{}) error { return fn(value, ctxOf(ctx)) }}
	case operators.SinkOperator:
		s := &feedrt.SinkSpec{Kind: "sink"}
		if o.Route != nil {
			fn := o.Route
			s.Route = func(value, msg interface{}) (interface{}, error) { return fn(value, msg) }
		} else if o.Queues != nil {
			s.Queues = o.Queues
		} else {
			s.Queues = []string{o.QueueName}
		}
		switch r := o.PartitionResolver.(type) {
		case string:
			s.HasPartition, s.Partition = true, r
		case operators.PartitionFn:
			s.PartitionFn = func(v interface{}) (interface{}, error) { return r(v), nil }
		}
		if o.PartitionValueFn != nil {
			s.PartitionFn = o.PartitionValueFn
		}
		return s
	}
	return nil
}
