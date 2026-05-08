package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/smartpricing/queen/client-go/streams/operators"
	"github.com/smartpricing/queen/client-go/streams/util"
)

const watermarkStateKey = "__wm__"

// Source abstracts the QueueBuilder methods the Runner needs from the
// queen-mq Go client. The real *queen.QueueBuilder satisfies this.
type Source interface {
	Name() string
	Batch(int) Source
	Wait(bool) Source
	TimeoutMillis(int) Source
	Group(string) Source
	Partitions(int) Source
	SubscriptionMode(string) Source
	SubscriptionFrom(string) Source
	Pop(context.Context) ([]Message, error)
}

// Sink abstracts the destination QueueBuilder.
type Sink interface {
	Name() string
}

// Message is the source message shape the Runner consumes. The real Queen
// Message can be adapted via NewMessageFromQueen helper (TBD).
type Message struct {
	ID            string                 `json:"id"`
	Data          map[string]interface{} `json:"data"`
	CreatedAt     string                 `json:"createdAt"`
	TransactionID string                 `json:"transactionId"`
	LeaseID       string                 `json:"leaseId"`
	Partition     string                 `json:"partition"`
	PartitionID   string                 `json:"partitionId"`
	ConsumerGroup string                 `json:"consumerGroup"`
}

// Stages is the compiled chain consumed by the Runner.
type Stages struct {
	Pre      []StatelessOp
	KeyBy    *operators.KeyByOperator
	Window   WindowOp
	Reducer  *operators.ReduceOperator
	Gate     *operators.GateOperator
	Post     []StatelessOp
	Sink     SinkLike
}

// CompiledStream is what Stream._compile() produces.
type CompiledStream struct {
	Source     Source
	Stages     Stages
	Operators  []util.Operator
	ConfigHash string
}

// StatelessOp is one of Map/Filter/FlatMap.
type StatelessOp interface {
	Apply(context.Context, operators.Envelope) ([]operators.Envelope, error)
	Kind() string
}

// WindowOp is the common interface implemented by all window operators
// (tumbling/sliding/cron/session). Implemented in operators package.
type WindowOp interface {
	Kind() string
	WindowKind() string
	GraceMs() int64
	IdleMs() int64
	AllowedLateMs() int64
	OnLate() string
	EventTime() operators.EventTimeFn
	OperatorTag() string
	Apply(context.Context, operators.Envelope) ([]operators.Envelope, error)
}

// SinkLike is implemented by SinkOperator and ForeachOperator. Discriminated
// by Kind() == "sink" vs "foreach".
type SinkLike interface {
	Kind() string
}

// RunOptions configure a Runner.
type RunOptions struct {
	QueryID          string
	URL              string
	BearerToken      string
	BatchSize        int
	MaxPartitions    int
	MaxWaitMillis    int
	SubscriptionMode string
	SubscriptionFrom string
	Reset            bool
	ConsumerGroup    string
	Logger           util.Logger
}

// Metrics is the snapshot returned by Runner.Metrics().
type Metrics struct {
	CyclesTotal       int64
	FlushCyclesTotal  int64
	MessagesTotal     int64
	PushItemsTotal    int64
	StateOpsTotal     int64
	LateEventsTotal   int64
	ErrorsTotal       int64
	GateAllowsTotal   int64
	GateDenialsTotal  int64
	LastError         string
}

// Runner is the streaming engine's pop/process/cycle loop.
type Runner struct {
	stream          *CompiledStream
	opts            RunOptions
	logger          util.Logger
	http            *HTTPClient
	consumerGroup   string
	serverQueryID   string
	mu              sync.Mutex
	stats           Metrics
	stopped         bool
	cancel          context.CancelFunc
	loopDone        chan struct{}
	flushDone       chan struct{}
	watermarks      map[string]int64
	recent          map[string]string
}

// NewRunner constructs a Runner around a compiled stream.
func NewRunner(stream *CompiledStream, opts RunOptions) *Runner {
	if opts.URL == "" {
		panic("Runner requires URL")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 200
	}
	if opts.MaxPartitions == 0 {
		opts.MaxPartitions = 4
	}
	if opts.MaxWaitMillis == 0 {
		opts.MaxWaitMillis = 1000
	}
	cg := opts.ConsumerGroup
	if cg == "" {
		cg = "streams." + opts.QueryID
	}
	return &Runner{
		stream:        stream,
		opts:          opts,
		logger:        util.MakeLogger(opts.Logger),
		http:          NewHTTPClient(opts.URL, opts.BearerToken, 0, 0),
		consumerGroup: cg,
		watermarks:    map[string]int64{},
		recent:        map[string]string{},
	}
}

// Start registers the query and begins the pop/process loop.
func (r *Runner) Start(ctx context.Context) error {
	srcName := r.stream.Source.Name()
	var sinkName string
	if r.stream.Stages.Sink != nil && r.stream.Stages.Sink.Kind() == "sink" {
		// Match on both value- and pointer-receiver to be tolerant of how
		// Stream.extend stored the operator.
		switch so := r.stream.Stages.Sink.(type) {
		case operators.SinkOperator:
			sinkName = so.QueueName
		case *operators.SinkOperator:
			sinkName = so.QueueName
		}
	}
	reg, err := RegisterQuery(ctx, r.http, r.opts.QueryID, srcName, sinkName, r.stream.ConfigHash, r.opts.Reset)
	if err != nil {
		return err
	}
	r.serverQueryID = reg.QueryID
	r.logger.Info("query registered", map[string]interface{}{
		"queryId": r.opts.QueryID, "fresh": reg.Fresh, "didReset": reg.DidReset,
	})

	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.loopDone = make(chan struct{})

	// Start the idle-flush loop if we have a window operator.
	if r.stream.Stages.Window != nil && r.stream.Stages.Window.IdleMs() > 0 {
		r.flushDone = make(chan struct{})
		go r.flushLoop(loopCtx, r.stream.Stages.Window.IdleMs())
	}

	go r.loop(loopCtx)
	return nil
}

// Stop gracefully shuts the runner down.
func (r *Runner) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
	if r.loopDone != nil {
		<-r.loopDone
	}
	if r.flushDone != nil {
		<-r.flushDone
	}
}

// Metrics returns a snapshot.
func (r *Runner) Metrics() Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// ---------------------------------------------------------------------------
// Loop / cycle
// ---------------------------------------------------------------------------

func (r *Runner) loop(ctx context.Context) {
	defer close(r.loopDone)
	for {
		if r.isStopped(ctx) {
			return
		}
		messages, err := r.popMessages(ctx)
		if err != nil {
			r.reportError(err, "pop")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		groups := r.groupByPartition(messages)
		for _, g := range groups {
			if r.isStopped(ctx) {
				return
			}
			r.touchPartition(g.PartitionID, g.PartitionName)
			if err := r.processCycle(ctx, g); err != nil {
				r.reportError(err, "cycle")
			} else {
				r.mu.Lock()
				r.stats.CyclesTotal++
				r.stats.MessagesTotal += int64(len(g.Messages))
				r.mu.Unlock()
			}
		}
	}
}

func (r *Runner) isStopped(ctx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return true
	}
	select {
	case <-ctx.Done():
		r.stopped = true
		return true
	default:
		return false
	}
}

func (r *Runner) reportError(err error, phase string) {
	r.mu.Lock()
	r.stats.ErrorsTotal++
	r.stats.LastError = err.Error()
	r.mu.Unlock()
	r.logger.Error(phase+" failed", map[string]interface{}{"error": err.Error()})
}

func (r *Runner) popMessages(ctx context.Context) ([]Message, error) {
	src := r.stream.Source.
		Batch(r.opts.BatchSize).
		Wait(true).
		TimeoutMillis(r.opts.MaxWaitMillis).
		Group(r.consumerGroup)
	if r.opts.MaxPartitions > 1 {
		src = src.Partitions(r.opts.MaxPartitions)
	}
	if r.opts.SubscriptionMode != "" {
		src = src.SubscriptionMode(r.opts.SubscriptionMode)
	}
	if r.opts.SubscriptionFrom != "" {
		src = src.SubscriptionFrom(r.opts.SubscriptionFrom)
	}
	return src.Pop(ctx)
}

type partitionGroup struct {
	PartitionID   string
	PartitionName string
	LeaseID       string
	ConsumerGroup string
	Messages      []Message
}

func (r *Runner) groupByPartition(messages []Message) []partitionGroup {
	idx := map[string]*partitionGroup{}
	for _, m := range messages {
		pid := m.PartitionID
		if pid == "" {
			pid = "unknown"
		}
		g, ok := idx[pid]
		if !ok {
			cg := m.ConsumerGroup
			if cg == "" {
				cg = r.consumerGroup
			}
			g = &partitionGroup{PartitionID: pid, PartitionName: m.Partition, LeaseID: m.LeaseID, ConsumerGroup: cg}
			idx[pid] = g
		}
		g.Messages = append(g.Messages, m)
	}
	out := make([]partitionGroup, 0, len(idx))
	for _, g := range idx {
		out = append(out, *g)
	}
	return out
}

func (r *Runner) touchPartition(pid, pname string) {
	if pid == "" {
		return
	}
	r.mu.Lock()
	r.recent[pid] = pname
	r.mu.Unlock()
}

func (r *Runner) processCycle(ctx context.Context, g partitionGroup) error {
	stages := r.stream.Stages
	pid := g.PartitionID

	// Sort messages by (createdAt, id) FIFO.
	ordered := append([]Message(nil), g.Messages...)
	sort.SliceStable(ordered, func(i, j int) bool {
		ta, _ := time.Parse(time.RFC3339Nano, ordered[i].CreatedAt)
		tb, _ := time.Parse(time.RFC3339Nano, ordered[j].CreatedAt)
		if !ta.Equal(tb) {
			return ta.Before(tb)
		}
		return ordered[i].ID < ordered[j].ID
	})

	// 1. pre-stages
	envs := make([]operators.Envelope, len(ordered))
	for i, m := range ordered {
		envs[i] = operators.Envelope{
			Msg:   messageToMap(m),
			Key:   m.PartitionID,
			Value: m.Data,
		}
	}
	for _, op := range stages.Pre {
		next := []operators.Envelope{}
		for _, env := range envs {
			outs, err := op.Apply(ctx, env)
			if err != nil {
				return err
			}
			next = append(next, outs...)
		}
		envs = next
	}

	// 2. keyBy
	if stages.KeyBy != nil {
		next := []operators.Envelope{}
		for _, env := range envs {
			outs, err := stages.KeyBy.Apply(ctx, env)
			if err != nil {
				return err
			}
			next = append(next, outs...)
		}
		envs = next
	}

	// 3. gate path
	if stages.Gate != nil {
		return r.processGate(ctx, stages, envs, ordered, g, pid)
	}

	// 3. window + reducer
	var stateOps []operators.StateOp
	var emits []operators.ReduceEmit
	if stages.Window != nil {
		if stages.Window.WindowKind() == "session" {
			res, err := r.processSession(ctx, stages, envs, pid)
			if err != nil {
				return err
			}
			stateOps = res.StateOps
			for _, e := range res.Emits {
				emits = append(emits, operators.ReduceEmit{
					Key: e.Key, WindowStart: e.WindowStart, WindowEnd: e.WindowEnd,
					WindowKey: e.WindowKey, Value: e.Value,
				})
			}
		} else {
			res, err := r.processStatelessWindow(ctx, stages, envs, pid, ordered)
			if err != nil {
				return err
			}
			stateOps = res.StateOps
			emits = res.Emits
		}
	} else {
		for _, env := range envs {
			emits = append(emits, operators.ReduceEmit{Key: env.Key, Value: env.Value})
		}
	}

	// 4. post-stages
	emits, err := r.applyPostStages(ctx, stages.Post, emits, pid, g.PartitionName, ordered)
	if err != nil {
		return err
	}

	// 5. sink / foreach
	pushItems, err := r.buildSink(ctx, stages.Sink, emits, pid, g.PartitionName)
	if err != nil {
		return err
	}

	// 6. ack
	var ack map[string]interface{}
	if len(ordered) > 0 {
		last := ordered[len(ordered)-1]
		leaseID := last.LeaseID
		if leaseID == "" {
			leaseID = g.LeaseID
		}
		ack = map[string]interface{}{
			"transactionId": last.TransactionID,
			"leaseId":       leaseID,
			"status":        "completed",
			"count":         len(ordered),
		}
	}

	r.mu.Lock()
	r.stats.StateOpsTotal += int64(len(stateOps))
	r.stats.PushItemsTotal += int64(len(pushItems))
	r.mu.Unlock()

	_, err = CommitCycle(ctx, r.http, r.serverQueryID, pid, g.ConsumerGroup, stateOps, pushItems, ack, true)
	return err
}

// ---------------------------------------------------------------------------
// Gate path
// ---------------------------------------------------------------------------

func (r *Runner) processGate(ctx context.Context, stages Stages, envs []operators.Envelope, ordered []Message, g partitionGroup, pid string) error {
	loaded, err := GetState(ctx, r.http, r.serverQueryID, pid, nil)
	if err != nil {
		return err
	}
	live := map[string]map[string]interface{}{}
	touched := map[string]bool{}
	now := time.Now().UnixMilli()
	allowedCount := 0
	firstDeny := -1
	var allowedEnvs []operators.Envelope

	ensure := func(key string) map[string]interface{} {
		if s, ok := live[key]; ok {
			return s
		}
		s := map[string]interface{}{}
		if v, ok := loaded[key]; ok {
			if obj, ok := v.(map[string]interface{}); ok {
				b, _ := json.Marshal(obj)
				_ = json.Unmarshal(b, &s)
			}
		}
		live[key] = s
		return s
	}

	for i, env := range envs {
		state := ensure(env.Key)
		gctx := operators.GateContext{
			State: state, StreamTimeMs: now, PartitionID: pid,
			Partition: g.PartitionName, Key: env.Key,
		}
		allow, err := stages.Gate.Evaluate(env, gctx)
		if err != nil {
			r.reportError(err, "gate-eval")
			return nil
		}
		if allow {
			touched[env.Key] = true
			allowedCount++
			allowedEnvs = append(allowedEnvs, env)
		} else {
			firstDeny = i
			break
		}
	}

	if allowedCount == 0 {
		r.mu.Lock()
		r.stats.GateDenialsTotal += int64(len(envs))
		r.mu.Unlock()
		return nil
	}

	var ops []operators.StateOp
	for k := range touched {
		ops = append(ops, operators.StateOp{Type: "upsert", Key: k, Value: live[k]})
	}

	emits := make([]operators.ReduceEmit, len(allowedEnvs))
	for i, env := range allowedEnvs {
		emits[i] = operators.ReduceEmit{Key: env.Key, Value: env.Value}
	}
	emits, err = r.applyPostStages(ctx, stages.Post, emits, pid, g.PartitionName, ordered)
	if err != nil {
		return err
	}
	pushItems, err := r.buildSink(ctx, stages.Sink, emits, pid, g.PartitionName)
	if err != nil {
		return err
	}

	last := allowedEnvs[len(allowedEnvs)-1]
	lastSrc, _ := last.Msg["__source__"].(Message)
	if lastSrc.TransactionID == "" {
		// Fall back to the original ordered slice.
		// Find msg whose ID matches last.Msg["id"].
		for _, m := range ordered {
			if m.ID == fmt.Sprint(last.Msg["id"]) {
				lastSrc = m
				break
			}
		}
	}
	leaseID := lastSrc.LeaseID
	if leaseID == "" {
		leaseID = g.LeaseID
	}
	ack := map[string]interface{}{
		"transactionId": lastSrc.TransactionID,
		"leaseId":       leaseID,
		"status":        "completed",
		"count":         allowedCount,
	}

	releaseLease := firstDeny < 0
	r.mu.Lock()
	r.stats.GateAllowsTotal += int64(allowedCount)
	if firstDeny >= 0 {
		r.stats.GateDenialsTotal += int64(len(envs) - allowedCount)
	}
	r.stats.StateOpsTotal += int64(len(ops))
	r.stats.PushItemsTotal += int64(len(pushItems))
	r.mu.Unlock()

	_, err = CommitCycle(ctx, r.http, r.serverQueryID, pid, g.ConsumerGroup, ops, pushItems, ack, releaseLease)
	return err
}

// ---------------------------------------------------------------------------
// Stateless window path (tumbling / sliding / cron)
// ---------------------------------------------------------------------------

func (r *Runner) processStatelessWindow(ctx context.Context, stages Stages, envs []operators.Envelope, pid string, ordered []Message) (operators.ReduceResult, error) {
	w := stages.Window
	reducer := stages.Reducer

	var loaded map[string]interface{}
	if reducer != nil {
		var err error
		loaded, err = GetState(ctx, r.http, r.serverQueryID, pid, nil)
		if err != nil {
			return operators.ReduceResult{}, err
		}
	}

	if w.EventTime() != nil {
		if row, ok := loaded[watermarkStateKey].(map[string]interface{}); ok {
			if v, ok := row["eventTimeMs"].(float64); ok {
				r.watermarks[pid] = int64(v)
			}
		}
	}

	var annotated []operators.Envelope
	wmExisting := r.watermarks[pid]
	maxObserved := int64(-1 << 62)
	dropped := int64(0)

	for _, env := range envs {
		outs, err := w.Apply(ctx, env)
		if err != nil {
			r.reportError(err, "window-apply")
			continue
		}
		for _, a := range outs {
			if w.EventTime() != nil {
				if a.EventTimeMs > maxObserved {
					maxObserved = a.EventTimeMs
				}
				if wmExisting != 0 && a.EventTimeMs < wmExisting {
					if w.OnLate() == "drop" {
						dropped++
						continue
					}
				}
			}
			annotated = append(annotated, a)
		}
	}
	if dropped > 0 {
		r.mu.Lock()
		r.stats.LateEventsTotal += dropped
		r.mu.Unlock()
	}

	if reducer == nil {
		emits := make([]operators.ReduceEmit, len(annotated))
		for i, a := range annotated {
			emits[i] = operators.ReduceEmit{Key: a.Key, Value: a.Value}
		}
		return operators.ReduceResult{Emits: emits}, nil
	}

	var clock int64
	if w.EventTime() != nil {
		advanced := wmExisting
		if maxObserved > int64(-1<<62) {
			cand := maxObserved - w.AllowedLateMs()
			if cand > advanced {
				advanced = cand
			}
		}
		r.watermarks[pid] = advanced
		clock = advanced
	} else {
		var pt int64 = -1 << 62
		for _, m := range ordered {
			if t, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err == nil {
				if t.UnixMilli() > pt {
					pt = t.UnixMilli()
				}
			}
		}
		clock = pt
	}

	res := reducer.Run(annotated, loaded, clock, w.OperatorTag(), w.GraceMs())

	if w.EventTime() != nil {
		if wm, ok := r.watermarks[pid]; ok && wm > int64(-1<<62) {
			res.StateOps = append(res.StateOps, operators.StateOp{
				Type: "upsert", Key: watermarkStateKey,
				Value: map[string]interface{}{"eventTimeMs": wm},
			})
		}
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Session path
// ---------------------------------------------------------------------------

func (r *Runner) processSession(ctx context.Context, stages Stages, envs []operators.Envelope, pid string) (operators.SessionResult, error) {
	w, ok := stages.Window.(*operators.WindowSessionOperator)
	if !ok {
		return operators.SessionResult{}, fmt.Errorf("session path requires WindowSessionOperator")
	}
	reducer := stages.Reducer
	if reducer == nil {
		return operators.SessionResult{}, fmt.Errorf("window_session requires .reduce() or .aggregate()")
	}

	all, err := GetState(ctx, r.http, r.serverQueryID, pid, nil)
	if err != nil {
		return operators.SessionResult{}, err
	}
	tag := w.OperatorTag()
	state := map[string]interface{}{}
	for k, v := range all {
		if strings.HasPrefix(k, tag+"\u001fopen\u001f") {
			state[k] = v
		}
	}

	enriched := []operators.Envelope{}
	for _, env := range envs {
		outs, err := w.Apply(ctx, env)
		if err != nil {
			continue
		}
		if len(outs) > 0 {
			enriched = append(enriched, outs[0])
		}
	}

	now := time.Now().UnixMilli()
	initial := func() interface{} {
		if reducer.Initial == nil {
			return float64(0)
		}
		b, _ := json.Marshal(reducer.Initial)
		var v interface{}
		_ = json.Unmarshal(b, &v)
		return v
	}
	res := w.RunSession(enriched, state, reducer.Fn, initial, now)

	if w.EventTime() != nil {
		if wm, ok := r.watermarks[pid]; ok && wm > int64(-1<<62) {
			res.StateOps = append(res.StateOps, operators.StateOp{
				Type: "upsert", Key: watermarkStateKey,
				Value: map[string]interface{}{"eventTimeMs": wm},
			})
		}
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Post stages, sink, idle flush
// ---------------------------------------------------------------------------

func (r *Runner) applyPostStages(ctx context.Context, post []StatelessOp, emits []operators.ReduceEmit, pid, pname string, _ []Message) ([]operators.ReduceEmit, error) {
	if len(post) == 0 || len(emits) == 0 {
		return emits, nil
	}
	working := emits
	for _, op := range post {
		next := []operators.ReduceEmit{}
		for _, e := range working {
			ec := operators.EmitCtx{
				"partition":   pname,
				"partitionId": pid,
				"key":         e.Key,
				"windowKey":   e.WindowKey,
				"windowStart": e.WindowStart,
				"windowEnd":   e.WindowEnd,
			}
			// In post-stage mode the user fn sees the aggregate VALUE
			// as its first argument (mirrors JS where Runner sets
			// `msg = e.value` on the post-stage envelope). The pre-
			// stage envelope passes the source Queen message; Map/Filter/
			// FlatMap now prefer Value over Msg when Msg is unset, so we
			// can leave Msg=nil here and the user fn still gets the value.
			env := operators.Envelope{Msg: nil, Key: e.Key, Value: e.Value, Ctx: ec}
			outs, err := op.Apply(ctx, env)
			if err != nil {
				return nil, err
			}
			for _, o := range outs {
				newVal := o.Value
				if newVal == nil {
					newVal = o.Msg
				}
				next = append(next, operators.ReduceEmit{
					Key: e.Key, Value: newVal,
					WindowStart: e.WindowStart, WindowEnd: e.WindowEnd, WindowKey: e.WindowKey,
				})
			}
		}
		working = next
	}
	return working, nil
}

func (r *Runner) buildSink(_ context.Context, sink SinkLike, emits []operators.ReduceEmit, pid, pname string) ([]operators.PushItem, error) {
	if sink == nil {
		return nil, nil
	}
	// Operators are stored as VALUES (not pointers) by Stream.extend, so the
	// type-switch must match values too.
	switch s := sink.(type) {
	case operators.SinkOperator:
		entries := make([]operators.SinkEntry, len(emits))
		for i, e := range emits {
			entries[i] = operators.SinkEntry{Key: e.Key, Value: e.Value}
		}
		return s.BuildPushItems(entries, pname), nil
	case *operators.SinkOperator:
		entries := make([]operators.SinkEntry, len(emits))
		for i, e := range emits {
			entries[i] = operators.SinkEntry{Key: e.Key, Value: e.Value}
		}
		return s.BuildPushItems(entries, pname), nil
	case operators.ForeachOperator:
		entries := make([]struct {
			Value interface{}
			Ctx   operators.EmitCtx
		}, len(emits))
		for i, e := range emits {
			entries[i] = struct {
				Value interface{}
				Ctx   operators.EmitCtx
			}{
				Value: e.Value,
				Ctx: operators.EmitCtx{
					"partition": pname, "partitionId": pid, "key": e.Key,
					"windowKey": e.WindowKey, "windowStart": e.WindowStart, "windowEnd": e.WindowEnd,
				},
			}
		}
		return nil, s.RunEffects(entries)
	case *operators.ForeachOperator:
		entries := make([]struct {
			Value interface{}
			Ctx   operators.EmitCtx
		}, len(emits))
		for i, e := range emits {
			entries[i] = struct {
				Value interface{}
				Ctx   operators.EmitCtx
			}{
				Value: e.Value,
				Ctx: operators.EmitCtx{
					"partition": pname, "partitionId": pid, "key": e.Key,
					"windowKey": e.WindowKey, "windowStart": e.WindowStart, "windowEnd": e.WindowEnd,
				},
			}
		}
		return nil, s.RunEffects(entries)
	}
	return nil, nil
}

// flushLoop runs the idle-flush sweep.
func (r *Runner) flushLoop(ctx context.Context, intervalMs int64) {
	defer close(r.flushDone)
	t := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r.isStopped(ctx) {
				return
			}
			r.flushTick(ctx)
		}
	}
}

func (r *Runner) flushTick(ctx context.Context) {
	w := r.stream.Stages.Window
	if w == nil {
		return
	}
	r.mu.Lock()
	partitions := make(map[string]string, len(r.recent))
	for k, v := range r.recent {
		partitions[k] = v
	}
	r.mu.Unlock()
	for pid, pname := range partitions {
		_ = r.flushPartition(ctx, pid, pname)
	}
}

func (r *Runner) flushPartition(ctx context.Context, pid, pname string) error {
	w := r.stream.Stages.Window
	stages := r.stream.Stages

	var clock int64
	if w.EventTime() != nil {
		clock = r.watermarks[pid]
	} else {
		clock = time.Now().UnixMilli()
	}
	if clock == 0 {
		return nil
	}
	if w.WindowKind() == "session" {
		return r.flushSessionPartition(ctx, pid, pname, clock)
	}
	ripeAt := clock - w.GraceMs()
	if ripeAt < 0 {
		return nil
	}

	body := map[string]interface{}{
		"query_id":          r.serverQueryID,
		"partition_id":      pid,
		"keys":              []string{},
		"key_prefix":        w.OperatorTag() + "\u001f",
		"ripe_at_or_before": ripeAt,
	}
	res, err := r.http.Post(ctx, "/streams/v1/state/get", body)
	if err != nil {
		return err
	}
	rows, _ := res["rows"].([]interface{})
	if len(rows) == 0 {
		return nil
	}
	var emits []operators.ReduceEmit
	var stateOps []operators.StateOp
	for _, ri := range rows {
		row, _ := ri.(map[string]interface{})
		if row == nil {
			continue
		}
		k, _ := row["key"].(string)
		parts := operators.ParseStateKey(k)
		if parts == nil || parts.OperatorTag != w.OperatorTag() {
			continue
		}
		v := row["value"]
		var ws, we int64
		var acc interface{} = v
		if obj, ok := v.(map[string]interface{}); ok {
			if x, ok := obj["windowStart"].(float64); ok {
				ws = int64(x)
			}
			if x, ok := obj["windowEnd"].(float64); ok {
				we = int64(x)
			}
			if a, ok := obj["acc"]; ok {
				acc = a
			}
		}
		emits = append(emits, operators.ReduceEmit{
			Key: parts.UserKey, WindowStart: ws, WindowEnd: we,
			WindowKey: parts.WindowKey, Value: acc,
		})
		stateOps = append(stateOps, operators.StateOp{Type: "delete", Key: k})
	}
	if len(emits) == 0 {
		return nil
	}
	emits, err = r.applyPostStages(ctx, stages.Post, emits, pid, pname, nil)
	if err != nil {
		return err
	}
	pushItems, err := r.buildSink(ctx, stages.Sink, emits, pid, pname)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.stats.FlushCyclesTotal++
	r.stats.StateOpsTotal += int64(len(stateOps))
	r.stats.PushItemsTotal += int64(len(pushItems))
	r.mu.Unlock()
	_, err = CommitCycle(ctx, r.http, r.serverQueryID, pid, r.consumerGroup, stateOps, pushItems, nil, true)
	return err
}

func (r *Runner) flushSessionPartition(ctx context.Context, pid, pname string, clock int64) error {
	w, _ := r.stream.Stages.Window.(*operators.WindowSessionOperator)
	if w == nil {
		return nil
	}
	all, err := GetState(ctx, r.http, r.serverQueryID, pid, nil)
	if err != nil {
		return err
	}
	state := map[string]interface{}{}
	for k, v := range all {
		if strings.HasPrefix(k, w.OperatorTag()+"\u001fopen\u001f") {
			state[k] = v
		}
	}
	if len(state) == 0 {
		return nil
	}
	reducer := r.stream.Stages.Reducer
	initial := func() interface{} {
		if reducer.Initial == nil {
			return float64(0)
		}
		b, _ := json.Marshal(reducer.Initial)
		var v interface{}
		_ = json.Unmarshal(b, &v)
		return v
	}
	res := w.RunSession([]operators.Envelope{}, state, reducer.Fn, initial, clock)
	if len(res.Emits) == 0 {
		return nil
	}
	emits := make([]operators.ReduceEmit, len(res.Emits))
	for i, e := range res.Emits {
		emits[i] = operators.ReduceEmit{Key: e.Key, WindowStart: e.WindowStart, WindowEnd: e.WindowEnd, WindowKey: e.WindowKey, Value: e.Value}
	}
	emits, err = r.applyPostStages(ctx, r.stream.Stages.Post, emits, pid, pname, nil)
	if err != nil {
		return err
	}
	pushItems, err := r.buildSink(ctx, r.stream.Stages.Sink, emits, pid, pname)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.stats.FlushCyclesTotal++
	r.stats.StateOpsTotal += int64(len(res.StateOps))
	r.stats.PushItemsTotal += int64(len(pushItems))
	r.mu.Unlock()
	_, err = CommitCycle(ctx, r.http, r.serverQueryID, pid, r.consumerGroup, res.StateOps, pushItems, nil, true)
	return err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func messageToMap(m Message) map[string]interface{} {
	return map[string]interface{}{
		"id":            m.ID,
		"data":          m.Data,
		"createdAt":     m.CreatedAt,
		"transactionId": m.TransactionID,
		"leaseId":       m.LeaseID,
		"partition":     m.Partition,
		"partitionId":   m.PartitionID,
		"consumerGroup": m.ConsumerGroup,
	}
}
