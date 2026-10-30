package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/streams/operators"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/util"
)

const watermarkStateKey = "__wm__"

// partitionRecencyMs bounds how long a partition stays in the idle-flush sweep
// after the last time this runner popped from it. Partition ownership rotates
// between replicas (the gate/window cycle releases the lease on every commit),
// so without a cutoff a runner keeps flushing partitions another replica now
// owns — and since the flush path commits with ack=nil, the server's lease
// check (inside `IF v_ack IS NOT NULL`) never runs and both replicas can read,
// emit and delete the same ripe window. Mirrors PARTITION_RECENCY_MS in
// clients/client-js/client-v2/streams/runtime/Runner.js. Only for a broker
// without POST /streams/v1/claim (before 2.7), and for event-time windows.
const partitionRecencyMs int64 = 5 * 60 * 1000 // 5 minutes

// Closing processing-time windows under a claim (broker 2.7, issue #153).
// Same values as clients/client-rust/src/streams/runner.rs.
const (
	// sweepEvery is how often a runner asks the broker for the partitions
	// holding the query's state (POST /streams/v1/state/partitions): after a
	// restart, a scale-down, or for partitions another runner left, these are
	// the windows nobody tracks.
	sweepEvery = 30 * time.Second
	// sweepUntouchedMs is how old a partition's newest state row must be
	// before a later sweep takes it on: a younger one is some runner's live
	// work, which tracks it already. The first sweep after a start takes
	// every partition: this runner tracks nothing yet.
	sweepUntouchedMs int64 = 30_000
	// sweepPage is the partitions per page of the sweep.
	sweepPage = 1000
	// claimBatch is the partitions per claim, which is also the most one
	// flush tick closes: a sweep that finds thousands of quiet partitions
	// (after an outage) works through them a batch per tick instead of all
	// at once.
	claimBatch = 100
	// claimLeaseS is the lease a claim asks for. A close is one state read
	// and one commit.
	claimLeaseS = 30
	// claimRetryMs is how soon to try again a partition someone else holds,
	// or whose claim or close failed.
	claimRetryMs int64 = 1000
	// stopFlushWait is how long Stop waits for a close in flight before
	// cutting it short.
	stopFlushWait = 10 * time.Second
)

// Whether the broker serves POST /streams/v1/claim.
const (
	claimsUnknown int32 = iota
	claimsYes
	// claimsNo: an older broker, so the unleased flush of recent partitions.
	claimsNo
)

// dueEntry is a partition with open windows, and when the earliest closes (ms,
// on the broker's clock: processing time is the messages' createdAt).
type dueEntry struct {
	name string
	atMs int64
}

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
	Conflation(bool) Source
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
	Pre     []StatelessOp
	KeyBy   *operators.KeyByOperator
	Window  WindowOp
	Reducer *operators.ReduceOperator
	Gate    *operators.GateOperator
	Post    []StatelessOp
	Sink    SinkLike
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
	// Conflation requests last-value delivery for this query's consumer group:
	// each pop of a partition carries only its newest visible message
	// (PLAN_CONFLATION.md §1.2). Sensible for a query whose windows are keyed
	// on the partition and only the freshest state matters; wrong for anything
	// that aggregates over every event, which is why it is off by default.
	//
	// Requires broker >= 1.1.0 - an older broker ignores it and the pop fails
	// loudly rather than silently replaying the whole backlog.
	Conflation bool
	Logger     util.Logger
}

// Metrics is the snapshot returned by Runner.Metrics().
type Metrics struct {
	CyclesTotal      int64
	FlushCyclesTotal int64
	MessagesTotal    int64
	PushItemsTotal   int64
	StateOpsTotal    int64
	LateEventsTotal  int64
	ErrorsTotal      int64
	GateAllowsTotal  int64
	GateDenialsTotal int64
	LastError        string
}

// Runner is the streaming engine's pop/process/cycle loop.
type Runner struct {
	stream        *CompiledStream
	opts          RunOptions
	logger        util.Logger
	http          *HTTPClient
	consumerGroup string
	serverQueryID string
	mu            sync.Mutex
	stats         Metrics
	stopped       bool
	cancel        context.CancelFunc
	// flushCancel cuts short the calls of the idle-flush goroutine. Stop
	// calls it only when a close is still running after stopFlushWait: a
	// close cut short keeps its claim on the partition until the lease runs
	// out.
	flushCancel context.CancelFunc
	loopDone    chan struct{}
	flushDone   chan struct{}
	watermarks  map[string]int64
	// due holds the processing-time windows still open, by partition id:
	// what the idle flush claims and closes once their time has come.
	// Guarded by mu.
	due map[string]dueEntry
	// claims is claimsUnknown, claimsYes or claimsNo.
	claims atomic.Int32
	// recent is the idle-flush sweep set: partitionId -> last pop. Entries
	// older than partitionRecencyMs are pruned on every touch and on every
	// sweep, so a partition lost to another replica leaves the sweep instead
	// of being flushed forever. Guarded by mu.
	recent map[string]recentPartition
	// nowMs is the clock used by the recency cutoff and the processing-time
	// flush. Set once at construction; overridden only by tests, before Start.
	nowMs func() int64
	// partLocks serialises the pop/cycle loop against the idle-flush goroutine
	// per partition. Both paths do read(state) -> compute -> commit against the
	// same (query_id, partition_id) rows; the broker serialises only the
	// COMMITS, so an unsynchronised flush can re-emit a window
	// the cycle just emitted+deleted (duplicate) or clobber a freshly-reduced
	// accumulator (drop). Guarded by mu.
	partLocks map[string]*sync.Mutex
}

// recentPartition is one entry of the idle-flush sweep set: the partition name
// to report downstream plus the last time this runner popped from it.
type recentPartition struct {
	name      string
	touchedAt int64
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
		due:           map[string]dueEntry{},
		recent:        map[string]recentPartition{},
		nowMs:         func() int64 { return time.Now().UnixMilli() },
		partLocks:     map[string]*sync.Mutex{},
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

	// Start the idle-flush loop if we have a window operator. It stops when
	// loopCtx ends, but its calls run on their own context: Stop lets a close
	// in flight finish (see stopFlushWait).
	if r.stream.Stages.Window != nil && r.stream.Stages.Window.IdleMs() > 0 {
		flushCtx, flushCancel := context.WithCancel(context.WithoutCancel(ctx))
		r.flushCancel = flushCancel
		r.flushDone = make(chan struct{})
		go r.flushLoop(loopCtx, flushCtx, r.stream.Stages.Window.IdleMs())
	}

	go r.loop(loopCtx)
	return nil
}

// Stop gracefully shuts the runner down: it waits for the cycle in flight,
// and up to 10 seconds for a close in flight. A close cut short would keep its
// claim on the partition until the lease ran out. Calling it again, or after
// the context given to Start ended, waits the same way.
func (r *Runner) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	// Ends the pop loop, and wakes the flush loop.
	if r.cancel != nil {
		r.cancel()
	}
	if r.loopDone != nil {
		<-r.loopDone
	}
	if r.flushDone != nil {
		select {
		case <-r.flushDone:
		case <-time.After(stopFlushWait):
			r.flushCancel()
			<-r.flushDone
		}
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
			// Under claims the due map tracks the partition (processCycle);
			// the recent set is the flush of a broker without them.
			if !r.leasedCloses() || r.claims.Load() != claimsYes {
				r.touchPartition(g.PartitionID, g.PartitionName)
			}
			// Mutual exclusion with the idle-flush goroutine (see partLocks).
			l := r.partitionLock(g.PartitionID)
			l.Lock()
			err := r.processCycle(ctx, g)
			l.Unlock()
			if err != nil {
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
	// Unconditional, and it has to be: the runtime has already defaulted
	// MaxPartitions (to 4), so this is always a decision the streams layer
	// made. Skipping the call for MaxPartitions == 1 used to be a harmless
	// optimisation -- 1 was what an omitted `partitions` meant on the wire --
	// but with pop autopilot an omitted `partitions` means "broker, you
	// choose", which would widen a query that explicitly asked for one
	// partition per pop.
	src = src.Partitions(r.opts.MaxPartitions)
	if r.opts.SubscriptionMode != "" {
		src = src.SubscriptionMode(r.opts.SubscriptionMode)
	}
	if r.opts.SubscriptionFrom != "" {
		src = src.SubscriptionFrom(r.opts.SubscriptionFrom)
	}
	if r.opts.Conflation {
		src = src.Conflation(true)
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

// partitionLock returns the mutex serialising cycle vs idle-flush work for one
// partition, creating it on first use.
func (r *Runner) partitionLock(pid string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.partLocks[pid]
	if !ok {
		l = &sync.Mutex{}
		r.partLocks[pid] = l
	}
	return l
}

// getWatermark / setWatermark guard the watermarks map, which is touched by
// both the cycle goroutine and the flush goroutine (possibly for different
// partitions at once — a per-partition lock does not make the shared map safe).
func (r *Runner) getWatermark(pid string) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.watermarks[pid]
	return v, ok
}

func (r *Runner) setWatermark(pid string, v int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.watermarks[pid] = v
}

func (r *Runner) touchPartition(pid, pname string) {
	if pid == "" {
		return
	}
	now := r.nowMs()
	r.mu.Lock()
	r.recent[pid] = recentPartition{name: pname, touchedAt: now}
	r.pruneRecentLocked(now)
	r.mu.Unlock()
}

// pruneRecentLocked drops sweep entries whose last pop is older than the
// recency window. Caller holds mu.
func (r *Runner) pruneRecentLocked(now int64) {
	cutoff := now - partitionRecencyMs
	for pid, e := range r.recent {
		if e.touchedAt < cutoff {
			delete(r.recent, pid)
		}
	}
}

// partitionsToFlush prunes the sweep set and returns what is still in it. The
// prune runs here as well as in touchPartition so a runner that stops popping
// entirely (quiet source, or every partition reassigned) also stops flushing.
func (r *Runner) partitionsToFlush() map[string]string {
	now := r.nowMs()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneRecentLocked(now)
	out := make(map[string]string, len(r.recent))
	for pid, e := range r.recent {
		out[pid] = e.name
	}
	return out
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
	// When the earliest window left open closes (nil: none).
	var nextClose *int64
	if stages.Window != nil {
		if stages.Window.WindowKind() == "session" {
			res, err := r.processSession(ctx, stages, envs, pid, ordered)
			if err != nil {
				return err
			}
			stateOps = res.StateOps
			nextClose = res.NextCloseMs
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
			nextClose = res.NextCloseMs
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
	if err != nil {
		return err
	}
	// What stays open closes when its time comes, under a claim. Set here,
	// under the partition lock, so a close on this partition cannot
	// overwrite it with an older answer. A broker without claims never
	// closes these, so they are not kept.
	if r.leasedCloses() && r.claims.Load() != claimsNo {
		r.setDue(pid, g.PartitionName, nextClose)
	}
	return nil
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
				r.setWatermark(pid, int64(v))
			}
		}
	}

	var annotated []operators.Envelope
	wmExisting, _ := r.getWatermark(pid)
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
		r.setWatermark(pid, advanced)
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
		if wm, ok := r.getWatermark(pid); ok && wm > int64(-1<<62) {
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

func (r *Runner) processSession(ctx context.Context, stages Stages, envs []operators.Envelope, pid string, ordered []Message) (operators.SessionResult, error) {
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

	// The close trigger. In processing time it is the batch's newest
	// createdAt: the broker's clock, the one a close under a claim uses too,
	// so a cycle and a close agree on which sessions are over whatever this
	// machine's clock says.
	now := time.Now().UnixMilli()
	if w.EventTime() == nil {
		if newest, ok := newestCreatedAt(ordered); ok {
			now = newest
		}
	}
	res := w.RunSession(enriched, state, reducer.Fn, reducerInitial(reducer), now)

	if w.EventTime() != nil {
		if wm, ok := r.getWatermark(pid); ok && wm > int64(-1<<62) {
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

// flushLoop runs the idle flush every intervalMs until loopCtx ends. The calls
// of a close under a claim use callCtx, which Stop cancels only after
// stopFlushWait.
//
// A processing-time window with a reducer closes under a claim (closeDue),
// once its time has come, also on a partition this process never read. An
// event-time window, or any window on a broker without claims, keeps the
// flush of the partitions this process read recently (flushTick).
func (r *Runner) flushLoop(loopCtx, callCtx context.Context, intervalMs int64) {
	defer close(r.flushDone)
	defer func() {
		if r.flushCancel != nil {
			r.flushCancel()
		}
	}()
	t := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer t.Stop()
	// The first sweep runs right at start and takes every partition holding
	// state: this runner tracks nothing yet.
	var nextSweep time.Time
	untouchedMs := int64(0)
	immediate := r.leasedCloses()
	for {
		if !immediate {
			select {
			case <-loopCtx.Done():
				return
			case <-t.C:
			}
		}
		immediate = false
		if r.isStopped(loopCtx) {
			return
		}
		if !r.leasedCloses() || r.claims.Load() == claimsNo {
			// The old flush, as it was: it holds no lease, so Stop cuts it
			// short.
			r.flushTick(loopCtx)
			continue
		}
		if !time.Now().Before(nextSweep) {
			r.sweep(loopCtx, callCtx, untouchedMs)
			nextSweep = time.Now().Add(sweepEvery)
			untouchedMs = sweepUntouchedMs
		}
		if r.claims.Load() != claimsNo {
			r.closeDue(loopCtx, callCtx)
		}
	}
}

// ---------------------------------------------------------------------------
// Closes under a claim
//
// A processing-time window closes when its time comes even if its partition
// gets no more messages. The runner keeps, per partition, when the earliest
// open window closes (from each cycle it commits, and from the sweep), claims
// the partition when that time has come (POST /streams/v1/claim: a lease with
// an empty batch, granted only when nobody holds the partition and the group
// has read all of it), and closes the windows in a cycle that acks the empty
// batch. The broker refuses that cycle whole when the lease is gone, so two
// runners never close one window, and a close never deletes a window another
// runner just extended. The clock is the broker's, the one createdAt comes
// from.
// ---------------------------------------------------------------------------

// leasedCloses reports whether this stream closes its windows under claims:
// processing-time windows with a reducer and an idle flush. Event-time
// windows close on the watermark, which only events move.
func (r *Runner) leasedCloses() bool {
	w := r.stream.Stages.Window
	return w != nil && w.EventTime() == nil && w.IdleMs() > 0 && r.stream.Stages.Reducer != nil
}

// setDue tracks (or forgets, atMs nil) when the partition's earliest open
// window closes.
func (r *Runner) setDue(pid, pname string, atMs *int64) {
	if pid == "" || pid == "unknown" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if atMs == nil {
		delete(r.due, pid)
		return
	}
	r.due[pid] = dueEntry{name: pname, atMs: *atMs}
}

// retryDue moves a tracked partition's due time to atMs at the earliest. A
// partition no longer tracked stays untracked: a cycle has closed what it had.
func (r *Runner) retryDue(pid string, atMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.due[pid]; ok && d.atMs < atMs {
		d.atMs = atMs
		r.due[pid] = d
	}
}

// claimsMissing reports whether err says the broker has no such route: a
// broker before 2.7, so no claims either.
func (r *Runner) claimsMissing(err error) bool {
	he, ok := err.(*HTTPError)
	if !ok || he.Status != 404 {
		return false
	}
	r.claims.Store(claimsNo)
	// Nothing left that would never be closed.
	r.mu.Lock()
	r.due = map[string]dueEntry{}
	r.mu.Unlock()
	r.logger.Info("broker has no stream claims; idle flush of recent partitions", nil)
	return true
}

// sweep tracks, as due now, the partitions holding the query's state that
// nobody touched for untouchedMs (0: all of them) and this runner does not
// track yet, so the next ticks claim them a batch at a time.
func (r *Runner) sweep(loopCtx, ctx context.Context, untouchedMs int64) {
	after := ""
	for {
		if r.isStopped(loopCtx) {
			return
		}
		body := map[string]interface{}{
			"query_id": r.serverQueryID,
			"limit":    sweepPage,
		}
		if untouchedMs > 0 {
			body["untouched_for_ms"] = untouchedMs
		}
		if after != "" {
			body["after"] = after
		}
		res, err := r.http.Post(ctx, "/streams/v1/state/partitions", body)
		if err != nil {
			if !r.claimsMissing(err) {
				r.reportError(err, "stream sweep")
			}
			return
		}
		if ok, isBool := res["success"].(bool); isBool && !ok {
			r.reportError(fmt.Errorf("stream sweep refused: %v", res["error"]), "stream sweep")
			return
		}
		now := r.nowMs()
		parts, _ := res["partitions"].([]interface{})
		r.mu.Lock()
		for _, pi := range parts {
			p, _ := pi.(map[string]interface{})
			pid, _ := p["partition_id"].(string)
			if pid == "" {
				continue
			}
			if _, tracked := r.due[pid]; !tracked {
				name, _ := p["partition"].(string)
				r.due[pid] = dueEntry{name: name, atMs: now}
			}
		}
		r.mu.Unlock()
		next, _ := res["next"].(string)
		if next == "" {
			return
		}
		after = next
	}
}

// closeDue claims the partitions whose windows are due (the oldest first, at
// most claimBatch), and closes them.
func (r *Runner) closeDue(loopCtx, ctx context.Context) {
	type ready struct {
		pid  string
		atMs int64
	}
	now := r.nowMs()
	var list []ready
	r.mu.Lock()
	for pid, d := range r.due {
		if d.atMs <= now {
			list = append(list, ready{pid, d.atMs})
		}
	}
	r.mu.Unlock()
	if len(list) == 0 {
		return
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].atMs != list[j].atMs {
			return list[i].atMs < list[j].atMs
		}
		return list[i].pid < list[j].pid
	})
	if len(list) > claimBatch {
		list = list[:claimBatch]
	}
	pids := make([]string, len(list))
	for i, p := range list {
		pids[i] = p.pid
	}

	res, err := r.http.Post(ctx, "/streams/v1/claim", map[string]interface{}{
		"query_id":       r.serverQueryID,
		"partition_ids":  pids,
		"consumer_group": r.consumerGroup,
		"lease_seconds":  claimLeaseS,
	})
	if err != nil {
		if r.claimsMissing(err) {
			return
		}
		r.reportError(err, "stream claim")
		retry := r.nowMs() + claimRetryMs
		for _, pid := range pids {
			r.retryDue(pid, retry)
		}
		return
	}
	r.claims.Store(claimsYes)
	if ok, isBool := res["success"].(bool); isBool && !ok {
		r.reportError(fmt.Errorf("stream claim refused: %v", res["error"]), "stream claim")
		return
	}
	leaseID, _ := res["lease_id"].(string)
	// The broker's clock at the claim: windows close on it.
	clock := r.nowMs()
	if v, ok := res["now_ms"].(float64); ok && v > 0 {
		clock = int64(v)
	}
	busyWait := r.stream.Stages.Window.IdleMs()
	if busyWait < claimRetryMs {
		busyWait = claimRetryMs
	}

	parts, _ := res["partitions"].([]interface{})
	for _, pi := range parts {
		p, _ := pi.(map[string]interface{})
		pid, _ := p["partition_id"].(string)
		name, _ := p["partition"].(string)
		status, _ := p["status"].(string)
		if pid == "" {
			continue
		}
		if name == "" {
			r.mu.Lock()
			name = r.due[pid].name
			r.mu.Unlock()
		}
		switch status {
		case "claimed":
			// Stopping: the claims not closed yet lapse with their lease,
			// nothing is lost.
			if r.isStopped(loopCtx) {
				continue
			}
			if err := r.closeClaimed(ctx, pid, name, leaseID, clock); err != nil {
				r.reportError(fmt.Errorf("partition %s: %w; its claim lapses", pid, err), "stream close")
				r.retryDue(pid, r.nowMs()+claimRetryMs)
			}
		case "busy":
			r.retryDue(pid, r.nowMs()+busyWait)
		default:
			// backlog: messages wait, and the cycle that takes them tracks
			// the partition again. missing: nothing to close there.
			r.setDue(pid, name, nil)
		}
	}
}

// closeClaimed closes what is ripe on a claimed partition at clock (the
// broker's time at the claim), in a cycle that acks the claim's empty batch,
// and tracks when the windows left open close.
func (r *Runner) closeClaimed(ctx context.Context, pid, pname, leaseID string, clock int64) error {
	stages := r.stream.Stages
	l := r.partitionLock(pid)
	l.Lock()
	defer l.Unlock()

	// Every row: the windows left open say when to come back.
	loaded, err := GetState(ctx, r.http, r.serverQueryID, pid, nil)
	if err != nil {
		return err
	}
	var stateOps []operators.StateOp
	var emits []operators.ReduceEmit
	var nextClose *int64
	if w, ok := stages.Window.(*operators.WindowSessionOperator); ok {
		state := map[string]interface{}{}
		for k, v := range loaded {
			if strings.HasPrefix(k, w.OperatorTag()+"\u001fopen\u001f") {
				state[k] = v
			}
		}
		res := w.RunSession(nil, state, stages.Reducer.Fn, reducerInitial(stages.Reducer), clock)
		stateOps, nextClose = res.StateOps, res.NextCloseMs
		for _, e := range res.Emits {
			emits = append(emits, operators.ReduceEmit{
				Key: e.Key, WindowStart: e.WindowStart, WindowEnd: e.WindowEnd,
				WindowKey: e.WindowKey, Value: e.Value,
			})
		}
	} else {
		res := stages.Reducer.Run(nil, loaded, clock, stages.Window.OperatorTag(), stages.Window.GraceMs())
		stateOps, emits, nextClose = res.StateOps, res.Emits, res.NextCloseMs
	}
	closed := len(emits) > 0
	emits, err = r.applyPostStages(ctx, stages.Post, emits, pid, pname, nil)
	if err != nil {
		return err
	}
	pushItems, err := r.buildSink(ctx, stages.Sink, emits, pid, pname)
	if err != nil {
		return err
	}
	// The empty batch's ack: the cursor stays and the lease goes, also when
	// nothing was ripe (another runner closed it first).
	ack := map[string]interface{}{
		"transactionId": "",
		"leaseId":       leaseID,
		"status":        "completed",
		"count":         0,
	}
	if _, err := CommitCycle(ctx, r.http, r.serverQueryID, pid, r.consumerGroup, stateOps, pushItems, ack, true); err != nil {
		return err
	}
	r.mu.Lock()
	if closed {
		r.stats.FlushCyclesTotal++
	}
	r.stats.StateOpsTotal += int64(len(stateOps))
	r.stats.PushItemsTotal += int64(len(pushItems))
	r.mu.Unlock()
	// Under the partition lock, like the cycle's own setDue.
	r.setDue(pid, pname, nextClose)
	return nil
}

func (r *Runner) flushTick(ctx context.Context) {
	w := r.stream.Stages.Window
	if w == nil {
		return
	}
	for pid, pname := range r.partitionsToFlush() {
		// Mutual exclusion with the pop/cycle loop (see partLocks).
		l := r.partitionLock(pid)
		l.Lock()
		_ = r.flushPartition(ctx, pid, pname)
		l.Unlock()
	}
}

func (r *Runner) flushPartition(ctx context.Context, pid, pname string) error {
	w := r.stream.Stages.Window
	stages := r.stream.Stages

	var clock int64
	if w.EventTime() != nil {
		clock, _ = r.getWatermark(pid)
	} else {
		clock = r.nowMs()
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
	res := w.RunSession([]operators.Envelope{}, state, reducer.Fn, reducerInitial(reducer), clock)
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

// reducerInitial gives a session a fresh copy of the reducer's initial value,
// so no two sessions share one accumulator.
func reducerInitial(reducer *operators.ReduceOperator) func() interface{} {
	return func() interface{} {
		if reducer.Initial == nil {
			return float64(0)
		}
		b, _ := json.Marshal(reducer.Initial)
		var v interface{}
		_ = json.Unmarshal(b, &v)
		return v
	}
}

// newestCreatedAt is the latest createdAt of a batch, in ms. false: none of
// them parses.
func newestCreatedAt(messages []Message) (int64, bool) {
	var newest int64
	found := false
	for _, m := range messages {
		if t, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err == nil {
			if ms := t.UnixMilli(); !found || ms > newest {
				newest, found = ms, true
			}
		}
	}
	return newest, found
}

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
