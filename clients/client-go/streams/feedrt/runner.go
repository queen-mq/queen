package feedrt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// PositionNamespace is the KV namespace of positions, layouts and snapshots.
const PositionNamespace = "queen-feed"

const (
	positionCommitMillis = 5000
	commitEveryMillis    = 1000
	maxPendingOutputs    = 5000
	maxBackoffMillis     = 10000
	defaultLockTTL       = 15 * time.Second
	defaultRetries       = 3
	retryDelayMillis     = 100
	snapshotEveryMillis  = 60000
	snapshotMaxEntries   = 200000
	// Skips remembered since the snapshot, for a replay to leave out the same
	// ones; past this many, a snapshot is due.
	maxSkips = 1000
	// A replay waits this long, at most, for a follower to apply the committed position.
	replayCatchUpMillis = 60000
	// A quiet feed with a window or deadline due: at most one clock-moving
	// write this often, and a short read after it.
	tickMinMillis      = 500
	tickReadWaitMillis = 100
	// The broker's clock moves only with writes. When none came for this
	// long and a deadline waits, one write tells this process the broker's time.
	clockSyncMillis = 5000
	queryIDMaxBytes = 200
	// The error a stopped record keeps, at most.
	stopErrorMaxChars = 500
	defaultWaitMilli  = 5000
)

// OutputPrefix is the prefix of every transaction id a query writes, every
// shard of it: its reads leave these out.
func OutputPrefix(queryID string) string {
	return "fd-" + sha256Hex("queen-feed\n" + queryID)[:8] + "-"
}

// FeedOutputID is the transaction id of the i-th output of one feed entry
// (the JS shape: position, queue, partition, offset): the same on every
// retry. scope (a shard's id) makes the rest its own.
func FeedOutputID(queryID string, entry map[string]interface{}, i int, epoch int, scope string) string {
	h := sha256Hex(scope + "\n" + fmt.Sprint(epoch) + "\n" + jsv.String(jsv.Get(entry, "position")) + "\n" +
		jsv.String(jsv.Get(entry, "queue")) + "\n" + jsv.String(jsv.Get(entry, "partition")) + "\n" +
		jsv.String(jsv.Get(entry, "offset")) + "\n" + fmt.Sprint(i))
	return OutputPrefix(queryID) + h[:40]
}

// ShardSpec is one shard of a stream: {index, count}.
type ShardSpec struct {
	Index int `json:"index"`
	Count int `json:"count"`
}

// ShardID is the KV key and lock name of one shard of a query (or of the query, unsharded).
func ShardID(queryID string, shard *ShardSpec) string {
	if shard == nil {
		return queryID
	}
	return fmt.Sprintf("%s#shard%dof%d", queryID, shard.Index, shard.Count)
}

func lockName(queryID string, shard *ShardSpec) string {
	if shard == nil {
		return "queen-feed/" + queryID
	}
	return fmt.Sprintf("queen-feed/%s/shard-%d-of-%d", queryID, shard.Index, shard.Count)
}

var queryIDBad = regexp.MustCompile(`[\x00-\x1f\x7f#]`)

func requireQueryID(queryID string) error {
	if queryID == "" || len(queryID) > queryIDMaxBytes || queryIDBad.MatchString(queryID) {
		return fmt.Errorf("Stream.fromFeed: queryId is a non-empty string of at most %d bytes, without '#' "+
			"and control characters (it names a KV key and a lock), got %s", queryIDMaxBytes, jsv.MustMarshal(queryID))
	}
	return nil
}

func requireShard(shard *ShardSpec) (*ShardSpec, error) {
	if shard == nil {
		return nil, nil
	}
	if shard.Count < 1 || shard.Count > 4096 || shard.Index < 0 || shard.Index >= shard.Count {
		return nil, fmt.Errorf("Stream.fromFeed: shard is { index, count }, count from 1 to 4096 and index from 0 to count - 1, got {\"index\":%d,\"count\":%d}", shard.Index, shard.Count)
	}
	if shard.Count == 1 {
		return nil, nil
	}
	return &ShardSpec{Index: shard.Index, Count: shard.Count}, nil
}

// FeedError is an error the runner raises with a code (feed_poison,
// feed_layout_changed, feed_chain_changed, feed_snapshot_moved, ...).
type FeedError struct {
	Msg    string
	Code   string
	Fatal  bool
	Fields map[string]interface{}
	Cause  error
}

func (e *FeedError) Error() string { return e.Msg }
func (e *FeedError) Unwrap() error { return e.Cause }

// ErrorCode answers the code of an error the feed raises ("" when none).
func ErrorCode(err error) string {
	var fe *FeedError
	if errors.As(err, &fe) {
		return fe.Code
	}
	var se *SnapshotError
	if errors.As(err, &se) {
		return se.Code
	}
	var nf *feed.NoFeedRouteError
	if errors.As(err, &nf) {
		return nf.Code()
	}
	var vt *ViewTimeoutError
	if errors.As(err, &vt) {
		return vt.Code()
	}
	if errors.Is(err, ErrLockNotHeld) {
		return "LOCK_NOT_HELD"
	}
	return ""
}

func isFatal(err error) bool {
	var fe *FeedError
	if errors.As(err, &fe) && fe.Fatal {
		return true
	}
	var se *SnapshotError
	return errors.As(err, &se) && se.Fatal
}

var errStopping = errors.New("stopping")

// RunOptions are run()'s options for a feed stream, with the JS names and
// defaults. The pointers are for options whose zero is a value of its own.
type RunOptions struct {
	QueryID string
	// From: where a NEW query starts ("now", "earliest" or a position); nil: the source's.
	From       interface{}
	WaitMillis *int // 5000
	MaxRecords int
	MaxBytes   int
	// Reset starts over at every start; ResetToken starts over once per token
	// (the first load that finds the stored record without it, a takeover too).
	Reset      bool
	ResetToken string
	// OnError hears every error (ctx: queryId, shard, phase, position, action, attempts, ...).
	OnError func(err error, ctx map[string]interface{})
	// Context stops the runner when done (JS abortSignal).
	Context              context.Context
	PositionCommitMillis *int  // 5000
	CommitEveryMillis    *int  // 1000
	MaxPendingOutputs    *int  // 5000
	Exclusive            *bool // true
	LockTTL              time.Duration
	// OnPoison: "stop" (the default), "skip", or a queue name the failing messages go to.
	OnPoison            string
	Retries             *int // 3
	RetryDelayMillis    *int // 100
	SnapshotEveryMillis *int // 60000
	SnapshotMaxEntries  *int // 200000
	SnapshotChunkChars  int  // 48000
	SnapshotPieceBytes  int  // 262144
	Shard               *ShardSpec
	// ShardBy: nil = by partition; else the shard key of a message.
	ShardBy         func(msg interface{}) (interface{}, error)
	ServeState      *bool // true
	ServeWaitMillis int
	// Now is the wall clock (tests move it).
	Now func() time.Time
	// Shards runs the stream in that many shards in this process (streams.RunFeed; 0 or 1: one runner).
	Shards int
}

func intOr(p *int, d int) int {
	if p == nil {
		return d
	}
	return *p
}

type resolved struct {
	from                 interface{}
	waitMillis           int
	maxRecords, maxBytes int
	reset                interface{} // false | true | string
	onError              func(error, map[string]interface{})
	positionCommitMillis float64
	commitEveryMillis    float64
	maxPendingOutputs    int
	exclusive            bool
	lockTTL              time.Duration
	retries              int
	retryDelayMillis     float64
	snapshotEveryMillis  float64
	snapshotMaxEntries   int
	snapshotChunkChars   int
	snapshotPieceBytes   int
	serveState           bool
	serveWaitMillis      int
}

type poisonTarget struct {
	kind  string // stop | skip | deadLetter
	queue string
}

func poisonTargetOf(onPoison string) poisonTarget {
	switch onPoison {
	case "", "stop":
		return poisonTarget{kind: "stop"}
	case "skip":
		return poisonTarget{kind: "skip"}
	}
	return poisonTarget{kind: "deadLetter", queue: onPoison}
}

type output struct {
	queue, partition string
	data             interface{}
	transactionID    string
}

type pendingT struct {
	outputs []output
	next    int64
	hasNext bool
	work    int
	effects int
	emitSeq int
	// readTo / caughtUp: the feed time next is at, and whether the read was caught up there (nil: null).
	readTo   *float64
	caughtUp *bool
}

type poisonStopT struct {
	where    string
	what     jsv.Ordered
	attempts int
	err      error
	reason   string
}

type checkpointT struct {
	state   []*stepCP
	outputs int
	work    int
	effects int
	emitSeq int
	clock   float64
	units   int
	late    int
	skips   int
	// A unit tried again counts its results once.
	emitted int64
}

type snapResult struct {
	pointer  map[string]interface{}
	wrote    bool
	moved    bool
	full     bool
	replaced []interface{}
	keys     []string
	millis   int64
}

type subject struct {
	entry map[string]interface{}
	emit  *emit
}

type counter struct{ n int }

// Runner runs a Stream whose source is the tenant feed (FeedRunner.js).
type Runner struct {
	mu sync.Mutex

	stream  *Compiled
	broker  Broker
	queryID string
	id      string
	shard   *ShardSpec
	shardBy func(msg interface{}) (interface{}, error)
	opts    resolved
	nowFn   func() time.Time
	poison  poisonTarget

	reader   *feed.Reader
	pipeline *Pipeline
	sink     *SinkSpec
	hash     string

	brokerQueues     []string
	brokerPartitions []string
	brokerWhere      []feed.Condition
	brokerShard      *feed.Shard
	localWhere       []feed.Condition
	localPartitions  map[string]bool
	streamQueues     []*regexp.Regexp
	localShard       bool
	tableQueues      []string

	lock     Lock
	lockLost atomic.Bool
	active   bool

	position       interface{} // int64 | "now" | "earliest"
	version        int64
	epoch          int
	resetToken     *string
	committedClock float64
	units          int
	committedUnits int
	replayCap      int // -1: no cap
	burstTarget    *float64

	snap             map[string]interface{}
	entriesSinceSnap int
	snapWallAt       float64
	oldKeys          []string
	hadStray         bool

	skips        []interface{}
	pendingSkips []interface{}
	skipSet      map[string]bool

	layoutVersion int64
	initialSnap   bool

	pending    pendingT
	clock      float64
	replaying  bool
	poisonStop *poisonStopT

	lastCommitAt       float64
	lastPositionCommit float64
	// The feed time the committed position is at, and whether it was read caught up.
	committedReadTo   float64
	committedCaughtUp bool
	// Why the stream stops, for the record of its last commit: {reason, at, ...}.
	stopMark jsv.Ordered
	// Stop or the Context asked for the stop.
	stopAsked atomic.Bool
	// What the stream is (about.go), made once; the first commit of each activation writes it.
	about      interface{}
	aboutMade  bool
	aboutOwed  bool
	lastTickAt float64
	resolvedMs float64
	resolvedAt float64
	offset     float64
	lastNow    float64
	nextWait   *int

	stopping   atomic.Bool
	stopCtx    context.Context
	stopCancel context.CancelFunc
	// loopDone is closed when the loop ends; nil until Start started it. An
	// atomic, so that Done() never waits for the runner.
	loopDone atomic.Pointer[chan struct{}]
	started  bool
	lastSaid map[string]time.Time
	server   *stateServer

	m metricsT
	// pub is what Metrics() answers: the metrics as of the last unit (at most
	// a millisecond old while units run), network wait, sleep, role change or
	// stop. It has its own small lock, never r.mu: Metrics() can be called
	// from anywhere, from inside a stream function too. pubAt (r.mu) is when.
	pubMu sync.Mutex
	pub   Metrics
	pubAt time.Time
}

type metricsT struct {
	role                                                         string
	cycles, entriesRead, emitted, outputs, effects, commits      int64
	conflicts, errors, retries, poisoned, skipped, deadLettered  int64
	late, ticks, takeovers, snapshots, snapshotBytes             int64
	snapshotChunks, snapshotsFull, snapshotLayers, snapshotMilli int64
	closeCommits, replays, replayEntries, replayMillis           int64
	replaySkipped, tableRows                                     int64
	position                                                     interface{}
	applied                                                      *int64
	lag                                                          *int64
	stopped                                                      map[string]interface{}
}

// NewRunner checks the options of a runner of one compiled stream.
func NewRunner(b Broker, stream *Compiled, o RunOptions) (*Runner, error) {
	if err := requireQueryID(o.QueryID); err != nil {
		return nil, err
	}
	shard, err := requireShard(o.Shard)
	if err != nil {
		return nil, err
	}
	retries := intOr(o.Retries, defaultRetries)
	if retries < 0 {
		return nil, errors.New("Stream.fromFeed: retries is a whole number from 0")
	}
	var reset interface{} = o.Reset
	if o.ResetToken != "" {
		reset = o.ResetToken
	}
	lockTTL := o.LockTTL
	if lockTTL <= 0 {
		lockTTL = defaultLockTTL
	}
	wait := intOr(o.WaitMillis, defaultWaitMilli)
	exclusive := true
	if o.Exclusive != nil {
		exclusive = *o.Exclusive
	}
	serve := true
	if o.ServeState != nil {
		serve = *o.ServeState
	}
	chunk := o.SnapshotChunkChars
	if chunk == 0 {
		chunk = DefaultChunkChars
	}
	pieceB := o.SnapshotPieceBytes
	if pieceB == 0 {
		pieceB = DefaultPieceBytes
	}
	r := &Runner{
		stream: stream, broker: b, queryID: o.QueryID, shard: shard, id: ShardID(o.QueryID, shard),
		shardBy: o.ShardBy, poison: poisonTargetOf(o.OnPoison), nowFn: o.Now,
		opts: resolved{
			from: o.From, waitMillis: wait, maxRecords: o.MaxRecords, maxBytes: o.MaxBytes, reset: reset,
			onError: o.OnError, positionCommitMillis: float64(intOr(o.PositionCommitMillis, positionCommitMillis)),
			commitEveryMillis: float64(intOr(o.CommitEveryMillis, commitEveryMillis)),
			maxPendingOutputs: intOr(o.MaxPendingOutputs, maxPendingOutputs), exclusive: exclusive, lockTTL: lockTTL,
			retries: retries, retryDelayMillis: float64(intOr(o.RetryDelayMillis, retryDelayMillis)),
			snapshotEveryMillis: float64(intOr(o.SnapshotEveryMillis, snapshotEveryMillis)),
			snapshotMaxEntries:  intOr(o.SnapshotMaxEntries, snapshotMaxEntries),
			snapshotChunkChars:  chunk, snapshotPieceBytes: pieceB, serveState: serve, serveWaitMillis: o.ServeWaitMillis,
		},
		replayCap: -1, offset: math.Inf(-1), skipSet: map[string]bool{}, lastSaid: map[string]time.Time{},
		m: metricsT{role: "starting"},
	}
	if r.nowFn == nil {
		r.nowFn = time.Now
	}
	r.stopCtx, r.stopCancel = context.WithCancel(context.Background())
	if o.Context != nil {
		ctx := o.Context
		go func() {
			select {
			case <-ctx.Done():
				r.stopAsked.Store(true)
				r.stopping.Store(true)
				r.stopCancel()
			case <-r.stopCtx.Done():
			}
		}()
	}
	r.publish()
	return r, nil
}

func (r *Runner) now() float64 { return float64(r.nowFn().UnixMilli()) }

// Shard is this runner's shard, or nil.
func (r *Runner) Shard() *ShardSpec { return r.shard }

// unlocked runs fn without the runner's lock held: a network wait or a
// sleep. What Metrics() answers is refreshed first.
func (r *Runner) unlocked(fn func()) {
	r.publish()
	r.mu.Unlock()
	defer r.mu.Lock()
	fn()
}

// Start checks the chain, takes the lock if it is free, loads the position
// and state, and starts the cycles.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.publish()
	if r.stream.Sink == nil {
		return errors.New("Stream.fromFeed: end the chain with .to(queen.queue(...)) or .foreach(fn)")
	}
	r.sink = r.stream.Sink
	p, err := NewPipeline(r.stream.Steps)
	if err != nil {
		return err
	}
	r.pipeline = p
	r.hash = r.stream.ChainHash
	r.planReads()
	if r.reader, err = r.makeReader("now", nil, false); err != nil {
		return err
	}
	// A broker without the feed (or a feed nobody can serve) fails Start here,
	// with the reason, instead of a stream that silently never runs.
	if _, err := r.reader.Probe(ctx); err != nil {
		return err
	}
	if err := r.checkLayout(ctx); err != nil {
		return err
	}
	if r.opts.exclusive {
		r.lock = r.broker.Lock(lockName(r.queryID, r.shard), r.opts.lockTTL, 250*time.Millisecond, 2*time.Second)
		r.lock.OnLost(func(why string) {
			r.lockLost.Store(true)
			r.mu.Lock()
			// It can commit nothing more: a standby from now on.
			if r.active && !r.stopping.Load() {
				r.setRole("standby")
			}
			srv := r.server
			r.mu.Unlock()
			if srv != nil {
				srv.stop()
			}
			logWarn("FeedRunner.lockLost", map[string]interface{}{"queryId": r.id, "why": why})
		})
	}
	// Active now when the lock is free, so that a chain the stored state was
	// not written by fails Start itself; otherwise a standby, waiting.
	got := true
	if r.lock != nil {
		if got, err = r.lock.TryAcquire(ctx); err != nil {
			return err
		}
	}
	if got {
		if err := r.becomeActive(ctx, true); err != nil {
			if r.lock != nil {
				_ = r.lock.Release(context.Background())
			}
			return err
		}
	} else {
		r.setRole("standby")
	}
	logInfo("FeedRunner.start", map[string]interface{}{"queryId": r.id, "role": r.m.role})
	done := make(chan struct{})
	r.loopDone.Store(&done)
	r.started = true
	go r.run()
	return nil
}

// Stop stops after the cycle in hand, commits what is pending (and a
// snapshot), gives the lock back; returns once done. It waits for the
// runner's loop: never call it from inside a stream function (use the run's
// Context, or call it from another goroutine).
func (r *Runner) Stop(ctx context.Context) error {
	r.stopAsked.Store(true)
	r.stopping.Store(true)
	r.stopCancel()
	if done := r.loopDone.Load(); done != nil {
		select {
		case <-*done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	reader, srv := r.reader, r.server
	r.mu.Unlock()
	if reader != nil {
		reader.Close()
	}
	if srv != nil {
		srv.stop()
	}
	return nil
}

// Done is closed when the runner has stopped: by Stop, its Context, a poison
// message, or a broker without the feed. It never waits for the runner.
func (r *Runner) Done() <-chan struct{} {
	if done := r.loopDone.Load(); done != nil {
		return *done
	}
	c := make(chan struct{})
	close(c)
	return c
}

// Metrics is a snapshot of the runner's metrics: as of its last unit (at most
// a millisecond old while it works), network wait, role change or stop. It
// never waits for the runner, so it can be called from anywhere, from inside
// a stream function too (a foreach that logs progress).
func (r *Runner) Metrics() Metrics {
	r.pubMu.Lock()
	m := r.pub
	r.pubMu.Unlock()
	return m.clone()
}

// publish refreshes what Metrics() answers (r.mu held, or no loop yet).
func (r *Runner) publish() {
	m := r.metricsLocked()
	r.pubAt = time.Now()
	r.pubMu.Lock()
	r.pub = m
	r.pubMu.Unlock()
}

// publishSoon is publish at the end of a unit, at most once a millisecond
// (wall clock: a test's clock may stand still).
func (r *Runner) publishSoon() {
	if time.Since(r.pubAt) >= time.Millisecond {
		r.publish()
	}
}

// setRole changes the role, and what Metrics() answers with it (r.mu held).
func (r *Runner) setRole(role string) {
	r.m.role = role
	r.publish()
}

func (r *Runner) metricsLocked() Metrics {
	m := r.m
	out := Metrics{
		Role: m.role, Cycles: m.cycles, EntriesRead: m.entriesRead, Emitted: m.emitted, Outputs: m.outputs,
		Effects: m.effects, Commits: m.commits, Conflicts: m.conflicts, Errors: m.errors, Retries: m.retries,
		Poisoned: m.poisoned, Skipped: m.skipped, DeadLettered: m.deadLettered, Late: m.late, Ticks: m.ticks,
		Takeovers: m.takeovers, Snapshots: m.snapshots, SnapshotBytes: m.snapshotBytes, SnapshotChunks: m.snapshotChunks,
		SnapshotsFull: m.snapshotsFull, SnapshotLayers: m.snapshotLayers, SnapshotMillis: m.snapshotMilli,
		CloseCommits: m.closeCommits, Replays: m.replays, ReplayEntries: m.replayEntries, ReplayMillis: m.replayMillis,
		ReplaySkipped: m.replaySkipped, TableRows: m.tableRows, Position: m.position, Applied: m.applied, Lag: m.lag,
		Stopped: m.stopped, PendingOutputs: int64(len(r.pending.outputs)),
	}
	if r.shard != nil {
		s := *r.shard
		out.Shard = &s
	}
	if r.pipeline != nil && r.pipeline.HasState() {
		out.HasState = true
		out.Open = int64(r.pipeline.Open())
		out.StateKeys = int64(r.pipeline.StateKeys())
		out.Steps = r.pipeline.StepMetrics(r.clock)
		out.Late = int64(r.pipeline.late)
		if r.clock > 0 {
			c := jsv.MustISO(r.clock)
			out.Clock = &c
		}
		if w, ok := r.pipeline.WatermarkAt(r.clock); ok {
			s := jsv.MustISO(w)
			out.Watermark = &s
		}
		if r.snap != nil {
			out.SnapshotPosition = r.snap["position"]
		}
		out.Units = int64(r.units)
	}
	return out
}

// stateReader reads one step's live state in this process.
type stateReader struct {
	r    *Runner
	step string
}

// State is the live state of one step: Get(key) and List(limit). They read
// the state while the runner waits on the network, so they wait for that:
// never call them from inside a stream function, which would wait for itself.
func (r *Runner) State(step string) StepState { return stateReader{r: r, step: step} }

// Get is {found, value} or {found: false, error}.
func (s stateReader) Get(key string) map[string]interface{} {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	if s.r.pipeline == nil {
		return map[string]interface{}{"found": false}
	}
	return s.r.pipeline.Inspect(s.step, key)
}

// List is {items, truncated, total} (limit 0: 100).
func (s stateReader) List(limit int) map[string]interface{} {
	if limit <= 0 {
		limit = 100
	}
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	if s.r.pipeline == nil {
		return map[string]interface{}{"items": []interface{}{}, "truncated": false, "total": 0.0}
	}
	return s.r.pipeline.List(s.step, limit)
}

// ---- reads --------------------------------------------------------------------

func glob(pattern string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String())
}

func hasGlob(s string) bool { return strings.ContainsAny(s, "*?") }

func (r *Runner) hasTableQueue(q string) bool {
	for _, t := range r.tableQueues {
		if t == q {
			return true
		}
	}
	return false
}

// planReads: what the broker filters and what this process filters.
func (r *Runner) planReads() {
	src := r.stream.Source
	queues := listOrNil(src.Queues)
	partitions := listOrNil(src.Partitions)
	tables := r.pipeline.tables()
	for _, t := range tables {
		for _, q := range t.op.Queues {
			if !r.hasTableQueue(q) {
				r.tableQueues = append(r.tableQueues, q)
			}
		}
	}
	// Never read back what this query wrote: its outputs carry its prefix.
	self := feed.Condition{Field: "transactionId", Op: "!prefix", Value: OutputPrefix(r.queryID)}
	where := r.stream.Pushdown
	if len(tables) == 0 {
		r.brokerQueues = queues
		r.brokerPartitions = partitions
		r.brokerWhere = append(append([]feed.Condition(nil), where...), self)
		if r.shard != nil && r.shardBy == nil {
			r.brokerShard = &feed.Shard{Count: r.shard.Count, Index: r.shard.Index}
		}
	} else {
		// A table queue is read whole: its rows pass no filter of the stream's
		// and no shard, so those filters move here, for the stream's messages.
		if queues != nil {
			seen := map[string]bool{}
			for _, q := range append(append([]string(nil), queues...), r.tableQueues...) {
				if !seen[q] {
					seen[q] = true
					r.brokerQueues = append(r.brokerQueues, q)
				}
			}
		}
		r.brokerWhere = []feed.Condition{self}
		r.localWhere = where
		if partitions != nil {
			r.localPartitions = map[string]bool{}
			for _, p := range partitions {
				r.localPartitions[p] = true
			}
		}
		if queues != nil {
			for _, q := range queues {
				r.streamQueues = append(r.streamQueues, glob(q))
			}
		}
		if r.shard != nil && r.shardBy == nil {
			// The broker splits only the stream's own queues: never a table
			// queue. A pattern that also matches a table queue cannot say
			// that to the broker: then the split happens here.
			var split []string
			if queues != nil {
				for _, q := range queues {
					if !r.hasTableQueue(q) {
						split = append(split, q)
					}
				}
			}
			hitsTable := false
			for _, pat := range split {
				if hasGlob(pat) {
					re := glob(pat)
					for _, t := range r.tableQueues {
						if re.MatchString(t) {
							hitsTable = true
						}
					}
				}
			}
			if len(split) > 0 && !hitsTable {
				r.brokerShard = &feed.Shard{Count: r.shard.Count, Index: r.shard.Index, Queues: split}
			} else {
				r.localShard = true
			}
		}
	}
	if r.shard != nil && r.shardBy != nil {
		r.localShard = true
	}
}

func listOrNil(v []string) []string {
	if len(v) == 0 {
		return nil
	}
	return v
}

// route: a table's rows, the stream, or nowhere (another shard's, filtered out here).
func (r *Runner) route(entry map[string]interface{}) string {
	q, _ := entry["queue"].(string)
	if r.hasTableQueue(q) {
		return "table"
	}
	if r.streamQueues != nil {
		match := false
		for _, re := range r.streamQueues {
			if re.MatchString(q) {
				match = true
				break
			}
		}
		if !match {
			return "skip"
		}
	}
	part, _ := entry["partition"].(string)
	if r.localPartitions != nil && !r.localPartitions[part] {
		return "skip"
	}
	for _, c := range r.localWhere {
		if !feed.ConditionHolds(c, entry) {
			return "skip"
		}
	}
	if r.localShard && r.shardBy == nil && ShardOf(part, r.shard.Count) != r.shard.Index {
		return "skip"
	}
	return "stream"
}

// owns: whether this shard takes a stream message, by shardBy; run inside the
// message's unit, so a function that fails meets the poison policy -- in
// shard 0 only, which takes such a message; the other shards leave it.
func (r *Runner) owns(entry map[string]interface{}) (bool, error) {
	if !r.localShard || r.shardBy == nil {
		return true, nil
	}
	key, err := callValue(func() (interface{}, error) { return r.shardBy(entry) })
	if err != nil {
		if r.shard.Index == 0 {
			return false, err
		}
		return false, nil
	}
	return ShardOf(jsv.String(key), r.shard.Count) == r.shard.Index, nil
}

func (r *Runner) makeReader(from interface{}, since interface{}, tablesOnly bool) (*feed.Reader, error) {
	src := r.stream.Source
	maxRecords := r.opts.maxRecords
	if maxRecords == 0 {
		maxRecords = src.MaxRecords
	}
	maxBytes := r.opts.maxBytes
	if maxBytes == 0 {
		maxBytes = src.MaxBytes
	}
	if tablesOnly {
		zero := 0
		return r.broker.NewReader(feed.Options{
			Queues: append([]string(nil), r.tableQueues...), FollowerURLs: src.FollowerURLs,
			Where: []feed.Condition{{Field: "transactionId", Op: "!prefix", Value: OutputPrefix(r.queryID)}},
			From:  from, WaitMillis: &zero, MaxRecords: maxRecords, MaxBytes: maxBytes,
		})
	}
	wait := r.opts.waitMillis
	return r.broker.NewReader(feed.Options{
		Queues: r.brokerQueues, Partitions: r.brokerPartitions, Kinds: src.Kinds, Since: since,
		FollowerURLs: src.FollowerURLs, Where: r.brokerWhere, Shard: r.brokerShard, From: from,
		WaitMillis: &wait, MaxRecords: maxRecords, MaxBytes: maxBytes,
	})
}

func versionOf(res TxResult, key string) (int64, bool) {
	for _, k := range res.KV {
		if k.Key == key && k.Version != nil {
			return *k.Version, true
		}
	}
	return 0, false
}

// checkLayout: the shard count (and how shards split) is part of the stream,
// kept in queen-feed / <queryId>#layout. Another one is refused: keys would
// move between shards with their state. Every commit checks its version.
func (r *Runner) checkLayout(ctx context.Context) error {
	ns := PositionNamespace
	key := r.queryID + "#layout"
	count := 1
	if r.shard != nil {
		count = r.shard.Count
	}
	by := "partition"
	if r.shardBy != nil {
		by = "function"
	}
	reset := r.opts.reset
	row, err := r.broker.KVGet(ctx, ns, key)
	if err != nil {
		return err
	}
	var stored map[string]interface{}
	if row.Found {
		if m, ok := row.Value.(map[string]interface{}); ok && jsv.IsInteger(m["shards"]) {
			stored = m
		}
	}
	tok, isTok := reset.(string)
	resetting := reset == true || (isTok && (stored == nil || stored["resetToken"] != tok))
	storedShards := 0
	storedBy := "partition"
	if stored != nil {
		f, _ := jsv.Num(stored["shards"])
		storedShards = int(f)
		if s, ok := stored["shardBy"].(string); ok {
			storedBy = s
		}
	}
	if stored != nil && storedShards == count && (count == 1 || storedBy == by) {
		r.layoutVersion = row.Version
		return nil
	}
	if stored == nil && count == 1 {
		r.layoutVersion = 0
		return nil
	}
	if stored == nil {
		// First sharded run: refuse when the query ran whole and kept a position.
		whole, err := r.broker.KVGet(ctx, ns, r.queryID)
		if err != nil {
			return err
		}
		if whole.Found && !resetting {
			return &FeedError{Code: "feed_layout_changed", Msg: fmt.Sprintf("%s ran as one stream (its position is in KV); running it in %d shards would move keys "+
				"between shards with their state: run it with { reset: true } (or a reset string) to start the shards over", r.queryID, count)}
		}
	} else if !resetting {
		plural := "s"
		if storedShards == 1 {
			plural = ""
		}
		return &FeedError{Code: "feed_layout_changed", Msg: fmt.Sprintf("%s runs in %d shard%s split by %s; "+
			"running it in %d split by %s would move keys between shards with their state: run it as before, "+
			"or with { reset: true } (or a reset string) to start over", r.queryID, storedShards, plural, storedBy, count, by)}
	}
	value := map[string]interface{}{"shards": float64(count), "shardBy": by, "updatedAt": jsv.ISOOf(time.Now())}
	if isTok {
		value["resetToken"] = tok
	}
	tx := r.broker.Transaction()
	if count == 1 {
		tx.KVDelete(ns, key)
	} else if row.Found {
		v := row.Version
		tx.KVPut(ns, key, value, &v, true, true)
	} else {
		tx.KVPutIfAbsent(ns, key, value, true, true)
	}
	res, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	if !res.Success {
		// Another process wrote it first: read it again, and judge that one.
		if res.Reason == ReasonKVPrecondition {
			return r.checkLayout(ctx)
		}
		reason := res.Reason
		if reason == "" {
			reason = "unknown"
		}
		return fmt.Errorf("FeedRunner: the shard layout could not be written (%s)", reason)
	}
	if count == 1 {
		r.layoutVersion = 0
	} else if v, ok := versionOf(res, key); ok {
		r.layoutVersion = v
	} else {
		again, err := r.broker.KVGet(ctx, ns, key)
		if err != nil {
			return err
		}
		r.layoutVersion = again.Version
	}
	return nil
}

// layoutMoved: the layout changed under this process; it stops.
func (r *Runner) layoutMoved(ctx context.Context) error {
	row, err := r.broker.KVGet(ctx, PositionNamespace, r.queryID+"#layout")
	if err != nil {
		return err
	}
	v := int64(0)
	if row.Found {
		v = row.Version
	}
	if v == r.layoutVersion {
		return nil
	}
	now := "one stream"
	if row.Found {
		now = jsv.String(jsv.Get(row.Value, "shards")) + " shards"
	}
	return &FeedError{Fatal: true, Code: "feed_layout_changed", Msg: fmt.Sprintf("the shard layout of %s changed while this process ran it (%s now): "+
		"another deploy runs it; this process stops", r.queryID, now)}
}

// ---- the loop -------------------------------------------------------------------

func (r *Runner) sleep(ms float64) {
	if ms <= 0 {
		return
	}
	r.unlocked(func() {
		select {
		case <-r.stopCtx.Done():
		case <-time.After(time.Duration(ms * float64(time.Millisecond))):
		}
	})
}

func isAborted(err error) bool {
	return errors.Is(err, context.Canceled)
}

func (r *Runner) run() {
	r.mu.Lock()
	defer func() {
		r.publish()
		done := *r.loopDone.Load()
		r.mu.Unlock()
		close(done)
	}()
	ctx := context.Background()
	backoff := 500.0
	for !r.stopping.Load() {
		err := func() error {
			if !r.active {
				if r.lock != nil && !r.lock.Held() {
					r.setRole("standby")
					var got bool
					var err error
					r.unlocked(func() {
						wctx, cancel := context.WithTimeout(r.stopCtx, time.Hour)
						got, err = r.lock.Acquire(wctx)
						cancel()
					})
					if err != nil {
						return err
					}
					if !got || r.stopping.Load() {
						return nil
					}
					r.m.takeovers++
				}
				if err := r.becomeActive(ctx, false); err != nil {
					return err
				}
				backoff = 500
			}
			if r.lock != nil && (r.lockLost.Load() || !r.lock.Held()) {
				r.standDown("lost")
				return nil
			}
			if err := r.cycle(ctx); err != nil {
				return err
			}
			backoff = 500
			return nil
		}()
		if err == nil {
			continue
		}
		// Stop cancelled the read in flight: what the pages before it did is
		// still pending, and the stop below commits it.
		if r.stopping.Load() && isAborted(err) {
			break
		}
		// Stop came while a message was being retried: leave it for later.
		if errors.Is(err, errStopping) {
			if r.active {
				r.rollback()
			}
			break
		}
		if isFatal(err) {
			r.report(err, map[string]interface{}{"phase": "load", "position": r.positionValue()})
			r.m.stopped = map[string]interface{}{"reason": "config", "error": err.Error()}
			r.stopping.Store(true)
			break
		}
		r.m.errors++
		if r.active {
			r.rollback()
		}
		if errors.Is(err, ErrLockNotHeld) {
			r.standDown("expired")
			continue
		}
		phase := "load"
		if r.active {
			phase = "cycle"
		}
		r.report(err, map[string]interface{}{"phase": phase, "position": r.positionValue()})
		// A broker without the feed will not grow one: stop rather than retry.
		if errors.Is(err, feed.ErrNoFeedRoute) {
			r.stopping.Store(true)
			break
		}
		r.sleep(backoff)
		backoff = math.Min(backoff*2, maxBackoffMillis)
	}
	// Stopping: what is done and not yet committed goes in now, with a
	// snapshot, so the next start has nothing to read again.
	snapshotOwed := r.pipeline.HasState() && r.pipeline.ChangedSinceSnapshot()
	// Asked to stop: the record of this last commit says so (stopped), so that
	// a stream stopped on purpose is not taken for one that died.
	// (Only at a position: a stream stopped before its first read has none to write.)
	_, atPosition := r.positionInt()
	atPosition = atPosition || r.pending.hasNext
	asked := r.stopAsked.Load() && r.m.stopped == nil && atPosition
	if r.active && (r.hasPending() || snapshotOwed || asked) && !(r.lock != nil && !r.lock.Held()) && r.poisonStop == nil {
		if asked {
			r.stopMark = jsv.Ordered{{K: "reason", V: "stop"}, {K: "at", V: jsv.MustISO(r.now())}}
		}
		if _, err := r.commit(context.Background()); err != nil {
			r.report(err, map[string]interface{}{"phase": "stop", "position": r.positionValue()})
		}
		r.stopMark = nil
	}
	r.active = false
	if r.server != nil {
		r.server.stop()
	}
	if r.lock != nil {
		lock := r.lock
		r.unlocked(func() {
			rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = lock.Release(rctx)
			cancel()
		})
	}
	r.setRole("stopped")
}

func (r *Runner) positionValue() interface{} {
	if p, ok := r.position.(int64); ok {
		return float64(p)
	}
	if r.position == nil {
		return nil
	}
	return r.position
}

func (r *Runner) becomeActive(ctx context.Context, first bool) error {
	if err := r.load(ctx, first); err != nil {
		return err
	}
	r.lockLost.Store(false)
	r.active = true
	r.setRole("active")
	// The first commit of each activation writes what the stream is (about.go).
	r.aboutOwed = true
	if eph := r.broker.Ephemeral(); r.opts.serveState && eph != nil {
		if r.server != nil {
			r.server.stop()
		}
		idx := 0
		if r.shard != nil {
			idx = r.shard.Index
		}
		r.server = newStateServer(eph, QueryQueueName(r.queryID, idx), r.answer, r.opts.serveWaitMillis)
		r.server.start()
	}
	return nil
}

// standDown: the lock is gone; drop what was not committed and wait to hold it again.
func (r *Runner) standDown(why string) {
	r.rollback()
	r.initialSnap = false
	r.active = false
	if r.server != nil {
		r.server.stop()
	}
	r.setRole("standby")
	logInfo("FeedRunner.standby", map[string]interface{}{"queryId": r.id, "why": why})
}

func (r *Runner) report(err error, ctx map[string]interface{}) {
	logError("FeedRunner", map[string]interface{}{"queryId": r.id, "error": err.Error(), "phase": ctx["phase"]})
	if r.opts.onError != nil {
		full := map[string]interface{}{"queryId": r.queryID}
		if r.shard != nil {
			full["shard"] = map[string]interface{}{"index": float64(r.shard.Index), "count": float64(r.shard.Count)}
		} else {
			full["shard"] = nil
		}
		for k, v := range ctx {
			full[k] = v
		}
		func() {
			defer func() { _ = recover() }()
			r.opts.onError(err, full)
		}()
		return
	}
	// No hook: say it on stderr, once per message every 30 s, so a stream
	// that cannot run never looks like a stream with nothing to do.
	msg := err.Error()
	if last, ok := r.lastSaid[msg]; !ok || time.Since(last) >= 30*time.Second {
		r.lastSaid[msg] = time.Now()
		phase, _ := ctx["phase"].(string)
		retrying := (phase == "cycle" || phase == "load") && !errors.Is(err, feed.ErrNoFeedRoute)
		suffix := ""
		if retrying {
			suffix = " (retrying)"
		}
		fmt.Fprintf(os.Stderr, "[queen feed] %s: %s%s\n", r.id, msg, suffix)
	}
}

// answer is a question from outside (query.go), answered from the live state.
func (r *Runner) answer(req map[string]interface{}) map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	base := map[string]interface{}{"shard": 0.0, "shards": 1.0, "role": r.m.role, "committed": r.positionValue()}
	if r.shard != nil {
		base["shard"], base["shards"] = float64(r.shard.Index), float64(r.shard.Count)
	}
	if r.reader != nil {
		pos := r.reader.Position()
		if p, ok := pos.(int64); ok {
			base["position"] = float64(p)
		} else {
			base["position"] = pos
		}
	} else {
		base["position"] = r.positionValue()
	}
	op, _ := req["op"].(string)
	merge := func(m map[string]interface{}) map[string]interface{} {
		for k, v := range m {
			base[k] = v
		}
		return base
	}
	if op == "status" {
		base["metrics"] = r.metricsLocked().Map()
		return base
	}
	if !r.pipeline.HasState() {
		base["error"] = "this stream keeps no state"
		return base
	}
	step := jsv.String(jsv.Get(req, "step"))
	if op == "get" {
		return merge(r.pipeline.Inspect(step, jsv.String(jsv.Get(req, "key"))))
	}
	if op == "list" {
		f, _ := jsv.Num(req["limit"])
		limit := int(f)
		if !jsv.Truthy(req["limit"]) || math.IsNaN(f) {
			limit = 100
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 1000 {
			limit = 1000
		}
		return merge(r.pipeline.List(step, limit))
	}
	base["error"] = "unknown question " + string(jsv.MustMarshal(jsv.Get(req, "op"))) + " (get, list, status)"
	if jsv.IsUndefined(jsv.Get(req, "op")) {
		base["error"] = "unknown question undefined (get, list, status)"
	}
	return base
}

// ---- one cycle --------------------------------------------------------------------

func tsOf(entry map[string]interface{}) float64 {
	s, _ := entry["ts"].(string)
	if s == "" {
		s, _ = entry["createdAt"].(string)
	}
	if t, ok := jsv.DateParse(s); ok {
		return t
	}
	return 0
}

func positionOf(entry map[string]interface{}) int64 {
	f, _ := jsv.Num(entry["position"])
	return int64(f)
}

func (r *Runner) cycle(ctx context.Context) error {
	wait := r.opts.waitMillis
	if r.nextWait != nil {
		wait = *r.nextWait
		r.nextWait = nil
	}
	// Stop cancels a long poll in flight: nothing of it was processed yet.
	var page feed.Page
	var err error
	reader := r.reader
	r.unlocked(func() { page, err = reader.Read(r.stopCtx, &wait) })
	if err != nil {
		return err
	}
	r.m.cycles++
	r.m.entriesRead += int64(len(page.Entries))
	r.m.applied = page.Applied
	r.m.lag = nil
	if page.Applied != nil {
		lag := *page.Applied - page.Next + 1
		if lag < 0 {
			lag = 0
		}
		r.m.lag = &lag
	}
	// The broker's clock as of the read: never ahead of the broker's time when
	// the answer left, so resolved - now is a lower bound of how far the
	// broker's clock is from this one.
	resolvedT, hasResolved := jsv.DateParse(page.ResolvedTS)
	if hasResolved {
		now := r.now()
		// This machine's clock was set back: learn the distance again.
		if now < r.lastNow {
			r.offset = math.Inf(-1)
		}
		r.lastNow = now
		if resolvedT > r.resolvedMs {
			r.resolvedMs = resolvedT
			r.resolvedAt = now
		}
		r.offset = math.Max(r.offset, resolvedT-now)
	}
	// Lost the lock while the read was out: what it read is the next holder's.
	if r.lock != nil && (r.lockLost.Load() || !r.lock.Held()) {
		r.standDown("lost")
		return nil
	}
	entries := make([]map[string]interface{}, len(page.Entries))
	for i, e := range page.Entries {
		entries[i] = e.Map()
	}
	if r.initialSnap {
		r.initialSnap = false
		var pos interface{} = float64(page.Next)
		clk := 0.0
		if hasResolved {
			clk = resolvedT
		}
		if len(entries) > 0 {
			pos = entries[0]["position"]
			clk = tsOf(entries[0])
		}
		units := r.units
		r.snap = map[string]interface{}{"id": nil, "chunks": 0.0, "bytes": 0.0, "position": pos, "clock": clk,
			"metas": r.pipeline.Metas(), "units": float64(units)}
		r.pipeline.SnapshotTaken()
	}
	var res *float64
	if hasResolved {
		res = &resolvedT
	}
	pr, err := r.processPage(ctx, entries, page.Next, res, page.CaughtUp)
	if err != nil {
		return err
	}
	if pr == "abandon" {
		return nil
	}
	if pr == "stop" {
		return r.stopOnPoison(ctx)
	}
	return r.settle(ctx, page)
}

type group struct {
	position float64
	entries  []map[string]interface{}
}

// groups: the entries of a page by position; one position is one transaction.
func groups(entries []map[string]interface{}, below float64) []group {
	var out []group
	for _, e := range entries {
		p, _ := jsv.Num(e["position"])
		if p >= below {
			break
		}
		if n := len(out); n > 0 && out[n-1].position == p {
			out[n-1].entries = append(out[n-1].entries, e)
		} else {
			out = append(out, group{position: p, entries: []map[string]interface{}{e}})
		}
	}
	return out
}

type cpRef struct{ cp checkpointT }

// processPage runs every message of a page, in order, then moves the clock to what the read resolved.
func (r *Runner) processPage(ctx context.Context, entries []map[string]interface{}, next int64, resolvedT *float64, caughtUp bool) (string, error) {
	for _, g := range groups(entries, math.Inf(1)) {
		ref := &cpRef{cp: r.checkpoint()}
		res, err := r.processGroup(ctx, g, ref)
		if err != nil {
			return "", err
		}
		if res == "abandon" {
			return "abandon", nil
		}
		if res == "stop" {
			// The whole transaction the poisoned message came in stays for later
			// (from the last commit, when one came in the middle of a close).
			r.restore(ref.cp)
			r.setNext(int64(g.position))
			return "stop", nil
		}
		r.setNext(int64(g.position) + 1)
		r.readTo(tsOf(g.entries[0]), false)
		// Enough waiting outputs: commit them now, between two positions.
		if len(r.pending.outputs) >= r.opts.maxPendingOutputs {
			c, err := r.commit(ctx)
			if err != nil {
				return "", err
			}
			if c != "ok" {
				return "abandon", nil
			}
		}
	}
	if r.pipeline.HasState() && r.pipeline.NeedsFeedClock() {
		ref := &cpRef{cp: r.checkpoint()}
		t := r.clock
		if resolvedT != nil {
			t = math.Max(r.clock, *resolvedT)
		} else {
			t = math.Max(r.clock, 0)
		}
		r.clock = t
		res, err := r.advanceTo(ctx, t, ref)
		if err != nil {
			return "", err
		}
		if res == "abandon" {
			return "abandon", nil
		}
		if res == "stop" {
			r.restore(ref.cp)
			r.setNext(next)
			r.readTo(resolvedOr0(resolvedT), caughtUp)
			return "stop", nil
		}
	}
	r.setNext(next)
	r.readTo(resolvedOr0(resolvedT), caughtUp)
	return "ok", nil
}

func resolvedOr0(t *float64) float64 {
	if t == nil {
		return 0
	}
	return *t
}

// readTo: the feed time the pending position is at (it only moves forward), and whether the read was caught up there.
func (r *Runner) readTo(t float64, caughtUp bool) {
	p := &r.pending
	base := r.committedReadTo
	if p.readTo != nil {
		base = *p.readTo
	}
	if !finite(t) {
		t = 0
	}
	v := math.Max(base, t)
	p.readTo = &v
	c := caughtUp
	p.caughtUp = &c
}

func (r *Runner) setNext(n int64) {
	r.pending.next = n
	r.pending.hasNext = true
}

func (r *Runner) processGroup(ctx context.Context, g group, ref *cpRef) (string, error) {
	// One position is one transaction of the log: its messages share a time.
	t := math.Max(r.clock, tsOf(g.entries[0]))
	if r.pipeline.HasState() {
		// Before the group's messages: a commit in the middle of this close names the group's position.
		res, err := r.advanceTo(ctx, t, ref)
		if err != nil || res != "ok" {
			return res, err
		}
	}
	r.clock = t
	// A table row of the transaction counts before its other messages.
	type routed struct {
		entry map[string]interface{}
		route string
	}
	rs := make([]routed, len(g.entries))
	for i, e := range g.entries {
		rs[i] = routed{e, r.route(e)}
	}
	sort.SliceStable(rs, func(a, b int) bool {
		return rs[a].route == "table" && rs[b].route != "table"
	})
	for _, x := range rs {
		entry := x.entry
		switch x.route {
		case "skip":
			continue
		case "table":
			res, err := r.unit(subject{entry: entry}, func(*counter) error { return r.pipeline.applyRow(entry) })
			if err != nil {
				return "", err
			}
			if res == "stop" {
				return "stop", nil
			}
			r.m.tableRows++
			r.pending.work++
			continue
		}
		res, err := r.unit(subject{entry: entry}, func(ctr *counter) error {
			owned, err := r.owns(entry)
			if err != nil || !owned {
				return err
			}
			return r.pipeline.entry(entry, t, func(env *envelope, ent map[string]interface{}) error {
				return r.out(env, ent, ctr)
			})
		})
		if err != nil {
			return "", err
		}
		if res == "stop" {
			return "stop", nil
		}
		if r.pipeline.HasState() {
			r.pending.work++
		}
	}
	r.entriesSinceSnap += len(g.entries)
	// An event-time step may have moved its watermark. After the group's
	// messages: a commit in the middle of this close names the next position.
	if r.pipeline.HasState() {
		r.setNext(int64(g.position) + 1)
		r.readTo(t, false)
		res, err := r.advanceTo(ctx, t, ref)
		if err != nil || res != "ok" {
			return res, err
		}
	}
	return "ok", nil
}

// advanceTo closes what is due at t; each result is delivered as a unit of
// its own, and counted (units). When many results wait, they commit in the
// middle of the close, after a batch; the commit records the count, and
// ref.cp becomes the point a stop comes back to. A replay stops at the count
// the stored record names.
func (r *Runner) advanceTo(ctx context.Context, t float64, ref *cpRef) (string, error) {
	if !finite(t) {
		return "ok", nil
	}
	live := !r.replaying
	maxUnits := -1
	if !live && r.replayCap >= 0 {
		maxUnits = r.replayCap - r.units
		if maxUnits < 0 {
			maxUnits = 0
		}
	}
	outer := r.burstTarget
	target := t
	if outer != nil {
		target = math.Max(*outer, t)
	}
	r.burstTarget = &target
	defer func() { r.burstTarget = outer }()
	var onBatch func(int) (string, error)
	if live {
		onBatch = func(int) (string, error) {
			if len(r.pending.outputs) < r.opts.maxPendingOutputs {
				return "ok", nil
			}
			r.m.closeCommits++
			c, err := r.commit(ctx)
			if err != nil {
				return "", err
			}
			if c != "ok" {
				return "abandon", nil
			}
			if ref != nil {
				ref.cp = r.checkpoint()
			}
			return "ok", nil
		}
	}
	return r.pipeline.Advance(t, func(em *emit, run func(out outFn) error) (string, error) {
		res, err := r.unit(subject{emit: em}, func(ctr *counter) error {
			return run(func(env *envelope, ent map[string]interface{}) error { return r.out(env, ent, ctr) })
		})
		if err != nil {
			return "", err
		}
		if res != "stop" {
			r.units++
		}
		return res, nil
	}, AdvanceOpts{MaxUnits: maxUnits, OnBatch: onBatch})
}

// out is one result at the end of the chain: the queues it goes to (pushed
// with the next commit) or the .foreach function. A replay sends nothing.
func (r *Runner) out(env *envelope, entry map[string]interface{}, ctr *counter) error {
	if r.replaying {
		return nil
	}
	r.m.emitted++
	var msg interface{} = entry
	if env.derived {
		msg = env.ctx
	}
	if r.sink.Kind == "foreach" {
		var fctx interface{}
		if env.derived || entry == nil {
			fctx = ctxArg(env.ctx)
		} else {
			fctx = map[string]interface{}{"position": entry["position"], "queue": entry["queue"], "partition": entry["partition"],
				"partitionId": entry["partitionId"], "offset": entry["offset"], "transactionId": entry["transactionId"]}
		}
		if err := callErr(func() error { return r.sink.Foreach(env.value, fctx) }); err != nil {
			return err
		}
		r.m.effects++
		r.pending.effects++
		return nil
	}
	fallback := interface{}(env.key)
	if !env.derived && entry != nil {
		fallback = entry["partition"]
	}
	targets, err := targetsFor(r.sink, env.value, msg, fallback)
	if err != nil {
		return err
	}
	for _, tg := range targets {
		var id string
		if entry != nil {
			id = FeedOutputID(r.queryID, entry, ctr.n, r.epoch, r.id)
			ctr.n++
		} else {
			id = r.emitID()
		}
		r.pending.outputs = append(r.pending.outputs, output{queue: tg.Queue, partition: tg.Partition, data: payloadOf(env.value), transactionID: id})
	}
	return nil
}

func (r *Runner) emitID() string {
	seq := r.pending.emitSeq
	r.pending.emitSeq++
	h := sha256Hex(fmt.Sprintf("%s\n%d\nemit\n%d\n%d", r.id, r.epoch, r.version, seq))
	return OutputPrefix(r.queryID) + h[:40]
}

// unit runs fn (a message, or one result of a step) until it passes, at
// most retries more times, each time from the state it saw the first time.
// Then the poison policy. In a replay a failure is left out at once.
func (r *Runner) unit(s subject, fn func(*counter) error) (string, error) {
	defer r.publishSoon()
	if r.replaying {
		// What the run before left out (its poison policy), a replay leaves out.
		if r.skipSet[r.skipID(s)] {
			r.m.replaySkipped++
			return "ok", nil
		}
		for attempt := 0; attempt <= r.opts.retries; attempt++ {
			cp := r.checkpoint()
			err := fn(&counter{})
			if err == nil {
				return "ok", nil
			}
			r.restore(cp)
			if attempt == r.opts.retries {
				r.m.replaySkipped++
				logWarn("FeedRunner.replaySkip", map[string]interface{}{"queryId": r.id, "error": err.Error()})
				return "ok", nil
			}
			r.sleep(r.opts.retryDelayMillis * math.Pow(2, float64(attempt)))
		}
		return "ok", nil
	}
	var lastErr error
	for attempt := 0; attempt <= r.opts.retries; attempt++ {
		cp := r.checkpoint()
		err := fn(&counter{})
		if err == nil {
			return "ok", nil
		}
		r.restore(cp)
		lastErr = err
		if r.stopping.Load() {
			return "", errStopping
		}
		if attempt < r.opts.retries {
			r.m.retries++
			r.sleep(r.opts.retryDelayMillis * math.Pow(2, float64(attempt)))
		}
	}
	return r.poisoned(s, lastErr, r.opts.retries+1), nil
}

// skipID is the identity of a unit, the same on a replay.
func (r *Runner) skipID(s subject) string {
	if s.entry != nil {
		e := s.entry
		return "e" + sep + jsv.String(e["position"]) + sep + jsv.String(e["queue"]) + sep + jsv.String(e["partition"]) + sep + jsv.String(e["offset"])
	}
	em := s.emit
	t := ""
	if em.hasTime {
		t = jsv.NumberString(em.time)
	}
	kind := ""
	if k := jsv.Get(em.ctx, "kind"); !jsv.IsNullish(k) {
		kind = jsv.String(k)
	}
	id := "r" + sep + fmt.Sprint(em.step) + sep + em.key + sep + t + sep + kind
	// A message an event-time step held: its place in the step's order.
	if em.id != "" {
		return id + sep + em.id
	}
	return id
}

// ErrorNamer names an error the way JS names its errors (TypeError, ...), for a dead-letter record.
type ErrorNamer interface{ ErrorName() string }

func errorName(err error) string {
	var n ErrorNamer
	if errors.As(err, &n) {
		return n.ErrorName()
	}
	return "Error"
}

func (r *Runner) poisoned(s subject, err error, attempts int) string {
	r.m.poisoned++
	if r.poison.kind != "stop" && r.pipeline.HasState() {
		r.pendingSkips = append(r.pendingSkips, r.skipID(s))
	}
	// A feed message: the unit's own, or one an event-time step held and took in its turn.
	entry := s.entry
	if entry == nil && s.emit != nil {
		entry = s.emit.entry
	}
	var what jsv.Ordered
	var where string
	if entry != nil {
		what = jsv.Ordered{{K: "position", V: entry["position"]}, {K: "queue", V: entry["queue"]}, {K: "partition", V: entry["partition"]},
			{K: "offset", V: entry["offset"]}, {K: "transactionId", V: entry["transactionId"]}}
		where = fmt.Sprintf("the message at position %s (%s/%s, offset %s)", jsv.String(entry["position"]), jsv.String(entry["queue"]),
			jsv.String(entry["partition"]), jsv.String(entry["offset"]))
	} else {
		what = jsv.Ordered{{K: "step", V: float64(s.emit.step)}, {K: "result", V: jsv.Get(s.emit.ctx, "kind")}, {K: "key", V: s.emit.key}}
		where = fmt.Sprintf("the %s result for key %s", jsv.String(jsv.Get(s.emit.ctx, "kind")), jsv.MustMarshal(s.emit.key))
	}
	reason := err.Error()
	ctxOf := func(action string) map[string]interface{} {
		c := map[string]interface{}{"phase": "poison", "action": action, "attempts": float64(attempts)}
		for _, p := range what {
			c[p.K] = p.V
		}
		return c
	}
	switch r.poison.kind {
	case "skip":
		r.m.skipped++
		r.report(&FeedError{Msg: fmt.Sprintf("%s failed %d times and was skipped: %s", where, attempts, reason), Cause: err}, ctxOf("skipped"))
		return "skipped"
	case "deadLetter":
		record := map[string]interface{}{
			"queryId":  r.queryID,
			"attempts": float64(attempts),
			"error":    map[string]interface{}{"name": errorName(err), "message": reason},
		}
		if entry != nil {
			record["entry"] = snapshotOf(entry, nil)
		} else {
			res := copyMap(s.emit.ctx)
			setDef(res, "value", s.emit.value)
			setDef(res, "waiting", jsv.Get(s.emit.state, "msg"))
			record["result"] = res
		}
		if r.shard != nil {
			record["shard"] = map[string]interface{}{"index": float64(r.shard.Index), "count": float64(r.shard.Count)}
		}
		// A message an event-time step held is one of maybe several items of it: its place tells them apart.
		held := ""
		if s.emit != nil && s.emit.release {
			held = fmt.Sprintf("\n%d\n%s", s.emit.step, s.emit.id)
		}
		var id, partition string
		if entry != nil {
			id = OutputPrefix(r.queryID) + sha256Hex(fmt.Sprintf("%s\n%d\ndlq\n%s\n%s\n%s\n%s%s", r.id, r.epoch, jsv.String(entry["position"]),
				jsv.String(entry["queue"]), jsv.String(entry["partition"]), jsv.String(entry["offset"]), held))[:40]
			partition = jsv.String(entry["partition"])
		} else {
			id = r.emitID()
			partition = s.emit.key
		}
		r.pending.outputs = append(r.pending.outputs, output{queue: r.poison.queue, partition: partition, data: record, transactionID: id})
		r.m.deadLettered++
		r.report(&FeedError{Msg: fmt.Sprintf("%s failed %d times and went to %s: %s", where, attempts, r.poison.queue, reason), Cause: err}, ctxOf("deadLettered"))
		return "deadLettered"
	}
	r.poisonStop = &poisonStopT{where: where, what: what, attempts: attempts, err: err, reason: reason}
	return "stop"
}

// stopOnPoison: onPoison 'stop' commits what came before the message, then stops there.
func (r *Runner) stopOnPoison(ctx context.Context) error {
	p := r.poisonStop
	// This commit's record says why the stream stopped, and at which message:
	// it is written even with nothing else to commit.
	mark := jsv.Ordered{{K: "reason", V: "poison"}, {K: "at", V: jsv.MustISO(r.now())},
		{K: "error", V: cutText(p.reason, stopErrorMaxChars)}, {K: "attempts", V: float64(p.attempts)}}
	if w, ok := clip(p.what, 0).(jsv.Ordered); ok {
		mark = append(mark, w...)
	}
	r.stopMark = mark
	c, err := r.commit(ctx)
	r.stopMark = nil
	if err != nil {
		return err
	}
	if c != "ok" {
		// Another commit won: the stream reads on from the stored position.
		r.poisonStop = nil
		return nil
	}
	fields := map[string]interface{}{}
	for _, kv := range p.what {
		fields[kv.K] = kv.V
	}
	e := &FeedError{Msg: fmt.Sprintf("%s failed %d times: %s. The stream stopped there: fix the function and start it again, "+
		"or run it with onPoison: 'skip' or a dead-letter queue", p.where, p.attempts, p.reason), Code: "feed_poison", Fields: fields, Cause: p.err}
	stopped := map[string]interface{}{"reason": "poison", "error": p.reason, "attempts": float64(p.attempts)}
	ctxMap := map[string]interface{}{"phase": "poison", "action": "stopped", "attempts": float64(p.attempts)}
	for _, kv := range p.what {
		stopped[kv.K] = kv.V
		ctxMap[kv.K] = kv.V
	}
	r.m.stopped = stopped
	r.report(e, ctxMap)
	r.stopping.Store(true)
	return nil
}

func (r *Runner) checkpoint() checkpointT {
	return checkpointT{
		state: r.pipeline.Checkpoint(), outputs: len(r.pending.outputs), work: r.pending.work, effects: r.pending.effects,
		emitSeq: r.pending.emitSeq, clock: r.clock, units: r.units, late: r.pipeline.late, skips: len(r.pendingSkips),
		emitted: r.m.emitted,
	}
}

func (r *Runner) restore(cp checkpointT) {
	r.pipeline.Restore(cp.state)
	if cp.outputs <= len(r.pending.outputs) {
		r.pending.outputs = r.pending.outputs[:cp.outputs]
	}
	r.pending.work = cp.work
	r.pending.effects = cp.effects
	r.pending.emitSeq = cp.emitSeq
	r.clock = cp.clock
	r.units = cp.units
	r.pipeline.late = cp.late
	if cp.skips <= len(r.pendingSkips) {
		r.pendingSkips = r.pendingSkips[:cp.skips]
	}
	r.m.emitted = cp.emitted
}

// rollback goes back to the last commit: what was produced since is dropped, and read again.
func (r *Runner) rollback() {
	r.pending = pendingT{}
	r.pendingSkips = nil
	r.clock = r.committedClock
	r.units = r.committedUnits
	r.pipeline.Discard()
	if r.reader != nil {
		r.reader.Seek(r.position)
	}
}

func (r *Runner) positionInt() (int64, bool) {
	p, ok := r.position.(int64)
	return p, ok
}

func (r *Runner) hasPending() bool {
	p := r.pending
	if len(p.outputs) > 0 || p.work > 0 || p.effects > 0 || (r.pipeline.HasState() && r.pipeline.Dirty()) {
		return true
	}
	if p.hasNext {
		pos, ok := r.positionInt()
		return !ok || p.next != pos
	}
	return false
}

// settle: after a page, commit when it is time, else move the broker's clock if a deadline is due.
func (r *Runner) settle(ctx context.Context, page feed.Page) error {
	p := r.pending
	now := r.now()
	// Outputs and done effects commit soon (at once when caught up); state
	// alone every commitEveryMillis; a position alone now and then.
	outputs := len(p.outputs) > 0 || p.effects > 0
	moved := false
	if p.hasNext {
		pos, ok := r.positionInt()
		moved = !ok || p.next != pos
	}
	due := false
	if outputs {
		due = page.CaughtUp || len(p.outputs) >= r.opts.maxPendingOutputs || now-r.lastCommitAt >= r.opts.commitEveryMillis
	} else if r.pipeline.Dirty() || p.work > 0 {
		due = now-r.lastCommitAt >= r.opts.commitEveryMillis
	} else if moved {
		due = now-r.lastPositionCommit >= r.opts.positionCommitMillis
	}
	if due {
		if _, err := r.commit(ctx); err != nil {
			return err
		}
	}
	if !r.pipeline.HasState() || !r.pipeline.NeedsFeedClock() || r.resolvedAt == 0 {
		return nil
	}
	next, ok := r.pipeline.NextDue()
	if !ok {
		return nil
	}
	// A window or a deadline is pending. Right after a write of this process,
	// a short read shows the broker's time (the best sample).
	if due {
		w := tickReadWaitMillis
		r.nextWait = &w
		return nil
	}
	if !page.CaughtUp {
		return nil
	}
	// Where the broker's clock is now, as far as this process can tell.
	estimate := now + r.offset
	if estimate >= next {
		if now-r.lastTickAt >= tickMinMillis {
			if err := r.tick(ctx, now); err != nil {
				return err
			}
		}
		w := tickReadWaitMillis
		r.nextWait = &w
	} else if now-r.resolvedAt >= clockSyncMillis && now-r.lastTickAt >= clockSyncMillis {
		// The last write anyone made is old: the estimate may be far behind.
		if err := r.tick(ctx, now); err != nil {
			return err
		}
		w := tickReadWaitMillis
		r.nextWait = &w
	} else {
		w := int(math.Max(0, math.Min(float64(r.opts.waitMillis), math.Ceil(next-estimate))))
		r.nextWait = &w
	}
	return nil
}

// tick writes the position: the write moves the broker's clock, and the next read shows it.
func (r *Runner) tick(ctx context.Context, now float64) error {
	r.lastTickAt = now
	r.m.ticks++
	_, err := r.commit(ctx)
	return err
}

// ---- commit -------------------------------------------------------------------------

// snapshotFor is the snapshot the commit at next names.
func (r *Runner) snapshotFor(ctx context.Context, next interface{}) (*snapResult, error) {
	p := r.pipeline
	prev := r.snap
	metas := p.Metas()
	units := r.units
	if prev != nil && !p.ChangedSinceSnapshot() {
		ptr := copyMap(prev)
		ptr["position"], ptr["clock"], ptr["metas"], ptr["units"] = jsPos(next), r.recordClock(), metas, float64(units)
		return &snapResult{pointer: ptr, moved: true}, nil
	}
	layers := layersOf(prev)
	if p.Empty() {
		return &snapResult{pointer: pointerOf(nil, jsPos(next), r.recordClock(), metas, &units), wrote: true, replaced: layers}, nil
	}
	// Due on the wall clock, after so many messages, at a stop, or with many skips.
	due := prev == nil || r.stopping.Load() ||
		r.now()-r.snapWallAt >= r.opts.snapshotEveryMillis ||
		r.entriesSinceSnap >= r.opts.snapshotMaxEntries ||
		len(r.skips)+len(r.pendingSkips) > maxSkips
	if !due {
		return &snapResult{pointer: prev}, nil
	}
	started := time.Now()
	var base interface{}
	if len(layers) > 0 {
		base = layers[0]
	}
	deltaBytes := 0.0
	for _, l := range layers[min(1, len(layers)):] {
		deltaBytes += layerBytes(l)
	}
	full := base == nil || jsv.Get(base, "v") == 1.0 || len(layers) >= MaxLayers || deltaBytes >= layerBytes(base) ||
		p.TouchedSinceSnapshot() >= p.StateKeys()
	sid := newSnapshotID()
	var steps []layerStep
	for _, st := range p.stateful() {
		entries := st.state.Touched()
		if full {
			entries = st.state.Dump()
		}
		steps = append(steps, layerStep{kind: st.kind, entries: entries})
	}
	w, err := writeLayer(ctx, r.broker, PositionNamespace, r.id, sid, r.lock, steps, r.opts.snapshotChunkChars, r.opts.snapshotPieceBytes)
	if err != nil {
		return nil, err
	}
	layer := map[string]interface{}{"sid": sid, "chunks": float64(w.chunks), "bytes": float64(w.bytes)}
	if full {
		layer["full"] = true
	}
	var named []interface{}
	switch {
	case full:
		named = []interface{}{layer}
	case w.chunks > 0:
		named = append(append([]interface{}(nil), layers...), layer)
	default:
		named = layers
	}
	res := &snapResult{
		pointer: pointerOf(named, jsPos(next), r.recordClock(), metas, &units),
		wrote:   true, full: full,
		keys:   layerKeys(r.id, []interface{}{layer}),
		millis: time.Since(started).Milliseconds(),
	}
	if full {
		res.replaced = layers
	}
	return res, nil
}

// recordClock is the clock a commit records: in the middle of a close, the time the close runs up to.
func (r *Runner) recordClock() float64 {
	if r.burstTarget == nil {
		return r.clock
	}
	return math.Max(r.clock, *r.burstTarget)
}

// commit is one transaction: the outputs, the position (and the snapshot it
// names). "ok", "conflict" or "lost".
func (r *Runner) commit(ctx context.Context) (string, error) {
	var next interface{} = r.position
	if r.pending.hasNext {
		next = r.pending.next
	}
	stateful := r.pipeline.HasState()
	var snap *snapResult
	if stateful {
		s, err := r.snapshotFor(ctx, next)
		if err != nil {
			// Chunks of this attempt may be in KV, named by nothing.
			r.hadStray = true
			return "", err
		}
		snap = s
	}
	res, err := r.commitWith(ctx, next, stateful, snap)
	if (err != nil || res != "ok") && snap != nil && snap.wrote && len(snap.keys) > 0 {
		r.hadStray = true
	}
	return res, err
}

func (r *Runner) commitWith(ctx context.Context, next interface{}, stateful bool, snap *snapResult) (string, error) {
	tx := r.broker.Transaction()
	if r.lock != nil {
		tx.Guard(r.lock)
	}
	type grp struct {
		queue, partition string
		items            []PushItem
	}
	var order []string
	groupsBy := map[string]*grp{}
	for _, it := range r.pending.outputs {
		k := it.queue + "\x00" + it.partition
		g, ok := groupsBy[k]
		if !ok {
			g = &grp{queue: it.queue, partition: it.partition}
			groupsBy[k] = g
			order = append(order, k)
		}
		g.items = append(g.items, PushItem{Data: it.data, TransactionID: it.transactionID})
	}
	for _, k := range order {
		g := groupsBy[k]
		tx.Push(g.queue, g.partition, g.items)
	}
	value := map[string]interface{}{"position": jsPos(next), "queryId": r.queryID, "epoch": float64(r.epoch), "updatedAt": jsv.MustISO(r.now())}
	if r.shard != nil {
		value["shard"] = map[string]interface{}{"index": float64(r.shard.Index), "count": float64(r.shard.Count)}
	}
	if r.resetToken != nil {
		value["resetToken"] = *r.resetToken
	}
	recorded := r.recordClock()
	if stateful {
		value["hash"], value["clock"], value["units"], value["snapshot"] = r.hash, recorded, float64(r.units), snap.pointer
	}
	// The skips since the snapshot this record names (none past a new one).
	var skips []interface{}
	if stateful && !(snap.wrote || snap.moved) {
		skips = append(append([]interface{}(nil), r.skips...), r.pendingSkips...)
	}
	if len(skips) > 0 {
		value["skips"] = skips
	}
	// For a reader of the record (the console): the feed time the position is
	// at, whether the read was caught up there, what each step holds, and why it stopped.
	readTo := r.committedReadTo
	if r.pending.readTo != nil {
		readTo = *r.pending.readTo
	}
	var caughtUp bool
	if r.pending.caughtUp != nil {
		caughtUp = *r.pending.caughtUp
	} else {
		pos, ok := r.positionInt()
		nx, nok := next.(int64)
		same := (ok && nok && pos == nx) || (!ok && !nok && r.position == next)
		caughtUp = same && r.committedCaughtUp
	}
	if readTo > 0 {
		value["readTo"] = readTo
	}
	value["caughtUp"] = caughtUp
	if stateful {
		open := jsv.Ordered{}
		var held jsv.Ordered
		for _, s := range r.pipeline.stateful() {
			n := 0
			if s.engine != nil {
				n = s.engine.open()
			} else {
				n = s.table.open()
			}
			open = open.Set(s.name, float64(n))
			if s.clock != nil && s.clock.kind() == "event" && s.clock.held() > 0 {
				held = held.Set(s.name, float64(s.clock.held()))
			}
		}
		value["open"] = open
		if held != nil {
			value["held"] = held
		}
	}
	if r.stopMark != nil {
		value["stopped"] = r.stopMark
	}
	// Every shard writes the same description, under the lock like the rest of the commit.
	if r.aboutOwed {
		if about := r.aboutValue(); about != nil {
			tx.KVPut(PositionNamespace, AboutKey(r.queryID), about, nil, false, true)
		}
	}
	// A layout another deploy wrote since this process started stops it here.
	tx.KVCheck(PositionNamespace, r.queryID+"#layout", r.layoutVersion, true)
	if r.version > 0 {
		v := r.version
		tx.KVPut(PositionNamespace, r.id, value, &v, true, true)
	} else {
		tx.KVPutIfAbsent(PositionNamespace, r.id, value, true, true)
	}
	res, err := tx.Commit(ctx)
	if err != nil {
		// A pushed output is already stored: another process committed these
		// messages -- or this one did, and its answer was lost -- and that
		// commit moved the position. Carry on from the stored position.
		var te *TxError
		if errors.As(err, &te) && te.Reason == "duplicate" {
			if err := r.reload(ctx, "duplicate"); err != nil {
				return "", err
			}
			return "conflict", nil
		}
		return "", err
	}
	if !res.Success {
		if res.Reason != ReasonKVPrecondition {
			reason := res.Reason
			if reason == "" {
				reason = "unknown"
			}
			return "", fmt.Errorf("FeedRunner: commit refused (%s): %s", reason, res.Error)
		}
		// The guard lost, or the position moved. Nothing of this commit was written.
		if r.lock != nil && !r.lock.Held() {
			r.standDown("guard")
			return "lost", nil
		}
		if err := r.layoutMoved(ctx); err != nil {
			return "", err
		}
		if err := r.reload(ctx, "position"); err != nil {
			return "", err
		}
		return "conflict", nil
	}
	version, hasVersion := versionOf(res, r.id)
	committedOutputs := len(r.pending.outputs)
	r.position = next
	r.committedClock = recorded
	r.committedUnits = r.units
	r.committedReadTo = readTo
	r.committedCaughtUp = caughtUp
	r.aboutOwed = false
	if stateful {
		r.snap = snap.pointer
		if snap.wrote || snap.moved {
			r.pipeline.SnapshotTaken()
			r.entriesSinceSnap = 0
			r.snapWallAt = r.now()
		}
		r.skips = skips
		r.skipSet = map[string]bool{}
		for _, s := range skips {
			r.skipSet[jsv.String(s)] = true
		}
		r.pendingSkips = nil
		if snap.wrote {
			r.m.snapshots++
			if snap.full {
				r.m.snapshotsFull++
			}
			b, _ := num(snap.pointer, "bytes")
			c, _ := num(snap.pointer, "chunks")
			r.m.snapshotBytes, r.m.snapshotChunks = int64(b), int64(c)
			r.m.snapshotLayers = int64(len(layersOf(snap.pointer)))
			r.m.snapshotMilli = snap.millis
			// The layers the new pointer does not name: nobody reads them again.
			if len(snap.replaced) > 0 {
				r.oldKeys = append(r.oldKeys, layerKeys(r.id, snap.replaced)...)
			}
		}
		r.pipeline.CommitPoint()
	}
	r.pending = pendingT{}
	r.lastCommitAt = r.now()
	r.lastPositionCommit = r.lastCommitAt
	r.m.commits++
	r.m.outputs += int64(committedOutputs)
	r.m.position = jsPos(next)
	if hasVersion {
		r.version = version
	} else {
		r.refreshVersion(ctx)
	}
	if stateful && snap.wrote && r.hadStray && r.lock != nil {
		// Holding the lock, this process is the only writer: every chunk not of
		// the snapshot just named is left over.
		r.hadStray = false
		keep := map[string]bool{}
		for _, l := range layersOf(snap.pointer) {
			keep[layerSid(l)] = true
		}
		if keys, err := strayChunkKeys(ctx, r.broker, PositionNamespace, r.id, keep); err != nil {
			logWarn("FeedRunner.snapshotStray", map[string]interface{}{"queryId": r.id, "error": err.Error()})
		} else {
			r.oldKeys = append(r.oldKeys, keys...)
		}
	}
	if len(r.oldKeys) > 0 {
		r.dropOldKeys(ctx)
	}
	return "ok", nil
}

// dropOldKeys deletes what no record names any more (best effort).
func (r *Runner) dropOldKeys(ctx context.Context) {
	keys := r.oldKeys
	r.oldKeys = nil
	if err := deleteKeys(ctx, r.broker, PositionNamespace, keys, r.lock); err != nil {
		logWarn("FeedRunner.snapshotCleanup", map[string]interface{}{"queryId": r.id, "keys": len(keys), "error": err.Error()})
	}
}

func (r *Runner) refreshVersion(ctx context.Context) {
	got, err := r.broker.KVGet(ctx, PositionNamespace, r.id)
	if err == nil && got.Found && got.Version != 0 {
		r.version = got.Version
	}
}

// ---- load -----------------------------------------------------------------------------

// reload: lost a commit to another one; take the stored position and state, read on from there.
func (r *Runner) reload(ctx context.Context, why string) error {
	r.m.conflicts++
	r.rollback()
	if err := r.load(ctx, false); err != nil {
		// Half loaded is not loaded: the next cycle loads all of it again.
		r.active = false
		return err
	}
	logInfo("FeedRunner.conflict", map[string]interface{}{"queryId": r.id, "why": why, "version": r.version})
	return nil
}

// load is the position, and the state as of it: the snapshot the record
// names, then the feed read again from the snapshot's position to the
// committed one, sending nothing.
func (r *Runner) load(ctx context.Context, first bool) error {
	qid := r.id
	// Nothing of an earlier load or start carries over.
	r.initialSnap = false
	r.oldKeys = nil
	row, err := r.broker.KVGet(ctx, PositionNamespace, qid)
	if err != nil {
		return err
	}
	var meta map[string]interface{}
	if row.Found {
		meta, _ = row.Value.(map[string]interface{})
	}
	var stored map[string]interface{}
	if meta != nil && jsv.IsInteger(meta["position"]) {
		stored = meta
	}
	r.version = 0
	if row.Found {
		r.version = row.Version
	}
	reset := r.opts.reset
	tok, isTok := reset.(string)
	// true starts over when this process starts; a string, once, at whichever
	// load first finds the stored record without it (a takeover too).
	startOver := (first && reset == true) || (isTok && stored != nil && stored["resetToken"] != tok)
	r.skips = nil
	if !startOver && stored != nil {
		if l, ok := stored["skips"].([]interface{}); ok {
			r.skips = l
		}
	}
	r.skipSet = map[string]bool{}
	for _, s := range r.skips {
		r.skipSet[jsv.String(s)] = true
	}
	r.pendingSkips = nil
	r.resetToken = nil
	if isTok {
		t := tok
		r.resetToken = &t
	} else if stored != nil {
		if s, ok := stored["resetToken"].(string); ok {
			r.resetToken = &s
		}
	}
	src := r.stream.Source
	p := r.pipeline
	isNew := false
	if stored == nil || startOver {
		isNew = true
		switch {
		case stored != nil:
			e := 0
			if jsv.IsInteger(stored["epoch"]) {
				f, _ := jsv.Num(stored["epoch"])
				e = int(f)
			}
			r.epoch = e + 1
		case meta != nil && jsv.IsInteger(meta["epoch"]):
			f, _ := jsv.Num(meta["epoch"])
			r.epoch = int(f) + 1
		default:
			r.epoch = 0
		}
		switch {
		case r.opts.from != nil:
			r.position = normPosition(r.opts.from)
		case src.From != nil:
			r.position = normPosition(src.From)
		case src.Since != nil:
			r.position = "earliest"
		default:
			r.position = "now"
		}
		r.clock = 0
		if p.HasState() {
			p.Wipe()
		}
		// What the stored record kept is nobody's now.
		if stored != nil && stored["snapshot"] != nil {
			r.oldKeys = append(r.oldKeys, layerKeys(qid, layersOf(stored["snapshot"]))...)
		}
		if stored != nil && jsv.IsInteger(stored["buckets"]) {
			f, _ := jsv.Num(stored["buckets"])
			for b := 0; b < int(f); b++ {
				r.oldKeys = append(r.oldKeys, fmt.Sprintf("%s#%d", qid, b))
			}
		}
		r.snap = nil
		if startOver {
			logInfo("FeedRunner.reset", map[string]interface{}{"queryId": qid, "epoch": r.epoch})
		}
	} else {
		r.epoch = 0
		if jsv.IsInteger(stored["epoch"]) {
			f, _ := jsv.Num(stored["epoch"])
			r.epoch = int(f)
		}
		f, _ := jsv.Num(stored["position"])
		r.position = int64(f)
		r.clock = 0
		if c, ok := num(stored, "clock"); ok {
			r.clock = c
		}
		if p.HasState() {
			if err := r.loadState(ctx, stored); err != nil {
				return err
			}
		}
	}
	r.committedClock = r.clock
	r.committedReadTo = r.clock
	if !isNew {
		if t, ok := num(stored, "readTo"); ok {
			r.committedReadTo = t
		}
	}
	r.committedCaughtUp = false
	if isNew {
		r.units = 0
	}
	r.committedUnits = r.units
	if isNew {
		r.entriesSinceSnap = 0
	}
	r.snapWallAt = r.now()
	r.pending = pendingT{}
	// Snapshot attempts that died between their chunks and their commit leave
	// chunks nobody names: looked for at the next snapshot.
	if p.HasState() {
		r.hadStray = true
	}
	if isNew {
		// 'now' is the position when the stream starts, not when its first
		// read comes: a message pushed once Start returned is the stream's.
		if r.position == "now" && src.Since == nil {
			var page feed.Page
			var err error
			reader := r.reader
			r.unlocked(func() { page, err = reader.Probe(ctx) })
			if err != nil {
				return err
			}
			r.position = page.Next
		}
		if r.position != "earliest" {
			for _, t := range p.tables() {
				if t.op.Initial == "all" {
					if err := r.prelude(ctx); err != nil {
						return err
					}
					break
				}
			}
		}
		// Empty where it starts (a table's first rows aside, which need a real snapshot).
		r.initialSnap = p.HasState() && p.Empty()
		if _, ok := r.position.(int64); ok {
			r.reader.Seek(r.position)
		} else if src.Since != nil {
			r.reader.Close()
			nr, err := r.makeReader(r.position, src.Since, false)
			if err != nil {
				return err
			}
			r.reader = nr
		} else {
			r.reader.Seek(r.position)
		}
	} else {
		r.reader.Seek(r.position)
	}
	return nil
}

// jsPos is a position as JSON has it: a number, or "now" / "earliest".
func jsPos(p interface{}) interface{} {
	if n, ok := p.(int64); ok {
		return float64(n)
	}
	return p
}

func normPosition(v interface{}) interface{} {
	if p, ok := feed.PositionOf(v); ok {
		return p
	}
	return v
}

func (r *Runner) loadState(ctx context.Context, stored map[string]interface{}) error {
	p := r.pipeline
	qid := r.id
	snap, _ := stored["snapshot"].(map[string]interface{})
	storedPos, _ := jsv.Num(stored["position"])
	// State the old chain kept: chunks, or messages to read again since the snapshot.
	hasState := false
	if snap != nil {
		if len(layersOf(snap)) > 0 {
			hasState = true
		}
		if sp := snap["position"]; jsv.IsInteger(sp) {
			if f, _ := jsv.Num(sp); f < storedPos {
				hasState = true
			}
		}
	}
	if jsv.IsInteger(stored["buckets"]) {
		hasState = true
	}
	if h, ok := stored["hash"].(string); hasState && ok && h != r.hash {
		return &FeedError{Fatal: true, Code: "feed_chain_changed", Msg: fmt.Sprintf("the steps of %s changed since it last ran, and its state was kept by the old chain: "+
			"run it with { reset: true } (or a reset string) to start it over, or give it a new queryId", r.queryID)}
	}
	if snap != nil {
		layers := layersOf(snap)
		err := func() error {
			if len(layers) > 0 && jsv.Get(layers[0], "v") == 1.0 {
				// Format 1: one JSON of the whole state across the chunks of one id.
				chunks, err := readChunks(ctx, r.broker, PositionNamespace, qid, layerSid(layers[0]), layerChunks(layers[0]))
				if err != nil {
					return err
				}
				dump, err := DecodeSnapshot(chunks)
				if err != nil {
					return err
				}
				p.Load(dump, snap["metas"])
				return nil
			}
			// Format 2: every layer in order, piece by piece.
			p.LoadBegin()
			for _, l := range layers {
				if err := readLayer(ctx, r.broker, PositionNamespace, qid, layerSid(l), layerChunks(l), func(pc piece) error {
					p.LoadPiece(pc.s, pc.k, pc.set, pc.del)
					return nil
				}); err != nil {
					return err
				}
			}
			p.LoadEnd(snap["metas"])
			return nil
		}()
		if err != nil {
			// Another process replaced the snapshot while this one read it: load again.
			now, gerr := r.broker.KVGet(ctx, PositionNamespace, qid)
			sids := func(x interface{}) string {
				var s []string
				for _, l := range layersOf(x) {
					s = append(s, layerSid(l))
				}
				return strings.Join(s, ",")
			}
			var se *SnapshotError
			if errors.As(err, &se) && se.Code == "feed_snapshot_missing" && gerr == nil && now.Found && sids(jsv.Get(now.Value, "snapshot")) != sids(snap) {
				return &FeedError{Code: "feed_snapshot_moved", Msg: fmt.Sprintf("the snapshot of %s was replaced while it loaded; loading again", qid)}
			}
			return err
		}
		// The closes the state holds (the snapshot's count), and how many the
		// stored record says came by its position: the replay stops there.
		r.units = 0
		if u := snap["units"]; jsv.IsInteger(u) {
			f, _ := jsv.Num(u)
			r.units = int(f)
		}
		capUnits := -1
		if jsv.IsInteger(snap["units"]) && jsv.IsInteger(stored["units"]) {
			f, _ := jsv.Num(stored["units"])
			capUnits = int(f)
		}
		snapClock := 0.0
		if c, ok := num(snap, "clock"); ok {
			snapClock = c
		}
		r.snap = snap
		snapPos, snapPosOK := 0.0, false
		if jsv.IsInteger(snap["position"]) {
			snapPos, _ = jsv.Num(snap["position"])
			snapPosOK = true
		}
		if (snapPosOK && snapPos < storedPos) || (capUnits >= 0 && capUnits > r.units) || snapClock < r.clock {
			from := int64(storedPos)
			if snapPosOK {
				from = int64(snapPos)
			}
			n, err := r.replay(ctx, from, int64(storedPos), snapClock, r.clock, capUnits)
			if err != nil {
				return err
			}
			r.entriesSinceSnap = n
		}
		return nil
	}
	if jsv.IsInteger(stored["buckets"]) {
		// A record of the bucket format (before snapshots): the buckets are the
		// state at the position. Loaded once; the next commit writes a snapshot.
		f, _ := jsv.Num(stored["buckets"])
		var keys []string
		for b := 0; b < int(f); b++ {
			keys = append(keys, fmt.Sprintf("%s#%d", qid, b))
		}
		var entries []KV
		if len(keys) > 0 {
			many, err := r.broker.KVGetMany(ctx, PositionNamespace, keys)
			if err != nil {
				return err
			}
			for _, row := range many.Rows {
				e, _ := jsv.Get(row.Value, "e").(map[string]interface{})
				ks := make([]string, 0, len(e))
				for k := range e {
					ks = append(ks, k)
				}
				sort.Strings(ks)
				for _, k := range ks {
					entries = append(entries, KV{K: k, V: e[k]})
				}
			}
		}
		metas := []interface{}{map[string]interface{}{"watermark": stored["watermark"]}}
		st := p.stateful()
		var dump []StepDump
		if len(st) > 0 {
			dump = []StepDump{{Name: st[0].name, Kind: st[0].kind, Entries: entries}}
		}
		p.Load(dump, metas)
		r.snap = nil
		r.oldKeys = append(r.oldKeys, keys...)
		return nil
	}
	p.Load(nil, nil)
	r.snap = nil
	return nil
}

// replay reads the feed from `from` (a snapshot's position) to `to` (the
// committed one) once more, as it ran: same filters, same clock, every result dropped.
func (r *Runner) replay(ctx context.Context, from, to int64, snapClock, committedClock float64, capUnits int) (int, error) {
	started := time.Now()
	reader, err := r.makeReader(from, nil, false)
	if err != nil {
		return 0, err
	}
	r.replaying = true
	r.replayCap = capUnits
	r.clock = snapClock
	n := 0
	err = func() error {
		defer func() {
			r.replaying = false
			r.replayCap = -1
			reader.Close()
		}()
		deadline := time.Now().Add(replayCatchUpMillis * time.Millisecond)
		for {
			pos, _ := reader.PositionInt()
			if pos >= to {
				break
			}
			var page feed.Page
			var err error
			wait := 100
			r.unlocked(func() { page, err = reader.Read(ctx, &wait) })
			if err != nil {
				return err
			}
			entries := make([]map[string]interface{}, len(page.Entries))
			for i, e := range page.Entries {
				entries[i] = e.Map()
			}
			for _, g := range groups(entries, float64(to)) {
				if _, err := r.processGroup(ctx, g, nil); err != nil {
					return err
				}
				n += len(g.entries)
			}
			pos, _ = reader.PositionInt()
			if page.CaughtUp && pos < to && time.Now().After(deadline) {
				return fmt.Errorf("FeedRunner: the follower read has not reached the committed position %d (at %d) after %d ms", to, pos, replayCatchUpMillis)
			}
		}
		r.clock = math.Max(r.clock, committedClock)
		_, err := r.advanceTo(ctx, r.clock, nil)
		return err
	}()
	if err != nil {
		return 0, err
	}
	if capUnits >= 0 && r.units != capUnits {
		logWarn("FeedRunner.replayUnits", map[string]interface{}{"queryId": r.id, "replayed": r.units, "stored": capUnits})
	}
	r.pipeline.CommitPoint()
	r.m.replays++
	r.m.replayEntries += int64(n)
	r.m.replayMillis += time.Since(started).Milliseconds()
	logInfo("FeedRunner.replay", map[string]interface{}{"queryId": r.id, "from": from, "to": to, "entries": n})
	return n, nil
}

// prelude: a new stream with a table reads the table queue from its
// beginning up to where the stream starts, rows only.
func (r *Runner) prelude(ctx context.Context) error {
	src := r.stream.Source
	start, ok := r.position.(int64)
	if !ok {
		if src.Since != nil {
			probe, err := r.makeReader("earliest", src.Since, false)
			if err != nil {
				return err
			}
			var page feed.Page
			zero := 0
			r.unlocked(func() { page, err = probe.Read(ctx, &zero) })
			probe.Close()
			if err != nil {
				return err
			}
			start = page.Next
			if len(page.Entries) > 0 {
				start = page.Entries[0].Position
			}
		} else {
			var page feed.Page
			var err error
			reader := r.reader
			r.unlocked(func() { page, err = reader.Probe(ctx) })
			if err != nil {
				return err
			}
			start = page.Next
		}
	}
	r.position = start
	reader, err := r.makeReader("earliest", nil, true)
	if err != nil {
		return err
	}
	defer reader.Close()
	rows := 0
	for {
		pos := reader.Position()
		if p, ok := pos.(int64); ok && p >= start {
			break
		}
		var page feed.Page
		zero := 0
		r.unlocked(func() { page, err = reader.Read(ctx, &zero) })
		if err != nil {
			return err
		}
		for _, e := range page.Entries {
			if e.Position >= start {
				break
			}
			if err := r.pipeline.applyRow(e.Map()); err != nil {
				return err
			}
			rows++
		}
		if page.CaughtUp {
			break
		}
	}
	r.m.tableRows += int64(rows)
	// The rows are the starting point: a rollback keeps them, and the first commit snapshots them.
	r.pipeline.CommitPoint()
	if rows > 0 {
		r.pending.work++
	}
	logInfo("FeedRunner.tablePrelude", map[string]interface{}{"queryId": r.id, "rows": rows, "start": start})
	return nil
}
