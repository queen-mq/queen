package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // -pprof
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var procStart = time.Now()

// slowUs: MQLOAD_SLOW_MS=<ms> prints (to stderr) the first 50 units whose
// produce latency reached it — a diagnostic, off by default.
var slowUs = func() int64 {
	v, _ := strconv.ParseFloat(os.Getenv("MQLOAD_SLOW_MS"), 64)
	return int64(v * 1000)
}()

// Init applies process-wide runtime defaults: GOGC=400 unless GOGC is set
// (goload ran with GOGC=400 on the loaders: fewer GC cycles for a
// high-allocation, low-live-heap process).
func Init() {
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(400)
	}
}

// Unit is one scheduled send: N messages for ONE entity of ONE topic.
type Unit struct {
	Seq      uint64    // launch sequence number in this process (producer client = Seq % producers)
	Sched    time.Time // scheduled instant (latency baseline)
	SchedUs  int64
	Topic    int
	Entity   uint64
	N        int
	Key      string   // "e<entity>" when -entities > 0, else ""
	Payloads [][]byte // N stamped payloads
	FirstSeq int64    // -txn: the run-unique id ("seq") of message 0; message j carries FirstSeq+j

	left   atomic.Int32
	failed atomic.Int32
	run    *Run
}

// MsgDone must be called exactly once per message of the unit (from any
// goroutine) with the broker's result. The last call completes the unit:
// latency = scheduled -> last broker ack, recorded when every message of the
// unit succeeded; the in-flight slot is released.
func (u *Unit) MsgDone(err error) {
	if err != nil {
		u.failed.Add(1)
		u.run.NoteErr("produce", err)
	}
	if u.left.Add(-1) != 0 {
		return
	}
	r := u.run
	f := int64(u.failed.Load())
	if ok := int64(u.N) - f; ok > 0 {
		r.pushed.Add(ok)
	}
	if r.ids != nil {
		fate := uint8(IdConfirmed)
		if f > 0 {
			fate = IdFailed
		}
		r.ids.Set(u.FirstSeq, u.N, fate)
	}
	if f == 0 {
		r.achievedUnits.Add(1)
		lat := time.Now().UnixMicro() - u.SchedUs
		r.prodLat.record(lat)
		if slowUs > 0 && lat >= slowUs && r.slowSeen.Add(1) <= 50 {
			fmt.Fprintf(os.Stderr, "[slow] %s unit seq=%d topic=%d entity=%d n=%d sched=%s lat=%.1fms inflight=%d\n",
				time.Now().UTC().Format("15:04:05.000"), u.Seq, u.Topic, u.Entity, u.N, u.Sched.UTC().Format("15:04:05.000"), float64(lat)/1000, r.inflight.Load())
		}
	} else {
		r.pushErrUnits.Add(1)
		r.pushErrMsgs.Add(f)
	}
	u.Payloads = nil
	r.inflight.Add(-1)
}

type pacerWorker struct {
	rng    *rand.Rand
	picker *WorkerPicker
}

// Run is one load process: counters, pacer, reporter, start barrier, final.
type Run struct {
	Cfg    *Config
	Tool   string
	Pool   *PayloadPool
	Picker *Picker
	Seed   int64

	// AckSem caps in-flight async acks/commits across the process's consumers
	// (-ack-inflight): acquire blocks, an ack is never shed.
	AckSem chan struct{}

	localVec []bool

	send    func(*Unit)
	workers []*pacerWorker
	unitSeq atomic.Uint64

	inflight      atomic.Int64
	offeredUnits  atomic.Int64
	offeredMsgs   atomic.Int64
	shedUnits     atomic.Int64
	shedMsgs      atomic.Int64
	achievedUnits atomic.Int64
	pushed        atomic.Int64
	pushErrUnits  atomic.Int64
	pushErrMsgs   atomic.Int64
	prodLat       *olHist
	slowSeen      atomic.Int64

	consMu sync.Mutex
	cons   []*ConsumerStats

	errMu   sync.Mutex
	errs    map[string]*errNote
	errKeys []string

	readyAt, t0, prodEnd, endAt time.Time
	late                        time.Duration
	plannedEnd                  time.Time     // t0 + duration (read by the reporter)
	pacerDone                   chan struct{} // closed when the pacer stopped offering

	repStop chan time.Time
	repDone chan struct{}
	winMu   sync.Mutex
	windows []Window
	prev    *snap

	info   map[string]any
	infoMu sync.Mutex

	// -txn (txn.go): transaction statistics, the id ledger and where it goes, the [txn] windows
	txn        *TxnStats
	ids        *IdLedger
	idsOut     string
	txnWindows []TxnWindow
}

type errNote struct {
	Count int64  `json:"count"`
	First string `json:"first"`
}

// NewRun builds the payload pool and the picker and prints the header.
func NewRun(cfg *Config, tool string) (*Run, error) {
	r := &Run{
		Cfg:     cfg,
		Tool:    tool,
		Seed:    cfg.ProcessSeed(),
		AckSem:  make(chan struct{}, cfg.AckInflight),
		prodLat: newOLHist(),
		errs:    map[string]*errNote{},
		info:    map[string]any{},
		repStop: make(chan time.Time, 1),
		repDone: make(chan struct{}),
	}
	pk, err := NewPicker(cfg)
	if err != nil {
		return nil, err
	}
	r.Picker = pk
	maxSrc := cfg.LoaderIndex
	for _, s := range cfg.LocalSrcList() {
		maxSrc = max(maxSrc, s)
	}
	r.localVec = make([]bool, maxSrc+1)
	for _, s := range cfg.LocalSrcList() {
		if s >= 0 {
			r.localVec[s] = true
		}
	}
	r.Pool = NewPayloadPool(r.Seed, cfg.Payload, cfg.MaxUnit(), cfg.LoaderIndex)
	if cfg.Pprof != "" {
		go func() {
			if err := http.ListenAndServe(cfg.Pprof, nil); err != nil {
				fmt.Printf("WARN -pprof %s: %v\n", cfg.Pprof, err)
			}
		}()
	}
	return r, nil
}

// Header prints the self-documenting configuration lines.
func (r *Run) Header(extra string) {
	c := r.Cfg
	ups := c.UnitsPerSec()
	W := PacerWorkers(ups)
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "400(default)"
	}
	fmt.Printf("%s tag=%s loader=%d/%d mode=%s rate=%.0f msg/s batch=%d batch-max=%d max-inflight=%d ramp=%v duration=%v report=%v drain=%v GOMAXPROCS=%d GOGC=%s seed=%d\n",
		r.Tool, c.Tag, c.LoaderIndex, c.Loaders, c.Mode, c.Rate, c.Batch, c.BatchMax, c.MaxInflight, c.Ramp, c.Duration, c.Report, c.Drain, runtime.GOMAXPROCS(0), gogc, r.Seed)
	if c.Rate > 0 {
		fmt.Printf("  offered: %.0f msg/s = %.1f units/s (avg %.1f msgs/unit) across %d pacer workers (%.2f units/s each)\n",
			c.Rate, ups, c.AvgUnit(), W, ups/float64(W))
	}
	fmt.Printf("  topics: %d x %d partitions (%s..%s), entities/topic=%d (%s) | picker: %s\n",
		c.Topics, c.Partitions, c.TopicName(0), c.TopicName(c.Topics-1), c.Entities, map[bool]string{true: "key e<id>, system key partitioner", false: "explicit partition"}[c.Keyed()], r.Picker.Describe())
	fmt.Printf("  payload: goload jsonEvent target %d B, avg %.1f B raw, ~%.0f B stamped ({\"ts\":µs,\"src\":%d,...}), pool %dx%d\n",
		c.Payload, r.Pool.AvgRaw, r.Pool.AvgStamped, c.LoaderIndex, PoolBatches, c.MaxUnit())
	fmt.Printf("  consumers: %d here (ci %d..%d of %d), poll=%d proc-us=%.0f ack=%s ack-inflight=%d local-src=%v\n",
		c.Consumers, c.ConsOffset, c.ConsOffset+c.Consumers-1, c.ConsTotal, c.Poll, c.ProcUs, c.Ack, c.AckInflight, c.LocalSrcList())
	if extra != "" {
		fmt.Println("  " + extra)
	}
}

// SetInfo records a system-specific value for the JSON result.
func (r *Run) SetInfo(k string, v any) {
	r.infoMu.Lock()
	r.info[k] = v
	r.infoMu.Unlock()
}

// NoteErr counts an error by kind, keeps the first message and prints the
// first few (then every 10000th) to stderr.
func (r *Run) NoteErr(kind string, err error) {
	if err == nil {
		return
	}
	r.errMu.Lock()
	e := r.errs[kind]
	if e == nil {
		e = &errNote{First: err.Error()}
		r.errs[kind] = e
		r.errKeys = append(r.errKeys, kind)
	}
	e.Count++
	n := e.Count
	r.errMu.Unlock()
	if n <= 5 || n%10000 == 0 {
		fmt.Fprintf(os.Stderr, "[err %s #%d] %s %v\n", kind, n, time.Now().UTC().Format("15:04:05.000"), err)
	}
}

// ---------------------------------------------------------------------------
// start barrier

// WaitStart prints READY <unix ms>, waits for the start instant (-start-file,
// polled every 100 ms, or -start-at; neither = now) and returns it. A start
// instant already in the past starts now with a WARN.
func (r *Run) WaitStart(ctx context.Context) time.Time {
	r.readyAt = time.Now()
	fmt.Printf("READY %d\n", r.readyAt.UnixMilli())
	var at int64
	if r.Cfg.StartFile != "" {
		var badSince time.Time
		for at == 0 {
			if b, err := os.ReadFile(r.Cfg.StartFile); err == nil {
				s := strings.TrimSpace(string(b))
				if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
					at = v
					break
				}
				if s != "" {
					if badSince.IsZero() {
						badSince = time.Now()
					} else if time.Since(badSince) > 5*time.Second {
						fmt.Printf("WARN start file %s holds %q (not unix ms): starting now\n", r.Cfg.StartFile, s)
						at = time.Now().UnixMilli()
						break
					}
				}
			}
			select {
			case <-ctx.Done():
				return time.Now()
			case <-time.After(100 * time.Millisecond):
			}
		}
	} else if r.Cfg.StartAt > 0 {
		at = r.Cfg.StartAt
	}
	t0 := time.Now()
	if at > 0 {
		start := time.UnixMilli(at)
		if d := time.Until(start); d > 0 {
			fmt.Printf("[start] waiting %v for the common start %d\n", d.Round(time.Millisecond), at)
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
			t0 = start
		} else {
			r.late = -d
			fmt.Printf("WARN late start by %d ms\n", (-d).Milliseconds())
		}
	}
	r.t0 = t0
	fmt.Printf("[start] GO %s (unix ms %d)\n", t0.UTC().Format("15:04:05.000"), t0.UnixMilli())
	return t0
}

// ---------------------------------------------------------------------------
// producing

// Produce paces units from t0 until t0+duration (or ctx done), calling send
// for every launched unit from the pacer workers (send must not block), and
// starts the window reporter. It returns when the pacer stopped.
func (r *Run) Produce(ctx context.Context, t0 time.Time, send func(*Unit)) {
	c := r.Cfg
	r.t0 = t0
	r.plannedEnd = t0.Add(c.Duration)
	r.prodEnd = r.plannedEnd
	r.pacerDone = make(chan struct{})
	defer close(r.pacerDone)
	r.Picker.SetStart(t0.UnixMicro())
	r.startReporter(t0)
	if c.Rate <= 0 || send == nil {
		select {
		case <-ctx.Done():
			r.prodEnd = time.Now()
		case <-time.After(time.Until(r.prodEnd)):
		}
		return
	}
	r.send = send
	ups := c.UnitsPerSec()
	W := PacerWorkers(ups)
	r.workers = make([]*pacerWorker, W)
	for w := range r.workers {
		seed := r.Seed*1_000_003 + int64(w)*7919 + 1
		r.workers[w] = &pacerWorker{rng: rand.New(rand.NewSource(seed)), picker: r.Picker.Worker(seed + 17)}
	}
	p := &Pacer{UnitsPerSec: ups, Ramp: c.Ramp, End: r.prodEnd, Seed: r.Seed, Launch: r.launch, Bulk: r.bulk}
	p.Run(ctx, t0)
	if ctx.Err() != nil {
		r.prodEnd = time.Now()
	}
}

func (r *Run) unitSize(pw *pacerWorker) int {
	c := r.Cfg
	if c.BatchMax > c.Batch {
		return c.Batch + pw.rng.Intn(c.BatchMax-c.Batch+1)
	}
	return c.Batch
}

// launch: the pacer's per-instant offer. Never blocks: the in-flight cap is
// tried non-blockingly and a full cap sheds the unit (offered+shed, not sent).
func (r *Run) launch(w int, sched time.Time) {
	pw := r.workers[w]
	n := r.unitSize(pw)
	r.offeredUnits.Add(1)
	r.offeredMsgs.Add(int64(n))
	if r.inflight.Add(1) > int64(r.Cfg.MaxInflight) {
		r.inflight.Add(-1)
		r.shedUnits.Add(1)
		r.shedMsgs.Add(int64(n))
		return
	}
	us := sched.UnixMicro()
	t, e := pw.picker.Pick(us)
	u := &Unit{Seq: r.unitSeq.Add(1) - 1, Sched: sched, SchedUs: us, Topic: t, Entity: e, N: n, run: r}
	if r.Cfg.Keyed() {
		u.Key = "e" + strconv.FormatUint(e, 10)
	}
	u.left.Store(int32(n))
	if r.ids != nil {
		u.FirstSeq = r.ids.Reserve(n)
		u.Payloads = r.Pool.StampSeq(us, n, u.FirstSeq)
	} else {
		u.Payloads = r.Pool.Stamp(us, n)
	}
	r.send(u)
}

// bulk: backlog beyond maxCatchUp, counted as offered+shed.
func (r *Run) bulk(_ int, n int64) {
	c := r.Cfg
	msgs := n * int64(c.Batch)
	if c.BatchMax > c.Batch {
		msgs += n * int64(c.BatchMax-c.Batch) / 2
	}
	r.offeredUnits.Add(n)
	r.shedUnits.Add(n)
	r.offeredMsgs.Add(msgs)
	r.shedMsgs.Add(msgs)
}

// Inflight returns the units in flight.
func (r *Run) Inflight() int64 { return r.inflight.Load() }

// WaitInflight waits (up to timeout) for every launched unit to complete.
func (r *Run) WaitInflight(timeout time.Duration) bool {
	dl := time.Now().Add(timeout)
	for r.inflight.Load() > 0 {
		if time.Now().After(dl) {
			fmt.Printf("WARN %d units still in flight %v after the producers stopped\n", r.inflight.Load(), timeout)
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// ProdEnd is the instant the producers stopped offering.
func (r *Run) ProdEnd() time.Time { return r.prodEnd }

// Pushed returns the messages acked by the broker so far.
func (r *Run) Pushed() int64 { return r.pushed.Load() }

// Popped returns the load messages consumed so far.
func (r *Run) Popped() int64 {
	var n int64
	for _, cs := range r.consumers() {
		n += cs.popped.Load()
	}
	return n
}

// ---------------------------------------------------------------------------
// consumers

// ConsumerStats is one consumer's counters and e2e histograms (no sharing on
// the hot path; the reporter sums them).
type ConsumerStats struct {
	run      *Run
	e2e      *olHist
	e2eLocal *olHist
	popped   atomic.Int64 // load messages (with "ts") received
	warm     atomic.Int64 // messages without "ts" (warm-up) received
	polls    atomic.Int64
	empty    atomic.Int64
	popErr   atomic.Int64
	acked    atomic.Int64
	ackErr   atomic.Int64
	ackCalls atomic.Int64
	ackLatUs atomic.Int64
}

// NewConsumerStats registers a consumer.
func (r *Run) NewConsumerStats() *ConsumerStats {
	cs := &ConsumerStats{run: r, e2e: newOLHist(), e2eLocal: newOLHist()}
	r.consMu.Lock()
	r.cons = append(r.cons, cs)
	r.consMu.Unlock()
	return cs
}

func (r *Run) consumers() []*ConsumerStats {
	r.consMu.Lock()
	defer r.consMu.Unlock()
	return append([]*ConsumerStats(nil), r.cons...)
}

// Observe parses one received value; for a load message it records the e2e
// latency (receive - scheduled send) and returns true. Warm-up messages
// (no "ts") return false.
func (cs *ConsumerStats) Observe(v []byte, nowUs int64) bool {
	ts, src, ok := ParseStamp(v)
	if !ok {
		return false
	}
	d := nowUs - ts
	if d < 0 {
		d = 0
	}
	cs.e2e.record(d)
	if src >= 0 && src < len(cs.run.localVec) && cs.run.localVec[src] {
		cs.e2eLocal.record(d)
	}
	return true
}

// Polled adds one poll's counts (load messages, warm messages).
func (cs *ConsumerStats) Polled(load, warm int) {
	cs.polls.Add(1)
	if load > 0 {
		cs.popped.Add(int64(load))
	}
	if warm > 0 {
		cs.warm.Add(int64(warm))
	}
}

// Empty counts an empty poll.
func (cs *ConsumerStats) Empty() { cs.empty.Add(1) }

// PopErr counts a consume-side error.
func (cs *ConsumerStats) PopErr(err error) {
	cs.popErr.Add(1)
	cs.run.NoteErr("consume", err)
}

// AckDone records one ack call covering n load messages.
func (cs *ConsumerStats) AckDone(n int, lat time.Duration, err error) {
	cs.ackCalls.Add(1)
	cs.ackLatUs.Add(lat.Microseconds())
	if err != nil {
		cs.ackErr.Add(int64(n))
		cs.run.NoteErr("ack", err)
		return
	}
	cs.acked.Add(int64(n))
}

// ProcSim simulates -proc-us per message: accumulated, slept in >= 1 ms
// chunks (the oversleep is credited, bounded to one chunk).
type ProcSim struct {
	per  time.Duration
	debt time.Duration
}

// NewProcSim returns a simulator for us microseconds per message.
func NewProcSim(us float64) *ProcSim {
	return &ProcSim{per: time.Duration(us * float64(time.Microsecond))}
}

// Add accounts n processed messages, sleeping once >= 1 ms is owed.
func (p *ProcSim) Add(n int) {
	if p.per <= 0 || n <= 0 {
		return
	}
	p.debt += time.Duration(n) * p.per
	if p.debt >= time.Millisecond {
		t := time.Now()
		time.Sleep(p.debt)
		p.debt -= time.Since(t)
		if p.debt < -time.Millisecond {
			p.debt = -time.Millisecond
		}
	}
}

// ---------------------------------------------------------------------------
// reporter

type snap struct {
	t                                            time.Time
	offU, offM, shedU, shedM, achU, pushed, errU int64
	popped, warm, polls, empty, popErr           int64
	acked, ackErr, ackCalls, ackLatUs            int64
	prodH, e2eH, e2eLH                           []int64
	cpu                                          time.Duration
	txOK, txMsgs, txAbort, txErr, txIn           int64 // -txn
	txH                                          []int64
}

func (r *Run) takeSnap() *snap {
	s := &snap{
		t:      time.Now(),
		offU:   r.offeredUnits.Load(),
		offM:   r.offeredMsgs.Load(),
		shedU:  r.shedUnits.Load(),
		shedM:  r.shedMsgs.Load(),
		achU:   r.achievedUnits.Load(),
		pushed: r.pushed.Load(),
		errU:   r.pushErrUnits.Load(),
		prodH:  make([]int64, olNumBuckets),
		e2eH:   make([]int64, olNumBuckets),
		e2eLH:  make([]int64, olNumBuckets),
		cpu:    CPUTime(),
	}
	r.prodLat.snapshot(s.prodH)
	cs := r.consumers()
	e, el := make([]*olHist, len(cs)), make([]*olHist, len(cs))
	for i, c := range cs {
		e[i], el[i] = c.e2e, c.e2eLocal
		s.popped += c.popped.Load()
		s.warm += c.warm.Load()
		s.polls += c.polls.Load()
		s.empty += c.empty.Load()
		s.popErr += c.popErr.Load()
		s.acked += c.acked.Load()
		s.ackErr += c.ackErr.Load()
		s.ackCalls += c.ackCalls.Load()
		s.ackLatUs += c.ackLatUs.Load()
	}
	sumSnapshot(s.e2eH, e)
	sumSnapshot(s.e2eLH, el)
	if r.txn != nil {
		r.txn.snap(s)
	}
	return s
}

func diff(a, b []int64) []int64 {
	out := make([]int64, len(a))
	for i := range a {
		out[i] = b[i] - a[i]
	}
	return out
}

func avgMs(latUs, calls int64) float64 {
	if calls == 0 {
		return 0
	}
	return float64(latUs) / float64(calls) / 1000.0
}

// Window is one reporter window, kept for the JSON result.
type Window struct {
	K          int     `json:"k"` // window k covers [t0+(k-1)*report, t0+k*report]
	End        string  `json:"end"`
	EndMs      int64   `json:"end_ms"`
	Secs       float64 `json:"secs"`
	Offered    float64 `json:"offered_per_s"`
	Achieved   float64 `json:"achieved_per_s"`
	Shed       float64 `json:"shed_per_s"`
	Popped     float64 `json:"popped_per_s"`
	Acked      float64 `json:"acked_per_s"`
	Inflight   int64   `json:"inflight"`
	P50        float64 `json:"p50_ms"`
	P99        float64 `json:"p99_ms"`
	P999       float64 `json:"p999_ms"`
	E2EP50     float64 `json:"e2e_p50_ms"`
	E2EP99     float64 `json:"e2e_p99_ms"`
	E2EP999    float64 `json:"e2e_p999_ms"`
	E2EN       int64   `json:"e2e_n"`
	LocalP50   float64 `json:"e2e_local_p50_ms"`
	LocalP99   float64 `json:"e2e_local_p99_ms"`
	LocalN     int64   `json:"e2e_local_n"`
	CPUPct     float64 `json:"cpu_pct"`
	Goroutines int     `json:"goroutines"`
}

// windowLine prints the SPEC §3 window line for (prev, cur] and records it.
func (r *Run) windowLine(k int, prev, cur *snap) {
	secs := cur.t.Sub(prev.t).Seconds()
	if secs <= 0 {
		return
	}
	ph := diff(prev.prodH, cur.prodH)
	eh := diff(prev.e2eH, cur.e2eH)
	el := diff(prev.e2eLH, cur.e2eLH)
	en, eln := countOf(eh), countOf(el)
	w := Window{
		K: k, End: cur.t.UTC().Format("15:04:05"), EndMs: cur.t.UnixMilli(), Secs: secs,
		Offered:  float64(cur.offM-prev.offM) / secs,
		Achieved: float64(cur.pushed-prev.pushed) / secs,
		Shed:     float64(cur.shedM-prev.shedM) / secs,
		Popped:   float64(cur.popped-prev.popped) / secs,
		Acked:    float64(cur.acked-prev.acked) / secs,
		Inflight: r.inflight.Load(),
		P50:      olPercentile(ph, 0.50), P99: olPercentile(ph, 0.99), P999: olPercentile(ph, 0.999),
		E2EP50: olPercentile(eh, 0.50), E2EP99: olPercentile(eh, 0.99), E2EP999: olPercentile(eh, 0.999), E2EN: en,
		LocalP50: olPercentile(el, 0.50), LocalP99: olPercentile(el, 0.99), LocalN: eln,
		CPUPct:     100 * (cur.cpu - prev.cpu).Seconds() / secs,
		Goroutines: runtime.NumGoroutine(),
	}
	fmt.Printf("[%s] offered=%9.0f/s achieved=%9.0f/s shed=%9.0f/s inflight=%6d | p50=%7.2f p99=%8.2f p999=%8.2f ms | push=%d pop=%d lag=%d | errs push=%d pop=%d empty=%d gor=%d | ack=%9.0f/s ackErr=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f n=%d | e2e_local p50=%.2f p99=%.2f n=%d\n",
		w.End, w.Offered, w.Achieved, w.Shed, w.Inflight,
		w.P50, w.P99, w.P999,
		cur.pushed, cur.popped, cur.pushed-cur.popped,
		cur.errU, cur.popErr, cur.empty, w.Goroutines,
		w.Acked, cur.ackErr, avgMs(cur.ackLatUs, cur.ackCalls),
		w.E2EP50, w.E2EP99, w.E2EP999, en,
		w.LocalP50, w.LocalP99, eln)
	r.winMu.Lock()
	r.windows = append(r.windows, w)
	r.winMu.Unlock()
	r.txnLine(k, prev, cur)
}

// startReporter prints a window at every t0 + k*report.
func (r *Run) startReporter(t0 time.Time) {
	r.prev = r.takeSnap()
	go func() {
		defer close(r.repDone)
		rep := r.Cfg.Report
		k := 1
		for {
			dl := t0.Add(time.Duration(k) * rep)
			tm := time.NewTimer(time.Until(dl))
			select {
			case <-tm.C:
				r.settleEnd(dl)
				cur := r.takeSnap()
				r.windowLine(k, r.prev, cur)
				r.prev = cur
				k++
			case end := <-r.repStop:
				tm.Stop()
				// print the windows whose deadline is not after the end
				for dl = t0.Add(time.Duration(k) * rep); !dl.After(end); dl = t0.Add(time.Duration(k) * rep) {
					time.Sleep(time.Until(dl))
					r.settleEnd(dl)
					cur := r.takeSnap()
					r.windowLine(k, r.prev, cur)
					r.prev = cur
					k++
				}
				return
			}
		}
	}()
}

// settleEnd: the window whose deadline reaches the end of producing waits for
// the pacer's last wake (<= 1 ms after the end) so every unit scheduled before
// the end is offered inside it, not in the next (drain) window.
func (r *Run) settleEnd(dl time.Time) {
	if dl.Before(r.plannedEnd) {
		return
	}
	select {
	case <-r.pacerDone:
	case <-time.After(250 * time.Millisecond):
	}
}

// StopReporter prints the remaining windows up to end and stops the reporter.
func (r *Run) StopReporter(end time.Time) {
	if r.prev == nil {
		return
	}
	r.repStop <- end
	<-r.repDone
}

// Drain keeps the consumers going for -drain after the producers stopped
// (returns early on ctx done). Returns the instant the drain ended.
func (r *Run) Drain(ctx context.Context) time.Time {
	end := r.prodEnd.Add(r.Cfg.Drain)
	if d := time.Until(end); d > 0 && ctx.Err() == nil {
		fmt.Printf("[drain] consumers keep going for %v (lag now %d)\n", d.Round(time.Millisecond), r.Pushed()-r.Popped())
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
	if now := time.Now(); now.Before(end) {
		end = now
	}
	return end
}

// ---------------------------------------------------------------------------
// final

// Finish prints the [final] + load_cpu lines and writes -out.
func (r *Run) Finish(end time.Time) {
	r.endAt = time.Now()
	fin := r.takeSnap()
	c := r.Cfg
	cpu := CPUTime()
	wall := time.Since(procStart)
	loadCPU := 100 * cpu.Seconds() / wall.Seconds()

	steady := r.steady()
	if s, ok := steady["secs"].(float64); ok && s > 0 {
		fmt.Printf("[info] steady window %.0fs (after the ramp, before the producers stopped): offered %.0f/s pushed %.0f/s popped %.0f/s cpu %.1f%% = %.2f cores per 100k msg/s (produce+consume)\n",
			s, steady["offered_per_s"], steady["pushed_per_s"], steady["popped_per_s"], steady["cpu_pct"], steady["cores_per_100k"])
	}
	for _, k := range r.errKeys {
		e := r.errs[k]
		fmt.Printf("[info] errors %s=%d first: %s\n", k, e.Count, e.First)
	}
	if fin.warm > 0 {
		fmt.Printf("[info] %d warm-up messages (no \"ts\") received and not counted as load\n", fin.warm)
	}
	pp := fin.prodH
	fmt.Printf("[final] offered=%d achieved=%d shed=%d (msgs: offered=%d achieved=%d shed=%d) pushErr=%d | pushed=%d popped=%d lag=%d | popErr=%d empty=%d | overall p50=%.2f p99=%.2f p999=%.2f ms | acked=%d ackErr=%d ackLag=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f ms\n",
		fin.offU, fin.achU, fin.shedU,
		fin.offM, fin.pushed, fin.shedM,
		fin.errU,
		fin.pushed, fin.popped, fin.pushed-fin.popped,
		fin.popErr, fin.empty,
		olPercentile(pp, 0.50), olPercentile(pp, 0.99), olPercentile(pp, 0.999),
		fin.acked, fin.ackErr, fin.pushed-fin.acked, avgMs(fin.ackLatUs, fin.ackCalls),
		olPercentile(fin.e2eH, 0.50), olPercentile(fin.e2eH, 0.99), olPercentile(fin.e2eH, 0.999))
	fmt.Printf("load_cpu=%.1f%%\n", loadCPU)
	txnRes := r.txnFinal(fin)
	if r.ids != nil && r.idsOut != "" {
		conf, failed, unans, err := r.ids.WriteFile(r.idsOut, c.LoaderIndex)
		if err != nil {
			fmt.Printf("WARN writing the id ledger %s: %v\n", r.idsOut, err)
		} else {
			fmt.Printf("[ids] ledger %s: %d confirmed, %d failed, %d unanswered\n", r.idsOut, conf, failed, unans)
		}
	}

	if c.Out == "" {
		return
	}
	lat := func(h []int64) map[string]any {
		return map[string]any{"p50": olPercentile(h, 0.50), "p90": olPercentile(h, 0.90), "p99": olPercentile(h, 0.99),
			"p999": olPercentile(h, 0.999), "max": maxOf(h), "n": countOf(h)}
	}
	r.winMu.Lock()
	wins := append([]Window(nil), r.windows...)
	r.winMu.Unlock()
	ms := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.UnixMilli()
	}
	res := map[string]any{
		"tool": r.Tool, "tag": c.Tag, "loader_index": c.LoaderIndex, "loaders": c.Loaders,
		"config": c.Flags(), "system": r.info, "seed": r.Seed,
		"ready_ms": ms(r.readyAt), "start_ms": ms(r.t0), "late_start_ms": r.late.Milliseconds(),
		"producers_end_ms": ms(r.prodEnd), "drain_end_ms": ms(end), "end_ms": ms(r.endAt),
		"units":  map[string]int64{"offered": fin.offU, "achieved": fin.achU, "shed": fin.shedU, "errors": fin.errU},
		"msgs":   map[string]int64{"offered": fin.offM, "achieved": fin.pushed, "shed": fin.shedM, "errors": r.pushErrMsgs.Load()},
		"pushed": fin.pushed, "popped": fin.popped, "lag": fin.pushed - fin.popped, "warm_received": fin.warm,
		"polls": fin.polls, "pop_errors": fin.popErr, "empty_polls": fin.empty,
		"acked": fin.acked, "ack_errors": fin.ackErr, "ack_lag": fin.pushed - fin.acked, "ack_calls": fin.ackCalls,
		"ack_avg_ms": avgMs(fin.ackLatUs, fin.ackCalls),
		"produce_ms": lat(fin.prodH), "e2e_ms": lat(fin.e2eH), "e2e_local_ms": lat(fin.e2eLH),
		"hist_layout":     "goload olHist: µs, [0,1024) unit buckets, then 64 sub-buckets per octave up to 2^26 µs; keys = bucket index",
		"hist_produce_us": sparse(fin.prodH), "hist_e2e_us": sparse(fin.e2eH), "hist_e2e_local_us": sparse(fin.e2eLH),
		"load_cpu_pct": loadCPU, "cpu_s": cpu.Seconds(), "wall_s": wall.Seconds(),
		"payload_avg_raw_bytes": r.Pool.AvgRaw, "payload_avg_bytes": r.Pool.AvgStamped,
		"steady": steady, "windows": wins, "errors": r.errs,
	}
	if txnRes != nil {
		res["txn"] = txnRes
	}
	js, err := json.MarshalIndent(res, "", "  ")
	if err == nil {
		err = os.WriteFile(c.Out, js, 0o644)
	}
	if err != nil {
		fmt.Printf("WARN writing %s: %v\n", c.Out, err)
	}
}

// steady summarizes the windows fully inside [t0+ramp, prodEnd]: the rates
// and this process's CPU there (cores per 100k produced msg/s, produce and
// consume both running in this process).
func (r *Run) steady() map[string]any {
	r.winMu.Lock()
	defer r.winMu.Unlock()
	rep := r.Cfg.Report
	end := r.prodEnd.Sub(r.t0) // planned t0+duration, or earlier on a signal
	var secs, off, push, pop, cpu float64
	for _, w := range r.windows {
		// by nominal window bounds: [ (k-1)*report, k*report ] inside [ramp, end]
		if time.Duration(w.K-1)*rep < r.Cfg.Ramp || time.Duration(w.K)*rep > end+time.Millisecond {
			continue
		}
		secs += w.Secs
		off += w.Offered * w.Secs
		push += w.Achieved * w.Secs
		pop += w.Popped * w.Secs
		cpu += w.CPUPct * w.Secs
	}
	out := map[string]any{"secs": secs}
	if secs > 0 {
		out["offered_per_s"] = off / secs
		out["pushed_per_s"] = push / secs
		out["popped_per_s"] = pop / secs
		out["cpu_pct"] = cpu / secs
		if push > 0 {
			out["cores_per_100k"] = (cpu / secs / 100) / (push / secs / 1e5)
		} else {
			out["cores_per_100k"] = 0.0
		}
	}
	return out
}

// SortedTopics is a helper for printing maps deterministically.
func SortedTopics(m map[string][]int32) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
