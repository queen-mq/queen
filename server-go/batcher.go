package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// Unified engine — a single drain orchestrator over per-type lanes sharing ONE
// global PG concurrency budget, dispatched with weighted round-robin fairness.
//
// This is the piece that makes Go match (or beat) the C++ broker on the BALANCED
// workload: with independent per-type batchers, the ultra-efficient push path
// grabbed all of Postgres' write bandwidth and starved pop. Here push/pop/ack
// draw fire permits from ONE budget, handed out fairly, so pop always gets its
// share of Postgres — exactly the intent of libqueen's round-robin drain
// orchestrator + shared slot pool.
// ============================================================================

type job struct {
	items    [][]byte
	parts    []string // gate keys ("queue\x1fpartition"), push only
	enqueued time.Time
	resp     chan jobResult
}

type jobResult struct {
	elems []json.RawMessage
	err   error
}

type idxRange struct {
	start int
	count int
}

const (
	lanePush = 0
	lanePop  = 1
	laneAck  = 2
)

type lane struct {
	name   string
	sql    string
	policy TypePolicy
	weight int // fair-share weight (fire permits per RR turn)

	metrics    *opMetrics
	gate       bool
	maxParts   int
	pushObject bool
	onPU       func(json.RawMessage)

	pending  []*job
	inflight map[string]int // per-partition gate refcounts
	active   int            // in-flight batches (per-lane ceiling = policy.MaxConcurrent)
}

// laneSpec is the shared (immutable) configuration for a lane; each engine
// shard builds its own lane state (pending/active/inflight) from it, but shares
// the metrics + partition-lookup callback pointers.
type laneSpec struct {
	name       string
	sql        string
	policy     TypePolicy
	weight     int
	metrics    *opMetrics
	gate       bool
	maxParts   int
	pushObject bool
	onPU       func(json.RawMessage)
}

// Engines shards the unified engine across N independent scheduler goroutines
// (the Go analogue of Queen's NUM_WORKERS). A single scheduler goroutine
// serializes submit+dispatch+completion and caps SP-call launch rate; sharding
// parallelizes it. Shards share ONE pgx pool (total PG concurrency unchanged)
// and the metrics; each owns its lane queues + budget.
type Engines struct {
	shards []*Engine
	rr     atomic.Uint64
}

func NewEngines(n int, pools []*pgxpool.Pool, stmtTO time.Duration, globalPerShard, pipeDepth int, specs []laneSpec) *Engines {
	if n < 1 {
		n = 1
	}
	es := &Engines{shards: make([]*Engine, n)}
	for i := 0; i < n; i++ {
		es.shards[i] = NewEngine(pools[i], stmtTO, globalPerShard, pipeDepth, specs)
	}
	return es
}

func (es *Engines) submit(laneIdx int, items [][]byte, parts []string) ([]json.RawMessage, error) {
	s := es.shards[int(es.rr.Add(1))%len(es.shards)]
	return s.submit(laneIdx, items, parts)
}

type submitReq struct {
	lane int
	job  *job
}

type doneMsg struct {
	lane     int
	reserved []string
}

// Engine owns all lane state on a single goroutine (no locks); handlers submit
// via a channel and block on their job's result.
type Engine struct {
	pool   *pgxpool.Pool
	stmtTO time.Duration

	global       int // shared concurrency budget = # of pipelined connections
	globalActive int
	pipeDepth    int // SP calls pipelined per connection per SendBatch
	rr           int // round-robin cursor for fairness

	lanes []*lane

	in    chan submitReq
	done  chan doneMsg
	debug bool
}

func NewEngine(pool *pgxpool.Pool, stmtTO time.Duration, global, pipeDepth int, specs []laneSpec) *Engine {
	if global < 1 {
		global = 1
	}
	if pipeDepth < 1 {
		pipeDepth = 1
	}
	lanes := make([]*lane, len(specs))
	for i, s := range specs {
		w := s.weight
		if w < 1 {
			w = 1
		}
		lanes[i] = &lane{
			name: s.name, sql: s.sql, policy: s.policy, weight: w,
			metrics: s.metrics, gate: s.gate, maxParts: s.maxParts,
			pushObject: s.pushObject, onPU: s.onPU,
			inflight: make(map[string]int),
		}
	}
	e := &Engine{
		pool:      pool,
		stmtTO:    stmtTO,
		global:    global,
		pipeDepth: pipeDepth,
		lanes:     lanes,
		in:        make(chan submitReq, 16384),
		done:      make(chan doneMsg, global+4),
		debug:     os.Getenv("QUEEN_DEBUG_BATCHER") != "",
	}
	go e.run()
	return e
}

func (e *Engine) submit(laneIdx int, items [][]byte, parts []string) ([]json.RawMessage, error) {
	j := &job{items: items, parts: parts, enqueued: time.Now(), resp: make(chan jobResult, 1)}
	e.in <- submitReq{lane: laneIdx, job: j}
	r := <-j.resp
	return r.elems, r.err
}

func (e *Engine) run() {
	var timer *time.Timer
	var timerC <-chan time.Time
	stop := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}
	arm := func(d time.Duration) {
		stop()
		if d < time.Millisecond {
			d = time.Millisecond
		}
		timer = time.NewTimer(d)
		timerC = timer.C
	}

	var dbgC <-chan time.Time
	if e.debug {
		dt := time.NewTicker(time.Second)
		defer dt.Stop()
		dbgC = dt.C
	}

	dispatch := func() {
		// Weighted round-robin: hand each free global slot to the next eligible
		// lane, rotating the start each pass so no lane monopolizes the budget.
		gateBlocked := false
		for e.globalActive < e.global {
			progress := false
			for k := 0; k < len(e.lanes); k++ {
				if e.globalActive >= e.global {
					break
				}
				laneIdx := (e.rr + k) % len(e.lanes)
				l := e.lanes[laneIdx]
				// Fire up to `weight` batches for this lane per turn (weighted RR),
				// each bounded by the per-lane ceiling and the shared global budget.
				for w := 0; w < l.weight && e.globalActive < e.global; w++ {
					if l.active >= l.policy.MaxConcurrent {
						break
					}
					if len(l.pending) == 0 {
						break
					}
					// Hold for fusion: fire only once Preferred jobs have
					// accumulated OR the oldest waited MaxHold. This bounds
					// commits/fsyncs (the real throughput lever under
					// synchronous_commit) instead of firing tiny batches eagerly.
					// Under load, pending >= Preferred so it fires immediately and
					// still fills concurrency across slots.
					if len(l.pending) < l.policy.Preferred {
						age := time.Since(l.pending[0].enqueued)
						if age < l.policy.MaxHold {
							arm(l.policy.MaxHold - age)
							break
						}
					}
					batch, reserved, blocked := e.selectBatch(l)
					if blocked {
						gateBlocked = true
					}
					if len(batch) == 0 {
						break
					}
					l.pending = removeSelected(l.pending, batch)
					for _, p := range reserved {
						l.inflight[p]++
					}
					l.active++
					e.globalActive++
					go e.fire(l, laneIdx, batch, reserved)
					progress = true
				}
			}
			e.rr = (e.rr + 1) % len(e.lanes)
			if !progress {
				break
			}
		}
		if gateBlocked {
			arm(2 * time.Millisecond)
		} else {
			stop()
		}
	}

	for {
		select {
		case r := <-e.in:
			l := e.lanes[r.lane]
			l.pending = append(l.pending, r.job)
			dispatch()
		case d := <-e.done:
			l := e.lanes[d.lane]
			l.active--
			e.globalActive--
			for _, p := range d.reserved {
				if l.inflight[p] <= 1 {
					delete(l.inflight, p)
				} else {
					l.inflight[p]--
				}
			}
			dispatch()
		case <-timerC:
			timerC = nil
			timer = nil
			dispatch()
		case <-dbgC:
			parts := make([]string, len(e.lanes))
			for i, l := range e.lanes {
				parts[i] = l.name + "(act=" + strconv.Itoa(l.active) + ",pend=" + strconv.Itoa(len(l.pending)) + ",fired=" + strconv.FormatUint(l.metrics.batchesFired.Load(), 10) + ")"
			}
			log.Printf("[engine] global=%d/%d %v", e.globalActive, e.global, parts)
		}
	}
}

// selectBatch picks up to Preferred jobs from a lane (so multiple concurrent
// batches form), honoring the per-partition gate + max-distinct-partitions cap.
func (e *Engine) selectBatch(l *lane) (batch []*job, reserved []string, blocked bool) {
	limit := l.policy.Preferred
	if limit < 1 {
		limit = 1
	}
	if limit > l.policy.MaxBatch {
		limit = l.policy.MaxBatch
	}
	if !l.gate {
		n := len(l.pending)
		if n > limit {
			n = limit
		}
		// COPY the job pointers: the caller (dispatch) immediately compacts
		// l.pending in-place via removeSelected, which reuses the SAME backing
		// array — returning l.pending[:n] would alias it and get overwritten
		// before fire() reads the batch (corrupting demux → duplicate resp sends
		// → deadlock). This was the pop bottleneck.
		batch := make([]*job, n)
		copy(batch, l.pending[:n])
		return batch, nil, false
	}
	maxParts := l.maxParts
	if maxParts < 1 {
		maxParts = 8
	}
	resSet := make(map[string]struct{})
	for _, j := range l.pending {
		if len(batch) >= limit {
			break
		}
		conflict := false
		for _, p := range j.parts {
			if _, inFlight := l.inflight[p]; inFlight {
				conflict = true
				break
			}
		}
		if conflict {
			blocked = true
			continue
		}
		newParts := 0
		for _, p := range j.parts {
			if _, seen := resSet[p]; !seen {
				newParts++
			}
		}
		if len(resSet) > 0 && len(resSet)+newParts > maxParts {
			blocked = true
			continue
		}
		batch = append(batch, j)
		for _, p := range j.parts {
			resSet[p] = struct{}{}
		}
	}
	for p := range resSet {
		reserved = append(reserved, p)
	}
	return batch, reserved, blocked
}

func removeSelected(pending, batch []*job) []*job {
	if len(batch) == 0 {
		return pending
	}
	sel := make(map[*job]struct{}, len(batch))
	for _, j := range batch {
		sel[j] = struct{}{}
	}
	out := pending[:0]
	for _, j := range pending {
		if _, ok := sel[j]; !ok {
			out = append(out, j)
		}
	}
	return out
}

// fire executes one fused SP call for a lane and demuxes the result.
//
// NOTE on pipelining: pgx SendBatch (libpq async pipelining) was evaluated here
// but only amortizes network round-trips — on a co-located (localhost/UDS)
// broker+PG the round-trip is ~tens of microseconds, and Postgres still
// processes a pipeline serially per backend, so it does not raise the pop
// ceiling. Real concurrency comes from many connections (the shared global
// budget), which this fire path provides. Pipelining is retained as an option
// (QUEEN_PIPELINE_DEPTH) but defaults to per-call for co-located deployments.
func (e *Engine) fire(l *lane, laneIdx int, batch []*job, reserved []string) {
	defer func() { e.done <- doneMsg{lane: laneIdx, reserved: reserved} }()

	merged, ranges := buildMerged(batch)
	items := 0
	for _, r := range ranges {
		items += r.count
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.stmtTO)
	defer cancel()

	start := time.Now()
	var raw []byte
	err := e.pool.QueryRow(ctx, l.sql, string(merged)).Scan(&raw)
	if l.metrics != nil {
		l.metrics.recordBatch(items, err == nil, time.Since(start))
	}
	if err != nil {
		for _, j := range batch {
			j.resp <- jobResult{err: err}
		}
		return
	}
	e.demux(l, raw, batch, ranges)
}

// demux splits one fused SP result back to the jobs of one group by idx range,
// using cheap byte-level idx extraction (never json.Unmarshal a result element).
func (e *Engine) demux(l *lane, raw []byte, batch []*job, ranges []idxRange) {
	var arr []json.RawMessage
	if l.pushObject {
		var obj struct {
			Items            []json.RawMessage `json:"items"`
			PartitionUpdates json.RawMessage   `json:"partition_updates"`
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			for _, j := range batch {
				j.resp <- jobResult{err: err}
			}
			return
		}
		arr = obj.Items
		if l.onPU != nil && len(obj.PartitionUpdates) > 0 &&
			!bytes.Equal(bytes.TrimSpace(obj.PartitionUpdates), []byte("[]")) {
			l.onPU(obj.PartitionUpdates)
		}
	} else {
		if err := json.Unmarshal(raw, &arr); err != nil {
			for _, j := range batch {
				j.resp <- jobResult{err: err}
			}
			return
		}
	}

	byIdx := make(map[int]json.RawMessage, len(arr))
	for _, el := range arr {
		if id := extractLeadingIdx(el); id >= 0 {
			byIdx[id] = el
		}
	}
	for i, j := range batch {
		r := ranges[i]
		elems := make([]json.RawMessage, 0, r.count)
		for k := r.start; k < r.start+r.count; k++ {
			if el, ok := byIdx[k]; ok {
				elems = append(elems, el)
			}
		}
		j.resp <- jobResult{elems: elems}
	}
}

// extractLeadingIdx reads the first "idx"/"index" integer from a JSON object's
// raw bytes without parsing the (potentially huge) rest of the object.
func extractLeadingIdx(raw []byte) int {
	for _, key := range [][]byte{[]byte(`"idx":`), []byte(`"index":`)} {
		i := bytes.Index(raw, key)
		if i < 0 {
			continue
		}
		j := i + len(key)
		for j < len(raw) && (raw[j] == ' ' || raw[j] == '\t') {
			j++
		}
		neg := false
		if j < len(raw) && raw[j] == '-' {
			neg = true
			j++
		}
		n, start := 0, j
		for j < len(raw) && raw[j] >= '0' && raw[j] <= '9' {
			n = n*10 + int(raw[j]-'0')
			j++
		}
		if j > start {
			if neg {
				return -n
			}
			return n
		}
	}
	return -1
}

// buildMerged concatenates every job's item objects into one JSON array,
// front-injecting a renumbered idx/index per item, recording per-job idx ranges.
// Byte-for-byte the same strategy as libqueen::_fire_batched (raw pass-through).
func buildMerged(batch []*job) ([]byte, []idxRange) {
	var buf bytes.Buffer
	size := 16
	for _, j := range batch {
		for _, it := range j.items {
			size += len(it) + 32
		}
	}
	buf.Grow(size)
	buf.WriteByte('[')

	ranges := make([]idxRange, len(batch))
	seq := 0
	first := true
	for bi, j := range batch {
		start := seq
		count := 0
		for _, item := range j.items {
			brace := bytes.IndexByte(item, '{')
			if brace < 0 {
				continue
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteString(`{"idx":`)
			buf.WriteString(strconv.Itoa(seq))
			buf.WriteString(`,"index":`)
			buf.WriteString(strconv.Itoa(seq))
			body := item[brace+1:]
			trimmed := bytes.TrimLeft(body, " \t\r\n")
			if len(trimmed) > 0 && trimmed[0] != '}' {
				buf.WriteByte(',')
				buf.Write(body)
			} else {
				buf.WriteByte('}')
			}
			seq++
			count++
		}
		ranges[bi] = idxRange{start: start, count: count}
	}
	buf.WriteByte(']')
	return buf.Bytes(), ranges
}
