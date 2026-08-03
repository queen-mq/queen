package workload

import (
	"math"
	"sync/atomic"
)

// slowDecodes counts payloads that missed the ordered fast path in DecodeStamp.
// A non-zero value on a broker that should round-trip bytes verbatim is a
// finding (something re-serialised the document), not just a slowdown.
var slowDecodes atomic.Int64

// SlowDecodes reports the fast-path miss count for the run.
func SlowDecodes() int64 { return slowDecodes.Load() }

// Counters is the run's application-level accounting. Every field is touched
// from many goroutines; read them only through the accessor methods.
type Counters struct {
	Offered   atomic.Int64 // events the pacer owed
	Shed      atomic.Int64 // events dropped before publish (backlog beyond catch-up)
	Published atomic.Int64 // ingress publishes that returned success
	Derived   atomic.Int64 // re-publishes from intermediate stages
	PushErr   atomic.Int64
	PushRetry atomic.Int64

	Delivered atomic.Int64 // messages handed to a stage handler (all 12 streams)
	Processed atomic.Int64 // messages a handler completed and recorded
	Acked     atomic.Int64
	AckErr    atomic.Int64

	latA latHist
	latB latHist
}

// ObserveE2E records an end-to-end latency for one terminal delivery. Only
// terminal stages should call this: intermediate hops would double-count.
func (c *Counters) ObserveE2E(f Flow, micros int64) {
	if f == FlowB {
		c.latB.observe(micros)
		return
	}
	c.latA.observe(micros)
}

// E2E returns p50/p95/p99 in milliseconds for one flow over the whole run.
func (c *Counters) E2E(f Flow) (p50, p95, p99 float64) {
	h := &c.latA
	if f == FlowB {
		h = &c.latB
	}
	return h.pct(0.50) / 1000, h.pct(0.95) / 1000, h.pct(0.99) / 1000
}

// ---------------------------------------------------------------------------
// lock-free log-bucketed histogram
// ---------------------------------------------------------------------------

// latHist buckets microsecond latencies at 8 sub-buckets per octave (~9%
// relative error), which is well inside what a p99 comparison needs and costs
// one atomic add per observation.
const (
	histBuckets  = 256
	histSubOctet = 8
)

type latHist struct {
	b [histBuckets]atomic.Int64
	n atomic.Int64
}

func (h *latHist) observe(micros int64) {
	if micros < 0 {
		micros = 0
	}
	h.b[histIdx(micros)].Add(1)
	h.n.Add(1)
}

func histIdx(micros int64) int {
	if micros < 1 {
		return 0
	}
	i := int(math.Log2(float64(micros)) * histSubOctet)
	if i < 0 {
		return 0
	}
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

// histValue is the representative (upper) latency of a bucket, in micros.
func histValue(i int) float64 { return math.Exp2(float64(i+1) / histSubOctet) }

func (h *latHist) pct(q float64) float64 {
	total := h.n.Load()
	if total == 0 {
		return 0
	}
	want := int64(math.Ceil(q * float64(total)))
	var cum int64
	for i := 0; i < histBuckets; i++ {
		cum += h.b[i].Load()
		if cum >= want {
			return histValue(i)
		}
	}
	return histValue(histBuckets - 1)
}

// StageCounters is the per-stream accounting the report and the cost table need.
type StageCounters struct {
	Topic     string
	Group     string
	Delivered atomic.Int64
	Processed atomic.Int64
	Acked     atomic.Int64
	AckErr    atomic.Int64
	Dups      atomic.Int64 // adapter-visible redeliveries, when the system reports them

	// Cycle-phase histograms (microseconds), filled by the adapter and the
	// stage runner. Together they decompose a consumer worker's cycle:
	// PopRTT (adapter pop call) + Barrier (lane wait + simulated work) +
	// Push (derived publish) + AckDisp (ack semaphore wait). What is left of
	// the cycle after these is loop overhead.
	PopRTT  latHist
	Barrier latHist
	Push    latHist
	AckDisp latHist

	// Age of each message at handler ENTRY, measured from the producer's
	// SCHEDULED instant (the same CO-corrected origin the e2e uses). Because
	// the stamp's ts is the origin for the WHOLE pipeline, per-stage age
	// percentiles decompose the end-to-end latency hop by hop: age(cm-db) is
	// the first hop's wait, age(ota-*) minus age(cm-db) is work+hop2, etc.
	Age latHist
}

// ObserveAge records one message's entry age in microseconds.
func (sc *StageCounters) ObserveAge(micros int64) { sc.Age.observe(micros) }

// Phase observers (microseconds).
func (sc *StageCounters) ObservePopRTT(us int64)  { sc.PopRTT.observe(us) }
func (sc *StageCounters) ObserveBarrier(us int64) { sc.Barrier.observe(us) }
func (sc *StageCounters) ObservePush(us int64)    { sc.Push.observe(us) }
func (sc *StageCounters) ObserveAckDisp(us int64) { sc.AckDisp.observe(us) }

// PhasePct returns (popRTT, barrier, push, ackDisp) at quantile q, in ms.
func (sc *StageCounters) PhasePct(q float64) (float64, float64, float64, float64) {
	return sc.PopRTT.pct(q) / 1000, sc.Barrier.pct(q) / 1000,
		sc.Push.pct(q) / 1000, sc.AckDisp.pct(q) / 1000
}

// AgePct returns the given age quantile in milliseconds.
func (sc *StageCounters) AgePct(q float64) float64 { return sc.Age.pct(q) / 1000 }
