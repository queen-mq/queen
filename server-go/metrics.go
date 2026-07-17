package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// opMetrics mirrors libqueen's PerTypeMetrics (lib/queen/metrics.hpp): per-op
// request/message counters plus batch-fusion counters and a bounded RTT ring
// (fire -> PG-completion) for percentiles. The fusion ratio is
// itemsFired/batchesFired = messages per stored-procedure call.
type opMetrics struct {
	name string

	requests       atomic.Uint64 // HTTP requests of this op
	messages       atomic.Uint64 // messages carried (push items / pop msgs / acks)
	empty          atomic.Uint64 // requests that returned 0 messages (pop)
	batchesFired   atomic.Uint64 // fused SP calls
	itemsFired     atomic.Uint64 // items merged across all fused calls
	completionsOK  atomic.Uint64
	completionsErr atomic.Uint64

	rttMu   sync.Mutex
	rttRing []float64
	rttHead int
	rttFull bool
}

const rttCap = 1024

func newOpMetrics(name string) *opMetrics {
	return &opMetrics{name: name, rttRing: make([]float64, rttCap)}
}

// recordRequest is called per HTTP request with the number of messages it
// carried (0 => counts as an empty pop).
func (m *opMetrics) recordRequest(msgs int) {
	m.requests.Add(1)
	if msgs > 0 {
		m.messages.Add(uint64(msgs))
	} else {
		m.empty.Add(1)
	}
}

// recordBatch is called per fused SP call from the batcher.
func (m *opMetrics) recordBatch(items int, ok bool, rtt time.Duration) {
	m.batchesFired.Add(1)
	m.itemsFired.Add(uint64(items))
	if ok {
		m.completionsOK.Add(1)
	} else {
		m.completionsErr.Add(1)
	}
	ms := float64(rtt.Microseconds()) / 1000.0
	m.rttMu.Lock()
	m.rttRing[m.rttHead] = ms
	m.rttHead = (m.rttHead + 1) % rttCap
	if m.rttHead == 0 {
		m.rttFull = true
	}
	m.rttMu.Unlock()
}

func (m *opMetrics) rttPercentile(p float64) float64 {
	m.rttMu.Lock()
	n := rttCap
	if !m.rttFull {
		n = m.rttHead
	}
	v := make([]float64, n)
	copy(v, m.rttRing[:n])
	m.rttMu.Unlock()
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	idx := int(p / 100.0 * float64(len(v)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(v) {
		idx = len(v) - 1
	}
	return v[idx]
}

type Metrics struct {
	Push  *opMetrics
	Pop   *opMetrics
	Ack   *opMetrics
	start time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		Push:  newOpMetrics("push"),
		Pop:   newOpMetrics("pop"),
		Ack:   newOpMetrics("ack"),
		start: time.Now(),
	}
}

// Prometheus renders a text-exposition body whose cluster counters line up 1:1
// with the C++ broker's /metrics/prometheus (queen_cluster_*_total), so the same
// scrape/diff computes req/s and msg/s for both. Adds fusion + RTT families.
func (m *Metrics) Prometheus() string {
	var b strings.Builder
	line := func(name, labels string, v interface{}) {
		b.WriteString(name)
		b.WriteString(labels)
		b.WriteByte(' ')
		fmt.Fprintf(&b, "%v", v)
		b.WriteByte('\n')
	}

	b.WriteString("# HELP queen_uptime_seconds Seconds since process start.\n# TYPE queen_uptime_seconds gauge\n")
	line("queen_uptime_seconds", "", int64(time.Since(m.start).Seconds()))

	b.WriteString("# HELP queen_process_resident_memory_bytes RSS of the broker process.\n# TYPE queen_process_resident_memory_bytes gauge\n")
	line("queen_process_resident_memory_bytes", "", residentMemoryBytes())

	b.WriteString("# HELP queen_go_goroutines Goroutines.\n# TYPE queen_go_goroutines gauge\n")
	line("queen_go_goroutines", "", runtime.NumGoroutine())

	ops := []*opMetrics{m.Push, m.Pop, m.Ack}

	b.WriteString("# HELP queen_cluster_push_requests_total Push HTTP requests.\n# TYPE queen_cluster_push_requests_total counter\n")
	line("queen_cluster_push_requests_total", `{scope="cluster"}`, m.Push.requests.Load())
	b.WriteString("# HELP queen_cluster_pop_requests_total Pop HTTP requests.\n# TYPE queen_cluster_pop_requests_total counter\n")
	line("queen_cluster_pop_requests_total", `{scope="cluster"}`, m.Pop.requests.Load())
	b.WriteString("# HELP queen_cluster_ack_requests_total Ack HTTP requests.\n# TYPE queen_cluster_ack_requests_total counter\n")
	line("queen_cluster_ack_requests_total", `{scope="cluster"}`, m.Ack.requests.Load())

	b.WriteString("# HELP queen_cluster_push_messages_total Messages pushed.\n# TYPE queen_cluster_push_messages_total counter\n")
	line("queen_cluster_push_messages_total", `{scope="cluster"}`, m.Push.messages.Load())
	b.WriteString("# HELP queen_cluster_pop_messages_total Messages popped.\n# TYPE queen_cluster_pop_messages_total counter\n")
	line("queen_cluster_pop_messages_total", `{scope="cluster"}`, m.Pop.messages.Load())
	b.WriteString("# HELP queen_cluster_ack_messages_total Ack attempts.\n# TYPE queen_cluster_ack_messages_total counter\n")
	line("queen_cluster_ack_messages_total", `{scope="cluster"}`, m.Ack.messages.Load())

	b.WriteString("# HELP queen_pop_empty_total Empty pop responses.\n# TYPE queen_pop_empty_total counter\n")
	line("queen_pop_empty_total", "", m.Pop.empty.Load())

	// Fusion + RTT, by op.
	b.WriteString("# HELP queen_batches_fired_total Fused stored-procedure calls, by op.\n# TYPE queen_batches_fired_total counter\n")
	for _, o := range ops {
		line("queen_batches_fired_total", `{op="`+o.name+`"}`, o.batchesFired.Load())
	}
	b.WriteString("# HELP queen_batch_items_fired_total Items merged into fused calls, by op.\n# TYPE queen_batch_items_fired_total counter\n")
	for _, o := range ops {
		line("queen_batch_items_fired_total", `{op="`+o.name+`"}`, o.itemsFired.Load())
	}
	b.WriteString("# HELP queen_fusion_items_per_batch Lifetime items per fused SP call (fusion ratio), by op.\n# TYPE queen_fusion_items_per_batch gauge\n")
	for _, o := range ops {
		bf := o.batchesFired.Load()
		ratio := 0.0
		if bf > 0 {
			ratio = float64(o.itemsFired.Load()) / float64(bf)
		}
		line("queen_fusion_items_per_batch", `{op="`+o.name+`"}`, strconv.FormatFloat(ratio, 'f', 2, 64))
	}
	b.WriteString("# HELP queen_batch_rtt_milliseconds Recent fire->PG-completion RTT, by op/quantile.\n# TYPE queen_batch_rtt_milliseconds gauge\n")
	for _, o := range ops {
		for _, q := range []struct {
			label string
			p     float64
		}{{"0.5", 50}, {"0.99", 99}} {
			line("queen_batch_rtt_milliseconds", `{op="`+o.name+`",quantile="`+q.label+`"}`,
				strconv.FormatFloat(o.rttPercentile(q.p), 'f', 3, 64))
		}
	}

	return b.String()
}

// residentMemoryBytes reads RSS from /proc (Linux; the benchmark box). Falls
// back to the Go runtime's resident estimate elsewhere (e.g. macOS dev).
func residentMemoryBytes() uint64 {
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 2 {
			if pages, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				return pages * uint64(os.Getpagesize())
			}
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}
