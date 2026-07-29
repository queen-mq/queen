package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// appCfg is the immutable knob set, passed by value to the pure helpers.
type appCfg struct {
	queue, stage2           string
	sessions                int
	skew                    float64
	pushBatch, producers    int
	popBatch, popPartitions int
	popTimeout              int
	popWait                 bool
	emptySleep              int
	processMs               int
	jitter                  float64
	slowMsgPct              float64
	slowMsgMs               int
	slowPartPct             float64
	slowPartMs              int
	failPct, poisonPct      float64
	txPct                   float64
	leaseTime               int
	renewLease              bool
	stage2ProcMs            int
	payloadMin, payloadMax  int
}

// ----- latency histogram (lock-free, atomic bucket counters) ---------------

var latBucketsMs = []float64{
	1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610,
	987, 1597, 2584, 4181, 6765, 10000, 20000, 60000,
}

type latHist struct{ counts []int64 }

func newLatHist() *latHist { return &latHist{counts: make([]int64, len(latBucketsMs)+1)} }

func (h *latHist) add(ms float64) {
	idx := sort.SearchFloat64s(latBucketsMs, ms)
	atomic.AddInt64(&h.counts[idx], 1)
}

func (h *latHist) snapshotReset() (n int64, p50, p99, p999 float64) {
	c := make([]int64, len(h.counts))
	var tot int64
	for i := range h.counts {
		c[i] = atomic.SwapInt64(&h.counts[i], 0)
		tot += c[i]
	}
	if tot == 0 {
		return 0, 0, 0, 0
	}
	pick := func(p float64) float64 {
		target := int64(math.Ceil(p * float64(tot)))
		var cum int64
		for i, v := range c {
			cum += v
			if cum >= target {
				if i < len(latBucketsMs) {
					return latBucketsMs[i]
				}
				return latBucketsMs[len(latBucketsMs)-1] * 1.5
			}
		}
		return latBucketsMs[len(latBucketsMs)-1]
	}
	return tot, pick(0.50), pick(0.99), pick(0.999)
}

// ----- online order checker ------------------------------------------------

// orderChecker verifies, per (consumer-group, partition), that first deliveries
// arrive in push order. With single-writer producers, per-partition seq is a
// contiguous 0,1,2,..; per-partition FIFO delivery within a CG (one lease at a
// time) means we should see exactly that order. We track only the next expected
// seq per key (low memory): seq==expected advances; seq<expected is a
// redelivery (OK under at-least-once); seq>expected is a skip/reorder/loss and
// is flagged once (then we resync). This is the signature the 0.16 cursor-skip
// fix targets. The offline verifier does the rigorous cross-check vs the
// producer manifest.
type partCheck struct {
	mu       sync.Mutex
	expected int64
}

type orderChecker struct {
	m        sync.Map // "cg:partition" -> *partCheck
	inOrder  int64
	dups     int64
	viol     int64
	sampleMu sync.Mutex
	samples  []string
}

func (oc *orderChecker) observe(cg, part string, seq, retry int64) {
	v, _ := oc.m.LoadOrStore(cg+":"+part, &partCheck{})
	pc := v.(*partCheck)
	pc.mu.Lock()
	switch {
	case seq == pc.expected:
		pc.expected++
		atomic.AddInt64(&oc.inOrder, 1)
	case seq < pc.expected:
		atomic.AddInt64(&oc.dups, 1)
	default: // seq > expected: skip / reorder / loss
		atomic.AddInt64(&oc.viol, 1)
		oc.sampleMu.Lock()
		if len(oc.samples) < 20 {
			oc.samples = append(oc.samples, fmt.Sprintf("%s part=%s expected=%d got=%d gap=%d retry=%d", cg, part, pc.expected, seq, seq-pc.expected, retry))
		}
		oc.sampleMu.Unlock()
		pc.expected = seq + 1
	}
	pc.mu.Unlock()
}

// ----- raw verification log (one writer goroutine per consumer group) -------

type logRec struct {
	tsNs   int64
	cg     string
	part   string
	seq    int64
	retry  int64
	status string
}

type logWriter struct {
	ch   chan logRec
	done chan struct{}
}

func newLogWriter(path string) (*logWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	lw := &logWriter{ch: make(chan logRec, 1<<16), done: make(chan struct{})}
	go func() {
		bw := bufio.NewWriterSize(f, 1<<20)
		defer func() {
			_ = bw.Flush()
			_ = f.Close()
			close(lw.done)
		}()
		// header
		_, _ = bw.WriteString("ackTsNs\tcg\tpartition\tseq\tretry\tstatus\n")
		for r := range lw.ch {
			bw.WriteString(strconv.FormatInt(r.tsNs, 10))
			bw.WriteByte('\t')
			bw.WriteString(r.cg)
			bw.WriteByte('\t')
			bw.WriteString(r.part)
			bw.WriteByte('\t')
			bw.WriteString(strconv.FormatInt(r.seq, 10))
			bw.WriteByte('\t')
			bw.WriteString(strconv.FormatInt(r.retry, 10))
			bw.WriteByte('\t')
			bw.WriteString(r.status)
			bw.WriteByte('\n')
		}
	}()
	return lw, nil
}

// write applies backpressure (blocks) rather than dropping, so the log is a
// complete record for the offline verifier. Run -verify-log at a moderate rate.
func (lw *logWriter) write(r logRec) {
	if lw == nil {
		return
	}
	lw.ch <- r
}

func (lw *logWriter) close() {
	if lw == nil {
		return
	}
	close(lw.ch)
	<-lw.done
}

// ----- pure helpers --------------------------------------------------------

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func boolField(m *queen.Message, k string) bool {
	if m.Data == nil {
		return false
	}
	v, _ := m.Data[k].(bool)
	return v
}

func pushTsOf(m *queen.Message) (int64, bool) {
	if m.Data == nil {
		return 0, false
	}
	switch v := m.Data["pushTs"].(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	}
	return 0, false
}

func seqOf(m *queen.Message) int64 {
	if m.Data == nil {
		return -1
	}
	switch v := m.Data["seq"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	}
	return -1
}

func isSlowPartition(part string, pct float64) bool {
	if pct <= 0 {
		return false
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(part))
	return float64(h.Sum32()%100000)/1000.0 < pct
}

func procForMsg(m *queen.Message, rng *rand.Rand, cfg appCfg) time.Duration {
	ms := float64(cfg.processMs) * (1 + (rng.Float64()*2-1)*cfg.jitter)
	if boolField(m, "slow") && float64(cfg.slowMsgMs) > ms {
		ms = float64(cfg.slowMsgMs)
	}
	if isSlowPartition(m.Partition, cfg.slowPartPct) && float64(cfg.slowPartMs) > ms {
		ms = float64(cfg.slowPartMs)
	}
	if ms < 0 {
		ms = 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// buildPayload returns the JSON payload plus its (poison, slow) flags so the
// producer can record poison in the manifest.
func buildPayload(rng *rand.Rand, cfg appCfg, base string, seq int64) (map[string]interface{}, bool, bool) {
	n := cfg.payloadMin
	if cfg.payloadMax > cfg.payloadMin {
		n += rng.Intn(cfg.payloadMax - cfg.payloadMin + 1)
	}
	if n > len(base) {
		n = len(base)
	}
	poison := rng.Float64()*100 < cfg.poisonPct
	slow := rng.Float64()*100 < cfg.slowMsgPct
	return map[string]interface{}{
		"pushTs": time.Now().UnixNano(),
		"seq":    seq,
		"poison": poison,
		"slow":   slow,
		"data":   base[:n],
		"src":    "appload",
	}, poison, slow
}

func makeZipfN(rng *rand.Rand, skew float64, n int) *rand.Zipf {
	if skew <= 1.0 || n < 2 {
		return nil
	}
	return rand.NewZipf(rng, skew, 1.0, uint64(n-1))
}

// ownedRange gives producer p its exclusive slice of the session space, so each
// session/partition has exactly one writer (a precondition for a per-partition
// monotonic seq and a meaningful order check).
func ownedRange(producerID, producers, sessions int) (start, n int) {
	if producers <= 0 {
		producers = 1
	}
	if sessions < 1 {
		sessions = 1
	}
	chunk := sessions / producers
	if chunk < 1 { // producers > sessions: fall back to shared (warned at startup)
		return producerID % sessions, 1
	}
	start = producerID * chunk
	n = chunk
	if producerID == producers-1 {
		n = sessions - start
	}
	return start, n
}

type partStat struct{ count, maxSeq, poison int64 }

func startRenewer(ctx context.Context, q *queen.Queen, msgs []*queen.Message, interval time.Duration, counter *int64) func() {
	if interval <= 0 {
		return func() {}
	}
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = q.Renew(ctx, msgs)
				atomic.AddInt64(counter, 1)
			}
		}
	}()
	return func() { close(stop) }
}

// ----- app mode ------------------------------------------------------------

func runAppMode(args []string) {
	fs := flag.NewFlagSet("goload-app", flag.ExitOnError)
	url_ := fs.String("url", "http://127.0.0.1:6632", "broker base URL")
	queueName := fs.String("queue", "appq", "stage-1 queue name")
	stage2 := fs.String("stage2-queue", "appq-stage2", "stage-2 queue for transactional ack+push")
	targetRate := fs.Int("target-rate", 20000, "total push msgs/s (closed loop); 0 = open loop (push as fast as possible)")
	producers := fs.Int("producers", 200, "producer goroutines (each OWNS a disjoint slice of sessions: single-writer per partition)")
	pushBatch := fs.Int("push-batch", 10, "messages per push (one session/partition per batch)")
	sessions := fs.Int("sessions", 5000, "size of the live session/partition working set")
	skew := fs.Float64("skew", 1.1, "Zipf exponent for session selection (<=1 = uniform; ~1.1 = hot/cold)")
	churn := fs.Float64("session-churn", 0.0, "fraction of the session set rotated per minute (new partitions appear, old go idle)")
	groups := fs.Int("consumer-groups", 2, "number of consumer groups (fan-out: each gets a full copy)")
	consPerGroup := fs.Int("consumers-per-group", 0, "consumer goroutines per group (0 = auto-size from rate x processing)")
	popBatch := fs.Int("pop-batch", 50, "max messages per pop")
	popPartitions := fs.Int("pop-partitions", 10, "v4 multi-partition pop: gather up to pop-batch msgs across up to N partitions")
	popWait := fs.Bool("pop-wait", true, "long-poll pop (park empty pops server-side)")
	popTimeout := fs.Int("pop-timeout", 1000, "pop long-poll timeout ms")
	emptySleep := fs.Int("empty-sleep", 5, "consumer sleep ms on empty pop (only when -pop-wait=false)")
	processMs := fs.Int("process-ms", 20, "median per-message processing time (simulated work)")
	jitter := fs.Float64("process-jitter", 0.5, "+/- jitter fraction on processing time (0..1)")
	slowMsgPct := fs.Float64("slow-msg-pct", 2.0, "percent of messages that take -slow-msg-ms (heavy tail)")
	slowMsgMs := fs.Int("slow-msg-ms", 500, "processing time for slow messages")
	slowPartPct := fs.Float64("slow-part-pct", 0.5, "percent of partitions that are 'stuck' (head-of-line)")
	slowPartMs := fs.Int("slow-part-ms", 3000, "processing time on stuck partitions")
	failPct := fs.Float64("fail-pct", 2.0, "percent of deliveries acked as failed (transient -> redelivery)")
	poisonPct := fs.Float64("poison-pct", 0.2, "percent of messages that ALWAYS fail (-> DLQ after retry-limit)")
	retryLimit := fs.Int("retry-limit", 5, "queue retryLimit (deliveries before a message moves to DLQ)")
	txPct := fs.Float64("tx-pct", 10.0, "percent of primary-group completions done as a transactional ack+push to stage-2")
	leaseTime := fs.Int("lease-time", 30, "queue leaseTime seconds")
	renewLease := fs.Bool("renew-lease", true, "renew the lease while processing slow batches")
	stage2ProcMs := fs.Int("stage2-process-ms", 5, "processing time for the stage-2 drain")
	stage2Consumers := fs.Int("stage2-consumers", 40, "consumer goroutines draining stage-2")
	payloadMin := fs.Int("payload-min", 512, "min payload bytes")
	payloadMax := fs.Int("payload-max", 4096, "max payload bytes")
	completedRet := fs.Int("completed-retention", 300, "completedRetentionSeconds")
	pendingRet := fs.Int("pending-retention", 7200, "retentionSeconds for pending msgs (0 = forever)")
	burstMult := fs.Float64("burst-mult", 1.0, "burst rate multiplier (1 = no burst)")
	burstEvery := fs.Duration("burst-every", 30*time.Minute, "time between bursts")
	burstFor := fs.Duration("burst-for", 5*time.Minute, "burst duration")
	verifyLog := fs.String("verify-log", "", "if set, write per-CG TSV consumption logs + manifest.tsv to this dir (for offline order/loss verification)")
	reset := fs.Bool("reset", false, "DELETE the stage-1/stage-2 queues before starting (clean seq space; recommended for -verify-log runs)")
	producerSeconds := fs.Int("producer-seconds", 0, "stop producers after N seconds, then let consumers drain (0 = producers run for the whole -duration). Use with -verify-log for a clean zero-loss/total-order proof (short=0)")
	drainQuiet := fs.Int("drain-quiet", 15, "with -producer-seconds>0: end the run after this many seconds of no consume progress (backlog fully drained, incl. poison->DLQ)")
	idleConns := fs.Int("idle-conns", 1024, "MaxIdleConnsPerHost")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	reportSec := fs.Int("report", 5, "report interval seconds")
	durationSec := fs.Int("duration", 0, "run duration seconds (0 = until SIGINT)")
	retries := fs.Int("retries", 2, "client RetryAttempts")
	_ = fs.String("mode", "app", "run mode: max | app")
	_ = fs.Parse(args)

	cfg := appCfg{
		queue: *queueName, stage2: *stage2,
		sessions: *sessions, skew: *skew,
		pushBatch: *pushBatch, producers: *producers,
		popBatch: *popBatch, popPartitions: *popPartitions, popTimeout: *popTimeout, popWait: *popWait,
		emptySleep: *emptySleep,
		processMs:  *processMs, jitter: *jitter,
		slowMsgPct: *slowMsgPct, slowMsgMs: *slowMsgMs,
		slowPartPct: *slowPartPct, slowPartMs: *slowPartMs,
		failPct: *failPct, poisonPct: *poisonPct, txPct: *txPct,
		leaseTime: *leaseTime, renewLease: *renewLease,
		stage2ProcMs: *stage2ProcMs,
		payloadMin:   *payloadMin, payloadMax: *payloadMax,
	}

	rateForSizing := *targetRate
	if rateForSizing <= 0 {
		rateForSizing = 50000
	}
	if *consPerGroup <= 0 {
		mp := (1-cfg.slowMsgPct/100)*float64(cfg.processMs) +
			(cfg.slowMsgPct/100)*float64(cfg.slowMsgMs) +
			(cfg.slowPartPct/100)*float64(cfg.slowPartMs)
		if mp < 1 {
			mp = 1
		}
		c := int(math.Ceil(float64(rateForSizing) * (mp / 1000.0) * 1.5))
		if c < 16 {
			c = 16
		}
		if c > 5000 {
			c = 5000
		}
		*consPerGroup = c
	}

	fmt.Printf("goload -mode app -> %s\n", *url_)
	fmt.Printf("  load:    target=%d msg/s  producers=%d (single-writer/session)  pushBatch=%d  payload=%d-%dB\n",
		*targetRate, cfg.producers, cfg.pushBatch, cfg.payloadMin, cfg.payloadMax)
	fmt.Printf("  topo:    queue=%s  sessions=%d  skew=%.2f  churn=%.2f/min\n",
		cfg.queue, cfg.sessions, cfg.skew, *churn)
	fmt.Printf("  consume: groups=%d  consumers/group=%d  popBatch=%d  popPartitions=%d  wait=%v\n",
		*groups, *consPerGroup, cfg.popBatch, cfg.popPartitions, cfg.popWait)
	fmt.Printf("  work:    process=%dms +/-%.0f%%  slowMsg=%.1f%%@%dms  slowPart=%.1f%%@%dms\n",
		cfg.processMs, cfg.jitter*100, cfg.slowMsgPct, cfg.slowMsgMs, cfg.slowPartPct, cfg.slowPartMs)
	fmt.Printf("  reliab:  fail=%.1f%%  poison=%.1f%%  retryLimit=%d  lease=%ds renew=%v\n",
		cfg.failPct, cfg.poisonPct, *retryLimit, cfg.leaseTime, cfg.renewLease)
	fmt.Printf("  pipeline: tx=%.1f%% of primary-group -> %s (drained by %d consumers @%dms)\n",
		cfg.txPct, cfg.stage2, *stage2Consumers, cfg.stage2ProcMs)
	if *verifyLog != "" {
		fmt.Printf("  verify:  raw logs -> %s  reset=%v\n", *verifyLog, *reset)
	}
	if *producerSeconds > 0 {
		fmt.Printf("  drain:   producers stop at %ds, then drain until quiet for %ds (clean zero-loss proof)\n", *producerSeconds, *drainQuiet)
	}
	if cfg.producers > cfg.sessions {
		fmt.Printf("  WARNING: producers(%d) > sessions(%d) -> some partitions share a writer; per-partition seq/order check is unreliable. Use sessions >= producers.\n", cfg.producers, cfg.sessions)
	}
	if *burstMult > 1 {
		fmt.Printf("  burst:   x%.1f for %s every %s\n", *burstMult, *burstFor, *burstEvery)
	}

	retryAttempts := *retries
	if retryAttempts <= 0 {
		retryAttempts = -1
	}
	q, err := queen.New(queen.ClientConfig{
		URL:                 *url_,
		TimeoutMillis:       *timeoutMs,
		MaxIdleConnsPerHost: *idleConns,
		RetryAttempts:       retryAttempts,
	})
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *reset {
		for _, name := range []string{cfg.queue, cfg.stage2} {
			dctx, dc := context.WithTimeout(ctx, 10*time.Second)
			_, _ = q.GetHttpClient().Delete(dctx, "/api/v1/resources/queues/"+url.PathEscape(name))
			dc()
		}
		fmt.Printf("[reset] deleted %s, %s\n", cfg.queue, cfg.stage2)
	}

	configure := func(name string, pending int) {
		cctx, cc := context.WithTimeout(ctx, 10*time.Second)
		defer cc()
		_, e := q.GetHttpClient().Post(cctx, "/api/v1/configure", map[string]interface{}{
			"queue": name,
			"options": map[string]interface{}{
				"retentionEnabled":          true,
				"completedRetentionSeconds": *completedRet,
				"retentionSeconds":          pending,
				"leaseTime":                 cfg.leaseTime,
				"retryLimit":                *retryLimit,
			},
		})
		if e != nil {
			fmt.Printf("[configure] WARNING %s: %v\n", name, e)
		} else {
			fmt.Printf("[configure] %s leaseTime=%ds retryLimit=%d completedRet=%ds pendingRet=%ds\n",
				name, cfg.leaseTime, *retryLimit, *completedRet, pending)
		}
	}
	configure(cfg.queue, *pendingRet)
	configure(cfg.stage2, *pendingRet)

	base := strings.Repeat("x", cfg.payloadMax)

	// verification log writers (one per stage-1 CG) + producer manifest
	var writers []*logWriter
	var manifestFile *os.File
	var manifestMu sync.Mutex
	if *verifyLog != "" {
		if err := os.MkdirAll(*verifyLog, 0o755); err != nil {
			fmt.Printf("verify-log mkdir failed: %v\n", err)
			os.Exit(1)
		}
		writers = make([]*logWriter, *groups)
		for g := 0; g < *groups; g++ {
			lw, werr := newLogWriter(fmt.Sprintf("%s/cg%d.tsv", *verifyLog, g))
			if werr != nil {
				fmt.Printf("verify-log open failed: %v\n", werr)
				os.Exit(1)
			}
			writers[g] = lw
		}
		mf, merr := os.Create(*verifyLog + "/manifest.tsv")
		if merr != nil {
			fmt.Printf("manifest open failed: %v\n", merr)
			os.Exit(1)
		}
		manifestFile = mf
		_, _ = manifestFile.WriteString("partition\tcount\tmaxSeq\tpoison\n")
	}

	var (
		pushed, pushDup, pushErr  int64
		popErr, ackErr, emptyPops int64
		redeliveries              int64
		txCommits, txErr          int64
		stage2Pushed, stage2Acked int64
		stage2PopErr, leaseRenews int64
		baseSession               int64
	)
	poppedG := make([]int64, *groups)
	completedG := make([]int64, *groups)
	failedG := make([]int64, *groups)
	qDelay := newLatHist()
	e2e := newLatHist()
	checker := &orderChecker{}

	var curRate int64 = int64(*targetRate)

	sumSlice := func(s []int64) int64 {
		var v int64
		for i := range s {
			v += atomic.LoadInt64(&s[i])
		}
		return v
	}
	// Producers run under their own context so they can be stopped early
	// (-producer-seconds) while consumers keep draining the backlog.
	producerCtx, producerCancel := context.WithCancel(ctx)
	defer producerCancel()

	var wg sync.WaitGroup

	// ----- consumers (group 0 is the tx-forwarding primary)
	consumerLoop := func(gi int, cg string, primary bool) {
		defer wg.Done()
		var lw *logWriter
		if writers != nil {
			lw = writers[gi]
		}
		rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(gi*1009+7)))
		renewEvery := time.Duration(cfg.leaseTime) * time.Second / 3
		renewFloor := time.Duration(cfg.leaseTime) * time.Second / 2
		for ctx.Err() == nil {
			qb := q.Queue(cfg.queue).Group(cg).AutoAck(false).Batch(cfg.popBatch)
			if cfg.popPartitions > 1 {
				qb = qb.Partitions(cfg.popPartitions)
			}
			if cfg.popWait {
				qb = qb.Wait(true).TimeoutMillis(cfg.popTimeout)
			} else {
				qb = qb.Wait(false)
			}
			msgs, e := qb.Pop(ctx)
			if e != nil {
				if ctx.Err() != nil {
					return
				}
				atomic.AddInt64(&popErr, 1)
				sleepCtx(ctx, 5*time.Millisecond)
				continue
			}
			if len(msgs) == 0 {
				atomic.AddInt64(&emptyPops, 1)
				if !cfg.popWait {
					sleepCtx(ctx, time.Duration(cfg.emptySleep)*time.Millisecond)
				}
				continue
			}
			atomic.AddInt64(&poppedG[gi], int64(len(msgs)))
			now := time.Now().UnixNano()
			for _, m := range msgs {
				if ts, ok := pushTsOf(m); ok {
					qDelay.add(float64(now-ts) / 1e6)
				}
				if m.RetryCount > 0 {
					atomic.AddInt64(&redeliveries, 1)
				}
				if s := seqOf(m); s >= 0 {
					checker.observe(cg, m.Partition, s, int64(m.RetryCount))
				}
			}

			procs := make([]time.Duration, len(msgs))
			var maxProc time.Duration
			for i, m := range msgs {
				procs[i] = procForMsg(m, rng, cfg)
				if procs[i] > maxProc {
					maxProc = procs[i]
				}
			}
			stopRenew := func() {}
			if cfg.renewLease && maxProc >= renewFloor {
				stopRenew = startRenewer(ctx, q, msgs, renewEvery, &leaseRenews)
			}
			var completed, failed []*queen.Message
			for i, m := range msgs {
				sleepCtx(ctx, procs[i])
				poison := boolField(m, "poison")
				fail := poison || rng.Float64()*100 < cfg.failPct
				if fail {
					failed = append(failed, m)
				} else {
					completed = append(completed, m)
				}
				if lw != nil {
					st := "ok"
					if fail {
						if poison {
							st = "poison"
						} else {
							st = "fail"
						}
					}
					lw.write(logRec{tsNs: time.Now().UnixNano(), cg: cg, part: m.Partition, seq: seqOf(m), retry: int64(m.RetryCount), status: st})
				}
			}
			stopRenew()
			ackNow := time.Now().UnixNano()
			recordE2E := func(m *queen.Message) {
				if ts, ok := pushTsOf(m); ok {
					e2e.add(float64(ackNow-ts) / 1e6)
				}
			}

			if primary && cfg.txPct > 0 && len(completed) > 0 {
				plain := completed[:0:0]
				for _, m := range completed {
					if rng.Float64()*100 >= cfg.txPct {
						plain = append(plain, m)
						continue
					}
					out := []interface{}{map[string]interface{}{
						"pushTs": time.Now().UnixNano(),
						"src":    "stage1->stage2",
						"data":   base[:minInt(64, len(base))],
					}}
					resp, terr := q.Transaction().
						Ack(m, "completed", queen.AckOptions{ConsumerGroup: cg}).
						Queue(cfg.stage2).Partition(m.Partition).Push(out).
						Commit(ctx)
					if terr != nil || (resp != nil && !resp.Success) {
						if ctx.Err() != nil {
							return
						}
						atomic.AddInt64(&txErr, 1)
						plain = append(plain, m)
						continue
					}
					atomic.AddInt64(&txCommits, 1)
					atomic.AddInt64(&stage2Pushed, 1)
					atomic.AddInt64(&completedG[gi], 1)
					recordE2E(m)
				}
				completed = plain
			}

			if len(completed) > 0 {
				if _, aerr := q.Ack(ctx, completed, true, queen.AckOptions{ConsumerGroup: cg}); aerr != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&ackErr, 1)
				} else {
					atomic.AddInt64(&completedG[gi], int64(len(completed)))
					for _, m := range completed {
						recordE2E(m)
					}
				}
			}
			if len(failed) > 0 {
				if _, aerr := q.Ack(ctx, failed, false, queen.AckOptions{ConsumerGroup: cg}); aerr != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&ackErr, 1)
				} else {
					atomic.AddInt64(&failedG[gi], int64(len(failed)))
				}
			}
		}
	}
	for g := 0; g < *groups; g++ {
		cg := fmt.Sprintf("cg%d", g)
		for c := 0; c < *consPerGroup; c++ {
			wg.Add(1)
			go consumerLoop(g, cg, g == 0)
		}
	}

	// ----- stage-2 drain
	for c := 0; c < *stage2Consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := q.Queue(cfg.stage2).Group("stage2").AutoAck(false).Batch(cfg.popBatch).Wait(true).TimeoutMillis(cfg.popTimeout)
				if cfg.popPartitions > 1 {
					qb = qb.Partitions(cfg.popPartitions)
				}
				msgs, e := qb.Pop(ctx)
				if e != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&stage2PopErr, 1)
					sleepCtx(ctx, 5*time.Millisecond)
					continue
				}
				if len(msgs) == 0 {
					continue
				}
				sleepCtx(ctx, time.Duration(cfg.stage2ProcMs)*time.Millisecond)
				if _, aerr := q.Ack(ctx, msgs, true, queen.AckOptions{ConsumerGroup: "stage2"}); aerr == nil {
					atomic.AddInt64(&stage2Acked, int64(len(msgs)))
				} else if ctx.Err() == nil {
					atomic.AddInt64(&ackErr, 1)
				}
			}
		}()
	}

	// ----- producers (single-writer per session, monotonic seq, closed-loop pacing)
	for i := 0; i < cfg.producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(id)*2654435761))
			start, n := ownedRange(id, cfg.producers, cfg.sessions)
			z := makeZipfN(rng, cfg.skew, n)
			seqByPart := make(map[int64]int64)
			var manifest map[int64]*partStat
			if manifestFile != nil {
				manifest = make(map[int64]*partStat)
			}
			payloads := make([]interface{}, cfg.pushBatch)
			next := time.Now()
			for producerCtx.Err() == nil {
				local := 0
				if z != nil {
					local = int(z.Uint64())
				} else if n > 1 {
					local = rng.Intn(n)
				}
				numeric := atomic.LoadInt64(&baseSession) + int64(start+local)
				part := "s" + strconv.FormatInt(numeric, 10)
				for j := range payloads {
					seq := seqByPart[numeric]
					seqByPart[numeric] = seq + 1
					pl, poison, _ := buildPayload(rng, cfg, base, seq)
					payloads[j] = pl
					if manifest != nil {
						st := manifest[numeric]
						if st == nil {
							st = &partStat{}
							manifest[numeric] = st
						}
						st.count++
						if seq > st.maxSeq {
							st.maxSeq = seq
						}
						if poison {
							st.poison++
						}
					}
				}
				resp, e := q.Queue(cfg.queue).Partition(part).Push(payloads).Execute(ctx)
				if e != nil {
					if ctx.Err() != nil {
						break
					}
					atomic.AddInt64(&pushErr, 1)
					// roll back the seq we reserved so the manifest/order space
					// stays contiguous (the messages were not accepted).
					seqByPart[numeric] -= int64(cfg.pushBatch)
					if manifest != nil {
						if st := manifest[numeric]; st != nil {
							st.count -= int64(cfg.pushBatch)
							st.maxSeq -= int64(cfg.pushBatch)
						}
					}
					sleepCtx(ctx, 5*time.Millisecond)
					continue
				}
				atomic.AddInt64(&pushed, int64(cfg.pushBatch))
				for _, r := range resp {
					if r.Status == "duplicate" {
						atomic.AddInt64(&pushDup, 1)
					}
				}
				rate := float64(atomic.LoadInt64(&curRate))
				if rate <= 0 {
					next = time.Now()
					continue
				}
				perProd := rate / float64(cfg.producers)
				interval := time.Duration(float64(cfg.pushBatch) / perProd * float64(time.Second))
				next = next.Add(interval)
				if d := time.Until(next); d > 0 {
					sleepCtx(ctx, d)
				} else if d < -2*time.Second {
					next = time.Now()
				}
			}
			if manifest != nil {
				manifestMu.Lock()
				for k, st := range manifest {
					if st.count <= 0 {
						continue
					}
					fmt.Fprintf(manifestFile, "s%d\t%d\t%d\t%d\n", k, st.count, st.maxSeq, st.poison)
				}
				manifestMu.Unlock()
			}
		}(i)
	}

	if *churn > 0 {
		go func() {
			step := int64(math.Max(1, *churn*float64(cfg.sessions)))
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					atomic.AddInt64(&baseSession, step)
				}
			}
		}()
	}

	if *burstMult > 1 && *targetRate > 0 {
		go func() {
			for ctx.Err() == nil {
				sleepCtx(ctx, *burstEvery)
				if ctx.Err() != nil {
					return
				}
				atomic.StoreInt64(&curRate, int64(float64(*targetRate)**burstMult))
				fmt.Printf("[%s] BURST x%.1f -> %d/s for %s\n", tsNow(), *burstMult, int64(float64(*targetRate)**burstMult), *burstFor)
				sleepCtx(ctx, *burstFor)
				atomic.StoreInt64(&curRate, int64(*targetRate))
				fmt.Printf("[%s] burst end -> %d/s\n", tsNow(), *targetRate)
			}
		}()
	}

	// ----- producer stop + drain-until-quiet (clean zero-loss / total-order proof)
	if *producerSeconds > 0 {
		go func() {
			sleepCtx(ctx, time.Duration(*producerSeconds)*time.Second)
			if ctx.Err() != nil {
				return
			}
			producerCancel()
			fmt.Printf("[%s] producers stopped after %ds; draining backlog...\n", tsNow(), *producerSeconds)
			if *drainQuiet <= 0 {
				return
			}
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			var last int64
			var flat time.Duration
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					prog := sumSlice(completedG) + sumSlice(failedG) + atomic.LoadInt64(&stage2Acked)
					if prog == last {
						flat += 2 * time.Second
						if flat >= time.Duration(*drainQuiet)*time.Second {
							fmt.Printf("[%s] drained (no consume progress for %ds) -> stopping\n", tsNow(), *drainQuiet)
							cancel()
							return
						}
					} else {
						flat = 0
						last = prog
					}
				}
			}
		}()
	}

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer t.Stop()
		var lPush, lPop, lComp, lFail, lTx, lS2 int64
		secs := float64(*reportSec)
		sum := func(s []int64) int64 {
			var v int64
			for i := range s {
				v += atomic.LoadInt64(&s[i])
			}
			return v
		}
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				p := atomic.LoadInt64(&pushed)
				pop := sum(poppedG)
				comp := sum(completedG)
				fail := sum(failedG)
				tx := atomic.LoadInt64(&txCommits)
				s2 := atomic.LoadInt64(&stage2Acked)
				qn, q50, q99, q999 := qDelay.snapshotReset()
				_, e50, e99, e999 := e2e.snapshotReset()
				fmt.Printf("[%s] push=%6.0f/s pop=%6.0f/s done=%6.0f/s fail=%5.0f/s tx=%5.0f/s s2=%5.0f/s | qDelay ms p50=%.0f p99=%.0f p999=%.0f | e2e ms p50=%.0f p99=%.0f p999=%.0f | inflight=%d redeliv=%d viol=%d empty=%d errs p=%d c=%d ack=%d tx=%d (n=%d)\n",
					tsNow(),
					float64(p-lPush)/secs, float64(pop-lPop)/secs, float64(comp-lComp)/secs,
					float64(fail-lFail)/secs, float64(tx-lTx)/secs, float64(s2-lS2)/secs,
					q50, q99, q999, e50, e99, e999,
					p*int64(*groups)-comp,
					atomic.LoadInt64(&redeliveries), atomic.LoadInt64(&checker.viol), atomic.LoadInt64(&emptyPops),
					atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&ackErr), atomic.LoadInt64(&txErr),
					qn)
				lPush, lPop, lComp, lFail, lTx, lS2 = p, pop, comp, fail, tx, s2
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if *durationSec > 0 {
		go func() { sleepCtx(ctx, time.Duration(*durationSec)*time.Second); cancel() }()
	}
	select {
	case <-sigCh:
		fmt.Println("\n[signal] stopping...")
		cancel()
	case <-ctx.Done():
	}
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	for _, lw := range writers {
		lw.close()
	}
	if manifestFile != nil {
		_ = manifestFile.Close()
	}
	_ = q.Close(context.Background())

	fmt.Printf("\n[final] pushed=%d (dup=%d, pushErr=%d)\n", atomic.LoadInt64(&pushed), atomic.LoadInt64(&pushDup), atomic.LoadInt64(&pushErr))
	for g := 0; g < *groups; g++ {
		fmt.Printf("        cg%d: popped=%d completed=%d failed=%d\n",
			g, atomic.LoadInt64(&poppedG[g]), atomic.LoadInt64(&completedG[g]), atomic.LoadInt64(&failedG[g]))
	}
	fmt.Printf("        stage2: pushed=%d acked=%d (popErr=%d)\n", atomic.LoadInt64(&stage2Pushed), atomic.LoadInt64(&stage2Acked), atomic.LoadInt64(&stage2PopErr))
	fmt.Printf("        txCommits=%d txErr=%d redeliveries=%d leaseRenews=%d popErr=%d ackErr=%d emptyPops=%d\n",
		atomic.LoadInt64(&txCommits), atomic.LoadInt64(&txErr), atomic.LoadInt64(&redeliveries),
		atomic.LoadInt64(&leaseRenews), atomic.LoadInt64(&popErr), atomic.LoadInt64(&ackErr), atomic.LoadInt64(&emptyPops))
	fmt.Printf("        order: inOrder=%d redeliv(seq<exp)=%d VIOLATIONS(seq>exp)=%d\n",
		atomic.LoadInt64(&checker.inOrder), atomic.LoadInt64(&checker.dups), atomic.LoadInt64(&checker.viol))
	if atomic.LoadInt64(&checker.viol) > 0 {
		checker.sampleMu.Lock()
		for _, s := range checker.samples {
			fmt.Printf("          ! %s\n", s)
		}
		checker.sampleMu.Unlock()
	}
	if *verifyLog != "" {
		fmt.Printf("        raw logs in %s (cg*.tsv, manifest.tsv). Verify: python3 verify-order.py %s\n", *verifyLog, *verifyLog)
	}
	fmt.Printf("        DLQ (poison) is visible via the dashboard or `queenctl dlq peek %s`.\n", cfg.queue)
}

func tsNow() string { return time.Now().UTC().Format("15:04:05") }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
