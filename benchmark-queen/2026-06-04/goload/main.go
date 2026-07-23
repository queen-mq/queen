// goload — a load generator for Queen MQ built on the official Go client
// (github.com/smartpricing/queen/clients/client-go).
//
// Two modes (select with -mode):
//
//	-mode max  (default)  Pure broker in/out throughput. Many producer
//	                      goroutines (batched push) and consumer goroutines
//	                      (pop with server-side autoAck), round-robin across N
//	                      partitions. Reports push/pop msg/s. This is the
//	                      "max pipe" test.
//
//	-mode app             Realistic application workload: closed-loop target
//	                      rate, explicit ack with simulated per-message
//	                      processing time, N consumer groups (fan-out),
//	                      key-skewed partitions, head-of-line slow partitions,
//	                      failure -> retry -> DLQ, transactional ack+push
//	                      pipeline, and end-to-end latency percentiles.
//	                      Run `goload -mode app -h` for its flags.
//
//	-mode openloop        Open-loop, paced offered load. Producers are a fixed
//	                      SCHEDULE (not a closed-loop worker pool): the pacer
//	                      offers -rate msg/s regardless of how fast the broker
//	                      responds, launching each push at its scheduled instant
//	                      in its own goroutine. In-flight push REQUESTS are capped
//	                      at -max-inflight; over the cap, a request's messages are
//	                      SHED (counted, not sent) so the pacer never blocks and
//	                      never silently degenerates into closed-loop. Latency is
//	                      coordinated-omission-correct (measured from each
//	                      request's SCHEDULED time, not its actual send time).
//	                      Consumers are the same closed-loop drainers as -mode max.
//	                      Run `goload -mode openloop -h` for its flags.
//
//	-mode cm              "Channel manager" realistic application workload: a
//	                      2-flow hotel channel-manager pipeline over N properties
//	                      with open-loop paced producers, multi-stage consumers
//	                      (DB update -> OTA sync fan-out; price calc -> OTA price
//	                      fan-out), per-message work sleeps, and a built-in
//	                      total-order-per-property verifier. See cm.go.
//	                      Run `goload -mode cm -h` for its flags.
//
// Build (static, for the loader VM):
//
//	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o goload-linux-amd64 .
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func main() {
	mode := scanMode(os.Args[1:])
	switch mode {
	case "app":
		runAppMode(os.Args[1:])
	case "openloop":
		runOpenLoopMode(os.Args[1:])
	case "cm":
		runCMMode(os.Args[1:])
	case "max", "":
		runMaxMode(os.Args[1:])
	default:
		fmt.Printf("goload: unknown -mode %q (want: max | app | openloop | cm)\n", mode)
		os.Exit(2)
	}
}

// scanMode extracts -mode / --mode from the raw args before any FlagSet is
// parsed, so we can dispatch to the right mode's flag set. Defaults to "max"
// to preserve the original (pre-mode) goload invocation for existing harnesses.
func scanMode(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			break
		}
		if a == "-mode" || a == "--mode" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if v, ok := strings.CutPrefix(a, "-mode="); ok {
			return v
		}
		if v, ok := strings.CutPrefix(a, "--mode="); ok {
			return v
		}
	}
	return "max"
}

func runMaxMode(args []string) {
	fs := flag.NewFlagSet("goload-max", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:6632", "broker base URL")
	queueName := fs.String("queue", "benchq", "queue name")
	partitions := fs.Int("partitions", 100, "number of partitions to spread across")
	producers := fs.Int("producers", 300, "producer goroutines")
	consumers := fs.Int("consumers", 150, "consumer goroutines")
	pushBatch := fs.Int("push-batch", 10, "messages per push request")
	popBatch := fs.Int("pop-batch", 200, "max messages per pop request")
	popWildcard := fs.Bool("pop-wildcard", true, "consumers use queue-level WILDCARD pop (broker drains any partition -> full batches, few consumers) instead of pinned per-partition pop")
	popPartitions := fs.Int("pop-partitions", 1, "multi-partition pop: claim up to N partitions per pop call (>1 enables v4 multi-partition wildcard -> up to pop-batch msgs gathered across N partitions)")
	popWait := fs.Bool("pop-wait", false, "long-poll pop (Wait=true): an empty pop parks server-side and re-checks (POP_WAIT_* cadence) instead of spinning a wasted round-trip")
	popTimeout := fs.Int("pop-timeout", 2000, "pop long-poll timeout ms (used when -pop-wait)")
	payloadBytes := fs.Int("payload", 256, "payload size in bytes")
	durationSec := fs.Int("duration", 0, "run duration seconds (0 = run until SIGINT)")
	idleConns := fs.Int("idle-conns", 512, "MaxIdleConnsPerHost for the client")
	reportSec := fs.Int("report", 5, "report interval seconds")
	completedRet := fs.Int("completed-retention", 300, "completed_retention_seconds for the queue")
	dedupWindow := fs.Int("dedup-window", 0, "dedupWindowSeconds set by goload's own configure at t=0 (0 = off). Avoids the mid-run-flip artifact (external configure races the broker 30s partition-meta TTL -> synchronized rehydration storm).")
	pendingRet := fs.Int("pending-retention", 0, "retention_seconds for pending (un-consumed) messages; 0 = keep forever")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	emptySleepMs := fs.Int("empty-sleep", 2, "consumer sleep ms on empty pop")
	retries := fs.Int("retries", 2, "producer/consumer client RetryAttempts (0 = disable retries; used to test the push>pop dedup gap)")
	manualAck := fs.Bool("manual-ack", false, "consumers LEASE (pop AutoAck=false) and immediately ack the whole received batch as completed — measures TRUE production consume cost (lease + explicit full-batch offset commit) instead of server-side autoAck. On ack failure: count ackErr, NO retry (lease expires -> redeliver).")
	ackAsync := fs.Bool("ack-async", false, "with -manual-ack: dispatch each batch's ackFullBatch on a goroutine and immediately pop the NEXT partition, instead of acking synchronously in the consumer loop. Models a real async-ack consumer that doesn't hold a partition's lease blocked on its own ack round-trip. No effect without -manual-ack.")
	ackInflight := fs.Int("ack-inflight", 256, "with -ack-async: cap on concurrently in-flight async acks (a global buffered-channel semaphore). When full the consumer BLOCKS until a slot frees — an ack is NEVER shed; blocking is the honest backpressure. Only used with -ack-async.")
	_ = fs.String("mode", "max", "run mode: max | app")
	_ = fs.Parse(args)

	fmt.Printf("goload -> %s queue=%s partitions=%d producers=%d consumers=%d pushBatch=%d popBatch=%d payload=%dB idleConns=%d retries=%d manualAck=%v ackAsync=%v ackInflight=%d\n",
		*url, *queueName, *partitions, *producers, *consumers, *pushBatch, *popBatch, *payloadBytes, *idleConns, *retries, *manualAck, *ackAsync, *ackInflight)

	payload := map[string]interface{}{"data": strings.Repeat("x", *payloadBytes), "src": "goload"}

	// client-go coerces RetryAttempts==0 to the default (3); a negative value is
	// the sentinel for "no retries" (clamped to a single attempt in doRequest).
	retryAttempts := *retries
	if retryAttempts <= 0 {
		retryAttempts = -1
	}

	q, err := queen.New(queen.ClientConfig{
		URL:                 *url,
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

	// Configure the queue once (retention so the table stays bounded).
	cfgCtx, cfgCancel := context.WithTimeout(ctx, 10*time.Second)
	if _, cerr := q.GetHttpClient().Post(cfgCtx, "/api/v1/configure", map[string]interface{}{
		"queue": *queueName,
		"options": map[string]interface{}{
			"retentionEnabled":          true, // configure is a full upsert: MUST set this or retention stays off
			"completedRetentionSeconds": *completedRet,
			"retentionSeconds":          *pendingRet,
			"leaseTime":                 30,
			"dedupWindowSeconds":        *dedupWindow, // 0 = off; set here at t=0 so tests never flip dedup mid-run
			"encryptionEnabled":         os.Getenv("GOLOAD_ENCRYPT") == "1", // enables per-queue encryption (C++)
		},
	}); cerr != nil {
		fmt.Printf("[configure] WARNING: %v\n", cerr)
	} else {
		fmt.Printf("[configure] queue=%s completedRetentionSeconds=%d\n", *queueName, *completedRet)
	}
	cfgCancel()

	var pushed, popped, pushErr, popErr, emptyPops int64
	// manual-ack counters (only mutated when -manual-ack): acked = msgs the
	// server confirmed committed; ackErr = msgs that failed to commit; ackCalls
	// + ackLatUs feed the avg ack-latency readout. See ackFullBatch.
	var acked, ackErr, ackCalls, ackLatUs int64

	// -ack-async plumbing (only exercised when -manual-ack -ack-async): a global
	// buffered-channel semaphore bounds in-flight acks at -ack-inflight, ackWg
	// tracks them for the shutdown drain, and ackCtx (rooted at Background, not
	// the run ctx) lets already-dispatched acks keep landing through teardown.
	// See the consumer loop and the shutdown drain for how they're used.
	ackSem := make(chan struct{}, *ackInflight)
	var ackWg sync.WaitGroup
	ackCtx, ackCancel := context.WithCancel(context.Background())
	defer ackCancel()

	var rr uint64
	nextPart := func() string {
		return fmt.Sprintf("p%d", int(atomic.AddUint64(&rr, 1)%uint64(*partitions)))
	}

	var wg sync.WaitGroup

	// Producers: batched push, round-robin partitions.
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payloads := make([]interface{}, *pushBatch)
			for j := range payloads {
				payloads[j] = payload
			}
			for ctx.Err() == nil {
				if _, e := q.Queue(*queueName).Partition(nextPart()).Push(payloads).Execute(ctx); e != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&pushErr, 1)
					time.Sleep(5 * time.Millisecond)
					continue
				}
				atomic.AddInt64(&pushed, int64(*pushBatch))
			}
		}()
	}

	// Consumers: each PINS to a home partition (p[i % partitions]) and drains it
	// repeatedly with server-side autoAck. Pinning keeps the partition lease warm
	// and avoids the lease-churn / advisory-lock contention that round-robin-every-pop
	// causes. This is how real competing consumers behave; round-robin-per-call was a
	// load-generator artifact that throttled pop.
	for i := 0; i < *consumers; i++ {
		home := fmt.Sprintf("p%d", i%*partitions)
		wg.Add(1)
		go func(part string) {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := q.Queue(*queueName)
				if *popPartitions > 1 {
					// multi-partition wildcard: one pop gathers up to pop-batch msgs
					// across up to N partitions (v4 global budget) -> full batches.
					qb = qb.Partitions(*popPartitions)
				} else if !*popWildcard {
					qb = qb.Partition(part) // pinned single-partition pop (only that partition's ~few msgs)
				}
				// else: plain wildcard -> /pop/queue/{q} -> one partition per call.
				if *popWait {
					qb = qb.Wait(true).TimeoutMillis(*popTimeout)
				} else {
					qb = qb.Wait(false)
				}
				// -manual-ack -> lease (AutoAck=false) and commit explicitly below;
				// otherwise server-side autoAck (original max behavior, unchanged).
				msgs, e := qb.Batch(*popBatch).AutoAck(!*manualAck).Pop(ctx)
				if e != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&popErr, 1)
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if len(msgs) == 0 {
					atomic.AddInt64(&emptyPops, 1)
					time.Sleep(time.Duration(*emptySleepMs) * time.Millisecond)
					continue
				}
				atomic.AddInt64(&popped, int64(len(msgs)))
				if *manualAck {
					dispatchAck(ctx, ackCtx, q, msgs, *ackAsync, ackSem, &ackWg,
						&acked, &ackErr, &ackCalls, &ackLatUs)
				}
			}
		}(home)
	}

	// Reporter.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer t.Stop()
		var lp, lo, la int64
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				p, o := atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped)
				secs := float64(*reportSec)
				line := fmt.Sprintf("[%s] push=%8.0f/s pop=%8.0f/s | tot push=%d pop=%d | errs p=%d c=%d empty=%d",
					time.Now().UTC().Format("15:04:05"),
					float64(p-lp)/secs, float64(o-lo)/secs, p, o,
					atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops))
				if *manualAck {
					a := atomic.LoadInt64(&acked)
					line += fmt.Sprintf(" | ack=%8.0f/s tot=%d ackErr=%d ackAvg=%.2fms",
						float64(a-la)/secs, a, atomic.LoadInt64(&ackErr), avgAckMs(&ackLatUs, &ackCalls))
					la = a
				}
				fmt.Println(line)
				lp, lo = p, o
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if *durationSec > 0 {
		go func() { time.Sleep(time.Duration(*durationSec) * time.Second); cancel() }()
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
	case <-time.After(10 * time.Second):
	}
	// Drain in-flight async acks (best effort, up to 5s) before closing the client.
	drainAsyncAcks(*ackAsync, &ackWg, ackCancel)
	_ = q.Close(context.Background())
	finalLine := fmt.Sprintf("[final] pushed=%d popped=%d pushErr=%d popErr=%d emptyPops=%d",
		atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped),
		atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops))
	if *manualAck {
		finalLine += fmt.Sprintf(" | acked=%d ackErr=%d ackAvg=%.2fms",
			atomic.LoadInt64(&acked), atomic.LoadInt64(&ackErr), avgAckMs(&ackLatUs, &ackCalls))
	}
	fmt.Println(finalLine)
}

// ---------------------------------------------------------------------------
// openloop mode
// ---------------------------------------------------------------------------

// olHist is a compact log-linear latency histogram (input in microseconds).
//
// Layout: values in [0, olLinearMax) get unit (1µs) resolution; larger values
// use olSubCount sub-buckets per power-of-two octave, giving ~1/olSubCount
// (~1.6%) relative error out to ~2^olMaxOctave µs (~67s). Anything larger
// clamps into the top bucket. All buckets are int64 counters mutated with
// atomics, so record() is lock-free and safe from every producer goroutine.
//
// Percentiles are computed by the reporter. Interval-local percentiles come
// from snapshot DIFFERENCING (snapshot() into a scratch slice, subtract the
// previous snapshot) rather than resetting the live buckets — so producers
// never race a reset and never pay for one. The same buckets also yield the
// cumulative (whole-run) percentiles for the final line.
const (
	olLinearMax  = 1024               // µs; unit-resolution region [0,1024)
	olSubBits    = 6                  // 2^6 = 64 sub-buckets per octave
	olSubCount   = 1 << olSubBits    // 64
	olBaseOctave = 10                 // log2(olLinearMax)
	olMaxOctave  = 26                 // 2^26 µs ≈ 67.1s ceiling
	olNumBuckets = olLinearMax + (olMaxOctave-olBaseOctave+1)*olSubCount
)

type olHist struct {
	buckets []int64
}

func newOLHist() *olHist { return &olHist{buckets: make([]int64, olNumBuckets)} }

// olBucketIndex maps a microsecond latency to its bucket index.
func olBucketIndex(v int64) int {
	if v <= 0 {
		return 0
	}
	if v < olLinearMax {
		return int(v)
	}
	octave := bits.Len64(uint64(v)) - 1 // floor(log2 v)
	if octave > olMaxOctave {
		return olNumBuckets - 1
	}
	shift := uint(octave - olSubBits)
	sub := int((v - (int64(1) << uint(octave))) >> shift) // 0..olSubCount-1
	return olLinearMax + (octave-olBaseOctave)*olSubCount + sub
}

func (h *olHist) record(v int64) { atomic.AddInt64(&h.buckets[olBucketIndex(v)], 1) }

// snapshot copies the current counts into dst (len must be olNumBuckets).
func (h *olHist) snapshot(dst []int64) {
	for i := range h.buckets {
		dst[i] = atomic.LoadInt64(&h.buckets[i])
	}
}

// olBucketMid returns the representative value (µs) of a bucket: the midpoint
// of the value range it covers. Used to turn a bucket index back into a latency.
func olBucketMid(idx int) float64 {
	if idx < olLinearMax {
		return float64(idx) + 0.5
	}
	j := idx - olLinearMax
	octave := olBaseOctave + j/olSubCount
	sub := j % olSubCount
	width := int64(1) << uint(octave-olSubBits)
	lo := (int64(1) << uint(octave)) + int64(sub)*width
	return float64(lo) + float64(width)/2
}

// olPercentile returns the p-th percentile (p in (0,1]) of a counts slice, in
// milliseconds. counts is either a cumulative snapshot or an interval diff.
func olPercentile(counts []int64, p float64) float64 {
	var total int64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range counts {
		cum += c
		if cum >= target {
			return olBucketMid(i) / 1000.0
		}
	}
	return olBucketMid(len(counts)-1) / 1000.0
}

func runOpenLoopMode(args []string) {
	fs := flag.NewFlagSet("goload-openloop", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:6632", "broker base URL")
	queueName := fs.String("queue", "benchq", "queue name")
	partitions := fs.Int("partitions", 100, "number of partitions to spread across")
	consumers := fs.Int("consumers", 150, "consumer goroutines (closed-loop drainers)")
	rate := fs.Int("rate", 0, "OPEN-LOOP total offered rate in msg/s across all producers (required, >0)")
	maxInflight := fs.Int("max-inflight", 20000, "cap on in-flight push REQUESTS; over the cap a request's messages are shed (counted, not sent)")
	rampSec := fs.Int("ramp-sec", 0, "OPEN-LOOP: linear ramp of the offered rate from 0 to -rate over N seconds (0 = full rate from t=0). Avoids the cold-start storm: pool dial-up, first-contact seeding and dedup-cache hydration happen under partial load.")
	pushBatch := fs.Int("push-batch", 10, "messages per push request (offered request rate = rate/push-batch)")
	popBatch := fs.Int("pop-batch", 200, "max messages per pop request")
	popWildcard := fs.Bool("pop-wildcard", true, "consumers use queue-level WILDCARD pop instead of pinned per-partition pop")
	popPartitions := fs.Int("pop-partitions", 1, "multi-partition pop: claim up to N partitions per pop call (>1 enables v4 multi-partition wildcard)")
	popWait := fs.Bool("pop-wait", false, "long-poll pop (Wait=true)")
	popTimeout := fs.Int("pop-timeout", 2000, "pop long-poll timeout ms (used when -pop-wait)")
	payloadBytes := fs.Int("payload", 256, "payload size in bytes")
	durationSec := fs.Int("duration", 0, "run duration seconds (0 = run until SIGINT)")
	idleConns := fs.Int("idle-conns", 2048, "MaxIdleConnsPerHost for the client (keep-alive pool; size near max-inflight to avoid churn)")
	reportSec := fs.Int("report", 5, "report interval seconds")
	completedRet := fs.Int("completed-retention", 300, "completed_retention_seconds for the queue")
	dedupWindow := fs.Int("dedup-window", 0, "dedupWindowSeconds set by goload's own configure at t=0 (0 = off). Avoids the mid-run-flip artifact (external configure races the broker 30s partition-meta TTL -> synchronized rehydration storm).")
	pendingRet := fs.Int("pending-retention", 0, "retention_seconds for pending (un-consumed) messages; 0 = keep forever")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	emptySleepMs := fs.Int("empty-sleep", 2, "consumer sleep ms on empty pop")
	manualAck := fs.Bool("manual-ack", false, "consumers LEASE (pop AutoAck=false) and immediately ack the whole received batch as completed — measures TRUE production consume cost (lease + explicit full-batch offset commit) instead of server-side autoAck. On ack failure: count ackErr, NO retry (lease expires -> redeliver).")
	ackAsync := fs.Bool("ack-async", false, "with -manual-ack: dispatch each batch's ackFullBatch on a goroutine and immediately pop the NEXT partition, instead of acking synchronously in the consumer loop. Models a real async-ack consumer that doesn't hold a partition's lease blocked on its own ack round-trip. No effect without -manual-ack.")
	ackInflight := fs.Int("ack-inflight", 256, "with -ack-async: cap on concurrently in-flight async acks (a global buffered-channel semaphore). When full the consumer BLOCKS until a slot frees — an ack is NEVER shed; blocking is the honest backpressure. Only used with -ack-async.")
	_ = fs.String("mode", "openloop", "run mode: max | app | openloop")
	_ = fs.Parse(args)

	if *rate <= 0 {
		fmt.Println("goload -mode openloop: -rate must be > 0 (total offered msg/s)")
		os.Exit(2)
	}
	if *pushBatch <= 0 {
		fmt.Println("goload -mode openloop: -push-batch must be > 0")
		os.Exit(2)
	}
	if *maxInflight <= 0 {
		fmt.Println("goload -mode openloop: -max-inflight must be > 0")
		os.Exit(2)
	}

	// Offered REQUEST rate (each request carries push-batch messages). The pacer
	// schedules at this many requests/s, split across W workers.
	reqPerSec := float64(*rate) / float64(*pushBatch)
	// W = min(64, requests/s / 50 + 1): ~50 req/s per worker, capped at 64 so a
	// tiny rate still gets a single pacer and a huge rate never spawns > 64.
	W := int(reqPerSec/50) + 1
	if W > 64 {
		W = 64
	}
	if W < 1 {
		W = 1
	}
	perWorkerRPS := reqPerSec / float64(W)

	fmt.Printf("goload -mode openloop -> %s queue=%s partitions=%d consumers=%d manualAck=%v ackAsync=%v ackInflight=%d\n",
		*url, *queueName, *partitions, *consumers, *manualAck, *ackAsync, *ackInflight)
	fmt.Printf("  offered: rate=%d msg/s | push-batch=%d -> %.1f req/s across %d pacer workers (%.2f req/s each) | max-inflight=%d | payload=%dB\n",
		*rate, *pushBatch, reqPerSec, W, perWorkerRPS, *maxInflight, *payloadBytes)

	// Reuse ONE payload slice across every push goroutine: the client only READS
	// payloads (buildItems copies into per-request PushItems), so sharing the
	// backing array is race-free and avoids a per-request allocation.
	payload := map[string]interface{}{"data": strings.Repeat("x", *payloadBytes), "src": "goload-ol"}
	payloads := make([]interface{}, *pushBatch)
	for j := range payloads {
		payloads[j] = payload
	}

	// Open-loop producers do NOT retry: RetryAttempts=-1 forces exactly one
	// attempt. A failed push is counted (pushErr) and dropped — the pacer will
	// offer more on the next tick anyway, and retrying would double-offer and
	// corrupt the offered-rate accounting (and hold an in-flight slot longer).
	q, err := queen.New(queen.ClientConfig{
		URL:                 *url,
		TimeoutMillis:       *timeoutMs,
		MaxIdleConnsPerHost: *idleConns,
		RetryAttempts:       -1,
	})
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure the queue once (retention so the table stays bounded).
	cfgCtx, cfgCancel := context.WithTimeout(ctx, 10*time.Second)
	if _, cerr := q.GetHttpClient().Post(cfgCtx, "/api/v1/configure", map[string]interface{}{
		"queue": *queueName,
		"options": map[string]interface{}{
			"retentionEnabled":          true,
			"completedRetentionSeconds": *completedRet,
			"retentionSeconds":          *pendingRet,
			"leaseTime":                 30,
			"dedupWindowSeconds":        *dedupWindow,
			"encryptionEnabled":         os.Getenv("GOLOAD_ENCRYPT") == "1",
		},
	}); cerr != nil {
		fmt.Printf("[configure] WARNING: %v\n", cerr)
	} else {
		fmt.Printf("[configure] queue=%s completedRetentionSeconds=%d\n", *queueName, *completedRet)
	}
	cfgCancel()

	var pushed, popped, pushErr, popErr, emptyPops int64
	// manual-ack counters (only mutated when -manual-ack); see ackFullBatch.
	var acked, ackErr, ackCalls, ackLatUs int64

	// -ack-async plumbing (only exercised when -manual-ack -ack-async); see the
	// consumer loop and the shutdown drain, and the max-mode note above for the
	// rationale (bounded in-flight, honest blocking backpressure, teardown drain).
	ackSem := make(chan struct{}, *ackInflight)
	var ackWg sync.WaitGroup
	ackCtx, ackCancel := context.WithCancel(context.Background())
	defer ackCancel()

	var offeredReq, sheddedReq, achievedReq int64
	var inflight int64
	lat := newOLHist()

	var rr uint64
	nextPart := func() string {
		return fmt.Sprintf("p%d", int(atomic.AddUint64(&rr, 1)%uint64(*partitions)))
	}

	// In-flight cap: a buffered-channel semaphore. Acquired NON-BLOCKINGLY at
	// each scheduled instant; if full the request is shed. NEVER block here —
	// blocking would silently turn the pacer into a closed-loop worker pool.
	sem := make(chan struct{}, *maxInflight)

	// doPush runs one push in its own goroutine. Latency is measured from the
	// request's SCHEDULED instant (sched), not from now, so a pacer that fell
	// behind (GC pause, CPU saturation) shows up as latency instead of vanishing
	// — this is the coordinated-omission correction.
	doPush := func(sched time.Time, part string) {
		defer func() {
			atomic.AddInt64(&inflight, -1)
			<-sem
		}()
		_, e := q.Queue(*queueName).Partition(part).Push(payloads).Execute(ctx)
		if e != nil {
			if ctx.Err() == nil {
				atomic.AddInt64(&pushErr, 1)
			}
			return // NO retry (see RetryAttempts=-1 note above).
		}
		atomic.AddInt64(&pushed, int64(*pushBatch))
		atomic.AddInt64(&achievedReq, 1)
		lat.record(time.Since(sched).Microseconds())
	}

	var wg sync.WaitGroup

	// Pacer workers. Each owns a fixed schedule anchored at t0 with a random
	// per-worker phase offset (decorrelates the W tickers so they don't all fire
	// in lockstep). On every wake we compute how many scheduled instants are now
	// due from the WALL CLOCK (not by counting ticks), so a late wake still
	// offers everything it owes — dropped ticks can't silently reduce the
	// offered rate. To bound per-wake work at rates beyond the rig, at most
	// maxCatchUp requests are handled individually (real semaphore try); any
	// further backlog is bulk-counted as offered+shed and the schedule advances.
	const maxCatchUp = 4096
	// Ticker cadence: clamp the per-worker spacing into [minTick, maxTick].
	//   - maxTick caps how coarse the wake is so that at LOW rates each request
	//     is launched within ~maxTick of its scheduled instant (otherwise a
	//     coarse spacing would add up to one full spacing of *loader* delay to
	//     every coordinated-omission latency — a measurement artifact, not
	//     broker latency).
	//   - minTick floors it so we don't spin a sub-ms ticker at very HIGH rates;
	//     there the catch-up loop launches several requests per wake instead
	//     (which limits scheduling resolution to ~minTick — see caveats).
	minTick := 250 * time.Microsecond
	maxTick := 1 * time.Millisecond
	step := time.Duration(float64(time.Second) / perWorkerRPS)
	tickEvery := step
	if tickEvery > maxTick {
		tickEvery = maxTick
	}
	if tickEvery < minTick {
		tickEvery = minTick
	}
	t0 := time.Now()
	for w := 0; w < W; w++ {
		// Random phase in [0, step) so the W schedules interleave.
		offset := time.Duration(rand.Int63n(int64(step) + 1))
		base := t0.Add(offset)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := time.NewTicker(tickEvery)
			defer tk.Stop()
			var k int64 // number of requests scheduled by this worker so far
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
				}
				now := time.Now()
				if now.Before(base) {
					continue
				}
				// targetK = # of scheduled instants with schedTime <= now.
				// With -ramp-sec R the schedule density ramps linearly 0→full:
				// cumulative F(t) = rps·t²/(2R) for t<R, then rps·(t−R/2). The
				// per-request sched instants (CO-correct latency baseline) are
				// F⁻¹(k) — see schedAt below. ramp==0 ⇒ the original linear
				// schedule, bit-identical.
				el := now.Sub(base).Seconds()
				ramp := float64(*rampSec)
				var cum float64
				if ramp <= 0 || el >= ramp {
					cum = perWorkerRPS * (el - maxf(ramp, 0)/2)
				} else {
					cum = perWorkerRPS * el * el / (2 * ramp)
				}
				targetK := int64(cum) + 1
				owed := targetK - k
				if owed <= 0 {
					continue
				}
				indiv := owed
				var bulk int64
				if owed > maxCatchUp {
					indiv = maxCatchUp
					bulk = owed - indiv
				}
				for n := int64(0); n < indiv; n++ {
					// F⁻¹(k): during the ramp k = rps·t²/(2R) ⇒ t = √(2kR/rps);
					// after it t = k/rps + R/2.
					var schedSec float64
					kf := float64(k)
					if ramp > 0 && kf < perWorkerRPS*ramp/2 {
						schedSec = math.Sqrt(2 * kf * ramp / perWorkerRPS)
					} else {
						schedSec = kf/perWorkerRPS + maxf(ramp, 0)/2
					}
					sched := base.Add(time.Duration(schedSec * float64(time.Second)))
					k++
					atomic.AddInt64(&offeredReq, 1)
					select {
					case sem <- struct{}{}:
						atomic.AddInt64(&inflight, 1)
						go doPush(sched, nextPart())
					default:
						atomic.AddInt64(&sheddedReq, 1) // cap hit -> shed
					}
				}
				if bulk > 0 {
					// Backlog beyond the per-wake cap: these are owed but the
					// broker is already saturated (we're at the in-flight cap),
					// so count them as offered+shed and jump the schedule forward.
					k += bulk
					atomic.AddInt64(&offeredReq, bulk)
					atomic.AddInt64(&sheddedReq, bulk)
				}
			}
		}()
	}

	// Consumers: unchanged closed-loop drainers (identical to -mode max).
	for i := 0; i < *consumers; i++ {
		home := fmt.Sprintf("p%d", i%*partitions)
		wg.Add(1)
		go func(part string) {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := q.Queue(*queueName)
				if *popPartitions > 1 {
					qb = qb.Partitions(*popPartitions)
				} else if !*popWildcard {
					qb = qb.Partition(part)
				}
				if *popWait {
					qb = qb.Wait(true).TimeoutMillis(*popTimeout)
				} else {
					qb = qb.Wait(false)
				}
				// -manual-ack -> lease (AutoAck=false) and commit explicitly below;
				// otherwise server-side autoAck (original openloop behavior).
				msgs, e := qb.Batch(*popBatch).AutoAck(!*manualAck).Pop(ctx)
				if e != nil {
					if ctx.Err() != nil {
						return
					}
					atomic.AddInt64(&popErr, 1)
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if len(msgs) == 0 {
					atomic.AddInt64(&emptyPops, 1)
					time.Sleep(time.Duration(*emptySleepMs) * time.Millisecond)
					continue
				}
				atomic.AddInt64(&popped, int64(len(msgs)))
				if *manualAck {
					dispatchAck(ctx, ackCtx, q, msgs, *ackAsync, ackSem, &ackWg,
						&acked, &ackErr, &ackCalls, &ackLatUs)
				}
			}
		}(home)
	}

	// Reporter. Percentiles are INTERVAL-LOCAL: computed from the difference
	// between successive cumulative snapshots of the latency histogram, so each
	// line reflects only the requests completed during that interval.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer t.Stop()
		prev := make([]int64, olNumBuckets)
		cur := make([]int64, olNumBuckets)
		diff := make([]int64, olNumBuckets)
		var lOff, lAch, lShed, lPop, lAck int64
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				secs := float64(*reportSec)
				off := atomic.LoadInt64(&offeredReq)
				ach := atomic.LoadInt64(&achievedReq)
				shed := atomic.LoadInt64(&sheddedReq)
				p := atomic.LoadInt64(&pushed)
				pop := atomic.LoadInt64(&popped)
				lat.snapshot(cur)
				for i := range diff {
					diff[i] = cur[i] - prev[i]
					prev[i] = cur[i]
				}
				b := int64(*pushBatch)
				line := fmt.Sprintf("[%s] offered=%9.0f/s achieved=%9.0f/s shed=%9.0f/s inflight=%6d | p50=%7.2f p99=%8.2f p999=%8.2f ms | push=%d pop=%d lag=%d | errs push=%d pop=%d empty=%d gor=%d",
					time.Now().UTC().Format("15:04:05"),
					float64(off-lOff)*float64(b)/secs,
					float64(ach-lAch)*float64(b)/secs,
					float64(shed-lShed)*float64(b)/secs,
					atomic.LoadInt64(&inflight),
					olPercentile(diff, 0.50), olPercentile(diff, 0.99), olPercentile(diff, 0.999),
					p, pop, p-pop,
					atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops),
					runtime.NumGoroutine())
				if *manualAck {
					a := atomic.LoadInt64(&acked)
					line += fmt.Sprintf(" | ack=%9.0f/s ackErr=%d ackAvg=%.2fms",
						float64(a-lAck)*1.0/secs, atomic.LoadInt64(&ackErr), avgAckMs(&ackLatUs, &ackCalls))
					lAck = a
				}
				fmt.Println(line)
				lOff, lAch, lShed, lPop = off, ach, shed, pop
				_ = lPop
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if *durationSec > 0 {
		go func() { time.Sleep(time.Duration(*durationSec) * time.Second); cancel() }()
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
	case <-time.After(10 * time.Second):
	}
	// Drain in-flight async acks (best effort, up to 5s) before closing the client.
	drainAsyncAcks(*ackAsync, &ackWg, ackCancel)
	_ = q.Close(context.Background())

	// Final totals + cumulative (whole-run) percentiles.
	final := make([]int64, olNumBuckets)
	lat.snapshot(final)
	b := int64(*pushBatch)
	off := atomic.LoadInt64(&offeredReq)
	shed := atomic.LoadInt64(&sheddedReq)
	p := atomic.LoadInt64(&pushed)
	pop := atomic.LoadInt64(&popped)
	finalLine := fmt.Sprintf("[final] offered=%d achieved=%d shed=%d (msgs: offered=%d achieved=%d shed=%d) pushErr=%d | pushed=%d popped=%d lag=%d | popErr=%d empty=%d | overall p50=%.2f p99=%.2f p999=%.2f ms",
		off, atomic.LoadInt64(&achievedReq), shed,
		off*b, p, shed*b,
		atomic.LoadInt64(&pushErr),
		p, pop, p-pop,
		atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops),
		olPercentile(final, 0.50), olPercentile(final, 0.99), olPercentile(final, 0.999))
	if *manualAck {
		// lag stays pushed-popped (delivery lag); ackLag = pushed-acked is the
		// COMMIT lag — messages delivered+received but not yet offset-committed.
		a := atomic.LoadInt64(&acked)
		finalLine += fmt.Sprintf(" | acked=%d ackErr=%d ackLag=%d ackAvg=%.2fms",
			a, atomic.LoadInt64(&ackErr), p-a, avgAckMs(&ackLatUs, &ackCalls))
	}
	fmt.Println(finalLine)
}

// maxf: float max without pulling in generics — used by the openloop ramp math.
func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// manual-ack consume path (shared by -mode max and -mode openloop)
// ---------------------------------------------------------------------------

// dispatchAck routes a leased pop batch to its ack, either SYNCHRONOUSLY (the
// original behavior: ack in the consumer loop, which caps the system at
// partitions×batch / full-cycle-latency because a popped partition stays lease-
// locked until its ack lands) or ASYNCHRONOUSLY (-ack-async: fire the ack on a
// goroutine and return immediately so the consumer pops the NEXT partition —
// how a real consumer behaves).
//
// Async backpressure is a global buffered-channel semaphore of -ack-inflight
// slots: we BLOCK for a free slot before dispatching so an ack is NEVER shed
// (dropping an ack would leave the batch to lease-expire and redeliver, which
// is not the behavior we're modeling). The one exception is shutdown: if the
// run ctx is cancelled while we're blocked for a slot, we skip this batch's ack
// and return — the lease expires, the batch redelivers, and NO counter is
// touched. The dispatched goroutine runs under ackCtx (not the run ctx) so an
// already-launched ack can still land during the teardown drain; ackWg tracks
// it so that drain can wait.
func dispatchAck(ctx, ackCtx context.Context, q *queen.Queen, msgs []*queen.Message,
	async bool, ackSem chan struct{}, ackWg *sync.WaitGroup,
	acked, ackErr, ackCalls, ackLatUs *int64) {
	if !async {
		ackFullBatch(ctx, q, msgs, acked, ackErr, ackCalls, ackLatUs)
		return
	}
	// Block for an in-flight slot (honest backpressure), bailing only if the run
	// is shutting down.
	select {
	case ackSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	ackWg.Add(1)
	go func(batch []*queen.Message) {
		defer ackWg.Done()
		defer func() { <-ackSem }()
		ackFullBatch(ackCtx, q, batch, acked, ackErr, ackCalls, ackLatUs)
	}(msgs)
}

// drainAsyncAcks gives already-dispatched async acks up to 5s to land (commit
// their cursor) after the run ctx is cancelled, then cancels ackCtx so any
// straggler still inside q.Ack unblocks. Best effort: ackFullBatch ignores
// ctx-cancelled failures, so a straggler that gets cut off is simply not counted
// — no ackErr inflation, no double count, no corruption. No-op unless -ack-async.
func drainAsyncAcks(async bool, ackWg *sync.WaitGroup, ackCancel context.CancelFunc) {
	if async {
		done := make(chan struct{})
		go func() { ackWg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	ackCancel()
}

// ackFullBatch immediately acknowledges an ENTIRE leased pop batch as
// `completed` — the honest production consume path: a leased pop (AutoAck=false)
// followed by an explicit full-batch offset commit. One /api/v1/ack/batch call
// echoes each message's transactionId + partitionId + the pop's leaseId (the
// client's Queen.Ack marshals all three straight off each *Message). Because
// Queen's ack is an offset COMMIT, acking the whole received batch advances
// queen.log_consumers.committed for that partition/group — this is what makes
// the cursor move (vs. server-side autoAck, which commits inside the pop).
//
// On failure we COUNT it and move on — we NEVER retry. A failed call (or a
// per-item rejection, e.g. the lease already expired) leaves the messages
// unacked; the lease then expires server-side and the messages redeliver. That
// is the real production behavior — a consumer that crashes or loses its lease
// does not get a free re-ack — and retrying here would both distort the
// measured per-message ack cost and double-commit. So: no retry, by design.
//
// Counting is per-message so acked+ackErr tracks popped exactly:
//   - acked  += messages the server confirmed (AckResponse.Success == true)
//   - ackErr += messages that did NOT commit (whole-call error -> all N in the
//     batch; otherwise N - (confirmed count) from per-item rejections)
//   - ackCalls/ackLatUs accumulate one sample per batch-ack call for avg latency
func ackFullBatch(ctx context.Context, q *queen.Queen, msgs []*queen.Message,
	acked, ackErr, ackCalls, ackLatUs *int64) {
	n := int64(len(msgs))
	t0 := time.Now()
	resp, err := q.Ack(ctx, msgs, true, queen.AckOptions{}) // success=true -> status "completed"
	atomic.AddInt64(ackLatUs, time.Since(t0).Microseconds())
	atomic.AddInt64(ackCalls, 1)
	if err != nil {
		// Whole-call failure (HTTP/timeout): none of the N committed. Ignore
		// shutdown-induced errors (ctx cancelled) so teardown isn't miscounted.
		if ctx.Err() == nil {
			atomic.AddInt64(ackErr, n)
		}
		return
	}
	// Per-item accounting: /api/v1/ack/batch returns one result per message in
	// request order; a rejected item (Success=false) did not commit.
	var ok int64
	for _, r := range resp {
		if r.Success {
			ok++
		}
	}
	atomic.AddInt64(acked, ok)
	if ok < n {
		atomic.AddInt64(ackErr, n-ok)
	}
}

// avgAckMs returns the mean ack-call latency in ms (cumulative). Reads both
// atomics; 0 when no ack call has completed yet.
func avgAckMs(ackLatUs, ackCalls *int64) float64 {
	calls := atomic.LoadInt64(ackCalls)
	if calls == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(ackLatUs)) / float64(calls) / 1000.0
}
