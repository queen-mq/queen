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
// Build (static, for the loader VM):
//
//	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o goload-linux-amd64 .
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
	case "max", "":
		runMaxMode(os.Args[1:])
	default:
		fmt.Printf("goload: unknown -mode %q (want: max | app)\n", mode)
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
	pendingRet := fs.Int("pending-retention", 0, "retention_seconds for pending (un-consumed) messages; 0 = keep forever")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	emptySleepMs := fs.Int("empty-sleep", 2, "consumer sleep ms on empty pop")
	retries := fs.Int("retries", 2, "producer/consumer client RetryAttempts (0 = disable retries; used to test the push>pop dedup gap)")
	_ = fs.String("mode", "max", "run mode: max | app")
	_ = fs.Parse(args)

	fmt.Printf("goload -> %s queue=%s partitions=%d producers=%d consumers=%d pushBatch=%d popBatch=%d payload=%dB idleConns=%d retries=%d\n",
		*url, *queueName, *partitions, *producers, *consumers, *pushBatch, *popBatch, *payloadBytes, *idleConns, *retries)

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
		},
	}); cerr != nil {
		fmt.Printf("[configure] WARNING: %v\n", cerr)
	} else {
		fmt.Printf("[configure] queue=%s completedRetentionSeconds=%d\n", *queueName, *completedRet)
	}
	cfgCancel()

	var pushed, popped, pushErr, popErr, emptyPops int64
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
				msgs, e := qb.Batch(*popBatch).AutoAck(true).Pop(ctx)
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
			}
		}(home)
	}

	// Reporter.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer t.Stop()
		var lp, lo int64
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				p, o := atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped)
				secs := float64(*reportSec)
				fmt.Printf("[%s] push=%8.0f/s pop=%8.0f/s | tot push=%d pop=%d | errs p=%d c=%d empty=%d\n",
					time.Now().UTC().Format("15:04:05"),
					float64(p-lp)/secs, float64(o-lo)/secs, p, o,
					atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops))
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
	_ = q.Close(context.Background())
	fmt.Printf("[final] pushed=%d popped=%d pushErr=%d popErr=%d emptyPops=%d\n",
		atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped),
		atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&emptyPops))
}
