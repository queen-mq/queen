package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	benchQueue       string
	benchTotal       int
	benchBatch       int
	benchPartitions  int
	benchProducers   int
	benchConsumers   int
	benchPayloadSize int
	benchSkipPop     bool
	benchAutoCreate  bool
	benchAutoDelete  bool
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Smoke benchmark: push N messages and pop them, report throughput + queue residency",
	Long: `Quick end-to-end benchmark useful during sizing or when validating a
new deployment. Pushes --total messages across --partitions partitions in
batches of --batch from --producers goroutines, then drains the queue with
--consumers goroutines.

Reported metrics:

  PUSH        wall time for the producer phase (msg/s = total / push_secs).
  POP         wall time for the consumer phase.
  RESIDENCY   per-message (pop_ts - push_ts) percentiles. NOT a network RTT;
              it includes the time the message sat waiting for a consumer.
              For tight RTT numbers, run with --producers=1 --consumers=1
              --batch=1 --partitions=1.

Lifecycle:

  --auto-create   create 'queenctl-bench' (or --queue) if missing  (default true)
  --auto-delete   drop the queue at the end                        (default false)

The queue is left in place by default so you can inspect what happened
through the dashboard or 'queenctl queue describe queenctl-bench'. Run
'queenctl queue delete queenctl-bench --yes' once you're done.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()

		if benchAutoCreate {
			_, err := c.Q.Queue(benchQueue).Config(queen.QueueConfig{
				LeaseTime: 30,
			}).Create().Execute(ctx)
			if err != nil {
				return clierr.Server(fmt.Errorf("create queue: %w", err))
			}
		}
		if benchAutoDelete {
			defer func() {
				_, _ = c.Q.Queue(benchQueue).Delete().Execute(ctx)
			}()
		}

		// Build a fixed-size payload to avoid masking server perf with JSON
		// reflection in the client.
		payload := map[string]any{
			"ts":   time.Now().UnixNano(),
			"data": padding(benchPayloadSize),
		}

		fmt.Fprintf(stdout(), "pushing %d messages across %d partitions...\n", benchTotal, benchPartitions)
		pushStart := time.Now()
		err = pushBench(ctx, c.Q, benchQueue, payload)
		if err != nil {
			return clierr.Server(err)
		}
		pushDur := time.Since(pushStart)

		var popDur time.Duration
		var lats []time.Duration
		var unique int
		if !benchSkipPop {
			fmt.Fprintf(stdout(), "draining %d messages with %d consumers...\n", benchTotal, benchConsumers)
			popStart := time.Now()
			lats, unique, err = popBench(ctx, c.Q, benchQueue)
			if err != nil {
				return clierr.Server(err)
			}
			popDur = time.Since(popStart)
		}

		fmt.Fprintln(stdout())
		fmt.Fprintf(stdout(), "PUSH       %d msgs in %s (%.0f msg/s)\n",
			benchTotal, pushDur.Round(time.Millisecond), float64(benchTotal)/pushDur.Seconds())
		if !benchSkipPop {
			fmt.Fprintf(stdout(), "POP        %d msgs in %s (%.0f msg/s)\n",
				unique, popDur.Round(time.Millisecond), float64(unique)/popDur.Seconds())
			if unique != benchTotal {
				fmt.Fprintf(stdout(), "WARN       expected %d, drained %d (%d short - check queue config / lease time)\n",
					benchTotal, unique, benchTotal-unique)
			}
			if len(lats) > 0 {
				p50, p95, p99 := percentiles(lats)
				fmt.Fprintf(stdout(), "RESIDENCY  p50=%s p95=%s p99=%s  (pop_ts - push_ts; not RTT)\n",
					p50.Round(time.Microsecond),
					p95.Round(time.Microsecond),
					p99.Round(time.Microsecond))
			}
		}
		if !benchAutoDelete {
			fmt.Fprintln(stdout())
			if benchSkipPop {
				fmt.Fprintf(stdout(), "queue %q has %d pending messages.\n", benchQueue, benchTotal)
			} else {
				fmt.Fprintf(stdout(), "queue %q drained (messages were popped + auto-acked).\n", benchQueue)
			}
			fmt.Fprintf(stdout(), "  inspect:  queenctl queue describe %s\n", benchQueue)
			fmt.Fprintf(stdout(), "  cleanup:  queenctl queue delete %s --yes\n", benchQueue)
		}
		return nil
	},
}

func padding(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func pushBench(ctx context.Context, q *queen.Queen, queueName string, payload map[string]any) error {
	if benchProducers <= 0 {
		benchProducers = 1
	}
	per := benchTotal / benchProducers
	rem := benchTotal % benchProducers
	var wg sync.WaitGroup
	errCh := make(chan error, benchProducers)
	for p := 0; p < benchProducers; p++ {
		count := per
		if p < rem {
			count++
		}
		producerID := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < count; i += benchBatch {
				end := i + benchBatch
				if end > count {
					end = count
				}
				items := make([]any, 0, end-i)
				for j := i; j < end; j++ {
					p := map[string]any{
						"producer": producerID,
						"i":        j,
						"ts":       time.Now().UnixNano(),
						"data":     payload["data"],
					}
					items = append(items, p)
				}
				partition := fmt.Sprintf("p%d", (producerID+i)%benchPartitions)
				if _, err := q.Queue(queueName).Partition(partition).Push(items).Execute(ctx); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

// popBench drains the queue using direct Pop calls with SERVER-side
// autoAck. We deliberately avoid Consume(...) here: the SDK's Consume
// auto-ack is a CLIENT-side ack call after the handler returns, and the
// natural "stop when N messages received" pattern (cancel a context from
// inside the handler) cancels the in-flight ack alongside the pop, so the
// last batch never gets acknowledged. Direct pop with ?autoAck=true makes
// the broker mark the message consumed-by-CG atomically with the pop and
// removes any client-side ack race entirely.
//
// Returns: per-message residency times, number of UNIQUE messages drained,
// any unrecoverable error.
func popBench(ctx context.Context, q *queen.Queen, queueName string) ([]time.Duration, int, error) {
	if benchConsumers <= 0 {
		benchConsumers = 1
	}
	var (
		mu     sync.Mutex
		lats   = make([]time.Duration, 0, benchTotal)
		seen   = make(map[string]struct{}, benchTotal)
		count  int
		wg     sync.WaitGroup
		err    error
	)
	popCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for w := 0; w < benchConsumers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idleSince := time.Time{}
			for {
				if popCtx.Err() != nil {
					return
				}
				msgs, perr := q.Queue(queueName).
					Group("queenctl-bench").
					AutoAck(true).
					Batch(benchBatch).
					Wait(true).
					TimeoutMillis(2_000).
					Pop(popCtx)
				if perr != nil {
					if errors.Is(perr, context.Canceled) {
						return
					}
					mu.Lock()
					if err == nil {
						err = perr
					}
					mu.Unlock()
					return
				}
				if len(msgs) == 0 {
					if idleSince.IsZero() {
						idleSince = time.Now()
					}
					if time.Since(idleSince) > 5*time.Second {
						return
					}
					continue
				}
				idleSince = time.Time{}
				now := time.Now().UnixNano()
				mu.Lock()
				for _, msg := range msgs {
					if _, dup := seen[msg.TransactionID]; dup {
						continue
					}
					seen[msg.TransactionID] = struct{}{}
					count++
					if ts, ok := msg.Data["ts"].(float64); ok {
						lats = append(lats, time.Duration(now-int64(ts))*time.Nanosecond)
					}
				}
				done := count >= benchTotal
				mu.Unlock()
				if done {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	if err != nil {
		return lats, count, err
	}
	return lats, count, nil
}

func percentiles(durs []time.Duration) (p50, p95, p99 time.Duration) {
	if len(durs) == 0 {
		return
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	pick := func(p float64) time.Duration {
		idx := int(p * float64(len(durs)-1))
		return durs[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}

func init() {
	benchCmd.Flags().StringVar(&benchQueue, "queue", "queenctl-bench", "queue name")
	benchCmd.Flags().IntVar(&benchTotal, "total", 10_000, "total messages")
	benchCmd.Flags().IntVar(&benchBatch, "batch", 100, "messages per push call")
	benchCmd.Flags().IntVar(&benchPartitions, "partitions", 4, "partitions to spread across")
	benchCmd.Flags().IntVar(&benchProducers, "producers", 4, "concurrent producers")
	benchCmd.Flags().IntVar(&benchConsumers, "consumers", 4, "concurrent consumers")
	benchCmd.Flags().IntVar(&benchPayloadSize, "payload-size", 64, "filler bytes per payload")
	benchCmd.Flags().BoolVar(&benchSkipPop, "skip-pop", false, "push only")
	benchCmd.Flags().BoolVar(&benchAutoCreate, "auto-create", true, "create the queue if missing")
	benchCmd.Flags().BoolVar(&benchAutoDelete, "auto-delete", false, "delete the queue when finished (off by default so you can inspect it)")
	rootCmd.AddCommand(benchCmd)
}
