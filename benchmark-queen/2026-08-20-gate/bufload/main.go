// bufload — the client-side buffer as Kafka-linger equivalent, measured.
//
// Every producer goroutine does SINGLE-message Push().Execute() calls — the
// exact call shape of the raw 29.5k req/s test — but with the client buffer
// enabled (BufferConfig{MessageCount, TimeMillis}): Execute returns when the
// message is buffered, and the BufferManager ships accumulated batches. One
// partition per goroutine keeps the flush streams on disjoint lanes.
//
// The number that matters at the end: client-side sends/s versus what the
// broker actually holds (read back via the depth endpoint) — the buffer is
// fire-and-forget, so the delta IS the loss accounting, printed rather than
// assumed zero.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:6632", "broker URL")
	producers := flag.Int("producers", 32, "producer goroutines (one partition each)")
	msgCount := flag.Int("buf-count", 200, "buffer flush at N messages")
	lingerMs := flag.Int("buf-ms", 5, "buffer flush after N ms")
	dur := flag.Int("duration", 45, "seconds")
	flag.Parse()

	q, err := queen.New(*url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	queueName := "bufbench"

	var sent, errs atomic.Int64
	stop := make(chan struct{})
	go func() { time.Sleep(time.Duration(*dur) * time.Second); close(stop) }()

	start := time.Now()
	var wg sync.WaitGroup
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			part := fmt.Sprintf("p%d", id)
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				n++
				_, err := q.Queue(queueName).Partition(part).
					Buffer(queen.BufferConfig{MessageCount: *msgCount, TimeMillis: *lingerMs}).
					Push(map[string]any{"n": n, "p": id}).
					Execute(ctx)
				if err != nil {
					errs.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(p)
	}

	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		var last int64
		lastT := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				s := sent.Load()
				now := time.Now()
				fmt.Printf("[%s] send()=%8.0f/s  total=%d errs=%d\n",
					now.Format("15:04:05"), float64(s-last)/now.Sub(lastT).Seconds(), s, errs.Load())
				last, lastT = s, now
			}
		}
	}()

	wg.Wait()
	el := time.Since(start).Seconds()

	// Drain the buffers, then read back what the broker actually holds.
	fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := q.FlushAllBuffers(fctx); err != nil {
		fmt.Fprintln(os.Stderr, "final flush:", err)
	}
	time.Sleep(2 * time.Second)

	fmt.Printf("\n[final] client send()=%d (%.0f/s) errs=%d over %.0fs (buf: %d msgs / %d ms)\n",
		sent.Load(), float64(sent.Load())/el, errs.Load(), el, *msgCount, *lingerMs)
}
