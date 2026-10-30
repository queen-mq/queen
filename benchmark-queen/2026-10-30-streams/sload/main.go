// sload: a load generator and checker for Queen stream processing.
//
// Producers push JSON events at a fixed rate into the partitions of a source
// queue. N stream runners (one consumer group, the Go client's runtime) window
// them per partition (tumbling or session), aggregate count and sum, and write
// one message per closed window to a sink queue. A verifier reads the sink and
// checks that every event is counted once (the sum of the windows' counts
// equals the events pushed: more = a window emitted twice, fewer = lost), and
// measures how long after its end each window was emitted.
//
// -active N with -dist rotate sends each second's events to N partitions, the
// next N the next second: every partition gets a burst and then silence, so a
// session window closes only through the runners' flush and claim (the case
// of issue #153).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

type config struct {
	url        string
	tag        string
	rate       int
	partitions int
	active     int
	batch      int
	producers  int
	duration   time.Duration
	drain      time.Duration
	window     string
	size       float64
	gap        float64
	idleFlush  int64
	runners    int
	runBatch   int
	report     time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", "http://127.0.0.1:6632", "broker base URL")
	flag.StringVar(&c.tag, "tag", "s", "queue name prefix: <tag>-src, <tag>-sink, query <tag>-q")
	flag.IntVar(&c.rate, "rate", 10000, "events per second, all producers together")
	flag.IntVar(&c.partitions, "partitions", 1000, "partitions of the source queue (one entity each)")
	flag.IntVar(&c.active, "active", 0, "0 = every partition all the time; N = rotate: N partitions per second, then the next N")
	flag.IntVar(&c.batch, "batch", 50, "events per push request (all to one partition)")
	flag.IntVar(&c.producers, "producers", 32, "producer goroutines")
	flag.DurationVar(&c.duration, "duration", 120*time.Second, "producing time")
	flag.DurationVar(&c.drain, "drain", 0, "after producing, wait this long for the last windows (0 = 3 windows or 3 gaps + 10 s)")
	flag.StringVar(&c.window, "window", "tumbling", "tumbling | session")
	flag.Float64Var(&c.size, "size", 5, "tumbling window seconds")
	flag.Float64Var(&c.gap, "gap", 5, "session gap seconds")
	flag.Int64Var(&c.idleFlush, "idle-flush-ms", 1000, "runner idle flush / claim cadence, ms")
	flag.IntVar(&c.runners, "runners", 4, "stream runners (replicas of one query, one consumer group)")
	flag.IntVar(&c.runBatch, "run-batch", 200, "messages per runner cycle")
	flag.DurationVar(&c.report, "report", 10*time.Second, "report interval")
	flag.Parse()
	if c.drain == 0 {
		w := c.size
		if c.window == "session" {
			w = c.gap
		}
		c.drain = time.Duration(3*w*float64(time.Second)) + 10*time.Second
	}
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, "sload:", err)
		os.Exit(1)
	}
}

// latencies holds emit delays (ms) of the windows read in one report interval
// and in the whole run.
type latencies struct {
	mu       sync.Mutex
	interval []float64
	all      []float64
}

func (l *latencies) add(ms float64) {
	l.mu.Lock()
	l.interval = append(l.interval, ms)
	l.all = append(l.all, ms)
	l.mu.Unlock()
}

func (l *latencies) takeInterval() []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.interval
	l.interval = nil
	return out
}

func (l *latencies) snapshotAll() []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]float64(nil), l.all...)
}

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	sort.Float64s(v)
	i := int(math.Ceil(p/100*float64(len(v)))) - 1
	if i < 0 {
		i = 0
	}
	return v[i]
}

func num(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return 0
}

func run(c config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	src, sink, query := c.tag+"-src", c.tag+"-sink", c.tag+"-q"

	q, err := queen.New(c.url)
	if err != nil {
		return err
	}
	for _, name := range []string{src, sink} {
		if _, err := q.Queue(name).Create().Execute(ctx); err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
	}

	// The stream: window per partition, count + sum, the window's bounds and
	// the emit instant added, one message per closed window into the sink.
	var s *streams.Stream
	base := streams.From(q.Queue(src).AsStreamSource())
	if c.window == "session" {
		s = base.WindowSession(c.gap, streams.WithIdleFlushMs(c.idleFlush))
	} else {
		s = base.WindowTumbling(c.size, streams.WithIdleFlushMs(c.idleFlush))
	}
	s = s.Aggregate(map[string]streams.ExtractorFn{
		"count": func(m interface{}) (float64, error) { return 1, nil },
		"sum": func(m interface{}) (float64, error) {
			if mm, ok := m.(map[string]interface{}); ok {
				if d, ok := mm["data"].(map[string]interface{}); ok {
					return num(d["amount"]), nil
				}
				return num(mm["amount"]), nil
			}
			return 0, nil
		},
	}, "count", "sum").Map(func(v interface{}, ectx streams.EmitCtx) (interface{}, error) {
		out := map[string]interface{}{}
		if m, ok := v.(map[string]interface{}); ok {
			for k, x := range m {
				out[k] = x
			}
		}
		out["windowStart"] = ectx["windowStart"]
		out["windowEnd"] = ectx["windowEnd"]
		out["partition"] = ectx["partition"]
		out["emittedAt"] = float64(time.Now().UnixMilli())
		return out, nil
	}).To(q.Queue(sink))

	runners := make([]*streams.Runner, 0, c.runners)
	for i := 0; i < c.runners; i++ {
		r, err := s.Run(ctx, streams.RunOptions{
			QueryID: query, URL: c.url, BatchSize: c.runBatch, Reset: i == 0,
			SubscriptionMode: "all",
		})
		if err != nil {
			return fmt.Errorf("runner %d: %w", i, err)
		}
		runners = append(runners, r)
	}
	defer func() {
		for _, r := range runners {
			r.Stop()
		}
	}()

	// The verifier: one consumer group over the sink.
	var windows, counted, sum int64
	lat := &latencies{}
	verCtx, verStop := context.WithCancel(context.Background())
	var verWG sync.WaitGroup
	for v := 0; v < 4; v++ {
		verWG.Add(1)
		go func() {
			defer verWG.Done()
			for verCtx.Err() == nil {
				pctx, cancel := context.WithTimeout(verCtx, 2*time.Second)
				batch, err := q.Queue(sink).Group(c.tag+"-verify").SubscriptionMode("all").
					Batch(500).Wait(true).TimeoutMillis(1000).Pop(pctx)
				cancel()
				if err != nil || len(batch) == 0 {
					continue
				}
				for _, m := range batch {
					atomic.AddInt64(&windows, 1)
					atomic.AddInt64(&counted, int64(num(m.Data["count"])))
					atomic.AddInt64(&sum, int64(num(m.Data["sum"])))
					if end, at := num(m.Data["windowEnd"]), num(m.Data["emittedAt"]); end > 0 && at > 0 {
						lat.add(at - end)
					}
				}
				_, _ = q.Ack(context.Background(), batch, true, queen.AckOptions{ConsumerGroup: c.tag + "-verify"})
			}
		}()
	}

	// The producers: open loop, one request = -batch events of one partition.
	var pushed, pushErr, lost int64
	var lastErr atomic.Value
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 512}}
	reqPerSec := float64(c.rate) / float64(c.batch)
	perProducer := reqPerSec / float64(c.producers)
	start := time.Now()
	prodCtx, prodStop := context.WithTimeout(ctx, c.duration)
	defer prodStop()
	var seq int64
	var prodWG sync.WaitGroup
	for p := 0; p < c.producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			interval := time.Duration(float64(time.Second) / perProducer)
			next := time.Now()
			for prodCtx.Err() == nil {
				next = next.Add(interval)
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				n := atomic.AddInt64(&seq, 1)
				part := int(n % int64(c.partitions))
				if c.active > 0 {
					sec := int(time.Since(start).Seconds())
					part = (sec*c.active + int(n%int64(c.active))) % c.partitions
				}
				// Every event has a transactionId of its own, so a push retried
				// after an answer that never came is deduplicated by the broker:
				// "pushed" counts each event once, and the windows must count
				// exactly that many.
				var b bytes.Buffer
				b.WriteString(`{"items":[`)
				now := time.Now().UnixMilli()
				for i := 0; i < c.batch; i++ {
					if i > 0 {
						b.WriteByte(',')
					}
					fmt.Fprintf(&b, `{"queue":%q,"partition":"p%d","transactionId":"%s-%d-%d","payload":{"amount":1,"ts":%d}}`,
						src, part, c.tag, n, i, now)
				}
				b.WriteString(`]}`)
				body := b.Bytes()
				for attempt := 0; ; attempt++ {
					resp, err := client.Post(c.url+"/api/v1/push", "application/json", bytes.NewReader(body))
					if err == nil {
						// Read to the end: only a drained body returns its
						// connection to the pool (else one TCP connection per push).
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode/100 == 2 {
							break
						}
						err = fmt.Errorf("%s", resp.Status)
					}
					atomic.AddInt64(&pushErr, int64(c.batch))
					lastErr.Store(err.Error())
					if attempt >= 20 {
						// Given up: these events may or may not be stored.
						atomic.AddInt64(&lost, int64(c.batch))
						break
					}
					time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
				}
				atomic.AddInt64(&pushed, int64(c.batch))
			}
		}(p)
	}

	report := func(final bool) {
		var cyc, flush, msgs, errs int64
		var lastRunnerErr string
		for _, r := range runners {
			m := r.Metrics()
			cyc += m.CyclesTotal
			flush += m.FlushCyclesTotal
			msgs += m.MessagesTotal
			errs += m.ErrorsTotal
			if m.LastError != "" {
				lastRunnerErr = m.LastError
			}
		}
		iv := lat.takeInterval()
		le, _ := lastErr.Load().(string)
		fmt.Printf("[%s] pushed=%d pushErr=%d | windows=%d counted=%d sum=%d | emit delay p50=%.0f p99=%.0f ms (n=%d) | runners cycles=%d flush=%d msgs=%d errors=%d%s%s\n",
			time.Now().Format("15:04:05"), atomic.LoadInt64(&pushed), atomic.LoadInt64(&pushErr),
			atomic.LoadInt64(&windows), atomic.LoadInt64(&counted), atomic.LoadInt64(&sum),
			pct(iv, 50), pct(iv, 99), len(iv), cyc, flush, msgs, errs,
			map[bool]string{true: " last=" + strings.TrimSpace(lastRunnerErr), false: ""}[lastRunnerErr != ""],
			map[bool]string{true: " pushLast=" + le, false: ""}[le != ""])
		if final {
			all := lat.snapshotAll()
			p, k := atomic.LoadInt64(&pushed), atomic.LoadInt64(&counted)
			verdict := "EXACT"
			if k > p {
				verdict = fmt.Sprintf("DUPLICATES (+%d counted)", k-p)
			} else if k < p {
				verdict = fmt.Sprintf("MISSING (%d not counted)", p-k)
			}
			elapsed := time.Since(start).Seconds()
			fmt.Printf("[final] window=%s rate=%d partitions=%d active=%d runners=%d | pushed=%d (%.0f/s) push retries=%d given-up=%d | windows=%d counted=%d -> %s | emit delay p50=%.0f p90=%.0f p99=%.0f max=%.0f ms | runner errors=%d\n",
				c.window, c.rate, c.partitions, c.active, c.runners, p, float64(p)/c.duration.Seconds(), atomic.LoadInt64(&pushErr), atomic.LoadInt64(&lost),
				atomic.LoadInt64(&windows), k, verdict, pct(all, 50), pct(all, 90), pct(all, 99), pct(all, 100), errs)
			_ = elapsed
		}
	}

	tick := time.NewTicker(c.report)
	defer tick.Stop()
	done := make(chan struct{})
	go func() { prodWG.Wait(); close(done) }()
loop:
	for {
		select {
		case <-tick.C:
			report(false)
		case <-done:
			break loop
		case <-ctx.Done():
			break loop
		}
	}
	fmt.Printf("[drain] producers stopped after %s; waiting %s for the last windows\n", time.Since(start).Round(time.Second), c.drain)
	drainEnd := time.After(c.drain)
drainLoop:
	for {
		select {
		case <-tick.C:
			report(false)
			if atomic.LoadInt64(&counted) >= atomic.LoadInt64(&pushed) {
				// Every event is in a closed window: one more interval for late duplicates.
				time.Sleep(c.report)
				break drainLoop
			}
		case <-drainEnd:
			break drainLoop
		case <-ctx.Done():
			break drainLoop
		}
	}
	verStop()
	verWG.Wait()
	report(true)
	// Exit here: a runner's Stop can wait on a close in flight for as long as
	// the broker takes, and the run's numbers are already printed.
	os.Exit(0)
	return nil
}
