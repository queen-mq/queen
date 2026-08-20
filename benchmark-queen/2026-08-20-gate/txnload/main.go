// txnload — the broker's OWN transaction-wire ceiling, in Gate's relay shape.
//
// Gate's measured ceiling (1,384 admitted/s after the ack-fix) left the 32-core
// rig ~90% idle: PG at 2.8 cores, broker under one. That number is therefore a
// property of Gate's relay pipeline (serial, lease-paced per lane), not of the
// broker. This loader asks the question Alice actually asked — how many
// relay-shaped transactions per second can the BROKER sustain — by running the
// same hop shape at real concurrency: W workers, each popping a batch from
// queue A and committing ONE /api/v1/transaction that pushes the batch to
// queue B and acks it from A. Ack+push in one call, the Gate relay contract.
//
// A feeder keeps queue A stocked so the pops never starve; the reported rate
// counts committed transactions and relayed items, and the latency is the
// transaction call itself (not queue depth).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	base    = flag.String("url", "http://127.0.0.1:6632", "broker base URL")
	workers = flag.Int("workers", 32, "concurrent relay workers")
	batch   = flag.Int("batch", 50, "items per pop / per transaction")
	dur     = flag.Int("duration", 60, "seconds")
	parts   = flag.Int("partitions", 16, "partitions on both queues")
	report  = flag.Int("report", 5, "report interval s")
)

type stat struct {
	txns, items, errs atomic.Int64
	mu                sync.Mutex
	lat               []float64
}

func (s *stat) obs(ms float64) {
	s.mu.Lock()
	if len(s.lat) < 500000 {
		s.lat = append(s.lat, ms)
	}
	s.mu.Unlock()
}
func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[int(float64(len(v)-1)*p)]
}

func post(cl *http.Client, url string, body any) (int, []byte, error) {
	b, _ := json.Marshal(body)
	resp, err := cl.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, raw, nil
}

func main() {
	flag.Parse()
	tr := &http.Transport{MaxIdleConns: *workers * 2, MaxIdleConnsPerHost: *workers * 2, MaxConnsPerHost: *workers * 2}
	cl := &http.Client{Transport: tr, Timeout: 60 * time.Second}

	qa, qb := "txnbench-a", "txnbench-b"
	// provision with enough partitions to spread the relay row locks
	for _, q := range []string{qa, qb} {
		code, raw, err := post(cl, *base+"/api/v1/configure", map[string]any{
			"queue": q, "options": map[string]any{"leaseTime": 30, "retentionEnabled": true, "retentionSeconds": 3600},
		})
		if err != nil || code/100 != 2 {
			fmt.Fprintf(os.Stderr, "configure %s: %d %s %v\n", q, code, string(raw), err)
			os.Exit(1)
		}
	}

	st := &stat{}
	stop := make(chan struct{})
	go func() { time.Sleep(time.Duration(*dur) * time.Second); close(stop) }()

	// ---- feeders: keep queue A stocked (plain pushes, spread over partitions).
	// Several of them: one goroutine tops out ~28k items/s and becomes the
	// measurement's ceiling instead of the broker's.
	var fed atomic.Int64
	for f := 0; f < 4; f++ {
	go func(fid int) {
		n := fid * 1000000
		for {
			select {
			case <-stop:
				return
			default:
			}
			// stay ~2 batches per worker ahead of the relays
			if fed.Load()-st.items.Load() > int64(*workers**batch*2) {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			items := make([]map[string]any, 0, 200)
			for i := 0; i < 200; i++ {
				n++
				items = append(items, map[string]any{
					"queue": qa, "partition": fmt.Sprintf("p%d", n%*parts),
					"payload": map[string]any{"n": n},
				})
			}
			code, _, err := post(cl, *base+"/api/v1/push", map[string]any{"items": items})
			if err == nil && code/100 == 2 {
				fed.Add(int64(len(items)))
			}
		}
	}(f)
	}

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			group := "relay"
			lane := fmt.Sprintf("p%d", id%*parts)
			popURL := fmt.Sprintf("%s/api/v1/pop/queue/%s/partition/%s?consumerGroup=%s&batch=%d&wait=false&subscriptionMode=all",
				*base, qa, lane, group, *batch)
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := cl.Get(popURL)
				if err != nil {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				raw, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var pr struct {
					Messages []struct {
						TransactionID string          `json:"transactionId"`
						PartitionID   string          `json:"partitionId"`
						Payload       json.RawMessage `json:"data"`
					} `json:"messages"`
				}
				if json.Unmarshal(raw, &pr) != nil || len(pr.Messages) == 0 {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				// ONE transaction: push the batch to B + ack it from A (the relay)
				ops := make([]map[string]any, 0, 2)
				items := make([]map[string]any, 0, len(pr.Messages))
				for i, m := range pr.Messages {
					_ = i
					items = append(items, map[string]any{
						"queue": qb, "partition": lane,
						"payload":       json.RawMessage(m.Payload),
						"transactionId": m.TransactionID + ":r", // downstream reuses upstream id (Gate contract)
					})
				}
				ops = append(ops, map[string]any{"type": "push", "items": items})
				for _, m := range pr.Messages {
					ops = append(ops, map[string]any{
						"type": "ack", "transactionId": m.TransactionID,
						"partitionId": m.PartitionID, "status": "completed", "consumerGroup": group,
					})
				}
				t0 := time.Now()
				scode, sraw, serr := post(cl, *base+"/api/v1/transaction", map[string]any{"operations": ops})
				el := float64(time.Since(t0).Microseconds()) / 1000.0
				var res struct {
					Success bool `json:"success"`
				}
				if serr != nil || scode/100 != 2 || json.Unmarshal(sraw, &res) != nil || !res.Success {
					st.errs.Add(1)
					if st.errs.Load() < 3 {
						fmt.Fprintf(os.Stderr, "txn err code=%d body=%.200s\n", scode, string(sraw))
					}
					continue
				}
				st.txns.Add(1)
				st.items.Add(int64(len(pr.Messages)))
				st.obs(el)
			}
		}(w)
	}

	go func() {
		tick := time.NewTicker(time.Duration(*report) * time.Second)
		defer tick.Stop()
		var lt, li int64
		last := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				t, i := st.txns.Load(), st.items.Load()
				now := time.Now()
				d := now.Sub(last).Seconds()
				st.mu.Lock()
				cp := append([]float64(nil), st.lat...)
				st.mu.Unlock()
				sort.Float64s(cp)
				fmt.Printf("[%s] txn=%6.0f/s items=%7.0f/s | txn p50=%6.2f p99=%7.2f ms | errs=%d fed=%d\n",
					now.Format("15:04:05"), float64(t-lt)/d, float64(i-li)/d,
					pct(cp, 0.50), pct(cp, 0.99), st.errs.Load(), fed.Load())
				lt, li, last = t, i, now
			}
		}
	}()

	wg.Wait()
	st.mu.Lock()
	sort.Float64s(st.lat)
	cp := st.lat
	st.mu.Unlock()
	el := float64(*dur)
	fmt.Printf("\n[final] workers=%d batch=%d txns=%d (%.0f/s) items=%d (%.0f/s) errs=%d\n",
		*workers, *batch, st.txns.Load(), float64(st.txns.Load())/el,
		st.items.Load(), float64(st.items.Load())/el, st.errs.Load())
	fmt.Printf("[final] txn-call latency p50=%.2f p90=%.2f p99=%.2f ms\n",
		pct(cp, 0.50), pct(cp, 0.90), pct(cp, 0.99))
}
