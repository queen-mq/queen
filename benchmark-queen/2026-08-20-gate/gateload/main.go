// gateload — throughput and multi-hop latency for Gate (queen-rrl).
//
// Gate paces egress through a declared graph: work is pushed at an ENTRY node,
// relayed hop by hop (each relay is one queen transaction carrying ack+push
// together), and consumed at a TERMINAL. Every hop can carry budgets, and an
// item is admitted only when every budget on the hop admits it.
//
// What this measures, and why each number is separate:
//
//   admitted/s at the terminal   the only rate that means anything: what Gate
//                                actually lets out. Push rate is not throughput
//                                — pushing faster than the budget just grows
//                                the backlog Gate is holding on purpose.
//   e2e latency                  push -> available at the terminal, measured
//                                from a stamp inside the payload. On a budgeted
//                                graph this is DOMINATED BY THE PACING, not by
//                                Gate's overhead: an item that waits for budget
//                                is working as designed. Read it together with
//                                the admitted rate, never alone.
//   per-hop delta                same run at 1 hop vs 2 hops isolates what a
//                                relay costs, which is the question "multihop"
//                                actually asks.
//
// Unlimited runs (-cap 0) are the ones that report Gate's own ceiling, because
// nothing is waiting for budget.
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
	gate  = flag.String("gate", "http://127.0.0.1:8788", "Gate internal API")
	app   = flag.String("app", "bench", "application")
	graph = flag.String("graph", "g1", "graph name")
	hops  = flag.Int("hops", 1, "1 = entry->terminal, 2 = entry->mid->terminal")
	cap_  = flag.Int("cap", 0, "terminal budget cap (0 = no budget: measures Gate's ceiling)")
	per   = flag.Int("period", 10, "terminal budget periodSeconds")
	midCap = flag.Int("mid-cap", 0, "interior node cap when -hops 2 (0 = none)")
	parts = flag.Int("partitions", 16, "admitted partitions on the terminal")
	pushers  = flag.Int("pushers", 32, "concurrent pushers")
	consumers = flag.Int("consumers", 8, "concurrent consumers")
	batch = flag.Int("batch", 100, "consume batch")
	waitMs = flag.Int("wait-ms", 1000, "consume long-poll wait")
	dur   = flag.Int("duration", 120, "seconds")
	rate  = flag.Int("rate", 0, "target push/s (0 = as fast as possible)")
	report = flag.Int("report", 5, "report interval s")
	leaseSec = flag.Int("lease", 5, "pacing leaseSeconds")
	pacBatch = flag.Int("pace-batch", 250, "pacing batch")
)

type stat struct {
	pushed, admitted, pushErr, popErr, ackErr atomic.Int64
	mu  sync.Mutex
	lat []float64
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

func budgets(id string, cap int, period int) []map[string]any {
	if cap <= 0 {
		return []map[string]any{}
	}
	return []map[string]any{{
		"id": id, "cap": cap, "periodSeconds": period, "alignment": "rolling",
		"confidence": "documented", "source": "bench", "asOf": "2026-08-20",
	}}
}

func declare(cl *http.Client) error {
	nodes := map[string]any{}
	edges := []map[string]any{}
	cost := map[string]any{"field": "httpCost", "default": 1, "max": 100}

	nodes["entry"] = map[string]any{"entry": true, "budgets": []map[string]any{}, "cost": cost}
	term := map[string]any{
		"budgets":  budgets("term", *cap_, *per),
		"cost":     cost,
		"admitted": map[string]any{"partitionBy": "connection", "partitions": *parts},
		"pacing":   map[string]any{"leaseSeconds": *leaseSec, "batch": *pacBatch},
	}
	if *hops >= 2 {
		nodes["mid"] = map[string]any{
			"budgets": budgets("mid", *midCap, *per), "cost": cost,
			"admitted": map[string]any{"partitionBy": "connection", "partitions": *parts},
			"pacing":   map[string]any{"leaseSeconds": *leaseSec, "batch": *pacBatch},
		}
		edges = append(edges,
			map[string]any{"from": "entry", "to": "mid", "priority": 0},
			map[string]any{"from": "mid", "to": "term", "priority": 0})
	} else {
		edges = append(edges, map[string]any{"from": "entry", "to": "term", "priority": 0})
	}
	nodes["term"] = term

	doc := map[string]any{
		"version": 1, "nodes": nodes, "edges": edges,
		"consume": []string{"term"},
	}
	b, _ := json.Marshal(doc)
	req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/v1/apps/%s/graphs/%s", *gate, *app, *graph), bytes.NewReader(b))
	req.Header.Set("content-type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("declare %d: %s", resp.StatusCode, string(raw))
	}
	fmt.Printf("declared %s hops=%d cap=%d/%ds midCap=%d parts=%d\n  -> %s\n",
		*graph, *hops, *cap_, *per, *midCap, *parts, truncate(string(raw), 300))
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func main() {
	flag.Parse()
	tr := &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512, MaxConnsPerHost: 512}
	cl := &http.Client{Transport: tr, Timeout: 60 * time.Second}

	if err := declare(cl); err != nil {
		fmt.Fprintln(os.Stderr, "declare failed:", err)
		os.Exit(1)
	}
	time.Sleep(2 * time.Second) // let the runners come up

	st := &stat{}
	stop := make(chan struct{})
	go func() { time.Sleep(time.Duration(*dur) * time.Second); close(stop) }()

	var wg sync.WaitGroup
	// ---- pushers ---------------------------------------------------------
	perPusher := 0
	if *rate > 0 {
		perPusher = *rate / *pushers
	}
	for p := 0; p < *pushers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var tick *time.Ticker
			if perPusher > 0 {
				tick = time.NewTicker(time.Second / time.Duration(perPusher))
				defer tick.Stop()
			}
			n := 0
			url := fmt.Sprintf("%s/v1/apps/%s/graphs/%s/nodes/entry/push", *gate, *app, *graph)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if tick != nil {
					<-tick.C
				}
				n++
				body, _ := json.Marshal(map[string]any{
					"op": "bench.push", "cost": 1,
					"txn": fmt.Sprintf("w%d-%d", id, n),
					"payload": map[string]any{
						"connection": fmt.Sprintf("c%d", id%64),
						"t":          time.Now().UnixMicro(),
					},
				})
				resp, err := cl.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					st.pushErr.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					st.pushErr.Add(1)
					continue
				}
				st.pushed.Add(1)
			}
		}(p)
	}

	// ---- consumers -------------------------------------------------------
	for c := 0; c < *consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nextURL := fmt.Sprintf("%s/v1/apps/%s/graphs/%s/nodes/term/next?batch=%d&wait_ms=%d",
				*gate, *app, *graph, *batch, *waitMs)
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := cl.Get(nextURL)
				if err != nil {
					st.popErr.Add(1)
					continue
				}
				raw, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					st.popErr.Add(1)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				var r struct {
					Items []struct {
						Payload map[string]any `json:"payload"`
					} `json:"items"`
					Lease  json.RawMessage `json:"lease"`
					Target string          `json:"target"`
					Lane   string          `json:"lane"`
				}
				if json.Unmarshal(raw, &r) != nil || len(r.Items) == 0 {
					continue
				}
				now := time.Now().UnixMicro()
				for _, it := range r.Items {
					if t, ok := it.Payload["t"].(float64); ok {
						st.obs(float64(now-int64(t)) / 1000.0)
					}
				}
				st.admitted.Add(int64(len(r.Items)))
				ack, _ := json.Marshal(map[string]any{
					"lease": r.Lease, "up_to": len(r.Items), "calls": len(r.Items),
					"outcome": "ok", "application": *app, "target": r.Target,
					"lane": r.Lane, "op": "bench.push",
				})
				ar, err := cl.Post(*gate+"/v1/leases/ack", "application/json", bytes.NewReader(ack))
				if err != nil {
					st.ackErr.Add(1)
					continue
				}
				io.Copy(io.Discard, ar.Body)
				ar.Body.Close()
				if ar.StatusCode/100 != 2 {
					st.ackErr.Add(1)
				}
			}
		}()
	}

	// ---- reporter --------------------------------------------------------
	go func() {
		tk := time.NewTicker(time.Duration(*report) * time.Second)
		defer tk.Stop()
		var lp, la int64
		last := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				p, a := st.pushed.Load(), st.admitted.Load()
				now := time.Now()
				d := now.Sub(last).Seconds()
				st.mu.Lock()
				cp := append([]float64(nil), st.lat...)
				st.mu.Unlock()
				sort.Float64s(cp)
				fmt.Printf("[%s] push=%7.0f/s admitted=%7.0f/s | e2e p50=%8.1f p99=%9.1f ms | backlog=%d | err push=%d pop=%d ack=%d\n",
					now.Format("15:04:05"), float64(p-lp)/d, float64(a-la)/d,
					pct(cp, 0.50), pct(cp, 0.99), p-a,
					st.pushErr.Load(), st.popErr.Load(), st.ackErr.Load())
				lp, la, last = p, a, now
			}
		}
	}()

	wg.Wait()
	st.mu.Lock()
	sort.Float64s(st.lat)
	cp := st.lat
	st.mu.Unlock()
	fmt.Printf("\n[final] hops=%d cap=%d/%ds pushed=%d admitted=%d (%.0f/s) backlog=%d\n",
		*hops, *cap_, *per, st.pushed.Load(), st.admitted.Load(),
		float64(st.admitted.Load())/float64(*dur), st.pushed.Load()-st.admitted.Load())
	fmt.Printf("[final] e2e p50=%.1f p90=%.1f p99=%.1f ms (n=%d)\n",
		pct(cp, 0.50), pct(cp, 0.90), pct(cp, 0.99), len(cp))
	fmt.Printf("[final] errors push=%d pop=%d ack=%d\n",
		st.pushErr.Load(), st.popErr.Load(), st.ackErr.Load())
}
