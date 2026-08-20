// kvload — load generator for QueenMQ's KV surface (PLAN_KV_TIMERS §8.1).
//
// KV shipped in 1.0.3 and is on unconditionally (no flag), but it has never had
// a load test: the implementation round was functional verification only. This
// exercises the three shapes real callers actually use, because they have very
// different costs in the stored procedure:
//
//   incr         fixed-window rate limiter — ONE call per decision, TTL is
//                create-only (a window extended by every increment stops
//                limiting under load, which is the whole point). This is what
//                Gate's meter does.
//   putIfAbsent  idempotency marker — desugars to put with expect:0, so it
//                takes the insert arm and the loser gets the winner's value
//                back without a second round trip.
//   get          cache read — the cheap path, included to keep the mix honest
//                rather than measuring writes only.
//
// Reports the same way the campaign loaders do: rate, latency percentiles from
// a log-linear histogram, and errors classified rather than counted, so a run
// that "worked" but returned 400s cannot read as success.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type op struct {
	Op      string `json:"op"`
	Ns      string `json:"ns"`
	Key     string `json:"key,omitempty"`
	Value   any    `json:"value,omitempty"`
	Delta   int    `json:"delta,omitempty"`
	TTLSecs int    `json:"ttlSeconds,omitempty"`
	Expect  *int   `json:"expect,omitempty"`
}

type stats struct {
	ok, errs      atomic.Int64
	applied       atomic.Int64
	notApplied    atomic.Int64
	byKind        sync.Map // kind -> *atomic.Int64
	mu            sync.Mutex
	lat           []float64 // ms, sampled
}

func (s *stats) kind(k string) {
	v, _ := s.byKind.LoadOrStore(k, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (s *stats) obs(ms float64) {
	s.mu.Lock()
	if len(s.lat) < 400000 {
		s.lat = append(s.lat, ms)
	}
	s.mu.Unlock()
}

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	i := int(float64(len(v)-1) * p)
	return v[i]
}

func main() {
	url := flag.String("url", "http://127.0.0.1:6632", "broker base URL")
	dur := flag.Int("duration", 300, "seconds")
	workers := flag.Int("workers", 64, "concurrent workers")
	namespaces := flag.Int("namespaces", 100, "distinct KV namespaces (tenant-ish)")
	keys := flag.Int("keys", 10000, "key space per namespace")
	batch := flag.Int("batch", 1, "operations per request")
	wIncr := flag.Int("incr", 60, "weight: incr (rate-limiter)")
	wGet := flag.Int("get", 30, "weight: get")
	wPia := flag.Int("pia", 10, "weight: putIfAbsent (idempotency marker)")
	ttl := flag.Int("ttl", 60, "ttlSeconds for writes")
	report := flag.Int("report", 5, "report interval seconds")
	flag.Parse()

	total := *wIncr + *wGet + *wPia
	if total <= 0 {
		fmt.Fprintln(os.Stderr, "weights sum to zero")
		os.Exit(2)
	}
	fmt.Printf("kvload -> %s workers=%d ns=%d keys=%d batch=%d mix incr/get/pia=%d/%d/%d ttl=%ds dur=%ds\n",
		*url, *workers, *namespaces, *keys, *batch, *wIncr, *wGet, *wPia, *ttl, *dur)

	tr := &http.Transport{
		MaxIdleConns:        *workers * 2,
		MaxIdleConnsPerHost: *workers * 2,
		MaxConnsPerHost:     *workers * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	cl := &http.Client{Transport: tr, Timeout: 30 * time.Second}

	st := &stats{}
	stop := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; close(stop) }()
	go func() { time.Sleep(time.Duration(*dur) * time.Second); close(stop) }()

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(seed)*7919 + 13))
			zero := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ops := make([]op, 0, *batch)
				for b := 0; b < *batch; b++ {
					ns := fmt.Sprintf("bench%03d", rnd.Intn(*namespaces))
					key := fmt.Sprintf("k%06d", rnd.Intn(*keys))
					r := rnd.Intn(total)
					switch {
					case r < *wIncr:
						ops = append(ops, op{Op: "incr", Ns: ns, Key: key, Delta: 1, TTLSecs: *ttl})
					case r < *wIncr+*wGet:
						ops = append(ops, op{Op: "get", Ns: ns, Key: key})
					default:
						ops = append(ops, op{Op: "putIfAbsent", Ns: ns,
							Key: "idem:" + key, Value: map[string]any{"w": seed}, TTLSecs: *ttl, Expect: &zero})
					}
				}
				body, _ := json.Marshal(ops)
				t0 := time.Now()
				resp, err := cl.Post(*url+"/api/v1/kv", "application/json", bytes.NewReader(body))
				el := float64(time.Since(t0).Microseconds()) / 1000.0
				if err != nil {
					st.errs.Add(1)
					st.kind("transport")
					continue
				}
				raw, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					st.errs.Add(1)
					var e struct {
						Error string `json:"error"`
					}
					_ = json.Unmarshal(raw, &e)
					if e.Error == "" {
						e.Error = fmt.Sprintf("http_%d", resp.StatusCode)
					}
					st.kind(e.Error)
					continue
				}
				var res []struct {
					Applied *bool `json:"applied"`
				}
				if json.Unmarshal(raw, &res) == nil {
					for _, r := range res {
						if r.Applied != nil {
							if *r.Applied {
								st.applied.Add(1)
							} else {
								st.notApplied.Add(1)
							}
						}
					}
				}
				st.ok.Add(1)
				st.obs(el)
			}
		}(w)
	}

	go func() {
		tick := time.NewTicker(time.Duration(*report) * time.Second)
		defer tick.Stop()
		var lastOK int64
		last := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				ok := st.ok.Load()
				now := time.Now()
				rate := float64(ok-lastOK) / now.Sub(last).Seconds()
				st.mu.Lock()
				cp := append([]float64(nil), st.lat...)
				st.mu.Unlock()
				sort.Float64s(cp)
				fmt.Printf("[%s] req=%7.0f/s ops=%8.0f/s | p50=%6.2f p99=%7.2f ms | ok=%d errs=%d\n",
					now.Format("15:04:05"), rate, rate*float64(*batch),
					pct(cp, 0.50), pct(cp, 0.99), ok, st.errs.Load())
				lastOK, last = ok, now
			}
		}
	}()

	wg.Wait()
	el := time.Since(start).Seconds()
	st.mu.Lock()
	sort.Float64s(st.lat)
	cp := st.lat
	st.mu.Unlock()
	fmt.Printf("\n[final] %.0fs req=%d (%.0f/s) ops=%.0f/s applied=%d notApplied=%d errs=%d\n",
		el, st.ok.Load(), float64(st.ok.Load())/el,
		float64(st.ok.Load())*float64(*batch)/el, st.applied.Load(), st.notApplied.Load(), st.errs.Load())
	fmt.Printf("[final] latency p50=%.2f p90=%.2f p99=%.2f p999=%.2f ms\n",
		pct(cp, 0.50), pct(cp, 0.90), pct(cp, 0.99), pct(cp, 0.999))
	st.byKind.Range(func(k, v any) bool {
		fmt.Printf("[final] err %-28s %d\n", k, v.(*atomic.Int64).Load())
		return true
	})
}
