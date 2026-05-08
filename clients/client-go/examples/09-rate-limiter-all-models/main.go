// Example 09 — Rate-limiter test, all canonical models.
//
// 1:1 port of streams/examples/09-rate-limiter-all-models.js. Validates that
// the same .Gate() primitive expresses every common rate-limit shape:
//
//	A — req/sec    : 1 token per HTTP call (Airbnb-style)
//	B — msg/sec    : 1 token per individual message (WhatsApp Cloud)
//	D — cost       : variable weight per message (TPM-style)
//	E — sliding    : sliding-window quota (SendGrid daily, OTA hourly)
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/09-rate-limiter-all-models
//
// Skip a scenario: SKIP=A,D
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/client-go"
	"github.com/smartpricing/queen/client-go/streams"
	"github.com/smartpricing/queen/client-go/streams/helpers"
)

var (
	url             string
	tenantsN        = 20
	elemsPerTenant  = 500
	runnersN        = 4
	leaseSec        = 2
	batch           = 200
	timeoutMs       = 180_000
	skip            = map[string]bool{}
	tagBase         = fmt.Sprintf("%x", time.Now().UnixMilli())
	rateReqPerSec   = 100
	rateMsgPerSec   = 100
	rateWeightPer   = 1000
	capacityReq     = rateReqPerSec * leaseSec
	capacityMsg     = rateMsgPerSec * leaseSec
	capacityWeight  = rateWeightPer * leaseSec
)

func main() {
	url = envStr("QUEEN_URL", "http://localhost:6632")
	tenantsN = envInt("TENANTS", tenantsN)
	elemsPerTenant = envInt("ELEMS_PER_TENANT", elemsPerTenant)
	runnersN = envInt("RUNNERS", runnersN)
	leaseSec = envInt("LEASE_SEC", leaseSec)
	batch = envInt("BATCH", batch)
	timeoutMs = envInt("TIMEOUT_MS", timeoutMs)
	for _, s := range strings.Split(envStr("SKIP", ""), ",") {
		if s != "" {
			skip[s] = true
		}
	}
	capacityReq = rateReqPerSec * leaseSec
	capacityMsg = rateMsgPerSec * leaseSec
	capacityWeight = rateWeightPer * leaseSec

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("RATE-LIMITER ALL-MODELS TEST")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Queen URL          = %s\n", url)
	fmt.Printf("tenants/scenario   = %d, elems/tenant = %d\n", tenantsN, elemsPerTenant)
	fmt.Printf("runners/scenario   = %d, lease=%ds, batch=%d\n", runnersN, leaseSec, batch)

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	tenants := make([]string, tenantsN)
	for i := range tenants {
		tenants[i] = fmt.Sprintf("tenant-%03d", i)
	}

	var results []scenarioResult
	if !skip["A"] {
		results = append(results, scenarioA(q, tenants))
	}
	if !skip["B"] {
		results = append(results, scenarioB(q, tenants))
	}
	if !skip["D"] {
		results = append(results, scenarioD(q, tenants))
	}
	if !skip["E"] {
		results = append(results, scenarioE(q, tenants))
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("=", 80))
	for _, r := range results {
		ok := "PASS"
		if r.Actual != r.Expected {
			ok = "PARTIAL"
		}
		fmt.Printf("  [%s] scenario %s: drained %d/%d\n", ok, r.Name, r.Actual, r.Expected)
	}
}

type scenarioResult struct {
	Name             string
	Expected, Actual int
}

func setupQueues(q *queen.Queen, src, sink string) {
	_, _ = q.Queue(src).Config(queen.QueueConfig{
		LeaseTime: leaseSec, RetryLimit: 100000,
		RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background())
	_, _ = q.Queue(sink).Config(queen.QueueConfig{
		LeaseTime: 30, RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background())
}

func pushAll(q *queen.Queen, src string, tenants []string, mk func(string, int) map[string]interface{}) int {
	for _, t := range tenants {
		for off := 0; off < elemsPerTenant; off += 500 {
			end := off + 500
			if end > elemsPerTenant {
				end = elemsPerTenant
			}
			items := make([]map[string]interface{}, end-off)
			for j := off; j < end; j++ {
				items[j-off] = mk(t, j)
			}
			_, _ = q.Queue(src).Partition(t).Push(items).Execute(context.Background())
		}
	}
	return len(tenants) * elemsPerTenant
}

func runGateStream(q *queen.Queen, src, sink string, gate streams.GateFn, qid string) []*streams.Runner {
	rs := make([]*streams.Runner, runnersN)
	for r := 0; r < runnersN; r++ {
		stream := streams.From(q.Queue(src).AsStreamSource()).Gate(gate).To(q.Queue(sink))
		runner, err := stream.Run(context.Background(), streams.RunOptions{
			QueryID: qid, URL: url,
			BatchSize:    batch,
			MaxPartitions: max(2, tenantsN/runnersN+1),
			MaxWaitMillis: 500,
			Reset:         r == 0,
		})
		if err != nil {
			panic(err)
		}
		rs[r] = runner
	}
	return rs
}

func drainUntil(q *queen.Queen, sink string, expected int, tag string) int {
	cg := "rl-" + tag
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	var drained int64
	for atomic.LoadInt64(&drained) < int64(expected) && time.Now().Before(deadline) {
		batch, err := q.Queue(sink).Group(cg).Batch(500).Wait(true).TimeoutMillis(500).Pop(context.Background())
		if err != nil || len(batch) == 0 {
			continue
		}
		atomic.AddInt64(&drained, int64(len(batch)))
		_, _ = q.Ack(context.Background(), batch, true, queen.AckOptions{ConsumerGroup: cg})
	}
	return int(atomic.LoadInt64(&drained))
}

func scenarioA(q *queen.Queen, tenants []string) scenarioResult {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SCENARIO A — req/sec (1 token per HTTP call)")
	fmt.Println(strings.Repeat("=", 80))
	src := "rl_a_req_" + tagBase
	sink := "rl_a_apx_" + tagBase
	qid := "rl-a-" + tagBase
	setupQueues(q, src, sink)
	expected := pushAll(q, src, tenants, func(t string, s int) map[string]interface{} {
		return map[string]interface{}{"tenantId": t, "seq": s, "kind": "req"}
	})
	gate := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: float64(capacityReq), RefillPerSec: float64(rateReqPerSec),
	})
	rs := runGateStream(q, src, sink, gate, qid)
	drained := drainUntil(q, sink, expected, "A-"+tagBase)
	for _, r := range rs {
		r.Stop()
	}
	return scenarioResult{Name: "A", Expected: expected, Actual: drained}
}

func scenarioB(q *queen.Queen, tenants []string) scenarioResult {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SCENARIO B — msg/sec (1 token per message)")
	fmt.Println(strings.Repeat("=", 80))
	src := "rl_b_msg_" + tagBase
	sink := "rl_b_apx_" + tagBase
	qid := "rl-b-" + tagBase
	setupQueues(q, src, sink)
	expected := pushAll(q, src, tenants, func(t string, s int) map[string]interface{} {
		return map[string]interface{}{"tenantId": t, "seq": s, "msgs": 1}
	})
	gate := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: float64(capacityMsg), RefillPerSec: float64(rateMsgPerSec),
	})
	rs := runGateStream(q, src, sink, gate, qid)
	drained := drainUntil(q, sink, expected, "B-"+tagBase)
	for _, r := range rs {
		r.Stop()
	}
	return scenarioResult{Name: "B", Expected: expected, Actual: drained}
}

func scenarioD(q *queen.Queen, tenants []string) scenarioResult {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SCENARIO D — cost-weighted (variable weight per message)")
	fmt.Println(strings.Repeat("=", 80))
	src := "rl_d_cost_" + tagBase
	sink := "rl_d_apx_" + tagBase
	qid := "rl-d-" + tagBase
	setupQueues(q, src, sink)
	expected := pushAll(q, src, tenants, func(t string, s int) map[string]interface{} {
		return map[string]interface{}{"tenantId": t, "seq": s, "weight": (s % 10) + 1}
	})
	gate := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: float64(capacityWeight), RefillPerSec: float64(rateWeightPer),
		CostFn: func(msg interface{}) float64 {
			if m, ok := msg.(map[string]interface{}); ok {
				if w, ok := m["weight"].(float64); ok {
					return w
				}
				if w, ok := m["weight"].(int); ok {
					return float64(w)
				}
			}
			return 1
		},
	})
	rs := runGateStream(q, src, sink, gate, qid)
	drained := drainUntil(q, sink, expected, "D-"+tagBase)
	for _, r := range rs {
		r.Stop()
	}
	return scenarioResult{Name: "D", Expected: expected, Actual: drained}
}

func scenarioE(q *queen.Queen, tenants []string) scenarioResult {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SCENARIO E — sliding-window quota")
	fmt.Println(strings.Repeat("=", 80))
	src := "rl_e_slide_" + tagBase
	sink := "rl_e_apx_" + tagBase
	qid := "rl-e-" + tagBase
	setupQueues(q, src, sink)
	expected := pushAll(q, src, tenants, func(t string, s int) map[string]interface{} {
		return map[string]interface{}{"tenantId": t, "seq": s}
	})
	gate := helpers.SlidingWindowGate(helpers.SlidingWindowGateOptions{Limit: 100, WindowSec: 1})
	rs := runGateStream(q, src, sink, gate, qid)
	drained := drainUntil(q, sink, expected, "E-"+tagBase)
	for _, r := range rs {
		r.Stop()
	}
	return scenarioResult{Name: "E", Expected: expected, Actual: drained}
}

func envStr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
