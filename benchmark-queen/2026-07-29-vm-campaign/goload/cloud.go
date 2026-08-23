package main

// cloud.go — `-mode cloud`: the multi-tenant workload for the queen_proxy
// campaign. Drives BOTH targets with the same code path, the same pacer and the
// same accounting, so a proxy number and a broker number are comparable:
//
//	-target proxy    every simulated tenant is a REAL tenant: its own cluster,
//	                 its own API key, its own Host. One queen.Client per tenant
//	                 with BearerToken + Headers{"Host": <cluster slug>} (client-go
//	                 maps a configured Host header onto req.Host; net/http ignores
//	                 Header["Host"], which is why this used to be impossible).
//
//	-target broker   direct to the broker, no proxy in the path — the July
//	                 baseline shape. With -broker-tenant-header (default) each
//	                 tenant sends the same `x-queen-tenant` UUID the proxy would
//	                 have injected (clusters.broker_tenant_uuid), so the two
//	                 targets exercise the SAME broker-side tenant rows and the A/B
//	                 isolates the proxy hop. With -broker-tenant-header=false the
//	                 header is omitted entirely (single shared client, everything
//	                 lands in the default tenant) — the pre-tenancy control.
//
// -shared-queue (default true) makes every tenant use the SAME queue name and
// the SAME consumer group name. That is the shared-cell shape the campaign
// hinges on: it stresses the tenant-keyed hot-list ring and the wake gates, and
// it turns tenant isolation into something the run can FALSIFY — any message of
// tenant B delivered to tenant A's consumer is counted as a cross-tenant
// delivery and fails the run.
//
// Correctness is measured IN the run (never in a separate pass): every message
// carries (tenant, monotonic seq); after the producers stop, consumers keep
// draining for -drain-sec, then the sent/received bitmaps are diffed per tenant
// for LOSS, DUPLICATION, EXTRAS and CROSS-TENANT delivery. -fault injects a
// deliberate discrepancy so the checker itself can be shown to fail.
//
// Every run writes <out>/<run-id>.json (full summary incl. per-tenant verdicts,
// error counts by kind AND by proxy code, latency percentiles, bytes) and
// <out>/<run-id>-interval.csv (per-report-interval time series).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// ---------------------------------------------------------------------------
// fault injection (proves the checker can fail)
// ---------------------------------------------------------------------------

type faultSpec struct {
	dupPush int64 // push N requests a second time -> DUPLICATION
	loseMsg int64 // claim N messages as sent but never push them -> LOSS
	dropAck int64 // receive N batches and never ack them -> redelivery (DUPLICATION, after leaseTime)

	dupDone  int64
	loseDone int64
	dropDone int64
}

func parseFaults(spec string) (*faultSpec, error) {
	f := &faultSpec{}
	if strings.TrimSpace(spec) == "" || spec == "none" {
		return f, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			v = "1"
			k = part
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("bad fault count in %q", part)
		}
		switch strings.TrimSpace(k) {
		case "dup-push":
			f.dupPush = n
		case "lose-msg":
			f.loseMsg = n
		case "drop-ack":
			f.dropAck = n
		default:
			return nil, fmt.Errorf("unknown fault %q (want dup-push|lose-msg|drop-ack)", k)
		}
	}
	return f, nil
}

func (f *faultSpec) any() bool { return f.dupPush > 0 || f.loseMsg > 0 || f.dropAck > 0 }

// take decrements a budget atomically, returning true if this caller got one.
func take(budget, done *int64) bool {
	for {
		cur := atomic.LoadInt64(budget)
		if cur <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt64(budget, cur, cur-1) {
			atomic.AddInt64(done, 1)
			return true
		}
	}
}

// ---------------------------------------------------------------------------
// per-tenant runtime state
// ---------------------------------------------------------------------------

type cloudTenant struct {
	cred   TenantCred
	client *queen.Queen
	queue  string
	group  string

	seq  int64 // atomic: last sequence handed out
	part uint64

	v *tenantVerify

	pushed, popped, acked int64 // atomic
}

// ---------------------------------------------------------------------------
// report shapes
// ---------------------------------------------------------------------------

type latSummary struct {
	Samples int64   `json:"samples"`
	P50Ms   float64 `json:"p50Ms"`
	P95Ms   float64 `json:"p95Ms"`
	P99Ms   float64 `json:"p99Ms"`
	P999Ms  float64 `json:"p999Ms"`
	MaxMs   float64 `json:"maxMs"`
}

func summarize(h *olHist, maxUs *int64) latSummary {
	buf := make([]int64, olNumBuckets)
	h.snapshot(buf)
	var n int64
	for _, c := range buf {
		n += c
	}
	return latSummary{
		Samples: n,
		P50Ms:   olPercentile(buf, 0.50),
		P95Ms:   olPercentile(buf, 0.95),
		P99Ms:   olPercentile(buf, 0.99),
		P999Ms:  olPercentile(buf, 0.999),
		MaxMs:   float64(atomic.LoadInt64(maxUs)) / 1000.0,
	}
}

func recordMax(dst *int64, v int64) {
	for {
		cur := atomic.LoadInt64(dst)
		if v <= cur || atomic.CompareAndSwapInt64(dst, cur, v) {
			return
		}
	}
}

type RunReport struct {
	RunID     string                 `json:"runId"`
	Tool      string                 `json:"tool"`
	StartedAt string                 `json:"startedAt"`
	EndedAt   string                 `json:"endedAt"`
	LoadSec   float64                `json:"loadSec"`
	DrainSec  float64                `json:"drainSec"`
	Config    map[string]interface{} `json:"config"`
	Offered   OfferedBlock           `json:"offered"`
	Achieved  AchievedBlock          `json:"achieved"`
	Latency   map[string]latSummary  `json:"latency"`
	Errors    map[string]errCounts   `json:"errors"`
	Bytes     BytesBlock             `json:"bytes"`
	Verify    VerifyBlock            `json:"verify"`
	Faults    map[string]int64       `json:"faultsInjected"`
	Verdict   string                 `json:"verdict"`
}

type OfferedBlock struct {
	MsgPerSec float64 `json:"msgPerSec"`
	ReqPerSec float64 `json:"reqPerSec"`
	Msgs      int64   `json:"msgs"`
	Reqs      int64   `json:"reqs"`
	ShedMsgs  int64   `json:"shedMsgs"`
	ShedReqs  int64   `json:"shedReqs"`
}

type AchievedBlock struct {
	PushMsgPerSec float64 `json:"pushMsgPerSec"`
	// PopMsgPerSecLoad is the delivery rate DURING the load phase — the number
	// comparable with pushMsgPerSec. PopMsgPerSec averages the same deliveries
	// over load+drain and is therefore always lower; it exists only to show how
	// much of the traffic had to be mopped up after the producers stopped.
	PopMsgPerSecLoad float64 `json:"popMsgPerSecLoadPhase"`
	PopMsgPerSec     float64 `json:"popMsgPerSecWholeRun"`
	AckMsgPerSec     float64 `json:"ackMsgPerSecWholeRun"`
	PushedMsgs       int64   `json:"pushedMsgs"`
	PoppedMsgs       int64   `json:"poppedMsgs"`
	AckedMsgs        int64   `json:"ackedMsgs"`
	PushReqs         int64   `json:"pushReqs"`
	PopReqs          int64   `json:"popReqs"`
	AckReqs          int64   `json:"ackReqs"`
	EmptyPops        int64   `json:"emptyPops"`
}

type BytesBlock struct {
	PerMsgNominal    int64  `json:"perMsgNominalBytes"`
	PushedNominal    int64  `json:"pushedNominalBytes"`
	DeliveredNominal int64  `json:"deliveredNominalBytes"`
	Note             string `json:"note"`
}

type VerifyBlock struct {
	Enabled     bool           `json:"enabled"`
	Verdict     string         `json:"verdict"`
	SentOK      int64          `json:"sentOk"`
	Received    int64          `json:"received"`
	Missing     int64          `json:"missing"`
	Duplicate   int64          `json:"duplicate"`
	Extra       int64          `json:"extra"`
	CrossTenant int64          `json:"crossTenant"`
	Undecodable int64          `json:"undecodable"`
	Tenants     []TenantResult `json:"tenants"`
}

// ---------------------------------------------------------------------------
// the mode
// ---------------------------------------------------------------------------

func runCloudMode(args []string) {
	fs := flag.NewFlagSet("goload-cloud", flag.ExitOnError)

	target := fs.String("target", "proxy", "proxy | broker — proxy drives real tenants through queen_proxy (Host + api key per tenant); broker goes straight to the cell broker")
	urlFlag := fs.String("url", "", "base URL (default: http://127.0.0.1:6711 for -target proxy, http://127.0.0.1:6632 for -target broker)")
	tenantsFile := fs.String("tenants-file", "tenants.json", "credential file from -mode provision (REQUIRED for -target proxy)")
	nTenants := fs.Int("tenants", 0, "how many tenants from the file to drive (0 = all)")
	brokerTenantHdr := fs.Bool("broker-tenant-header", true, "-target broker: send x-queen-tenant (the same UUID the proxy injects) so both targets hit the same broker-side tenant rows. false = no header at all (pre-tenancy control, single shared client)")

	sharedQueue := fs.Bool("shared-queue", true, "every tenant uses the SAME queue name and consumer group (shared-cell shape). false = per-tenant queue <queue>-tNNNN")
	queueName := fs.String("queue", "orders", "queue name (shared) or prefix (per-tenant)")
	groupName := fs.String("group", "workers", "consumer group name")
	partitions := fs.Int("partitions", 1, "partitions per queue (<=1 = default partition)")

	rate := fs.Int("rate", 0, "TOTAL offered msg/s across all tenants (open-loop, coordinated-omission correct)")
	perTenantRate := fs.Float64("per-tenant-rate", 0, "offered msg/s PER TENANT (overrides -rate)")
	pushBatch := fs.Int("push-batch", 1, "messages per push request")
	producersPer := fs.Int("producers-per-tenant", 1, "pacer goroutines per tenant (raise when a single tenant's request rate exceeds ~2k/s: one goroutine cannot time finer than the runtime timer)")
	maxInflight := fs.Int("max-inflight", 4096, "cap on in-flight push REQUESTS; over the cap the request is SHED (counted, not sent) so the pacer never degenerates into closed-loop")

	consumersPer := fs.Int("consumers-per-tenant", 1, "consumer goroutines per tenant")
	popBatch := fs.Int("pop-batch", 100, "max messages per pop")
	popWait := fs.Bool("pop-wait", true, "long-poll pop (wait=true) — the realistic consumer shape and what holds a proxy parked slot")
	popTimeout := fs.Int("pop-timeout", 5000, "long-poll timeout ms")
	popPartitions := fs.Int("pop-partitions", 1, "claim up to N partitions per pop call (v4 multi-partition wildcard)")
	activeFrac := fs.Float64("active-fraction", 1.0, "fraction of each queue's partitions that actually RECEIVE traffic (1.0 = every partition, the uniform default; 0.002 = production-like, where a large cold tail exists but only a small working set is hot)")
	autoAck := fs.Bool("auto-ack", false, "server-side autoAck inside the pop instead of a leased pop + explicit batch ack (the default, honest consumer path)")
	emptySleepMs := fs.Int("empty-sleep", 5, "sleep ms after an empty pop (only when -pop-wait=false)")

	payloadBytes := fs.Int("payload", 256, "payload padding bytes")
	durationSec := fs.Int("duration", 60, "LOAD duration seconds")
	drainSec := fs.Int("drain", 15, "after producers stop, keep consuming this long before the correctness verdict (an in-flight tail is not loss)")
	reportSec := fs.Int("report", 5, "report interval seconds")

	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	idleConns := fs.Int("idle-conns", 0, "MaxIdleConnsPerHost PER TENANT CLIENT (0 = auto: consumers + inflight share + 4)")
	retry429 := fs.Int("retry429-attempts", 1, "client 429 policy: 1 = surface the 429 immediately (default — the campaign MEASURES 429s, it does not hide them), n>1 = bounded client backoff, 0 = client-go kind defaults (bounded push / unbounded long-poll)")
	retries := fs.Int("retries", 0, "5xx/network retry attempts (0 = none: a failed request is counted and dropped, never re-offered)")

	completedRet := fs.Int("completed-retention", 300, "completedRetentionSeconds on each queue")
	pendingRet := fs.Int("pending-retention", 3600, "retentionSeconds for pending messages")
	leaseTime := fs.Int("lease-time", 30, "queue leaseTime seconds")
	dedupWindow := fs.Int("dedup-window", 0, "dedupWindowSeconds (0 = off)")
	// TASK M: queue-level minimum pop wait, in MILLISECONDS. Posted in the raw
	// options bag (client-go's QueueConfig struct has no field for it), so the
	// sweep configures the queue exactly the way a tenant would.
	minPopWait := fs.Int("min-pop-wait", 0, "minPopWaitTime ms on each queue (0 = off)")
	skipConfigure := fs.Bool("skip-configure", false, "do not configure queues at t=0 (assume they exist)")

	verify := fs.Bool("verify", true, "per-message (tenant, seq) delivery accounting: loss / duplication / cross-tenant")
	failOnVerify := fs.Bool("fail-on-verify", true, "exit 3 when the correctness verdict is FAIL")
	fault := fs.String("fault", "", "deliberate discrepancy to prove the checker works: dup-push=N,lose-msg=N,drop-ack=N")

	outDir := fs.String("out", ".", "directory for <run-id>.json and <run-id>-interval.csv")
	runID := fs.String("run-id", "", "run identifier (default: cloud-<target>-<unix>)")
	note := fs.String("note", "", "free-text note recorded in the run JSON (cell shape, experiment name, ...)")
	_ = fs.String("mode", "cloud", "run mode")
	_ = fs.Parse(args)

	// ------------------------------------------------------------------ setup
	if *target != "proxy" && *target != "broker" {
		fmt.Println("goload -mode cloud: -target must be proxy or broker")
		os.Exit(2)
	}
	baseURL := *urlFlag
	if baseURL == "" {
		if *target == "proxy" {
			baseURL = "http://127.0.0.1:6711"
		} else {
			baseURL = "http://127.0.0.1:6632"
		}
	}
	if *pushBatch <= 0 || *popBatch <= 0 {
		fmt.Println("goload -mode cloud: -push-batch and -pop-batch must be > 0")
		os.Exit(2)
	}
	faults, ferr := parseFaults(*fault)
	if ferr != nil {
		fmt.Printf("goload -mode cloud: %v\n", ferr)
		os.Exit(2)
	}

	// Credentials. -target proxy REQUIRES them (an api key is bound to its
	// cluster; a wrong pairing is a 403, not a silent fallback).
	var creds []TenantCred
	tf, terr := loadTenantsFile(*tenantsFile)
	switch {
	case terr == nil:
		creds = tf.Tenants
	case *target == "proxy":
		fmt.Printf("goload -mode cloud -target proxy: cannot read %s: %v\n  run: goload -mode provision -tenants N -file %s\n", *tenantsFile, terr, *tenantsFile)
		os.Exit(2)
	default:
		// broker target without a file: synthesize identities. The tenant UUIDs
		// are deterministic so a re-run hits the same broker rows.
		n := *nTenants
		if n <= 0 {
			n = 1
		}
		for i := 0; i < n; i++ {
			creds = append(creds, TenantCred{
				Idx:          i,
				TenantSlug:   fmt.Sprintf("synth-%04d", i),
				ClusterSlug:  fmt.Sprintf("synth-%04d", i),
				BrokerTenant: deterministicUUID(i),
			})
		}
		fmt.Printf("[cloud] no tenants file (%v) -> synthesized %d broker-only tenant identities\n", terr, n)
	}
	if *nTenants > 0 && *nTenants < len(creds) {
		creds = creds[:*nTenants]
	}
	if len(creds) == 0 {
		fmt.Println("goload -mode cloud: no tenants to drive")
		os.Exit(2)
	}
	nT := len(creds)

	// Offered rate.
	ptRate := *perTenantRate
	if ptRate <= 0 {
		if *rate <= 0 {
			fmt.Println("goload -mode cloud: set -rate (total msg/s) or -per-tenant-rate")
			os.Exit(2)
		}
		ptRate = float64(*rate) / float64(nT)
	}
	totalRate := ptRate * float64(nT)
	ptReqRate := ptRate / float64(*pushBatch)
	if *producersPer < 1 {
		*producersPer = 1
	}
	perProducerRPS := ptReqRate / float64(*producersPer)
	if perProducerRPS <= 0 {
		fmt.Println("goload -mode cloud: computed per-producer request rate is 0")
		os.Exit(2)
	}

	idle := *idleConns
	if idle <= 0 {
		idle = *consumersPer + *maxInflight/nT + 4
		if idle > 512 {
			idle = 512
		}
	}

	rid := *runID
	if rid == "" {
		rid = fmt.Sprintf("cloud-%s-%d", *target, time.Now().Unix())
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Printf("cannot create -out %s: %v\n", *outDir, err)
		os.Exit(1)
	}

	fmt.Printf("goload -mode cloud [%s] -> %s\n", rid, baseURL)
	// Traffic lands on partitions p0..p(activeParts-1); the remainder are created
	// by configure and then stay dormant. Real deployments look like this — 53k
	// partitions with a few hundred hot at any second — and spreading load evenly
	// over every partition is the most expensive distribution that exists: every
	// partition is faintly warm, none is worth visiting, and the rotation sweeps
	// the whole set to collect almost nothing.
	activeParts := *partitions
	if *activeFrac > 0 && *activeFrac < 1 {
		activeParts = int(math.Ceil(float64(*partitions) * *activeFrac))
		if activeParts < 1 {
			activeParts = 1
		}
	}
	fmt.Printf("  active partitions: %d of %d per queue (%.3f%%)\n",
		activeParts, *partitions, 100*float64(activeParts)/float64(*partitions))
	fmt.Printf("  target=%s tenants=%d sharedQueue=%v queue=%q group=%q partitions=%d brokerTenantHeader=%v\n",
		*target, nT, *sharedQueue, *queueName, *groupName, *partitions, *target == "broker" && *brokerTenantHdr)
	fmt.Printf("  offered=%.1f msg/s total (%.2f/tenant) push-batch=%d -> %.2f req/s per tenant across %d producer(s) | max-inflight=%d\n",
		totalRate, ptRate, *pushBatch, ptReqRate, *producersPer, *maxInflight)
	fmt.Printf("  consumers=%d/tenant popBatch=%d popWait=%v autoAck=%v | payload=%dB | duration=%ds drain=%ds | idleConns=%d/client verify=%v\n",
		*consumersPer, *popBatch, *popWait, *autoAck, *payloadBytes, *durationSec, *drainSec, idle, *verify)
	if faults.any() {
		fmt.Printf("  FAULT INJECTION: dup-push=%d lose-msg=%d drop-ack=%d (the correctness verdict is EXPECTED to fail)\n",
			faults.dupPush, faults.loseMsg, faults.dropAck)
	}

	// ---------------------------------------------------------------- clients
	var r429 *queen.Retry429Config
	if *retry429 != 0 {
		r429 = &queen.Retry429Config{MaxAttempts: *retry429}
	}
	retryAttempts := *retries
	if retryAttempts <= 0 {
		retryAttempts = -1 // client-go sentinel: exactly one attempt
	}

	tenants := make([]*cloudTenant, nT)
	var sharedClient *queen.Queen
	for i, c := range creds {
		cfg := queen.ClientConfig{
			URL:                 baseURL,
			TimeoutMillis:       *timeoutMs,
			MaxIdleConnsPerHost: idle,
			RetryAttempts:       retryAttempts,
			Retry429:            r429,
		}
		switch {
		case *target == "proxy":
			cfg.BearerToken = c.APIKey
			cfg.Headers = map[string]string{"Host": c.ClusterSlug}
		case *brokerTenantHdr:
			cfg.Headers = map[string]string{"x-queen-tenant": c.BrokerTenant}
		}
		var cl *queen.Queen
		if *target == "broker" && !*brokerTenantHdr {
			if sharedClient == nil {
				var err error
				sharedClient, err = queen.New(cfg)
				if err != nil {
					fmt.Printf("client init failed: %v\n", err)
					os.Exit(1)
				}
			}
			cl = sharedClient
		} else {
			var err error
			cl, err = queen.New(cfg)
			if err != nil {
				fmt.Printf("client init failed for tenant %s: %v\n", c.ClusterSlug, err)
				os.Exit(1)
			}
		}
		q := *queueName
		if !*sharedQueue {
			q = fmt.Sprintf("%s-t%04d", *queueName, c.Idx)
		}
		tenants[i] = &cloudTenant{cred: c, client: cl, queue: q, group: *groupName, v: newTenantVerify()}
	}

	ctxAll, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	prodCtx, stopProducers := context.WithCancel(ctxAll)
	defer stopProducers()

	// ------------------------------------------------------------- configure
	cfgErrs := newErrStats()
	if !*skipConfigure {
		t0 := time.Now()
		var cfgWg sync.WaitGroup
		sem := make(chan struct{}, 32)
		var nCfgErr int64
		for _, t := range tenants {
			cfgWg.Add(1)
			sem <- struct{}{}
			go func(t *cloudTenant) {
				defer cfgWg.Done()
				defer func() { <-sem }()
				cctx, ccancel := context.WithTimeout(ctxAll, 20*time.Second)
				defer ccancel()
				_, err := t.client.GetHttpClient().Post(cctx, "/api/v1/configure", map[string]interface{}{
					"queue": t.queue,
					"options": map[string]interface{}{
						"retentionEnabled":          true,
						"completedRetentionSeconds": *completedRet,
						"retentionSeconds":          *pendingRet,
						"leaseTime":                 *leaseTime,
						"dedupWindowSeconds":        *dedupWindow,
						"minPopWaitTime":            *minPopWait,
					},
				})
				if err != nil {
					cfgErrs.record(err)
					atomic.AddInt64(&nCfgErr, 1)
				}
			}(t)
		}
		cfgWg.Wait()
		if nCfgErr > 0 {
			fmt.Printf("[configure] %d/%d FAILED — a run over half-provisioned queues measures nothing:\n", nCfgErr, nT)
			s := cfgErrs.snapshot()
			for k, v := range s.ByKind {
				fmt.Printf("    %s=%d  %s\n", k, v, s.FirstMsg[k])
			}
			os.Exit(1)
		}
		fmt.Printf("[configure] %d queue(s) configured in %.2fs\n", nT, time.Since(t0).Seconds())
	}

	// ---------------------------------------------------------------- counters
	var (
		offeredReq, shedReq, pushReq, pushedMsg int64
		popReq, poppedMsg, emptyPops            int64
		ackReq, ackedMsg                        int64
		inflight                                int64
		crossTenant, undecodable                int64
	)
	pushErrs, popErrs, ackErrs := newErrStats(), newErrStats(), newErrStats()
	e2eSched, e2eSend, pushRTT, ackRTT := newOLHist(), newOLHist(), newOLHist(), newOLHist()
	var e2eSchedMax, e2eSendMax, pushRTTMax, ackRTTMax int64

	pad := strings.Repeat("x", *payloadBytes)
	sampleBytes := nominalPayloadBytes(pad)

	sem := make(chan struct{}, *maxInflight)
	var wg sync.WaitGroup
	startWall := time.Now()

	// ---------------------------------------------------------------- producers
	// One open-loop pacer per (tenant, producer): the schedule is anchored at t0
	// and each request is launched at its scheduled instant, so latency is
	// measured from the SCHEDULE (coordinated-omission correct) and a pacer that
	// falls behind shows up as latency rather than vanishing. Over the in-flight
	// cap a request is SHED — counted, never sent, never re-offered.
	stepNs := float64(time.Second) / perProducerRPS
	for ti := range tenants {
		t := tenants[ti]
		for p := 0; p < *producersPer; p++ {
			wg.Add(1)
			go func(t *cloudTenant, phase float64) {
				defer wg.Done()
				base := startWall.Add(time.Duration(phase * stepNs))
				var k int64
				for prodCtx.Err() == nil {
					sched := base.Add(time.Duration(float64(k) * stepNs))
					k++
					if d := time.Until(sched); d > 0 {
						tm := time.NewTimer(d)
						select {
						case <-tm.C:
						case <-prodCtx.Done():
							tm.Stop()
							return
						}
					} else if d < -5*time.Second {
						// Hopelessly behind (the rig cannot offer this rate):
						// jump the schedule forward and bulk-count the gap as
						// offered+shed instead of spinning.
						skip := int64(-d.Seconds() * 1e9 / stepNs)
						k += skip
						atomic.AddInt64(&offeredReq, skip)
						atomic.AddInt64(&shedReq, skip)
						continue
					}
					atomic.AddInt64(&offeredReq, 1)
					select {
					case sem <- struct{}{}:
					default:
						atomic.AddInt64(&shedReq, 1)
						continue
					}
					atomic.AddInt64(&inflight, 1)
					go func(sched time.Time) {
						defer func() {
							atomic.AddInt64(&inflight, -1)
							<-sem
						}()
						// NOTE: the push runs under ctxAll, NOT prodCtx. Cancelling
						// an in-flight push at producer-stop would abandon a request
						// the broker may already have committed: the message would be
						// delivered during the drain with no record of having been
						// sent, and the checker would report a spurious "extra".
						// Scheduling stops with prodCtx; requests already issued are
						// allowed to land.
						doCloudPush(ctxAll, t, sched, *pushBatch, activeParts, pad, *verify, faults,
							&pushReq, &pushedMsg, pushErrs, pushRTT, &pushRTTMax)
					}(sched)
				}
			}(t, rand.Float64())
		}
	}

	// ---------------------------------------------------------------- consumers
	for ti := range tenants {
		t := tenants[ti]
		for c := 0; c < *consumersPer; c++ {
			wg.Add(1)
			go func(t *cloudTenant) {
				defer wg.Done()
				for ctxAll.Err() == nil {
					qb := t.client.Queue(t.queue).Group(t.group).Batch(*popBatch).AutoAck(*autoAck)
					if *popPartitions > 1 {
						qb = qb.Partitions(*popPartitions)
					}
					if *popWait {
						qb = qb.Wait(true).TimeoutMillis(*popTimeout)
					} else {
						qb = qb.Wait(false)
					}
					atomic.AddInt64(&popReq, 1)
					msgs, err := qb.Pop(ctxAll)
					if err != nil {
						sm := popErrs.record(err)
						if sm.Kind == errKindCancel || ctxAll.Err() != nil {
							return
						}
						time.Sleep(20 * time.Millisecond)
						continue
					}
					if len(msgs) == 0 {
						atomic.AddInt64(&emptyPops, 1)
						if !*popWait {
							time.Sleep(time.Duration(*emptySleepMs) * time.Millisecond)
						}
						continue
					}
					now := time.Now().UnixMicro()
					atomic.AddInt64(&poppedMsg, int64(len(msgs)))
					atomic.AddInt64(&t.popped, int64(len(msgs)))
					for _, m := range msgs {
						tid, seq, tsSched, tsSend, ok := decodeStamp(m)
						if !ok {
							atomic.AddInt64(&undecodable, 1)
							if *verify {
								t.v.recordUndecodable()
							}
							continue
						}
						if d := now - tsSched; d > 0 {
							e2eSched.record(d)
							recordMax(&e2eSchedMax, d)
						}
						if d := now - tsSend; d > 0 {
							e2eSend.record(d)
							recordMax(&e2eSendMax, d)
						}
						if tid != t.cred.Idx {
							// TENANT ISOLATION BREACH: this consumer is
							// authenticated as tenant t but was handed a
							// message produced by another tenant.
							atomic.AddInt64(&crossTenant, 1)
							if *verify {
								t.v.recordCross()
							}
							continue
						}
						if *verify {
							t.v.recordRecv(seq)
						}
					}
					if *autoAck {
						continue
					}
					if take(&faults.dropAck, &faults.dropDone) {
						continue // deliberately never acked -> lease expiry -> redelivery
					}
					a0 := time.Now()
					atomic.AddInt64(&ackReq, 1)
					resp, aerr := t.client.Ack(ctxAll, msgs, true, queen.AckOptions{ConsumerGroup: t.group})
					d := time.Since(a0).Microseconds()
					ackRTT.record(d)
					recordMax(&ackRTTMax, d)
					if aerr != nil {
						ackErrs.record(aerr)
						continue
					}
					var okN int64
					for _, r := range resp {
						if r.Success {
							okN++
						}
					}
					atomic.AddInt64(&ackedMsg, okN)
					atomic.AddInt64(&t.acked, okN)
				}
			}(t)
		}
	}

	// ---------------------------------------------------------------- reporter
	csvPath := fmt.Sprintf("%s/%s-interval.csv", strings.TrimRight(*outDir, "/"), rid)
	csvFile, cerr := os.Create(csvPath)
	if cerr != nil {
		fmt.Printf("cannot create %s: %v\n", csvPath, cerr)
		os.Exit(1)
	}
	fmt.Fprintln(csvFile, "t_sec,wall_utc,phase,offered_msg_s,pushed_msg_s,popped_msg_s,acked_msg_s,shed_msg_s,inflight,e2e_p50_ms,e2e_p99_ms,push_p50_ms,push_p99_ms,err_push,err_pop,err_ack,err_429,err_403,err_5xx,err_timeout,err_conn,empty_pops,cross_tenant,goroutines")

	// phase is read by the reporter goroutine and written by the main goroutine
	// at producer-stop, so it is an atomic, not a plain string.
	var phaseFlag int32 // 0 = load, 1 = drain
	phaseName := func() string {
		if atomic.LoadInt32(&phaseFlag) == 0 {
			return "load"
		}
		return "drain"
	}
	stopReport := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer tk.Stop()
		prevE, curE, diffE := make([]int64, olNumBuckets), make([]int64, olNumBuckets), make([]int64, olNumBuckets)
		prevP, curP, diffP := make([]int64, olNumBuckets), make([]int64, olNumBuckets), make([]int64, olNumBuckets)
		var lOff, lPush, lPop, lAck, lShed int64
		for {
			select {
			case <-stopReport:
				return
			case <-tk.C:
			}
			secs := float64(*reportSec)
			off, pu := atomic.LoadInt64(&offeredReq), atomic.LoadInt64(&pushedMsg)
			po, ac := atomic.LoadInt64(&poppedMsg), atomic.LoadInt64(&ackedMsg)
			sh := atomic.LoadInt64(&shedReq)
			b := int64(*pushBatch)
			e2eSched.snapshot(curE)
			for i := range diffE {
				diffE[i] = curE[i] - prevE[i]
				prevE[i] = curE[i]
			}
			pushRTT.snapshot(curP)
			for i := range diffP {
				diffP[i] = curP[i] - prevP[i]
				prevP[i] = curP[i]
			}
			e50, e99 := olPercentile(diffE, 0.50), olPercentile(diffE, 0.99)
			p50, p99 := olPercentile(diffP, 0.50), olPercentile(diffP, 0.99)
			offRate := float64(off-lOff) * float64(b) / secs
			pushRate := float64(pu-lPush) / secs
			popRate := float64(po-lPop) / secs
			ackRate := float64(ac-lAck) / secs
			shedRate := float64(sh-lShed) * float64(b) / secs
			ep, eo, ea := pushErrs.snapshot(), popErrs.snapshot(), ackErrs.snapshot()
			e429 := ep.ByKind[errKind429] + eo.ByKind[errKind429] + ea.ByKind[errKind429]
			e403 := ep.ByKind[errKind403] + eo.ByKind[errKind403] + ea.ByKind[errKind403]
			e5xx := ep.ByKind[errKind5xx] + eo.ByKind[errKind5xx] + ea.ByKind[errKind5xx]
			eTo := ep.ByKind[errKindTimeout] + eo.ByKind[errKindTimeout] + ea.ByKind[errKindTimeout]
			eCo := ep.ByKind[errKindConn] + eo.ByKind[errKindConn] + ea.ByKind[errKindConn]
			el := time.Since(startWall).Seconds()
			fmt.Printf("[%s %-5s] offered=%8.0f/s push=%8.0f/s pop=%8.0f/s ack=%8.0f/s shed=%7.0f/s inflight=%5d | e2e p50=%7.2f p99=%8.2f ms | pushRTT p50=%6.2f p99=%7.2f ms | lag=%d | err push=%d pop=%d ack=%d (429=%d 403=%d 5xx=%d to=%d conn=%d) empty=%d cross=%d gor=%d\n",
				time.Now().UTC().Format("15:04:05"), phaseName(),
				offRate, pushRate, popRate, ackRate, shedRate, atomic.LoadInt64(&inflight),
				e50, e99, p50, p99, pu-po,
				ep.Total, eo.Total, ea.Total, e429, e403, e5xx, eTo, eCo,
				atomic.LoadInt64(&emptyPops), atomic.LoadInt64(&crossTenant), runtime.NumGoroutine())
			fmt.Fprintf(csvFile, "%.1f,%s,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
				el, time.Now().UTC().Format(time.RFC3339), phaseName(),
				offRate, pushRate, popRate, ackRate, shedRate, atomic.LoadInt64(&inflight),
				e50, e99, p50, p99,
				ep.Total, eo.Total, ea.Total, e429, e403, e5xx, eTo, eCo,
				atomic.LoadInt64(&emptyPops), atomic.LoadInt64(&crossTenant), runtime.NumGoroutine())
			_ = csvFile.Sync()
			lOff, lPush, lPop, lAck, lShed = off, pu, po, ac, sh
		}
	}()

	// ---------------------------------------------------------------- run
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	loadStart := time.Now()
	select {
	case <-sigCh:
		fmt.Println("\n[signal] stopping producers early")
	case <-time.After(time.Duration(*durationSec) * time.Second):
	}
	loadSec := time.Since(loadStart).Seconds()
	stopProducers()
	atomic.StoreInt32(&phaseFlag, 1)
	fmt.Printf("[drain] producers stopped after %.1fs; draining %ds before the correctness verdict\n", loadSec, *drainSec)

	// Drain, sampling the delivery counter 2s before the cutoff. If the
	// consumers were STILL pulling messages when we cut them off, a "missing"
	// count is more likely an in-flight tail than real loss — the run says so
	// instead of leaving the reader to guess.
	drainStart := time.Now()
	poppedAtDrainStart := atomic.LoadInt64(&poppedMsg)
	drainDeadline := drainStart.Add(time.Duration(*drainSec) * time.Second)
	poppedAtTailMark := int64(-1)
	tick := time.NewTicker(100 * time.Millisecond)
drainLoop:
	for time.Now().Before(drainDeadline) {
		select {
		case <-sigCh:
			fmt.Println("\n[signal] cutting the drain short")
			break drainLoop
		case <-tick.C:
			if poppedAtTailMark < 0 && time.Until(drainDeadline) <= 2*time.Second {
				poppedAtTailMark = atomic.LoadInt64(&poppedMsg)
			}
		}
	}
	tick.Stop()
	drainSecActual := time.Since(drainStart).Seconds()
	drainDeliveries := atomic.LoadInt64(&poppedMsg) - poppedAtDrainStart
	tailDeliveries := int64(-1)
	if poppedAtTailMark >= 0 {
		tailDeliveries = atomic.LoadInt64(&poppedMsg) - poppedAtTailMark
	}
	cancelAll()
	close(stopReport)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		fmt.Println("[shutdown] some goroutines did not stop within 15s")
	}
	_ = csvFile.Close()
	endWall := time.Now()

	// ---------------------------------------------------------------- verdict
	vb := VerifyBlock{Enabled: *verify, Verdict: "SKIPPED"}
	if *verify {
		vb.Verdict = "PASS"
		for i, t := range tenants {
			r := t.v.result(i, t.cred.TenantSlug, t.cred.ClusterSlug, t.queue)
			vb.Tenants = append(vb.Tenants, r)
			vb.SentOK += r.SentOK
			vb.Received += r.Received
			vb.Missing += r.Missing
			vb.Duplicate += r.Duplicate
			vb.Extra += r.Extra
			vb.CrossTenant += r.CrossIn
			vb.Undecodable += r.Undecodable
			if !r.OK {
				vb.Verdict = "FAIL"
			}
		}
	}

	pushedT, poppedT := atomic.LoadInt64(&pushedMsg), atomic.LoadInt64(&poppedMsg)
	rep := RunReport{
		RunID:     rid,
		Tool:      "goload -mode cloud",
		StartedAt: startWall.UTC().Format(time.RFC3339),
		EndedAt:   endWall.UTC().Format(time.RFC3339),
		LoadSec:   loadSec,
		DrainSec:  drainSecActual,
		Config: map[string]interface{}{
			"target": *target, "url": baseURL, "tenants": nT,
			"tenantsFile": *tenantsFile, "sharedQueue": *sharedQueue,
			"queue": *queueName, "group": *groupName, "partitions": *partitions,
			"brokerTenantHeader":    *target == "broker" && *brokerTenantHdr,
			"offeredMsgPerSecTotal": totalRate, "offeredMsgPerSecPerTenant": ptRate,
			"pushBatch": *pushBatch, "producersPerTenant": *producersPer,
			"maxInflight": *maxInflight, "consumersPerTenant": *consumersPer,
			"popBatch": *popBatch, "popWait": *popWait, "popTimeoutMs": *popTimeout,
			"popPartitions": *popPartitions, "autoAck": *autoAck,
			"payloadBytes": *payloadBytes, "durationSec": *durationSec, "drainSec": *drainSec,
			"timeoutMs": *timeoutMs, "idleConnsPerClient": idle,
			"retry429Attempts": *retry429, "retries": *retries,
			"leaseTime": *leaseTime, "dedupWindowSeconds": *dedupWindow,
			"minPopWaitTime":            *minPopWait,
			"completedRetentionSeconds": *completedRet, "retentionSeconds": *pendingRet,
			"verify": *verify, "fault": *fault, "note": *note,
			"drainDeliveries":       drainDeliveries,
			"drainLast2sDeliveries": tailDeliveries,
		},
		Offered: OfferedBlock{
			MsgPerSec: float64(atomic.LoadInt64(&offeredReq)) * float64(*pushBatch) / loadSec,
			ReqPerSec: float64(atomic.LoadInt64(&offeredReq)) / loadSec,
			Msgs:      atomic.LoadInt64(&offeredReq) * int64(*pushBatch),
			Reqs:      atomic.LoadInt64(&offeredReq),
			ShedMsgs:  atomic.LoadInt64(&shedReq) * int64(*pushBatch),
			ShedReqs:  atomic.LoadInt64(&shedReq),
		},
		Achieved: AchievedBlock{
			PushMsgPerSec:    float64(pushedT) / loadSec,
			PopMsgPerSecLoad: float64(poppedAtDrainStart) / loadSec,
			PopMsgPerSec:     float64(poppedT) / (loadSec + drainSecActual),
			AckMsgPerSec:     float64(atomic.LoadInt64(&ackedMsg)) / (loadSec + drainSecActual),
			PushedMsgs:       pushedT,
			PoppedMsgs:       poppedT,
			AckedMsgs:        atomic.LoadInt64(&ackedMsg),
			PushReqs:         atomic.LoadInt64(&pushReq),
			PopReqs:          atomic.LoadInt64(&popReq),
			AckReqs:          atomic.LoadInt64(&ackReq),
			EmptyPops:        atomic.LoadInt64(&emptyPops),
		},
		Latency: map[string]latSummary{
			"e2eFromSchedule": summarize(e2eSched, &e2eSchedMax),
			"e2eFromSend":     summarize(e2eSend, &e2eSendMax),
			"pushRtt":         summarize(pushRTT, &pushRTTMax),
			"ackRtt":          summarize(ackRTT, &ackRTTMax),
		},
		Errors: map[string]errCounts{
			"push":      pushErrs.snapshot(),
			"pop":       popErrs.snapshot(),
			"ack":       ackErrs.snapshot(),
			"configure": cfgErrs.snapshot(),
		},
		Bytes: BytesBlock{
			PerMsgNominal:    sampleBytes,
			PushedNominal:    pushedT * sampleBytes,
			DeliveredNominal: poppedT * sampleBytes,
			Note:             "nominal = JSON-encoded payload size x messages; excludes HTTP framing and the envelope the broker adds. The authoritative wire bytes are the proxy's own metered bytes_in/bytes_out.",
		},
		Verify: vb,
		Faults: map[string]int64{
			"dupPush": atomic.LoadInt64(&faults.dupDone),
			"loseMsg": atomic.LoadInt64(&faults.loseDone),
			"dropAck": atomic.LoadInt64(&faults.dropDone),
		},
	}
	rep.Verdict = vb.Verdict
	if !*verify {
		rep.Verdict = "UNVERIFIED"
	}

	jsonPath := fmt.Sprintf("%s/%s.json", strings.TrimRight(*outDir, "/"), rid)
	jb, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(jsonPath, append(jb, '\n'), 0o644); err != nil {
		fmt.Printf("cannot write %s: %v\n", jsonPath, err)
	}

	printCloudSummary(&rep, atomic.LoadInt64(&crossTenant), atomic.LoadInt64(&undecodable))
	// "missing" means LOSS only if the consumers had actually finished. Two
	// things make it a BACKLOG instead: deliveries still arriving at the cutoff,
	// or a consumer side that was being refused (429) and so could not drain.
	// Say which, instead of letting a backlog be read as data loss.
	if vb.Missing > 0 {
		popFail := rep.Errors["pop"].Total
		if tailDeliveries > 0 || popFail > 0 {
			fmt.Printf("CAUTION: %d missing may be BACKLOG, not loss — %d messages were still arriving in the last 2s of the drain and the consumer side hit %d error(s) (%v). Re-run with a longer -drain, or size -drain to the rate the plan actually permits.\n",
				vb.Missing, tailDeliveries, popFail, rep.Errors["pop"].ByCode)
		} else {
			fmt.Printf("NOTE: consumers were idle at the cutoff and the consume path reported no errors, so %d missing is real LOSS, not a tail.\n", vb.Missing)
		}
	}
	fmt.Printf("\n[artifacts] %s\n            %s\n", jsonPath, csvPath)

	if *verify && vb.Verdict != "PASS" && *failOnVerify {
		os.Exit(3)
	}
}

// doCloudPush issues one push request for a tenant, stamping every message with
// (tenant, seq, scheduled-µs, sent-µs) and folding the outcome into the
// verifier. Latency is recorded from `sched` for the e2e histogram (the
// coordinated-omission-correct baseline) and from the actual send for pushRTT.
func doCloudPush(ctx context.Context, t *cloudTenant, sched time.Time, batch, parts int,
	pad string, verify bool, f *faultSpec,
	pushReq, pushedMsg *int64, errs *errStats, rtt *olHist, rttMax *int64) {

	last := atomic.AddInt64(&t.seq, int64(batch))
	first := last - int64(batch) + 1
	if verify {
		t.v.noteAssigned(last)
	}

	schedUs := sched.UnixMicro()
	sendUs := time.Now().UnixMicro()
	payloads := make([]interface{}, 0, batch)
	for j := 0; j < batch; j++ {
		payloads = append(payloads, map[string]interface{}{
			"t": t.cred.Idx, "s": first + int64(j),
			"ts": schedUs, "ta": sendUs, "pad": pad,
		})
	}

	// FAULT lose-msg: claim the FIRST message of this block as sent but drop it
	// on the floor. The checker must report exactly one missing sequence (the
	// rest of the block is marked by the normal path below).
	lost := false
	if take(&f.loseMsg, &f.loseDone) {
		lost = true
		if verify {
			t.v.markSentFI(first, 1)
		}
		if batch == 1 {
			return
		}
		payloads = payloads[1:]
	}

	qb := t.client.Queue(t.queue)
	partName := ""
	if parts > 1 {
		partName = "p" + strconv.Itoa(int(atomic.AddUint64(&t.part, 1)%uint64(parts)))
		qb = qb.Partition(partName)
	}
	n := int64(len(payloads))
	atomic.AddInt64(pushReq, 1)
	p0 := time.Now()
	_, err := qb.Push(payloads).Execute(ctx)
	d := time.Since(p0).Microseconds()
	if err != nil {
		sm := errs.record(err)
		if sm.Kind != errKindCancel && verify {
			t.v.markSentFail(n)
		}
		return
	}
	rtt.record(d)
	recordMax(rttMax, d)
	atomic.AddInt64(pushedMsg, n)
	atomic.AddInt64(&t.pushed, n)
	if verify {
		lo := first
		if lost {
			lo = first + 1
		}
		t.v.markSent(lo, n)
	}

	// FAULT dup-push: send the identical (tenant, seq) block a second time, to
	// the same partition. The sequences are already marked sent exactly once, so
	// the second delivery must surface as DUPLICATION. The extra messages ARE
	// real load, so they are counted in the push totals.
	if take(&f.dupPush, &f.dupDone) {
		dqb := t.client.Queue(t.queue)
		if partName != "" {
			dqb = dqb.Partition(partName)
		}
		atomic.AddInt64(pushReq, 1)
		if _, e := dqb.Push(payloads).Execute(ctx); e != nil {
			errs.record(e)
		} else {
			atomic.AddInt64(pushedMsg, n)
			atomic.AddInt64(&t.pushed, n)
		}
	}
}

// decodeStamp pulls (tenant, seq, schedUs, sendUs) back out of a delivered
// message. JSON numbers arrive as float64; every value here is well inside the
// 2^53 exactly-representable range.
func decodeStamp(m *queen.Message) (tenant int, seq, schedUs, sendUs int64, ok bool) {
	if m == nil || m.Data == nil {
		return 0, 0, 0, 0, false
	}
	tf, ok1 := m.Data["t"].(float64)
	sf, ok2 := m.Data["s"].(float64)
	if !ok1 || !ok2 {
		return 0, 0, 0, 0, false
	}
	tsf, _ := m.Data["ts"].(float64)
	taf, _ := m.Data["ta"].(float64)
	return int(tf), int64(sf), int64(tsf), int64(taf), true
}

func nominalPayloadBytes(pad string) int64 {
	b, err := json.Marshal(map[string]interface{}{
		"t": 0, "s": int64(0), "ts": int64(0), "ta": int64(0), "pad": pad,
	})
	if err != nil {
		return int64(len(pad))
	}
	return int64(len(b))
}

// deterministicUUID builds a stable UUID from an index so a broker-only run
// (no tenants file) still hits the same tenant rows on every repeat.
func deterministicUUID(i int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
}

func printCloudSummary(rep *RunReport, cross, undec int64) {
	fmt.Printf("\n========== %s ==========\n", rep.RunID)
	fmt.Printf("offered   %10.1f msg/s (%d msgs, %d shed)\n", rep.Offered.MsgPerSec, rep.Offered.Msgs, rep.Offered.ShedMsgs)
	fmt.Printf("achieved  push %8.1f msg/s (%d msgs)   pop %8.1f msg/s during load (%8.1f msg/s over load+drain, %d msgs)   ack %d msgs\n",
		rep.Achieved.PushMsgPerSec, rep.Achieved.PushedMsgs,
		rep.Achieved.PopMsgPerSecLoad, rep.Achieved.PopMsgPerSec, rep.Achieved.PoppedMsgs,
		rep.Achieved.AckedMsgs)
	for _, k := range []string{"e2eFromSchedule", "e2eFromSend", "pushRtt", "ackRtt"} {
		l := rep.Latency[k]
		fmt.Printf("%-16s n=%-9d p50=%8.2f p95=%8.2f p99=%8.2f p999=%8.2f max=%9.2f ms\n",
			k, l.Samples, l.P50Ms, l.P95Ms, l.P99Ms, l.P999Ms, l.MaxMs)
	}
	for _, k := range []string{"push", "pop", "ack", "configure"} {
		e := rep.Errors[k]
		if e.Total == 0 {
			continue
		}
		fmt.Printf("errors %-10s total=%d kinds=%v codes=%v", k, e.Total, e.ByKind, e.ByCode)
		if e.RetryN > 0 {
			fmt.Printf(" retryAfter(avg=%.2fs max=%.2fs n=%d)", e.RetryAvg, e.RetryMax, e.RetryN)
		}
		fmt.Println()
		for kind, msg := range e.FirstMsg {
			fmt.Printf("    first %s: %s\n", kind, msg)
		}
	}
	if rep.Errors["push"].Total == 0 && rep.Errors["pop"].Total == 0 && rep.Errors["ack"].Total == 0 {
		fmt.Println("errors            none")
	}
	fmt.Printf("bytes     nominal %d B/msg -> pushed %.2f MiB, delivered %.2f MiB\n",
		rep.Bytes.PerMsgNominal, float64(rep.Bytes.PushedNominal)/1048576, float64(rep.Bytes.DeliveredNominal)/1048576)

	v := rep.Verify
	if !v.Enabled {
		fmt.Printf("verify            DISABLED (-verify=false): this run proves nothing about delivery (cross-tenant deliveries seen: %d, undecodable: %d)\n", cross, undec)
		return
	}
	fmt.Printf("\n---- delivery correctness (after the drain) ----\n")
	fmt.Printf("%-4s %-14s %-12s %9s %9s %8s %8s %7s %7s %s\n",
		"idx", "tenant", "queue", "sentOk", "received", "missing", "dup", "extra", "cross", "verdict")
	for _, r := range v.Tenants {
		flag := "OK"
		if !r.OK {
			flag = "FAIL"
		}
		fmt.Printf("%-4d %-14s %-12s %9d %9d %8d %8d %7d %7d %s",
			r.Idx, r.Tenant, r.Queue, r.SentOK, r.Received, r.Missing, r.Duplicate, r.Extra, r.CrossIn, flag)
		if len(r.FirstMissed) > 0 {
			fmt.Printf("  firstMissing=%v", r.FirstMissed)
		}
		fmt.Println()
	}
	fmt.Printf("%-4s %-14s %-12s %9d %9d %8d %8d %7d %7d\n",
		"", "TOTAL", "", v.SentOK, v.Received, v.Missing, v.Duplicate, v.Extra, v.CrossTenant)
	if v.Undecodable > 0 {
		fmt.Printf("undecodable payloads: %d\n", v.Undecodable)
	}
	fi := rep.Faults
	if fi["dupPush"]+fi["loseMsg"]+fi["dropAck"] > 0 {
		fmt.Printf("faults injected: dup-push=%d lose-msg=%d drop-ack=%d\n", fi["dupPush"], fi["loseMsg"], fi["dropAck"])
	}
	fmt.Printf("VERDICT: %s\n", v.Verdict)
}
