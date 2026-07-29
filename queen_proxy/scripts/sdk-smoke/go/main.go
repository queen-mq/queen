// sdk-smoke (Go) -- the REAL published client (clients/client-go, wired in via
// the `replace` directive in go.mod, no publishing step) driven against a live
// queen_proxy + broker. No httptest server anywhere in this file.
//
// One phase per invocation (os.Args[1]); the driver (../run.sh) owns the cell
// and the control plane, this program owns the SDK. Output mirrors
// scripts/isolation-smoke.sh: "  ok  - desc" / "  FAIL- desc", exit 1 on any
// failure so the driver can score a language per phase.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

var (
	proxyURL   string
	hostA      string
	keyA       string
	hostB      string
	keyB       string
	queueName  string
	isoQueue   string
	runID      string
	deadline   time.Duration
	burnMax    int
	recoverN   int
	passCount  int
	failCount  int
	phaseLabel string
)

func env(name, dflt string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	if dflt == "" {
		fmt.Fprintf(os.Stderr, "sdk-smoke(go): missing env %s\n", name)
		os.Exit(2)
	}
	return dflt
}

func envInt(name, dflt string) int {
	n, err := strconv.Atoi(env(name, dflt))
	if err != nil {
		fmt.Fprintf(os.Stderr, "sdk-smoke(go): bad int in %s: %v\n", name, err)
		os.Exit(2)
	}
	return n
}

func ok(desc string)  { passCount++; fmt.Printf("  ok  - %s\n", desc) }
func bad(desc string) { failCount++; fmt.Printf("  FAIL- %s\n", desc) }

func check(desc string, cond bool, detail string) {
	if cond {
		ok(desc)
		return
	}
	if detail != "" {
		bad(fmt.Sprintf("%s (%s)", desc, detail))
		return
	}
	bad(desc)
}

// client wires a client exactly like a customer would at a proxy: base URL,
// api key as bearer token, Host header selecting the cluster. net/http ignores
// Header["Host"], so the client maps it onto req.Host -- see
// clients/client-go/http_client.go.
func client(host, key string, retry *queen.Retry429Config) *queen.Queen {
	q, err := queen.New(queen.ClientConfig{
		URL:         proxyURL,
		BearerToken: key,
		Headers:     map[string]string{"Host": host},
		Retry429:    retry,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "sdk-smoke(go): client init failed: %v\n", err)
		os.Exit(2)
	}
	return q
}

func httpErr(err error) *queen.HTTPError {
	var he *queen.HTTPError
	if errors.As(err, &he) {
		return he
	}
	return nil
}

func jsonOf(v interface{}) string {
	b, _ := json.Marshal(v)
	s := string(b)
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

// ---------------------------------------------------------------------------
// 1. push -> pop -> ack round trip through the proxy, api-key auth
// ---------------------------------------------------------------------------
func roundtrip(ctx context.Context) {
	q := client(hostA, keyA, nil)
	defer q.Close(ctx)

	res, err := q.Queue(queueName).Push([]interface{}{
		map[string]interface{}{"n": 1, "run": runID},
		map[string]interface{}{"n": 2, "run": runID},
		map[string]interface{}{"n": 3, "run": runID},
	}).Execute(ctx)
	allQueued := err == nil && len(res) == 3
	if allQueued {
		for _, r := range res {
			if r.Status != "queued" {
				allQueued = false
			}
		}
	}
	check("push 3 items accepted through the proxy", allQueued, fmt.Sprintf("err=%v res=%s", err, jsonOf(res)))

	msgs, err := q.Queue(queueName).Batch(10).Wait(false).Pop(ctx)
	ns := []int{}
	for _, m := range msgs {
		if f, isF := m.Data["n"].(float64); isF {
			ns = append(ns, int(f))
		}
	}
	sort.Ints(ns)
	check("pop returns the 3 pushed messages",
		err == nil && len(msgs) == 3 && fmt.Sprint(ns) == "[1 2 3]",
		fmt.Sprintf("err=%v got %d: %v", err, len(msgs), ns))

	haveIDs := len(msgs) > 0
	for _, m := range msgs {
		if m.PartitionID == "" || m.TransactionID == "" {
			haveIDs = false
		}
	}
	check("messages carry partitionId + transactionId", haveIDs, "")

	if _, err := q.Ack(ctx, msgs, true, queen.AckOptions{}); err != nil {
		bad(fmt.Sprintf("ack failed: %v", err))
		return
	}
	after, err := q.Queue(queueName).Batch(10).Wait(false).Pop(ctx)
	check("queue drained after ack", err == nil && len(after) == 0, fmt.Sprintf("err=%v got %d", err, len(after)))
}

// ---------------------------------------------------------------------------
// 2. tenant isolation with the real client: same queue name, two clusters
// ---------------------------------------------------------------------------
func isolation(ctx context.Context) {
	a := client(hostA, keyA, nil)
	defer a.Close(ctx)
	b := client(hostB, keyB, nil)
	defer b.Close(ctx)
	foreign := client(hostB, keyA, nil) // A's key presented on B's Host
	defer foreign.Close(ctx)

	ra, errA := a.Queue(isoQueue).Push(map[string]interface{}{"who": "A", "run": runID}).Execute(ctx)
	rb, errB := b.Queue(isoQueue).Push(map[string]interface{}{"who": "B", "run": runID}).Execute(ctx)
	check("both clusters accept a push to the same queue name",
		errA == nil && errB == nil && len(ra) == 1 && len(rb) == 1 &&
			ra[0].Status == "queued" && rb[0].Status == "queued",
		fmt.Sprintf("errA=%v errB=%v", errA, errB))

	ma, errA := a.Queue(isoQueue).Batch(10).Wait(false).Pop(ctx)
	mb, errB := b.Queue(isoQueue).Batch(10).Wait(false).Pop(ctx)
	check("cluster A sees only its own message",
		errA == nil && len(ma) == 1 && ma[0].Data["who"] == "A",
		fmt.Sprintf("err=%v %d msg(s) %s", errA, len(ma), jsonOf(ma)))
	check("cluster B sees only its own message",
		errB == nil && len(mb) == 1 && mb[0].Data["who"] == "B",
		fmt.Sprintf("err=%v %d msg(s) %s", errB, len(mb), jsonOf(mb)))

	_, crossErr := foreign.Queue(isoQueue).Push(map[string]interface{}{"who": "X", "run": runID}).Execute(ctx)
	he := httpErr(crossErr)
	check("cluster A key on cluster B host -> 403 forbidden",
		he != nil && he.StatusCode == 403 && he.Code == "forbidden",
		fmt.Sprintf("%v", crossErr))

	if _, err := a.Ack(ctx, ma, true, queen.AckOptions{}); err != nil {
		bad(fmt.Sprintf("ack A failed: %v", err))
	}
	if _, err := b.Ack(ctx, mb, true, queen.AckOptions{}); err != nil {
		bad(fmt.Sprintf("ack B failed: %v", err))
	}
	ea, _ := a.Queue(isoQueue).Batch(10).Wait(false).Pop(ctx)
	eb, _ := b.Queue(isoQueue).Batch(10).Wait(false).Pop(ctx)
	check("both clusters drained independently after ack",
		len(ea) == 0 && len(eb) == 0, fmt.Sprintf("%d/%d", len(ea), len(eb)))
}

// ---------------------------------------------------------------------------
// 3. live 429 from the real limiter + transparent recovery
// ---------------------------------------------------------------------------
func ratelimit(ctx context.Context) {
	// (a) 429 backoff DISABLED, so the live 429 surfaces as an error we can
	//     inspect (code + Retry-After) instead of being absorbed.
	strict := client(hostA, keyA, &queen.Retry429Config{MaxAttempts: 1})
	var hit *queen.HTTPError
	sent := 0
	for i := 0; i < burnMax && hit == nil; i++ {
		_, err := strict.Queue(queueName).Push(map[string]interface{}{"burn": i, "run": runID}).Execute(ctx)
		if err == nil {
			sent++
			continue
		}
		he := httpErr(err)
		if he != nil && he.StatusCode == 429 {
			hit = he
			break
		}
		bad(fmt.Sprintf("unexpected error while burning the bucket: %v", err))
		break
	}
	strict.Close(ctx)

	check("the real limiter returned a live 429 to the SDK", hit != nil, fmt.Sprintf("no 429 after %d pushes", sent))
	check("429 body carries code=rate_limited", hit != nil && hit.Code == "rate_limited", codeOf(hit))
	check("429 carries a Retry-After the client parsed",
		hit != nil && hit.RetryAfterSeconds != nil && *hit.RetryAfterSeconds >= 1,
		retryAfterOf(hit))

	// (b) the same traffic through a stock client (default 429 policy): the
	//     bucket is empty, so every one of these has to be paced by the
	//     client's own Retry-After backoff -- and all of them must still land.
	normal := client(hostA, keyA, nil)
	defer normal.Close(ctx)
	start := time.Now()
	done := 0
	for i := 0; i < recoverN; i++ {
		r, err := normal.Queue(queueName).Push(map[string]interface{}{"recover": i, "run": runID}).Execute(ctx)
		if err != nil {
			bad(fmt.Sprintf("stock client push %d failed: %v", i, err))
			break
		}
		if len(r) == 1 && r[0].Status == "queued" {
			done++
		}
	}
	elapsed := time.Since(start)
	check(fmt.Sprintf("stock client completed all %d pushes against an empty bucket", recoverN),
		done == recoverN, fmt.Sprintf("%d/%d", done, recoverN))
	check("the run was paced by backoff, not served instantly",
		elapsed >= 1500*time.Millisecond, elapsed.String())
}

func codeOf(he *queen.HTTPError) string {
	if he == nil {
		return "n/a"
	}
	return he.Code
}

func retryAfterOf(he *queen.HTTPError) string {
	if he == nil || he.RetryAfterSeconds == nil {
		return "n/a"
	}
	return fmt.Sprintf("%v", *he.RetryAfterSeconds)
}

// ---------------------------------------------------------------------------
// 4a. terminal 403: storage quota tripped by the driver's limit override
// ---------------------------------------------------------------------------
func blocked(ctx context.Context) {
	q := client(hostA, keyA, nil)
	defer q.Close(ctx)

	stop := time.Now().Add(deadline)
	var terminal *queen.HTTPError
	var callMs int64
	for time.Now().Before(stop) && terminal == nil {
		t := time.Now()
		_, err := q.Queue(queueName).Push(map[string]interface{}{"probe": time.Now().UnixMilli(), "run": runID}).Execute(ctx)
		if err != nil {
			callMs = time.Since(t).Milliseconds()
			he := httpErr(err)
			if he != nil && he.StatusCode == 403 {
				terminal = he
				break
			}
			bad(fmt.Sprintf("unexpected error while waiting for the block: %v", err))
			break
		}
		time.Sleep(2 * time.Second)
	}

	check("push eventually rejected with a terminal 403", terminal != nil,
		fmt.Sprintf("still accepted after %s", deadline))
	check("terminal code is storage_quota_exceeded",
		terminal != nil && terminal.Code == "storage_quota_exceeded", codeOf(terminal))
	check("terminal 403 surfaced immediately (not retried with backoff)",
		terminal != nil && callMs < 2000, fmt.Sprintf("%dms", callMs))

	// consume must stay open while pushes are blocked: the rate-limit phase
	// left plenty of un-popped messages on this queue, so an empty result here
	// would mean the read path was blocked too.
	msgs, err := q.Queue(queueName).Batch(1).Wait(false).Pop(ctx)
	check("consume still allowed while push-blocked", err == nil && len(msgs) >= 1,
		fmt.Sprintf("err=%v %d msg(s)", err, len(msgs)))
	if len(msgs) > 0 {
		q.Ack(ctx, msgs, true, queen.AckOptions{})
	}
}

// ---------------------------------------------------------------------------
// 4b. recovery once the driver clears the override
// ---------------------------------------------------------------------------
func unblocked(ctx context.Context) {
	q := client(hostA, keyA, nil)
	defer q.Close(ctx)

	stop := time.Now().Add(deadline)
	released := false
	lastCode := "none"
	for time.Now().Before(stop) && !released {
		r, err := q.Queue(queueName).Push(map[string]interface{}{"release": time.Now().UnixMilli(), "run": runID}).Execute(ctx)
		if err == nil {
			released = len(r) == 1 && r[0].Status == "queued"
		} else {
			he := httpErr(err)
			if he == nil || he.StatusCode != 403 {
				bad(fmt.Sprintf("unexpected error while waiting for release: %v", err))
				break
			}
			lastCode = he.Code
		}
		if !released {
			time.Sleep(2 * time.Second)
		}
	}
	check("push accepted again after the override is cleared", released,
		fmt.Sprintf("still %s after %s", lastCode, deadline))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "sdk-smoke(go): usage: sdk-smoke <phase>")
		os.Exit(2)
	}
	phaseLabel = os.Args[1]

	proxyURL = env("SDK_SMOKE_URL", "")
	hostA = env("SDK_SMOKE_HOST_A", "")
	keyA = env("SDK_SMOKE_KEY_A", "")
	hostB = env("SDK_SMOKE_HOST_B", "")
	keyB = env("SDK_SMOKE_KEY_B", "")
	queueName = env("SDK_SMOKE_QUEUE", "")
	isoQueue = env("SDK_SMOKE_ISO_QUEUE", "")
	runID = env("SDK_SMOKE_RUN_ID", "")
	deadline = time.Duration(envInt("SDK_SMOKE_DEADLINE_MS", "150000")) * time.Millisecond
	burnMax = envInt("SDK_SMOKE_BURN_MAX", "400")
	recoverN = envInt("SDK_SMOKE_RECOVER_N", "20")

	ctx := context.Background()
	switch phaseLabel {
	case "roundtrip":
		roundtrip(ctx)
	case "isolation":
		isolation(ctx)
	case "ratelimit":
		ratelimit(ctx)
	case "blocked":
		blocked(ctx)
	case "unblocked":
		unblocked(ctx)
	default:
		fmt.Fprintf(os.Stderr, "sdk-smoke(go): unknown phase '%s' (want: %s)\n",
			phaseLabel, strings.Join([]string{"roundtrip", "isolation", "ratelimit", "blocked", "unblocked"}, "|"))
		os.Exit(2)
	}

	fmt.Printf("  -- go/%s: %d ok, %d fail\n", phaseLabel, passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}
