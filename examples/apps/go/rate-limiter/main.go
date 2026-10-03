// docs:start(app-go-rate-limiter)
//
// A rate limiter built from a streaming query.
//
// The textbook rate limiter counts requests per API key in a fixed window,
// usually with a counter in Redis, which is one more system to run and one
// more place where the count can drift from the requests.
//
// Here the counter is a windowed aggregation over the request queue itself.
// Each cycle of the stream commits the window state, the closed windows it
// emits and the ack of the requests it counted as one entry in the broker's
// log, so the count cannot drift from the requests, and it survives a restart
// of this process because the state is in the broker.
//
//	api-requests (one partition per API key)
//	  |-- streaming query: tumbling window, count per key
//	        |-- api-usage  -> the gate: over quota becomes a throttle decision
//	              |-- api-throttled
//
// Run it:
//
//	QUEEN_URL=http://localhost:6632 GOWORK=off go run ./rate-limiter
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

var runID = strconv.FormatInt(time.Now().UnixMilli(), 36)

var (
	requestsQueue  = "app-go-api-requests-" + runID
	usageQueue     = "app-go-api-usage-" + runID
	throttledQueue = "app-go-api-throttled-" + runID

	// The query id is this streaming query's identity in the broker: its
	// window state is keyed by it, so a restart with the same id resumes the
	// same windows.
	queryID = "app-go-rate-limiter-" + runID
)

const (
	windowSeconds  = 2
	quotaPerWindow = 5
	gateGroup      = "rate-limiter-gate"
	quietKey       = "key-quiet"
	noisyKey       = "key-noisy"
	quietRequests  = 3
	noisyRequests  = 20
)

// Why those numbers make the check deterministic: a window is a slice of time,
// so a burst can land on either side of a boundary. Twenty requests split in
// any way at all leave at least ten on one side, which is over a quota of five,
// so the noisy key is always caught. Three requests cannot reach five however
// they are split, so the quiet key is never caught by accident.

var checks int

func assert(condition bool, description string) error {
	if !condition {
		return fmt.Errorf("%s", description)
	}
	checks++
	fmt.Printf("  ok: %s\n", description)
	return nil
}

// stopping is set just before the stream is shut down, and read by the logger
// below.
var stopping atomic.Bool

// streamLogger is what the streaming runner reports through. Stopping the
// runner cancels whatever poll it had in flight, and the pop loop reports that
// cancellation on its way out. That report is the shutdown itself, so it is
// dropped. Everything else is printed, because a query failing to commit its
// windows would otherwise fail this run with no explanation.
type streamLogger struct{}

func (streamLogger) Info(msg string, ctx map[string]interface{}) {}

func (streamLogger) Warn(msg string, ctx map[string]interface{}) {
	fmt.Fprintf(os.Stderr, "  stream warning: %s %v\n", msg, ctx)
}

func (streamLogger) Error(msg string, ctx map[string]interface{}) {
	if stopping.Load() {
		return
	}
	fmt.Fprintf(os.Stderr, "  stream error: %s %v\n", msg, ctx)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nFAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nPASS: %d checks\n", checks)
}

func run() error {
	brokerURL := os.Getenv("QUEEN_URL")
	if brokerURL == "" {
		brokerURL = "http://localhost:6632"
	}

	// One context bounds the whole program, including the streaming runner it
	// starts, so a broker that stops answering fails the run when it expires.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	fmt.Printf("broker %s\n", brokerURL)

	for _, q := range []string{requestsQueue, usageQueue, throttledQueue} {
		if _, err := client.Queue(q).
			Config(queen.QueueConfig{LeaseTime: 30, RetryLimit: 3}).
			Create().Execute(ctx); err != nil {
			return fmt.Errorf("create %s: %w", q, err)
		}
	}

	// ------------------------------------------------------------- the counter
	//
	// The stream runs in this process, as a consumer group of its own. A new
	// group starts at the tail of the queue unless it asks otherwise, and a
	// request pushed while the stream was still starting would be missed, so it
	// asks for SubscriptionMode "all": every request in the queue is counted.
	//
	// The partition is the aggregation key, so the window state is per API key
	// without a word about keys here: the producer decides, by partition.
	fmt.Println("\nstarting the counter")
	runner, err := streams.
		// AsStreamSource adapts a queue builder to what the streaming engine
		// reads from; To takes the queue builder itself, since a sink is only
		// a name.
		From(client.Queue(requestsQueue).AsStreamSource()).
		WindowTumbling(windowSeconds, streams.WithIdleFlushMs(800)).
		// The extractors receive the payload itself, not the envelope, and as
		// an interface{}: nothing about its shape is checked by the compiler,
		// so the fallback lives in the extractor (see cost below, which counts
		// a request with no cost of its own as one). The field order is passed
		// explicitly after the map because a Go map has no order of its own and
		// that order goes into the query's identity hash: left out, the client
		// falls back to sorting the names, which hashes to a different query
		// than the JavaScript object literal's insertion order.
		Aggregate(map[string]streams.ExtractorFn{
			"requests": func(m interface{}) (float64, error) { return 1, nil },
			"cost":     func(m interface{}) (float64, error) { return cost(m), nil },
		}, "requests", "cost").
		To(client.Queue(usageQueue)).
		Run(ctx, streams.RunOptions{
			QueryID:          queryID,
			URL:              brokerURL,
			BatchSize:        200,
			MaxPartitions:    8,
			MaxWaitMillis:    200,
			SubscriptionMode: queen.SubscriptionModeAll,
			Logger:           streamLogger{},
		})
	if err != nil {
		return fmt.Errorf("start the counter: %w", err)
	}
	// Stop waits for the pop loop and the idle-flush loop to leave, and is
	// idempotent, so it is safe both here as a guard and explicitly below.
	stop := func() {
		stopping.Store(true)
		runner.Stop()
	}
	defer stop()

	// ------------------------------------------------------------- the traffic
	fmt.Println("\ntaking traffic")
	send := func(key string, n int) error {
		for i := 1; i <= n; i++ {
			if _, err := client.Queue(requestsQueue).
				Partition(key).
				Push(map[string]interface{}{
					"key":  key,
					"path": "/v1/things",
					"cost": 1,
					"at":   time.Now().UnixMilli(),
				}).
				Execute(ctx); err != nil {
				return fmt.Errorf("push request for %s: %w", key, err)
			}
		}
		fmt.Printf("  %s: %d requests\n", key, n)
		return nil
	}
	if err := send(quietKey, quietRequests); err != nil {
		return err
	}
	if err := send(noisyKey, noisyRequests); err != nil {
		return err
	}

	// ---------------------------------------------------------------- the gate
	//
	// The enforcement point. It reads each closed window and turns the ones over
	// quota into throttle decisions. It is separate from the counter on purpose:
	// the counting is exact and stays the same, while the policy is yours and
	// changes on its own schedule.
	fmt.Println("\nenforcing")
	type decision struct {
		key    string
		overBy int
	}
	counted := map[string]int{}
	var decisions []decision

	// The loop waits for the totals it expects, with a deadline. Stopping on a
	// quiet period would be a race: a window closes when its timer fires,
	// whatever the reader is doing, and a burst that straddles a boundary
	// arrives as two windows.
	complete := func() bool {
		return counted[quietKey] == quietRequests && counted[noisyKey] == noisyRequests
	}
	deadline := time.Now().Add(30 * time.Second)

	for !complete() && time.Now().Before(deadline) {
		windows, err := client.Queue(usageQueue).
			Group(gateGroup).
			SubscriptionMode(queen.SubscriptionModeAll).
			Batch(50).
			// Each key's windows land in that key's partition. Partitions(10)
			// lets one pop take the windows of both keys, with Batch as the
			// budget they share.
			Partitions(10).
			Wait(true).
			TimeoutMillis(2000).
			Pop(ctx)
		if err != nil {
			return fmt.Errorf("read closed windows: %w", err)
		}

		for _, w := range windows {
			// The window's key is the partition it was computed for.
			key := w.Partition
			requests, ok := w.Data["requests"].(float64)
			if !ok {
				return fmt.Errorf("window on %s has no numeric count", key)
			}
			counted[key] += int(requests)
			overBy := int(requests) - quotaPerWindow

			if overBy > 0 {
				// The decision is a message: whatever enforces it (an edge
				// worker, a gateway, the API itself) reads this queue and gets
				// the decisions in order, per key. It commits with the ack of
				// the window it came from, so a crash in between cannot lose a
				// decision or make two.
				if _, err := client.Transaction().
					Queue(throttledQueue).
					Partition(key).
					Push(map[string]interface{}{
						"key":    key,
						"window": int(requests),
						"quota":  quotaPerWindow,
						"overBy": overBy,
					}).
					Ack(w, "completed", queen.AckOptions{ConsumerGroup: gateGroup}).
					Commit(ctx); err != nil {
					return fmt.Errorf("commit throttle decision: %w", err)
				}
				decisions = append(decisions, decision{key: key, overBy: overBy})
				fmt.Printf("  %s: %d in a window, over by %d\n", key, int(requests), overBy)
			} else {
				// A Pop leaves the ack to the caller, and the ack has to name
				// the consumer group: without it the same windows come back on
				// the next turn and every count is added twice.
				if _, err := client.Ack(ctx, w, true, queen.AckOptions{ConsumerGroup: gateGroup}); err != nil {
					return fmt.Errorf("ack window: %w", err)
				}
				fmt.Printf("  %s: %d in a window, within quota\n", key, int(requests))
			}
		}
	}

	// --------------------------------------------------------------- checking
	fmt.Println("\nchecking")
	if err := assert(complete(), "every request reached a closed window before the deadline"); err != nil {
		return err
	}
	if err := assert(counted[quietKey] == quietRequests, "the quiet key was counted exactly"); err != nil {
		return err
	}
	if err := assert(counted[noisyKey] == noisyRequests, "the noisy key was counted exactly"); err != nil {
		return err
	}

	if err := assert(len(decisions) > 0, "the noisy key was throttled"); err != nil {
		return err
	}
	onlyNoisy := true
	for _, d := range decisions {
		if d.key != noisyKey {
			onlyNoisy = false
		}
	}
	if err := assert(
		onlyNoisy,
		"the quiet key was never throttled, so the limiter is not just firing at everything",
	); err != nil {
		return err
	}

	// The decisions are readable by whatever enforces them, in order, per key.
	// No consumer group is named, so this read goes through the queue's own
	// cursor, and TimeoutMillis bounds the call at two seconds (the default
	// long poll is 30 s).
	throttled, err := client.Queue(throttledQueue).
		Batch(50).
		Partitions(10).
		Wait(true).
		TimeoutMillis(2000).
		Pop(ctx)
	if err != nil {
		return fmt.Errorf("read throttle decisions: %w", err)
	}

	if err := assert(
		len(throttled) == len(decisions),
		"every decision is on the queue the gateway reads",
	); err != nil {
		return err
	}
	carriesCounts := true
	for _, m := range throttled {
		window, okW := m.Data["window"].(float64)
		quota, okQ := m.Data["quota"].(float64)
		if !okW || !okQ || window <= quota {
			carriesCounts = false
		}
	}
	if err := assert(
		carriesCounts,
		"each decision carries the count and the quota that produced it",
	); err != nil {
		return err
	}

	stop()

	// Clean up on success only: a failed run returns before this and leaves the
	// three queues, and the query's window state, on the broker.
	for _, q := range []string{requestsQueue, usageQueue, throttledQueue} {
		if _, err := client.Queue(q).Delete().Execute(ctx); err != nil {
			return fmt.Errorf("delete %s: %w", q, err)
		}
	}

	return nil
}

// cost reads the request's cost out of a payload. Extractors are handed the
// decoded payload as an interface{}, so the shape is checked here at run time,
// and a request that carries no cost counts as one.
func cost(m interface{}) float64 {
	payload, ok := m.(map[string]interface{})
	if !ok {
		return 1
	}
	v, ok := payload["cost"].(float64)
	if !ok {
		return 1
	}
	return v
}

// docs:end
