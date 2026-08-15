// docs:start(tut-go-streaming)
//
// Tutorial 5 of 5: streaming.
//
// The four tutorials before this one move messages. This one aggregates them:
// a tumbling window per entity, whose state, output and acknowledgements commit
// in the same PostgreSQL transaction. That is exactly-once aggregation with no
// changelog topic and no state store to operate, because the state and the
// queue are already in the same database.
//
// A stream is a running process, so the order here is the order you would use
// in production: start it, then let events arrive.
//
// Run it:
//
//	QUEEN_URL=http://localhost:6632 GOWORK=off go run ./streaming
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
)

var runID = strconv.FormatInt(time.Now().UnixMilli(), 36)

var (
	eventsQueue = "tut-go-stream-events-" + runID
	totalsQueue = "tut-go-stream-totals-" + runID

	// The query id is this streaming query's identity in the database. Its
	// window state is keyed by it, so restarting the program with the same id
	// resumes the same windows instead of starting new ones.
	queryID = "tut-go-totals-" + runID
)

const collectorGroup = "tut-go-stream-collector"

type sale struct {
	Customer string
	Amount   float64
}

var sales = []sale{
	{Customer: "acme", Amount: 10},
	{Customer: "acme", Amount: 32.5},
	{Customer: "globex", Amount: 7.25},
	{Customer: "acme", Amount: 0.25},
	{Customer: "globex", Amount: 100},
}

var checks int

// stopping is set just before the stream is shut down, and read by the logger
// below.
var stopping atomic.Bool

// streamLogger is what the runner reports through. Stopping the runner cancels
// whatever poll it had in flight, and the pop loop reports that cancellation as
// an error on its way out: that one is the shutdown itself, not a fault, so it
// is dropped. Everything else is printed, because a stream that is failing to
// commit its windows would otherwise fail this run with no explanation.
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

func assert(condition bool, description string) error {
	if !condition {
		return fmt.Errorf("%s", description)
	}
	checks++
	fmt.Printf("  ok: %s\n", description)
	return nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	fmt.Printf("broker %s\n", brokerURL)

	// Both queues are created up front here, rather than by the first push, so
	// the stream has something to attach to before any event exists.
	if _, err := client.Queue(eventsQueue).
		Config(queen.QueueConfig{LeaseTime: 30, RetryLimit: 3}).
		Create().Execute(ctx); err != nil {
		return fmt.Errorf("create %s: %w", eventsQueue, err)
	}
	if _, err := client.Queue(totalsQueue).
		Config(queen.QueueConfig{LeaseTime: 30}).
		Create().Execute(ctx); err != nil {
		return fmt.Errorf("create %s: %w", totalsQueue, err)
	}

	fmt.Println("\nstarting the stream")

	// AsStreamSource adapts a queue builder to what the streaming engine reads
	// from; .To takes the queue builder itself, since a sink is only a name.
	runner, err := streams.
		From(client.Queue(eventsQueue).AsStreamSource()).
		// Tumbling: fixed, non-overlapping windows, one set per partition. A
		// window closes when its time is up; WithIdleFlushMs also closes one
		// whose partition has gone quiet, which is what lets a short program
		// finish.
		WindowTumbling(2, streams.WithIdleFlushMs(800)).
		// The extractors receive the message payload itself, not the envelope:
		// it is m["amount"], not m["data"]["amount"]. The difference is silent,
		// because a missing field aggregates to zero.
		//
		// The name is the aggregation: count, sum, min, max and avg each fold
		// their extractor's value differently, and any other name accumulates
		// like a sum. Go map iteration is random, so the field order is passed
		// explicitly after the map. Leave it out and the client falls back to
		// alphabetical, which is stable but is not the order the same query
		// written in JavaScript or Python hashes to, so the two would no longer
		// be the same query.
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return amount(m), nil },
			"max":   func(m interface{}) (float64, error) { return amount(m), nil },
		}, "count", "sum", "max").
		// Every closed window is pushed here, in the same transaction that
		// commits the window state and acknowledges the inputs it was computed
		// from.
		To(client.Queue(totalsQueue)).
		Run(ctx, streams.RunOptions{
			QueryID:       queryID,
			URL:           brokerURL,
			BatchSize:     100,
			MaxPartitions: 8,
			MaxWaitMillis: 200,
			Logger:        streamLogger{},
		})
	if err != nil {
		return fmt.Errorf("start stream: %w", err)
	}
	// Stop waits for the pop loop and the idle-flush loop to leave, and is
	// idempotent, so it is safe both here as a guard and explicitly below.
	stop := func() {
		stopping.Store(true)
		runner.Stop()
	}
	defer stop()

	// Run returns as soon as the query is registered, with the pop loop running
	// in its own goroutine. That loop's first poll is what creates the stream's
	// consumer group, and a new group starts at the tail: give it that first
	// poll before producing, or the events raced past a cursor that did not
	// exist yet.
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\npushing sales")
	for _, s := range sales {
		// The partition is the aggregation key: window state is per partition,
		// so one customer's totals are computed from that customer's lane
		// alone.
		if _, err := client.Queue(eventsQueue).
			Partition(s.Customer).
			Push(map[string]interface{}{"customer": s.Customer, "amount": s.Amount}).
			Execute(ctx); err != nil {
			return fmt.Errorf("push sale: %w", err)
		}
		fmt.Printf("  %s %v\n", s.Customer, s.Amount)
	}

	fmt.Println("\ncollecting closed windows")

	// A window is a slice of time, so a customer's sales can fall on either side
	// of a boundary and arrive as two windows instead of one. That is what
	// windowing is, and it is why this adds the windows up per customer instead
	// of expecting exactly one each.
	//
	// The loop waits for the totals it expects, with a deadline. Stopping on a
	// quiet period instead would be a race: the last window closes when its
	// timer says so, not when the reader is tired of waiting.
	type totals struct {
		count float64
		sum   float64
		max   float64
	}
	perCustomer := map[string]*totals{}
	complete := func() bool {
		acme, globex := perCustomer["acme"], perCustomer["globex"]
		return acme != nil && acme.count == 3 && globex != nil && globex.count == 2
	}
	deadline := time.Now().Add(30 * time.Second)

	for !complete() && time.Now().Before(deadline) {
		closed, err := client.Queue(totalsQueue).
			Group(collectorGroup).
			SubscriptionMode(queen.SubscriptionModeAll).
			Batch(20).
			// Each customer's windows land in that customer's partition, and a
			// pop claims a single partition unless asked for more.
			Partitions(10).
			Wait(true).
			TimeoutMillis(2000).
			Pop(ctx)
		if err != nil {
			return fmt.Errorf("collect windows: %w", err)
		}

		for _, msg := range closed {
			payload, _ := json.Marshal(msg.Data)
			fmt.Printf("  %s: %s\n", msg.Partition, payload)

			// The window's key is the partition it was computed for.
			t := perCustomer[msg.Partition]
			if t == nil {
				t = &totals{}
				perCustomer[msg.Partition] = t
			}
			count, _ := msg.Data["count"].(float64)
			sum, _ := msg.Data["sum"].(float64)
			max, _ := msg.Data["max"].(float64)
			t.count += count
			t.sum += sum
			if max > t.max {
				t.max = max
			}

			// This is a Pop, not a Consume loop, so nothing acknowledges on your
			// behalf: without this the same windows come back on the next turn
			// and every total is counted twice.
			if _, err := client.Ack(ctx, msg, true, queen.AckOptions{ConsumerGroup: collectorGroup}); err != nil {
				return fmt.Errorf("ack window: %w", err)
			}
		}
	}

	if err := assert(complete(), "every sale reached a closed window before the deadline"); err != nil {
		return err
	}
	if err := assert(abs(perCustomer["acme"].sum-42.75) < 0.001, "acme summed to 42.75 across its windows"); err != nil {
		return err
	}
	if err := assert(abs(perCustomer["globex"].sum-107.25) < 0.001, "globex summed to 107.25"); err != nil {
		return err
	}
	if err := assert(perCustomer["globex"].max == 100, "globex kept its largest single sale"); err != nil {
		return err
	}

	stop()

	if _, err := client.Queue(eventsQueue).Delete().Execute(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", eventsQueue, err)
	}
	if _, err := client.Queue(totalsQueue).Delete().Execute(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", totalsQueue, err)
	}

	return nil
}

// amount reads the sale amount out of a payload. Extractors are handed the
// decoded payload as an interface{}, so the shape is checked here rather than
// by the compiler, and anything unexpected aggregates as zero.
func amount(m interface{}) float64 {
	payload, ok := m.(map[string]interface{})
	if !ok {
		return 0
	}
	v, _ := payload["amount"].(float64)
	return v
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// docs:end
