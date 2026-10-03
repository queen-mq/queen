// docs:start(app-go-webhooks)
//
// A webhook sender: ordered per endpoint, retried by the broker, and
// dead-lettered with its error when it never succeeds.
//
// Deliveries to one endpoint have to arrive in the order the events happened,
// an endpoint that is down must not slow anybody else down, a failure is
// retried a bounded number of times, and what never succeeds has to end up
// somewhere a person can read it. Each endpoint gets a partition of its own,
// created by its first delivery, so a dead endpoint backs up its own partition
// and nothing else. Retries are the broker's retry budget, and an exhausted
// delivery lands in the dead-letter queue with the error attached.
//
//	webhook-deliveries (one partition per endpoint)
//	  `-- group "sender"  POSTs each delivery; an error spends one retry
//	        `-- RetryLimit spent -> dead-letter queue, with the error
//
// Run it:
//
//	QUEEN_URL=http://localhost:6632 GOWORK=off go run ./webhooks
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

var runID = strconv.FormatInt(time.Now().UnixMilli(), 36)

var deliveriesQueue = "app-go-webhooks-" + runID

const group = "sender"

// Three subscribers. One of them answers 500 to everything. It is listed first,
// so its deliveries are the oldest in the queue and its partition is usually
// handed out first: a sender that let a failing endpoint hold up the others
// would fail the checks below. The list is a slice so the queuing order is the
// same on every run, which Go map iteration would not give.
type endpoint struct {
	host    string
	healthy bool
}

var endpoints = []endpoint{
	{host: "initech.example", healthy: false},
	{host: "acme.example", healthy: true},
	{host: "globex.example", healthy: true},
}

const (
	eventsPerEndpoint = 3
	retryLimit        = 2
	deadEndpoint      = "initech.example"
)

func isHealthy(host string) bool {
	for _, e := range endpoints {
		if e.host == host {
			return e.healthy
		}
	}
	return false
}

// postToEndpoint stands in for the HTTP POST to the subscriber. A real sender
// calls net/http and returns an error for any status that is not 2xx, which is
// what this does.
func postToEndpoint(host string, event map[string]interface{}) error {
	if !isHealthy(host) {
		return fmt.Errorf("%s answered 500", host)
	}
	return nil
}

var checks int

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

	// One context bounds the whole program, so a broker that stops answering
	// fails the run when it expires.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	fmt.Printf("broker %s\n", brokerURL)

	// RetryLimit is the delivery budget: a delivery that fails RetryLimit + 1
	// times is filed in the dead-letter queue with its last error, because
	// DlqAfterMaxRetries is on. LeaseTime is how long the broker waits for a
	// sender that took a delivery and never came back before it hands the
	// delivery to another sender.
	if _, err := client.Queue(deliveriesQueue).
		Config(queen.QueueConfig{
			LeaseTime:          30,
			RetryLimit:         retryLimit,
			DlqAfterMaxRetries: true,
		}).
		Create().Execute(ctx); err != nil {
		return fmt.Errorf("create %s: %w", deliveriesQueue, err)
	}

	// ------------------------------------------------------------------ queuing
	//
	// The application emits events. Each delivery goes into the partition of
	// the endpoint it is for, which is what makes "in order per subscriber" a
	// property of the storage instead of something the sender has to arrange.
	fmt.Println("\nqueuing deliveries")
	for seq := 1; seq <= eventsPerEndpoint; seq++ {
		for _, e := range endpoints {
			// The event id, which this client takes on the push builder. An
			// application that retries its own emit does not create a second
			// delivery.
			if _, err := client.Queue(deliveriesQueue).
				Partition(e.host).
				Push(map[string]interface{}{
					"endpoint":  e.host,
					"seq":       seq,
					"type":      "invoice.paid",
					"invoiceId": fmt.Sprintf("INV-%d", seq),
				}).
				TransactionID(fmt.Sprintf("%s-evt-%d", e.host, seq)).
				Execute(ctx); err != nil {
				return fmt.Errorf("queue %s/%d: %w", e.host, seq, err)
			}
		}
	}
	fmt.Printf("  %d deliveries queued\n", eventsPerEndpoint*len(endpoints))

	// ------------------------------------------------------------------ sending
	//
	// The sender pool. Auto-ack is the default, so a handler that returns nil
	// acknowledges the delivery and a handler that returns an error gives it
	// back with the error: the broker redelivers it and counts one retry. The
	// retries live in the broker, so they survive the sender dying halfway,
	// which a retry loop inside the handler would not.
	//
	// Partitions(1): every pop takes ONE endpoint. After a failed delivery the
	// client skips the rest of that pop, so deliveries to other endpoints that
	// came in the same pop would wait for their lease to run out.
	fmt.Println("\nsending")
	var mu sync.Mutex
	deliveredTo := map[string][]int{}
	attempts := map[string]int{}
	// The broker numbers every delivery (deliveryAttempt in the pop response),
	// but this client's Message does not expose that field, and RetryCount is
	// filled only on a dead-letter read. So the sender counts its own attempts
	// at each event, by event id, for the log line.
	tried := map[string]int{}

	err = client.Queue(deliveriesQueue).
		Group(group).
		SubscriptionMode(queen.SubscriptionModeAll).
		Concurrency(3).
		Partitions(1).
		Each().
		// Long polls end after a second and a sender stops after three quiet
		// seconds, so the program finishes. A service runs without these two.
		TimeoutMillis(1000).
		IdleMillis(3000).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			host, _ := msg.Data["endpoint"].(string)
			seq, ok := msg.Data["seq"].(float64)
			if !ok {
				return fmt.Errorf("delivery %s has no numeric seq", msg.TransactionID)
			}

			// Three senders are three goroutines in this handler, so the
			// bookkeeping is behind a mutex.
			mu.Lock()
			attempts[host]++
			tried[msg.TransactionID]++
			attempt := tried[msg.TransactionID]
			mu.Unlock()

			if err := postToEndpoint(host, msg.Data); err != nil {
				fmt.Printf("  %s <- event %d failed on attempt %d: %v\n", host, int(seq), attempt, err)
				return err
			}

			mu.Lock()
			deliveredTo[host] = append(deliveredTo[host], int(seq))
			mu.Unlock()
			fmt.Printf("  %s <- event %d\n", host, int(seq))
			return nil
		}).
		Execute(ctx)
	if err != nil {
		return fmt.Errorf("sending: %w", err)
	}

	// ------------------------------------------------------------------ checking
	fmt.Println("\nchecking")

	for _, e := range endpoints {
		if !e.healthy {
			continue
		}
		seqs := deliveredTo[e.host]
		if err := assert(
			len(seqs) == eventsPerEndpoint,
			fmt.Sprintf("%s received all %d events (got %d)", e.host, eventsPerEndpoint, len(seqs)),
		); err != nil {
			return err
		}
		if err := assert(
			slices.Equal(seqs, []int{1, 2, 3}),
			fmt.Sprintf("%s received its events in the order they happened (got %s)", e.host, orNone(joinInts(seqs))),
		); err != nil {
			return err
		}
	}

	if err := assert(len(deliveredTo[deadEndpoint]) == 0, "the dead endpoint received nothing"); err != nil {
		return err
	}
	if err := assert(
		attempts[deadEndpoint] == eventsPerEndpoint*(retryLimit+1),
		fmt.Sprintf("each dead delivery was tried %d times before it was given up (%d attempts)",
			retryLimit+1, attempts[deadEndpoint]),
	); err != nil {
		return err
	}

	// The dead letters are records you can query. Each one keeps the payload,
	// so it names the endpoint and the invoice, and the last error, which is
	// what answers "why did this customer not get the webhook". DLQ takes a
	// consumer group to filter by; empty means every group on this queue.
	dlq, err := client.Queue(deliveriesQueue).DLQ("").Limit(50).Get(ctx)
	if err != nil {
		return fmt.Errorf("read dead letters: %w", err)
	}

	var dead []queen.Message
	for _, m := range dlq.Messages {
		if host, _ := m.Data["endpoint"].(string); host == deadEndpoint {
			dead = append(dead, m)
		}
	}

	if err := assert(
		len(dead) == eventsPerEndpoint,
		fmt.Sprintf("all %d dead deliveries are in the dead-letter queue", eventsPerEndpoint),
	); err != nil {
		return err
	}

	carriesError := true
	for _, m := range dead {
		if !strings.Contains(m.ErrorMessage, "answered 500") {
			carriesError = false
		}
	}
	if err := assert(carriesError, "each dead letter carries the error that killed it"); err != nil {
		return err
	}
	if err := assert(
		len(dlq.Messages) == len(dead),
		"no healthy delivery ended up in the dead-letter queue",
	); err != nil {
		return err
	}

	names := make([]string, 0, len(dead))
	for _, m := range dead {
		host, _ := m.Data["endpoint"].(string)
		invoice, _ := m.Data["invoiceId"].(string)
		names = append(names, fmt.Sprintf("%s/%s: %s", host, invoice, m.ErrorMessage))
	}
	fmt.Printf("\n  dead letters: %s\n", strings.Join(names, "; "))

	// Clean up on success only: a failed run leaves the queue and its dead
	// letters on the broker to be looked at.
	if _, err := client.Queue(deliveriesQueue).Delete().Execute(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", deliveriesQueue, err)
	}

	return nil
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// docs:end
