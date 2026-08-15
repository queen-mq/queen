// docs:start(app-go-webhooks)
//
// A webhook delivery system.
//
// Every SaaS product ends up writing this one, and it is harder than it looks:
// deliveries to one customer's endpoint must arrive in order, a customer whose
// endpoint is down must not slow down anybody else's, failures must be retried
// a bounded number of times, and what never succeeds has to end up somewhere a
// human can look at.
//
// The shape here is one ordered lane per destination, created by the first
// delivery to it. A dead endpoint backs up its own lane and no other; retries
// are the broker's retry budget rather than a loop in your code; and what
// exhausts the budget lands in the dead-letter queue with the error attached.
//
//	webhook-deliveries (one partition per destination)
//	  |-- group "sender"  posts each delivery, fails on a dead endpoint
//	        |-- retryLimit exhausted -> dead-letter queue
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

// Three subscribers. One of them has let its certificate expire, which is the
// most common way a webhook endpoint dies: it answers, but it answers 500. The
// list is a slice rather than a map so the queuing order below is the same on
// every run, which Go map iteration would not give.
type endpoint struct {
	host    string
	healthy bool
}

var endpoints = []endpoint{
	{host: "acme.example", healthy: true},
	{host: "globex.example", healthy: true},
	{host: "initech.example", healthy: false},
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
// would use net/http and treat any non-2xx as a failure, which is exactly what
// returning an error does here: with auto-ack on, an error from the handler is
// what the client turns into a negative acknowledgement.
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

	// One context bounds the whole program: a broker that stops answering ends
	// the run instead of wedging it.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	fmt.Printf("broker %s\n", brokerURL)

	// RetryLimit is the delivery budget, and DlqAfterMaxRetries is what happens
	// when it runs out. Without the second flag an exhausted message is simply
	// marked failed and stays put; with it, the broker moves it to the
	// dead-letter table with the last error on the row.
	//
	// LeaseTime is the other half of the contract: it is how long the broker
	// waits for a sender that took a delivery and never came back before
	// handing that delivery to someone else.
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
	// The application emits events. Each one goes into the partition of the
	// endpoint it is destined for, which is what makes "in order per subscriber"
	// a property of the storage rather than of the sender.
	fmt.Println("\nqueuing deliveries")
	for seq := 1; seq <= eventsPerEndpoint; seq++ {
		for _, e := range endpoints {
			// The event id makes the enqueue idempotent: an application that
			// retries its own emit does not create a second delivery. This
			// client carries it on the push builder, not in the payload.
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
	// The sender pool. Auto-ack is the default here, so a handler that returns
	// nil acknowledges the delivery and one that returns an error nacks it: the
	// broker then redelivers it until the retry budget is gone. That is the
	// whole retry mechanism, and it survives the sender process dying
	// mid-flight, which a loop inside the handler would not.
	fmt.Println("\nsending")
	var mu sync.Mutex
	deliveredTo := map[string][]int{}
	attempts := map[string]int{}

	err = client.Queue(deliveriesQueue).
		Group(group).
		SubscriptionMode(queen.SubscriptionModeAll).
		Concurrency(3).
		Each().
		// Enough turns for every good delivery plus every attempt at the bad
		// ones. Limit is per worker rather than a budget shared by the pool,
		// so it is a ceiling on a runaway goroutine; what actually
		// ends the pool is the idle bound, once the healthy lanes are drained
		// and the dead one has burnt its budget into the dead-letter queue.
		// TimeoutMillis caps each poll at a second so that bound is noticed
		// promptly instead of inside a 30 s long poll.
		Limit(eventsPerEndpoint*2+eventsPerEndpoint*(retryLimit+1)).
		IdleMillis(6000).
		TimeoutMillis(1000).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			host, _ := msg.Data["endpoint"].(string)
			seq, ok := msg.Data["seq"].(float64)
			if !ok {
				return fmt.Errorf("delivery %s has no numeric seq", msg.TransactionID)
			}

			// Three poll loops means three goroutines in this handler, so the
			// bookkeeping is behind a mutex. The JavaScript version needs no
			// lock because it has no threads.
			mu.Lock()
			attempts[host]++
			mu.Unlock()

			// msg.RetryCount exists on the struct but stays zero here: the
			// broker's pop response carries no attempt counter, and the field is
			// only filled on a dead-letter read. The budget is the broker's, and
			// returning an error is how one attempt of it is spent. A sender
			// that recognises a permanent error can skip the attempts it has
			// left only through the HTTP ack route, which takes a status string:
			// this client's Ack takes a bool, completed or failed, so unlike the
			// JavaScript one it cannot mark a delivery dead on the spot.
			if err := postToEndpoint(host, msg.Data); err != nil {
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
			fmt.Sprintf("%s received all %d events", e.host, eventsPerEndpoint),
		); err != nil {
			return err
		}
		if err := assert(
			slices.Equal(seqs, []int{1, 2, 3}),
			fmt.Sprintf("%s received them in the order they happened", e.host),
		); err != nil {
			return err
		}
	}

	if err := assert(
		len(deliveredTo[deadEndpoint]) == 0,
		"the dead endpoint received nothing, as it should",
	); err != nil {
		return err
	}
	if err := assert(
		attempts[deadEndpoint] > eventsPerEndpoint,
		"the dead endpoint was retried rather than dropped on the first failure",
	); err != nil {
		return err
	}

	// The dead-letter queue is a table you can read, not a log line. Each row
	// carries the payload, the endpoint it was for, and the last error, which is
	// what a support engineer needs to answer "why did this customer not get
	// it". DLQ takes a consumer group to filter by; empty means every group on
	// this queue, which is what the check below wants.
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
	if err := assert(carriesError, "each dead-letter row carries the error that killed it"); err != nil {
		return err
	}

	onlyDead := true
	for _, m := range dlq.Messages {
		if host, _ := m.Data["endpoint"].(string); host != deadEndpoint {
			onlyDead = false
		}
	}
	if err := assert(onlyDead, "no healthy endpoint put anything in the dead-letter queue"); err != nil {
		return err
	}

	names := make([]string, 0, len(dead))
	for _, m := range dead {
		host, _ := m.Data["endpoint"].(string)
		invoice, _ := m.Data["invoiceId"].(string)
		names = append(names, host+"/"+invoice)
	}
	fmt.Printf("\n  dead letters: %s\n", strings.Join(names, ", "))

	// Clean up on success only: a failed run leaves the queue and its dead
	// letters on the broker to be looked at.
	if _, err := client.Queue(deliveriesQueue).Delete().Execute(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", deliveriesQueue, err)
	}

	return nil
}

// docs:end
