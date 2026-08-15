// docs:start(tut-go-replay)
//
// Tutorial 4 of 5: replay.
//
// Acknowledging a message does not delete it. Consumption is a cursor per
// consumer group, and the messages stay until retention removes them, so a new
// group can read the whole history and an existing group can be moved back.
//
// This is the tutorial that shows what a cursor buys you: reprocessing after a
// bug, backfilling a new consumer, and auditing what was delivered, all without
// asking the producer to send anything twice.
//
// Run it:
//
//	QUEEN_URL=http://localhost:6632 GOWORK=off go run ./replay
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

var runID = strconv.FormatInt(time.Now().UnixMilli(), 36)

var eventsQueue = "tut-go-replay-" + runID

const (
	lane       = "order-1"
	liveGroup  = "tut-go-live"
	auditGroup = "tut-go-audit"
)

var eventsIn = []map[string]interface{}{
	{"seq": 1, "type": "created"},
	{"seq": 2, "type": "updated"},
	{"seq": 3, "type": "shipped"},
	{"seq": 4, "type": "delivered"},
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

// drain reads one lane with one consumer group and returns the sequence numbers
// it saw, in the order it saw them. An empty mode leaves the subscription mode
// off the request, which is what you want for a group that already exists.
func drain(ctx context.Context, client *queen.Queen, group string, expected int, mode string) ([]int, error) {
	var seen []int

	builder := client.Queue(eventsQueue).
		Partition(lane).
		Group(group).
		Each().
		Limit(expected).
		// The consumer gives up after 4s of silence rather than waiting
		// forever, so a group that reads nothing fails this run instead of
		// blocking it. The poll is capped at a second so that deadline is
		// noticed promptly.
		IdleMillis(4000).
		TimeoutMillis(1000)
	if mode != "" {
		builder = builder.SubscriptionMode(mode)
	}

	err := builder.Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
		seq, ok := msg.Data["seq"].(float64)
		if !ok {
			return fmt.Errorf("event %s has no numeric seq", msg.TransactionID)
		}
		seen = append(seen, int(seq))
		return nil
	}).Execute(ctx)

	return seen, err
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

	for _, event := range eventsIn {
		if _, err := client.Queue(eventsQueue).Partition(lane).Push(event).Execute(ctx); err != nil {
			return fmt.Errorf("push %v: %w", event["seq"], err)
		}
	}
	fmt.Printf("pushed %d events\n", len(eventsIn))

	// The live consumer. It drains the lane and commits as it goes.
	fmt.Println("\nthe live consumer")
	live, err := drain(ctx, client, liveGroup, 4, queen.SubscriptionModeAll)
	if err != nil {
		return fmt.Errorf("live consumer: %w", err)
	}
	fmt.Printf("  saw %v\n", live)
	if err := assert(slices.Equal(live, []int{1, 2, 3, 4}), "the live group read the lane in order"); err != nil {
		return err
	}

	// A second group, created now, after every message was already stored and
	// acknowledged by someone else. SubscriptionMode("all") is what points its
	// new cursor at the beginning: the default for a new group is the tail, so
	// without it this group would sit idle waiting for the next event.
	//
	// The mode applies when the cursor is created and never again, so it cannot
	// rewind a group that already exists. That is what the seek below is for.
	fmt.Println("\na new group, backfilled from the beginning")
	audit, err := drain(ctx, client, auditGroup, 4, queen.SubscriptionModeAll)
	if err != nil {
		return fmt.Errorf("audit consumer: %w", err)
	}
	fmt.Printf("  saw %v\n", audit)
	if err := assert(slices.Equal(audit, []int{1, 2, 3, 4}), "a new group replayed the whole history"); err != nil {
		return err
	}

	// Nothing was re-pushed and nothing was copied: both groups read the same
	// stored messages through their own cursors.
	fmt.Println("\nrewinding an existing group")

	// Move the live group's cursor back an hour, which is before anything in
	// this run was pushed. The seek also releases any live lease, so an
	// in-flight batch is abandoned rather than acknowledged.
	//
	// The admin API takes exactly one of a timestamp or ToEnd; there is no
	// "seek to the beginning" flag, an early enough timestamp is how you say it.
	if _, err := client.Admin().SeekConsumerGroup(ctx, liveGroup, eventsQueue, queen.SeekConsumerGroupOptions{
		Timestamp: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	replayed, err := drain(ctx, client, liveGroup, 4, "")
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	fmt.Printf("  saw %v\n", replayed)
	if err := assert(
		slices.Equal(replayed, []int{1, 2, 3, 4}),
		"the rewound group read the same events again, in the same order",
	); err != nil {
		return err
	}

	// Replay is per group. The audit group was not moved, so it stays where it
	// was and sees nothing new.
	auditAgain, err := client.Queue(eventsQueue).
		Partition(lane).
		Group(auditGroup).
		Batch(10).
		Wait(false).
		Pop(ctx)
	if err != nil {
		return fmt.Errorf("audit re-check: %w", err)
	}
	if err := assert(len(auditAgain) == 0, "rewinding one group left the other where it was"); err != nil {
		return err
	}

	if _, err := client.Queue(eventsQueue).Delete().Execute(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", eventsQueue, err)
	}

	return nil
}

// docs:end
