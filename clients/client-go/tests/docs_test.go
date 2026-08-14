package tests

// The tests behind the published examples.
//
// Every marked region in this file is rendered on queenmq.com through
// webdoc/scripts/gen-snippets.mjs: a real queue name, a partition key that
// means something, and the error handling a reader should copy. Assertions
// stay outside the markers. After editing a region, regenerate the partials
// with `pnpm --dir webdoc gen` or the docs CI check fails on drift. The
// queues used here (orders, payments, invoices) are wiped by cleanupTestData
// in helpers_test.go, exactly like the test-go-% queues.

import (
	"context"
	"fmt"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestDocsExamples(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := docsProduceAndConsume(ctx, client); err != nil {
		t.Fatalf("docs push/consume/pop example failed: %v", err)
	}
	if err := docsDeduplication(ctx, client); err != nil {
		t.Fatalf("docs deduplication example failed: %v", err)
	}
	if err := docsTransactionalHandoff(ctx, client); err != nil {
		t.Fatalf("docs transaction example failed: %v", err)
	}
}

// docsProduceAndConsume is the body of the published push, consume and pop
// examples: one order in, one consumer group draining it, and a raw pop
// showing that a second cursor still sees the same message.
func docsProduceAndConsume(ctx context.Context, client *queen.Queen) error {
	// docs:start(go-push)
	res, err := client.Queue("orders").
		Partition("customer-42").
		Push(map[string]any{"orderId": 9137, "amount": 99.5}).
		Execute(ctx)
	if err != nil {
		return err
	}
	// res[0].Status == "queued"
	// docs:end
	if len(res) != 1 {
		return fmt.Errorf("expected 1 result, got %d", len(res))
	}
	if res[0].Status != "queued" {
		return fmt.Errorf("expected queued, got %q", res[0].Status)
	}

	// The loop acks each message when the handler returns nil, so by the time
	// Execute returns, the billing group's cursor has moved. Limit(1) is what
	// ends it: without a limit the loop long-polls for the next message.
	// docs:start(go-consume)
	err = client.Queue("orders").
		Group("billing").
		SubscriptionMode("all").
		Limit(1).
		Each().
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			fmt.Println(msg.Data)
			return nil
		}).
		Execute(ctx)
	if err != nil {
		return err
	}
	// docs:end

	drained, err := client.Queue("orders").
		Group("billing").
		Batch(1).
		Wait(false).
		Pop(ctx)
	if err != nil {
		return err
	}
	if len(drained) != 0 {
		return fmt.Errorf("billing group cursor did not advance: %d message(s) still pending", len(drained))
	}

	// A raw pop reads on the queue's own cursor, which the billing group never
	// touched: consumer groups are fan-out.
	// docs:start(go-pop)
	messages, err := client.Queue("orders").
		Batch(10).
		Wait(true).
		Pop(ctx)
	if err != nil {
		return err
	}
	// docs:end
	if len(messages) != 1 {
		return fmt.Errorf("fan-out pop expected 1 message, got %d", len(messages))
	}
	if messages[0].Data["orderId"] != float64(9137) {
		return fmt.Errorf("fan-out pop returned the wrong order: %v", messages[0].Data)
	}
	return nil
}

// docsDeduplication is the body of the published deduplication example: the
// same transaction id twice, the second push writing nothing.
func docsDeduplication(ctx context.Context, client *queen.Queen) error {
	// The fixed transaction id below survives reruns because cleanupTestData
	// purges log_txns for these queues before the suite starts.
	// docs:start(go-push-dedup)
	first, err := client.Queue("payments").
		Partition("customer-42").
		Push(map[string]any{"orderId": 9137, "amount": 99.5}).
		TransactionID("order-9137-paid").
		Execute(ctx)
	if err != nil {
		return err
	}

	retry, err := client.Queue("payments").
		Partition("customer-42").
		Push(map[string]any{"orderId": 9137, "amount": 99.5}).
		TransactionID("order-9137-paid").
		Execute(ctx)
	if err != nil {
		return err
	}
	// retry[0].Status == "duplicate": the second push wrote nothing.
	// docs:end
	if first[0].Status != "queued" {
		return fmt.Errorf("first push not queued: %q", first[0].Status)
	}
	if retry[0].Status != "duplicate" {
		return fmt.Errorf("retry not deduplicated: %q", retry[0].Status)
	}
	return nil
}

// docsTransactionalHandoff is the body of the published transaction example:
// the ack of one stage and the push of the next, committed together.
func docsTransactionalHandoff(ctx context.Context, client *queen.Queen) error {
	seeded, err := client.Queue("orders").
		Partition("customer-77").
		Push(map[string]any{"orderId": 4102, "amount": 18}).
		Execute(ctx)
	if err != nil {
		return err
	}
	if seeded[0].Status != "queued" {
		return fmt.Errorf("seed push not queued: %q", seeded[0].Status)
	}

	// docs:start(go-transaction)
	messages, err := client.Queue("orders").
		Group("invoicing").
		SubscriptionMode("all").
		Batch(1).
		Wait(true).
		Pop(ctx)
	if err != nil {
		return err
	}
	message := messages[0]

	_, err = client.Transaction().
		Queue("invoices").
		Push(map[string]any{"orderId": message.Data["orderId"], "invoiced": true}).
		Ack(message, queen.AckStatusCompleted, queen.AckOptions{ConsumerGroup: "invoicing"}).
		Commit(ctx)
	if err != nil {
		return err
	}
	// docs:end

	// The pop above may hand back an order another docs example pushed to this
	// queue, so assert on identity rather than on a hardcoded value.
	invoiced, err := client.Queue("invoices").
		Batch(1).
		Wait(true).
		Pop(ctx)
	if err != nil {
		return err
	}
	if len(invoiced) != 1 {
		return fmt.Errorf("expected 1 invoice, got %d", len(invoiced))
	}
	if invoiced[0].Data["orderId"] != message.Data["orderId"] {
		return fmt.Errorf("invoice carries the wrong order: %v", invoiced[0].Data)
	}
	if invoiced[0].Data["invoiced"] != true {
		return fmt.Errorf("invoice payload corrupted: %v", invoiced[0].Data)
	}
	return nil
}
