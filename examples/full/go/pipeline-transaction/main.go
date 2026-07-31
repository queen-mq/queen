// docs:start(full-go-pipeline)
// A transactional pipeline: consume from one queue and produce to another
// without ever losing or duplicating work.
//
// Stage one reads an order, and in a SINGLE broker transaction acknowledges
// that order and pushes the invoice derived from it. Stage two reads the
// invoices back and checks that each one is there exactly once. The last part
// acknowledges an order as failed and shows it being redelivered rather than
// dropped.
//
// Run it with:
//
//	QUEEN_URL=http://localhost:6699 go run ./pipeline-transaction
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

const (
	// Each stage of a pipeline reads with its own consumer group, so the two
	// cursors advance independently.
	ordersGroup   = "ex-go-invoicing"
	invoicesGroup = "ex-go-ledger"

	orderCount = 3

	// What stage one adds to every order it turns into an invoice.
	shippingFee = 5
)

// A per-run suffix gives every run queues that have never existed before, so a
// second run behaves exactly like the first.
var runID = time.Now().UnixMilli()

var (
	ordersQueue   = fmt.Sprintf("ex-go-pipeline-orders-%d", runID)
	invoicesQueue = fmt.Sprintf("ex-go-pipeline-invoices-%d", runID)
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK: every order produced exactly one invoice, and the failed order came back")
}

func run(ctx context.Context) error {
	brokerURL := os.Getenv("QUEEN_URL")
	if brokerURL == "" {
		brokerURL = "http://localhost:6699"
	}

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	fmt.Printf("broker: %s\n", brokerURL)

	for _, name := range []string{ordersQueue, invoicesQueue} {
		if _, err := client.Queue(name).Config(queen.QueueConfig{
			LeaseTime:  60, // seconds the handler has before the message is offered to someone else
			RetryLimit: 3,  // failed deliveries tolerated before the message is dead lettered
		}).Create().Execute(ctx); err != nil {
			return fmt.Errorf("create queue %s: %w", name, err)
		}
	}
	fmt.Printf("queues %q and %q ready\n", ordersQueue, invoicesQueue)

	for i := 1; i <= orderCount; i++ {
		if err := pushOrder(ctx, client, fmt.Sprintf("order-%d", i), 100*i); err != nil {
			return err
		}
	}

	if err := stageOne(ctx, client, orderCount); err != nil {
		return err
	}
	if err := stageTwo(ctx, client); err != nil {
		return err
	}
	if err := showFailureIsRedelivered(ctx, client); err != nil {
		return err
	}

	// Housekeeping, not part of the lesson: drop the queues this run created.
	for _, name := range []string{ordersQueue, invoicesQueue} {
		if _, err := client.Queue(name).Delete().Execute(ctx); err != nil {
			return fmt.Errorf("delete queue %s: %w", name, err)
		}
	}
	return nil
}

func pushOrder(ctx context.Context, client *queen.Queen, orderID string, amount int) error {
	responses, err := client.Queue(ordersQueue).
		Push(map[string]interface{}{"orderId": orderID, "amount": amount}).
		Execute(ctx)
	if err != nil {
		return fmt.Errorf("push %s: %w", orderID, err)
	}
	if responses[0].Status != "queued" {
		return fmt.Errorf("push %s: expected status queued, got %q", orderID, responses[0].Status)
	}
	fmt.Printf("pushed %s\n", orderID)
	return nil
}

// stageOne reads orders and hands each one to the invoices queue inside a
// single broker transaction.
func stageOne(ctx context.Context, client *queen.Queen, count int) error {
	for handled := 0; handled < count; handled++ {
		msg, err := popOne(ctx, client, ordersQueue, ordersGroup)
		if err != nil {
			return err
		}
		if msg == nil {
			return fmt.Errorf("only %d of %d orders were delivered", handled, count)
		}

		orderID, amount, err := readOrder(msg)
		if err != nil {
			return err
		}
		invoice := map[string]interface{}{
			"invoiceFor": orderID,
			"total":      amount + shippingFee,
		}

		// The handoff. The acknowledgement of the input and the push of the
		// derived message run in one PostgreSQL transaction, so they either
		// both happen or neither does. Ack first and crash, and the invoice is
		// lost; push first and crash, and the order is invoiced twice on
		// redelivery. This is the shape that has neither failure.
		//
		// The message's leaseId travels with the ack as a required lease, so if
		// the lease expired while this handler was working the commit fails
		// instead of invoicing an order another consumer already owns.
		resp, err := client.Transaction().
			Ack(msg, queen.AckStatusCompleted, queen.AckOptions{ConsumerGroup: ordersGroup}).
			Queue(invoicesQueue).Push(invoice).
			Commit(ctx)
		if err != nil {
			return fmt.Errorf("handoff for %s: %w", orderID, err)
		}
		// Commit reports a rolled back transaction in the body, not as an
		// error, so check Success as well.
		if !resp.Success {
			return fmt.Errorf("handoff for %s rolled back: %s", orderID, resp.Error)
		}
		fmt.Printf("handed off %s as an invoice of %.0f in one transaction\n", orderID, amount+shippingFee)
	}
	return nil
}

// stageTwo reads the invoices back and checks that stage one produced exactly
// one for each order.
func stageTwo(ctx context.Context, client *queen.Queen) error {
	seen := make(map[string]int, orderCount)

	collected := 0
	deadline := time.Now().Add(30 * time.Second)
	for collected < orderCount && time.Now().Before(deadline) {
		msgs, err := client.Queue(invoicesQueue).
			Group(invoicesGroup).
			Batch(orderCount).
			Wait(true).
			TimeoutMillis(5000).
			Pop(ctx)
		if err != nil {
			return fmt.Errorf("pop invoices: %w", err)
		}
		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			invoiceFor, ok := msg.Data["invoiceFor"].(string)
			if !ok {
				return fmt.Errorf("invoice %s has an unexpected payload: %v", msg.TransactionID, msg.Data)
			}
			seen[invoiceFor]++
			collected++
			fmt.Printf("invoice for %s: total %v\n", invoiceFor, msg.Data["total"])
		}

		if err := ackAll(ctx, client, msgs, invoicesGroup); err != nil {
			return err
		}
	}

	if collected != orderCount {
		return fmt.Errorf("expected %d invoices, got %d", orderCount, collected)
	}
	for i := 1; i <= orderCount; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		if seen[orderID] != 1 {
			return fmt.Errorf("expected exactly 1 invoice for %s, got %d", orderID, seen[orderID])
		}
	}
	return nil
}

// showFailureIsRedelivered acknowledges an order as failed and checks that the
// broker hands it back instead of dropping it.
func showFailureIsRedelivered(ctx context.Context, client *queen.Queen) error {
	const orderID = "order-4"
	if err := pushOrder(ctx, client, orderID, 400); err != nil {
		return err
	}

	first, err := popOne(ctx, client, ordersQueue, ordersGroup)
	if err != nil {
		return err
	}
	if first == nil {
		return fmt.Errorf("%s was never delivered", orderID)
	}

	// This is what a handler that could not do its job reports. Nothing was
	// pushed to the invoices queue, because the transaction was never
	// committed, and the failure counts against the queue's retry limit.
	acks, err := client.Ack(ctx, first, false, queen.AckOptions{
		ConsumerGroup: ordersGroup,
		Error:         "payment gateway timed out",
	})
	if err != nil {
		return fmt.Errorf("failed ack: %w", err)
	}
	if !acks[0].Success {
		return fmt.Errorf("failed ack rejected: %s", acks[0].Error)
	}
	fmt.Printf("acked %s as failed\n", orderID)

	// The consumer group cursor stays clamped at the failed message, so the
	// very next pop gets it again. A nack costs a redelivery, never the message.
	second, err := popOne(ctx, client, ordersQueue, ordersGroup)
	if err != nil {
		return err
	}
	if second == nil {
		return fmt.Errorf("%s was not redelivered after the failed ack", orderID)
	}
	if second.TransactionID != first.TransactionID {
		return fmt.Errorf("expected %s back, got a different message %s",
			first.TransactionID, second.TransactionID)
	}
	fmt.Printf("redelivered %s (%s)\n", orderID, second.TransactionID)

	// The retry succeeds, so it takes the same transactional handoff as any
	// other order.
	_, amount, err := readOrder(second)
	if err != nil {
		return err
	}
	resp, err := client.Transaction().
		Ack(second, queen.AckStatusCompleted, queen.AckOptions{ConsumerGroup: ordersGroup}).
		Queue(invoicesQueue).Push(map[string]interface{}{
		"invoiceFor": orderID,
		"total":      amount + shippingFee,
	}).
		Commit(ctx)
	if err != nil {
		return fmt.Errorf("handoff for %s: %w", orderID, err)
	}
	if !resp.Success {
		return fmt.Errorf("handoff for %s rolled back: %s", orderID, resp.Error)
	}

	// One invoice, not two: the failed attempt pushed nothing at all.
	invoice, err := popOne(ctx, client, invoicesQueue, invoicesGroup)
	if err != nil {
		return err
	}
	if invoice == nil {
		return fmt.Errorf("the retry of %s produced no invoice", orderID)
	}
	if invoice.Data["invoiceFor"] != orderID {
		return fmt.Errorf("expected an invoice for %s, got %v", orderID, invoice.Data["invoiceFor"])
	}
	if err := ackAll(ctx, client, []*queen.Message{invoice}, invoicesGroup); err != nil {
		return err
	}
	fmt.Printf("invoice for %s: total %v\n", orderID, invoice.Data["total"])

	extra, err := client.Queue(invoicesQueue).
		Group(invoicesGroup).
		Batch(10).
		Wait(false).
		Pop(ctx)
	if err != nil {
		return fmt.Errorf("invoice drain check: %w", err)
	}
	if len(extra) != 0 {
		return fmt.Errorf("expected 1 invoice for %s, found %d more", orderID, len(extra))
	}
	return nil
}

// readOrder pulls the fields out of an order payload. JSON numbers arrive as
// float64 in the untyped payload map.
func readOrder(msg *queen.Message) (string, float64, error) {
	orderID, okID := msg.Data["orderId"].(string)
	amount, okAmount := msg.Data["amount"].(float64)
	if !okID || !okAmount {
		return "", 0, fmt.Errorf("order %s has an unexpected payload: %v", msg.TransactionID, msg.Data)
	}
	return orderID, amount, nil
}

// popOne long polls for a single message and gives up after its own deadline.
// Wait(true) is what makes it a long poll: Go's Pop returns immediately with
// whatever is already there unless you ask it to wait.
func popOne(ctx context.Context, client *queen.Queen, queueName, group string) (*queen.Message, error) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := client.Queue(queueName).
			Group(group).
			Batch(1).
			Wait(true).
			TimeoutMillis(5000).
			Pop(ctx)
		if err != nil {
			return nil, fmt.Errorf("pop %s: %w", queueName, err)
		}
		if len(msgs) > 0 {
			return msgs[0], nil
		}
	}
	return nil, nil
}

// ackAll acknowledges a batch and checks every per-item verdict, because a
// rejected ack still arrives as HTTP 200.
func ackAll(ctx context.Context, client *queen.Queen, msgs []*queen.Message, group string) error {
	acks, err := client.Ack(ctx, msgs, true, queen.AckOptions{ConsumerGroup: group})
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	for i, ack := range acks {
		if !ack.Success {
			return fmt.Errorf("ack rejected for %s: %s", msgs[i].TransactionID, ack.Error)
		}
	}
	return nil
}

// docs:end
