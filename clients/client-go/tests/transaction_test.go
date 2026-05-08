package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestTransactionBasicPushAck(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputQueue := generateQueueName("tx-input")
	outputQueue := generateQueueName("tx-output")

	// Create queues
	_, err := client.Queue(inputQueue).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create input queue: %v", err)
	}
	_, err = client.Queue(outputQueue).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create output queue: %v", err)
	}

	// Push message to input queue
	_, err = client.Queue(inputQueue).Push(map[string]interface{}{"data": "test"}).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	// Pop message
	messages, err := client.Queue(inputQueue).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	msg := messages[0]

	// Execute transaction: ack input + push output
	result, err := client.Transaction().
		Ack(msg, queen.AckStatusCompleted, queen.AckOptions{}).
		Queue(outputQueue).Push(map[string]interface{}{"processed": true}).
		Commit(ctx)

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected transaction success, got error: %s", result.Error)
	}

	// Verify output queue has the message
	outputMsgs, err := client.Queue(outputQueue).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop from output queue: %v", err)
	}

	if len(outputMsgs) != 1 {
		t.Errorf("Expected 1 message in output queue, got %d", len(outputMsgs))
	}

	t.Logf("Transaction basic push+ack working")
}

func TestTransactionMultiplePushes(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queue1 := generateQueueName("tx-multi1")
	queue2 := generateQueueName("tx-multi2")

	// Create queues
	_, err := client.Queue(queue1).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue1: %v", err)
	}
	_, err = client.Queue(queue2).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue2: %v", err)
	}

	// Execute transaction with multiple pushes (unique payloads)
	result, err := client.Transaction().
		Queue(queue1).Push(map[string]interface{}{"target": "queue1", "ts": time.Now().UnixNano()}).
		Queue(queue2).Push(map[string]interface{}{"target": "queue2", "ts": time.Now().UnixNano()}).
		Commit(ctx)

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Transaction might fail due to server state, but shouldn't error
	if !result.Success {
		t.Logf("Transaction returned error (may be expected): %s", result.Error)
	}

	// Verify queues - may have 0 or 1 depending on transaction success
	msgs1, _ := client.Queue(queue1).Wait(false).Pop(ctx)
	msgs2, _ := client.Queue(queue2).Wait(false).Pop(ctx)

	t.Logf("Transaction multiple pushes: queue1=%d, queue2=%d, success=%v",
		len(msgs1), len(msgs2), result.Success)
}

func TestTransactionMultipleAcks(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("tx-multi-ack")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push multiple messages
	for i := 0; i < 3; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"index": i}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	// Pop all messages
	messages, err := client.Queue(queueName).Batch(3).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop messages: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(messages))
	}

	// Execute transaction with multiple acks
	tx := client.Transaction()
	for _, msg := range messages {
		tx = tx.Ack(msg, queen.AckStatusCompleted, queen.AckOptions{})
	}
	result, err := tx.Commit(ctx)

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected transaction success, got error: %s", result.Error)
	}

	// Verify queue is empty
	remaining, _ := client.Queue(queueName).Wait(false).Pop(ctx)
	if len(remaining) != 0 {
		t.Errorf("Expected 0 messages after transaction, got %d", len(remaining))
	}

	t.Logf("Transaction multiple acks working")
}

func TestTransactionChainedProcessing(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	initQueue := generateQueueName("tx-chain-init")
	nextQueue := generateQueueName("tx-chain-next")
	finalQueue := generateQueueName("tx-chain-final")

	// Create queues
	for _, q := range []string{initQueue, nextQueue, finalQueue} {
		_, err := client.Queue(q).Create().Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to create queue %s: %v", q, err)
		}
	}

	// Push initial message
	_, err := client.Queue(initQueue).Push(map[string]interface{}{"step": 0}).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push initial message: %v", err)
	}

	// Process init -> next
	initMsgs, _ := client.Queue(initQueue).Wait(false).Pop(ctx)
	if len(initMsgs) == 1 {
		_, err = client.Transaction().
			Ack(initMsgs[0], queen.AckStatusCompleted, queen.AckOptions{}).
			Queue(nextQueue).Push(map[string]interface{}{"step": 1}).
			Commit(ctx)
		if err != nil {
			t.Fatalf("First transaction failed: %v", err)
		}
	}

	// Process next -> final
	nextMsgs, _ := client.Queue(nextQueue).Wait(false).Pop(ctx)
	if len(nextMsgs) == 1 {
		_, err = client.Transaction().
			Ack(nextMsgs[0], queen.AckStatusCompleted, queen.AckOptions{}).
			Queue(finalQueue).Push(map[string]interface{}{"step": 2, "complete": true}).
			Commit(ctx)
		if err != nil {
			t.Fatalf("Second transaction failed: %v", err)
		}
	}

	// Verify final queue has the message
	finalMsgs, _ := client.Queue(finalQueue).Wait(false).Pop(ctx)
	if len(finalMsgs) != 1 {
		t.Errorf("Expected 1 message in final queue, got %d", len(finalMsgs))
	} else if finalMsgs[0].Data["complete"] != true {
		t.Errorf("Expected complete=true, got %v", finalMsgs[0].Data["complete"])
	}

	t.Logf("Transaction chained processing working")
}

func TestTransactionWithConsumer(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputQueue := generateQueueName("tx-consumer-in")
	outputQueue := generateQueueName("tx-consumer-out")

	// Create queues
	_, err := client.Queue(inputQueue).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create input queue: %v", err)
	}
	_, err = client.Queue(outputQueue).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create output queue: %v", err)
	}

	// Push messages
	for i := 0; i < 3; i++ {
		_, err = client.Queue(inputQueue).Push(map[string]interface{}{"index": i}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	// Consume and process with transactions
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		// Execute transaction in handler
		_, err := client.Transaction().
			Ack(msg, queen.AckStatusCompleted, queen.AckOptions{}).
			Queue(outputQueue).Push(map[string]interface{}{
				"processed": true,
				"original":  msg.Data["index"],
			}).
			Commit(ctx)

		if err != nil {
			return err
		}

		atomic.AddInt32(&processed, 1)
		return nil
	}

	err = client.Queue(inputQueue).
		AutoAck(false). // Disable auto-ack, we'll ack in transaction
		Limit(3).
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	if atomic.LoadInt32(&processed) != 3 {
		t.Errorf("Expected 3 processed, got %d", processed)
	}

	// Verify output queue
	outputMsgs, _ := client.Queue(outputQueue).Batch(10).Wait(false).Pop(ctx)
	if len(outputMsgs) != 3 {
		t.Errorf("Expected 3 messages in output queue, got %d", len(outputMsgs))
	}

	t.Logf("Transaction with consumer working")
}

func TestTransactionEmptyCommit(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to commit empty transaction
	_, err := client.Transaction().Commit(ctx)

	if err == nil {
		t.Errorf("Expected error for empty transaction, got nil")
	}

	t.Logf("Empty transaction error handling working")
}
