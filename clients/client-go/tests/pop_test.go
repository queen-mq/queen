package tests

import (
	"context"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestPopEmptyQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("pop-empty")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Pop from empty queue (no wait)
	messages, err := client.Queue(queueName).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop from empty queue: %v", err)
	}

	if len(messages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(messages))
	}

	t.Logf("Pop from empty queue working: %s", queueName)
}

func TestPopNonEmptyQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("pop-nonempty")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push message
	payload := map[string]interface{}{"test": "data", "value": 42}
	_, err = client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	// Pop message
	messages, err := client.Queue(queueName).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	msg := messages[0]
	if msg.Data["test"] != "data" {
		t.Errorf("Expected data.test='data', got '%v'", msg.Data["test"])
	}
	if msg.Data["value"] != float64(42) {
		t.Errorf("Expected data.value=42, got '%v'", msg.Data["value"])
	}

	t.Logf("Pop from non-empty queue working: %s", queueName)
}

func TestPopWithWait(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	queueName := generateQueueName("pop-wait")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Start a goroutine to push after delay
	go func() {
		time.Sleep(2 * time.Second)
		payload := map[string]interface{}{"delayed": true}
		client.Queue(queueName).Push(payload).Execute(context.Background())
	}()

	// Pop with wait (should block until message arrives)
	start := time.Now()
	messages, err := client.Queue(queueName).Wait(true).TimeoutMillis(10000).Pop(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to pop with wait: %v", err)
	}

	// Should have received message after ~2 seconds
	if elapsed < 1*time.Second {
		t.Errorf("Pop returned too quickly: %v", elapsed)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if messages[0].Data["delayed"] != true {
		t.Errorf("Expected data.delayed=true, got '%v'", messages[0].Data["delayed"])
	}

	t.Logf("Pop with wait working: %s (waited %v)", queueName, elapsed)
}

func TestPopWithAck(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("pop-ack")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push message
	payload := map[string]interface{}{"test": "ack"}
	_, err = client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	// Pop message
	messages, err := client.Queue(queueName).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	msg := messages[0]

	// Acknowledge message
	ackResponses, err := client.Ack(ctx, msg, true, queen.AckOptions{})
	if err != nil {
		t.Fatalf("Failed to ack message: %v", err)
	}

	if len(ackResponses) != 1 || !ackResponses[0].Success {
		t.Errorf("Expected successful ack, got %v", ackResponses)
	}

	// Try to pop again - should be empty
	messages, err = client.Queue(queueName).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop after ack: %v", err)
	}

	if len(messages) != 0 {
		t.Errorf("Expected 0 messages after ack, got %d", len(messages))
	}

	t.Logf("Pop with ack working: %s", queueName)
}

func TestPopBatch(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("pop-batch")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push multiple messages
	for i := 0; i < 5; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Pop batch
	messages, err := client.Queue(queueName).Batch(3).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("Failed to pop batch: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(messages))
	}

	t.Logf("Pop batch working: %s (got %d messages)", queueName, len(messages))
}
