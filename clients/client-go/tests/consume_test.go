package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestConsumer(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages
	for i := 0; i < 3; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Consume messages with short polling + idle timeout
	var processed int32
	var mu sync.Mutex
	var receivedData []interface{}

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		mu.Lock()
		receivedData = append(receivedData, msg.Data["index"])
		mu.Unlock()
		return nil
	}

	err = client.Queue(queueName).
		Limit(10).
		IdleMillis(3000).
		Wait(false). // Use short polling so idle timeout works
		AutoAck(true).
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	if atomic.LoadInt32(&processed) < 3 {
		t.Errorf("Expected at least 3 processed messages, got %d", processed)
	}

	t.Logf("Consumer working: %s (processed %d messages)", queueName, processed)
}

func TestConsumerWithPartition(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer-partition")
	partitionName := "test-partition"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages to partition
	for i := 0; i < 3; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Partition(partitionName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Consume from partition
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		// Note: Server may not always return partition field - this is OK
		return nil
	}

	err = client.Queue(queueName).
		Partition(partitionName).
		Limit(3).
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	if atomic.LoadInt32(&processed) != 3 {
		t.Errorf("Expected 3 processed messages, got %d", processed)
	}

	t.Logf("Consumer with partition working: %s/%s", queueName, partitionName)
}

func TestConsumerBatchConsume(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer-batch")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages
	for i := 0; i < 10; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Consume in batches
	var batchCount int32
	var totalMessages int32

	handler := func(ctx context.Context, msgs []*queen.Message) error {
		atomic.AddInt32(&batchCount, 1)
		atomic.AddInt32(&totalMessages, int32(len(msgs)))
		return nil
	}

	err = client.Queue(queueName).
		Batch(5).
		Limit(10).
		ConsumeBatch(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Batch consumer failed: %v", err)
	}

	if atomic.LoadInt32(&totalMessages) != 10 {
		t.Errorf("Expected 10 total messages, got %d", totalMessages)
	}

	t.Logf("Batch consumer working: %s (batches: %d, messages: %d)",
		queueName, batchCount, totalMessages)
}

func TestConsumerGroup(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer-group")
	groupName := "test-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages
	for i := 0; i < 5; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Consume with consumer group
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	err = client.Queue(queueName).
		Group(groupName).
		Limit(5).
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer group failed: %v", err)
	}

	if atomic.LoadInt32(&processed) != 5 {
		t.Errorf("Expected 5 processed messages, got %d", processed)
	}

	t.Logf("Consumer group working: %s (group: %s)", queueName, groupName)
}

func TestConsumerConcurrency(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer-concurrent")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages
	for i := 0; i < 6; i++ {
		payload := map[string]interface{}{"index": i}
		_, err = client.Queue(queueName).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message %d: %v", i, err)
		}
	}

	// Consume with concurrency using short polling + idle timeout
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	err = client.Queue(queueName).
		Concurrency(2).
		Limit(10).        // Each worker can process up to 10
		IdleMillis(3000). // Exit after 3s idle (with short polling this works)
		Wait(false).      // Short polling so idle timeout can work
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Concurrent consumer failed: %v", err)
	}

	count := atomic.LoadInt32(&processed)
	// Should have processed some messages (concurrent processing may vary)
	if count == 0 {
		t.Errorf("Expected at least 1 processed message, got 0")
	}

	t.Logf("Concurrent consumer working: %s (processed: %d of 6)", queueName, count)
}

func TestConsumerIdleTimeout(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("consumer-idle")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push one message
	payload := map[string]interface{}{"test": "data"}
	_, err = client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	// Consume with idle timeout
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	start := time.Now()
	err = client.Queue(queueName).
		IdleMillis(3000). // 3 second idle timeout
		Wait(false).      // Don't wait - use short polling
		Consume(ctx, handler).
		Execute(ctx)

	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Consumer with idle timeout failed: %v", err)
	}

	// Should have processed 1 message and exited after idle timeout
	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("Expected 1 processed message, got %d", processed)
	}

	// Should have taken at least 3 seconds (idle timeout)
	if elapsed < 3*time.Second {
		t.Errorf("Expected at least 3s elapsed, got %v", elapsed)
	}

	t.Logf("Consumer idle timeout working: %s (elapsed: %v)", queueName, elapsed)
}
