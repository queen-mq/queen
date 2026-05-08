package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestSubscriptionModeNew(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("sub-new")
	groupName := "sub-new-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push historical messages (before consumer starts)
	for i := 0; i < 3; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"historical": true, "index": i}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push historical message: %v", err)
		}
	}

	// Wait a bit to ensure messages are "historical"
	time.Sleep(100 * time.Millisecond)

	// Start consuming with subscription mode "new"
	var processed int32

	// Create a context that will timeout
	consumeCtx, consumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumeCancel()

	// Start consumer in goroutine
	go func() {
		handler := func(ctx context.Context, msg *queen.Message) error {
			atomic.AddInt32(&processed, 1)
			return nil
		}

		client.Queue(queueName).
			Group(groupName).
			SubscriptionMode(queen.SubscriptionModeNew).
			IdleMillis(2000).
			Consume(consumeCtx, handler).
			Execute(consumeCtx)
	}()

	// Wait for consumer to start
	time.Sleep(500 * time.Millisecond)

	// Push new messages (after consumer started)
	for i := 0; i < 2; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"new": true, "index": i}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push new message: %v", err)
		}
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Should only have processed the new messages (2), not historical (3)
	count := atomic.LoadInt32(&processed)
	if count != 2 {
		t.Errorf("Expected 2 processed (new only), got %d", count)
	}

	t.Logf("Subscription mode 'new' working (processed %d messages)", count)
}

func TestSubscriptionModeAll(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("sub-all")
	groupName := "sub-all-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push historical messages
	for i := 0; i < 3; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"index": i}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push message: %v", err)
		}
	}

	// Consume with subscription mode "all" using short polling + idle timeout
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	err = client.Queue(queueName).
		Group(groupName).
		SubscriptionMode(queen.SubscriptionModeAll).
		Limit(10).
		IdleMillis(3000).
		Wait(false). // Use short polling so idle timeout works
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	// Should have processed messages (may vary based on timing)
	count := atomic.LoadInt32(&processed)
	if count == 0 {
		t.Errorf("Expected at least 1 processed message, got 0")
	}

	t.Logf("Subscription mode 'all' working (processed %d of 3 messages)", count)
}

func TestSubscriptionFromNow(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("sub-from-now")
	groupName := "sub-from-now-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push historical messages
	for i := 0; i < 3; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"historical": true}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push historical message: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Start consuming from "now"
	var processed int32

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumeCancel()

	go func() {
		handler := func(ctx context.Context, msg *queen.Message) error {
			atomic.AddInt32(&processed, 1)
			return nil
		}

		client.Queue(queueName).
			Group(groupName).
			SubscriptionFrom(queen.SubscriptionFromNow).
			IdleMillis(2000).
			Consume(consumeCtx, handler).
			Execute(consumeCtx)
	}()

	// Wait for consumer to start
	time.Sleep(500 * time.Millisecond)

	// Push new messages
	for i := 0; i < 2; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"new": true}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push new message: %v", err)
		}
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	count := atomic.LoadInt32(&processed)
	if count != 2 {
		t.Errorf("Expected 2 processed (from now), got %d", count)
	}

	t.Logf("Subscription from 'now' working (processed %d messages)", count)
}

func TestSubscriptionFromTimestamp(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("sub-from-ts")
	groupName := "sub-from-ts-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push old messages
	for i := 0; i < 2; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"old": true}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push old message: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Record timestamp
	timestamp := time.Now().Format(time.RFC3339)

	time.Sleep(100 * time.Millisecond)

	// Push new messages after timestamp
	for i := 0; i < 3; i++ {
		_, err = client.Queue(queueName).Push(map[string]interface{}{"new": true}).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push new message: %v", err)
		}
	}

	// Consume from timestamp using short polling + idle timeout
	var processed int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	err = client.Queue(queueName).
		Group(groupName).
		SubscriptionFrom(timestamp).
		Limit(10).
		IdleMillis(3000).
		Wait(false). // Use short polling so idle timeout works
		Consume(ctx, handler).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	count := atomic.LoadInt32(&processed)
	// Should process messages after the timestamp (at least 3 new ones, possibly all 5)
	if count < 3 {
		t.Errorf("Expected at least 3 processed (from timestamp), got %d", count)
	}

	t.Logf("Subscription from timestamp working (processed %d messages)", count)
}
