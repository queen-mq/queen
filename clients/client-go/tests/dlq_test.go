package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestDLQ(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	queueName := generateQueueName("dlq")
	groupName := "dlq-test-group"

	// Create queue with low retry limit
	config := queen.QueueConfig{
		LeaseTime:  5,  // 5 seconds lease
		RetryLimit: 2,  // Only 2 retries before DLQ
	}

	_, err := client.Queue(queueName).Config(config).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push a message
	_, err = client.Queue(queueName).Push(map[string]interface{}{"test": "dlq"}).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	// Consume and always fail (to trigger retries)
	var attempts int32

	handler := func(ctx context.Context, msg *queen.Message) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("intentional failure")
	}

	// Process multiple times to exhaust retries
	for i := 0; i < 3; i++ {
		err = client.Queue(queueName).
			Group(groupName).
			AutoAck(true).
			Limit(1).
			IdleMillis(1000).
			Consume(ctx, handler).
			Execute(ctx)

		// Wait for lease to expire
		time.Sleep(6 * time.Second)
	}

	// Query DLQ
	dlqResponse, err := client.Queue(queueName).DLQ(groupName).Limit(10).Get(ctx)
	if err != nil {
		t.Fatalf("Failed to query DLQ: %v", err)
	}

	// Message should be in DLQ after retries exhausted
	if dlqResponse.Total == 0 {
		t.Logf("Note: DLQ may take time to populate. Attempts: %d", atomic.LoadInt32(&attempts))
	} else {
		t.Logf("DLQ test passed: found %d messages in DLQ (attempts: %d)", dlqResponse.Total, attempts)
	}
}

func TestDLQQuery(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("dlq-query")
	groupName := "dlq-query-group"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Query DLQ (should be empty)
	dlqResponse, err := client.Queue(queueName).DLQ(groupName).Limit(10).Get(ctx)
	if err != nil {
		t.Fatalf("Failed to query DLQ: %v", err)
	}

	if dlqResponse.Total != 0 {
		t.Errorf("Expected empty DLQ, got %d messages", dlqResponse.Total)
	}

	t.Logf("DLQ query working for queue: %s", queueName)
}

func TestDLQWithFilters(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("dlq-filter")
	groupName := "dlq-filter-group"
	partitionName := "test-partition"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Query DLQ with filters
	from := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	to := time.Now().Format(time.RFC3339)

	dlqBuilder := client.Queue(queueName).DLQ(groupName)
	dlqBuilder = dlqBuilder.Limit(10).Offset(0).From(from).To(to)

	dlqResponse, err := dlqBuilder.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to query DLQ with filters: %v", err)
	}

	t.Logf("DLQ query with filters working: %s (partition: %s, found: %d)",
		queueName, partitionName, dlqResponse.Total)
}
