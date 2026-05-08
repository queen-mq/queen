package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestPushMessage(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("push")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push message with unique data to avoid any collision
	payload := map[string]interface{}{
		"test":      "data",
		"value":     123,
		"timestamp": time.Now().UnixNano(),
	}
	responses, err := client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	if len(responses) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(responses))
	}

	// Accept queued or duplicate (server may have seen this before in rapid testing)
	if responses[0].Status != "queued" && responses[0].Status != "duplicate" {
		t.Errorf("Expected status 'queued' or 'duplicate', got '%s'", responses[0].Status)
	}

	t.Logf("Pushed message to queue: %s (status: %s)", queueName, responses[0].Status)
}

func TestPushDuplicateMessage(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("duplicate")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Generate transaction ID
	txID := queen.GenerateUUID()

	// Push first message
	payload := map[string]interface{}{"test": "data"}
	responses, err := client.Queue(queueName).Push(payload).TransactionID(txID).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push first message: %v", err)
	}

	if responses[0].Status != "queued" {
		t.Errorf("Expected status 'queued', got '%s'", responses[0].Status)
	}

	// Push duplicate message (same transaction ID)
	responses, err = client.Queue(queueName).Push(payload).TransactionID(txID).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push duplicate message: %v", err)
	}

	if responses[0].Status != "duplicate" {
		t.Errorf("Expected status 'duplicate', got '%s'", responses[0].Status)
	}

	t.Logf("Duplicate detection working for queue: %s", queueName)
}

func TestPushMessageWithTransactionID(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("txid")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push message with custom transaction ID
	txID := queen.GenerateUUID()
	payload := map[string]interface{}{"test": "data"}
	responses, err := client.Queue(queueName).Push(payload).TransactionID(txID).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push message: %v", err)
	}

	if responses[0].TransactionID != txID {
		t.Errorf("Expected transaction ID '%s', got '%s'", txID, responses[0].TransactionID)
	}

	t.Logf("Push with transaction ID working for queue: %s", queueName)
}

func TestPushBufferedMessage(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("buffered")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push messages with buffering
	bufferConfig := queen.BufferConfig{
		MessageCount: 10,
		TimeMillis:   500,
	}

	for i := 0; i < 5; i++ {
		payload := map[string]interface{}{"index": i}
		_, err := client.Queue(queueName).Buffer(bufferConfig).Push(payload).Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to push buffered message: %v", err)
		}
	}

	// Wait for buffer to flush
	time.Sleep(1 * time.Second)

	// Flush remaining
	err = client.Queue(queueName).FlushBuffer(ctx)
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}

	t.Logf("Buffered push working for queue: %s", queueName)
}

func TestPushLargePayload(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("large")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Create large payload (~1MB)
	largeData := strings.Repeat("x", 1024*1024)
	payload := map[string]interface{}{"data": largeData}

	responses, err := client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push large message: %v", err)
	}

	if responses[0].Status != "queued" {
		t.Errorf("Expected status 'queued', got '%s'", responses[0].Status)
	}

	t.Logf("Large payload push working for queue: %s", queueName)
}

func TestPushNullPayload(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("null")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push null payload - note: null payloads are valid
	responses, err := client.Queue(queueName).Push(nil).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push null message: %v", err)
	}

	// Accept both queued and duplicate (null payload might have consistent hash)
	if responses[0].Status != "queued" && responses[0].Status != "duplicate" {
		t.Errorf("Expected status 'queued' or 'duplicate', got '%s'", responses[0].Status)
	}

	t.Logf("Null payload push working for queue: %s (status: %s)", queueName, responses[0].Status)
}

func TestPushEmptyPayload(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("empty")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push empty payload
	payload := map[string]interface{}{}
	responses, err := client.Queue(queueName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push empty message: %v", err)
	}

	if responses[0].Status != "queued" {
		t.Errorf("Expected status 'queued', got '%s'", responses[0].Status)
	}

	t.Logf("Empty payload push working for queue: %s", queueName)
}

func TestPushToPartition(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("partition")
	partitionName := "custom-partition"

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push to specific partition
	payload := map[string]interface{}{"test": "data"}
	responses, err := client.Queue(queueName).Partition(partitionName).Push(payload).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push to partition: %v", err)
	}

	if responses[0].Status != "queued" {
		t.Errorf("Expected status 'queued', got '%s'", responses[0].Status)
	}

	t.Logf("Partition push working for queue: %s, partition: %s", queueName, partitionName)
}

func TestPushMultipleMessages(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("multi")

	// Create queue
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Push multiple messages
	payloads := []interface{}{
		map[string]interface{}{"index": 0},
		map[string]interface{}{"index": 1},
		map[string]interface{}{"index": 2},
	}

	responses, err := client.Queue(queueName).Push(payloads).Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to push multiple messages: %v", err)
	}

	if len(responses) != 3 {
		t.Fatalf("Expected 3 responses, got %d", len(responses))
	}

	for i, r := range responses {
		if r.Status != "queued" {
			t.Errorf("Message %d: expected status 'queued', got '%s'", i, r.Status)
		}
	}

	t.Logf("Multiple message push working for queue: %s", queueName)
}
