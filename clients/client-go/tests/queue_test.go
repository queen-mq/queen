package tests

import (
	"context"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestCreateQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("create")

	// Create queue
	result, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Verify result
	if configured, ok := result["configured"].(bool); !ok || !configured {
		t.Errorf("Expected configured=true, got %v", result["configured"])
	}

	t.Logf("Created queue: %s", queueName)
}

func TestDeleteQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("delete")

	// Create queue first
	_, err := client.Queue(queueName).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Delete queue
	_, err = client.Queue(queueName).Delete().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to delete queue: %v", err)
	}

	t.Logf("Deleted queue: %s", queueName)
}

func TestConfigureQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := generateQueueName("configure")

	// Create queue with configuration
	config := queen.QueueConfig{
		LeaseTime:         60,
		RetryLimit:        5,
		Priority:          10,
		MaxSize:           1000,
		EncryptionEnabled: false,
	}

	result, err := client.Queue(queueName).Config(config).Create().Execute(ctx)
	if err != nil {
		t.Fatalf("Failed to create queue with config: %v", err)
	}

	if configured, ok := result["configured"].(bool); !ok || !configured {
		t.Errorf("Expected configured=true, got %v", result["configured"])
	}

	t.Logf("Created queue with config: %s", queueName)
}
