package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

var (
	testClient *queen.Queen
	serverURL  string
)

// TestMain creates the shared client, runs the suite and closes the client.
//
// There is no cleanup step. The harness (test/run.sh) gives every lane a fresh
// broker, its raft data volume created empty and destroyed after the lane, so
// the suite can assume an empty broker. Every assertion goes through the
// public HTTP API.
func TestMain(m *testing.M) {
	// Get server URL from environment or use default
	serverURL = os.Getenv("QUEEN_SERVER_URL")
	if serverURL == "" {
		serverURL = "http://localhost:6632"
	}

	// Create client
	var err error
	testClient, err = queen.New(serverURL)
	if err != nil {
		fmt.Printf("Failed to create Queen client: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	testClient.Close(context.Background())
	os.Exit(code)
}

// generateQueueName generates a unique queue name for testing.
func generateQueueName(prefix string) string {
	return fmt.Sprintf("test-go-%s-%d", prefix, time.Now().UnixNano())
}

// requireClient ensures the test client is available.
func requireClient(t *testing.T) *queen.Queen {
	if testClient == nil {
		t.Skip("Queen client not available")
	}
	return testClient
}

// waitForMessages waits for messages to be available.
func waitForMessages(ctx context.Context, client *queen.Queen, queueName string, count int, timeout time.Duration) ([]*queen.Message, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := client.Queue(queueName).Batch(count).Pop(ctx)
		if err != nil {
			return nil, err
		}
		if len(msgs) >= count {
			return msgs, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("timeout waiting for %d messages", count)
}
