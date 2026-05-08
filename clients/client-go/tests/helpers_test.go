package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testClient *queen.Queen
	dbPool     *pgxpool.Pool
	serverURL  string
)

// TestMain sets up the test environment.
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

	// Connect to database for cleanup (optional)
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		// Build from components
		host := getEnvOrDefault("PG_HOST", "localhost")
		port := getEnvOrDefault("PG_PORT", "5432")
		db := getEnvOrDefault("PG_DB", "queen")
		user := getEnvOrDefault("PG_USER", "postgres")
		password := getEnvOrDefault("PG_PASSWORD", "postgres")
		dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, db)
	}

	ctx := context.Background()
	dbPool, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Printf("Warning: Failed to connect to database: %v\n", err)
		// Continue without DB - some tests may fail
	} else {
		// Cleanup test data
		if err := cleanupTestData(ctx); err != nil {
			fmt.Printf("Warning: Failed to cleanup test data: %v\n", err)
		}
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if testClient != nil {
		testClient.Close(ctx)
	}
	if dbPool != nil {
		dbPool.Close()
	}

	os.Exit(code)
}

// cleanupTestData removes test data from the database.
func cleanupTestData(ctx context.Context) error {
	if dbPool == nil {
		return nil
	}

	patterns := []string{"test-%", "edge-%", "pattern-%", "workflow-%"}
	for _, pattern := range patterns {
		_, err := dbPool.Exec(ctx, "DELETE FROM partitions WHERE queue_name LIKE $1", pattern)
		if err != nil {
			return err
		}
		_, err = dbPool.Exec(ctx, "DELETE FROM queues WHERE name LIKE $1", pattern)
		if err != nil {
			return err
		}
	}

	return nil
}

// getEnvOrDefault returns an environment variable or a default value.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
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
