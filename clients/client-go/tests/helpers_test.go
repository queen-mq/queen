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
//
// Mirrors the Python (tests/conftest.py) and JS (test-v2/run.js) suites. The
// tables live in the `queen` schema, which is NOT on the default search_path,
// so every statement is schema-qualified. Queue identity is now the
// queen.queues id (log_queues was merged away): log_partitions,
// consumer_watermarks, consumer_groups_metadata and queue_lag_metrics all
// cascade from the queues row. Only log_txns/log_dlq have no FK by design,
// so they get an explicit purge keyed via log_partitions first.
func cleanupTestData(ctx context.Context) error {
	if dbPool == nil {
		return nil
	}

	// NB: this package runs CONCURRENTLY with ./tests/streams_integration
	// (the runner entrypoint passes both packages to one `go test` call, which
	// executes the two binaries in parallel). Its queues are named
	// `test-stream-...`, so the patterns here must NOT cover `test-%` broadly
	// or this wipe deletes the streams suite's in-flight queues (observed as
	// TestGateTokenBucketBasic "expected 60 drained, got 20"). Match only the
	// prefixes THIS package creates: test-go-*, test-auth-go-*,
	// test-ackwindow-* (plus the legacy edge/pattern/workflow leftovers).
	// The three exact names are the documentation queues (docs_test.go).
	patterns := []string{"test-go-%", "test-auth-go-%", "test-ackwindow-%", "edge-%", "pattern-%", "workflow-%", "orders", "payments", "invoices"}

	// FK-less log tables first, best-effort: a rows-only server without the
	// log schema errors here, so we ignore it and fall through to the queues
	// delete below rather than failing the whole run.
	logTxnsPurge := `WITH parts AS (
		SELECT lp.id FROM queen.log_partitions lp
		JOIN queen.queues q ON q.id = lp.queue_id
		WHERE q.name LIKE ANY($1::text[])
	),
	d1 AS (DELETE FROM queen.log_txns WHERE partition_id IN (SELECT id FROM parts)),
	d2 AS (DELETE FROM queen.log_dlq  WHERE partition_id IN (SELECT id FROM parts))
	SELECT 1`
	_, _ = dbPool.Exec(ctx, logTxnsPurge, patterns) // err: log schema not installed (rows-only server)

	// KV and timers (PLAN_KV_TIMERS.md §10.4). Neither table hangs off
	// queen.queues -- queen.kv is keyed by (tenant, namespace, key) and
	// queen.log_timers carries the queue as TEXT -- so neither is reached by the
	// cascade below and both need their own purge.
	//
	// This is not cosmetic. Without it a putIfAbsent test is green on the first
	// run and red forever after (the marker survives and the second run loses
	// the race it was supposed to win), an incr accumulates between runs and
	// meets its ceiling early, and a pending timer outlives its test to fire
	// into a queue that no longer belongs to anybody.
	//
	// Each in its own statement and each best-effort, like the purge above: on a
	// broker built before this feature the two tables simply do not exist, and
	// that must not fail the whole suite.
	_, _ = dbPool.Exec(ctx, `DELETE FROM queen.kv WHERE namespace LIKE 'test-go-kv-%'`) // err: no kv schema
	_, _ = dbPool.Exec(ctx, `DELETE FROM queen.log_timers WHERE queue LIKE ANY($1::text[])`, patterns)

	// Deleting the queues rows cascades to everything else
	// (partitions/watermarks/metadata/lag-metrics).
	if _, err := dbPool.Exec(ctx, `DELETE FROM queen.queues WHERE name LIKE ANY($1::text[])`, patterns); err != nil {
		return err
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
