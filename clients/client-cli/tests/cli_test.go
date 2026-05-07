// Package tests is the queenctl end-to-end suite, ported from the JS
// client-v2 (clients/client-js/test-v2/*) and the Python client tests
// (clients/client-py/tests/*) to exercise the same conceptual surface
// through the CLI.
//
// The suite is gated on the QUEEN_E2E env var so plain `go test ./...`
// stays fast. To run the full suite:
//
//	docker run -d --name qpg -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16
//	docker run -d --name queen --link qpg:qpg -p 6632:6632 \
//	  -e PG_HOST=qpg -e PG_PORT=5432 -e PG_PASSWORD=postgres \
//	  smartnessai/queen-mq:0.14.3
//	cd clients/client-cli && make build
//	QUEEN_E2E=1 \
//	  QUEEN_SERVER=http://localhost:6632 \
//	  PG_HOST=localhost PG_PORT=5433 PG_PASSWORD=postgres \
//	  go test -v ./tests/...
//
// Filter with -run as usual, e.g. `go test -v -run TestQueue ./tests/...`.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// gating + shared state
// ---------------------------------------------------------------------------

const (
	envE2E      = "QUEEN_E2E"
	envServer   = "QUEEN_SERVER"
	envPGHost   = "PG_HOST"
	envPGPort   = "PG_PORT"
	envPGUser   = "PG_USER"
	envPGPass   = "PG_PASSWORD"
	envPGDB     = "PG_DB"
	envQueuePfx = "QUEEN_TEST_QUEUE_PREFIX"
)

var (
	binPath    string
	serverURL  string
	queuePfx   string
	pg         *pgxpool.Pool
	createdQs  sync.Map // queueName -> struct{}{}
)

func TestMain(m *testing.M) {
	if os.Getenv(envE2E) != "1" {
		fmt.Fprintln(os.Stderr, "queenctl e2e: set QUEEN_E2E=1 to run; skipping.")
		// Skip without failing.
		os.Exit(0)
	}
	if err := setup(); err != nil {
		fmt.Fprintln(os.Stderr, "queenctl e2e setup failed:", err)
		os.Exit(2)
	}
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() error {
	serverURL = strings.TrimRight(getenv(envServer, "http://localhost:6632"), "/")

	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..")
	binPath = filepath.Join(root, "bin", "queenctl")
	if _, err := os.Stat(binPath); err != nil {
		return fmt.Errorf("binary not built (run `make build` first): %w", err)
	}

	queuePfx = getenv(envQueuePfx, fmt.Sprintf("ct-e2e-%d", time.Now().Unix()%100000))

	// Optional Postgres pool for DB-side assertions. The suite degrades
	// gracefully if PG isn't reachable: tests that need it call
	// requirePG(t).
	dsn := pgDSN()
	if dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err := p.Ping(ctx); err == nil {
				pg = p
			} else {
				p.Close()
			}
		}
	}
	return nil
}

func teardown() {
	// Best-effort delete every queue we created. Test cases also call
	// cleanupQueue at end-of-test, but a panic mid-test could leave
	// stragglers behind.
	createdQs.Range(func(k, _ any) bool {
		name := k.(string)
		_, _, _ = run("queue", "delete", name, "--yes")
		return true
	})
	if pg != nil {
		pg.Close()
	}
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func pgDSN() string {
	host := os.Getenv(envPGHost)
	if host == "" {
		return ""
	}
	port := getenv(envPGPort, "5432")
	user := getenv(envPGUser, "postgres")
	pw := getenv(envPGPass, "postgres")
	db := getenv(envPGDB, "postgres")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pw, host, port, db)
}

// requirePG fails the test if no Postgres pool is configured.
func requirePG(t *testing.T) {
	t.Helper()
	if pg == nil {
		t.Skip("PG_HOST not set; this test needs database access for assertions")
	}
}

// ---------------------------------------------------------------------------
// invocation helpers
// ---------------------------------------------------------------------------

// runOpts controls a single CLI invocation.
type runOpts struct {
	stdin   io.Reader
	timeout time.Duration
	env     []string
	// rawArgs disables the automatic `--server <global>` prepend. Tests that
	// drive their own httptest server (auth tests) set this so the global
	// broker URL doesn't override their fake.
	rawArgs bool
}

func run(args ...string) (stdout, stderr string, code int) {
	out, errOut, code := runWith(runOpts{}, args...)
	return out, errOut, code
}

func runWith(o runOpts, args ...string) (stdout, stderr string, code int) {
	full := args
	if !o.rawArgs {
		full = append([]string{"--server", serverURL}, args...)
	}
	if o.timeout == 0 {
		o.timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, full...)
	cmd.Env = append(os.Environ(), o.env...)
	if o.stdin != nil {
		cmd.Stdin = o.stdin
	}
	var so, se bytes.Buffer
	cmd.Stdout = &so
	cmd.Stderr = &se
	err := cmd.Run()
	code = 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		code = exitErr.ExitCode()
	} else if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		// Process never started or some other system error: surface it.
		se.WriteString("\n[exec error] " + err.Error())
		code = -1
	}
	return so.String(), se.String(), code
}

// runOK runs the command and asserts exit 0; returns stdout. Use for the
// happy-path invocations that dominate the suite.
func runOK(t *testing.T, args ...string) string {
	t.Helper()
	stdout, stderr, code := run(args...)
	if code != 0 {
		t.Fatalf("queenctl %s -> exit %d\n--- stdout ---\n%s\n--- stderr ---\n%s",
			strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout
}

// runJSON runs the command with `-o json` appended (if not already present)
// and unmarshals the result into v.
func runJSON(t *testing.T, v any, args ...string) {
	t.Helper()
	hasOut := false
	for i, a := range args {
		if a == "-o" || a == "--output" {
			hasOut = true
			break
		}
		if strings.HasPrefix(a, "-o=") || strings.HasPrefix(a, "--output=") {
			hasOut = true
			break
		}
		_ = i
	}
	if !hasOut {
		args = append(args, "-o", "json")
	}
	out := runOK(t, args...)
	if err := json.Unmarshal([]byte(out), v); err != nil {
		t.Fatalf("queenctl %s: decode JSON: %v\nbody: %s",
			strings.Join(args, " "), err, out)
	}
}

// stdinFromString builds a runOpts with an in-memory stdin reader from s.
func stdinFromString(s string) runOpts {
	return runOpts{stdin: strings.NewReader(s)}
}

// ---------------------------------------------------------------------------
// queue lifecycle helpers
// ---------------------------------------------------------------------------

// uniqueQueue returns a queue name that's safe to create from this test, and
// schedules it for cleanup once the test ends.
func uniqueQueue(t *testing.T, label string) string {
	t.Helper()
	// 't' identifies the calling test; we can't read its sub-name easily so we
	// trust the caller to pass a slug. Use ts-millis suffix to keep parallel
	// tests from colliding.
	name := fmt.Sprintf("%s-%s-%d", queuePfx, label, time.Now().UnixNano()%1_000_000)
	createdQs.Store(name, struct{}{})
	t.Cleanup(func() { cleanupQueue(name) })
	return name
}

// cleanupQueue deletes the queue if it exists. Best-effort; never fails the
// test.
func cleanupQueue(name string) {
	_, _, _ = run("queue", "delete", name, "--yes")
	createdQs.Delete(name)
}

// createQueue runs `queenctl queue configure <name> [flags...]` and asserts
// success.
func createQueue(t *testing.T, name string, extraArgs ...string) {
	t.Helper()
	args := append([]string{"queue", "configure", name}, extraArgs...)
	runOK(t, args...)
}

// ---------------------------------------------------------------------------
// pop / push helpers (CLI-shaped)
// ---------------------------------------------------------------------------

type cliMessage struct {
	TransactionID string         `json:"transactionId"`
	PartitionID   string         `json:"partitionId"`
	LeaseID       string         `json:"leaseId,omitempty"`
	Queue         string         `json:"queue"`
	Partition     string         `json:"partition"`
	Data          map[string]any `json:"data"`
	CreatedAt     string         `json:"createdAt"`
	RetryCount    int            `json:"retryCount"`
}

type pushResult struct {
	Status        string `json:"status"`
	TransactionID string `json:"transactionId"`
}

// pushOne pushes one JSON object to a queue/partition and returns the parsed
// summary line (`queued=N duplicate=N failed=N`).
func pushOne(t *testing.T, queue, partition string, payload any) string {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	args := []string{"push", queue}
	if partition != "" {
		args = append(args, "--partition", partition)
	}
	out, stderr, code := runWith(stdinFromString(string(body)+"\n"), args...)
	if code != 0 {
		t.Fatalf("push: exit %d\nstdout: %s\nstderr: %s", code, out, stderr)
	}
	return out
}

// pushNDJSON pushes the given payloads as a single batch.
func pushNDJSON(t *testing.T, queue, partition string, payloads []any, extra ...string) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, p := range payloads {
		if err := enc.Encode(p); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	args := []string{"push", queue}
	if partition != "" {
		args = append(args, "--partition", partition)
	}
	args = append(args, extra...)
	out, stderr, code := runWith(runOpts{stdin: &buf}, args...)
	if code != 0 {
		t.Fatalf("push %d items: exit %d\nstdout: %s\nstderr: %s", len(payloads), code, out, stderr)
	}
	return out
}

// popN runs `queenctl pop` with -n N and returns parsed messages. Pass extra
// CLI flags via extra.
func popN(t *testing.T, queue string, n int, extra ...string) []cliMessage {
	t.Helper()
	args := []string{"pop", queue, "-n", fmt.Sprintf("%d", n), "-o", "ndjson"}
	args = append(args, extra...)
	out, stderr, code := run(args...)
	// pop emits exit 4 (Empty) when no messages were available; treat as 0
	// rows.
	if code == 4 {
		return nil
	}
	if code != 0 {
		t.Fatalf("pop: exit %d\nstdout: %s\nstderr: %s", code, out, stderr)
	}
	return parseNDJSONMessages(t, out)
}

func parseNDJSONMessages(t *testing.T, blob string) []cliMessage {
	t.Helper()
	var out []cliMessage
	for _, line := range strings.Split(strings.TrimRight(blob, "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var m cliMessage
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode message line: %v\nline: %s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// ---------------------------------------------------------------------------
// admin / db helpers
// ---------------------------------------------------------------------------

// listAllMessages returns every message currently visible for the queue via
// the messages list endpoint, paginated to be safe.
func listAllMessages(t *testing.T, queue string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	runJSON(t, &rows, "messages", "list", "--queue", queue, "--limit", "10000")
	return rows
}

// queueDetail fetches `queenctl queue describe <q>` as a parsed map.
func queueDetail(t *testing.T, queue string) map[string]any {
	t.Helper()
	out := runOK(t, "queue", "describe", queue, "-o", "json")
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("decode queue detail: %v\nbody: %s", err, out)
	}
	return m
}

// pgRow runs a single-row query against the broker's database and decodes
// columns into dst (in order).
func pgRow(t *testing.T, sql string, args ...any) []any {
	t.Helper()
	requirePG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	row := pg.QueryRow(ctx, sql, args...)
	cols := []any{}
	// Drive: caller passes (sql, args...) and expects []any result. We use
	// pgx Row.Scan via reflection-free pattern: query for one row and let
	// the caller use len() against expected layout.
	// Instead: return Values() helper via QueryRow → pgx doesn't expose
	// that directly; use Query so we can call Values().
	rows, err := pg.Query(ctx, sql, args...)
	_ = row
	if err != nil {
		t.Fatalf("pg query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	vals, err := rows.Values()
	if err != nil {
		t.Fatalf("pg values: %v", err)
	}
	cols = vals
	return cols
}

// retry runs fn repeatedly until it returns nil or the timeout expires. Used
// to wait out the broker's async stats-refresh / partition-lookup commits.
func retry(t *testing.T, total time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(total)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := fn(); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("retry timed out after %s: %v", total, lastErr)
}

// rfc3339Now returns the current time in the broker's expected timestamp
// format.
func rfc3339Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// jsonDecode is a tiny wrapper that accepts a string and any pointer; used
// in places where we want a one-liner without importing encoding/json in
// each topic file.
func jsonDecode(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

// fmtErr is the error-shaped equivalent of fmt.Errorf used inside retry()
// closures.
func fmtErr(format string, a ...any) error {
	return fmt.Errorf(format, a...)
}

// tempConfig wraps an isolated ~/.queen/config.yaml-style file used by the
// auth tests so they don't pollute the developer's real config.
type tempConfig struct {
	path string
}

func newTempConfig(t *testing.T) *tempConfig {
	t.Helper()
	dir := t.TempDir()
	return &tempConfig{path: filepath.Join(dir, "config.yaml")}
}

func (c *tempConfig) cleanup(t *testing.T) { /* TempDir auto-cleans */ }

// setContext writes a context entry via the CLI itself so the test
// exercises both halves of the round-trip (write and load).
func (c *tempConfig) setContext(t *testing.T, name, server string) {
	t.Helper()
	args := []string{
		"--config", c.path,
		"config", "set-context", name,
		"--server", server,
		"--no-keychain", "--token", "placeholder",
		"-q",
	}
	out, stderr, code := runWith(runOpts{
		env:     []string{"QUEEN_CONFIG=" + c.path},
		rawArgs: true,
	}, args...)
	if code != 0 {
		t.Fatalf("set-context: exit %d\nstdout: %s\nstderr: %s", code, out, stderr)
	}
}
