package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// What `queenctl queue configure` puts on the wire
// (PLAN_DASHBOARD_ACTIONS.md §2.2).
//
// `/configure` MERGES since 1.6.0: an option the body does not carry keeps the
// value the queue has. That turns every key this command sends without being
// asked into a silent edit of somebody else's setting — and `--dlq` defaults to
// TRUE, so `queenctl queue configure orders --lease-time 60` used to re-enable
// dead-lettering on a queue deliberately configured to drop. The rule this file
// pins is one sentence: ONLY THE FLAGS THAT WERE TYPED.

// runQueenctl executes the root command with args, against a broker URL, and
// returns the error. The global flags are cobra state, so they are restored
// afterwards and no test here runs in parallel.
func runQueenctl(t *testing.T, server string, args ...string) error {
	t.Helper()
	saved := gf
	t.Cleanup(func() { gf = saved })

	// An empty config file, so the developer's own ~/.queen/config.yaml (and its
	// current-context) cannot decide what this test talks to.
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte("contexts: []\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("QUEEN_SERVER", "")
	t.Setenv("QUEEN_TOKEN", "")

	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	rootCmd.SetArgs(append(args, "--config", cfg, "--server", server, "--quiet"))
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	return rootCmd.ExecuteContext(context.Background())
}

// captureConfigure answers every POST /api/v1/configure with a plausible echo
// and records the bodies it was sent.
func captureConfigure(t *testing.T) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/configure" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("configure body is not JSON: %v", err)
			}
			bodies = append(bodies, body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"configured":true,"queue":"orders","options":{}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &bodies
}

func onlyOptions(t *testing.T, bodies *[]map[string]any) map[string]any {
	t.Helper()
	if len(*bodies) != 1 {
		t.Fatalf("expected exactly one configure call, got %d: %+v", len(*bodies), *bodies)
	}
	opts, _ := (*bodies)[0]["options"].(map[string]any)
	if opts == nil {
		opts = map[string]any{}
	}
	return opts
}

func TestQueueConfigureSendsOnlyTheFlagsThatWereTyped(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := runQueenctl(t, srv.URL, "queue", "configure", "orders", "--lease-time", "60"); err != nil {
		t.Fatalf("configure: %v", err)
	}

	opts := onlyOptions(t, bodies)
	if got, want := opts["leaseTime"], float64(60); got != want {
		t.Fatalf("leaseTime is %v, want %v (options: %+v)", got, want, opts)
	}
	// The two that used to ride along on every call. Under merge they are not
	// "the default": they are an edit of whatever the queue holds.
	for _, key := range []string{"deadLetterQueue", "dlqAfterMaxRetries", "encryptionEnabled"} {
		if _, present := opts[key]; present {
			t.Errorf("%s was sent by a --lease-time call: %+v", key, opts)
		}
	}
	if len(opts) != 1 {
		t.Fatalf("a one-flag configure sent %d options: %+v", len(opts), opts)
	}
	// No `mode`: merge is the broker's default, and this command is the merging
	// one. `queenctl apply` is where `replace` is spelled.
	if _, present := (*bodies)[0]["mode"]; present {
		t.Errorf("queue configure must not send a mode: %+v", (*bodies)[0])
	}
}

// The inverse, and the reason the flags go through Option(): a false is a Go
// zero value, which the SDK's QueueConfig omits. Without an explicit false on
// the wire, --dlq=false could not undo what --dlq sets.
func TestQueueConfigureCanTurnDeadLetteringOff(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := runQueenctl(t, srv.URL, "queue", "configure", "orders",
		"--dlq=false", "--encrypt=false"); err != nil {
		t.Fatalf("configure: %v", err)
	}

	opts := onlyOptions(t, bodies)
	for _, key := range []string{"deadLetterQueue", "dlqAfterMaxRetries", "encryptionEnabled"} {
		v, present := opts[key]
		if !present {
			t.Errorf("%s was not sent at all, so the queue keeps it ON: %+v", key, opts)
			continue
		}
		if v != false {
			t.Errorf("%s is %v, want false", key, v)
		}
	}
}

func TestQueueConfigureSendsTheDlqFlagWhenItIsTypedTrue(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := runQueenctl(t, srv.URL, "queue", "configure", "orders", "--dlq"); err != nil {
		t.Fatalf("configure: %v", err)
	}
	opts := onlyOptions(t, bodies)
	if opts["deadLetterQueue"] != true || opts["dlqAfterMaxRetries"] != true {
		t.Fatalf("--dlq must switch dead-lettering on: %+v", opts)
	}
}
