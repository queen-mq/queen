package cmd

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	clierr "github.com/smartpricing/queen/clients/client-cli/v2/internal/errors"
)

func TestTailSupervision(t *testing.T) {
	for _, mode := range []string{"disabled", "enabled", "denied"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var reports []map[string]any
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != "/api/v1/kv" {
					_, _ = io.Copy(io.Discard, r.Body)
					_, _ = io.WriteString(w, popBody("orders", ""))
					return
				}
				mu.Lock()
				defer mu.Unlock()
				requests++
				if r.Header.Get("Authorization") != "Bearer supervision-test" {
					t.Error("status request did not use the configured token")
				}
				var body struct {
					Operations []struct {
						Op, Ns, Key string
						TTLSeconds  int
						Value       map[string]any
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Operations) != 2 {
					t.Error("expected atomic head/chunk publication")
					w.WriteHeader(400)
					return
				}
				for _, op := range body.Operations {
					if op.Op != "put" || op.Ns != "queen-supervisor" || op.TTLSeconds != 60 || !strings.HasPrefix(op.Key, "cli-production/") {
						t.Errorf("unexpected status operation: %+v", op)
					}
				}
				encoded, _ := body.Operations[1].Value["data"].(string)
				data, err := base64.StdEncoding.DecodeString(encoded)
				var doc map[string]any
				if err != nil || json.Unmarshal(data, &doc) != nil {
					t.Error("invalid consumer document")
					w.WriteHeader(400)
					return
				}
				reports = append(reports, doc)
				if mode == "denied" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, `{"error":"denied"}`)
					return
				}
				_, _ = io.WriteString(w, `{"results":[{"applied":true},{"applied":true}]}`)
			}))
			defer srv.Close()
			stop := captureStdio(t)
			args := []string{"--token", "supervision-test", "tail", "orders", "--limit", "1"}
			if mode != "disabled" {
				args = append(args, "--supervision-group", "cli-production")
			}
			err := runCLI(t, srv.URL, args...)
			out, _ := stop()
			if err != nil {
				t.Fatal(err)
			}
			var message map[string]any
			if strings.Count(out, "\n") != 1 || json.Unmarshal([]byte(out), &message) != nil || message["queue"] != "orders" {
				t.Fatalf("stdout must contain exactly one message: %q", out)
			}
			mu.Lock()
			defer mu.Unlock()
			if mode == "disabled" {
				if requests != 0 {
					t.Fatal("default tail must not publish status")
				}
				return
			}
			if len(reports) == 0 {
				t.Fatal("missing consumer status")
			}
			last := reports[len(reports)-1]
			if last["schema"] != "queen.consumer.status/v1" || last["engine"] != "go" || last["state"] != "stopped" {
				t.Fatalf("unexpected final document: %+v", last)
			}
			pool := last["pool_status"].([]any)[0].(map[string]any)
			if pool["running"] != float64(0) || pool["busy"] != float64(0) || pool["completed"] != float64(1) || pool["consumer_group"] != "queenctl-tail" {
				t.Fatalf("unexpected final counts: %+v", pool)
			}
		})
	}
}

func TestTailRejectsInvalidSupervisionGroup(t *testing.T) {
	for _, group := range []string{"", "coordination", "../other", "trailing\n"} {
		t.Run(group, func(t *testing.T) {
			err := runCLI(t, "http://127.0.0.1:1", "tail", "orders", "--supervision-group", group)
			if err == nil || clierr.CodeOf(err) != clierr.CodeUser {
				t.Fatalf("invalid group must fail before network access: %v", err)
			}
		})
	}
}
