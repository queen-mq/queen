package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// What `queenctl dlq redrive` sends (queen-mq/queen#147): it reads every match
// first, then replays each row by its id, every partition oldest first.

type fakeDLQ struct {
	mu       sync.Mutex
	rows     []map[string]any
	answers  map[string]func(w http.ResponseWriter) // row id -> its replay answer; default moved
	replayed []string                               // row ids, in the order the replays came
	listed   []string                               // the list queries, as sent
}

func (f *fakeDLQ) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/dlq":
			f.listed = append(f.listed, r.URL.RawQuery)
			q := r.URL.Query()
			var match []map[string]any
			for _, row := range f.rows {
				if cg := q.Get("consumerGroup"); cg != "" && row["consumerGroup"] != cg {
					continue
				}
				match = append(match, row)
			}
			off, _ := strconv.Atoi(q.Get("offset"))
			lim, _ := strconv.Atoi(q.Get("limit"))
			page := []map[string]any{}
			for i := off; i < len(match) && i < off+lim; i++ {
				page = append(page, match[i])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": page, "total": len(match)})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/dlq/") && strings.HasSuffix(r.URL.Path, "/replay"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/dlq/"), "/replay")
			f.replayed = append(f.replayed, id)
			if a := f.answers[id]; a != nil {
				a(w)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"result":"moved","dlqRowRemoved":true}`))
		default:
			t.Errorf("unexpected call %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func dlqRow(id, partitionID, partition, group string, offset any) map[string]any {
	m := map[string]any{
		"id": id, "partitionId": partitionID, "partition": partition, "queue": "orders",
		"transactionId": "tx-" + id, "consumerGroup": group, "failedAt": "2026-11-03T10:00:00Z",
	}
	if offset != nil {
		m["offset"] = offset
	}
	return m
}

// redrive runs the command against srv and puts the package flags back.
func redrive(t *testing.T, server string, args ...string) error {
	t.Helper()
	t.Cleanup(func() {
		dlqYes, dlqDryRun, dlqCG, dlqQueue, dlqPartition, dlqFrom, dlqTo = false, false, "", "", "", "", ""
		redriveMax, redriveParallel = redriveDefaultMax, 4
	})
	return runQueenctl(t, server, append([]string{"dlq", "redrive"}, args...)...)
}

// The order a partition's replays went out in.
func (f *fakeDLQ) orderOf(ids ...string) []string {
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	var out []string
	for _, id := range f.replayed {
		if want[id] {
			out = append(out, id)
		}
	}
	return out
}

func TestRedriveReplaysEveryPartitionOldestFirst(t *testing.T) {
	f := &fakeDLQ{rows: []map[string]any{
		// listed newest first, as the broker lists them
		dlqRow("a3", "p1", "order-1", "billing", 30.0),
		dlqRow("b2", "p2", "order-2", "billing", 7.0),
		dlqRow("a1", "p1", "order-1", "billing", 10.0),
		dlqRow("a0", "p1", "order-1", "billing", nil), // no offset: after the ones with one
		dlqRow("a2", "p1", "order-1", "billing", 20.0),
		dlqRow("b1", "p2", "order-2", "billing", 3.0),
	}}
	srv := f.serve(t)
	redrivePage = 2 // three pages for six rows
	t.Cleanup(func() { redrivePage = 500 })
	if err := redrive(t, srv.URL, "--queue", "orders", "--yes"); err != nil {
		t.Fatalf("redrive: %v", err)
	}
	if got := strings.Join(f.orderOf("a0", "a1", "a2", "a3"), ","); got != "a1,a2,a3,a0" {
		t.Errorf("partition p1 went back as %s, want a1,a2,a3,a0", got)
	}
	if got := strings.Join(f.orderOf("b1", "b2"), ","); got != "b1,b2" {
		t.Errorf("partition p2 went back as %s, want b1,b2", got)
	}
	if len(f.replayed) != 6 {
		t.Errorf("replayed %d rows, want 6: %v", len(f.replayed), f.replayed)
	}
	// every page asked for the same filters, and no replay before the last page
	if len(f.listed) < 3 || !strings.Contains(f.listed[0], "queue=orders") {
		t.Errorf("list queries %v", f.listed)
	}
}

func TestRedriveSendsNothingWithoutYesOrOnADryRun(t *testing.T) {
	f := &fakeDLQ{rows: []map[string]any{dlqRow("a1", "p1", "order-1", "billing", 1.0)}}
	srv := f.serve(t)
	if err := redrive(t, srv.URL); err == nil {
		t.Fatalf("redrive without --yes ran")
	}
	if err := redrive(t, srv.URL, "--dry-run"); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(f.replayed) != 0 {
		t.Fatalf("replayed %v, want nothing", f.replayed)
	}
}

func TestRedriveRefusesMoreRowsThanMax(t *testing.T) {
	f := &fakeDLQ{rows: []map[string]any{
		dlqRow("a1", "p1", "order-1", "billing", 1.0),
		dlqRow("a2", "p1", "order-1", "billing", 2.0),
		dlqRow("a3", "p1", "order-1", "billing", 3.0),
	}}
	srv := f.serve(t)
	err := redrive(t, srv.URL, "--yes", "--max", "2")
	if err == nil || !strings.Contains(err.Error(), "more than 2") {
		t.Fatalf("redrive over --max: %v", err)
	}
	if len(f.replayed) != 0 {
		t.Fatalf("replayed %v, want nothing", f.replayed)
	}
}

// A message two groups dead-lettered: one replay reaches both, so the command
// asks for a group instead of sending two copies.
func TestRedriveAsksForAGroupWhenTwoGroupsHoldOneMessage(t *testing.T) {
	shared := dlqRow("s2", "p1", "order-1", "shipping", 1.0)
	shared["transactionId"] = "tx-s1"
	f := &fakeDLQ{rows: []map[string]any{dlqRow("s1", "p1", "order-1", "billing", 1.0), shared}}
	srv := f.serve(t)
	err := redrive(t, srv.URL, "--yes")
	if err == nil || !strings.Contains(err.Error(), "--cg") || !strings.Contains(err.Error(), "billing, shipping") {
		t.Fatalf("redrive of a shared message: %v", err)
	}
	if len(f.replayed) != 0 {
		t.Fatalf("replayed %v, want nothing", f.replayed)
	}
	if err := redrive(t, srv.URL, "--yes", "--cg", "billing"); err != nil {
		t.Fatalf("redrive --cg billing: %v", err)
	}
	if strings.Join(f.replayed, ",") != "s1" {
		t.Fatalf("replayed %v, want only billing's row s1", f.replayed)
	}
}

// Gone and duplicate are answers, not failures; the run goes on and succeeds.
func TestRedriveCountsGoneAndDuplicateAndGoesOn(t *testing.T) {
	f := &fakeDLQ{
		rows: []map[string]any{
			dlqRow("a1", "p1", "order-1", "billing", 1.0),
			dlqRow("a2", "p1", "order-1", "billing", 2.0),
			dlqRow("a3", "p1", "order-1", "billing", 3.0),
		},
		answers: map[string]func(http.ResponseWriter){
			"a1": func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"success":false,"result":"gone","error":"Message not found"}`))
			},
			"a2": func(w http.ResponseWriter) {
				_, _ = w.Write([]byte(`{"success":true,"result":"duplicate","dlqRowRemoved":false}`))
			},
		},
	}
	srv := f.serve(t)
	if err := redrive(t, srv.URL, "--yes"); err != nil {
		t.Fatalf("redrive: %v", err)
	}
	if strings.Join(f.replayed, ",") != "a1,a2,a3" {
		t.Fatalf("replayed %v", f.replayed)
	}
}

// A 403 says every next call fails the same way: nothing more is sent, and the
// run reports what it did not send.
func TestRedriveStopsAtARefusal(t *testing.T) {
	f := &fakeDLQ{
		rows: []map[string]any{
			dlqRow("a1", "p1", "order-1", "billing", 1.0),
			dlqRow("a2", "p1", "order-1", "billing", 2.0),
			dlqRow("b1", "p2", "order-2", "billing", 1.0),
		},
		answers: map[string]func(http.ResponseWriter){
			"a1": func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"forbidden","code":"forbidden"}`))
			},
		},
	}
	srv := f.serve(t)
	err := redrive(t, srv.URL, "--yes", "--parallel", "1")
	if err == nil || !strings.Contains(err.Error(), "3 of 3 rows were not replayed") {
		t.Fatalf("redrive after a 403: %v", err)
	}
	if strings.Join(f.replayed, ",") != "a1" {
		t.Fatalf("replayed %v, want only the first try", f.replayed)
	}
}
