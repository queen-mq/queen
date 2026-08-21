package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

// The two operator-facing surfaces conflation adds to queenctl (PLAN_CONFLATION
// §5.3 depth, §2.6/§5.4 the consumer-group listing).
//
// Both matter for the same reason: under conflation the number an operator has
// been reading for years changes meaning. `pending` stops being "work left" and
// becomes "log positions left to retire". A queue at pending 4,000,000 with
// effective 12 is healthy; the same two numbers on a non-conflating group are an
// incident. If the console cannot tell those apart it will page someone at 3am
// for a healthy queue.

// ---------------------------------------------------------------------------
// queue depth
// ---------------------------------------------------------------------------

func TestDepthRowUsesTheServersPartitionsPending(t *testing.T) {
	// Since 1.1.0 the SP computes the non-empty count inside the aggregate it
	// was already running (§2.5). queenctl must stop recomputing it client-side:
	// its own scan is over the partitions array, which is exactly the thing an
	// operator caps with -o json on a 100k-partition queue.
	row := depthTableRow(map[string]any{
		"queue":             "orders",
		"group":             "workers",
		"pending":           float64(4_000_000),
		"partitionsPending": float64(12),
		"effectivePending":  float64(12),
		"conflation":        true,
		"partitions": []any{
			map[string]any{"partition": "a", "pending": float64(0)},
			map[string]any{"partition": "b", "pending": float64(3)},
		},
	})

	if got := row["partitionsPending"]; got != float64(12) {
		t.Errorf("partitionsPending = %v, want the server's 12 (not the 1 the local array implies)", got)
	}
	if got := row["effective"]; got != float64(12) {
		t.Errorf("effective = %v, want 12", got)
	}
	if got := row["conflation"]; got != true {
		t.Errorf("conflation = %v, want true", got)
	}
	if got := row["pending"]; got != float64(4_000_000) {
		t.Errorf("pending = %v, want the log depth 4000000 unchanged", got)
	}
	if _, present := row["partitionsNonEmpty"]; present {
		t.Errorf("the client-side partitionsNonEmpty must be gone, replaced by partitionsPending")
	}
}

func TestDepthRowFallsBackToTheLocalCountOnAnOlderBroker(t *testing.T) {
	// A 1.0.x broker sends neither field. `queenctl queue depth` predates
	// conflation and must keep working unchanged against it.
	row := depthTableRow(map[string]any{
		"queue":   "orders",
		"group":   "",
		"pending": float64(3),
		"partitions": []any{
			map[string]any{"partition": "a", "pending": float64(0)},
			map[string]any{"partition": "b", "pending": float64(3)},
		},
	})

	if got := row["partitionsPending"]; got != 1 {
		t.Errorf("partitionsPending = %v, want the locally counted 1", got)
	}
	if _, present := row["effective"]; present {
		t.Errorf("effective must be absent when the broker did not send it, got %v", row["effective"])
	}
	if _, present := row["conflation"]; present {
		t.Errorf("conflation must be absent when the broker did not send it")
	}
	if got := row["partitions"]; got != 2 {
		t.Errorf("partitions = %v, want 2", got)
	}
}

func TestDepthRowOmitsEffectiveForANonConflatingGroup(t *testing.T) {
	// A 1.1.0 broker sends effectivePending for EVERY group; for a
	// non-conflating one it equals pending and repeating it is noise.
	row := depthTableRow(map[string]any{
		"queue":             "orders",
		"group":             "audit",
		"pending":           float64(500),
		"partitionsPending": float64(4),
		"effectivePending":  float64(500),
		"conflation":        false,
		"partitions":        []any{},
	})

	if _, present := row["effective"]; present {
		t.Errorf("effective duplicates pending for a non-conflating group; want it omitted, got %v", row["effective"])
	}
	if _, present := row["conflation"]; present {
		t.Errorf("conflation=false is the default; want the key omitted")
	}
	if got := row["partitionsPending"]; got != float64(4) {
		t.Errorf("partitionsPending = %v, want 4", got)
	}
}

// ---------------------------------------------------------------------------
// consumer-group listing
// ---------------------------------------------------------------------------

func TestConsumerGroupTableShowsConflation(t *testing.T) {
	// §2.6 fixes M7 (the SP hard-coded subscriptionMode to NULL and had no
	// conflation field at all). Without a column here the policy that decides
	// how MESSAGES and LAG on the same row should be read is invisible.
	fb := newFakeBroker(t)
	fb.route("/api/v1/consumer-groups", `{"consumerGroups":[
        {"name":"workers","queue":"orders","lagMessages":4000000,"lagSeconds":30,"conflation":true},
        {"name":"audit","queue":"orders","lagMessages":12,"lagSeconds":1,"conflation":false}
    ]}`)

	stop := captureStdio(t)
	err := runCLI(t, fb.url(), "cg", "list", "-o", "table")
	stdout, _ := stop()
	if err != nil {
		t.Fatalf("cg list: %v", err)
	}

	if !strings.Contains(stdout, "CONFLATION") {
		t.Fatalf("cg list has no CONFLATION column:\n%s", stdout)
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	var workers, audit string
	for _, l := range lines {
		if strings.HasPrefix(l, "workers") {
			workers = l
		}
		if strings.HasPrefix(l, "audit") {
			audit = l
		}
	}
	if workers == "" || audit == "" {
		t.Fatalf("both groups should be listed:\n%s", stdout)
	}
	if !strings.Contains(workers, "yes") {
		t.Errorf("conflating group row does not say so: %q", workers)
	}
	if strings.Contains(audit, "yes") {
		t.Errorf("non-conflating group row claims conflation: %q", audit)
	}
}

func TestConsumerGroupTableSurvivesAnOlderBroker(t *testing.T) {
	// A 1.0.x broker sends no conflation field; the column renders "-" rather
	// than blowing up or printing "<nil>".
	fb := newFakeBroker(t)
	fb.route("/api/v1/consumer-groups", `{"consumerGroups":[
        {"name":"workers","queue":"orders","lagMessages":3,"lagSeconds":0}
    ]}`)

	stop := captureStdio(t)
	err := runCLI(t, fb.url(), "cg", "list", "-o", "table")
	stdout, _ := stop()
	if err != nil {
		t.Fatalf("cg list: %v", err)
	}
	if strings.Contains(stdout, "<nil>") || strings.Contains(stdout, "false") {
		t.Fatalf("missing conflation should render as '-':\n%s", stdout)
	}
}

// ---------------------------------------------------------------------------
// `cg describe` keeps every field the SP now sends
// ---------------------------------------------------------------------------

func TestConsumerGroupDescribeCarriesConflation(t *testing.T) {
	fb := newFakeBroker(t)
	fb.route("/api/v1/consumer-groups/workers",
		`{"name":"workers","queue":"orders","subscriptionMode":"new","conflation":true}`)

	stop := captureStdio(t)
	err := runCLI(t, fb.url(), "cg", "describe", "workers", "-o", "json")
	stdout, _ := stop()
	if err != nil {
		t.Fatalf("cg describe: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("describe output is not JSON: %v\n%s", err, stdout)
	}
	if got["conflation"] != true {
		t.Errorf("describe dropped conflation: %v", got)
	}
	if got["subscriptionMode"] != "new" {
		t.Errorf("describe dropped subscriptionMode: %v", got)
	}
}
