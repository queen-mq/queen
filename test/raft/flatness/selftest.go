package main

// The offline smoke test, shared by `flatness selftest` and `go test`.
//
// It builds RESULTS tables in memory and asserts the verdicts, including the
// negative cases. A comparator that never says FAIL is a rubber stamp, and a
// flatness gate that rubber-stamps is worse than no gate: it would let a store
// whose cost follows its VOLUME (the whole thing G-3 exists to prevent) ship
// with a green line in RAFT_STATUS.md.

import (
	"fmt"
	"strings"
)

func SelfTest() error {
	for _, c := range []struct {
		name string
		fn   func() error
	}{
		{"RESULTS round-trips", checkResultsRoundTrip},
		{"a header without host and commit is refused", checkHeaderRequired},
		{"an equal pair passes", checkEqualPasses},
		{"a 30% latency regression fails", checkRegressionFails},
		{"RSS growth inside a run fails", checkRSSDriftFails},
		{"a metric on one side only is an error", checkMissingMetricErrors},
		{"two runs of the same state are refused", checkSameStateRefused},
		{"snapshot build time is reported, not gated", checkSnapshotNotGated},
		{"the preload plan is the §13.6 shape", checkPreloadPlan},
		{"the preload CLI refuses an impossible shape", checkPreloadRefusals},
	} {
		if err := c.fn(); err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
	}
	return nil
}

func sample(state string, over map[string]float64) *Results {
	r := NewResults()
	r.Path = state + ".results"
	r.Header = map[string]string{
		"regime": "A20k", "state": state, "host": "vm-164.90.215.224", "commit": "abc1234",
		"topology": "raft3", "duration_s": "3600", "command": "goload -openloop -rate 20000",
	}
	base := map[string]float64{
		"p50_ms": 18.9, "p99_ms": 41.2, "p999_ms": 120, "ack_rtt_ms": 22.4,
		"cpu_cores": 3.1, "disk_mbps": 118, "durable_point_ms": 4.8,
		"snapshot_build_s": 41, "rss_start_mb": 1024, "rss_end_mb": 1040,
	}
	for k, v := range over {
		base[k] = v
	}
	for _, k := range []string{"p50_ms", "p99_ms", "p999_ms", "ack_rtt_ms", "cpu_cores", "disk_mbps",
		"durable_point_ms", "snapshot_build_s", "rss_start_mb", "rss_end_mb"} {
		r.Metrics[k] = base[k]
		r.Order = append(r.Order, k)
	}
	return r
}

func checkResultsRoundTrip() error {
	in := sample("empty", nil)
	text := in.String()
	out, err := ParseResults("<roundtrip>", strings.NewReader(text))
	if err != nil {
		return err
	}
	if out.String() != text {
		return fmt.Errorf("a table did not survive a write/parse round trip:\n%s\n---\n%s", text, out.String())
	}
	if out.Header["command"] != in.Header["command"] {
		return fmt.Errorf("the quoted command was lost: %q", out.Header["command"])
	}
	return nil
}

func checkHeaderRequired() error {
	_, err := ParseResults("<x>", strings.NewReader("# regime=A20k state=empty\np50_ms 1\n"))
	if err == nil {
		return fmt.Errorf("a table with no host and no commit was accepted")
	}
	if !strings.Contains(err.Error(), "host") {
		return fmt.Errorf("the error does not name the missing field: %v", err)
	}
	return nil
}

func checkEqualPasses() error {
	c := Compare(sample("empty", nil), sample("preloaded", nil))
	if !c.Passed() {
		return fmt.Errorf("two identical runs failed:\n%s", c.Text())
	}
	return nil
}

func checkRegressionFails() error {
	c := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"p99_ms": 41.2 * 1.30}))
	if c.Passed() {
		return fmt.Errorf("a 30%% p99 regression passed:\n%s", c.Text())
	}
	for _, r := range c.Rows {
		if r.Metric == "p99_ms" {
			if r.Verdict != "FAIL" {
				return fmt.Errorf("p99_ms verdict is %s", r.Verdict)
			}
			return nil
		}
	}
	return fmt.Errorf("p99_ms was not compared at all")
}

func checkRSSDriftFails() error {
	// Within ±15% of the baseline on every metric, but RSS grows 10% inside the
	// run: the store's RAM follows its volume. This is the case a
	// metric-by-metric comparison alone would pass.
	cand := sample("preloaded", map[string]float64{"rss_start_mb": 1024, "rss_end_mb": 1126})
	c := Compare(sample("empty", nil), cand)
	if c.Passed() {
		return fmt.Errorf("a 10%% RSS drift inside the run passed:\n%s", c.Text())
	}
	found := false
	for _, r := range c.Drift {
		if strings.HasPrefix(r.Metric, "candidate") {
			found = true
			if r.Verdict != "FAIL" {
				return fmt.Errorf("the candidate drift verdict is %s", r.Verdict)
			}
		}
	}
	if !found {
		return fmt.Errorf("no drift row for the candidate")
	}
	return nil
}

func checkMissingMetricErrors() error {
	cand := sample("preloaded", nil)
	delete(cand.Metrics, "durable_point_ms")
	c := Compare(sample("empty", nil), cand)
	if c.Passed() {
		return fmt.Errorf("a comparison missing a metric passed")
	}
	for _, r := range c.Rows {
		if r.Metric == "durable_point_ms" && r.Verdict == "ERROR" {
			return nil
		}
	}
	return fmt.Errorf("the missing metric was not reported as an ERROR")
}

func checkSameStateRefused() error {
	c := Compare(sample("empty", nil), sample("empty", nil))
	if c.Passed() {
		return fmt.Errorf("comparing two empty-store runs passed as a flatness result")
	}
	if len(c.Problems) == 0 {
		return fmt.Errorf("no problem was reported for two runs of the same state")
	}
	return nil
}

func checkSnapshotNotGated() error {
	// Snapshot build doubles: reported, and not a failure on its own (I8).
	c := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"snapshot_build_s": 82}))
	if !c.Passed() {
		return fmt.Errorf("a longer snapshot build failed the gate, but I8 exempts it:\n%s", c.Text())
	}
	for _, r := range c.Rows {
		if r.Metric == "snapshot_build_s" {
			if r.Verdict != "REPORT" {
				return fmt.Errorf("snapshot_build_s verdict is %s, want REPORT", r.Verdict)
			}
			if r.DeltaPct < 99 {
				return fmt.Errorf("the doubling was not reported: delta %.1f%%", r.DeltaPct)
			}
			return nil
		}
	}
	return fmt.Errorf("snapshot_build_s was not in the report")
}

func checkPreloadPlan() error {
	cfg := &PreloadConfig{
		URL: "http://vm:6632", Queues: 10, Partitions: 1_000_000, Messages: 300_000_000,
		PayloadBytes: 256, Batch: 500, Concurrency: 16, DedupWindowS: 3600, RetentionOff: true,
		StateFile: "/root/raft/preload.state", AssumeRate: 200_000,
	}
	p, err := cfg.Plan()
	if err != nil {
		return err
	}
	if p.MessagesPerPartition != 300 {
		return fmt.Errorf("300 M over 1 M partitions is %d per partition, want 300", p.MessagesPerPartition)
	}
	if p.PartitionsPerQueue != 100_000 {
		return fmt.Errorf("%d partitions per queue, want 100000", p.PartitionsPerQueue)
	}
	if p.Batches != 600_000 {
		return fmt.Errorf("%d batches of 500, want 600000", p.Batches)
	}
	if len(p.Warnings) != 0 {
		return fmt.Errorf("the §13.6 shape produced warnings: %v", p.Warnings)
	}
	text := p.Text(cfg)
	for _, want := range []string{"300M", "1M", "dedupWindowSeconds", "estimate"} {
		if !strings.Contains(text, want) {
			return fmt.Errorf("the plan does not mention %q:\n%s", want, text)
		}
	}
	// Retention on and a short dedup window must warn, loudly.
	cfg2 := *cfg
	cfg2.RetentionOff = false
	cfg2.DedupWindowS = 60
	p2, err := cfg2.Plan()
	if err != nil {
		return err
	}
	if len(p2.Warnings) < 2 {
		return fmt.Errorf("retention on + a 60s dedup window produced %d warning(s)", len(p2.Warnings))
	}
	return nil
}

func checkPreloadRefusals() error {
	bad := []*PreloadConfig{
		{URL: "", Queues: 1, Partitions: 1, Messages: 1, Batch: 1, Concurrency: 1},
		{URL: "u", Queues: 10, Partitions: 5, Messages: 100, Batch: 1, Concurrency: 1},               // fewer partitions than queues
		{URL: "u", Queues: 1, Partitions: 1000, Messages: 100, Batch: 1, Concurrency: 1},             // empty partitions
		{URL: "u", Queues: 1, Partitions: 10, Messages: 100, Batch: 0, Concurrency: 1},               // batch 0
		{URL: "u", Queues: 1, Partitions: 10, Messages: 100, Batch: 1, Concurrency: 1, Resume: true}, // resume without state
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			return fmt.Errorf("config %d was accepted: %+v", i, c)
		}
	}
	for _, s := range []string{"300m", "1g", "1_000_000", "500k"} {
		if _, err := ParseCount(s); err != nil {
			return fmt.Errorf("ParseCount(%q): %v", s, err)
		}
	}
	if v, _ := ParseCount("300m"); v != 300_000_000 {
		return fmt.Errorf("300m parsed as %d", v)
	}
	if _, err := ParseCount("lots"); err == nil {
		return fmt.Errorf("ParseCount accepted %q", "lots")
	}
	return nil
}
