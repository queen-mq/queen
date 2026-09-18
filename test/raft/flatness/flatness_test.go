package main

// `GOWORK=off go test ./...` runs the same cases as `flatness selftest`, one
// per test, plus the CLI surface.

import (
	"os"
	"strings"
	"testing"
)

func TestResultsRoundTrip(t *testing.T) {
	if err := checkResultsRoundTrip(); err != nil {
		t.Fatal(err)
	}
}

func TestHeaderRequiresHostAndCommit(t *testing.T) {
	if err := checkHeaderRequired(); err != nil {
		t.Fatal(err)
	}
}

func TestEqualRunsPass(t *testing.T) {
	if err := checkEqualPasses(); err != nil {
		t.Fatal(err)
	}
}

func TestLatencyRegressionFails(t *testing.T) {
	if err := checkRegressionFails(); err != nil {
		t.Fatal(err)
	}
}

func TestRSSDriftFails(t *testing.T) {
	if err := checkRSSDriftFails(); err != nil {
		t.Fatal(err)
	}
}

func TestMissingMetricIsAnError(t *testing.T) {
	if err := checkMissingMetricErrors(); err != nil {
		t.Fatal(err)
	}
}

func TestSameStateIsRefused(t *testing.T) {
	if err := checkSameStateRefused(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBuildIsReportedNotGated(t *testing.T) {
	if err := checkSnapshotNotGated(); err != nil {
		t.Fatal(err)
	}
}

func TestPreloadPlanIsTheSection136Shape(t *testing.T) {
	if err := checkPreloadPlan(); err != nil {
		t.Fatal(err)
	}
}

func TestPreloadRefusals(t *testing.T) {
	if err := checkPreloadRefusals(); err != nil {
		t.Fatal(err)
	}
}

func TestToleranceBoundary(t *testing.T) {
	// 15% exactly passes, 15.1% fails: the acceptance is "within ±15%".
	just := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"p50_ms": 18.9 * 1.15}))
	if !just.Passed() {
		t.Errorf("exactly +15%% failed:\n%s", just.Text())
	}
	over := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"p50_ms": 18.9 * 1.151}))
	if over.Passed() {
		t.Errorf("+15.1%% passed:\n%s", over.Text())
	}
}

func TestOneSidedMetricsAllowImprovement(t *testing.T) {
	// Half the p99 on a preloaded store is odd but not a flatness failure;
	// half the disk throughput is, because the run did less work.
	better := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"p99_ms": 20}))
	if !better.Passed() {
		t.Errorf("a faster preloaded run failed:\n%s", better.Text())
	}
	fewer := Compare(sample("empty", nil), sample("preloaded", map[string]float64{"disk_mbps": 59}))
	if fewer.Passed() {
		t.Errorf("half the disk throughput passed:\n%s", fewer.Text())
	}
}

func TestDifferentCommitIsRefused(t *testing.T) {
	cand := sample("preloaded", nil)
	cand.Header["commit"] = "deadbee"
	c := Compare(sample("empty", nil), cand)
	if c.Passed() || len(c.Problems) == 0 {
		t.Fatalf("two runs on different commits compared clean:\n%s", c.Text())
	}
}

func TestMarkdownCarriesTheEvidence(t *testing.T) {
	md := Compare(sample("empty", nil), sample("preloaded", nil)).Markdown()
	for _, want := range []string{"vm-164.90.215.224", "abc1234", "| metric |", "Verdict"} {
		if !strings.Contains(md, want) {
			t.Errorf("the markdown table does not carry %q", want)
		}
	}
}

func TestPreloadFlagsDefaultToTheSection136Shape(t *testing.T) {
	cfg, err := ParsePreloadFlags([]string{"-url", "http://vm:6632"}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messages != 300_000_000 || cfg.Partitions != 1_000_000 {
		t.Fatalf("defaults are %d messages over %d partitions, want 300M over 1M", cfg.Messages, cfg.Partitions)
	}
	if cfg.DedupWindowS != 3600 || !cfg.RetentionOff {
		t.Fatalf("defaults do not match §13.6: dedup=%d retentionOff=%v", cfg.DedupWindowS, cfg.RetentionOff)
	}
}

func TestCompareCommandExitCodes(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, r *Results) string {
		p := dir + "/" + name
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := r.Write(f); err != nil {
			t.Fatal(err)
		}
		return p
	}
	empty := write("empty.results", sample("empty", nil))
	good := write("good.results", sample("preloaded", nil))
	bad := write("bad.results", sample("preloaded", map[string]float64{"cpu_cores": 6.2}))
	if rc := run([]string{"compare", "-baseline", empty, "-candidate", good}); rc != 0 {
		t.Errorf("a passing comparison exited %d", rc)
	}
	if rc := run([]string{"compare", "-baseline", empty, "-candidate", bad}); rc != 1 {
		t.Errorf("a failing comparison exited %d, want 1", rc)
	}
	if rc := run([]string{"compare", "-baseline", empty, "-candidate", dir + "/missing.results"}); rc != 2 {
		t.Errorf("an unreadable file exited %d, want 2", rc)
	}
}

func TestPreloadDryRunExitsZeroAndARealRunRefuses(t *testing.T) {
	if rc := run([]string{"preload", "-url", "http://x:1", "-dry-run"}); rc != 0 {
		t.Errorf("a dry run exited %d", rc)
	}
	if rc := run([]string{"preload", "-url", "http://x:1"}); rc != 2 {
		t.Errorf("the stub send loop exited %d, want 2", rc)
	}
}
