package main

// The comparator: two RESULTS tables in, one verdict out (PLAN_RAFT.md §13.6).
//
// THE ACCEPTANCE, quoted:
//
//	Compare against the same regimes on an empty store: p50, p99, p999, ack
//	round trip, CPU cores, disk MB/s, durable point and snapshot times.
//	Acceptance: every metric within ±15% of the empty-store run, and RSS flat
//	(±5% after warm-up) across the 60 minutes.
//
// Three things this file insists on, because each of them is a way a flatness
// claim goes wrong:
//
//  1. A metric present on one side only is an ERROR. Two runs that measured
//     different things are not a comparison.
//  2. RSS is judged TWICE: preloaded-vs-empty like everything else, and
//     within each run (start vs end), which is the "RSS flat across the 60
//     minutes" half — a store whose RAM grows with what it holds passes the
//     first test and fails the second (I8).
//  3. Better than baseline is not automatically a pass for every metric. For
//     latency and cost, faster is fine; but a run that moved 40% fewer bytes
//     may simply have done less work, so throughput-shaped metrics are judged
//     two-sided and the reason is printed.
//
// Snapshot build time is reported but NOT gated by the ±15%: I8 exempts it
// ("a snapshot build for transfer may cost O(state)"). It is compared and
// printed with its own note, so the number is never lost, and a reviewer sees
// exactly how much it grew.

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Rule says how a metric is judged.
type Rule struct {
	Tolerance float64 // fractional, e.g. 0.15 for ±15%
	TwoSided  bool    // false: only "worse than baseline" fails
	Gate      bool    // false: compared and printed, never fails the run
	Note      string
}

// DefaultTolerance is the §13.6 acceptance for everything the plan lists.
const DefaultTolerance = 0.15

// RSSDriftTolerance is "RSS flat (±5% after warm-up) across the 60 minutes".
const RSSDriftTolerance = 0.05

// boundaryEpsilon keeps "within ±15%" from turning into "within 14.999…%":
// 18.9 * 1.15 computes to 15.000000000000004% in float64, and a gate that fails
// a number a human wrote as exactly the limit is a gate people route around.
const boundaryEpsilon = 1e-9

// Rules for the metrics §13.6 names. A metric with no rule gets the default
// one-sided ±15% gate, so a new metric is judged rather than ignored; add it
// here when it needs different treatment.
var Rules = map[string]Rule{
	"p50_ms":           {Tolerance: DefaultTolerance, Gate: true, Note: "latency: higher is worse"},
	"p99_ms":           {Tolerance: DefaultTolerance, Gate: true},
	"p999_ms":          {Tolerance: DefaultTolerance, Gate: true},
	"ack_rtt_ms":       {Tolerance: DefaultTolerance, Gate: true},
	"cpu_cores":        {Tolerance: DefaultTolerance, Gate: true, Note: "cost must follow the write rate, not the stored volume (G-3)"},
	"disk_mbps":        {Tolerance: DefaultTolerance, Gate: true, TwoSided: true, Note: "two-sided: moving far fewer bytes means the run did less work"},
	"durable_point_ms": {Tolerance: DefaultTolerance, Gate: true, Note: "I8: a durable point costs what changed, not what is stored"},
	"throughput_msgs":  {Tolerance: DefaultTolerance, Gate: true, TwoSided: true, Note: "two-sided: a slower run is a regression, a faster one means the runs are not comparable"},
	"snapshot_build_s": {Tolerance: DefaultTolerance, Gate: false, Note: "REPORTED, NOT GATED: I8 exempts snapshot build (O(state) by design)"},
	"rss_start_mb":     {Tolerance: DefaultTolerance, Gate: false, Note: "reported; the gate is the in-run drift below"},
	"rss_end_mb":       {Tolerance: DefaultTolerance, Gate: false, Note: "reported; the gate is the in-run drift below"},
}

func ruleFor(metric string) Rule {
	if r, ok := Rules[metric]; ok {
		return r
	}
	return Rule{Tolerance: DefaultTolerance, Gate: true, Note: "no rule declared: judged with the default ±15% gate"}
}

// MetricComparison is one row of the verdict.
type MetricComparison struct {
	Metric    string
	Baseline  float64
	Candidate float64
	DeltaPct  float64
	Verdict   string // PASS | FAIL | REPORT | ERROR
	Note      string
}

// Comparison is the whole verdict.
type Comparison struct {
	BaselinePath  string
	CandidatePath string
	Header        []string // the header lines of both files, for the report
	Rows          []MetricComparison
	Drift         []MetricComparison // the in-run RSS drift checks
	Problems      []string           // structural problems (missing metric, mismatched regime)
}

func (c *Comparison) Passed() bool {
	if len(c.Problems) > 0 {
		return false
	}
	for _, r := range append(append([]MetricComparison{}, c.Rows...), c.Drift...) {
		if r.Verdict == "FAIL" || r.Verdict == "ERROR" {
			return false
		}
	}
	return true
}

// Compare judges `candidate` (the preloaded store) against `baseline` (empty).
func Compare(baseline, candidate *Results) *Comparison {
	c := &Comparison{BaselinePath: baseline.Path, CandidatePath: candidate.Path}

	// The two runs must be the same regime, the same topology and the same
	// commit; otherwise the comparison measures something else.
	for _, k := range []string{"regime", "topology", "commit"} {
		b, cand := baseline.Header[k], candidate.Header[k]
		if b != "" && cand != "" && b != cand {
			c.Problems = append(c.Problems,
				fmt.Sprintf("%s differs: baseline %s=%q, candidate %s=%q — these two runs are not comparable", k, k, b, k, cand))
		}
	}
	if baseline.Header["state"] == candidate.Header["state"] {
		c.Problems = append(c.Problems, fmt.Sprintf(
			"both files say state=%q: the flatness test compares an EMPTY store with a PRELOADED one", baseline.Header["state"]))
	}
	c.Header = []string{
		"baseline  " + headerLine(baseline),
		"candidate " + headerLine(candidate),
	}

	names := map[string]bool{}
	for k := range baseline.Metrics {
		names[k] = true
	}
	for k := range candidate.Metrics {
		names[k] = true
	}
	sorted := make([]string, 0, len(names))
	for k := range names {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	for _, name := range sorted {
		b, okB := baseline.Metrics[name]
		cand, okC := candidate.Metrics[name]
		if !okB || !okC {
			side := "candidate"
			if !okB {
				side = "baseline"
			}
			c.Rows = append(c.Rows, MetricComparison{
				Metric: name, Baseline: b, Candidate: cand, Verdict: "ERROR",
				Note: "missing on the " + side + ": the two runs did not measure the same thing",
			})
			continue
		}
		rule := ruleFor(name)
		delta := pctDelta(b, cand)
		row := MetricComparison{Metric: name, Baseline: b, Candidate: cand, DeltaPct: delta, Note: rule.Note}
		switch {
		case !rule.Gate:
			row.Verdict = "REPORT"
		case math.Abs(delta) <= rule.Tolerance*100+boundaryEpsilon:
			row.Verdict = "PASS"
		case !rule.TwoSided && delta < 0:
			row.Verdict = "PASS" // better than baseline on a one-sided metric
		default:
			row.Verdict = "FAIL"
		}
		c.Rows = append(c.Rows, row)
	}

	c.Drift = append(c.Drift, rssDrift("baseline", baseline), rssDrift("candidate", candidate))
	return c
}

// rssDrift is the second half of the acceptance: RSS flat within one run.
func rssDrift(side string, r *Results) MetricComparison {
	start, okS := r.Metrics["rss_start_mb"]
	end, okE := r.Metrics["rss_end_mb"]
	row := MetricComparison{Metric: side + " rss drift", Baseline: start, Candidate: end}
	if !okS || !okE {
		row.Verdict = "ERROR"
		row.Note = "rss_start_mb / rss_end_mb missing: \"RSS flat (±5% after warm-up)\" cannot be judged"
		return row
	}
	row.DeltaPct = pctDelta(start, end)
	row.Note = fmt.Sprintf("±%.0f%% across the run (§13.6)", RSSDriftTolerance*100)
	if math.Abs(row.DeltaPct) <= RSSDriftTolerance*100+boundaryEpsilon {
		row.Verdict = "PASS"
	} else {
		row.Verdict = "FAIL"
	}
	return row
}

func pctDelta(baseline, candidate float64) float64 {
	if baseline == 0 {
		if candidate == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return (candidate - baseline) / baseline * 100
}

func headerLine(r *Results) string {
	keys := make([]string, 0, len(r.Header))
	for k := range r.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+r.Header[k])
	}
	return strings.Join(parts, " ")
}

// Text renders the verdict.
func (c *Comparison) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "flatness comparison (PLAN_RAFT.md §13.6, G-3, I8)\n")
	for _, h := range c.Header {
		fmt.Fprintf(&b, "  %s\n", h)
	}
	fmt.Fprintf(&b, "\n  %-22s %12s %12s %9s  %s\n", "metric", "empty", "preloaded", "delta", "verdict")
	for _, r := range c.Rows {
		fmt.Fprintf(&b, "  %-22s %12.4g %12.4g %8.1f%%  %-7s %s\n",
			r.Metric, r.Baseline, r.Candidate, r.DeltaPct, r.Verdict, r.Note)
	}
	fmt.Fprintf(&b, "\n  RSS drift within each run (acceptance: ±%.0f%%)\n", RSSDriftTolerance*100)
	for _, r := range c.Drift {
		fmt.Fprintf(&b, "  %-22s %12.4g %12.4g %8.1f%%  %-7s %s\n",
			r.Metric, r.Baseline, r.Candidate, r.DeltaPct, r.Verdict, r.Note)
	}
	for _, p := range c.Problems {
		fmt.Fprintf(&b, "\n  PROBLEM: %s\n", p)
	}
	verdict := "FAIL"
	if c.Passed() {
		verdict = "PASS"
	}
	fmt.Fprintf(&b, "\n  verdict: %s (gate: every gated metric within ±%.0f%%, RSS drift within ±%.0f%%)\n",
		verdict, DefaultTolerance*100, RSSDriftTolerance*100)
	return b.String()
}

// Markdown renders the same verdict for RAFT_STATUS.md (§15.0 Measurements).
func (c *Comparison) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "### Flatness: %s vs %s\n\n", c.CandidatePath, c.BaselinePath)
	for _, h := range c.Header {
		fmt.Fprintf(&b, "- %s\n", h)
	}
	fmt.Fprintf(&b, "\n| metric | empty store | preloaded | delta | verdict |\n|---|---:|---:|---:|---|\n")
	for _, r := range append(append([]MetricComparison{}, c.Rows...), c.Drift...) {
		fmt.Fprintf(&b, "| %s | %.4g | %.4g | %+.1f%% | %s |\n", r.Metric, r.Baseline, r.Candidate, r.DeltaPct, r.Verdict)
	}
	verdict := "FAIL"
	if c.Passed() {
		verdict = "PASS"
	}
	fmt.Fprintf(&b, "\nVerdict: **%s**\n", verdict)
	return b.String()
}
