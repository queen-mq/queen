package main

// flatness — the G-3 / I8 harness of PLAN_RAFT.md §13.6.
//
// The test in three steps, and where each one lives:
//
//	1. preload 300 M messages over 1 M partitions      -> `flatness preload`   (CLI: real, loop: stub)
//	2. run A20k and C1000 for 60 minutes each          -> `flatness run`       (stub: drives goload, writes RESULTS)
//	3. compare against the same regimes on an empty    -> `flatness compare`   (real)
//	   store; every metric within ±15%, RSS flat ±5%
//
// Usage:
//
//	flatness compare -baseline empty-A20k.results -candidate preloaded-A20k.results
//	flatness preload -dry-run
//	flatness run -regime A20k                 # stub
//	flatness selftest
//
// Exit codes: 0 pass · 1 the comparison failed · 2 could not run.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help":
		fmt.Print(usage)
		return 0
	case "selftest", "-selftest", "--selftest":
		if err := SelfTest(); err != nil {
			fmt.Fprintf(os.Stderr, "selftest FAILED: %v\n", err)
			return 2
		}
		fmt.Println("flatness selftest ok (RESULTS format, comparator thresholds, preload plan)")
		return 0
	case "compare":
		return cmdCompare(args[1:])
	case "preload":
		return cmdPreload(args[1:])
	case "run":
		return cmdRun(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "flatness: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

func cmdCompare(args []string) int {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	baseline := fs.String("baseline", "", "RESULTS of the EMPTY-store run")
	candidate := fs.String("candidate", "", "RESULTS of the PRELOADED-store run")
	markdown := fs.Bool("markdown", false, "print the RAFT_STATUS.md table instead of the text report")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, compareUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *baseline == "" || *candidate == "" {
		fmt.Fprintln(os.Stderr, "flatness compare: -baseline and -candidate are both required")
		return 2
	}
	b, err := LoadResults(*baseline)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flatness compare: %v\n", err)
		return 2
	}
	c, err := LoadResults(*candidate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flatness compare: %v\n", err)
		return 2
	}
	cmp := Compare(b, c)
	if *markdown {
		fmt.Print(cmp.Markdown())
	} else {
		fmt.Print(cmp.Text())
	}
	if !cmp.Passed() {
		return 1
	}
	return 0
}

func cmdPreload(args []string) int {
	cfg, err := ParsePreloadFlags(args, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(os.Stderr, "flatness preload: %v\n", err)
		return 2
	}
	plan, err := cfg.Plan()
	if err != nil {
		fmt.Fprintf(os.Stderr, "flatness preload: %v\n", err)
		return 2
	}
	fmt.Print(plan.Text(cfg))
	if cfg.DryRun {
		fmt.Println("\ndry run: nothing was pushed.")
		return 0
	}
	fmt.Fprintln(os.Stderr, "\nflatness preload: the send loop is a documented stub (WP-0.7, preload.go).")
	fmt.Fprintln(os.Stderr, "It owes: the partition walk with the state file, the push loop with -concurrency "+
		"pushers and -rate pacing, per-item status checking (a 201 alone proves nothing, §0.3), progress "+
		"reporting, and the configure calls above.")
	return 2
}

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	regime := fs.String("regime", "A20k", "regime to run (Appendix G): A20k, A50k, B1, C1000, D1")
	minutes := fs.Int("minutes", 60, "run length (§13.6 uses 60)")
	state := fs.String("state", "preloaded", "store state for the RESULTS header: empty | preloaded")
	out := fs.String("out", "", "RESULTS file to write")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	fmt.Fprintf(os.Stderr, "flatness run: the regime runner is a documented stub (WP-0.7).\n")
	fmt.Fprintf(os.Stderr, "It would run goload %s for %d minutes against a %s store and write %s in the "+
		"RESULTS format of results.go (p50_ms, p99_ms, p999_ms, ack_rtt_ms, cpu_cores, disk_mbps, "+
		"durable_point_ms, snapshot_build_s, rss_start_mb, rss_end_mb), with the header carrying regime, "+
		"state, host, commit, command and duration.\n", *regime, *minutes, *state, orDash(*out))
	fmt.Fprintf(os.Stderr, "Until then: run goload by hand (benchmark-queen/2026-07-29-vm-campaign/goload), "+
		"write the RESULTS file, and use `flatness compare`.\n")
	return 2
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "<no -out given>"
	}
	return s
}

const usage = `flatness — the flatness test of PLAN_RAFT.md §13.6 (G-3, I8)

  flatness compare -baseline <empty.results> -candidate <preloaded.results>
  flatness preload -dry-run
  flatness run -regime A20k -minutes 60        (stub)
  flatness selftest

Acceptance (§13.6): every metric within ±15% of the empty-store run, and RSS
flat (±5% after warm-up) across the 60 minutes. Snapshot build time is reported
and NOT gated: I8 exempts it.

State (WP-0.7): the RESULTS format and the comparator are real; the preloader's
CLI, validation, plan and estimate are real; the send loop and the regime runner
are documented stubs.
`

const compareUsage = `flatness compare — judge a preloaded run against an empty-store run

Both files are RESULTS tables (the format is documented in results.go). The
comparison fails when a gated metric moves more than ±15%, when RSS drifts more
than ±5% inside a run, or when the two files are not comparable (different
regime, topology or commit, or a metric only one of them measured).

Flags:
`
