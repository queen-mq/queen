package main

// Entry point. Three modes:
//
//   -h / -help    the flag surface (documented in config.go).
//   -list-ops     the operation catalogue of §13.4 with what is implemented
//                 and what is still a stub. No broker needed.
//   -selftest     the offline smoke test: generator determinism, normalization
//                 and comparison, with no broker and no network. Same checks as
//                 `GOWORK=off go test ./...`, callable from a shell script.
//
// Otherwise: run one seeded sequence against the two brokers.
//
// Exit codes, because CI reads them:
//   0  ran, no divergence
//   1  ran, divergence(s) found        (the run is the bug report)
//   2  could not run (flags, transport, broker down)

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	for _, a := range args {
		switch a {
		case "-list-ops", "--list-ops":
			listOps(os.Stdout)
			return 0
		case "-selftest", "--selftest":
			if err := SelfTest(); err != nil {
				fmt.Fprintf(os.Stderr, "selftest FAILED: %v\n", err)
				return 2
			}
			fmt.Println("difffuzz selftest ok (generator determinism, normalization, comparison)")
			return 0
		}
	}

	cfg, err := ParseFlags(args, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0 // -h printed the usage; asking for help is not a failure
		}
		fmt.Fprintf(os.Stderr, "difffuzz: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("difffuzz seed=%d run-id=%s ops=%d mix=%s\n", cfg.Seed, cfg.RunID, cfg.Ops, cfg.Mix.String())
	fmt.Printf("  A(%s)=%s  B(%s)=%s\n", cfg.NameA, cfg.URLA, cfg.NameB, cfg.URLB)

	r := NewRunner(cfg, os.Stdout)
	closeLogs, err := openSideLogs(cfg, r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "difffuzz: %v\n", err)
		return 2
	}
	defer closeLogs()
	if err := r.Preflight(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "difffuzz: %v\n", err)
		return 2
	}
	report, runErr := r.Run(ctx)
	fmt.Print(report.Text())
	if cfg.OutDir != "" {
		if err := writeReport(cfg, report); err != nil {
			fmt.Fprintf(os.Stderr, "difffuzz: writing report: %v\n", err)
		}
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "difffuzz: run stopped: %v\n", runErr)
		return 2
	}
	if len(report.Divergences) > 0 {
		return 1
	}
	return 0
}

// openSideLogs creates the per-side checker run logs under cfg.LogDir (one per
// broker: <name>.jsonl), attaches them to the runner, and returns a closer. When
// LogDir is empty it is a no-op and the runner writes no logs.
func openSideLogs(cfg *Config, r *Runner) (func(), error) {
	if cfg.LogDir == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		return nil, fmt.Errorf("logdir: %w", err)
	}
	open := func(name string) (*os.File, error) {
		p := filepath.Join(cfg.LogDir, fmt.Sprintf("%s-%d-%s.jsonl", name, cfg.Seed, cfg.RunID))
		return os.Create(p)
	}
	fa, err := open(cfg.NameA)
	if err != nil {
		return nil, fmt.Errorf("logdir: %w", err)
	}
	fb, err := open(cfg.NameB)
	if err != nil {
		_ = fa.Close()
		return nil, fmt.Errorf("logdir: %w", err)
	}
	_, _ = fmt.Fprintf(fa, "# difffuzz side=%s seed=%d run-id=%s\n", cfg.NameA, cfg.Seed, cfg.RunID)
	_, _ = fmt.Fprintf(fb, "# difffuzz side=%s seed=%d run-id=%s\n", cfg.NameB, cfg.Seed, cfg.RunID)
	r.SetLogs(NewRunLog(fa, cfg.NameA), NewRunLog(fb, cfg.NameB))
	fmt.Printf("  logs: %s/{%s,%s}-%d-%s.jsonl\n", cfg.LogDir, cfg.NameA, cfg.NameB, cfg.Seed, cfg.RunID)
	return func() { _ = fa.Close(); _ = fb.Close() }, nil
}

func writeReport(cfg *Config, report *Report) error {
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(cfg.OutDir, fmt.Sprintf("difffuzz-%d-%s.json", cfg.Seed, cfg.RunID))
	if err := os.WriteFile(path, report.JSON(), 0o644); err != nil {
		return err
	}
	fmt.Printf("  report: %s\n", path)
	return nil
}

func listOps(w *os.File) {
	fmt.Fprintln(w, "difffuzz operation catalogue (PLAN_RAFT.md §13.4)")
	fmt.Fprintln(w, "  [x] implemented, [ ] documented stub")
	names := AllKindNames()
	sort.Strings(names)
	for _, n := range names {
		k := OpKind(n)
		mark := " "
		if Implemented(k) {
			mark = "x"
		}
		fmt.Fprintf(w, "  [%s] %-14s %s\n", mark, n, kinds[k].note)
	}
	fmt.Fprintf(w, "\ndefault mix: %s\n", DefaultMix().String())
}
