package main

// checker — the invariants of PLAN_RAFT.md §13.7 over a run log.
//
//	checker -log run.jsonl                  # every implemented check
//	checker -log run.jsonl -check payload-hash
//	checker -list-checks                    # the catalogue, implemented and stubs
//	checker -selftest                       # offline smoke test, no log needed
//
// Exit codes, because every harness in test/raft/ ends with a call to this:
//   0  every selected check passed (a SKIP is printed, and does not fail)
//   1  at least one check FAILED
//   2  could not run (flags, unreadable log, unknown kind)
//
// -strict turns SKIP into a failure: the crash and kill runners use it, because
// a kill run whose log cannot be judged is a kill run that proved nothing.

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("checker", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		logPath    = fs.String("log", "", "run log to judge (JSONL, see log.go); \"-\" reads stdin")
		checkList  = fs.String("check", "", "comma-separated check ids (default: every implemented check)")
		listChecks = fs.Bool("list-checks", false, "print the check catalogue and exit")
		selftest   = fs.Bool("selftest", false, "run the offline smoke test and exit")
		asJSON     = fs.Bool("json", false, "print the results as JSON")
		strict     = fs.Bool("strict", false, "treat SKIP as a failure")
		maxViol    = fs.Int("max-violations", 20, "how many counter-examples to print per check (0: all)")
	)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *listChecks {
		printCatalogue(os.Stdout)
		return 0
	}
	if *selftest {
		if err := SelfTest(); err != nil {
			fmt.Fprintf(os.Stderr, "selftest FAILED: %v\n", err)
			return 2
		}
		fmt.Println("checker selftest ok (log format, at-least-once, payload hash, skip reporting)")
		return 0
	}
	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "checker: -log is required (or -list-checks / -selftest)")
		return 2
	}

	var (
		l   *Log
		err error
	)
	if *logPath == "-" {
		l, err = ReadLog("<stdin>", os.Stdin)
	} else {
		l, err = LoadLog(*logPath)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "checker: %v\n", err)
		return 2
	}

	var ids []string
	if strings.TrimSpace(*checkList) != "" {
		for _, id := range strings.Split(*checkList, ",") {
			if id = strings.TrimSpace(id); id != "" {
				ids = append(ids, id)
			}
		}
	}
	results, err := RunChecks(l, ids)
	if err != nil {
		fmt.Fprintf(os.Stderr, "checker: %v\n", err)
		return 2
	}

	if *asJSON {
		out := map[string]any{"log": l.Path, "events": len(l.Events), "counts": l.CountsString(), "results": results}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Printf("%s\n", b)
	} else {
		fmt.Printf("checker %s: %d events (%s)\n", l.Path, len(l.Events), l.CountsString())
		for _, r := range results {
			fmt.Printf("  %-4s %-26s %s\n", r.State, r.ID, r.Title)
			if r.Reason != "" {
				fmt.Printf("        reason: %s\n", r.Reason)
			}
			if r.Evidence != "" {
				fmt.Printf("        judged: %s\n", r.Evidence)
			}
			shown := len(r.Violations)
			if *maxViol > 0 && shown > *maxViol {
				shown = *maxViol
			}
			for _, v := range r.Violations[:shown] {
				fmt.Printf("        - %s\n", v)
			}
			if shown < len(r.Violations) {
				fmt.Printf("        … %d more\n", len(r.Violations)-shown)
			}
		}
	}

	exit := 0
	for _, r := range results {
		if r.State == Fail || (*strict && r.State == Skip) {
			exit = 1
		}
	}
	return exit
}

func printCatalogue(w *os.File) {
	fmt.Fprintln(w, "checker catalogue (PLAN_RAFT.md §13.7)")
	fmt.Fprintln(w, "  [x] implemented, [ ] documented stub")
	for _, c := range Checks {
		mark := " "
		if c.Implemented {
			mark = "x"
		}
		fmt.Fprintf(w, "  [%s] %-26s %s\n", mark, c.ID, c.Title)
		if c.Note != "" {
			fmt.Fprintf(w, "      owes: %s\n", c.Note)
		}
	}
}

const usage = `checker — run-log invariants for the raft storage class (PLAN_RAFT.md §13.7)

Reads ONE run log (JSONL, written by difffuzz / crash / kill / flatness — the
format is documented in log.go) and judges the invariants over it. Three states:
PASS, FAIL and SKIP; a SKIP always says what the log did not contain.

  checker -log run.jsonl
  checker -log run.jsonl -check delivery-at-least-once -json
  checker -list-checks

Flags:
`
