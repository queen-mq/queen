package main

import (
	"fmt"
	"reflect"
	"sort"
)

// The suite contract's output, and nothing else on stdout: one `ok NAME` or
// `FAIL NAME: detail` per assertion, `#`-prefixed observations that are not
// verdicts, and one `RESULT:` line at the end.
//
// The counters are package state rather than a field on [rig] for the same
// reason the python suites use module globals: an assertion helper that had to
// be handed the run would be threaded through every call in the file, and there
// is exactly one run per process.
var (
	passes   int
	failures []string
)

func ok(name string) {
	passes++
	fmt.Printf("ok %s\n", name)
}

func fail(name, detail string) {
	failures = append(failures, name)
	fmt.Printf("FAIL %s: %s\n", name, detail)
}

// note is an observation that is not a verdict: this facade's behaviour is
// defensible and only a run against real AWS settles whether it is right. Same
// device as `smoke_m4_sns.py`'s `note`, and it never touches the count.
func note(format string, args ...any) {
	fmt.Printf("# note %s\n", fmt.Sprintf(format, args...))
}

// info is a measurement worth reading beside the verdicts — a count, a
// duration — and, like note, is not one.
func info(format string, args ...any) {
	fmt.Printf("#   %s\n", fmt.Sprintf(format, args...))
}

func check(name string, condition bool, detail string) bool {
	if condition {
		ok(name)
		return true
	}
	if detail == "" {
		detail = "condition was false"
	}
	fail(name, detail)
	return false
}

// checkEq compares with reflect.DeepEqual so that slices and maps are one
// assertion rather than a loop of them, and prints both sides when they differ.
func checkEq[T any](name string, got, want T) bool {
	if reflect.DeepEqual(got, want) {
		ok(name)
		return true
	}
	fail(name, fmt.Sprintf("got %#v, want %#v", got, want))
	return false
}

// checkNoErr is the shape most calls need: an action that must simply work.
func checkNoErr(name string, err error) bool {
	return check(name, err == nil, errText(err))
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// report prints the protocol line, the tally and the verdict, and answers the
// process's exit status.
func report() int {
	for _, line := range recorded.lines() {
		fmt.Printf("# protocol spoken: %s\n", line)
	}
	fmt.Printf("# %d passed, %d failed\n", passes, len(failures))
	sorted := append([]string(nil), failures...)
	sort.Strings(sorted)
	for _, name := range sorted {
		fmt.Printf("#   failed: %s\n", name)
	}
	if len(failures) > 0 {
		fmt.Println("RESULT: FAIL")
		return 1
	}
	fmt.Println("RESULT: PASS")
	return 0
}
