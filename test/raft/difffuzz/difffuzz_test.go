package main

// `GOWORK=off go test ./...` runs the same checks as `-selftest`, one per test
// so a failure names itself, plus the flag-surface tests that only make sense
// from a test binary.

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestGeneratorIsSeeded(t *testing.T) {
	if err := checkGeneratorIsSeeded(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizerKeepsIdIdentity(t *testing.T) {
	if err := checkNormalizerIdentity(); err != nil {
		t.Fatal(err)
	}
}

func TestComparisonNamesTheFirstDifference(t *testing.T) {
	if err := checkFirstDiffPath(); err != nil {
		t.Fatal(err)
	}
}

func TestMixParsing(t *testing.T) {
	if err := checkMixParsing(); err != nil {
		t.Fatal(err)
	}
}

func TestHonestSidesDoNotDiverge(t *testing.T) {
	if err := checkHonestRun(); err != nil {
		t.Fatal(err)
	}
}

func TestChangedFieldIsCaught(t *testing.T) {
	if err := checkDishonestRun(); err != nil {
		t.Fatal(err)
	}
}

func TestParseFlagsRefusesOneBrokerTwice(t *testing.T) {
	_, err := ParseFlags([]string{"-a", "http://x:1", "-b", "http://x:1"}, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "same broker") {
		t.Fatalf("want a refusal naming the same broker, got %v", err)
	}
}

func TestParseFlagsDefaultsAreReplayable(t *testing.T) {
	cfg, err := ParseFlags([]string{"-seed", "123", "-ops", "10"}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Seed != 123 || cfg.Ops != 10 {
		t.Fatalf("flags not applied: %+v", cfg)
	}
	if cfg.RunID == "" {
		t.Fatal("run id is empty: a fixture could not be replayed")
	}
	cmd := cfg.ReplayCommand()
	for _, want := range []string{"-seed 123", "-ops 10", "-run-id " + cfg.RunID, "-mix "} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("replay command %q does not carry %q", cmd, want)
		}
	}
}

func TestListOpsCoversEveryPlannedKind(t *testing.T) {
	// §13.4 names these; the catalogue must declare every one of them, either
	// implemented or as a documented stub.
	for _, k := range []OpKind{OpPush, OpPop, OpAck, OpRenew, OpNack, OpTxn, OpKV, OpTimer,
		OpConfigure, OpSeek, OpGroupDelete, OpDLQMove, OpDLQPurge, OpPopAuto, OpPopDiscover, OpAckByHash} {
		if !KnownKind(k) {
			t.Errorf("operation kind %q from §13.4 is not in the catalogue", k)
		}
		if kinds[k].note == "" {
			t.Errorf("operation kind %q has no note: -list-ops would print a blank line", k)
		}
	}
}

func TestReportRendersAndWrites(t *testing.T) {
	cfg, err := ParseFlags([]string{"-seed", "7", "-ops", "3", "-out", t.TempDir()}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(cfg, io.Discard)
	rep := r.report()
	rep.Divergences = append(rep.Divergences, Divergence{
		Op: 1, Kind: "pop", What: "body", Path: "$.messages[0].offset", A: "1", B: "2", Request: "pop q",
	})
	text := rep.Text()
	for _, want := range []string{"seed=7", "$.messages[0].offset", "replay:"} {
		if !strings.Contains(text, want) {
			t.Errorf("the report does not carry %q:\n%s", want, text)
		}
	}
	if !json.Valid(rep.JSON()) {
		t.Fatalf("the JSON report is not valid JSON: %s", rep.JSON())
	}
	if err := writeReport(cfg, rep); err != nil {
		t.Fatalf("writing the report: %v", err)
	}
	entries, err := os.ReadDir(cfg.OutDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one report file in %s, got %v (%v)", cfg.OutDir, entries, err)
	}
}
