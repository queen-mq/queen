package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

// What `queenctl apply -f` puts on the wire (PLAN_DASHBOARD_ACTIONS.md §2.2).
//
// The decoding half is pinned in apply_test.go; this file is the half that
// actually reaches a broker, and the two are not the same claim. A manifest key
// can bind perfectly and still be dropped between the decoder and the request:
// that is exactly what the SDK's QueueConfig bag did to every false and every
// zero, and under `mode: replace` a dropped key is the broker's default written
// over the operator's file, reported as `applied`.
//
// The rule, in one sentence: the wire carries the manifest's keys, all of them,
// with the values the manifest wrote, and nothing else.

// applyManifest runs `queenctl apply -f <tmpfile>` on src against server.
//
// The two flag variables are package state that cobra does not reset between
// Execute calls, so a --dry-run here would otherwise make every later apply in
// this process send nothing.
func applyManifest(t *testing.T, server, src string, extra ...string) error {
	t.Helper()
	t.Cleanup(func() { applyFile = ""; applyDryRun = false })
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return runQueenctl(t, server, append([]string{"apply", "-f", path}, extra...)...)
}

// A manifest is a replacement, and it says so on the wire. Without this key the
// broker merges, every absent option keeps whatever the queue happened to hold,
// and the file stops being the queue's description.
func TestApplySendsModeReplace(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL, "kind: Queue\nname: orders\nconfig:\n  leaseTime: 60\n"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(*bodies) != 1 {
		t.Fatalf("expected one configure call, got %d: %+v", len(*bodies), *bodies)
	}
	if got := (*bodies)[0]["mode"]; got != "replace" {
		t.Fatalf("mode is %v, want \"replace\": %+v", got, (*bodies)[0])
	}
	if got := (*bodies)[0]["queue"]; got != "orders" {
		t.Fatalf("queue is %v, want \"orders\"", got)
	}
}

// THE FIX. Every one of these lines used to reach the broker as an absent key,
// which under `replace` is the default: dead-lettering back ON for a queue whose
// manifest disables it, the lease back to 300s, retention back off. The
// manifest said the opposite in writing each time.
func TestApplySendsAnExplicitFalseAndZero(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL, `kind: Queue
name: orders
config:
  leaseTime: 0
  maxSize: 0
  retentionSeconds: 0
  retentionEnabled: false
  deadLetterQueue: false
  dlqAfterMaxRetries: false
  encryptionEnabled: false
`); err != nil {
		t.Fatalf("apply: %v", err)
	}

	opts := onlyOptions(t, bodies)
	for _, key := range []string{"leaseTime", "maxSize", "retentionSeconds"} {
		v, present := opts[key]
		if !present {
			t.Errorf("%s was not sent, so `replace` lands it on the default: %+v", key, opts)
			continue
		}
		// JSON numbers arrive as float64; a literal zero is the claim.
		if v != float64(0) {
			t.Errorf("%s is %v (%T), want a literal 0", key, v, v)
		}
	}
	for _, key := range []string{
		"retentionEnabled", "deadLetterQueue", "dlqAfterMaxRetries", "encryptionEnabled",
	} {
		v, present := opts[key]
		if !present {
			t.Errorf("%s was not sent, so the queue keeps the default (ON for the DLQ pair): %+v", key, opts)
			continue
		}
		if v != false {
			t.Errorf("%s is %v (%T), want a literal false", key, v, v)
		}
	}
	if len(opts) != 7 {
		t.Fatalf("a seven-key manifest sent %d options: %+v", len(opts), opts)
	}
}

// The inverse, and what makes `replace` safe to send: a key the manifest does
// not carry must not appear on the wire either. If the CLI padded the bag with
// zero values, `replace` would still reset the option, but now with the CLI's
// fingerprints on it rather than the broker's defaults.
func TestApplySendsOnlyTheKeysTheManifestCarries(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL, `kind: Queue
name: orders
namespace: billing
task: ingest
config:
  leaseTime: 60
  retryLimit: 5
`); err != nil {
		t.Fatalf("apply: %v", err)
	}

	opts := onlyOptions(t, bodies)
	if opts["leaseTime"] != float64(60) || opts["retryLimit"] != float64(5) {
		t.Fatalf("the two keys the manifest wrote did not arrive: %+v", opts)
	}
	for _, key := range []string{
		"priority", "delayedProcessing", "windowBuffer", "maxSize",
		"retentionSeconds", "completedRetentionSeconds", "retentionEnabled",
		"deadLetterQueue", "dlqAfterMaxRetries", "encryptionEnabled",
	} {
		if _, present := opts[key]; present {
			t.Errorf("%s was sent by a manifest that never wrote it: %+v", key, opts)
		}
	}
	if len(opts) != 2 {
		t.Fatalf("a two-key manifest sent %d options: %+v", len(opts), opts)
	}
	// namespace and task ride at the top level, where the broker folds them
	// into the same bag.
	body := (*bodies)[0]
	if body["namespace"] != "billing" || body["task"] != "ingest" {
		t.Errorf("namespace/task lost: %+v", body)
	}
}

// A manifest with no config block at all is still a replacement: the queue is
// described as "all defaults", and that is a legitimate thing to write down.
func TestApplyWithoutAConfigBlockStillReplaces(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL, "kind: Queue\nname: orders\n"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(*bodies) != 1 {
		t.Fatalf("expected one configure call, got %d", len(*bodies))
	}
	if (*bodies)[0]["mode"] != "replace" {
		t.Errorf("mode lost on a config-less document: %+v", (*bodies)[0])
	}
	if opts, present := (*bodies)[0]["options"]; present {
		if m, _ := opts.(map[string]any); len(m) != 0 {
			t.Errorf("a config-less document sent options: %+v", m)
		}
	}
}

// An unknown key fails the file, and it fails it BEFORE anything is sent: the
// document is refused as a whole, so a typo on line 9 cannot leave the queue
// half-applied from lines 1 to 8.
func TestApplyRefusesAnUnknownKeyWithoutSendingAnything(t *testing.T) {
	for _, src := range []string{
		"kind: Queue\nname: orders\nconfig:\n  leasetime: 60\n",
		"kind: Queue\nname: orders\nconfig:\n  deadLetter: false\n",
		"kind: Queue\nname: orders\nretries: 3\n",
	} {
		srv, bodies := captureConfigure(t)
		err := applyManifest(t, srv.URL, src)
		if err == nil {
			t.Errorf("manifest %q was applied", src)
		}
		if len(*bodies) != 0 {
			t.Errorf("manifest %q reached the broker anyway: %+v", src, *bodies)
		}
	}
}

// Multiple documents in one stream: the second queue's keys must not inherit
// anything from the first. The option bag is per document, and a builder reused
// across documents would carry the first one's options into the second one's
// replacement.
func TestApplyDoesNotCarryOptionsBetweenDocuments(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL, `kind: Queue
name: orders
config:
  deadLetterQueue: false
  leaseTime: 30
---
kind: Queue
name: shipments
config:
  leaseTime: 60
`); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(*bodies) != 2 {
		t.Fatalf("expected two configure calls, got %d: %+v", len(*bodies), *bodies)
	}
	second, _ := (*bodies)[1]["options"].(map[string]any)
	if len(second) != 1 || second["leaseTime"] != float64(60) {
		t.Fatalf("the second document sent %+v, want only leaseTime 60", second)
	}
}

// --dry-run parses and validates, and sends nothing. It is also where a bad
// manifest is meant to be caught, so the config block has to be validated on
// this path too.
func TestApplyDryRunValidatesAndSendsNothing(t *testing.T) {
	srv, bodies := captureConfigure(t)
	if err := applyManifest(t, srv.URL,
		"kind: Queue\nname: orders\nconfig:\n  leaseTime: 60\n", "--dry-run"); err != nil {
		t.Fatalf("apply --dry-run: %v", err)
	}
	if len(*bodies) != 0 {
		t.Fatalf("--dry-run sent %d configure calls: %+v", len(*bodies), *bodies)
	}

	srv2, bodies2 := captureConfigure(t)
	if err := applyManifest(t, srv2.URL,
		"kind: Queue\nname: orders\nconfig:\n  leasetime: 60\n", "--dry-run"); err == nil {
		t.Errorf("--dry-run accepted a key apply would refuse")
	}
	if len(*bodies2) != 0 {
		t.Errorf("--dry-run sent something: %+v", *bodies2)
	}
}
