package cmd

import (
	"bytes"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// What `queenctl apply -f` actually binds out of a manifest
// (PLAN_DASHBOARD_ACTIONS.md §2.2).
//
// WHY THIS IS WORTH A TEST. `apply` sends `mode: replace`: the document is the
// whole configuration of the queue it names, so an option the file does not
// carry goes back to its default. That is the right contract for a manifest —
// and it makes every key that fails to bind, or binds and then does not reach
// the wire, a silent RESET reported as `applied`. Two rounds of exactly that
// bug live here: first the `config` keys had no yaml names at all, so yaml.v3
// matched the lowercased field name (`leasetime`) and every key in this
// command's own help example bound nothing; then they bound into a
// queen.QueueConfig, whose ints and bools cannot tell `deadLetterQueue: false`
// from a key nobody wrote, so the SDK dropped it and the broker's default
// (dead-lettering ON) won.
//
// The decoder is the one apply.go uses, on the document apply.go prints. What
// the decoded block puts on the WIRE is pinned next door, in apply_wire_test.go.

// The manifest from applyCmd's Long help, verbatim.
const helpManifest = `kind: Queue
name: orders
namespace: billing
task: ingest
config:
  leaseTime: 60
  retryLimit: 5
  maxSize: 100000
  deadLetterQueue: false
`

func decodeManifest(t *testing.T, src string) applyDoc {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader([]byte(src)))
	dec.KnownFields(true)
	var doc applyDoc
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("decode %q: %v", src, err)
	}
	return doc
}

// configOf returns the option bag a manifest's `config:` block produced, and
// fails if the block bound nothing at all.
func configOf(t *testing.T, src string) map[string]interface{} {
	t.Helper()
	doc := decodeManifest(t, src)
	if doc.Config == nil {
		t.Fatal("the config block bound nothing at all")
	}
	if len(doc.Config.keys) != len(doc.Config.opts) {
		t.Fatalf("key order list and option bag disagree: %v vs %+v",
			doc.Config.keys, doc.Config.opts)
	}
	return doc.Config.opts
}

// assertOptions compares the bag key for key, including which keys are absent:
// under `replace` an extra key is an edit nobody asked for and a missing one is
// a reset, so "close enough" is not a pass.
func assertOptions(t *testing.T, got map[string]interface{}, want map[string]interface{}) {
	t.Helper()
	for k, w := range want {
		g, present := got[k]
		if !present {
			t.Errorf("%s did not bind, so `replace` puts it back to the default: %+v", k, got)
			continue
		}
		if g != w {
			t.Errorf("%s bound to %v (%T), want %v (%T)", k, g, g, w, w)
		}
	}
	for k := range got {
		if _, expected := want[k]; !expected {
			t.Errorf("%s was bound but the manifest never wrote it: %+v", k, got)
		}
	}
}

func TestApplyBindsTheManifestItsHelpPrints(t *testing.T) {
	doc := decodeManifest(t, helpManifest)

	if doc.Kind != "Queue" || doc.Name != "orders" ||
		doc.Namespace != "billing" || doc.Task != "ingest" {
		t.Fatalf("identity lost: %+v", doc)
	}
	if doc.Config == nil {
		t.Fatal("the config block bound nothing at all")
	}
	assertOptions(t, doc.Config.opts, map[string]interface{}{
		"leaseTime": 60, "retryLimit": 5, "maxSize": 100000,
		"deadLetterQueue": false,
	})

	// And it is genuinely the help's manifest: the example is indented two
	// spaces inside the heredoc, and is the only documentation most people will
	// read before writing a file that replaces a live queue's configuration.
	for _, line := range strings.Split(strings.TrimSpace(helpManifest), "\n") {
		if !strings.Contains(applyCmd.Long, "  "+line+"\n") {
			t.Errorf("the help example no longer prints %q", line)
		}
	}
}

// Every field of QueueConfig, in the camelCase the API and the help both use.
// A field added to the SDK without a yaml tag must not be able to slip past
// this: the vocabulary apply accepts IS QueueConfig's tagged field set, read
// off the struct at run time, so a missing tag turns the key an operator writes
// into a parse error rather than a silent default.
func TestApplyBindsEveryQueueConfigField(t *testing.T) {
	got := configOf(t, `kind: Queue
name: orders
config:
  leaseTime: 60
  retryLimit: 5
  priority: 7
  delayedProcessing: 30
  windowBuffer: 15
  maxSize: 100000
  retentionSeconds: 86400
  completedRetentionSeconds: 3600
  retentionEnabled: true
  deadLetterQueue: true
  dlqAfterMaxRetries: true
  encryptionEnabled: true
`)
	assertOptions(t, got, map[string]interface{}{
		"leaseTime": 60, "retryLimit": 5, "priority": 7, "delayedProcessing": 30,
		"windowBuffer": 15, "maxSize": 100000, "retentionSeconds": 86400,
		"completedRetentionSeconds": 3600, "retentionEnabled": true,
		"deadLetterQueue": true, "dlqAfterMaxRetries": true, "encryptionEnabled": true,
	})
	if n := len(queueConfigKeys()); n != len(got) {
		t.Fatalf("QueueConfig exposes %d manifest keys, this document writes %d; extend it", n, len(got))
	}
}

// THE BUG THIS FILE IS NAMED AFTER. A false and a zero are values, not absences:
// the manifest wrote them, so they are what the queue must end up with.
// Everything about the old path was type-driven — QueueConfig's `bool` cannot
// hold "unset" — and every one of these lines silently became its opposite.
func TestApplyKeepsAFalseAndAZero(t *testing.T) {
	got := configOf(t, `kind: Queue
name: orders
config:
  leaseTime: 0
  maxSize: 0
  retentionSeconds: 0
  retentionEnabled: false
  deadLetterQueue: false
  dlqAfterMaxRetries: false
  encryptionEnabled: false
`)
	assertOptions(t, got, map[string]interface{}{
		"leaseTime": 0, "maxSize": 0, "retentionSeconds": 0,
		"retentionEnabled": false, "deadLetterQueue": false,
		"dlqAfterMaxRetries": false, "encryptionEnabled": false,
	})
}

// The other half of the same contract: a key the manifest does NOT write must
// not be invented on its way through. Absence is how `replace` is told to put
// an option back to its default, so a bag padded with zero values would make
// every manifest describe all twelve options whatever it said.
func TestApplyBindsOnlyTheKeysTheManifestWrites(t *testing.T) {
	got := configOf(t, "kind: Queue\nname: orders\nconfig:\n  leaseTime: 60\n")
	assertOptions(t, got, map[string]interface{}{"leaseTime": 60})
}

// An explicit null is the broker's own "restore this option's default", and it
// is the one value a typed field cannot carry: decoded into an int it would
// arrive as a literal zero, which now means something else entirely.
func TestApplyBindsAnExplicitNullAsNull(t *testing.T) {
	got := configOf(t, "kind: Queue\nname: orders\nconfig:\n  leaseTime: null\n")
	v, present := got["leaseTime"]
	if !present {
		t.Fatalf("an explicit null dropped the key: %+v", got)
	}
	if v != nil {
		t.Fatalf("leaseTime bound to %v (%T), want a null", v, v)
	}
}

// An empty block is a manifest that asks for the defaults, and says so.
func TestApplyAcceptsAnEmptyConfigBlock(t *testing.T) {
	doc := decodeManifest(t, "kind: Queue\nname: orders\nconfig: {}\n")
	if doc.Config == nil {
		t.Fatal("an empty config block should still bind a block")
	}
	if len(doc.Config.opts) != 0 {
		t.Fatalf("an empty config block bound %+v", doc.Config.opts)
	}
}

// A key this CLI cannot bind is an error, not a shrug: under `mode: replace` an
// ignored key is an option reset to its default while the command prints
// "applied". `leasetime` — the spelling the untagged struct used to accept — is
// the one that matters most here, because manifests written against the old
// behaviour are exactly the files that would otherwise reset a live queue.
func TestApplyRefusesAKeyItCannotBind(t *testing.T) {
	for _, src := range []string{
		"kind: Queue\nname: orders\nconfig:\n  leasetime: 60\n",
		"kind: Queue\nname: orders\nconfig:\n  leaseTimeSeconds: 60\n",
		"kind: Queue\nname: orders\nretries: 3\n",
	} {
		dec := yaml.NewDecoder(bytes.NewReader([]byte(src)))
		dec.KnownFields(true)
		var doc applyDoc
		err := dec.Decode(&doc)
		if err == nil {
			t.Fatalf("manifest %q was accepted; it binds %+v", src, doc.Config)
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Fatalf("manifest %q: unexpected error %v", src, err)
		}
	}
}

// Two more ways a config block can be wrong without being unknown. A value of
// the wrong type would otherwise reach the broker as a string it parses to
// NULL, i.e. the default; a key written twice is an option whose value depends
// on which line the reader's eye landed on.
func TestApplyRefusesAMalformedConfigBlock(t *testing.T) {
	for _, tc := range []struct{ src, want string }{
		{"kind: Queue\nname: orders\nconfig:\n  leaseTime: sixty\n", "leaseTime"},
		{"kind: Queue\nname: orders\nconfig:\n  leaseTime: 60\n  leaseTime: 90\n", "twice"},
		{"kind: Queue\nname: orders\nconfig:\n  - leaseTime\n", "mapping"},
	} {
		dec := yaml.NewDecoder(bytes.NewReader([]byte(tc.src)))
		dec.KnownFields(true)
		var doc applyDoc
		err := dec.Decode(&doc)
		if err == nil {
			t.Errorf("manifest %q was accepted; it binds %+v", tc.src, doc.Config)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("manifest %q: error %v does not mention %q", tc.src, err, tc.want)
		}
	}
}

// The ConsumerGroup half of the same stream still decodes with KnownFields on —
// its hyphenated key has a tag of its own and must not become a parse error.
func TestApplyStillBindsAConsumerGroupDocument(t *testing.T) {
	doc := decodeManifest(t, "kind: ConsumerGroup\nname: analyzer\nqueue: orders\nseek-to: beginning\n")
	if doc.Kind != "ConsumerGroup" || doc.Queue != "orders" || doc.SeekTo != "beginning" {
		t.Fatalf("consumer group document bound to %+v", doc)
	}
}
