// M7 F4: the write half of the config surface against a running facade —
// AlterConfigs (33) and IncrementalAlterConfigs (44), driven by franz-go's
// `kmsg` at the versions a real AdminClient negotiates.
//
// The claim under test is the RETENTION ROUND TRIP, which did not exist before
// F4: `kafka-configs.sh --entity-type topics --describe` used to show a blank
// where the retention it had just set should be, because Queen exposes no HTTP
// read of a queue's config columns. It round-trips now out of the facade's own
// record of the options bag it posted (src/topic_record.rs), and these tests
// walk exactly the sequence the tool walks: describe, alter, describe, delete,
// describe.
//
// Key 44 is the one that matters. `kafka-configs.sh --alter` has sent
// IncrementalAlterConfigs since Kafka 2.3 and 3.9's ConfigCommand has no
// fallback, so TestIncrementalAlterConfigsRoundTripsRetention is this stage's
// acceptance. Key 33 is here because the deprecated shape must still decode and
// because its FULL-REPLACEMENT semantics are honoured literally.
//
// Every assertion is about something a client acts on: an error code it branches
// on, a config value it renders, the read_only flag an edit control reads.
package compat

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The versions an AdminClient negotiates against the facade's advertised window
// (versions.rs: AlterConfigs 0-2, IncrementalAlterConfigs 0-1).
const (
	alterConfigsV            = 2
	incrementalAlterConfigsV = 1
)

// Kafka's AlterConfigOp.OpType.
const (
	opSet      int8 = 0
	opDelete   int8 = 1
	opAppend   int8 = 2
	opSubtract int8 = 3
)

// incrementalAlter sends one resource's worth of operations and returns the one
// resource response. `validateOnly` is the flag the tool exposes as
// `--dry-run`.
func incrementalAlter(t *testing.T, cl *kgo.Client, kind int8, name string, validateOnly bool, ops ...kmsg.IncrementalAlterConfigsRequestResourceConfig) kmsg.IncrementalAlterConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.SetVersion(incrementalAlterConfigsV)
	req.ValidateOnly = validateOnly
	r := kmsg.NewIncrementalAlterConfigsRequestResource()
	r.ResourceType = kmsg.ConfigResourceType(kind)
	r.ResourceName = name
	r.Configs = ops
	req.Resources = append(req.Resources, r)

	// Pinned to broker 0 rather than routed by franz-go: for a BROKER resource
	// the client routes to the broker the resource NAMES, so a request meant to
	// be REFUSED for naming another node never leaves the client. The refusal is
	// the broker's to make, so the request has to reach one.
	raw, err := cl.Broker(0).Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs(%d,%q): %v", kind, name, err)
	}
	resp, ok := raw.(*kmsg.IncrementalAlterConfigsResponse)
	if !ok {
		t.Fatalf("IncrementalAlterConfigs(%d,%q): unexpected response type %T", kind, name, raw)
	}
	if len(resp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs(%d,%q): %d resources, want 1", kind, name, len(resp.Resources))
	}
	return resp.Resources[0]
}

func incOp(name string, operation int8, value *string) kmsg.IncrementalAlterConfigsRequestResourceConfig {
	c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	c.Name = name
	c.Op = kmsg.IncrementalAlterConfigOp(operation)
	c.Value = value
	return c
}

// alterConfigs sends the deprecated full-replacement form.
func alterConfigs(t *testing.T, cl *kgo.Client, kind int8, name string, validateOnly bool, configs ...kmsg.AlterConfigsRequestResourceConfig) kmsg.AlterConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrAlterConfigsRequest()
	req.SetVersion(alterConfigsV)
	req.ValidateOnly = validateOnly
	r := kmsg.NewAlterConfigsRequestResource()
	r.ResourceType = kmsg.ConfigResourceType(kind)
	r.ResourceName = name
	r.Configs = configs
	req.Resources = append(req.Resources, r)

	raw, err := cl.Broker(0).Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("AlterConfigs(%d,%q): %v", kind, name, err)
	}
	resp, ok := raw.(*kmsg.AlterConfigsResponse)
	if !ok {
		t.Fatalf("AlterConfigs(%d,%q): unexpected response type %T", kind, name, raw)
	}
	if len(resp.Resources) != 1 {
		t.Fatalf("AlterConfigs(%d,%q): %d resources, want 1", kind, name, len(resp.Resources))
	}
	return resp.Resources[0]
}

func alterConfig(name string, value *string) kmsg.AlterConfigsRequestResourceConfig {
	c := kmsg.NewAlterConfigsRequestResourceConfig()
	c.Name = name
	c.Value = value
	return c
}

func str(s string) *string { return &s }

// retentionOf reads the value and the read_only flag of `retention.ms`, plus
// whether it is reported at all. All three are things a settings tab renders.
func retentionOf(t *testing.T, cl *kgo.Client, topic string) (value string, source kmsg.ConfigSource, readOnly, present bool) {
	t.Helper()
	r := describeConfigs(t, cl, resourceTopic, topic)
	if r.ErrorCode != errNone {
		t.Fatalf("describe %s: error code %d (%v)", topic, r.ErrorCode, r.ErrorMessage)
	}
	for _, c := range r.Configs {
		if c.Name == "retention.ms" {
			v := ""
			if c.Value != nil {
				v = *c.Value
			}
			return v, c.Source, c.ReadOnly, true
		}
	}
	return "", 0, false, false
}

// TestIncrementalAlterConfigsRoundTripsRetention is THE acceptance for key 44,
// and it is the sequence `kafka-configs.sh` walks. Before M7 F4 the retention
// line was absent from all three describes.
func TestIncrementalAlterConfigsRoundTripsRetention(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	// 1. A topic this facade created, with no retention: Queen's default is
	//    retention OFF, which IS Kafka's -1, and it is a DEFAULT because nobody
	//    set it.
	v, source, readOnly, present := retentionOf(t, cl, topic)
	if !present || v != "-1" {
		t.Fatalf("first describe: retention.ms = %q (present=%v), want -1", v, present)
	}
	if source != kmsg.ConfigSourceDefaultConfig {
		t.Fatalf("first describe: retention.ms source = %v, want DEFAULT_CONFIG", source)
	}
	if readOnly {
		t.Fatal("retention.ms is reported read-only on a topic this facade tracks")
	}

	// 2. The alter the tool sends.
	got := incrementalAlter(t, cl, resourceTopic, topic, false,
		incOp("retention.ms", opSet, str("604800000")))
	if got.ErrorCode != errNone {
		t.Fatalf("alter: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}

	// 3. ...and it reads back, sourced TOPIC because somebody set it.
	v, source, readOnly, present = retentionOf(t, cl, topic)
	if !present || v != "604800000" {
		t.Fatalf("second describe: retention.ms = %q (present=%v), want 604800000", v, present)
	}
	if source != kmsg.ConfigSourceDynamicTopicConfig {
		t.Fatalf("second describe: retention.ms source = %v, want DYNAMIC_TOPIC_CONFIG", source)
	}
	if readOnly {
		t.Fatal("a retention this call just set is reported read-only")
	}

	// 4. `--delete-config retention.ms` resets it to Queen's default, which is
	//    back to -1. The value is ignored for a DELETE, and this sends one.
	got = incrementalAlter(t, cl, resourceTopic, topic, false,
		incOp("retention.ms", opDelete, str("ignored")))
	if got.ErrorCode != errNone {
		t.Fatalf("delete-config: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	v, source, _, present = retentionOf(t, cl, topic)
	if !present || v != "-1" {
		t.Fatalf("third describe: retention.ms = %q (present=%v), want -1", v, present)
	}
	if source != kmsg.ConfigSourceDefaultConfig {
		t.Fatalf("third describe: retention.ms source = %v, want DEFAULT_CONFIG", source)
	}
}

// A sub-second retention is refused rather than rounded to zero seconds, which
// would mean "delete everything"; an unknown key is refused by name rather than
// dropped; and compaction is refused loudly, which is what stops Kafka Connect
// at startup instead of losing its config topic on a later restart.
func TestIncrementalAlterConfigsRefusals(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	for _, tc := range []struct {
		what string
		op   kmsg.IncrementalAlterConfigsRequestResourceConfig
		code int16
	}{
		{"a sub-second retention", incOp("retention.ms", opSet, str("500")), errInvalidConfig},
		{"a retention that is not a number", incOp("retention.ms", opSet, str("later")), errInvalidConfig},
		{"an unknown key", incOp("segment.bytes", opSet, str("1073741824")), errInvalidConfig},
		{"compaction", incOp("cleanup.policy", opSet, str("compact")), errInvalidConfig},
		{"append on a scalar", incOp("retention.ms", opAppend, str("1000")), errInvalidConfig},
		{"subtracting the only cleanup policy", incOp("cleanup.policy", opSubtract, str("delete")), errInvalidConfig},
		{"an operation that is not one of the four", incOp("retention.ms", 9, str("-1")), errInvalidRequest},
	} {
		got := incrementalAlter(t, cl, resourceTopic, topic, false, tc.op)
		if got.ErrorCode != tc.code {
			t.Fatalf("%s: error code %d (%v), want %d", tc.what, got.ErrorCode, got.ErrorMessage, tc.code)
		}
	}

	// ...and none of it changed anything.
	if v, _, _, present := retentionOf(t, cl, topic); !present || v != "-1" {
		t.Fatalf("a refused alter changed the retention to %q (present=%v)", v, present)
	}

	// Setting min.insync.replicas to the value the broker itself reports is a
	// no-op, not an unknown key. That asymmetry was real before F4 and it bit
	// exactly the obvious command.
	if got := incrementalAlter(t, cl, resourceTopic, topic, false,
		incOp("min.insync.replicas", opSet, str("1"))); got.ErrorCode != errNone {
		t.Fatalf("min.insync.replicas=1: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	if got := incrementalAlter(t, cl, resourceTopic, topic, false,
		incOp("min.insync.replicas", opSet, str("2"))); got.ErrorCode != errInvalidConfig {
		t.Fatalf("min.insync.replicas=2: error code %d (%v), want %d", got.ErrorCode, got.ErrorMessage, errInvalidConfig)
	}
}

// `--dry-run`: everything is computed, the response is fully formed, and the
// following describe is unchanged.
func TestIncrementalAlterConfigsValidateOnlyWritesNothing(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	got := incrementalAlter(t, cl, resourceTopic, topic, true,
		incOp("retention.ms", opSet, str("604800000")))
	if got.ErrorCode != errNone {
		t.Fatalf("validate-only alter: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	if v, _, _, present := retentionOf(t, cl, topic); !present || v != "-1" {
		t.Fatalf("a validate-only alter wrote: retention.ms = %q (present=%v)", v, present)
	}

	// ...and a validate-only alter that would have been refused still says so.
	if got := incrementalAlter(t, cl, resourceTopic, topic, true,
		incOp("segment.bytes", opSet, str("1"))); got.ErrorCode != errInvalidConfig {
		t.Fatalf("validate-only refusal: error code %d, want %d", got.ErrorCode, errInvalidConfig)
	}
}

// The BROKER resource is always a refusal, and the message names where the knob
// actually is. The name rule is DescribeConfigs': “ or this node's id.
func TestAlterConfigsRefusesTheBrokerResource(t *testing.T) {
	cl := newClient(t)

	for _, name := range []string{"", "0"} {
		got := incrementalAlter(t, cl, resourceBroker, name, false,
			incOp("num.partitions", opSet, str("8")))
		if got.ErrorCode != errInvalidConfig {
			t.Fatalf("broker %q: error code %d (%v), want %d", name, got.ErrorCode, got.ErrorMessage, errInvalidConfig)
		}
	}
	// Another node's id is INVALID_REQUEST, exactly as DescribeConfigs answers
	// it: this process cannot alter another node's running configuration.
	got := incrementalAlter(t, cl, resourceBroker, "7", false,
		incOp("num.partitions", opSet, str("8")))
	if got.ErrorCode != errInvalidRequest {
		t.Fatalf("broker 7: error code %d (%v), want %d", got.ErrorCode, got.ErrorMessage, errInvalidRequest)
	}
}

// A topic that is not there is UNKNOWN, not INVALID_CONFIG: a client can tell
// "there is nothing to change" from "you cannot change this".
func TestAlterConfigsOnAnUnknownTopic(t *testing.T) {
	cl := newClient(t)
	got := incrementalAlter(t, cl, resourceTopic, newTopic(t), false,
		incOp("retention.ms", opSet, str("-1")))
	if got.ErrorCode != errUnknownTopicOrPartition {
		t.Fatalf("unknown topic: error code %d (%v), want %d", got.ErrorCode, got.ErrorMessage, errUnknownTopicOrPartition)
	}
}

// The deprecated key 33 decodes, and its FULL-REPLACEMENT semantics are honoured
// literally: a request naming only `cleanup.policy` resets `retention.ms`,
// because retention is a key it did not name. That is what a real broker does
// with this key and it is why the docs say to prefer key 44.
func TestAlterConfigsIsFullReplacement(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	if got := incrementalAlter(t, cl, resourceTopic, topic, false,
		incOp("retention.ms", opSet, str("604800000"))); got.ErrorCode != errNone {
		t.Fatalf("setup alter: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	if v, _, _, _ := retentionOf(t, cl, topic); v != "604800000" {
		t.Fatalf("setup: retention.ms = %q, want 604800000", v)
	}

	got := alterConfigs(t, cl, resourceTopic, topic, false,
		alterConfig("cleanup.policy", str("delete")))
	if got.ErrorCode != errNone {
		t.Fatalf("full replacement: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	v, source, _, present := retentionOf(t, cl, topic)
	if !present || v != "-1" {
		t.Fatalf("after full replacement: retention.ms = %q (present=%v), want -1", v, present)
	}
	if source != kmsg.ConfigSourceDefaultConfig {
		t.Fatalf("after full replacement: source = %v, want DEFAULT_CONFIG", source)
	}

	// ...and the deprecated key refuses what the incremental one refuses.
	if got := alterConfigs(t, cl, resourceTopic, topic, false,
		alterConfig("cleanup.policy", str("compact"))); got.ErrorCode != errInvalidConfig {
		t.Fatalf("compaction through key 33: error code %d, want %d", got.ErrorCode, errInvalidConfig)
	}
}

// Every advertised version of both keys decodes and answers. The window is each
// schema's whole one (33: 0-2, 44: 0-1) because no field varies inside either,
// and this is the test that would catch a version that nonetheless does.
func TestAlterConfigsAtEveryAdvertisedVersion(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	for v := int16(0); v <= alterConfigsV; v++ {
		req := kmsg.NewPtrAlterConfigsRequest()
		req.SetVersion(v)
		r := kmsg.NewAlterConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceType(resourceTopic)
		r.ResourceName = topic
		r.Configs = []kmsg.AlterConfigsRequestResourceConfig{
			alterConfig("retention.ms", str("3600000")),
		}
		req.Resources = append(req.Resources, r)
		raw, err := cl.Broker(0).Request(ctxFor(t, 30*time.Second), req)
		if err != nil {
			t.Fatalf("AlterConfigs v%d: %v", v, err)
		}
		resp := raw.(*kmsg.AlterConfigsResponse)
		if len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != errNone {
			t.Fatalf("AlterConfigs v%d: %+v", v, resp.Resources)
		}
	}

	for v := int16(0); v <= incrementalAlterConfigsV; v++ {
		req := kmsg.NewPtrIncrementalAlterConfigsRequest()
		req.SetVersion(v)
		r := kmsg.NewIncrementalAlterConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceType(resourceTopic)
		r.ResourceName = topic
		r.Configs = []kmsg.IncrementalAlterConfigsRequestResourceConfig{
			incOp("retention.ms", opSet, str("7200000")),
		}
		req.Resources = append(req.Resources, r)
		raw, err := cl.Broker(0).Request(ctxFor(t, 30*time.Second), req)
		if err != nil {
			t.Fatalf("IncrementalAlterConfigs v%d: %v", v, err)
		}
		resp := raw.(*kmsg.IncrementalAlterConfigsResponse)
		if len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != errNone {
			t.Fatalf("IncrementalAlterConfigs v%d: %+v", v, resp.Resources)
		}
	}

	if v, _, _, _ := retentionOf(t, cl, topic); v != "7200000" {
		t.Fatalf("after the version walk: retention.ms = %q, want 7200000", v)
	}
}
