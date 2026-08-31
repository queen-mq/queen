// M7 F1: the topics-admin trio against a running facade — CreateTopics,
// DeleteTopics and DescribeConfigs, driven by franz-go's `kmsg` at the
// versions a real AdminClient negotiates.
//
// Every assertion here is about something a client ACTS on: an error code it
// branches on, the width it will hash records modulo, a config value it renders
// in a settings tab. Nothing asserts on a message string.
package compat

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The Kafka error codes this file branches on, by name, so a failure message
// says what it means.
const (
	errNone                    int16 = 0
	errUnknownTopicOrPartition int16 = 3
	errInvalidTopic            int16 = 17
	errTopicAlreadyExists      int16 = 36
	errInvalidReplicaAssign    int16 = 39
	errInvalidConfig           int16 = 40
	errInvalidRequest          int16 = 42
)

// The version an AdminClient negotiates against the facade's advertised window
// (versions.rs: CreateTopics 2-6, DeleteTopics 1-5, DescribeConfigs 1-4).
const (
	createTopicsV    = 6
	deleteTopicsV    = 5
	describeConfigsV = 4
)

// resourceType values from Kafka's ConfigResource.Type.
const (
	resourceTopic  int8 = 2
	resourceBroker int8 = 4
)

func createTopics(t *testing.T, cl *kgo.Client, version int16, req *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse {
	t.Helper()
	req.SetVersion(version)
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("CreateTopics v%d: %v", version, err)
	}
	return resp
}

func newCreate(topic string, partitions int32, replication int16) kmsg.CreateTopicsRequestTopic {
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = partitions
	rt.ReplicationFactor = replication
	return rt
}

func withConfig(rt kmsg.CreateTopicsRequestTopic, name, value string) kmsg.CreateTopicsRequestTopic {
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = name
	c.Value = &value
	rt.Configs = append(rt.Configs, c)
	return rt
}

func deleteTopics(t *testing.T, cl *kgo.Client, names ...string) *kmsg.DeleteTopicsResponse {
	t.Helper()
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(deleteTopicsV)
	req.TimeoutMillis = 30_000
	req.TopicNames = names
	for _, n := range names {
		rt := kmsg.NewDeleteTopicsRequestTopic()
		name := n
		rt.Topic = &name
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("DeleteTopics: %v", err)
	}
	return resp
}

// describeConfigs sends the request to node 0 EXPLICITLY rather than through
// franz-go's sharding.
//
// That is not a shortcut, it is the only way to ask the question: franz-go
// splits a DescribeConfigs by resource and routes a BROKER resource to the
// broker whose id it names, so a resource named "7" never leaves the client —
// it fails locally with "unknown broker". The facade's answer to a broker
// resource it is not is exactly what this file has to observe, so the request
// goes to the one node there is.
func describeConfigs(t *testing.T, cl *kgo.Client, kind int8, name string) kmsg.DescribeConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.SetVersion(describeConfigsV)
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType = kmsg.ConfigResourceType(kind)
	r.ResourceName = name
	req.Resources = append(req.Resources, r)
	raw, err := cl.Broker(0).Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("DescribeConfigs(%d,%q): %v", kind, name, err)
	}
	resp, ok := raw.(*kmsg.DescribeConfigsResponse)
	if !ok {
		t.Fatalf("DescribeConfigs(%d,%q): unexpected response type %T", kind, name, raw)
	}
	if len(resp.Resources) != 1 {
		t.Fatalf("DescribeConfigs(%d,%q): %d resources, want 1", kind, name, len(resp.Resources))
	}
	return resp.Resources[0]
}

func configValue(r kmsg.DescribeConfigsResponseResource, key string) (string, bool) {
	for _, c := range r.Configs {
		if c.Name == key {
			if c.Value == nil {
				return "", true
			}
			return *c.Value, true
		}
	}
	return "", false
}

// listedTopics is what a `--list` sees: the topic names in a full metadata
// answer, which is the path every admin tool takes.
func listedTopics(t *testing.T, cl *kgo.Client) map[string]bool {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = nil // null: all topics
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("Metadata(all): %v", err)
	}
	out := map[string]bool{}
	for _, mt := range resp.Topics {
		if mt.Topic != nil {
			out[*mt.Topic] = true
		}
	}
	return out
}

// TestCreateTopicsCreatesAQueueASecondClientCanSee is the whole point of the
// stage: a topic created through the Kafka admin API is a Queen queue, visible
// to a SECOND connection's metadata and writable by an ordinary producer.
func TestCreateTopicsCreatesAQueueASecondClientCanSee(t *testing.T) {
	admin := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics, newCreate(topic, 4, 1))
	resp := createTopics(t, admin, createTopicsV, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("%d results, want 1", len(resp.Topics))
	}
	got := resp.Topics[0]
	if got.ErrorCode != errNone {
		t.Fatalf("create %s: error code %d (%v)", topic, got.ErrorCode, got.ErrorMessage)
	}
	// The width the facade will actually serve, NOT the 4 that was asked for:
	// Queen declares no width per queue. The contract is that this number and
	// the client's next Metadata agree, which is what the check below is.
	if got.NumPartitions != topicWidth(t) {
		t.Fatalf("create %s reported %d partitions, the facade serves %d",
			topic, got.NumPartitions, topicWidth(t))
	}
	if got.ReplicationFactor != 1 {
		t.Fatalf("create %s reported replication %d, want 1", topic, got.ReplicationFactor)
	}

	// A SECOND client, so nothing is being read out of the first one's cache.
	second := newClient(t)
	if !listedTopics(t, second)[topic] {
		t.Fatalf("%s was created but is not in another client's topic list", topic)
	}
	md := metadataFor(t, second, topic)
	if len(md.Topics) != 1 || md.Topics[0].ErrorCode != errNone {
		t.Fatalf("%s: metadata says %+v", topic, md.Topics)
	}
	if int32(len(md.Topics[0].Partitions)) != got.NumPartitions {
		t.Fatalf("%s: create said %d partitions, metadata says %d",
			topic, got.NumPartitions, len(md.Topics[0].Partitions))
	}

	// ...and it is a real queue: an ordinary producer writes to it.
	produceSync(t, second, []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v")},
	})
}

// The rule the whole handler is arranged around: an existing name is answered
// TOPIC_ALREADY_EXISTS and its configuration is NOT rewritten by an upsert.
func TestCreateTopicsRefusesAnExistingTopic(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics, newCreate(topic, 4, 1))
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errTopicAlreadyExists {
		t.Fatalf("second create of %s: error code %d, want %d",
			topic, resp.Topics[0].ErrorCode, errTopicAlreadyExists)
	}
}

// The refusal that decides whether Kafka Connect can run, and the reason
// CreateTopics does not unlock it: Connect's internal topics are compacted.
func TestCreateTopicsRefusesCompaction(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics,
		withConfig(newCreate(topic, 1, 1), "cleanup.policy", "compact"))
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errInvalidConfig {
		t.Fatalf("compacted create: error code %d, want %d",
			resp.Topics[0].ErrorCode, errInvalidConfig)
	}
	if resp.Topics[0].ErrorMessage == nil ||
		!strings.Contains(*resp.Topics[0].ErrorMessage, "compaction") {
		t.Fatalf("the refusal does not name compaction: %v", resp.Topics[0].ErrorMessage)
	}
	// Nothing was created.
	if listedTopics(t, newClient(t))[topic] {
		t.Fatalf("%s was refused and exists anyway", topic)
	}
}

// A retention the mapping accepts is applied and echoed back — the one place a
// client can read it, because Queen exposes no HTTP read of a queue's config.
func TestCreateTopicsAppliesRetentionAndEchoesIt(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics,
		withConfig(newCreate(topic, 1, 1), "retention.ms", "604800000"))
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errNone {
		t.Fatalf("create with retention: %d (%v)", resp.Topics[0].ErrorCode, resp.Topics[0].ErrorMessage)
	}
	var retention string
	for _, c := range resp.Topics[0].Configs {
		if c.Name == "retention.ms" && c.Value != nil {
			retention = *c.Value
		}
	}
	if retention != "604800000" {
		t.Fatalf("the create echoed retention.ms=%q, want 604800000", retention)
	}
}

// An unknown config name is refused rather than dropped: dropping it would tell
// a client it got a durability setting it did not get.
func TestCreateTopicsRefusesAnUnknownConfig(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics,
		withConfig(newCreate(topic, 1, 1), "min.insync.replicas", "2"))
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errInvalidConfig {
		t.Fatalf("unknown config: error code %d, want %d",
			resp.Topics[0].ErrorCode, errInvalidConfig)
	}
}

// A `__` name and an illegal one are both INVALID_TOPIC_EXCEPTION here — this
// is the surface where a NAME is validated, so the client is told which of the
// two things is wrong.
func TestCreateTopicsRefusesReservedAndIllegalNames(t *testing.T) {
	cl := newClient(t)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	for _, bad := range []string{"__evil", "has spaces", "a/b"} {
		req.Topics = append(req.Topics, newCreate(bad, 1, 1))
	}
	resp := createTopics(t, cl, createTopicsV, req)

	for _, got := range resp.Topics {
		if got.ErrorCode != errInvalidTopic {
			t.Fatalf("%s: error code %d, want %d", got.Topic, got.ErrorCode, errInvalidTopic)
		}
	}
}

// A manual replica assignment names broker ids to place partitions on. This
// facade places nothing anywhere, so it refuses rather than silently discarding
// an explicit operator instruction.
func TestCreateTopicsRefusesAManualReplicaAssignment(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	rt := newCreate(topic, -1, -1)
	a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	a.Partition = 0
	a.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, a)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics, rt)
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errInvalidReplicaAssign {
		t.Fatalf("assignment: error code %d, want %d",
			resp.Topics[0].ErrorCode, errInvalidReplicaAssign)
	}
}

// A name repeated in one request refuses every entry and creates none —
// Apache Kafka's own answer.
func TestCreateTopicsRefusesARepeatedName(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics, newCreate(topic, 1, 1), newCreate(topic, 1, 1))
	resp := createTopics(t, cl, createTopicsV, req)

	for _, got := range resp.Topics {
		if got.ErrorCode != errInvalidRequest {
			t.Fatalf("repeated name: error code %d, want %d", got.ErrorCode, errInvalidRequest)
		}
	}
	if listedTopics(t, newClient(t))[topic] {
		t.Fatalf("%s was refused as a duplicate and exists anyway", topic)
	}
}

// validate_only answers what would have happened and writes nothing.
func TestCreateTopicsValidateOnlyWritesNothing(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.ValidateOnly = true
	req.Topics = append(req.Topics, newCreate(topic, 4, 1))
	resp := createTopics(t, cl, createTopicsV, req)

	if resp.Topics[0].ErrorCode != errNone {
		t.Fatalf("validate_only: error code %d (%v)", resp.Topics[0].ErrorCode, resp.Topics[0].ErrorMessage)
	}
	if resp.Topics[0].NumPartitions != topicWidth(t) {
		t.Fatalf("validate_only reported %d partitions, want %d",
			resp.Topics[0].NumPartitions, topicWidth(t))
	}
	if listedTopics(t, newClient(t))[topic] {
		t.Fatalf("validate_only created %s", topic)
	}
}

// Every advertised version answers, and the fields the version carries are the
// ones that come back. Below v5 the width and the configs are not on the wire
// at all, which is the version boundary and not a missing answer.
func TestCreateTopicsAtEveryAdvertisedVersion(t *testing.T) {
	cl := newClient(t)
	for version := int16(2); version <= createTopicsV; version++ {
		topic := newTopic(t) + "-v" + strconv.Itoa(int(version))
		req := kmsg.NewPtrCreateTopicsRequest()
		req.TimeoutMillis = 30_000
		req.Topics = append(req.Topics, newCreate(topic, -1, -1))
		resp := createTopics(t, cl, version, req)

		if resp.Topics[0].ErrorCode != errNone {
			t.Fatalf("v%d: error code %d (%v)", version, resp.Topics[0].ErrorCode, resp.Topics[0].ErrorMessage)
		}
		if version >= 5 && resp.Topics[0].NumPartitions != topicWidth(t) {
			t.Fatalf("v%d reported %d partitions, want %d",
				version, resp.Topics[0].NumPartitions, topicWidth(t))
		}
		t.Cleanup(func() { deleteTopics(t, cl, topic) })
	}
}

// TestDeleteTopicsRemovesTheQueue is the other half of the round trip: the
// topic goes, a second client stops seeing it, and deleting it again is
// UNKNOWN_TOPIC_OR_PARTITION.
func TestDeleteTopicsRemovesTheQueue(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	produceSync(t, cl, []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v")},
	})

	resp := deleteTopics(t, cl, topic)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != errNone {
		t.Fatalf("delete %s: %+v", topic, resp.Topics)
	}

	if listedTopics(t, newClient(t))[topic] {
		t.Fatalf("%s was deleted and is still listed", topic)
	}

	again := deleteTopics(t, cl, topic)
	if again.Topics[0].ErrorCode != errUnknownTopicOrPartition {
		t.Fatalf("second delete of %s: error code %d, want %d",
			topic, again.Topics[0].ErrorCode, errUnknownTopicOrPartition)
	}
	// v5 is in the window so that the refusal can say so in words too.
	if again.Topics[0].ErrorMessage == nil {
		t.Fatalf("v%d carried no error message", deleteTopicsV)
	}
}

// Not a name-validation surface: a reserved or illegal name is "this facade
// has no such topic", which is the code every client accepts on this API.
func TestDeleteTopicsAnswersUnknownForAReservedName(t *testing.T) {
	cl := newClient(t)
	resp := deleteTopics(t, cl, "__consumer_offsets", "never-existed-"+newTopic(t))
	for _, got := range resp.Topics {
		if got.ErrorCode != errUnknownTopicOrPartition {
			t.Fatalf("%v: error code %d, want %d", got.Topic, got.ErrorCode, errUnknownTopicOrPartition)
		}
	}
}

// The answers line up with the request, name by name, including a mix of
// outcomes — which is what a client indexes by.
func TestDeleteTopicsAnswersLineUpWithTheRequest(t *testing.T) {
	cl := newClient(t)
	live, gone := newTopic(t)+"-live", newTopic(t)+"-gone"
	ensureTopic(t, cl, live)

	resp := deleteTopics(t, cl, gone, live, "__internal")
	if len(resp.Topics) != 3 {
		t.Fatalf("%d results, want 3", len(resp.Topics))
	}
	want := []struct {
		topic string
		code  int16
	}{{gone, errUnknownTopicOrPartition}, {live, errNone}, {"__internal", errUnknownTopicOrPartition}}
	for i, w := range want {
		got := resp.Topics[i]
		if got.Topic == nil || *got.Topic != w.topic {
			t.Fatalf("result %d names %v, want %s", i, got.Topic, w.topic)
		}
		if got.ErrorCode != w.code {
			t.Fatalf("%s: error code %d, want %d", w.topic, got.ErrorCode, w.code)
		}
	}
}

// TestDescribeConfigsOnATopic is the campaign's decisive red: sarama's
// ClusterAdmin.ListTopics chains one of these per topic, so nothing on its
// admin object worked without it.
func TestDescribeConfigsOnATopic(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	r := describeConfigs(t, cl, resourceTopic, topic)
	if r.ErrorCode != errNone {
		t.Fatalf("describe %s: error code %d (%v)", topic, r.ErrorCode, r.ErrorMessage)
	}
	// The two the facade can name the enforcer of, and no invented fourth.
	if v, ok := configValue(r, "cleanup.policy"); !ok || v != "delete" {
		t.Fatalf("cleanup.policy = %q (present=%v), want delete", v, ok)
	}
	if v, ok := configValue(r, "min.insync.replicas"); !ok || v != "1" {
		t.Fatalf("min.insync.replicas = %q (present=%v), want 1", v, ok)
	}
	// ...and the third, since M7 F4: this topic was created THROUGH the facade,
	// so the facade has its own record of the options bag it posted and reports
	// retention from it. The bag named no retention, so what is in force is
	// Queen's default — retention off — which is Kafka's -1, reported as a
	// DEFAULT because nobody set it.
	if v, ok := configValue(r, "retention.ms"); !ok || v != "-1" {
		t.Fatalf("retention.ms = %q (present=%v), want -1 for a topic this facade created", v, ok)
	}
	// `read_only` is PER ROW now. The two rows whose only legal value is the one
	// already reported cannot be changed and say so; retention can be, because
	// AlterConfigs and IncrementalAlterConfigs land on it. A UI that greys out
	// its edit button on this flag is still being told the truth.
	writable := map[string]bool{"retention.ms": true}
	for _, c := range r.Configs {
		if c.ReadOnly == writable[c.Name] {
			t.Fatalf("%s: read_only=%v, want %v", c.Name, c.ReadOnly, !writable[c.Name])
		}
		if c.IsSensitive {
			t.Fatalf("%s is reported sensitive; nothing here is a credential", c.Name)
		}
	}
}

func TestDescribeConfigsOnAnUnknownTopic(t *testing.T) {
	cl := newClient(t)
	for _, name := range []string{"never-existed-" + newTopic(t), "__consumer_offsets"} {
		r := describeConfigs(t, cl, resourceTopic, name)
		if r.ErrorCode != errUnknownTopicOrPartition {
			t.Fatalf("%s: error code %d, want %d", name, r.ErrorCode, errUnknownTopicOrPartition)
		}
	}
}

// The broker resource is where this API earns its place: every value is one
// this process actually enforces, so `--entity-type brokers` is a real window
// onto the facade.
func TestDescribeConfigsOnTheBroker(t *testing.T) {
	cl := newClient(t)
	for _, name := range []string{"", "0"} {
		r := describeConfigs(t, cl, resourceBroker, name)
		if r.ErrorCode != errNone {
			t.Fatalf("broker %q: error code %d (%v)", name, r.ErrorCode, r.ErrorMessage)
		}
		// The facade's REAL width, which is the number every producer will
		// hash modulo for an auto-created topic.
		v, ok := configValue(r, "num.partitions")
		if !ok {
			t.Fatalf("broker %q reports no num.partitions", name)
		}
		if want := topicWidth(t); v != strconv.Itoa(int(want)) {
			t.Fatalf("broker num.partitions = %s, the facade serves %d", v, want)
		}
		for _, key := range []string{
			"auto.create.topics.enable", "compression.type", "connections.max.idle.ms",
			"group.initial.rebalance.delay.ms", "group.min.session.timeout.ms",
			"group.max.session.timeout.ms",
		} {
			if _, ok := configValue(r, key); !ok {
				t.Fatalf("broker %q reports no %s", name, key)
			}
		}
	}
}

// Another node's id is refused rather than answered with this node's running
// configuration under somebody else's name.
func TestDescribeConfigsRefusesAnotherBroker(t *testing.T) {
	cl := newClient(t)
	r := describeConfigs(t, cl, resourceBroker, "7")
	if r.ErrorCode != errInvalidRequest {
		t.Fatalf("broker 7: error code %d, want %d", r.ErrorCode, errInvalidRequest)
	}
}

// BROKER_LOGGER (8) and every other resource type: refused by number rather
// than answered with an empty set, which would read as "this exists and is
// empty".
func TestDescribeConfigsRefusesEveryOtherResourceType(t *testing.T) {
	cl := newClient(t)
	for _, kind := range []int8{0, 1, 8, 16} {
		r := describeConfigs(t, cl, kind, "whatever")
		if r.ErrorCode != errInvalidRequest {
			t.Fatalf("resource type %d: error code %d, want %d", kind, r.ErrorCode, errInvalidRequest)
		}
	}
}

// The whole admin round trip in the order a provisioner does it: create,
// describe, produce, delete — and a describe of what is gone.
func TestTopicsAdminRoundTrip(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics, newCreate(topic, -1, -1))
	if got := createTopics(t, cl, createTopicsV, req).Topics[0]; got.ErrorCode != errNone {
		t.Fatalf("create: %d (%v)", got.ErrorCode, got.ErrorMessage)
	}

	if r := describeConfigs(t, cl, resourceTopic, topic); r.ErrorCode != errNone {
		t.Fatalf("describe after create: %d (%v)", r.ErrorCode, r.ErrorMessage)
	}

	produceSync(t, cl, []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v")},
	})

	if got := deleteTopics(t, cl, topic).Topics[0]; got.ErrorCode != errNone {
		t.Fatalf("delete: %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	if r := describeConfigs(t, cl, resourceTopic, topic); r.ErrorCode != errUnknownTopicOrPartition {
		t.Fatalf("describe after delete: %d, want %d", r.ErrorCode, errUnknownTopicOrPartition)
	}
}

// A CreateTopics with a request timeout the facade does not act on still
// answers, which is the deviation being pinned rather than hidden: `timeout_ms`
// is not honoured here, the same way Produce's is not.
func TestCreateTopicsIgnoresTimeoutMillis(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 0
	req.Topics = append(req.Topics, newCreate(topic, -1, -1))
	if got := createTopics(t, cl, createTopicsV, req).Topics[0]; got.ErrorCode != errNone {
		t.Fatalf("timeout_ms=0: error code %d (%v)", got.ErrorCode, got.ErrorMessage)
	}
	t.Cleanup(func() { deleteTopics(t, cl, topic) })
}

// A refused create must not leave a half-made queue behind, and the code it
// answers must not be one that makes a client give up on the whole request:
// the neighbours in the same request are still served.
func TestCreateTopicsRefusalDoesNotTakeItsNeighboursWithIt(t *testing.T) {
	cl := newClient(t)
	good, bad := newTopic(t)+"-ok", newTopic(t)+"-bad"

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.Topics = append(req.Topics,
		withConfig(newCreate(bad, 1, 1), "cleanup.policy", "compact"),
		newCreate(good, -1, -1))
	resp := createTopics(t, cl, createTopicsV, req)

	byName := map[string]int16{}
	for _, got := range resp.Topics {
		byName[got.Topic] = got.ErrorCode
	}
	if byName[bad] != errInvalidConfig {
		t.Fatalf("%s: error code %d, want %d", bad, byName[bad], errInvalidConfig)
	}
	if byName[good] != errNone {
		t.Fatalf("%s: error code %d, want 0", good, byName[good])
	}
	t.Cleanup(func() { deleteTopics(t, cl, good) })
	if resp.ThrottleMillis < 0 {
		t.Fatalf("negative throttle %d", resp.ThrottleMillis)
	}
}
