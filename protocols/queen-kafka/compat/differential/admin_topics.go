package main

// M7 F1: CreateTopics, DeleteTopics and DescribeConfigs, against the facade and
// against apache/kafka:3.9.1 side by side.
//
// Three scenarios rather than one, so each API's divergences classify under its
// own regexp prefix in main.go and no stage of this campaign can widen
// another's.
//
// Two things in here are MEASUREMENTS and not assertions — they were written
// down before the handler was, and the handler was made to match what Kafka
// answered:
//
//   - `create.duplicate.*`: Kafka answers every entry of a repeated topic name
//     INVALID_REQUEST and creates none of them (kmsg's own doc comment on
//     CreateTopicsResponseTopic.ErrorCode says so, and this run confirms it).
//   - `describeconfigs/topic.empty_filter.*`: whether an EMPTY (non-null)
//     `ConfigNames` means "every key" or "no key". `kafka-protocol`'s
//     `DescribeConfigsResource::default()` is `Some(vec![])`, so a hand-rolled
//     client that never touches the field sends an empty list — and the answer
//     decides whether such a client sees anything at all.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios,
		scenario{
			name: "createtopics",
			desc: "creating a topic explicitly: the width and replication a broker reports back, the names it refuses, and the configs it accepts",
			run:  scenCreateTopics,
		},
		scenario{
			name: "deletetopics",
			desc: "deleting a topic, deleting one that is not there, and what a listing says afterwards",
			run:  scenDeleteTopics,
		},
		scenario{
			name: "describeconfigs",
			desc: "the configuration a broker reports for a topic and for itself",
			run:  scenDescribeConfigs,
		},
	)
}

// The versions an AdminClient negotiates against the facade's advertised
// windows (versions.rs). Both brokers support all three, so the same version
// goes to both and the answers are directly comparable.
const (
	vCreateTopics    = 6
	vDeleteTopics    = 5
	vDescribeConfigs = 4
)

// adminErrName is errName plus the codes only the admin APIs can answer. It is
// a local helper rather than an edit to wire.go's table so that this stage
// touches only its own files.
func adminErrName(code int16) string {
	switch code {
	case 38:
		return "38/INVALID_REPLICATION_FACTOR"
	case 39:
		return "39/INVALID_REPLICA_ASSIGNMENT"
	case 40:
		return "40/INVALID_CONFIG"
	case 41:
		return "41/NOT_CONTROLLER"
	case 44:
		return "44/POLICY_VIOLATION"
	case 89:
		return "89/THROTTLING_QUOTA_EXCEEDED"
	}
	return errName(code)
}

// brokerID is the node id each target answers for. It is broker-specific by
// definition — the facade is node 0 and this Kafka is node 1 — and it matters
// because a DescribeConfigs BROKER resource named "" returns only the DYNAMIC
// configs on Apache Kafka, while one named the broker's own id returns the
// static ones too. Asking each broker about ITSELF is the comparable question.
func brokerID(c *runctx) string {
	if c.target.label == "kafka" {
		return "1"
	}
	return "0"
}

// createTopic issues one CreateTopics and returns the single result.
func createTopic(k *conn, rt kmsg.CreateTopicsRequestTopic, validateOnly bool) (kmsg.CreateTopicsResponseTopic, error) {
	req := kmsg.NewCreateTopicsRequest()
	req.TimeoutMillis = 30_000
	req.ValidateOnly = validateOnly
	req.Topics = append(req.Topics, rt)
	resp, _, err := k.doT(&req, vCreateTopics, 20*time.Second)
	if err != nil {
		return kmsg.CreateTopicsResponseTopic{}, err
	}
	ct := resp.(*kmsg.CreateTopicsResponse)
	if len(ct.Topics) != 1 {
		return kmsg.CreateTopicsResponseTopic{}, fmt.Errorf("%d results, want 1", len(ct.Topics))
	}
	return ct.Topics[0], nil
}

func newTopicSpec(name string, partitions int32, replication int16) kmsg.CreateTopicsRequestTopic {
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = name
	rt.NumPartitions = partitions
	rt.ReplicationFactor = replication
	return rt
}

func withTopicConfig(rt kmsg.CreateTopicsRequestTopic, name, value string) kmsg.CreateTopicsRequestTopic {
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name = name
	c.Value = &value
	rt.Configs = append(rt.Configs, c)
	return rt
}

// recordCreate writes the fields a client acts on. `num_partitions` and
// `replication_factor` are only on the wire from v5.
func recordCreate(c *runctx, key string, got kmsg.CreateTopicsResponseTopic, err error) {
	if err != nil {
		c.rec.bad(key+".error_code", err)
		return
	}
	c.rec.add(key+".error_code", "%s", adminErrName(got.ErrorCode))
	c.rec.add(key+".num_partitions", "%d", got.NumPartitions)
	c.rec.add(key+".replication_factor", "%d", got.ReplicationFactor)
	// The MESSAGE is never diffed — the two brokers word things differently and
	// always will — but whether there IS one is a real difference: it is what a
	// user sees instead of a bare number.
	c.rec.add(key+".has_error_message", "%t", got.ErrorMessage != nil && *got.ErrorMessage != "")
	// Recorded unconditionally: a key one target never writes is reported as a
	// divergence in its own right, which would bury the real one.
	message := ""
	if got.ErrorMessage != nil {
		message = *got.ErrorMessage
	}
	c.rec.info(key+".error_message", "%s", message)
}

// metadataPartitions is how many partitions a full Metadata reports for one
// topic, and -1 when the topic is not in the listing at all. It is the number
// every producer hashes modulo, so it is the number that has to agree.
func metadataPartitions(k *conn, topic string) (int32, error) {
	req := kmsg.NewMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	name := topic
	rt.Topic = &name
	req.Topics = append(req.Topics, rt)
	allow := false
	req.AllowAutoTopicCreation = allow
	resp, _, err := k.doT(&req, 9, 20*time.Second)
	if err != nil {
		return 0, err
	}
	md := resp.(*kmsg.MetadataResponse)
	for _, mt := range md.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			if mt.ErrorCode != 0 {
				return -1, nil
			}
			return int32(len(mt.Partitions)), nil
		}
	}
	return -1, nil
}

func scenCreateTopics(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	// 1. The ordinary create, asking for a width. Since M7 the ask is HONOURED
	//    as a per-topic floor, so both brokers report the 4 that were asked for
	//    and this case no longer diverges. The facade still reports the width it
	//    will SERVE — which for a brand-new topic with no lanes is the floor.
	fresh := c.topic("ct-new")
	got, err := createTopic(k, newTopicSpec(fresh, 4, 1), false)
	recordCreate(c, "create.new", got, err)

	// ...and whatever it reported, the next Metadata must agree with it. That
	// is the property that actually matters, and it is one both brokers keep.
	if n, err := metadataPartitions(k, fresh); err != nil {
		c.rec.bad("create.new.metadata_agrees", err)
	} else {
		c.rec.add("create.new.metadata_agrees", "%t", n == got.NumPartitions)
		c.rec.info("create.new.metadata_partitions", "%d", n)
	}

	// 2. The same name again.
	got, err = createTopic(k, newTopicSpec(fresh, 4, 1), false)
	recordCreate(c, "create.again", got, err)

	// 3. A name that is not a legal Kafka topic name.
	got, err = createTopic(k, newTopicSpec("has spaces", 1, 1), false)
	recordCreate(c, "create.illegal_name", got, err)

	// 4. A `__`-prefixed name. Apache Kafka treats these as ordinary names;
	//    the facade hides them everywhere, so it cannot let one be made. The
	//    `__` has to be the FIRST thing in the name, which is why this one is
	//    not built through c.topic().
	got, err = createTopic(k, newTopicSpec("__"+c.topic("ct-internal"), 1, 1), false)
	recordCreate(c, "create.internal_name", got, err)

	// 5. A config name NEITHER broker knows. This is the strong check on the
	//    config path: both should answer INVALID_CONFIG.
	got, err = createTopic(k,
		withTopicConfig(newTopicSpec(c.topic("ct-bogus"), 1, 1), "bogus.config.name", "1"), false)
	recordCreate(c, "create.unknown_config", got, err)

	// 6. A config Kafka knows and the facade does not have a mechanism for.
	got, err = createTopic(k,
		withTopicConfig(newTopicSpec(c.topic("ct-isr"), 1, 1), "min.insync.replicas", "1"), false)
	recordCreate(c, "create.kafka_only_config", got, err)

	// 7. Compaction — the refusal that decides whether Kafka Connect can run.
	got, err = createTopic(k,
		withTopicConfig(newTopicSpec(c.topic("ct-compact"), 1, 1), "cleanup.policy", "compact"), false)
	recordCreate(c, "create.compact", got, err)

	// 8. Retention, which BOTH brokers accept — and both echo back at v5+.
	retained := c.topic("ct-retained")
	got, err = createTopic(k,
		withTopicConfig(newTopicSpec(retained, 1, 1), "retention.ms", "604800000"), false)
	recordCreate(c, "create.retention", got, err)
	c.rec.add("create.retention.echo", "%s", configOf(got.Configs, "retention.ms"))

	// 9. A manual replica assignment naming a broker that is not there.
	assigned := newTopicSpec(c.topic("ct-assigned"), -1, -1)
	a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	a.Partition = 0
	a.Replicas = []int32{7}
	assigned.ReplicaAssignment = append(assigned.ReplicaAssignment, a)
	got, err = createTopic(k, assigned, false)
	recordCreate(c, "create.assignment", got, err)

	// 10. The same name twice in one request. MEASURED: see the file header.
	dup := c.topic("ct-dup")
	dupReq := kmsg.NewCreateTopicsRequest()
	dupReq.TimeoutMillis = 30_000
	dupReq.Topics = append(dupReq.Topics, newTopicSpec(dup, 1, 1), newTopicSpec(dup, 1, 1))
	if resp, _, err := k.doT(&dupReq, vCreateTopics, 20*time.Second); err != nil {
		c.rec.bad("create.duplicate.error_codes", err)
	} else {
		ct := resp.(*kmsg.CreateTopicsResponse)
		var codes []string
		for _, r := range ct.Topics {
			codes = append(codes, adminErrName(r.ErrorCode))
		}
		sort.Strings(codes)
		c.rec.add("create.duplicate.error_codes", "%s", strings.Join(codes, ","))
	}
	if n, err := metadataPartitions(k, dup); err != nil {
		c.rec.bad("create.duplicate.exists_after", err)
	} else {
		c.rec.add("create.duplicate.exists_after", "%t", n >= 0)
	}

	// 11. validate_only writes nothing.
	dry := c.topic("ct-dry")
	got, err = createTopic(k, newTopicSpec(dry, 1, 1), true)
	recordCreate(c, "create.validate_only", got, err)
	if n, err := metadataPartitions(k, dry); err != nil {
		c.rec.bad("create.validate_only.exists_after", err)
	} else {
		c.rec.add("create.validate_only.exists_after", "%t", n >= 0)
	}
}

// configOf is one config's value out of a CreateTopics echo, or `absent`.
func configOf(configs []kmsg.CreateTopicsResponseTopicConfig, name string) string {
	for _, c := range configs {
		if c.Name == name {
			if c.Value == nil {
				return "null"
			}
			return *c.Value
		}
	}
	return "absent"
}

func deleteOne(k *conn, names ...string) (*kmsg.DeleteTopicsResponse, error) {
	req := kmsg.NewDeleteTopicsRequest()
	req.TimeoutMillis = 30_000
	req.TopicNames = names
	for _, n := range names {
		rt := kmsg.NewDeleteTopicsRequestTopic()
		name := n
		rt.Topic = &name
		req.Topics = append(req.Topics, rt)
	}
	resp, _, err := k.doT(&req, vDeleteTopics, 20*time.Second)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.DeleteTopicsResponse), nil
}

func scenDeleteTopics(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	// A topic that exists, made through the same API so the scenario stands on
	// its own.
	live := c.topic("dt-live")
	if _, err := createTopic(k, newTopicSpec(live, 1, 1), false); err != nil {
		c.rec.bad("delete.setup", err)
		return
	}

	resp, err := deleteOne(k, live)
	if err != nil {
		c.rec.bad("delete.existing.error_code", err)
	} else {
		c.rec.add("delete.existing.error_code", "%s", adminErrName(resp.Topics[0].ErrorCode))
		c.rec.add("delete.existing.has_error_message", "%t",
			resp.Topics[0].ErrorMessage != nil && *resp.Topics[0].ErrorMessage != "")
	}
	if n, err := metadataPartitions(k, live); err != nil {
		c.rec.bad("delete.existing.exists_after", err)
	} else {
		c.rec.add("delete.existing.exists_after", "%t", n >= 0)
	}

	// The same name again, and two names neither broker has. A `__`-prefixed
	// name that does not exist is used rather than a real internal topic:
	// deleting `__consumer_offsets` on the oracle would take the oracle with it.
	resp, err = deleteOne(k, live, c.topic("dt-never"), "__"+c.topic("dt-internal"), "has spaces")
	if err != nil {
		c.rec.bad("delete.missing.error_codes", err)
		return
	}
	labels := []string{"again", "never", "internal", "illegal_name"}
	for i, label := range labels {
		if i >= len(resp.Topics) {
			c.rec.add("delete."+label+".error_code", "<no result>")
			continue
		}
		c.rec.add("delete."+label+".error_code", "%s", adminErrName(resp.Topics[i].ErrorCode))
	}
	// The answers line up with the request, name by name — which is what a
	// client indexes by, and the one shape a batched API can get wrong.
	aligned := len(resp.Topics) == 4
	for i, want := range []string{live, c.topic("dt-never"), "__" + c.topic("dt-internal"), "has spaces"} {
		if i >= len(resp.Topics) || resp.Topics[i].Topic == nil || *resp.Topics[i].Topic != want {
			aligned = false
		}
	}
	c.rec.add("delete.results_line_up", "%t", aligned)
	var got []string
	for _, r := range resp.Topics {
		if r.Topic == nil {
			got = append(got, "<null>")
			continue
		}
		got = append(got, *r.Topic)
	}
	c.rec.info("delete.result_names", "%s", strings.Join(got, ","))
}

func describeOne(k *conn, kind int8, name string, configNames []string) (kmsg.DescribeConfigsResponseResource, error) {
	req := kmsg.NewDescribeConfigsRequest()
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType = kmsg.ConfigResourceType(kind)
	r.ResourceName = name
	r.ConfigNames = configNames
	req.Resources = append(req.Resources, r)
	resp, _, err := k.doT(&req, vDescribeConfigs, 20*time.Second)
	if err != nil {
		return kmsg.DescribeConfigsResponseResource{}, err
	}
	dc := resp.(*kmsg.DescribeConfigsResponse)
	if len(dc.Resources) != 1 {
		return kmsg.DescribeConfigsResponseResource{}, fmt.Errorf("%d resources, want 1", len(dc.Resources))
	}
	return dc.Resources[0], nil
}

func describedValue(r kmsg.DescribeConfigsResponseResource, name string) string {
	for _, c := range r.Configs {
		if c.Name == name {
			if c.Value == nil {
				return "null"
			}
			return *c.Value
		}
	}
	return "absent"
}

func scenDescribeConfigs(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	topic := c.topic("dc-topic")
	if _, err := createTopic(k, newTopicSpec(topic, 1, 1), false); err != nil {
		c.rec.bad("topic.setup", err)
		return
	}

	// ---------------------------------------------------------------- a topic
	r, err := describeOne(k, 2, topic, nil)
	if err != nil {
		c.rec.bad("topic.error_code", err)
	} else {
		c.rec.add("topic.error_code", "%s", adminErrName(r.ErrorCode))
		// The two the facade can name the enforcer of. Both brokers report
		// them, and they have to agree — a tool computing under-replication or
		// deciding whether a log compacts reads exactly these.
		c.rec.add("topic.cleanup_policy", "%s", describedValue(r, "cleanup.policy"))
		c.rec.add("topic.min_insync_replicas", "%s", describedValue(r, "min.insync.replicas"))
		// The gap the design records rather than papers over: Queen exposes no
		// read of a queue's config, so retention is writable and not readable.
		c.rec.add("topic.retention_ms", "%s", describedValue(r, "retention.ms"))
		// The SIZE of the answer is legitimately broker-specific — Kafka has a
		// hundred topic knobs and the facade has two — so it is reported and
		// not diffed.
		c.rec.info("topic.config_count", "%d", len(r.Configs))
		c.rec.add("topic.cleanup_policy_read_only", "%t", readOnlyOf(r, "cleanup.policy"))
	}

	// An EMPTY (non-null) ConfigNames. MEASURED: see the file header.
	full, errFull := describeOne(k, 2, topic, nil)
	empty, errEmpty := describeOne(k, 2, topic, []string{})
	switch {
	case errFull != nil || errEmpty != nil:
		c.rec.add("topic.empty_filter.means", "ERROR")
	case len(empty.Configs) == len(full.Configs):
		c.rec.add("topic.empty_filter.means", "every key")
	case len(empty.Configs) == 0:
		c.rec.add("topic.empty_filter.means", "no key")
	default:
		c.rec.add("topic.empty_filter.means", "%d of %d keys", len(empty.Configs), len(full.Configs))
	}

	// A named filter: exactly the keys asked for, and a key the broker does not
	// report is simply absent rather than an error.
	filtered, err := describeOne(k, 2, topic, []string{"cleanup.policy", "not.a.config"})
	if err != nil {
		c.rec.bad("topic.filter.error_code", err)
	} else {
		c.rec.add("topic.filter.error_code", "%s", adminErrName(filtered.ErrorCode))
		c.rec.add("topic.filter.count", "%d", len(filtered.Configs))
		c.rec.add("topic.filter.cleanup_policy", "%s", describedValue(filtered, "cleanup.policy"))
	}

	// A topic neither broker has.
	if r, err := describeOne(k, 2, c.topic("dc-never"), nil); err != nil {
		c.rec.bad("topic.unknown.error_code", err)
	} else {
		c.rec.add("topic.unknown.error_code", "%s", adminErrName(r.ErrorCode))
	}

	// --------------------------------------------------------------- a broker
	//
	// Named by each broker's OWN id: on Apache Kafka an empty name returns only
	// the DYNAMIC configs, so the two forms are two different questions and
	// only the explicit one is comparable.
	if r, err := describeOne(k, 4, brokerID(c), nil); err != nil {
		c.rec.bad("broker.error_code", err)
	} else {
		c.rec.add("broker.error_code", "%s", adminErrName(r.ErrorCode))
		// The width an auto-created topic gets. Both stacks are configured for
		// 8, so this is the check that the facade reports the number it serves.
		c.rec.add("broker.num_partitions", "%s", describedValue(r, "num.partitions"))
		c.rec.add("broker.auto_create", "%s", describedValue(r, "auto.create.topics.enable"))
		c.rec.add("broker.compression_type", "%s", describedValue(r, "compression.type"))
		c.rec.info("broker.config_count", "%d", len(r.Configs))
	}

	// The empty name: a different question on Kafka (dynamic configs only), so
	// only the error code is diffed.
	if r, err := describeOne(k, 4, "", nil); err != nil {
		c.rec.bad("broker.empty_name.error_code", err)
	} else {
		c.rec.add("broker.empty_name.error_code", "%s", adminErrName(r.ErrorCode))
	}

	// A broker that is neither of them.
	if r, err := describeOne(k, 4, "7", nil); err != nil {
		c.rec.bad("broker.other_node.error_code", err)
	} else {
		c.rec.add("broker.other_node.error_code", "%s", adminErrName(r.ErrorCode))
	}

	// BROKER_LOGGER, and a type neither broker has ever defined.
	if r, err := describeOne(k, 8, brokerID(c), nil); err != nil {
		c.rec.bad("broker_logger.error_code", err)
	} else {
		c.rec.add("broker_logger.error_code", "%s", adminErrName(r.ErrorCode))
	}
	if r, err := describeOne(k, 99, "whatever", nil); err != nil {
		c.rec.bad("unknown_type.error_code", err)
	} else {
		c.rec.add("unknown_type.error_code", "%s", adminErrName(r.ErrorCode))
	}
}

func readOnlyOf(r kmsg.DescribeConfigsResponseResource, name string) bool {
	for _, c := range r.Configs {
		if c.Name == name {
			return c.ReadOnly
		}
	}
	return false
}
