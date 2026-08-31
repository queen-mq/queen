// M7 F4: the two remaining admin writes against a running facade —
// CreatePartitions (37) and OffsetDelete (47), driven by franz-go's `kmsg`.
//
// Both of them exist because a real operator's tools reach for them, so what
// these tests assert is what a CLIENT gets back, which is the half the crate's
// own unit tests cannot see:
//
//   - CreatePartitions is a REFUSAL, and two thirds of it is Apache Kafka's own
//     sentence. `kafka-topics.sh --alter --partitions` prints the
//     `error_message` verbatim out of an InvalidPartitionsException, so a
//     message nobody can decode is a message nobody reads.
//   - OffsetDelete is Kafka's subscription rule, kept exactly: a live consumer
//     group's SUBSCRIBED topics are refused 86 and everything else is
//     deletable, live group or not. That second half is the one a merely
//     conservative implementation gets wrong, so it has its own test.
//
// The oracle diff of the two Kafka sentences lives in the differential runner
// (`compat/differential/admin_partitions.go`), which asks apache/kafka:3.9.1
// the same questions. Here they are pinned as literals, so a drift in the copy
// fails against a running facade too.
package compat

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The versions an AdminClient negotiates against the facade's advertised
// window (versions.rs: CreatePartitions 0-3, OffsetDelete 0-0).
const (
	createPartitionsV int16 = 3
	offsetDeleteV     int16 = 0
)

// `errUnknownTopicOrPartition` (admin_topics_test.go) and `errGroupIDNotFound`
// (admin_groups_test.go) are already declared in this package and are reused.
const (
	errInvalidPartitions        int16 = 37
	errInvalidReplicaAssignment int16 = 39
	errGroupSubscribedToTopic   int16 = 86
)

func createPartitions(
	t *testing.T,
	cl *kgo.Client,
	topic string,
	count int32,
	assignment []kmsg.CreatePartitionsRequestTopicAssignment,
	validateOnly bool,
) kmsg.CreatePartitionsResponseTopic {
	t.Helper()
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic
	rt.Count = count
	rt.Assignment = assignment

	req := kmsg.NewPtrCreatePartitionsRequest()
	req.SetVersion(createPartitionsV)
	req.TimeoutMillis = 30_000
	req.ValidateOnly = validateOnly
	req.Topics = []kmsg.CreatePartitionsRequestTopic{rt}

	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("CreatePartitions(%s, %d): %v", topic, count, err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("CreatePartitions answered %d results, want 1", len(resp.Topics))
	}
	return resp.Topics[0]
}

func partitionsMessage(got kmsg.CreatePartitionsResponseTopic) string {
	if got.ErrorMessage == nil {
		return ""
	}
	return *got.ErrorMessage
}

// CHECK P1. A DECREASE is Apache Kafka's own answer, byte for byte.
//
// This is not the exotic case: a provisioner that declares 12 partitions
// against a facade whose default is 1024 sends exactly this, and what it gets
// back is indistinguishable from a real broker's answer.
func TestCreatePartitionsRefusesADecreaseInKafkasOwnWords(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	width := topicWidth(t)

	got := createPartitions(t, cl, topic, width-1, nil, false)
	if got.ErrorCode != errInvalidPartitions {
		t.Fatalf("a decrease answered %d, want INVALID_PARTITIONS (%d)",
			got.ErrorCode, errInvalidPartitions)
	}
	want := fmt.Sprintf(
		"The topic %s currently has %d partition(s); %d would not be an increase.",
		topic, width, width-1)
	if partitionsMessage(got) != want {
		t.Errorf("message is %q, want %q", partitionsMessage(got), want)
	}
	// The typed error is what an AdminClient raises and what
	// `kafka-topics.sh` prints; a code outside that mapping ends a tool
	// instead of explaining itself.
	if err := kerr.ErrorForCode(got.ErrorCode); !errors.Is(err, kerr.InvalidPartitions) {
		t.Errorf("the code does not decode to InvalidPartitions: %v", err)
	}
}

// CHECK P2. A count EQUAL to the current width is Kafka's other sentence, and a
// non-positive count is not a case of its own on either broker: the comparison
// catches it first, so it answers as a decrease.
func TestCreatePartitionsRefusesEqualAndNonPositiveCounts(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	width := topicWidth(t)

	same := createPartitions(t, cl, topic, width, nil, false)
	if same.ErrorCode != errInvalidPartitions {
		t.Fatalf("an equal count answered %d, want INVALID_PARTITIONS", same.ErrorCode)
	}
	if want := fmt.Sprintf("Topic already has %d partition(s).", width); partitionsMessage(same) != want {
		t.Errorf("message is %q, want %q", partitionsMessage(same), want)
	}

	zero := createPartitions(t, cl, topic, 0, nil, false)
	if zero.ErrorCode != errInvalidPartitions {
		t.Fatalf("a count of 0 answered %d, want INVALID_PARTITIONS", zero.ErrorCode)
	}
	want := fmt.Sprintf(
		"The topic %s currently has %d partition(s); 0 would not be an increase.", topic, width)
	if partitionsMessage(zero) != want {
		t.Errorf("a count of 0 answered %q, want the decrease sentence %q",
			partitionsMessage(zero), want)
	}
}

// CHECK P3. An INCREASE is the one genuine capability gap, and the message has
// to name the knob that would actually change the width — otherwise an operator
// reads "no" with nothing to do about it.
func TestCreatePartitionsRefusesAnIncreaseAndNamesTheBrokerKnob(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	got := createPartitions(t, cl, topic, topicWidth(t)+4, nil, false)
	if got.ErrorCode != errInvalidPartitions {
		t.Fatalf("an increase answered %d, want INVALID_PARTITIONS", got.ErrorCode)
	}
	msg := partitionsMessage(got)
	for _, want := range []string{"QUEEN_KAFKA_DEFAULT_PARTITIONS", "produce to the higher lanes"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the increase message does not mention %q: %q", want, msg)
		}
	}
	// ...and nothing was widened. Metadata is the only authority on that.
	md := metadataFor(t, cl, topic)
	if got := int32(len(md.Topics[0].Partitions)); got != topicWidth(t) {
		t.Errorf("a refused increase widened the topic to %d", got)
	}
}

// CHECK P4. The two shapes that are not a count at all: a manual replica
// assignment on an increase, and a topic nobody has.
func TestCreatePartitionsRefusesAssignmentsAndUnknownTopics(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	one := kmsg.NewCreatePartitionsRequestTopicAssignment()
	one.Replicas = []int32{0}
	assigned := createPartitions(t, cl, topic, topicWidth(t)+1,
		[]kmsg.CreatePartitionsRequestTopicAssignment{one}, false)
	if assigned.ErrorCode != errInvalidReplicaAssignment {
		t.Errorf("an assignment answered %d, want INVALID_REPLICA_ASSIGNMENT (%d)",
			assigned.ErrorCode, errInvalidReplicaAssignment)
	}
	if !strings.Contains(partitionsMessage(assigned), "one logical broker") {
		t.Errorf("the assignment message does not say why: %q", partitionsMessage(assigned))
	}

	absent := createPartitions(t, cl, newTopic(t), 99, nil, false)
	if absent.ErrorCode != errUnknownTopicOrPartition {
		t.Errorf("an absent topic answered %d, want UNKNOWN_TOPIC_OR_PARTITION",
			absent.ErrorCode)
	}
	// The oracle sends no message for this code, and neither does the facade.
	if absent.ErrorMessage != nil {
		t.Errorf("an absent topic carried a message: %q", *absent.ErrorMessage)
	}
}

// CHECK P5. `validate_only` answers the same thing, because nothing was ever
// going to be written — and the field must not be silently dropped.
func TestCreatePartitionsValidateOnlyAnswersTheSame(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	live := createPartitions(t, cl, topic, topicWidth(t)-1, nil, false)
	dry := createPartitions(t, cl, topic, topicWidth(t)-1, nil, true)
	if live.ErrorCode != dry.ErrorCode || partitionsMessage(live) != partitionsMessage(dry) {
		t.Errorf("validate_only changed the answer: %d/%q vs %d/%q",
			live.ErrorCode, partitionsMessage(live), dry.ErrorCode, partitionsMessage(dry))
	}
}

// ------------------------------------------------------------- OffsetDelete

func offsetDelete(
	t *testing.T,
	cl *kgo.Client,
	group, topic string,
	partitions ...int32,
) *kmsg.OffsetDeleteResponse {
	t.Helper()
	rt := kmsg.NewOffsetDeleteRequestTopic()
	rt.Topic = topic
	for _, p := range partitions {
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rp.Partition = p
		rt.Partitions = append(rt.Partitions, rp)
	}
	req := kmsg.NewPtrOffsetDeleteRequest()
	req.SetVersion(offsetDeleteV)
	req.Group = group
	req.Topics = []kmsg.OffsetDeleteRequestTopic{rt}

	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetDelete(%s, %s): %v", group, topic, err)
	}
	return resp
}

// deletedCode is one partition's verdict out of an OffsetDelete answer.
func deletedCode(t *testing.T, resp *kmsg.OffsetDeleteResponse, topic string, partition int32) int16 {
	t.Helper()
	for _, tp := range resp.Topics {
		if tp.Topic != topic {
			continue
		}
		for _, p := range tp.Partitions {
			if p.Partition == partition {
				return p.ErrorCode
			}
		}
	}
	t.Fatalf("%s-%d is not in the OffsetDelete answer (top level %d)", topic, partition, resp.ErrorCode)
	return -1
}

// CHECK O1. A stopped group's offsets are deleted, and the GROUP SURVIVES.
//
// This is the deliberate deviation, pinned here so it cannot drift silently:
// OffsetDelete removes offsets, DeleteGroups removes groups. Apache Kafka 3.9.1
// drops an already-empty group once its last offset goes away; this facade
// keeps it listed, which keeps one API the only thing that makes a group stop
// existing (compat/ERRORS.md).
func TestOffsetDeleteRemovesOffsetsAndLeavesTheGroupListed(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 1)
	group := stoppedGroup(t, topic)
	admin := newClient(t)

	before := offsetFetchFor(t, admin, group, topic, 0)
	if before < 0 {
		t.Fatalf("%s has nothing committed on %s-0, so there is nothing to delete", group, topic)
	}

	resp := offsetDelete(t, admin, group, topic, 0)
	if resp.ErrorCode != 0 {
		t.Fatalf("a stopped group was refused at the top level: %d", resp.ErrorCode)
	}
	if code := deletedCode(t, resp, topic, 0); code != 0 {
		t.Fatalf("deleting a stopped group's offset answered %d", code)
	}
	if after := offsetFetchFor(t, admin, group, topic, 0); after != -1 {
		t.Errorf("the offset is still %d after the delete", after)
	}
	if state := listedState(listGroups(t, admin, listGroupsV), group); state == "" {
		t.Errorf("OffsetDelete removed the group %s from ListGroups", group)
	}
}

// CHECK O2. Deleting an offset that was never committed is error 0.
//
// The store answers "not applied" for a key that is not there, and reading that
// as a verdict would turn `--delete-offsets` on a fresh group into a run of
// spurious failures. Kafka answers 0 for the same thing.
func TestOffsetDeleteOfANeverCommittedOffsetIsNotAnError(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 1)
	group := stoppedGroup(t, topic)
	admin := newClient(t)

	// Delete twice: the second run finds nothing, which is what makes a
	// partially failed delete safe to re-run.
	for i := 0; i < 2; i++ {
		resp := offsetDelete(t, admin, group, topic, 0)
		if resp.ErrorCode != 0 {
			t.Fatalf("run %d was refused at the top level: %d", i, resp.ErrorCode)
		}
		if code := deletedCode(t, resp, topic, 0); code != 0 {
			t.Errorf("run %d answered %d, want 0", i, code)
		}
	}
}

// CHECK O3. Kafka's subscription rule, both halves.
//
// A live consumer group's SUBSCRIBED topic is refused 86 and keeps its offset;
// the same live group's offsets for a topic it is NOT subscribed to are
// deleted. The second half is the one that proves the facade applies Kafka's
// rule rather than refusing whenever a group has members.
func TestOffsetDeleteAppliesKafkasSubscriptionRule(t *testing.T) {
	subscribed, _ := seedAcrossPartitions(t, 1)
	other, _ := seedAcrossPartitions(t, 1)
	group := groupName(t)
	admin := newClient(t)

	// The offset on the topic this group will NOT consume, committed BEFORE the
	// group goes live: a simple commit (generation -1, no member id) is refused
	// UNKNOWN_MEMBER_ID once a group has members, on this facade and on Apache
	// Kafka alike, so this is the only order that puts an offset on a topic
	// nothing in the group is subscribed to.
	commitDirectly(t, admin, group, other, 0, 7)
	if got := offsetFetchFor(t, admin, group, other, 0); got != 7 {
		t.Fatalf("the setup commit on %s did not land (%d)", other, got)
	}

	live := newClient(t, eagerGroup(group, subscribed, kgo.DisableAutoCommit())...)
	got := drain(t, live, 1, 60*time.Second)
	if len(got) == 0 {
		t.Fatalf("%s read nothing", group)
	}
	if err := live.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	defer live.Close()

	// Half one: subscribed, so refused, and the offset survives.
	resp := offsetDelete(t, admin, group, subscribed, 0)
	if resp.ErrorCode != 0 {
		t.Fatalf("a live group was refused at the TOP level (%d); the rule is per partition",
			resp.ErrorCode)
	}
	if code := deletedCode(t, resp, subscribed, 0); code != errGroupSubscribedToTopic {
		t.Fatalf("a subscribed topic answered %d, want GROUP_SUBSCRIBED_TO_TOPIC (%d)",
			code, errGroupSubscribedToTopic)
	}
	if after := offsetFetchFor(t, admin, group, subscribed, 0); after < 0 {
		t.Errorf("a refused delete removed the offset anyway")
	}

	// Half two: the topic this group has an offset on but is NOT consuming.
	// The group is live, and this is deleted anyway, which is Kafka's rule.
	resp = offsetDelete(t, admin, group, other, 0)
	if code := deletedCode(t, resp, other, 0); code != 0 {
		t.Fatalf("an UNSUBSCRIBED topic of a live group answered %d, want 0", code)
	}
	if after := offsetFetchFor(t, admin, group, other, 0); after != -1 {
		t.Errorf("the unsubscribed topic's offset is still %d", after)
	}
}

// CHECK O4. A group nobody has heard of is GROUP_ID_NOT_FOUND at the TOP level,
// with no partitions in the answer at all — the shape the Java AdminClient
// checks before it looks at a partition.
func TestOffsetDeleteOnAnUnknownGroupIsTopLevelNotFound(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 1)
	admin := newClient(t)

	resp := offsetDelete(t, admin, groupName(t), topic, 0)
	if resp.ErrorCode != errGroupIDNotFound {
		t.Fatalf("an unknown group answered %d, want GROUP_ID_NOT_FOUND (%d)",
			resp.ErrorCode, errGroupIDNotFound)
	}
	if len(resp.Topics) != 0 {
		t.Errorf("a group-level refusal carried %d topics, want none", len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); !errors.Is(err, kerr.GroupIDNotFound) {
		t.Errorf("the code does not decode to GroupIDNotFound: %v", err)
	}
}

// commitDirectly is the simple-consumer commit — generation -1, no member id —
// which is the shape `alterConsumerGroupOffsets` sends and the one way to put
// an offset on a topic a group is not consuming.
func commitDirectly(t *testing.T, cl *kgo.Client, group, topic string, partition int32, offset int64) {
	t.Helper()
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = partition
	rp.Offset = offset
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rt.Partitions = []kmsg.OffsetCommitRequestTopicPartition{rp}

	req := kmsg.NewPtrOffsetCommitRequest()
	req.SetVersion(6)
	req.Group = group
	req.Generation = -1
	req.MemberID = ""
	req.Topics = []kmsg.OffsetCommitRequestTopic{rt}

	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetCommit(%s, %s): %v", group, topic, err)
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("OffsetCommit(%s, %s-%d) answered %d", group, topic, p.Partition, p.ErrorCode)
			}
		}
	}
}

// offsetFetchFor reads one committed offset back. -1 is Kafka's own "never
// committed", which is exactly what a deleted offset reads as.
func offsetFetchFor(t *testing.T, cl *kgo.Client, group, topic string, partition int32) int64 {
	t.Helper()
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{partition}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.SetVersion(5)
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{rt}

	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetFetch(%s, %s): %v", group, topic, err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("OffsetFetch(%s) answered %d at the top level", group, resp.ErrorCode)
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.Partition == partition {
				if p.ErrorCode != 0 {
					t.Fatalf("OffsetFetch(%s-%d) answered %d", topic, partition, p.ErrorCode)
				}
				return p.Offset
			}
		}
	}
	t.Fatalf("%s-%d is not in the OffsetFetch answer", topic, partition)
	return -1
}
