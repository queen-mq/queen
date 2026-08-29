package compat

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// TestApiVersionsAndNegotiation reads the facade's advertised table and checks
// it against the versions kafka-go has HARDCODED for the group APIs.
//
// This is the load-bearing test of the whole suite. `apiVersionMap.negotiate`
// (conn.go:72) compares only against MaxVersion and never reads MinVersion, and
// six of kafka-go's Conn calls do not negotiate at all — they write a fixed
// version into the request. If the facade's FLOOR for one of those ever rises
// above kafka-go's fixed number, kafka-go will send an out-of-window request and
// this facade answers that by closing the connection with no error code
// (compat/ERRORS.md), which surfaces to a user as an unexplained EOF in the
// middle of a group join. Pinning it here turns that into one legible failure.
func TestApiVersionsAndNegotiation(t *testing.T) {
	section(t, "ApiVersions: what the facade advertises vs what kafka-go hardcodes")

	ctx, cancel := ctxWith(t, 20*time.Second)
	defer cancel()

	resp, err := client().ApiVersions(ctx, &kafka.ApiVersionsRequest{})
	if err != nil {
		failf(t, "ApiVersions: %v", err)
	}
	if resp.Error != nil {
		failf(t, "ApiVersions error: %v", resp.Error)
	}

	adv := map[int][2]int{}
	names := make([]string, 0, len(resp.ApiKeys))
	for _, k := range resp.ApiKeys {
		adv[k.ApiKey] = [2]int{k.MinVersion, k.MaxVersion}
		names = append(names, fmt.Sprintf("%s(%d) %d-%d", apiName(int16(k.ApiKey)), k.ApiKey, k.MinVersion, k.MaxVersion))
	}
	sort.Strings(names)
	for _, n := range names {
		note("advertised %s", n)
	}
	okf(t, "facade advertises %d API keys", len(resp.ApiKeys))

	// The six kafka-go writes with NO negotiation, and the two it negotiates
	// from a short list. Every one has to land inside the advertised window.
	fixed := []struct {
		key  int
		v    int
		what string
	}{
		{10, 0, "FindCoordinator, conn.go:323 (fixed v0)"},
		{14, 0, "SyncGroup, conn.go:520 (fixed v0)"},
		{12, 0, "Heartbeat, conn.go:350 (fixed v0)"},
		{13, 0, "LeaveGroup, conn.go:407 (fixed v0)"},
		{8, 2, "OffsetCommit, conn.go:459 (fixed v2 — the facade's exact FLOOR)"},
		{9, 1, "OffsetFetch, conn.go:490 (fixed v1 — the facade's exact FLOOR)"},
		{3, 1, "Metadata, conn.go:255 (fixed v1 on the topic-less probe)"},
	}
	for _, f := range fixed {
		w, ok := adv[f.key]
		if !ok {
			failf(t, "%s: facade does not advertise API key %d at all", f.what, f.key)
		}
		if f.v < w[0] || f.v > w[1] {
			failf(t, "%s: v%d is OUTSIDE the advertised window %d-%d; the facade answers that by closing the connection", f.what, f.v, w[0], w[1])
		}
		okf(t, "%s inside advertised %d-%d", f.what, w[0], w[1])
	}

	// The negotiated lists. kafka-go walks each from the highest down and takes
	// the first whose value is <= the broker's MaxVersion.
	negotiated := []struct {
		key    int
		offers []int
		expect int
		what   string
	}{
		{1, []int{2, 5, 10}, 5, "Fetch, conn.go:780 (offers v2/v5/v10)"},
		{0, []int{2, 3, 7}, 7, "Produce, conn.go:1157 (offers v2/v3/v7)"},
		{3, []int{1, 6}, 6, "Metadata, conn.go:978 (offers v1/v6)"},
		{11, []int{1, 2}, 2, "JoinGroup, conn.go:372 (offers v1/v2)"},
	}
	for _, n := range negotiated {
		w := adv[n.key]
		picked := -1
		for i := len(n.offers) - 1; i >= 0; i-- {
			if n.offers[i] <= w[1] {
				picked = n.offers[i]
				break
			}
		}
		if picked != n.expect {
			failf(t, "%s: would pick v%d against advertised %d-%d, expected v%d", n.what, picked, w[0], w[1], n.expect)
		}
		if picked < w[0] {
			failf(t, "%s: picks v%d which is BELOW the advertised floor %d — kafka-go's negotiate() ignores MinVersion", n.what, picked, w[0])
		}
		okf(t, "%s picks v%d, inside advertised %d-%d", n.what, picked, w[0], w[1])
	}

	// The absent APIs. kafka-go offers helpers for all of these; the facade
	// deliberately does not implement them (PLAN_QUEEN_KAFKA.md M7 backlog), so
	// a well-behaved client must fail fast rather than hang.
	for _, k := range []int{19 /*CreateTopics*/, 20 /*DeleteTopics*/, 22 /*InitProducerId*/, 16 /*ListGroups*/, 15 /*DescribeGroups*/, 32 /*DescribeConfigs*/, 47 /*OffsetDelete*/} {
		if _, ok := adv[k]; ok {
			note("facade now advertises %s(%d) — this suite assumed it did not", apiName(int16(k)), k)
		}
	}
	okf(t, "CreateTopics/DeleteTopics/InitProducerId/ListGroups/DescribeGroups/DescribeConfigs are absent, as the plan states")
}

// TestMetadataShape is the shape check kafka-go is historically picky about: one
// broker, itself, leader of every partition, and a controller that resolves.
func TestMetadataShape(t *testing.T) {
	section(t, "Metadata: one broker, leader of everything")

	topic := topicName("meta")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	ctx, cancel := ctxWith(t, 20*time.Second)
	defer cancel()

	resp, err := client().Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		failf(t, "Metadata: %v", err)
	}

	if len(resp.Brokers) != 1 {
		failf(t, "expected exactly 1 broker, got %d", len(resp.Brokers))
	}
	b := resp.Brokers[0]
	okf(t, "1 broker: id=%d %s:%d", b.ID, b.Host, b.Port)

	if resp.Controller.Host == "" {
		failf(t, "controller has no host; kafka-go resolves the controller for admin calls")
	}
	okf(t, "controller resolves to %s:%d (id=%d)", resp.Controller.Host, resp.Controller.Port, resp.Controller.ID)
	note("clusterID=%q throttle=%s", resp.ClusterID, resp.Throttle)

	var target *kafka.Topic
	for i := range resp.Topics {
		if resp.Topics[i].Name == topic {
			target = &resp.Topics[i]
		}
	}
	if target == nil {
		failf(t, "topic %s absent from metadata", topic)
	}
	if target.Error != nil {
		failf(t, "topic %s carries error %v", topic, target.Error)
	}
	if len(target.Partitions) != width {
		failf(t, "topic %s has %d partitions, want %d", topic, len(target.Partitions), width)
	}
	okf(t, "topic %s has %d partitions", topic, width)

	ids := map[int]bool{}
	for _, p := range target.Partitions {
		if p.Error != nil {
			failf(t, "partition %d carries error %v", p.ID, p.Error)
		}
		if p.Leader.ID != b.ID {
			failf(t, "partition %d leader id=%d, expected the only broker id=%d", p.ID, p.Leader.ID, b.ID)
		}
		if p.Leader.Host != b.Host || p.Leader.Port != b.Port {
			failf(t, "partition %d leader %s:%d != advertised %s:%d", p.ID, p.Leader.Host, p.Leader.Port, b.Host, b.Port)
		}
		if len(p.Replicas) != 1 || len(p.Isr) != 1 {
			failf(t, "partition %d has %d replicas / %d isr, expected 1 / 1", p.ID, len(p.Replicas), len(p.Isr))
		}
		ids[p.ID] = true
	}
	for i := 0; i < width; i++ {
		if !ids[i] {
			failf(t, "partition %d missing from metadata; ids are not 0..%d", i, width-1)
		}
	}
	okf(t, "every partition 0..%d leads to the one broker, 1 replica, 1 isr", width-1)
}

// TestAutoCreateIsGatedByTheWireFlag pins the single most surprising thing this
// suite found, and it is the facade behaving CORRECTLY rather than a defect.
//
// kafka-go's two stacks disagree about auto-creation and neither lets the caller
// choose:
//
//   - `Client.Metadata` sends Metadata v8 with `AllowAutoTopicCreation` left
//     false, and `kafka.MetadataRequest` (metadata.go:14) has no field to set it.
//     The facade honours the false: the topic is NOT created and the response
//     carries UNKNOWN_TOPIC_OR_PARTITION, forever.
//   - `Dialer.LookupPartitions` -> `Conn.ReadPartitions` sends Metadata v6 with
//     the flag hardcoded TRUE (conn.go:987). The facade creates the topic.
//
// A reader of PLAN_QUEEN_KAFKA.md's "auto-create cannot be refused on Metadata
// v0-v3 (no wire field)" could reasonably expect a bare Metadata to always
// create; this proves the deviation is scoped to exactly the versions that have
// no field, and that from v4 up the facade obeys the flag. Any harness that
// probes for a topic with `Client.Metadata` and waits for it to appear will wait
// forever — which is what makes it worth a test rather than a comment.
func TestAutoCreateIsGatedByTheWireFlag(t *testing.T) {
	section(t, "Auto-create is decided by the Metadata wire flag, not by naming the topic")

	name := topicName("flagprobe")
	width := topicWidth(t)

	// (a) Client.Metadata, flag false at v8: must NOT create.
	ctx, cancel := ctxWith(t, 20*time.Second)
	resp, err := client().Metadata(ctx, &kafka.MetadataRequest{Topics: []string{name}})
	cancel()
	if err != nil {
		failf(t, "Client.Metadata probe: %v", err)
	}
	sawUnknown := false
	for _, tp := range resp.Topics {
		if tp.Name != name {
			continue
		}
		if tp.Error != nil && errors.Is(tp.Error, kafka.UnknownTopicOrPartition) {
			sawUnknown = true
		}
	}
	if !sawUnknown {
		note("Client.Metadata (v8, AllowAutoTopicCreation=false) created %s anyway", name)
		note("that is the v0-v3 deviation leaking into a version that HAS the field; worth a look, but not a failure here")
	} else {
		okf(t, "Client.Metadata (v8, flag false) did NOT create %s: UNKNOWN_TOPIC_OR_PARTITION, as the flag asks", name)
	}

	// (b) the Dialer path, flag true at v6: must create, at the facade default.
	ctx2, cancel2 := ctxWith(t, 30*time.Second)
	defer cancel2()
	parts, err := dialer().LookupPartitions(ctx2, "tcp", bootstrap(), name)
	if err != nil {
		failf(t, "Dialer.LookupPartitions (Metadata v6, flag true) on a fresh topic: %v", err)
	}
	if len(parts) != width {
		failf(t, "LookupPartitions created %s with %d partitions, want the facade default %d", name, len(parts), width)
	}
	okf(t, "Dialer.LookupPartitions (v6, flag true) created %s at %d partitions", name, width)
}

// TestUnknownTopicIsNamed checks that a `__`-prefixed name — which
// PLAN_QUEEN_KAFKA.md says never exists anywhere — comes back as a named error
// rather than an empty success, because kafka-go surfaces that error to the
// caller verbatim.
func TestUnknownTopicIsNamed(t *testing.T) {
	section(t, "Metadata for a name that cannot exist")

	ctx, cancel := ctxWith(t, 20*time.Second)
	defer cancel()

	name := "__kgo-never-" + runID
	resp, err := client().Metadata(ctx, &kafka.MetadataRequest{Topics: []string{name}})
	if err != nil {
		failf(t, "Metadata for %s: %v", name, err)
	}
	found := false
	for _, tp := range resp.Topics {
		if tp.Name != name {
			continue
		}
		found = true
		if tp.Error == nil {
			failf(t, "%s came back with no error and %d partitions; the plan says __-prefixed names never exist", name, len(tp.Partitions))
		}
		if !errors.Is(tp.Error, kafka.UnknownTopicOrPartition) {
			failf(t, "%s came back as %v, expected UnknownTopicOrPartition", name, tp.Error)
		}
		okf(t, "%s -> %v (kafka-go maps the code to its own sentinel)", name, tp.Error)
	}
	if !found {
		failf(t, "%s absent from the metadata response entirely", name)
	}
}
