package main

import (
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// theVersion is the explicit Config.Version every scenario but `defaults`,
// `versionsweep` and `noapiversions` runs at. It is deliberately INSIDE the
// facade's window only after clamping: Kafka 3.6 speaks Fetch v12 and Produce
// v10, and the facade advertises 4..=6 and 3..=9. See the file header of main.go.
var theVersion = sarama.V3_6_0_0

func brokers(e env) []string { return []string{e.bootstrap} }

// ------------------------------------------------------------------ versions

func scenarioVersions(r *runner, st *state) {
	tap := newTap()
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-versions", version: theVersion, apiVersionsReq: true, tap: tap})

	b := sarama.NewBroker(st.env.bootstrap)
	if err := b.Open(cfg); err != nil {
		r.fail("open broker %s: %v", st.env.bootstrap, err)
		return
	}
	defer func() { _ = b.Close() }()
	if ok, err := b.Connected(); !ok {
		r.fail("broker not connected: %v", err)
		return
	}
	r.ok("connected to %s", st.env.bootstrap)

	resp, err := b.ApiVersions(&sarama.ApiVersionsRequest{
		Version:               3,
		ClientSoftwareName:    "queen-kafka-compat-sarama",
		ClientSoftwareVersion: "1",
	})
	if err != nil {
		r.fail("ApiVersions v3: %v", err)
		return
	}
	r.ok("ApiVersions v3 answered %d keys, error_code=%d", len(resp.ApiKeys), resp.ErrorCode)

	adv := map[int16]sarama.ApiVersionsResponseKey{}
	for _, k := range resp.ApiKeys {
		adv[k.ApiKey] = k
	}
	keys := make([]int, 0, len(adv))
	for k := range adv {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, k := range keys {
		a := adv[int16(k)]
		r.info("advertised %-16s (key %2d) v%d..v%d", apiName(a.ApiKey), a.ApiKey, a.MinVersion, a.MaxVersion)
	}

	// The whole advertised table, row by row: every key in versions.rs and the
	// window it names. It is the WHOLE table and not just the rows sarama
	// happens to use, because the count below is asserted against it — a key
	// added or removed on the facade has to be written here on purpose before
	// this scenario can go green again.
	wantAPIs := []struct {
		key      int16
		min, max int16
	}{
		{0, 3, 9},  // Produce
		{1, 4, 6},  // Fetch  — the ceiling that matters
		{2, 1, 5},  // ListOffsets
		{3, 0, 9},  // Metadata
		{8, 2, 6},  // OffsetCommit
		{9, 1, 7},  // OffsetFetch
		{10, 0, 3}, // FindCoordinator
		{11, 0, 4}, // JoinGroup
		{12, 0, 2}, // Heartbeat
		{13, 0, 2}, // LeaveGroup
		{14, 0, 2}, // SyncGroup
		{15, 0, 3}, // DescribeGroups  — M7 F2; v4 is group_instance_id
		{16, 0, 4}, // ListGroups      — M7 F2; v4 is KIP-518's states_filter
		{17, 0, 1}, // SaslHandshake
		{18, 0, 3}, // ApiVersions
		{19, 2, 6}, // CreateTopics    — M7 F1; v7 answers a topic id
		{20, 1, 5}, // DeleteTopics    — M7 F1; v6 names topics by id
		{22, 0, 4}, // InitProducerId  — M7 F3; v5 exists for KIP-890 only
		{24, 0, 3}, // AddPartitionsToTxn — M9; v4 is a DIFFERENT request, the one a coordinator sends a leader
		{25, 0, 3}, // AddOffsetsToTxn    — M9; at v4 the client stops sending this API at all
		{26, 0, 3}, // EndTxn             — M9; v5's response carries a TV2 epoch bump this facade does not perform
		{28, 0, 3}, // TxnOffsetCommit    — M9; the max is a FLOOR: kafka-clients throws below v3 whenever group metadata is set
		{29, 1, 3}, // DescribeAcls    — M7 F4; SECURITY_DISABLED, v0 is KIP-896's floor
		{30, 1, 3}, // CreateAcls      — M7 F4
		{31, 1, 3}, // DeleteAcls      — M7 F4
		{32, 1, 4}, // DescribeConfigs — M7 F1; the whole schema
		{33, 0, 2}, // AlterConfigs    — M7 F4; the deprecated full-replacement write
		{36, 0, 1}, // SaslAuthenticate
		{37, 0, 3}, // CreatePartitions        — M7 F4; refuses, with Kafka's own sentences
		{42, 0, 2}, // DeleteGroups            — M7 F2; the whole schema
		{44, 0, 1}, // IncrementalAlterConfigs — M7 F4; what kafka-configs.sh --alter sends
		{47, 0, 0}, // OffsetDelete            — M7 F4; the whole schema is v0
	}
	for _, want := range wantAPIs {
		got, ok := adv[want.key]
		if !ok {
			r.fail("%s (key %d) is not advertised", apiName(want.key), want.key)
			continue
		}
		r.check(got.MinVersion == want.min && got.MaxVersion == want.max,
			"%s advertised v%d..v%d (want v%d..v%d)",
			apiName(want.key), got.MinVersion, got.MaxVersion, want.min, want.max)
	}
	r.check(len(resp.ApiKeys) == len(wantAPIs) && len(resp.ApiKeys) == 32,
		"exactly 32 APIs advertised and every one of them has a row above, got %d", len(resp.ApiKeys))

	// The twelve keys M7 ADDED. This loop asserted their ABSENCE until F1/F2/F3
	// and then F4 landed, and it is inverted rather than deleted because the
	// absence was the reason sarama's whole ClusterAdmin was unreachable here
	// (see the `edges` scenario): the file has to keep saying which keys moved,
	// and a build that dropped one has to fail on this line rather than
	// somewhere downstream in an admin call that suddenly times out.
	for _, k := range []int16{16, 19, 20, 22, 29, 30, 31, 32, 33, 37, 44, 47} {
		_, present := adv[k]
		r.check(present, "%s (key %d) is advertised since M7 — it was deliberately absent before", apiName(k), k)
	}

	// What is STILL absent, listed by name so a future build that adds one has
	// to update this line on purpose. DescribeCluster is the interesting one to
	// keep after M7: sarama's ClusterAdmin now works without it, because
	// librdkafka-style clients answer that question from Metadata alone.
	for _, k := range []int16{60} {
		_, present := adv[k]
		r.check(!present, "%s (key %d) is deliberately NOT advertised", apiName(k), k)
	}

	// sarama's clamp, stated: Config.Version = 3.6 wants Fetch v12 and Produce
	// v10; restrictApiVersion lowers both to the advertised ceiling.
	r.info("Config.Version=%s wants Fetch v12 / Produce v10; sarama's restrictApiVersion clamps to the advertised max", theVersion.String())
	r.check(clampAvailable(),
		"this build of sarama (%s) has restrictApiVersion — it arrived in v1.46.0, and below that Config.Version must be <= 1.0.0 or Fetch goes out at v10 and the consumer loops on EOF forever",
		saramaModuleVersion())
	tap.report()
}

// ------------------------------------------------------------------- produce

const mainCount = 512

func ensureProduced(r *runner, st *state) bool {
	if st.topic != "" {
		return true
	}
	scenarioProduce(r, st)
	return st.topic != ""
}

func scenarioProduce(r *runner, st *state) {
	if st.topic != "" {
		r.info("already produced to %s", st.topic)
		return
	}
	parts := int32(st.env.partsWant)
	topic := fmt.Sprintf("sarama-%s-main", st.env.runID)
	fx := buildFixtures(mainCount, parts, "m")

	tap := newTap()
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-producer", version: theVersion, apiVersionsReq: true, tap: tap})
	cfg.Producer.Partitioner = sarama.NewManualPartitioner

	p, err := sarama.NewSyncProducer(brokers(st.env), cfg)
	if err != nil {
		r.fail("NewSyncProducer: %v", err)
		return
	}
	defer func() { _ = p.Close() }()

	msgs := make([]*sarama.ProducerMessage, len(fx))
	for i, f := range fx {
		msgs[i] = f.message(topic)
	}

	start := time.Now()
	ok := r.deadline(fmt.Sprintf("SendMessages: %d records over %d partitions, keys + headers, uncompressed, acks=all", len(msgs), parts),
		90*time.Second, func() error { return p.SendMessages(msgs) })
	if !ok {
		for _, l := range saramaLog.since(0, 6, "error", "Error", "Failed") {
			r.info("sarama: %s", l)
		}
		return
	}
	r.info("produced in %s", time.Since(start).Round(time.Millisecond))

	for i, f := range fx {
		f.offset = msgs[i].Offset
		if msgs[i].Partition != f.partition {
			r.fail("record %d: manual partitioner put it on %d, not %d", i, msgs[i].Partition, f.partition)
			return
		}
	}

	st.topic = topic
	st.want = fx
	st.byPart = byPartition(fx)

	// Per-partition offsets: contiguous from 0, ascending in production order.
	bad := 0
	for part, list := range st.byPart {
		for i, f := range list {
			if f.offset != int64(i) {
				bad++
				if bad <= 3 {
					r.fail("partition %d record %d got offset %d, want %d", part, i, f.offset, i)
				}
			}
		}
	}
	r.check(bad == 0, "every partition's offsets are contiguous from 0 in production order (%d partitions)", len(st.byPart))
	r.check(len(st.byPart) == int(parts), "records landed on all %d partitions (got %d)", parts, len(st.byPart))
	tap.report()
}

// --------------------------------------------------------------- compression

func scenarioCompression(r *runner, st *state) {
	parts := int32(st.env.partsWant)
	codecs := []struct {
		name  string
		codec sarama.CompressionCodec
		needs sarama.KafkaVersion
	}{
		{"none", sarama.CompressionNone, sarama.V0_11_0_0},
		{"gzip", sarama.CompressionGZIP, sarama.V0_11_0_0},
		{"snappy", sarama.CompressionSnappy, sarama.V0_11_0_0},
		{"lz4", sarama.CompressionLZ4, sarama.V0_11_0_0},
		{"zstd", sarama.CompressionZSTD, sarama.V2_1_0_0},
	}
	const n = 128
	baseline := int64(0)

	for _, c := range codecs {
		if !theVersion.IsAtLeast(c.needs) {
			r.info("%s skipped: sarama gates it on Config.Version >= %s", c.name, c.needs.String())
			continue
		}
		topic := fmt.Sprintf("sarama-%s-%s", st.env.runID, c.name)
		fx := buildFixtures(n, parts, c.name)

		tap := newTap()
		cfg := newConfig(cfgOpts{clientID: "qk-sarama-" + c.name, version: theVersion, apiVersionsReq: true, tap: tap})
		cfg.Producer.Partitioner = sarama.NewManualPartitioner
		cfg.Producer.Compression = c.codec

		p, err := sarama.NewSyncProducer(brokers(st.env), cfg)
		if err != nil {
			r.fail("%s: NewSyncProducer: %v", c.name, err)
			continue
		}
		msgs := make([]*sarama.ProducerMessage, len(fx))
		for i, f := range fx {
			msgs[i] = f.message(topic)
		}
		sent := r.deadline(fmt.Sprintf("%s: produced %d records", c.name, n), 60*time.Second,
			func() error { return p.SendMessages(msgs) })
		produceBytes := tap.requestBytes(0)
		_ = p.Close()
		if !sent {
			continue
		}
		for i, f := range fx {
			f.offset = msgs[i].Offset
		}
		if c.name == "none" {
			baseline = produceBytes
			r.info("none: %d produce request bytes on the wire (the baseline)", produceBytes)
		} else if baseline > 0 {
			r.check(produceBytes < baseline,
				"%s actually compressed: %d produce bytes vs %d uncompressed (%.0f%%)",
				c.name, produceBytes, baseline, 100*float64(produceBytes)/float64(baseline))
		}

		got, err := consumePartitions(st.env, topic, parts, n, 45*time.Second)
		if err != nil {
			r.fail("%s: consume back: %v", c.name, err)
			continue
		}
		r.check(len(got) == n, "%s: read back %d of %d records", c.name, len(got), n)
		if bad := compareAll(fx, got); len(bad) > 0 {
			r.fail("%s: round trip not byte-exact: %s", c.name, strings.Join(bad[:min(3, len(bad))], "; "))
		} else {
			r.ok("%s: every key, value and header round-tripped byte-exact", c.name)
		}
	}
}

// ---------------------------------------------------------------- the group

func scenarioGroup(r *runner, st *state) {
	if !ensureProduced(r, st) {
		return
	}
	group := fmt.Sprintf("sarama-%s-g1", st.env.runID)
	st.group = group

	col, err := runGroup(st.env, group, []string{st.topic}, mainCount, 90*time.Second, true)
	if err != nil {
		r.fail("consumer group %s: %v", group, err)
		for _, l := range saramaLog.since(0, 8, "consumer/", "kafka:", "error") {
			r.info("sarama: %s", l)
		}
		return
	}
	r.ok("consumer group %s formed (%d generation(s), %s to first record)", group, col.generations, col.firstAt.Round(time.Millisecond))
	r.check(len(col.msgs) == mainCount, "the group read %d of %d records", len(col.msgs), mainCount)

	// Per-partition: offsets strictly ascending, and the records in production
	// order, byte-exact.
	perPart := map[int32][]*sarama.ConsumerMessage{}
	for _, m := range col.msgs {
		perPart[m.Partition] = append(perPart[m.Partition], m)
	}
	r.check(len(perPart) == st.env.partsWant, "every one of the %d partitions was delivered (got %d)", st.env.partsWant, len(perPart))

	orderBad, exactBad := 0, []string{}
	for part, got := range perPart {
		want := st.byPart[part]
		if len(got) != len(want) {
			r.fail("partition %d: %d records, want %d", part, len(got), len(want))
			continue
		}
		for i := range got {
			if got[i].Offset != int64(i) {
				orderBad++
			}
			if b := want[i].compare(got[i]); len(b) > 0 {
				exactBad = append(exactBad, fmt.Sprintf("p%d[%d] %s: %s", part, i, want[i].label, strings.Join(b, ", ")))
			}
		}
	}
	r.check(orderBad == 0, "every partition arrived in offset order, 0..n-1, no gaps (%d violations)", orderBad)
	if len(exactBad) == 0 {
		r.ok("all %d records byte-exact: keys, values, headers, null-vs-empty, non-UTF-8, 64 KiB, 256 distinct bytes", mainCount)
	} else {
		r.fail("%d records not byte-exact; first: %s", len(exactBad), exactBad[0])
		for _, b := range exactBad[:min(4, len(exactBad))] {
			r.info("  %s", b)
		}
	}

	// The named edges, called out one by one so a regression names itself.
	byLabel := map[string]*fixture{}
	for _, f := range st.want {
		if f.label != "ordinary" {
			byLabel[f.label] = f
		}
	}
	for label, f := range byLabel {
		var got *sarama.ConsumerMessage
		for _, m := range perPart[f.partition] {
			if m.Offset == f.offset {
				got = m
				break
			}
		}
		if got == nil {
			r.fail("edge %q (p%d@%d) never arrived", label, f.partition, f.offset)
			continue
		}
		r.check(len(f.compare(got)) == 0, "edge %q survived", label)
	}
}

// --------------------------------------------------------------- commit/resume

func scenarioResume(r *runner, st *state) {
	if !ensureProduced(r, st) {
		return
	}
	if st.group == "" {
		// `resume` run alone: do the first pass here.
		scenarioGroup(r, st)
		if st.group == "" {
			return
		}
	}

	// 1. Read the committed offsets back off the broker with OffsetFetch,
	//    rather than believing what the client thinks it committed.
	committed, err := fetchCommitted(st.env, st.group, st.topic, int32(st.env.partsWant))
	if err != nil {
		r.fail("OffsetFetch for %s: %v", st.group, err)
		return
	}
	total := int64(0)
	allGood := true
	for part, off := range committed {
		want := int64(len(st.byPart[part]))
		if off != want {
			allGood = false
			r.fail("partition %d committed at %d, want %d", part, off, want)
		}
		total += off
	}
	r.check(allGood, "OffsetFetch says the group committed every partition at its high watermark (%d records)", total)

	// 2. More records arrive while the group is down.
	const extra = 64
	parts := int32(st.env.partsWant)
	fx := buildFixtures(extra, parts, "resume")
	tap := newTap()
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-resume-producer", version: theVersion, apiVersionsReq: true, tap: tap})
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	p, err := sarama.NewSyncProducer(brokers(st.env), cfg)
	if err != nil {
		r.fail("NewSyncProducer: %v", err)
		return
	}
	msgs := make([]*sarama.ProducerMessage, len(fx))
	for i, f := range fx {
		msgs[i] = f.message(st.topic)
	}
	sent := r.deadline(fmt.Sprintf("produced %d more records while the group was down", extra),
		60*time.Second, func() error { return p.SendMessages(msgs) })
	_ = p.Close()
	if !sent {
		return
	}
	for i, f := range fx {
		f.offset = msgs[i].Offset
	}
	// The topic is longer now, and `offsets` reads these two maps to know how
	// long. Appended in production order, so each partition's list stays in
	// ascending offset order.
	st.want = append(st.want, fx...)
	for _, f := range fx {
		st.byPart[f.partition] = append(st.byPart[f.partition], f)
	}

	// 3. A brand new member of the SAME group, told to start at the beginning:
	//    the committed offset must win, so it sees the 64 and none of the 512.
	col, err := runGroup(st.env, st.group, []string{st.topic}, extra, 90*time.Second, true)
	if err != nil {
		r.fail("second group instance: %v", err)
		return
	}
	r.check(len(col.msgs) == extra,
		"a NEW member of %s with Consumer.Offsets.Initial=OffsetOldest got exactly the %d new records (got %d) — the commit won",
		st.group, extra, len(col.msgs))

	byPart := byPartition(fx)
	perPart := map[int32][]*sarama.ConsumerMessage{}
	for _, m := range col.msgs {
		perPart[m.Partition] = append(perPart[m.Partition], m)
	}
	replayed := 0
	for part, got := range perPart {
		floor := committed[part]
		for _, m := range got {
			if m.Offset < floor {
				replayed++
			}
		}
		want := byPart[part]
		if len(got) != len(want) {
			r.fail("partition %d: resumed with %d records, want %d", part, len(got), len(want))
			continue
		}
		for i := range got {
			if b := want[i].compare(got[i]); len(b) > 0 {
				r.fail("partition %d record %d after resume: %s", part, i, strings.Join(b, ", "))
			}
		}
	}
	r.check(replayed == 0, "nothing below the committed offset was redelivered (%d replays)", replayed)
}

// ----------------------------------------------------------------- autocreate

func scenarioAutocreate(r *runner, st *state) {
	parts := int32(st.env.partsWant)
	topic := fmt.Sprintf("sarama-%s-autocreate", st.env.runID)
	const n = 32
	fx := buildFixtures(n, parts, "auto")

	tap := newTap()
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-autocreate", version: theVersion, apiVersionsReq: true, tap: tap})
	cfg.Producer.Partitioner = sarama.NewManualPartitioner

	// The topic has never been named to anything. sarama's first Metadata for
	// it carries allow_auto_topic_creation=true (Config.Metadata.AllowAutoTopicCreation,
	// the default), which is what makes the facade create the queue.
	p, err := sarama.NewSyncProducer(brokers(st.env), cfg)
	if err != nil {
		r.fail("NewSyncProducer: %v", err)
		return
	}
	msgs := make([]*sarama.ProducerMessage, len(fx))
	for i, f := range fx {
		msgs[i] = f.message(topic)
	}
	sent := r.deadline(fmt.Sprintf("produced %d records to %s, a topic that did not exist", n, topic),
		60*time.Second, func() error { return p.SendMessages(msgs) })
	_ = p.Close()
	if !sent {
		for _, l := range saramaLog.since(0, 6, "UNKNOWN_TOPIC", "LEADER_NOT_AVAILABLE", "error") {
			r.info("sarama: %s", l)
		}
		return
	}
	for i, f := range fx {
		f.offset = msgs[i].Offset
	}

	client, err := sarama.NewClient(brokers(st.env), newConfig(cfgOpts{
		clientID: "qk-sarama-autocreate-meta", version: theVersion, apiVersionsReq: true, tap: tap,
	}))
	if err != nil {
		r.fail("NewClient: %v", err)
		return
	}
	defer func() { _ = client.Close() }()
	ps, err := client.Partitions(topic)
	if err != nil {
		r.fail("Partitions(%s): %v", topic, err)
		return
	}
	r.check(len(ps) == int(parts), "the auto-created topic is %d partitions wide (QUEEN_KAFKA_DEFAULT_PARTITIONS), got %d", parts, len(ps))

	got, err := consumePartitions(st.env, topic, parts, n, 45*time.Second)
	if err != nil {
		r.fail("consume the auto-created topic: %v", err)
		return
	}
	r.check(len(got) == n, "read back %d of %d records from the auto-created topic", len(got), n)
	if bad := compareAll(fx, got); len(bad) > 0 {
		r.fail("auto-created topic round trip: %s", bad[0])
	} else {
		r.ok("auto-created topic round-tripped byte-exact")
	}
}

// -------------------------------------------------------------------- offsets

func scenarioOffsets(r *runner, st *state) {
	if !ensureProduced(r, st) {
		return
	}
	parts := int32(st.env.partsWant)
	tap := newTap()
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-offsets", version: theVersion, apiVersionsReq: true, tap: tap})
	client, err := sarama.NewClient(brokers(st.env), cfg)
	if err != nil {
		r.fail("NewClient: %v", err)
		return
	}
	defer func() { _ = client.Close() }()

	// GetOffset is ListOffsets: OffsetOldest is -2, OffsetNewest is -1.
	badOld, badNew := 0, 0
	for p := int32(0); p < parts; p++ {
		oldest, err := client.GetOffset(st.topic, p, sarama.OffsetOldest)
		if err != nil {
			r.fail("GetOffset(oldest, p%d): %v", p, err)
			return
		}
		newest, err := client.GetOffset(st.topic, p, sarama.OffsetNewest)
		if err != nil {
			r.fail("GetOffset(newest, p%d): %v", p, err)
			return
		}
		if oldest != 0 {
			badOld++
		}
		if newest != int64(len(st.byPart[p])) {
			badNew++
			r.info("p%d newest=%d, produced=%d", p, newest, len(st.byPart[p]))
		}
	}
	r.check(badOld == 0, "ListOffsets earliest is 0 on all %d written partitions", parts)
	r.check(badNew == 0, "ListOffsets latest equals the number of records produced, on all %d partitions", parts)

	// A topic nobody has written: earliest == latest == 0 on every partition.
	empty := fmt.Sprintf("sarama-%s-empty", st.env.runID)
	if err := client.RefreshMetadata(empty); err != nil {
		r.fail("RefreshMetadata(%s) (auto-create on a bare metadata request): %v", empty, err)
	} else {
		o, e1 := client.GetOffset(empty, 0, sarama.OffsetOldest)
		n, e2 := client.GetOffset(empty, 0, sarama.OffsetNewest)
		if e1 != nil || e2 != nil {
			r.fail("GetOffset on the never-written topic: %v / %v", e1, e2)
		} else {
			r.check(o == 0 && n == 0, "a never-written partition reports earliest=%d latest=%d", o, n)
		}
	}

	// Seek: a partition consumer started at an explicit offset.
	cons, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		r.fail("NewConsumerFromClient: %v", err)
		return
	}
	defer func() { _ = cons.Close() }()

	const seekTo = int64(7)
	pc, err := cons.ConsumePartition(st.topic, 0, seekTo)
	if err != nil {
		r.fail("ConsumePartition(p0, offset %d): %v", seekTo, err)
	} else {
		ok := r.deadline(fmt.Sprintf("a seek to offset %d delivers offset %d first", seekTo, seekTo), 20*time.Second, func() error {
			select {
			case m := <-pc.Messages():
				if m.Offset != seekTo {
					return fmt.Errorf("first message was offset %d", m.Offset)
				}
				want := st.byPart[0][seekTo]
				if b := want.compare(m); len(b) > 0 {
					return fmt.Errorf("not the record we produced there: %s", strings.Join(b, ", "))
				}
				return nil
			case e := <-pc.Errors():
				return e
			}
		})
		_ = ok
		r.info("high watermark reported by the partition consumer: %d", pc.HighWaterMarkOffset())
		_ = pc.Close()
	}

	// OffsetNewest as a seek: parks at the end, delivers nothing until a write.
	pcNew, err := cons.ConsumePartition(st.topic, 1, sarama.OffsetNewest)
	if err != nil {
		r.fail("ConsumePartition(p1, OffsetNewest): %v", err)
	} else {
		select {
		case m := <-pcNew.Messages():
			r.fail("a consumer seeked to OffsetNewest got a record at offset %d", m.Offset)
		case e := <-pcNew.Errors():
			r.fail("a consumer seeked to OffsetNewest errored: %v", e)
		case <-time.After(3 * time.Second):
			r.ok("a consumer seeked to OffsetNewest sits at the end and delivers nothing")
		}
		_ = pcNew.Close()
	}

	// Past the end. NOTE: this verdict is sarama's own — chooseStartingOffset
	// compares the requested offset against the bounds our ListOffsets just
	// gave it and refuses before any Fetch goes out. It is a ListOffsets test,
	// not an OFFSET_OUT_OF_RANGE test.
	beyond := int64(len(st.byPart[0])) + 100
	_, err = cons.ConsumePartition(st.topic, 0, beyond)
	r.check(err == sarama.ErrOffsetOutOfRange,
		"a seek past the end (offset %d) is refused with %v — sarama's own check against our ListOffsets bounds", beyond, err)
	tap.report()
}

// ------------------------------------------------------- the library defaults

func scenarioDefaults(r *runner, st *state) {
	// sarama.NewConfig() and nothing else: Version = DefaultVersion (2.8),
	// ApiVersionsRequest = true, acks = WaitForLocal, MaxOpenRequests = 5,
	// Idempotent = false. The only edits are the two a SyncProducer requires.
	cfg := sarama.NewConfig()
	cfg.ClientID = "qk-sarama-defaults"
	cfg.Producer.Return.Successes = true
	tap := newTap()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = &tapDialer{inner: dialerFor(cfg), tap: tap, sniff: true}

	r.info("sarama.NewConfig() defaults: Version=%s ApiVersionsRequest=%v RequiredAcks=%v Idempotent=%v MaxOpenRequests=%d",
		cfg.Version.String(), cfg.ApiVersionsRequest, cfg.Producer.RequiredAcks, cfg.Producer.Idempotent, cfg.Net.MaxOpenRequests)

	topic := fmt.Sprintf("sarama-%s-defaults", st.env.runID)
	const n = 64
	parts := int32(st.env.partsWant)
	fx := buildFixtures(n, parts, "def")

	p, err := sarama.NewSyncProducer(brokers(st.env), cfg)
	if err != nil {
		r.fail("default config: NewSyncProducer: %v", err)
		return
	}
	msgs := make([]*sarama.ProducerMessage, len(fx))
	for i, f := range fx {
		// The default partitioner is the hashing one, so partition is the
		// client's choice here; the fixture's own partition is not used.
		m := f.message(topic)
		m.Partition = 0
		msgs[i] = m
	}
	sent := r.deadline(fmt.Sprintf("default config produced %d records (hash partitioner)", n), 60*time.Second,
		func() error { return p.SendMessages(msgs) })
	_ = p.Close()
	if !sent {
		for _, l := range saramaLog.since(0, 6, "error", "Error", "kafka:") {
			r.info("sarama: %s", l)
		}
		tap.report()
		return
	}

	got, err := consumePartitionsCfg(cfg, st.env, topic, parts, n, 45*time.Second)
	if err != nil {
		r.fail("default config: consume back: %v", err)
		tap.report()
		return
	}
	r.check(len(got) == n, "default config read back %d of %d records", len(got), n)
	r.ok("the LIBRARY DEFAULT (Config.Version=%s, untouched) produces and consumes against the facade", cfg.Version.String())
	tap.report()
}

// -------------------------------------------------------------- version sweep

func scenarioVersionSweep(r *runner, st *state) {
	parts := int32(st.env.partsWant)
	sweep := []struct {
		v    sarama.KafkaVersion
		want string // "pass" or "fail"
		why  string
	}{
		{sarama.V0_10_2_0, "fail", "Produce v2 is a legacy message set, below the advertised floor of 3, and the clamp never raises a version"},
		{sarama.V0_11_0_0, "pass", "the first RecordBatch v2 release: Produce v3, Fetch v5"},
		{sarama.V1_1_0_0, "pass", "wants Fetch v7 (fetch sessions); clamped to v6"},
		{sarama.V2_1_0_0, "pass", "the floor for zstd in sarama"},
		{sarama.V2_8_0_0, "pass", "sarama's own DefaultVersion"},
		{sarama.V3_6_0_0, "pass", "wants Fetch v12 / Produce v10; clamped to v6 / v9"},
		{sarama.MaxVersion, "pass", "sarama's MaxVersion, the highest release it knows"},
	}
	for _, s := range sweep {
		tap := newTap()
		topic := fmt.Sprintf("sarama-%s-v%s", st.env.runID, strings.ReplaceAll(s.v.String(), ".", "-"))
		err := probeRoundTrip(st.env, s.v, tap, topic, parts)
		lines := tap.lines()
		verdict := "pass"
		if err != nil {
			verdict = "fail"
		}
		r.check(verdict == s.want, "Config.Version=%-8s produce+consume %s (expected %s) — %s",
			s.v.String(), strings.ToUpper(verdict), s.want, s.why)
		if err != nil {
			r.info("    %v", err)
		}
		for _, l := range lines {
			r.info("    %s", l)
		}
	}
}

// ----------------------------------------------- the trap: no ApiVersions call

func scenarioNoApiVersions(r *runner, st *state) {
	// The experiment this suite exists for. Config.ApiVersionsRequest = false
	// leaves brokerAPIVersions empty, restrictApiVersion has nothing to clamp
	// against, and sarama sends what Config.Version says: Produce v10, Fetch
	// v12. Both are outside the advertised windows, and the facade answers an
	// out-of-window version on an advertised key by closing the connection.
	tap := newTap()
	topic := fmt.Sprintf("sarama-%s-noapiversions", st.env.runID)
	mark := saramaLog.mark()

	err := probeRoundTripCfg(st.env, newConfig(cfgOpts{
		clientID:       "qk-sarama-noapiversions",
		version:        theVersion,
		apiVersionsReq: false,
		tap:            tap,
	}), topic, int32(st.env.partsWant))

	for _, l := range tap.lines() {
		r.info("wire: %s", l)
	}
	if err == nil {
		r.fail("ApiVersionsRequest=false at Config.Version=%s produced and consumed — the clamp was not load-bearing after all", theVersion.String())
		return
	}
	r.ok("ApiVersionsRequest=false at Config.Version=%s FAILS, as designed: %v", theVersion.String(), err)
	for _, l := range saramaLog.since(mark, 6, "EOF", "broker", "error", "Error", "closed") {
		r.info("sarama: %s", l)
	}
	r.info("the fix is the client's: leave Config.ApiVersionsRequest at its default true, or set Config.Version to a release inside the window (<= 1.0)")

	// ...and the same config at a version whose unclamped requests are inside
	// the window works, which is what makes the diagnosis unambiguous.
	tap2 := newTap()
	err2 := probeRoundTripCfg(st.env, newConfig(cfgOpts{
		clientID:       "qk-sarama-noapiversions-low",
		version:        sarama.V1_0_0_0,
		apiVersionsReq: false,
		tap:            tap2,
	}), topic+"-low", int32(st.env.partsWant))
	r.check(err2 == nil, "the SAME ApiVersionsRequest=false client at Config.Version=1.0.0 works (Produce v5, Fetch v6): %v", err2)
	for _, l := range tap2.lines() {
		r.info("wire: %s", l)
	}
}

// ---------------------------------------------------------------------- edges

func scenarioEdges(r *runner, st *state) {
	// 1. The idempotent producer. Until M7 F3, InitProducerId (key 22) was not
	//    advertised, an unadvertised key closed the connection, and sarama
	//    retried it Producer.Transaction.Retry.Max (50) times on 51 fresh
	//    connections before giving up. F3 advertises the key and enforces the
	//    sequence window, so this is now a SEND — one InitProducerId, one
	//    Produce, one connection.
	{
		tap := newTap()
		cfg := newConfig(cfgOpts{clientID: "qk-sarama-idempotent", version: theVersion, apiVersionsReq: true, tap: tap})
		cfg.Producer.Idempotent = true
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Net.MaxOpenRequests = 1
		mark := saramaLog.mark()
		var err error
		r.deadline("Producer.Idempotent=true completes rather than hanging", 60*time.Second, func() error {
			p, e := sarama.NewSyncProducer(brokers(st.env), cfg)
			if e != nil {
				err = e
				return nil
			}
			_, _, e = p.SendMessage(&sarama.ProducerMessage{
				Topic: fmt.Sprintf("sarama-%s-idem", st.env.runID),
				Value: sarama.ByteEncoder([]byte("nope")),
			})
			err = e
			_ = p.Close()
			return nil
		})
		r.check(err == nil, "Producer.Idempotent=true produces (M7 F3): %v", err)
		for _, l := range tap.lines() {
			r.info("wire: %s", l)
		}
		r.check(tap.requests(22) == 1,
			"exactly ONE InitProducerId was sent (51 before M7 F3): %d",
			tap.requests(22))
		for _, l := range saramaLog.since(mark, 4, "producer", "init-producer-id", "EOF", "broker") {
			r.info("sarama: %s", l)
		}
	}

	// 2. ClusterAdmin, WHICH NOW WORKS — and the reason it is checked here at
	//    all is that it used to be the loudest thing in this file. sarama's
	//    ListTopics is a Metadata request followed by a DescribeConfigs for
	//    every topic it found (admin.go: "In order to build TopicDetails we
	//    need to first get the list of all topics using a MetadataRequest and
	//    then get their configs using a DescribeConfigsRequest"), so before M7
	//    F1 the whole object was unreachable, not just its obviously
	//    administrative half: even the call that looks like pure Metadata was
	//    refused on the DescribeConfigs it makes second.
	//
	//    Every check below therefore asserts a SUCCESS and then asserts the
	//    ANSWER, because a call that returns nil error and an empty result
	//    would satisfy the inversion while proving nothing. The keys are
	//    ListGroups (16), CreateTopics (19), DeleteTopics (20) and
	//    DescribeConfigs (32); the `versions` scenario pins their windows.
	{
		cfg := newConfig(cfgOpts{clientID: "qk-sarama-admin", version: theVersion, apiVersionsReq: true})
		admin, err := sarama.NewClusterAdmin(brokers(st.env), cfg)
		if err != nil {
			r.fail("NewClusterAdmin: %v", err)
		} else {
			defer func() { _ = admin.Close() }()

			// ListTopics: Metadata + DescribeConfigs per topic, in one call.
			var topics map[string]sarama.TopicDetail
			var listErr error
			r.deadline("ClusterAdmin.ListTopics returns rather than hanging", 45*time.Second, func() error {
				topics, listErr = admin.ListTopics()
				return nil
			})
			if r.check(listErr == nil,
				"ClusterAdmin.ListTopics works — Metadata AND the DescribeConfigs it issues after it: %v", listErr) {
				r.check(len(topics) > 0, "ListTopics returned %d topic(s) with their configs", len(topics))
				if st.topic != "" {
					d, seen := topics[st.topic]
					if r.check(seen, "the topic this suite produced to (%s) is in the listing", st.topic) {
						r.check(d.NumPartitions == int32(st.env.partsWant),
							"...with %d partitions, the width the facade was booted at", d.NumPartitions)
					}
				}
			}

			// ListConsumerGroups: the answer is Queen's durable offsets index
			// plus this facade's live members, so it can only be checked
			// against a group after the group scenarios have run.
			var groups map[string]string
			var groupsErr error
			r.deadline("ClusterAdmin.ListConsumerGroups returns rather than hanging", 45*time.Second, func() error {
				groups, groupsErr = admin.ListConsumerGroups()
				return nil
			})
			if r.check(groupsErr == nil,
				"ClusterAdmin.ListConsumerGroups works (ListGroups, key 16): %v", groupsErr) {
				r.check(len(groups) > 0, "ListConsumerGroups returned %d group(s)", len(groups))
				if st.group != "" {
					proto, seen := groups[st.group]
					r.check(seen, "the group this suite formed (%s) is listed, protocol type %q", st.group, proto)
				} else {
					r.info("no group in this run's state (the group scenario did not run), so only the count is checked")
				}
			}

			// CreateTopic, then Metadata on the SAME name: the create is only
			// worth anything if a second client can see what it made, and a
			// facade that answered error_code 0 and created nothing would pass
			// a bare "did not return an error" inversion.
			//
			// NumPartitions is asked for as 4 and is NOT expected back as 4.
			// Queen has no declared per-topic width: the number every client
			// sees is max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS), so a
			// create's num_partitions is accepted, not acted on, and the next
			// Metadata reports the facade's width. That is a deliberate
			// deviation on PLAN_QUEEN_KAFKA.md's list, and it is asserted here
			// rather than avoided because a sarama user WILL pass a number and
			// needs this file to say what becomes of it.
			created := fmt.Sprintf("sarama-%s-created", st.env.runID)
			var createErr error
			r.deadline("ClusterAdmin.CreateTopic returns rather than hanging", 45*time.Second, func() error {
				createErr = admin.CreateTopic(created,
					&sarama.TopicDetail{NumPartitions: 4, ReplicationFactor: 1}, false)
				return nil
			})
			if r.check(createErr == nil, "ClusterAdmin.CreateTopic makes %s (CreateTopics, key 19): %v", created, createErr) {
				var after map[string]sarama.TopicDetail
				r.deadline("the created topic is visible to a second ListTopics", 45*time.Second, func() error {
					var e error
					after, e = admin.ListTopics()
					return e
				})
				d, seen := after[created]
				if r.check(seen, "%s exists after CreateTopic, without anyone producing to it", created) {
					r.check(d.NumPartitions == int32(st.env.partsWant),
						"...at the facade's own width of %d and not the 4 that were requested: "+
							"num_partitions is accepted and not acted on, by design", d.NumPartitions)
				}
			}

			// DescribeConfig on the topic just created rather than on the one
			// the produce scenario made, so this block stands on its own when
			// `edges` is run alone.
			var entries []sarama.ConfigEntry
			var cfgErr error
			r.deadline("ClusterAdmin.DescribeConfig returns rather than hanging", 45*time.Second, func() error {
				entries, cfgErr = admin.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: created})
				return nil
			})
			if r.check(cfgErr == nil, "ClusterAdmin.DescribeConfig answers for %s (DescribeConfigs, key 32): %v", created, cfgErr) {
				for _, e := range entries {
					r.info("config %s=%q (read_only=%v, source=%v)", e.Name, e.Value, e.ReadOnly, e.Source)
				}
				r.check(len(entries) > 0,
					"DescribeConfig reported %d config entrie(s) rather than an empty answer", len(entries))
			}

			// DeleteTopic closes the loop and leaves nothing behind: the run
			// created this name, so the run removes it.
			var delErr error
			r.deadline("ClusterAdmin.DeleteTopic returns rather than hanging", 45*time.Second, func() error {
				delErr = admin.DeleteTopic(created)
				return nil
			})
			r.check(delErr == nil, "ClusterAdmin.DeleteTopic removes %s again (DeleteTopics, key 20): %v", created, delErr)
		}
	}

	// 3. ...and the alternative that was the only thing that worked before M7,
	//    kept because it is still the cheapest way to ask these questions:
	//    sarama.Client is pure Metadata.
	{
		cfg := newConfig(cfgOpts{clientID: "qk-sarama-client-admin", version: theVersion, apiVersionsReq: true})
		client, err := sarama.NewClient(brokers(st.env), cfg)
		if err != nil {
			r.fail("NewClient: %v", err)
		} else {
			defer func() { _ = client.Close() }()
			r.deadline("sarama.Client.Topics() (Metadata only) lists topics without a DescribeConfigs per topic", 30*time.Second,
				func() error {
					ts, e := client.Topics()
					if e != nil {
						return e
					}
					if len(ts) == 0 {
						return fmt.Errorf("no topics returned")
					}
					r.info("Client.Topics() saw %d topics", len(ts))
					return nil
				})
			if st.topic != "" {
				r.deadline("sarama.Client.Partitions() works on a live topic", 30*time.Second, func() error {
					ps, e := client.Partitions(st.topic)
					if e != nil {
						return e
					}
					if len(ps) != st.env.partsWant {
						return fmt.Errorf("%d partitions, want %d", len(ps), st.env.partsWant)
					}
					return nil
				})
			}
		}
	}

	// 4. sarama's own config validation on the SASL default, which is why the
	//    SASL lane below sets SASLHandshakeV1. No network needed.
	{
		cfg := newConfig(cfgOpts{clientID: "qk-sarama-saslv0", version: theVersion, apiVersionsReq: true,
			saslUser: "x", saslPassword: "y", saslV0: true})
		err := cfg.Validate()
		r.check(err != nil,
			"sarama refuses Net.SASL.Version=SASLHandshakeV0 (its DEFAULT) together with ApiVersionsRequest: %v", err)
		r.info("that matters here: turning ApiVersionsRequest off to get SASL v0 also turns off the clamp — see the noapiversions scenario")
	}
}

// ------------------------------------------------------------ TLS + SASL/PLAIN

func scenarioSasl(r *runner, st *state) {
	if st.env.tlsBoot == "" {
		r.info("skipped: QUEEN_KAFKA_TLS_BOOTSTRAP is not set (no --m5 listener)")
		return
	}
	if st.env.saslToken == "" {
		r.info("skipped: QUEEN_KAFKA_SASL_TOKEN is not set")
		return
	}
	host := st.env.tlsBoot
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	// The rig's certificate has SANs kafka.example.com / shared.queenmq.cloud /
	// localhost / 127.0.0.1. Go sends no SNI for an IP literal, so ServerName is
	// set explicitly to a DNS SAN — the same thing compat/go/m5_test.go does,
	// and what makes QUEEN_KAFKA_FORWARD_SNI_HOST observable.
	tlsConf := &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if pool := certPool(st.env.tlsCert); pool != nil {
		tlsConf.RootCAs = pool
		r.info("verifying the listener against %s (ServerName=localhost)", st.env.tlsCert)
	} else {
		tlsConf.InsecureSkipVerify = true
		r.info("QUEEN_KAFKA_TLS_CERT unreadable or unset: falling back to InsecureSkipVerify")
	}

	topic := fmt.Sprintf("sarama-%s-sasl", st.env.runID)
	parts := int32(st.env.partsWant)
	tap := newTap()

	good := newConfig(cfgOpts{
		clientID: "qk-sarama-sasl", version: theVersion, apiVersionsReq: true, tap: tap,
		tlsConf: tlsConf, saslUser: "sarama", saslPassword: st.env.saslToken,
	})
	err := probeRoundTripAt(st.env.tlsBoot, good, topic, parts)
	r.check(err == nil, "SASL/PLAIN over TLS: produce and consume through %s: %v", st.env.tlsBoot, err)

	// The wrong password must be FATAL, not a retry loop.
	bad := newConfig(cfgOpts{
		clientID: "qk-sarama-sasl-wrong", version: theVersion, apiVersionsReq: true,
		tlsConf: tlsConf, saslUser: "sarama", saslPassword: st.env.saslToken + "-wrong",
	})
	bad.Metadata.Retry.Max = 0
	mark := saramaLog.mark()
	var wrongErr error
	r.deadline("a wrong SASL password fails fast rather than looping", 45*time.Second, func() error {
		c, e := sarama.NewClient([]string{st.env.tlsBoot}, bad)
		if e != nil {
			wrongErr = e
			return nil
		}
		_, wrongErr = c.Topics()
		_ = c.Close()
		return nil
	})
	r.check(wrongErr != nil, "a wrong SASL password is refused: %v", wrongErr)
	for _, l := range saramaLog.since(mark, 5, "SASL", "auth", "Failed", "error") {
		r.info("sarama: %s", l)
	}

	// No credential at all on a SASL listener.
	none := newConfig(cfgOpts{clientID: "qk-sarama-sasl-none", version: theVersion, apiVersionsReq: true, tlsConf: tlsConf})
	none.Metadata.Retry.Max = 0
	var noneErr error
	r.deadline("an unauthenticated client on the SASL listener fails fast", 45*time.Second, func() error {
		c, e := sarama.NewClient([]string{st.env.tlsBoot}, none)
		if e != nil {
			noneErr = e
			return nil
		}
		_, noneErr = c.Topics()
		_ = c.Close()
		return nil
	})
	r.check(noneErr != nil, "a client with no credential reads nothing from the SASL listener: %v", noneErr)
	tap.report()
}
