package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// ------------------------------------------------------------------- helpers

func showBytes(b []byte, null bool) string {
	if null {
		return "<null>"
	}
	return fmt.Sprintf("%q", string(b))
}

func showHeaders(hs []header) string {
	if len(hs) == 0 {
		return "<none>"
	}
	var parts []string
	for _, h := range hs {
		v := "<null>"
		if !h.valNull {
			v = fmt.Sprintf("%q", string(h.val))
		}
		parts = append(parts, fmt.Sprintf("%s=%s", h.key, v))
	}
	return strings.Join(parts, ",")
}

func showStrPtr(s *string) string {
	if s == nil {
		return "<null>"
	}
	return fmt.Sprintf("%q", *s)
}

// ensureTopic asks for metadata with auto-creation on until the topic exists
// with its full width. Kafka creates a topic on the metadata path and answers
// LEADER_NOT_AVAILABLE the first time; the facade creates it too. Nothing here
// is recorded: this is rig work, and its result is asserted by the scenarios
// that follow.
func ensureTopic(k *conn, topic string, parts int32) error {
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		req := kmsg.NewMetadataRequest()
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = &topic
		req.Topics = []kmsg.MetadataRequestTopic{t}
		req.AllowAutoTopicCreation = true
		resp, _, err := k.do(&req, 4)
		if err != nil {
			return err
		}
		md := resp.(*kmsg.MetadataResponse)
		if len(md.Topics) == 1 && md.Topics[0].ErrorCode == 0 && int32(len(md.Topics[0].Partitions)) >= parts {
			healthy := true
			for _, p := range md.Topics[0].Partitions {
				if p.ErrorCode != 0 {
					healthy = false
					last = fmt.Sprintf("partition %d: %s", p.Partition, errName(p.ErrorCode))
				}
			}
			if healthy {
				return nil
			}
		} else if len(md.Topics) == 1 {
			last = fmt.Sprintf("topic error %s, %d partitions",
				errName(md.Topics[0].ErrorCode), len(md.Topics[0].Partitions))
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("topic %s never became ready (%s)", topic, last)
}

func produceBatch(k *conn, topic string, partition int32, recs []record, codec int8, acks int16) (*kmsg.ProduceResponseTopicPartition, error) {
	batch, err := buildBatch(recs, codec)
	if err != nil {
		return nil, err
	}
	req := kmsg.NewProduceRequest()
	req.Acks = acks
	req.TimeoutMillis = 10000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = partition
	rp.Records = batch
	rt.Partitions = []kmsg.ProduceRequestTopicPartition{rp}
	req.Topics = []kmsg.ProduceRequestTopic{rt}
	resp, _, err := k.do(&req, 7)
	if err != nil {
		return nil, err
	}
	pr := resp.(*kmsg.ProduceResponse)
	if len(pr.Topics) != 1 || len(pr.Topics[0].Partitions) != 1 {
		return nil, fmt.Errorf("a produce response with %d topics", len(pr.Topics))
	}
	return &pr.Topics[0].Partitions[0], nil
}

type fetched struct {
	errCode   int16
	hw        int64
	lso       int64
	logStart  int64
	batches   []parsedBatch
	truncated bool
	raw       []byte
}

func fetchFrom(k *conn, topic string, partition int32, offset int64, maxWait int32) (*fetched, error) {
	req := kmsg.NewFetchRequest()
	req.ReplicaID = -1
	req.MaxWaitMillis = maxWait
	req.MinBytes = 1
	req.MaxBytes = 10 << 20
	req.IsolationLevel = 0
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = partition
	fp.FetchOffset = offset
	fp.LogStartOffset = -1
	fp.PartitionMaxBytes = 10 << 20
	ft.Partitions = []kmsg.FetchRequestTopicPartition{fp}
	req.Topics = []kmsg.FetchRequestTopic{ft}
	resp, body, err := k.doT(&req, 6, time.Duration(maxWait)*time.Millisecond+20*time.Second)
	if err != nil {
		return nil, err
	}
	fr := resp.(*kmsg.FetchResponse)
	if len(fr.Topics) != 1 || len(fr.Topics[0].Partitions) != 1 {
		return nil, fmt.Errorf("a fetch response with %d topics", len(fr.Topics))
	}
	p := fr.Topics[0].Partitions[0]
	out := &fetched{
		errCode: p.ErrorCode, hw: p.HighWatermark, lso: p.LastStableOffset,
		logStart: p.LogStartOffset, raw: body,
	}
	batches, truncated, err := parseBatches(p.RecordBatches)
	out.batches = batches
	out.truncated = truncated
	return out, err
}

// drain fetches from `from` until `want` records have arrived or the broker
// stops handing any over. The first response is the one whose bounds are
// recorded; the later ones only exist so that a broker that answers a fetch in
// several pieces is not reported as having lost records.
func drain(k *conn, topic string, partition int32, from int64, want int) (*fetched, []record, []int64, error) {
	first, err := fetchFrom(k, topic, partition, from, 1000)
	if err != nil {
		return nil, nil, nil, err
	}
	var recs []record
	var offs []int64
	collect := func(f *fetched) {
		for _, b := range f.batches {
			recs = append(recs, b.records...)
			offs = append(offs, b.offsets...)
		}
	}
	collect(first)
	next := from
	for len(recs) < want {
		if len(offs) > 0 {
			next = offs[len(offs)-1] + 1
		}
		if next >= first.hw {
			break
		}
		more, err := fetchFrom(k, topic, partition, next, 1000)
		if err != nil {
			return first, recs, offs, err
		}
		if len(more.batches) == 0 || more.errCode != 0 {
			break
		}
		collect(more)
	}
	return first, recs, offs, nil
}

// -------------------------------------------------- 1. produce/consume round trip

func init() {
	scenarios = append(scenarios, scenario{
		name: "produce-consume",
		desc: "keys, headers, timestamps, null key and null header value, gzip and zstd, contiguous offsets",
		run:  scenProduceConsume,
	})
}

func scenProduceConsume(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	for _, codec := range []int8{codecNone, codecGzip, codecZstd} {
		cn := codecName(codec)
		topic := c.topic("rt-" + cn)
		if err := ensureTopic(k, topic, c.parts); err != nil {
			c.rec.bad(cn+".ensure_topic", err)
			continue
		}

		first := []record{
			{key: []byte("k0"), val: []byte("v0"), ts: c.baseTS},
			{
				key: []byte("k1"),
				val: []byte(strings.Repeat("payload-", 32)),
				ts:  c.baseTS + 1000,
				headers: []header{
					{key: "h-first", val: []byte("one")},
					{key: "h-null", valNull: true},
					{key: "h-empty", val: []byte{}},
				},
			},
			{keyNull: true, val: []byte("v2"), ts: c.baseTS + 2000},
		}
		second := []record{
			{key: []byte("k3"), val: []byte("v3"), ts: c.baseTS + 3000},
			{key: []byte("k4"), valNull: true, ts: c.baseTS + 4000},
		}

		p1, err := produceBatch(k, topic, 0, first, codec, -1)
		if err != nil {
			c.rec.bad(cn+".produce1", err)
			continue
		}
		c.rec.add(cn+".produce1.error_code", "%s", errName(p1.ErrorCode))
		c.rec.add(cn+".produce1.base_offset", "%d", p1.BaseOffset)
		c.rec.add(cn+".produce1.log_append_time", "%d", p1.LogAppendTime)
		c.rec.add(cn+".produce1.log_start_offset", "%d", p1.LogStartOffset)

		p2, err := produceBatch(k, topic, 0, second, codec, -1)
		if err != nil {
			c.rec.bad(cn+".produce2", err)
			continue
		}
		c.rec.add(cn+".produce2.error_code", "%s", errName(p2.ErrorCode))
		c.rec.add(cn+".produce2.base_offset", "%d", p2.BaseOffset)
		c.rec.add(cn+".produce2.contiguous_with_first", "%t",
			p2.BaseOffset == p1.BaseOffset+int64(len(first)))

		f, recs, offs, err := drain(k, topic, 0, 0, len(first)+len(second))
		if err != nil {
			c.rec.bad(cn+".fetch", err)
			continue
		}
		c.rec.add(cn+".fetch.error_code", "%s", errName(f.errCode))
		c.rec.add(cn+".fetch.high_watermark", "%d", f.hw)
		c.rec.add(cn+".fetch.last_stable_offset", "%d", f.lso)
		c.rec.add(cn+".fetch.log_start_offset", "%d", f.logStart)
		c.rec.add(cn+".fetch.records", "%d", len(recs))
		c.rec.info(cn+".fetch.batches", "%d", len(f.batches))
		c.rec.add(cn+".fetch.truncated_tail", "%t", f.truncated)

		allCRC := true
		for _, b := range f.batches {
			if !b.crcOK {
				allCRC = false
			}
		}
		c.rec.add(cn+".fetch.crc_valid", "%t", allCRC)
		if len(f.batches) > 0 {
			b := f.batches[0]
			c.rec.add(cn+".fetch.batch0.magic", "%d", b.magic)
			c.rec.add(cn+".fetch.batch0.codec", "%s", codecName(b.codec))
			c.rec.add(cn+".fetch.batch0.timestamp_type", "%s",
				map[bool]string{true: "LogAppendTime", false: "CreateTime"}[b.attributes&0x8 != 0])
			c.rec.add(cn+".fetch.batch0.first_offset", "%d", b.firstOffset)
			c.rec.add(cn+".fetch.batch0.last_offset_delta", "%d", b.lastOffsetDelta)
			c.rec.add(cn+".fetch.batch0.first_timestamp", "%d", b.firstTS)
			c.rec.add(cn+".fetch.batch0.max_timestamp", "%d", b.maxTS)
			c.rec.add(cn+".fetch.batch0.producer_id", "%d", b.producerID)
			c.rec.add(cn+".fetch.batch0.producer_epoch", "%d", b.producerEpoch)
			c.rec.add(cn+".fetch.batch0.first_sequence", "%d", b.firstSequence)
			c.rec.add(cn+".fetch.batch0.num_records_field", "%d", b.numRecords)
			c.rec.info(cn+".fetch.batch0.leader_epoch", "%d", b.leaderEpoch)
		}

		want := append(append([]record{}, first...), second...)
		for i := range want {
			key := fmt.Sprintf("%s.fetch.record%d", cn, i)
			if i >= len(recs) {
				c.rec.add(key, "<missing>")
				continue
			}
			got := recs[i]
			c.rec.add(key+".offset", "%d", offs[i])
			c.rec.add(key+".key", "%s", showBytes(got.key, got.keyNull))
			c.rec.add(key+".value", "%s", showBytes(got.val, got.valNull))
			c.rec.add(key+".timestamp", "%d", got.ts)
			c.rec.add(key+".timestamp_matches_produced", "%t", got.ts == want[i].ts)
			c.rec.add(key+".headers", "%s", showHeaders(got.headers))
		}
	}

	// acks=0 is fire and forget: Kafka writes no response frame at all, and
	// the facade must not either — a stray frame would desynchronise every
	// later request on the connection. The proof is that the next frame to
	// arrive carries the NEXT request's correlation id.
	ackTopic := c.topic("rt-acks0")
	if err := ensureTopic(k, ackTopic, c.parts); err != nil {
		c.rec.bad("acks0.ensure_topic", err)
		return
	}
	batch, err := buildBatch([]record{
		{key: []byte("a0"), val: []byte("fire and forget"), ts: c.baseTS},
	}, codecNone)
	if err != nil {
		c.rec.bad("acks0.build", err)
		return
	}
	preq := kmsg.NewProduceRequest()
	preq.Acks = 0
	preq.TimeoutMillis = 10000
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = ackTopic
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition = 0
	pp.Records = batch
	pt.Partitions = []kmsg.ProduceRequestTopicPartition{pp}
	preq.Topics = []kmsg.ProduceRequestTopic{pt}
	if err := k.send(&preq, 7); err != nil {
		c.rec.bad("acks0.send", err)
		return
	}
	produceCorr := k.corr
	mreq := kmsg.NewMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &ackTopic
	mreq.Topics = []kmsg.MetadataRequestTopic{mt}
	if err := k.send(&mreq, 4); err != nil {
		c.rec.bad("acks0.followup_send", err)
		return
	}
	corr, _, err := k.recvFrame(20 * time.Second)
	if err != nil {
		c.rec.bad("acks0.followup", err)
		return
	}
	switch corr {
	case produceCorr:
		c.rec.add("acks0.next_frame", "a response to the acks=0 produce itself")
	case produceCorr + 1:
		c.rec.add("acks0.next_frame", "the follow-up request's response, so the produce was unanswered")
	default:
		c.rec.add("acks0.next_frame", "correlation id %d, neither request's", corr)
	}
	// The write still has to have happened.
	if f, err := fetchFrom(k, ackTopic, 0, 0, 2000); err != nil {
		c.rec.bad("acks0.readback", err)
	} else {
		n := 0
		for _, b := range f.batches {
			n += len(b.records)
		}
		c.rec.add("acks0.readback.error_code", "%s", errName(f.errCode))
		c.rec.add("acks0.readback.high_watermark", "%d", f.hw)
		c.rec.add("acks0.readback.records", "%d", n)
	}
}

// ------------------------------------------------------------ 2. ListOffsets

func init() {
	scenarios = append(scenarios, scenario{
		name: "listoffsets",
		desc: "earliest and latest on a written and an unwritten partition, and a concrete timestamp",
		run:  scenListOffsets,
	})
}

func listOffsets(k *conn, version int16, topic string, partition int32, ts int64) (*kmsg.ListOffsetsResponseTopicPartition, error) {
	req := kmsg.NewListOffsetsRequest()
	req.ReplicaID = -1
	req.IsolationLevel = 0
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition = partition
	lp.Timestamp = ts
	lp.MaxNumOffsets = 1
	lp.CurrentLeaderEpoch = -1
	lt.Partitions = []kmsg.ListOffsetsRequestTopicPartition{lp}
	req.Topics = []kmsg.ListOffsetsRequestTopic{lt}
	resp, _, err := k.do(&req, version)
	if err != nil {
		return nil, err
	}
	lr := resp.(*kmsg.ListOffsetsResponse)
	if len(lr.Topics) != 1 || len(lr.Topics[0].Partitions) != 1 {
		return nil, fmt.Errorf("a ListOffsets response with %d topics", len(lr.Topics))
	}
	return &lr.Topics[0].Partitions[0], nil
}

func scenListOffsets(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	topic := c.topic("lo")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("ensure_topic", err)
		return
	}
	recs := []record{
		{key: []byte("a"), val: []byte("1"), ts: c.baseTS},
		{key: []byte("b"), val: []byte("2"), ts: c.baseTS + 1000},
		{key: []byte("c"), val: []byte("3"), ts: c.baseTS + 2000},
	}
	p, err := produceBatch(k, topic, 0, recs, codecNone, -1)
	if err != nil {
		c.rec.bad("produce", err)
		return
	}
	c.rec.add("produce.error_code", "%s", errName(p.ErrorCode))
	c.rec.add("produce.base_offset", "%d", p.BaseOffset)

	probes := []struct {
		key string
		par int32
		ts  int64
	}{
		{"written.earliest", 0, -2},
		{"written.latest", 0, -1},
		{"unwritten.earliest", 1, -2},
		{"unwritten.latest", 1, -1},
	}
	for _, pr := range probes {
		r, err := listOffsets(k, 1, topic, pr.par, pr.ts)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(r.ErrorCode))
		c.rec.add(pr.key+".offset", "%d", r.Offset)
		c.rec.add(pr.key+".timestamp", "%d", r.Timestamp)
	}

	// The documented deviation: a concrete timestamp. baseTS+1000 is exactly
	// the second record's timestamp, so the correct Kafka answer is offset 1.
	for _, pr := range []struct {
		key string
		ts  int64
	}{
		{"ts.concrete.exact", c.baseTS + 1000},
		{"ts.concrete.before_all", c.baseTS - 10_000},
		{"ts.concrete.after_all", c.baseTS + 60_000},
	} {
		r, err := listOffsets(k, 1, topic, 0, pr.ts)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(r.ErrorCode))
		c.rec.add(pr.key+".offset", "%d", r.Offset)
		c.rec.add(pr.key+".timestamp", "%d", r.Timestamp)
	}

	// v5 carries a leader epoch, which is the one field a fetching client can
	// use to detect truncation.
	if r, err := listOffsets(k, 5, topic, 0, -1); err != nil {
		c.rec.bad("v5.latest", err)
	} else {
		c.rec.add("v5.latest.error_code", "%s", errName(r.ErrorCode))
		c.rec.add("v5.latest.offset", "%d", r.Offset)
		c.rec.add("v5.latest.timestamp", "%d", r.Timestamp)
		c.rec.add("v5.latest.leader_epoch", "%d", r.LeaderEpoch)
	}

	// Partitions outside the width metadata reports. The topic exists; these
	// partitions of it do not.
	for _, pr := range []struct {
		key  string
		part int32
		ts   int64
	}{
		{"one_past_width.earliest", c.parts, -2},
		{"one_past_width.latest", c.parts, -1},
		{"far_outside_width.latest", 4242, -1},
	} {
		r, err := listOffsets(k, 1, topic, pr.part, pr.ts)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(r.ErrorCode))
		c.rec.add(pr.key+".offset", "%d", r.Offset)
	}

	// A topic that was never created and is never asked for with
	// auto-creation on: ListOffsets must not invent it.
	nope := c.topic("lo-never-created")
	for _, pr := range []struct {
		key string
		ts  int64
	}{
		{"nonexistent.latest", -1},
		{"nonexistent.earliest", -2},
		{"nonexistent.concrete", c.baseTS},
	} {
		r, err := listOffsets(k, 1, nope, 0, pr.ts)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(r.ErrorCode))
		c.rec.add(pr.key+".offset", "%d", r.Offset)
	}
}

// ---------------------------------------------------------- 3. fetch past the end

func init() {
	scenarios = append(scenarios, scenario{
		name: "fetch-bounds",
		desc: "at the high watermark, past it, negative, an empty partition, an unknown partition and an unknown topic",
		run:  scenFetchBounds,
	})
}

func scenFetchBounds(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	topic := c.topic("fb")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("ensure_topic", err)
		return
	}
	recs := []record{
		{key: []byte("a"), val: []byte("1"), ts: c.baseTS},
		{key: []byte("b"), val: []byte("2"), ts: c.baseTS + 1},
		{key: []byte("c"), val: []byte("3"), ts: c.baseTS + 2},
	}
	if p, err := produceBatch(k, topic, 0, recs, codecNone, -1); err != nil {
		c.rec.bad("produce", err)
		return
	} else if p.ErrorCode != 0 {
		c.rec.bad("produce", fmt.Errorf("error %s", errName(p.ErrorCode)))
		return
	}

	probes := []struct {
		key   string
		topic string
		part  int32
		off   int64
	}{
		{"at_watermark", topic, 0, 3},
		{"one_past_watermark", topic, 0, 4},
		{"far_past_watermark", topic, 0, 103},
		{"negative_offset", topic, 0, -1},
		{"empty_partition", topic, 7, 0},
		{"empty_partition_past_end", topic, 7, 5},
		{"one_past_the_topic_width", topic, 8, 0},
		{"unknown_partition", topic, 4242, 0},
		{"unknown_topic", c.topic("fb-never-created"), 0, 0},
	}
	for _, pr := range probes {
		f, err := fetchFrom(k, pr.topic, pr.part, pr.off, 500)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(f.errCode))
		c.rec.add(pr.key+".high_watermark", "%d", f.hw)
		c.rec.add(pr.key+".log_start_offset", "%d", f.logStart)
		c.rec.add(pr.key+".last_stable_offset", "%d", f.lso)
		n := 0
		for _, b := range f.batches {
			n += len(b.records)
		}
		c.rec.add(pr.key+".records", "%d", n)
		c.rec.info(pr.key+".raw_hex", "%s", hex.EncodeToString(f.raw[:min(56, len(f.raw))]))
	}

	// Producing outside the advertised width is the same question from the
	// other end: metadata says the topic is `parts` wide.
	for _, pr := range []struct {
		key  string
		part int32
	}{
		{"produce.one_past_the_topic_width", c.parts},
		{"produce.far_outside_the_topic_width", 4242},
	} {
		one := []record{{key: []byte("out"), val: []byte("of range"), ts: c.baseTS}}
		p, err := produceBatch(k, topic, pr.part, one, codecNone, -1)
		if err != nil {
			c.rec.bad(pr.key, err)
			continue
		}
		c.rec.add(pr.key+".error_code", "%s", errName(p.ErrorCode))
		c.rec.add(pr.key+".base_offset", "%d", p.BaseOffset)
	}

	// And whether metadata then reports a wider topic than it did before.
	if md, err := metadataFor(k, 4, false, topic); err != nil {
		c.rec.bad("width_after_out_of_range_produce", err)
	} else if len(md.Topics) == 1 {
		c.rec.add("width_after_out_of_range_produce", "%d", len(md.Topics[0].Partitions))
	}
}

// ----------------------------------------------------------------- 5. Metadata

func init() {
	scenarios = append(scenarios, scenario{
		name: "metadata",
		desc: "an unknown topic with auto-creation refused, an internal topic, and the all-topics listing",
		run:  scenMetadata,
	})
}

func metadataFor(k *conn, version int16, autoCreate bool, topics ...string) (*kmsg.MetadataResponse, error) {
	req := kmsg.NewMetadataRequest()
	req.AllowAutoTopicCreation = autoCreate
	if topics != nil {
		for i := range topics {
			t := kmsg.NewMetadataRequestTopic()
			t.Topic = &topics[i]
			req.Topics = append(req.Topics, t)
		}
	}
	resp, _, err := k.do(&req, version)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.MetadataResponse), nil
}

func recordTopicMetadata(c *runctx, prefix string, md *kmsg.MetadataResponse) {
	if len(md.Topics) != 1 {
		c.rec.add(prefix+".topics_in_response", "%d", len(md.Topics))
		return
	}
	t := md.Topics[0]
	c.rec.add(prefix+".error_code", "%s", errName(t.ErrorCode))
	c.rec.add(prefix+".name", "%s", showStrPtr(t.Topic))
	c.rec.add(prefix+".is_internal", "%t", t.IsInternal)
	c.rec.add(prefix+".partitions", "%d", len(t.Partitions))
	if len(t.Partitions) > 0 {
		sorted := append([]kmsg.MetadataResponseTopicPartition{}, t.Partitions...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Partition < sorted[j].Partition })
		p := sorted[0]
		c.rec.add(prefix+".p0.error_code", "%s", errName(p.ErrorCode))
		c.rec.add(prefix+".p0.replicas", "%d", len(p.Replicas))
		c.rec.add(prefix+".p0.isr", "%d", len(p.ISR))
		c.rec.add(prefix+".p0.offline_replicas", "%d", len(p.OfflineReplicas))
		c.rec.add(prefix+".p0.leader_is_a_known_broker", "%t", knownBroker(md, p.Leader))
		c.rec.add(prefix+".p0.leader_epoch", "%d", p.LeaderEpoch)
		ids := map[int32]bool{}
		contiguous := true
		for i, q := range sorted {
			ids[q.Partition] = true
			if int32(i) != q.Partition {
				contiguous = false
			}
		}
		c.rec.add(prefix+".partition_ids_are_0_to_n", "%t", contiguous)
	}
}

func knownBroker(md *kmsg.MetadataResponse, id int32) bool {
	for _, b := range md.Brokers {
		if b.NodeID == id {
			return true
		}
	}
	return false
}

func scenMetadata(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	// A topic that exists, for the shape of a normal answer.
	known := c.topic("md")
	if err := ensureTopic(k, known, c.parts); err != nil {
		c.rec.bad("ensure_topic", err)
		return
	}
	if md, err := metadataFor(k, 4, false, known); err != nil {
		c.rec.bad("known", err)
	} else {
		recordTopicMetadata(c, "known", md)
		c.rec.info("known.brokers", "%d", len(md.Brokers))
	}
	if md, err := metadataFor(k, 9, false, known); err != nil {
		c.rec.bad("known_v9", err)
	} else {
		recordTopicMetadata(c, "known_v9", md)
	}

	// The point of the scenario: auto-creation refused on the wire.
	unknown := c.topic("md-unknown")
	if md, err := metadataFor(k, 4, false, unknown); err != nil {
		c.rec.bad("noautocreate", err)
	} else {
		recordTopicMetadata(c, "noautocreate", md)
	}
	// And it must still not exist afterwards.
	if md, err := metadataFor(k, 4, false, unknown); err != nil {
		c.rec.bad("noautocreate_again", err)
	} else {
		recordTopicMetadata(c, "noautocreate_again", md)
	}
	// ... nor may a ListOffsets on it succeed.
	if r, err := listOffsets(k, 1, unknown, 0, -1); err != nil {
		c.rec.bad("noautocreate.listoffsets", err)
	} else {
		c.rec.add("noautocreate.listoffsets.error_code", "%s", errName(r.ErrorCode))
	}

	// An internal topic. __consumer_offsets exists on Kafka as soon as a
	// group has committed; the group scenario runs before this one.
	if md, err := metadataFor(k, 4, false, "__consumer_offsets"); err != nil {
		c.rec.bad("internal", err)
	} else {
		recordTopicMetadata(c, "internal", md)
	}
	if md, err := metadataFor(k, 4, true, "__consumer_offsets"); err != nil {
		c.rec.bad("internal_autocreate", err)
	} else {
		recordTopicMetadata(c, "internal_autocreate", md)
	}

	// The all-topics listing.
	if md, err := metadataFor(k, 4, false); err != nil {
		c.rec.bad("alltopics", err)
	} else {
		hasInternal, hasOurs := false, false
		for _, t := range md.Topics {
			if t.Topic == nil {
				continue
			}
			if *t.Topic == "__consumer_offsets" {
				hasInternal = true
			}
			if *t.Topic == known {
				hasOurs = true
			}
		}
		c.rec.add("alltopics.has_consumer_offsets", "%t", hasInternal)
		c.rec.add("alltopics.has_our_topic", "%t", hasOurs)
		c.rec.add("alltopics.is_null_request_all_topics", "%t", len(md.Topics) > 0)
		c.rec.info("alltopics.count", "%d", len(md.Topics))
	}
}

// --------------------------------------------------------------- 6. ApiVersions

func init() {
	scenarios = append(scenarios, scenario{
		name: "apiversions",
		desc: "v0, v3 and an absurd version: the byte-level shape of the fallback",
		run:  scenApiVersions,
	})
}

// analyzeApiVersions decodes an ApiVersions response body (header already
// stripped) without trusting the version it was asked at, which is the whole
// question for the fallback.
func analyzeApiVersions(body []byte) apiVersionsShape {
	if len(body) < 2 {
		return apiVersionsShape{shape: "too short", trailing: len(body)}
	}
	errCode := int16(binary.BigEndian.Uint16(body[:2]))
	rest := body[2:]
	// Non-flexible: an int32 array length then 6 bytes per entry.
	if len(rest) >= 4 {
		n := int32(binary.BigEndian.Uint32(rest[:4]))
		if n >= 0 && n < 1000 && int(n)*6+4 <= len(rest) {
			after := len(rest) - 4 - int(n)*6
			switch after {
			case 0:
				return apiVersionsShape{"v0 (error+array, no throttle)", errCode, int(n), 0, 0}
			case 4:
				return apiVersionsShape{"v1/v2 (error+array+throttle)", errCode, int(n), 0, 0}
			default:
				return apiVersionsShape{
					fmt.Sprintf("error+array+%d trailing bytes", after), errCode, int(n), after, 0,
				}
			}
		}
	}
	// Flexible: a compact array length (uvarint n+1), 6 bytes plus a tag
	// buffer per entry, then throttle_time_ms and the response's own tag
	// buffer. The tags are where Kafka puts supported and finalized features,
	// so their CONTENT is broker-specific and only their presence is
	// comparable.
	r := &reader{src: rest}
	n := int(r.uvarint())
	if r.err == nil && n >= 1 {
		n--
		ok := true
		for i := 0; i < n && ok; i++ {
			r.take(6)
			next, err := skipTags(r.src)
			if err != nil || r.err != nil {
				ok = false
				break
			}
			r.src = next
		}
		if ok && r.err == nil {
			r.i32() // throttle_time_ms
			tags := 0
			if r.err == nil {
				t := &reader{src: r.src}
				count := t.uvarint()
				for i := uint64(0); i < count; i++ {
					t.uvarint()
					size := t.uvarint()
					t.take(int(size))
				}
				if t.err == nil {
					tags = int(count)
					r.src = t.src
				}
			}
			if r.err == nil {
				return apiVersionsShape{
					"v3 flexible (compact array, throttle, response tag buffer)",
					errCode, n, len(r.src), tags,
				}
			}
		}
	}
	return apiVersionsShape{"undecodable as any ApiVersions shape", errCode, 0, len(rest), 0}
}

// apiVersionsShape is what the byte-level comparison is about: the layout the
// broker chose, not the contents. The number of api keys and the number of
// response tags are broker-specific by nature — Kafka 3.9 attaches its feature
// tags here and the facade has no features to attach — so they are carried
// separately and reported as information.
type apiVersionsShape struct {
	shape    string
	errCode  int16
	nkeys    int
	trailing int
	tags     int
}

func scenApiVersions(c *runctx) {
	// v0 and v3 through the normal path.
	for _, v := range []int16{0, 3} {
		k, err := c.target.dial()
		if err != nil {
			c.rec.bad(fmt.Sprintf("v%d.dial", v), err)
			continue
		}
		req := kmsg.NewApiVersionsRequest()
		req.ClientSoftwareName = "qk-diff"
		req.ClientSoftwareVersion = "0"
		resp, body, err := k.do(&req, v)
		if err != nil {
			c.rec.bad(fmt.Sprintf("v%d", v), err)
			k.Close()
			continue
		}
		av := resp.(*kmsg.ApiVersionsResponse)
		s := analyzeApiVersions(body)
		c.rec.add(fmt.Sprintf("v%d.error_code", v), "%s", errName(av.ErrorCode))
		c.rec.add(fmt.Sprintf("v%d.raw_error_code", v), "%s", errName(s.errCode))
		c.rec.add(fmt.Sprintf("v%d.shape", v), "%s", s.shape)
		c.rec.add(fmt.Sprintf("v%d.trailing_bytes", v), "%d", s.trailing)
		c.rec.info(fmt.Sprintf("v%d.response_tags", v), "%d", s.tags)
		c.rec.info(fmt.Sprintf("v%d.api_keys", v), "%d (raw %d)", len(av.ApiKeys), s.nkeys)
		// Whether the keys this suite depends on are advertised at all is a
		// fact about the facade, not a divergence: kafka advertises more.
		c.rec.info(fmt.Sprintf("v%d.keys_hex_head", v), "%s", hex.EncodeToString(body[:min(16, len(body))]))
		k.Close()
	}

	// The absurd version. Its header is written flexible because the
	// ApiVersions spec says versions 3+ are flexible, and 32767 is 3+ — which
	// is exactly what a broker's own header parser concludes.
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("absurd.dial", err)
		return
	}
	defer k.Close()

	var frame []byte
	frame = append(frame, 0, 0, 0, 0) // size
	frame = binary.BigEndian.AppendUint16(frame, 18)
	frame = binary.BigEndian.AppendUint16(frame, uint16(int16(32767)))
	frame = binary.BigEndian.AppendUint32(frame, 1) // correlation id
	cid := "qk-diff"
	frame = binary.BigEndian.AppendUint16(frame, uint16(len(cid)))
	frame = append(frame, cid...)
	frame = append(frame, 0)                      // request header tag buffer
	frame = append(frame, byte(len("qk-diff")+1)) // compact client software name
	frame = append(frame, "qk-diff"...)
	frame = append(frame, byte(len("0")+1)) // compact client software version
	frame = append(frame, "0"...)
	frame = append(frame, 0) // request body tag buffer
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))

	if err := k.c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.rec.bad("absurd.deadline", err)
		return
	}
	if _, err := k.c.Write(frame); err != nil {
		c.rec.bad("absurd.write", err)
		return
	}
	body, err := k.recvRaw(false, 10*time.Second)
	if err != nil {
		c.rec.add("absurd.answer", "no response: %v", err)
		return
	}
	s := analyzeApiVersions(body)
	c.rec.add("absurd.answered", "yes")
	c.rec.add("absurd.error_code", "%s", errName(s.errCode))
	c.rec.add("absurd.error_code_hex", "%s", hex.EncodeToString(body[:min(2, len(body))]))
	c.rec.add("absurd.shape", "%s", s.shape)
	c.rec.add("absurd.trailing_bytes", "%d", s.trailing)
	c.rec.add("absurd.body_len_matches_shape", "%t", s.trailing == 0)
	// A client that gets the fallback needs to be told which ApiVersions
	// version to retry at, and the Java client reads exactly this entry
	// (NetworkClient.handleApiVersionsResponse) before falling back to v0.
	c.rec.add("absurd.names_the_apiversions_key", "%t", s.nkeys > 0)
	c.rec.info("absurd.api_keys", "%d", s.nkeys)
	c.rec.info("absurd.body_len", "%d", len(body))
	c.rec.info("absurd.head_hex", "%s", hex.EncodeToString(body[:min(24, len(body))]))

	// The connection has to survive it: a client that gets a fallback and
	// then reconnects is doing the right thing, but a broker that hangs up is
	// a different behaviour and clients notice.
	req := kmsg.NewApiVersionsRequest()
	if _, _, err := k.do(&req, 0); err != nil {
		c.rec.add("absurd.connection_usable_after", "no: %v", err)
	} else {
		c.rec.add("absurd.connection_usable_after", "yes")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
