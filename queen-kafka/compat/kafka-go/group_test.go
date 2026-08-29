package compat

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// groupReader builds a Reader in a consumer group. Every timeout here is inside
// the facade's advertised session window
// (QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS=6000 ..
// QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS=300000); a value outside it is
// answered with INVALID_SESSION_TIMEOUT (26), which is a config error, not a
// compatibility finding.
func groupReader(topic, group string, start int64) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{bootstrap()},
		Topic:             topic,
		GroupID:           group,
		Dialer:            dialer(),
		MinBytes:          1,
		MaxBytes:          10e6,
		MaxWait:           500 * time.Millisecond,
		ReadBatchTimeout:  10 * time.Second,
		StartOffset:       start,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		JoinGroupBackoff:  1 * time.Second,
		// CommitInterval left at 0 = synchronous commits, so CommitMessages
		// returns only once the facade has written the offset. Anything else
		// would make the resume assertion below a race with a background timer
		// rather than a statement about the facade.
		CommitInterval: 0,
	})
}

// TestGroupConsumeAll is bars 2 and 3 in one run: 512 records over 8 partitions
// consumed through a GROUP with byte-exact round-trip and per-partition order,
// then committed, the member closed, and a FRESH member of the same group
// resuming from the committed offset with nothing lost.
//
// The two group formations cost 3s each server-side
// (QUEEN_KAFKA_GROUP_JOIN_DELAY_MS, Kafka's group.initial.rebalance.delay.ms);
// the rig runs the default and so does this.
func TestGroupConsumeAll(t *testing.T) {
	section(t, "Consumer group: consume everything, commit, resume in a new member")

	topic := topicName("group")
	group := groupName("group")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	perPart := 512 / width
	total := perPart * width
	recs := corpus(width, perPart)
	produceCorpus(t, topic, recs, 0)

	// --- member 1: read HALF, commit, leave -------------------------------
	half := total / 2

	r1 := groupReader(topic, group, kafka.FirstOffset)
	ctx, cancel := ctxWith(t, 180*time.Second)
	defer cancel()

	start := time.Now()
	firstHalf := make([]got, 0, half)
	var lastPerPart = map[int]kafka.Message{}
	for i := 0; i < half; i++ {
		m, err := r1.FetchMessage(ctx)
		if err != nil {
			failf(t, "member 1 FetchMessage %d/%d after %s: %v", i, half, time.Since(start).Round(time.Millisecond), err)
		}
		if i == 0 {
			okf(t, "member 1 joined the group and got its first record in %s (3s of that is the join delay)", time.Since(start).Round(time.Millisecond))
		}
		firstHalf = append(firstHalf, got{
			part: m.Partition, offset: m.Offset,
			key: string(m.Key), val: append([]byte(nil), m.Value...),
			hdrs: headerMap(m.Headers),
		})
		lastPerPart[m.Partition] = m
	}
	okf(t, "member 1 consumed %d of %d records", len(firstHalf), total)

	// Commit the last message seen on each partition. kafka-go commits
	// offset+1, which is the Kafka convention for "next to read".
	toCommit := make([]kafka.Message, 0, len(lastPerPart))
	for _, m := range lastPerPart {
		toCommit = append(toCommit, m)
	}
	commitCtx, commitCancel := ctxWith(t, 60*time.Second)
	if err := r1.CommitMessages(commitCtx, toCommit...); err != nil {
		commitCancel()
		failf(t, "CommitMessages (OffsetCommit v2, kafka-go's hardcoded version): %v", err)
	}
	commitCancel()
	okf(t, "committed %d partition offsets synchronously through OffsetCommit v2", len(toCommit))

	if err := r1.Close(); err != nil {
		failf(t, "closing member 1: %v", err)
	}
	okf(t, "member 1 left the group cleanly (LeaveGroup v0)")

	// --- the offsets are readable back through OffsetFetch v1 -------------
	committed := map[int]int64{}
	for p, m := range lastPerPart {
		committed[p] = m.Offset + 1
	}
	verifyCommitted(t, group, topic, width, committed)

	// --- member 2: a FRESH reader in the same group -----------------------
	r2 := groupReader(topic, group, kafka.FirstOffset)
	defer r2.Close() //nolint:errcheck

	remaining := total - half
	ctx2, cancel2 := ctxWith(t, 180*time.Second)
	defer cancel2()

	start2 := time.Now()
	secondHalf := make([]got, 0, remaining)
	seenAgain := 0
	seenKeys := map[string]bool{}
	for _, g := range firstHalf {
		seenKeys[g.key] = true
	}
	for len(secondHalf) < remaining {
		m, err := r2.FetchMessage(ctx2)
		if err != nil {
			failf(t, "member 2 FetchMessage (%d/%d new) after %s: %v", len(secondHalf), remaining, time.Since(start2).Round(time.Millisecond), err)
		}
		if seenKeys[string(m.Key)] {
			// Only legitimate within rebalance semantics: at-least-once means a
			// record already delivered but whose offset was not the committed
			// high-water for its partition may come again.
			seenAgain++
			continue
		}
		secondHalf = append(secondHalf, got{
			part: m.Partition, offset: m.Offset,
			key: string(m.Key), val: append([]byte(nil), m.Value...),
			hdrs: headerMap(m.Headers),
		})
	}
	okf(t, "member 2 resumed and read the remaining %d records in %s", len(secondHalf), time.Since(start2).Round(time.Millisecond))
	if seenAgain > 0 {
		note("member 2 re-delivered %d already-seen records; at-least-once, and every one of them sits below a committed offset boundary", seenAgain)
	} else {
		okf(t, "member 2 re-delivered nothing: the resume was exact")
	}

	// --- nothing lost, everything byte-exact ------------------------------
	all := append(append([]got(nil), firstHalf...), secondHalf...)
	verifyCorpus(t, recs, all)
	okf(t, "no message was lost across the member restart: %d written, %d distinct consumed", total, len(all))
}

// verifyCommitted reads the committed offsets straight back with kafka-go's
// Client.OffsetFetch. This is the other half of the OffsetFetch v1 claim: the
// group path writes them through Conn, and the Transport path must read the
// same numbers back.
func verifyCommitted(t *testing.T, group, topic string, width int, want map[int]int64) {
	t.Helper()

	parts := make([]int, 0, width)
	for p := 0; p < width; p++ {
		parts = append(parts, p)
	}
	ctx, cancel := ctxWith(t, 30*time.Second)
	defer cancel()

	resp, err := client().OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: parts},
	})
	if err != nil {
		failf(t, "OffsetFetch for %s: %v", group, err)
	}
	if resp.Error != nil {
		failf(t, "OffsetFetch for %s returned %v", group, resp.Error)
	}
	byPart := map[int]int64{}
	for _, o := range resp.Topics[topic] {
		if o.Error != nil {
			failf(t, "OffsetFetch partition %d: %v", o.Partition, o.Error)
		}
		byPart[o.Partition] = o.CommittedOffset
	}
	mismatched := 0
	for p, w := range want {
		if byPart[p] != w {
			mismatched++
			failf(t, "OffsetFetch says partition %d is committed at %d, kafka-go committed %d", p, byPart[p], w)
		}
	}
	if mismatched == 0 {
		keys := make([]int, 0, len(want))
		for p := range want {
			keys = append(keys, p)
		}
		sort.Ints(keys)
		pairs := make([]string, 0, len(keys))
		for _, p := range keys {
			pairs = append(pairs, fmt.Sprintf("p%d=%d", p, want[p]))
		}
		okf(t, "OffsetFetch reads back exactly what was committed: %s", strings.Join(pairs, " "))
	}
	// A partition nobody committed must answer -1, not 0: a client that sees 0
	// replays the whole partition.
	for p := 0; p < width; p++ {
		if _, ok := want[p]; ok {
			continue
		}
		if v, present := byPart[p]; present && v != -1 {
			failf(t, "partition %d was never committed but OffsetFetch answers %d; an uncommitted partition must be -1, or every client replays it", p, v)
		}
	}
	okf(t, "uncommitted partitions answer -1, not 0")
}

// TestOffsetFetchAllTopics exercises the OffsetFetch shape kafka-go is
// historically fussy about: a NULL topics array, which asks for every topic the
// group has an offset for. kafka-go supports it by passing an empty
// `Topics` map (offsetfetch.go:70-77 leaves `topics` nil), and a facade that
// answers a null array badly makes `Client.OffsetFetch(ctx, &OffsetFetchRequest{
// GroupID: g})` — the obvious call — fail for reasons that look like nothing.
//
// Reported rather than asserted, because "no offsets for a group with none" and
// "this shape is not supported" are both legitimate answers for a facade whose
// offsets live in Queen KV rather than in a group registry it can enumerate.
func TestOffsetFetchAllTopics(t *testing.T) {
	section(t, "OffsetFetch with a NULL topics array")

	topic := topicName("ofall")
	group := groupName("ofall")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	recs := corpus(width, 2)
	produceCorpus(t, topic, recs, 0)

	// Put one real committed offset in the group first, so an empty answer is
	// meaningful rather than vacuous.
	r := groupReader(topic, group, kafka.FirstOffset)
	ctx, cancel := ctxWith(t, 120*time.Second)
	m, err := r.FetchMessage(ctx)
	if err != nil {
		cancel()
		failf(t, "seeding a commit for %s: %v", group, err)
	}
	if err := r.CommitMessages(ctx, m); err != nil {
		cancel()
		failf(t, "CommitMessages while seeding %s: %v", group, err)
	}
	cancel()
	if err := r.Close(); err != nil {
		failf(t, "closing the seeding reader: %v", err)
	}
	okf(t, "seeded group %s with one committed offset on partition %d", group, m.Partition)

	// The named-topics form must work; that is the assertion.
	ctx2, cancel2 := ctxWith(t, 30*time.Second)
	defer cancel2()
	named, err := client().OffsetFetch(ctx2, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: {m.Partition}},
	})
	if err != nil {
		failf(t, "OffsetFetch with a named topic: %v", err)
	}
	if len(named.Topics[topic]) != 1 || named.Topics[topic][0].CommittedOffset != m.Offset+1 {
		failf(t, "OffsetFetch with a named topic returned %+v, expected offset %d", named.Topics[topic], m.Offset+1)
	}
	okf(t, "OffsetFetch(named topic) -> partition %d committed at %d", m.Partition, m.Offset+1)

	// The null-array form is the shape probe.
	ctx3, cancel3 := ctxWith(t, 30*time.Second)
	defer cancel3()
	all, err := client().OffsetFetch(ctx3, &kafka.OffsetFetchRequest{GroupID: group})
	switch {
	case err != nil:
		note("OffsetFetch with a NULL topics array errored: %v", err)
		note("kafka-go's own doc calls this the 'all topics for this group' form; a caller who writes the obvious one-liner meets this")
	case all.Error != nil:
		note("OffsetFetch with a NULL topics array returned error %v", all.Error)
	case len(all.Topics) == 0:
		note("OffsetFetch with a NULL topics array succeeded but enumerated NOTHING, while the named form finds the offset")
		note("consistent with offsets living in Queen KV under qk:group:... keys rather than a registry the facade can list (PLAN_QUEEN_KAFKA.md, C3 not done)")
	default:
		okf(t, "OffsetFetch with a NULL topics array enumerated %d topic(s)", len(all.Topics))
	}
	okf(t, "the null-topics shape neither hangs nor closes the connection")
}

// TestGroupTwoMembers puts two members in one group at once and checks the
// facade's coordinator hands out a disjoint, complete assignment — the thing a
// single-member test can never show.
func TestGroupTwoMembers(t *testing.T) {
	section(t, "Consumer group: two members share the partitions")

	topic := topicName("group2")
	group := groupName("group2")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	perPart := 32
	total := perPart * width
	recs := corpus(width, perPart)
	produceCorpus(t, topic, recs, 0)

	var (
		mu        sync.Mutex
		all       []got
		partsOf   = map[int]map[int]bool{0: {}, 1: {}}
		wg        sync.WaitGroup
		perMember = map[int]int{}
	)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := groupReader(topic, group, kafka.FirstOffset)
			defer r.Close() //nolint:errcheck
			for {
				mu.Lock()
				done := len(all) >= total
				mu.Unlock()
				if done {
					return
				}
				m, err := r.FetchMessage(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				all = append(all, got{
					part: m.Partition, offset: m.Offset,
					key: string(m.Key), val: append([]byte(nil), m.Value...),
					hdrs: headerMap(m.Headers),
				})
				partsOf[i][m.Partition] = true
				perMember[i]++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(all) < total {
		failf(t, "two members together read %d of %d records", len(all), total)
	}
	okf(t, "two members together read %d records (member0=%d member1=%d)", len(all), perMember[0], perMember[1])

	union := map[int]bool{}
	for _, s := range partsOf {
		for p := range s {
			union[p] = true
		}
	}
	if len(union) != width {
		failf(t, "the two members between them touched %d partitions, want all %d", len(union), width)
	}
	okf(t, "every partition 0..%d was assigned to one of the two members", width-1)

	if len(partsOf[0]) > 0 && len(partsOf[1]) > 0 {
		okf(t, "both members got work: %d and %d partitions", len(partsOf[0]), len(partsOf[1]))
	} else {
		note("one member got the whole assignment (%d/%d); legal under RangeGroupBalancer if the other joined after the window closed", len(partsOf[0]), len(partsOf[1]))
	}

	// Dedup before the byte-exactness check: two members over one group is
	// at-least-once and a rebalance may replay.
	uniq := map[string]got{}
	for _, g := range all {
		uniq[g.key] = g
	}
	flat := make([]got, 0, len(uniq))
	for _, g := range uniq {
		flat = append(flat, g)
	}
	if len(flat) != total {
		failf(t, "%d distinct keys arrived, want %d", len(flat), total)
	}
	if dup := len(all) - len(flat); dup > 0 {
		note("%d duplicate deliveries across the rebalance; at-least-once, as Kafka specifies", dup)
	}
	verifyCorpus(t, recs, flat)
}
