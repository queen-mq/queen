package cloud

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ===========================================================================
// 1-4. TWO TENANTS ARE TWO TENANTS, on the Kafka wire
//
// Every one of these uses the SAME topic name and the SAME group id on both
// sides on purpose. Isolation that only holds while the names differ is not
// isolation, it is luck, and a name collision between two Cloud tenants is not
// a hypothetical: `orders` is what everybody calls their first topic.
// ===========================================================================

func TestTwoTenantsDoNotSeeEachOthersTopics(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyB := needEnv(t, "QKC_KEY_B_FULL", "tenant B's credential")

	shared := uniq("shared")
	onlyA := uniq("only-a")

	a := client(t, keyA)
	b := client(t, keyB)
	mustCreateTopic(t, a, shared)
	mustCreateTopic(t, a, onlyA)
	mustCreateTopic(t, b, shared)

	seenByB := topicNames(t, b)
	if seenByB[onlyA] {
		t.Errorf("tenant B's Metadata lists %q, which only tenant A created", onlyA)
	}
	if !seenByB[shared] {
		t.Errorf("tenant B cannot see its OWN %q", shared)
	}
	if !topicNames(t, a)[onlyA] {
		t.Errorf("tenant A cannot see its own %q", onlyA)
	}
	t.Logf("A sees %d topics, B sees %d; the collision name is in both and A's private one is in one",
		len(topicNames(t, a)), len(seenByB))
}

func TestTwoTenantsDoNotReadEachOthersRecords(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyB := needEnv(t, "QKC_KEY_B_FULL", "tenant B's credential")

	topic := uniq("collide")
	a := client(t, keyA)
	b := client(t, keyB)
	mustCreateTopic(t, a, topic)
	mustCreateTopic(t, b, topic)

	produce(t, a, topic, "a-1", "a-2", "a-3")
	produce(t, b, topic, "b-1", "b-2")

	readA := client(t, keyA, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(0)},
	}))
	readB := client(t, keyB, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(0)},
	}))

	gotA := consumeN(ctxFor(t, 45*time.Second), readA, 3)
	gotB := consumeN(ctxFor(t, 45*time.Second), readB, 2)
	t.Logf("A read %v; B read %v", gotA, gotB)

	for _, v := range gotA {
		if strings.HasPrefix(v, "b-") {
			t.Fatalf("tenant A read %q, which tenant B wrote to the same topic name", v)
		}
	}
	for _, v := range gotB {
		if strings.HasPrefix(v, "a-") {
			t.Fatalf("tenant B read %q, which tenant A wrote to the same topic name", v)
		}
	}
	if len(gotA) != 3 || len(gotB) != 2 {
		t.Fatalf("each tenant must read its own whole log: A got %d/3, B got %d/2", len(gotA), len(gotB))
	}
}

func TestTwoTenantsDoNotShareAConsumerGroup(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyB := needEnv(t, "QKC_KEY_B_FULL", "tenant B's credential")

	topic := uniq("grp")
	group := uniq("g")

	setup := client(t, keyA)
	mustCreateTopic(t, setup, topic)
	produce(t, setup, topic, "a-1", "a-2")
	setupB := client(t, keyB)
	mustCreateTopic(t, setupB, topic)
	produce(t, setupB, topic, "b-1", "b-2", "b-3")

	// The SAME group id on both sides. Each must consume and commit its own
	// log, and neither may be moved along by the other's commits.
	readOne := func(key string, want int) []string {
		cl := client(t, key,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
		)
		got := consumeN(ctxFor(t, 60*time.Second), cl, want)
		if err := cl.CommitUncommittedOffsets(ctxFor(t, 30*time.Second)); err != nil {
			t.Fatalf("commit for %s: %v", group, err)
		}
		return got
	}

	gotA := readOne(keyA, 2)
	gotB := readOne(keyB, 3)
	t.Logf("group %q: A read %v, B read %v", group, gotA, gotB)
	if len(gotA) != 2 {
		t.Errorf("tenant A's side of group %q read %d records, want 2", group, len(gotA))
	}
	if len(gotB) != 3 {
		t.Errorf("tenant B's side of group %q read %d records, want 3 — a shared group would have "+
			"been carried past its own log by A's commits", group, len(gotB))
	}
}

func TestTwoTenantsDoNotShareCommittedOffsets(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyB := needEnv(t, "QKC_KEY_B_FULL", "tenant B's credential")

	topic := uniq("off")
	group := uniq("og")

	a := client(t, keyA)
	b := client(t, keyB)
	mustCreateTopic(t, a, topic)
	mustCreateTopic(t, b, topic)
	produce(t, a, topic, "a-1", "a-2", "a-3", "a-4")
	produce(t, b, topic, "b-1")

	drain := func(key string, want int) {
		cl := client(t, key,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
		)
		consumeN(ctxFor(t, 60*time.Second), cl, want)
		if err := cl.CommitUncommittedOffsets(ctxFor(t, 30*time.Second)); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	drain(keyA, 4)
	drain(keyB, 1)

	offA := committedOffsets(t, a, group)
	offB := committedOffsets(t, b, group)
	t.Logf("group %q offsets: A=%v B=%v", group, offA, offB)

	key := fmt.Sprintf("%s/0", topic)
	if offA[key] != 4 {
		t.Errorf("tenant A's committed offset for %s is %d, want 4 (a Kafka offset is the NEXT "+
			"record to read)", key, offA[key])
	}
	if offB[key] != 1 {
		t.Errorf("tenant B's committed offset for %s is %d, want 1 — B is reading A's cursor", key, offB[key])
	}
}

// ===========================================================================
// 5-7, 15. SCOPES: narrow credentials, and refusals that name themselves
// ===========================================================================

// The design's §0.4 headline, from the other end: a consume-scoped key does
// DATA and is refused ADMIN, and the refusal carries the proxy's own sentence
// rather than a bare code. `kafka-topics --create` printing
// TOPIC_AUTHORIZATION_FAILED and nothing else is an operator with no next step.
func TestAConsumeScopedKeyCanReadButNotCreateTopics(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyConsume := needEnv(t, "QKC_KEY_A_CONSUME", "a {consume, read} credential")

	topic := uniq("scoped")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)
	produce(t, admin, topic, "s-1", "s-2")

	// It reads. A consume key is a working consumer.
	reader := client(t, keyConsume, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(0)},
	}))
	got := consumeN(ctxFor(t, 45*time.Second), reader, 2)
	if len(got) != 2 {
		t.Fatalf("a {consume, read} key read %d records, want 2: %v", len(got), got)
	}

	// ...and it cannot create.
	consumer := client(t, keyConsume)
	res := createTopic(t, consumer, uniq("forbidden"))
	if res.ErrorCode == 0 {
		t.Fatal("a {consume, read} key created a topic")
	}
	if want := kerr.TopicAuthorizationFailed.Code; res.ErrorCode != want {
		t.Errorf("CreateTopics refused with code %d, want %d (TOPIC_AUTHORIZATION_FAILED)",
			res.ErrorCode, want)
	}
	why := msg(res.ErrorMessage)
	t.Logf("CreateTopics error_message: %q", why)
	if why == "" {
		t.Fatal("the refusal carried no error_message: the operator is left with a code and no fix")
	}
	if !strings.Contains(why, "403") {
		t.Errorf("the error_message does not name the status it came from: %q", why)
	}
	if !strings.Contains(strings.ToLower(why), "permit") && !strings.Contains(strings.ToLower(why), "forbidden") {
		t.Errorf("the error_message does not carry the proxy's own reason: %q", why)
	}
}

func TestAProduceScopedKeyCannotConsume(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyProduce := needEnv(t, "QKC_KEY_A_PRODUCE", "a {produce, read} credential")

	topic := uniq("prodonly")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)

	// It produces.
	writer := client(t, keyProduce)
	produce(t, writer, topic, "p-1")

	// ...and its fetch is refused by name.
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = 2000
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = 0
	fp.FetchOffset = 0
	fp.PartitionMaxBytes = 1 << 20
	ft.Partitions = append(ft.Partitions, fp)
	req.Topics = append(req.Topics, ft)
	req.SessionEpoch = -1

	raw, err := writer.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("Fetch never got an answer: %v", err)
	}
	resp := raw.(*kmsg.FetchResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("Fetch answered %d topics", len(resp.Topics))
	}
	code := resp.Topics[0].Partitions[0].ErrorCode
	if code != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("a {produce, read} key's Fetch answered %d, want %d (TOPIC_AUTHORIZATION_FAILED)",
			code, kerr.TopicAuthorizationFailed.Code)
	}
}

// THE sentence this whole track exists to produce. A key with the verb and
// WITHOUT `read` is refused at SASL, because the facade's credential check is
// the queue listing and every Kafka client's first request is Metadata, which
// IS that listing. Before the split, the message said the password was wrong.
func TestAKeyWithoutReadCannotEvenAuthenticate(t *testing.T) {
	keyNoRead := needEnv(t, "QKC_KEY_A_NOREAD", "a credential scoped {consume} and nothing else")

	cl := client(t, keyNoRead, kgo.RequestRetries(0), kgo.RetryTimeout(5*time.Second))
	_, err := cl.Request(ctxFor(t, 30*time.Second), kmsg.NewPtrMetadataRequest())
	if err == nil {
		t.Fatal("a credential with no `read` scope authenticated and read metadata")
	}
	t.Logf("SASL refusal: %v", err)
	if !errors.Is(err, kerr.SaslAuthenticationFailed) {
		t.Fatalf("the client was given %v, not SASL_AUTHENTICATION_FAILED", err)
	}
	text := err.Error()
	for _, want := range []string{"403", "SCOPE", "read", "Metadata"} {
		if !strings.Contains(text, want) {
			t.Errorf("the SASL refusal does not contain %q — an operator reading it would go and "+
				"check a password that was never wrong: %s", want, text)
		}
	}
	if !strings.Contains(text, "not a bad password") {
		t.Errorf("the SASL refusal does not say it is not a bad password: %s", text)
	}
}

// The design's §2.2 item 3, proven rather than assumed: after the qk-prefix
// rule a `POST /api/v1/kv` carrying only `qk:` keys is CONSUME-classified, so
// the transactional producer — which writes a `qk:txn:` marker — needs
// `consume` beside `produce`. Its credential is {produce, consume, read}.
func TestTheTransactionRouteIsStillProduceClassified(t *testing.T) {
	keyTxn := needEnv(t, "QKC_KEY_A_TXN", "a {produce, consume, read} credential")
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")

	topic := uniq("txn")
	mustCreateTopic(t, client(t, keyFull), topic)

	producer := txnClient(t, keyTxn, uniq("txnid"), kgo.DefaultProduceTopic(topic))
	ctx := ctxFor(t, 90*time.Second)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Partition: 0, Value: []byte("t-1")},
		&kgo.Record{Topic: topic, Partition: 0, Value: []byte("t-2")},
	).FirstErr(); err != nil {
		t.Fatalf("transactional produce: %v", err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction(commit): %v", err)
	}

	reader := client(t, keyFull, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(0)},
	}))
	got := consumeN(ctxFor(t, 45*time.Second), reader, 2)
	if len(got) != 2 {
		t.Fatalf("a committed transaction produced %d readable records, want 2: %v", len(got), got)
	}

	record(t, "txn classification asymmetry",
		"POST /api/v1/transaction stays Produce-classified even though its top-level `kv` rider "+
			"carries the same qk: marker keys that, sent to /api/v1/kv, are now Consume-classified. "+
			"Harmless (a qk:txn: marker grants no data path) and asymmetric. The credential that "+
			"works for both is {produce, consume, read}.")
}

// ===========================================================================
// 8, 11-14. THE GATES a Cloud tenant meets
// ===========================================================================

// The two gates that used to stand between a Kafka consumer and its own
// offsets, in one scenario, because they are one situation: both clusters here
// are on the `free` plan, whose `features` is `{}` — so NEITHER has the `kv`
// feature that `Gated(Kv, Mixed)` demands. Every commit below would have been
// 403 `not_in_your_plan` before the qk-prefix rule.
func TestOffsetsCommitForATenantWhosePlanHasNoKv(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")

	if shim := env("QKC_PSQL"); shim != "" && env("QKC_CLUSTER_A") != "" {
		plan := psql(t, "QKC_PSQL", fmt.Sprintf(
			"SELECT p.code || ' features=' || p.features::text FROM queen_proxy.clusters c "+
				"JOIN queen_proxy.plans p ON p.id = c.plan_id WHERE c.id = '%s'", env("QKC_CLUSTER_A")))
		t.Logf("tenant A's plan: %s", plan)
		if strings.Contains(plan, `"kv"`) {
			t.Skipf("this cluster's plan DOES carry the kv feature, so the scenario proves nothing: %s", plan)
		}
	}

	topic := uniq("nokv")
	group := uniq("nokvg")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)
	produce(t, admin, topic, "k-1", "k-2", "k-3")

	cl := client(t, keyFull,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	got := consumeN(ctxFor(t, 60*time.Second), cl, 3)
	if len(got) != 3 {
		t.Fatalf("read %d records, want 3: %v", len(got), got)
	}
	if err := cl.CommitUncommittedOffsets(ctxFor(t, 30*time.Second)); err != nil {
		t.Fatalf("OffsetCommit on a plan with no `kv` feature: %v", err)
	}
	off := committedOffsets(t, admin, group)
	if off[fmt.Sprintf("%s/0", topic)] != 3 {
		t.Fatalf("OffsetFetch on a plan with no `kv` feature read %v, want offset 3", off)
	}
	t.Logf("committed and fetched %v with the `kv` plan feature absent", off)
}

// The regression guard on the qk rule: it is a rule about ONE key prefix in ONE
// namespace, and every other KV batch is gated exactly as it was.
func TestAPlainKvBatchIsStillGated(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")

	body := `{"operations":[{"op":"put","ns":"app","key":"cache:x","value":{"v":1}}]}`
	code, resp := httpThroughProxy(t, "POST", "/api/v1/kv", keyFull, body)
	t.Logf("plain KV batch: HTTP %d %s", code, strings.TrimSpace(resp))
	if code == 200 {
		t.Fatalf("a non-qk KV batch went through on a plan with no `kv` feature: the prefix rule "+
			"is reclassifying more than the facade's own key space (HTTP %d)", code)
	}
	if code != 403 {
		t.Errorf("a non-qk KV batch answered HTTP %d, want 403 feature_gated", code)
	}
	// ...and the same call with a qk: key is the one that goes through.
	qk := `{"operations":[{"op":"getPrefix","ns":"queen-kafka","prefix":"qk:group:"}]}`
	code2, resp2 := httpThroughProxy(t, "POST", "/api/v1/kv", keyFull, qk)
	t.Logf("qk: KV batch:    HTTP %d %s", code2, strings.TrimSpace(resp2)[:min(160, len(strings.TrimSpace(resp2)))])
	if code2 != 200 {
		t.Errorf("a qk:-only KV batch answered HTTP %d, want 200 — the facade's own bookkeeping "+
			"is being gated by the kv plan feature again", code2)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// A long-poll Fetch takes NO parked slot at the proxy (`is_wait_pop` matches
// only the pop routes), so it is bounded by the upstream request timeout rather
// than by the long-poll budget. The facade clamps its own wait to 30s. The
// margin between the two is the number an operator must not shrink, and this
// scenario measures it rather than asserting a target.
func TestALongPollFetchIsNotCutByTheProxyTimeout(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")

	topic := uniq("longpoll")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)
	// One record, so the partition exists and the fetch is a genuine wait at
	// the head rather than a refusal for an unknown partition.
	produce(t, admin, topic, "seed")

	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = 30000
	req.MinBytes = 1 << 20 // more than will ever arrive: the wait runs its course
	req.SessionEpoch = -1
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = 0
	fp.FetchOffset = 1 // past the seed: nothing to read
	fp.PartitionMaxBytes = 1 << 20
	ft.Partitions = append(ft.Partitions, fp)
	req.Topics = append(req.Topics, ft)

	start := time.Now()
	raw, err := admin.Request(ctxFor(t, 120*time.Second), req)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("a 30s long-poll Fetch failed after %s: %v", elapsed, err)
	}
	resp := raw.(*kmsg.FetchResponse)
	if resp.ErrorCode != 0 {
		t.Errorf("a 30s long-poll Fetch answered top-level error %d after %s", resp.ErrorCode, elapsed)
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				t.Errorf("partition %d answered error %d after %s", p.Partition, p.ErrorCode, elapsed)
			}
		}
	}
	t.Logf("the 30s long poll returned cleanly after %s", elapsed.Round(time.Millisecond))

	upstream := env("QKC_UPSTREAM_TIMEOUT_MS")
	record(t, "long-poll margin", fmt.Sprintf(
		"facade MAX_FETCH_WAIT_MS=30000 against QUEEN_PROXY_UPSTREAM_TIMEOUT_MS=%s: %s of headroom. "+
			"A Fetch takes NO parked slot (routes.rs is_wait_pop matches only /api/v1/pop/queue/ and "+
			"/api/v1/ephemeral/pop), so parked Kafka consumers are uncapped and ungauged, and this "+
			"margin is what an operator must not shrink.", upstream, marginText(upstream)))
}

func marginText(upstreamMs string) string {
	var ms int
	if _, err := fmt.Sscanf(upstreamMs, "%d", &ms); err != nil || ms == 0 {
		return "unknown (QKC_UPSTREAM_TIMEOUT_MS unset)"
	}
	return fmt.Sprintf("%dms", ms-30000)
}

// A rate cap must SLOW a Kafka client, not fail it. The proxy answers 429 with
// a Retry-After; the facade turns that into `throttle_time_ms`, which every
// Kafka client obeys natively and reports to nobody.
func TestARateCappedTenantIsThrottledAndNotFailed(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	cluster := needEnv(t, "QKC_CLUSTER_A", "the cluster whose rate cap this scenario parks")

	topic := uniq("capped")
	mustCreateTopic(t, client(t, keyFull), topic)

	wide := `{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,"max_queues":500}`
	t.Cleanup(func() { setLimitOverride(t, cluster, wide) })
	setLimitOverride(t, cluster, `{"max_req_per_sec":1,"req_burst":2,"max_queues":500}`)

	// CreateTopics is the cheapest admin call that carries `throttle_time_ms`
	// AND a version at which a client is required to understand a throttle
	// code (KIP-599). Fired faster than one per second, it must meet the cap.
	admin := client(t, keyFull)
	maxThrottle := int32(0)
	fatal := 0
	for i := 0; i < 12; i++ {
		req := kmsg.NewPtrCreateTopicsRequest()
		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic = topic
		ct.NumPartitions = -1
		ct.ReplicationFactor = -1
		req.Topics = append(req.Topics, ct)
		req.TimeoutMillis = 5000
		raw, err := admin.Request(ctxFor(t, 20*time.Second), req)
		if err != nil {
			fatal++
			t.Logf("call %d: transport-level failure %v", i, err)
			continue
		}
		resp := raw.(*kmsg.CreateTopicsResponse)
		if resp.ThrottleMillis > maxThrottle {
			maxThrottle = resp.ThrottleMillis
		}
	}
	t.Logf("under max_req_per_sec=1: highest throttle_time_ms seen = %d, transport failures = %d",
		maxThrottle, fatal)
	if fatal > 0 {
		t.Errorf("a rate cap produced %d transport-level failures; a cap must slow a client, not "+
			"break its connection", fatal)
	}
	if maxThrottle <= 0 {
		t.Errorf("no throttle_time_ms was ever reported under a 1 req/s cap: a Kafka client has no " +
			"way to learn it is being asked to wait")
	}

	// ...and the work still COMPLETES. That is the whole claim.
	setLimitOverride(t, cluster, wide)
	writer := client(t, keyFull)
	produce(t, writer, topic, "after-the-cap")
	reader := client(t, keyFull, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(0)},
	}))
	if got := consumeN(ctxFor(t, 45*time.Second), reader, 1); len(got) != 1 {
		t.Fatalf("nothing readable after the cap was lifted: %v", got)
	}
}

// ===========================================================================
// 9, 10 + the smart mirror. WHAT THE CELL SEES of a Kafka client
// ===========================================================================

func TestMeteringRowsAppearInPxdbForKafkaTraffic(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyB := needEnv(t, "QKC_KEY_B_FULL", "tenant B's credential")
	clusterA := needEnv(t, "QKC_CLUSTER_A", "tenant A's cluster id")
	clusterB := needEnv(t, "QKC_CLUSTER_B", "tenant B's cluster id")

	for _, tc := range []struct{ key, tag string }{{keyA, "a"}, {keyB, "b"}} {
		topic := uniq("meter-" + tc.tag)
		cl := client(t, tc.key)
		mustCreateTopic(t, cl, topic)
		produce(t, cl, topic, "m-1", "m-2", "m-3")
		reader := client(t, tc.key, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(0)},
		}))
		consumeN(ctxFor(t, 45*time.Second), reader, 3)
	}

	// The meter flushes on its own cadence; give it one.
	time.Sleep(12 * time.Second)

	rows := psql(t, "QKC_PSQL",
		"SELECT cluster_id || ' ' || op_class || ' reqs=' || SUM(reqs) || ' msgs=' || SUM(msgs) || "+
			"' in=' || SUM(bytes_in) || ' out=' || SUM(bytes_out) "+
			"FROM queen_proxy.usage_minutes GROUP BY cluster_id, op_class ORDER BY cluster_id, op_class")
	t.Logf("queen_proxy.usage_minutes:\n%s", rows)

	for name, id := range map[string]string{"A": clusterA, "B": clusterB} {
		if !strings.Contains(rows, id) {
			t.Errorf("no metering rows for tenant %s (cluster %s): Kafka traffic crossed the proxy "+
				"and was not booked", name, id)
		}
	}
	if !strings.Contains(rows, "push") {
		t.Errorf("no `push` op_class rows: a Kafka Produce is not being message-metered")
	}

	// The design's OUT item, asserted as CURRENT BEHAVIOUR rather than fixed:
	// a Kafka Fetch books reqs on `read` and never a message.
	readMsgs := psql(t, "QKC_PSQL",
		"SELECT COALESCE(SUM(msgs),0) || '/' || COALESCE(SUM(reqs),0) "+
			"FROM queen_proxy.usage_minutes WHERE op_class='read'")
	record(t, "fetch billed as requests", fmt.Sprintf(
		"op_class=read is msgs/reqs = %s. A Kafka Fetch books a REQUEST and zero MESSAGES "+
			"(routes.rs: `Billing Kafka deliveries is a decision, not a default`; "+
			"gateway.rs asserts !is_pop_path(\"/api/v1/fetch\")). Produce IS message-metered. "+
			"A tenant consuming a million records through Kafka is billed for the requests that "+
			"carried them and not for the records. Revenue decision, not an engineering gap.",
		readMsgs))
	if strings.HasPrefix(readMsgs, "0/") {
		t.Logf("confirmed: op_class=read carries reqs and msgs=0")
	} else {
		t.Logf("NOTE: op_class=read reported %s — a non-zero msgs count means something other than "+
			"Fetch is booking messages on `read`", readMsgs)
	}
}

// T3(a) live: `/auth/me` answers a BEARER, so the facade keys a group by the
// TENANT and not by the credential. Two keys of one cluster in one group id
// must be one group, not two.
func TestTheFacadeResolvesItsTenantFromAuthMe(t *testing.T) {
	keyA := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	keyA2 := needEnv(t, "QKC_KEY_A_FULL2", "a SECOND credential of the same cluster")
	partitions := 4
	if v := env("QKC_PARTITIONS"); v != "" {
		fmt.Sscanf(v, "%d", &partitions)
	}
	if partitions < 2 {
		t.Skip("the split needs at least two partitions")
	}

	// The identity call itself, from the outside: this is what the facade sees.
	code, body := httpThroughProxy(t, "GET", "/auth/me", keyA, "")
	t.Logf("GET /auth/me with a bearer: HTTP %d %s", code, strings.TrimSpace(body))
	if code != 200 {
		t.Fatalf("/auth/me answered HTTP %d for a bearer; the facade degrades to keying groups by "+
			"credential, which is the duplicate-consumption bug", code)
	}
	idA := clusterIDOf(body)
	if idA == "" {
		t.Fatalf("/auth/me carries no acting_cluster.id, which is the ONE field the facade reads: %s", body)
	}
	_, body2 := httpThroughProxy(t, "GET", "/auth/me", keyA2, "")
	if id2 := clusterIDOf(body2); id2 != idA {
		t.Fatalf("two keys of one cluster resolved to %q and %q; the group scope would be two scopes",
			idA, id2)
	}
	t.Logf("both of tenant A's keys resolve to acting_cluster.id = %s", idA)

	topic := uniq("identity")
	group := uniq("ig")
	admin := client(t, keyA)
	mustCreateTopic(t, admin, topic)

	// Two members, two DIFFERENT credentials, one group id.
	mk := func(key string) *kgo.Client {
		return client(t, key,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
		)
	}
	c1 := mk(keyA)
	c2 := mk(keyA2)
	// Let both finish joining before there is anything to read, so the split is
	// a property of the group and not of who got there first.
	warm := ctxFor(t, 25*time.Second)
	c1.PollFetches(warm)
	c2.PollFetches(warm)
	time.Sleep(3 * time.Second)

	recs := make([]*kgo.Record, 0, partitions)
	for p := 0; p < partitions; p++ {
		recs = append(recs, &kgo.Record{
			Topic: topic, Partition: int32(p), Value: []byte(fmt.Sprintf("p%d", p)),
		})
	}
	if err := admin.ProduceSync(ctxFor(t, 60*time.Second), recs...).FirstErr(); err != nil {
		t.Fatalf("produce one record per partition: %v", err)
	}

	deadline := time.Now().Add(45 * time.Second)
	seen := map[string]int{}
	total := 0
	for time.Now().Before(deadline) && total < partitions {
		for _, c := range []*kgo.Client{c1, c2} {
			ctx, cancel := ctxWithDeadline(2 * time.Second)
			c.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
				seen[string(r.Value)]++
				total++
			})
			cancel()
		}
	}
	t.Logf("one group, two credentials, %d partitions: %d records read, distinct %d, counts %v",
		partitions, total, len(seen), seen)
	if len(seen) != partitions {
		t.Errorf("the group read %d distinct records of %d", len(seen), partitions)
	}
	if total >= 2*partitions {
		t.Errorf("every record was read %dx: the two credentials are filing as TWO groups, which is "+
			"the duplicate-consumption bug /auth/me exists to close", total/max(1, len(seen)))
	}

	log := facadeLog(t)
	if !strings.Contains(log, "this credential's tenant was resolved from /auth/me") {
		t.Errorf("the facade never logged a tenant resolved from /auth/me; it is keying groups by "+
			"credential. Facade log tail:\n%s", tail(log, 20))
	} else {
		t.Log("facade log: `this credential's tenant was resolved from /auth/me`")
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// The smart mirror (T1), read THROUGH the proxy: a Kafka consumer group is a
// row in the same console listing as a native one, carrying `kind: "kafka"`.
func TestTheSmartMirrorShowsAKafkaGroupThroughTheProxy(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")

	topic := uniq("mirror")
	group := uniq("mg")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)
	produce(t, admin, topic, "m-1", "m-2", "m-3", "m-4")

	cl := client(t, keyFull,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if got := consumeN(ctxFor(t, 60*time.Second), cl, 4); len(got) != 4 {
		t.Fatalf("read %d of 4 before committing", len(got))
	}
	if err := cl.CommitUncommittedOffsets(ctxFor(t, 30*time.Second)); err != nil {
		t.Fatalf("commit: %v", err)
	}

	code, body := httpThroughProxy(t, "GET", "/api/v1/consumer-groups", keyFull, "")
	if code != 200 {
		t.Fatalf("GET /api/v1/consumer-groups through the proxy: HTTP %d %s", code, tail(body, 5))
	}
	if !strings.Contains(body, group) {
		t.Fatalf("the console listing has no row for Kafka group %q. Body: %s", group, tail(body, 5))
	}
	if !strings.Contains(body, `"kind":"kafka"`) && !strings.Contains(body, `"kind": "kafka"`) {
		t.Errorf("the listing carries no kind=\"kafka\" row; a reader cannot tell a Kafka cursor "+
			"from a native one. Body: %s", tail(body, 5))
	}
	// The lag arithmetic, against the number the client's own OffsetFetch says.
	off := committedOffsets(t, admin, group)
	t.Logf("Kafka group %q: committed %v; console row present with kind=kafka", group, off)
	if off[fmt.Sprintf("%s/0", topic)] != 4 {
		t.Errorf("OffsetFetch says %v, want offset 4", off)
	}

	record(t, "parked gauge invisibility", "a Kafka Fetch takes no parked slot, so the proxy's "+
		"parked-pop gauge shows ZERO however many Kafka consumers are long-polling. The gauge an "+
		"operator watches for consumer pressure cannot see them at all.")
}

// --------------------------------------------------------------------- small

func ctxWithDeadline(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func tail(s string, lines int) string {
	parts := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

// clusterIDOf pulls `acting_cluster.id` out of an /auth/me body — the ONE field
// `queen-kafka/src/identity.rs::tenant_of` reads. Done by hand rather than with
// a JSON decode so the assertion is about the exact bytes on the wire.
func clusterIDOf(body string) string {
	i := strings.Index(body, `"acting_cluster"`)
	if i < 0 {
		return ""
	}
	rest := body[i:]
	j := strings.Index(rest, `"id"`)
	if j < 0 {
		return ""
	}
	rest = rest[j+4:]
	k := strings.Index(rest, `"`)
	if k < 0 {
		return ""
	}
	rest = rest[k+1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// The read-strands-consumers trap, which is the reason the qk rule reclassifies
// to Consume rather than merely widening a scope. A tenant over its storage cap
// must still be able to move its cursor: refusing the read leaves a consumer
// parked at an offset it can never move past while the backlog it would drain
// keeps growing.
func TestABlockedTenantCanStillCommitAndReadOffsets(t *testing.T) {
	keyFull := needEnv(t, "QKC_KEY_A_FULL", "tenant A's credential")
	cluster := needEnv(t, "QKC_CLUSTER_A", "the cluster whose storage cap this scenario parks")

	topic := uniq("blocked")
	group := uniq("blockedg")
	admin := client(t, keyFull)
	mustCreateTopic(t, admin, topic)
	produce(t, admin, topic, "b-1", "b-2")

	cl := client(t, keyFull,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if got := consumeN(ctxFor(t, 60*time.Second), cl, 2); len(got) != 2 {
		t.Fatalf("read %d records before the block, want 2", len(got))
	}

	// A 64-byte storage cap. The retained-bytes lane runs every 5s in the rig,
	// so a moment is needed before the proxy has a measurement to refuse on.
	wide := `{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,"max_queues":500}`
	// LAST in the file on purpose, and the cleanup WAITS: lifting a storage
	// block is not the write to pxdb, it is the registry reconciler noticing
	// (QUEEN_PROXY_RECONCILE_MS) on top of the broker's retained-bytes lane
	// (RETAINED_BYTES_INTERVAL_MS). Returning from this test with the block
	// still in force would fail the NEXT scenario for this one's reason.
	t.Cleanup(func() {
		setLimitOverride(t, cluster, wide)
		for i := 0; i < 20; i++ {
			w := client(t, keyFull)
			if err := w.ProduceSync(ctxFor(t, 15*time.Second),
				&kgo.Record{Topic: topic, Partition: 0, Value: []byte("unblocked")}).FirstErr(); err == nil {
				t.Logf("the block lifted %ds after the cap was removed", i)
				return
			}
			time.Sleep(time.Second)
		}
		t.Errorf("the storage block was still in force 20s after the cap was removed")
	})
	setLimitOverride(t, cluster,
		`{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,`+
			`"max_queues":500,"max_retained_bytes":64}`)

	blocked := false
	for i := 0; i < 12; i++ {
		writer := client(t, keyFull)
		err := writer.ProduceSync(ctxFor(t, 20*time.Second),
			&kgo.Record{Topic: topic, Partition: 0, Value: []byte("after-the-block")}).FirstErr()
		if err != nil {
			t.Logf("produce refused after the block: %v", err)
			blocked = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !blocked {
		t.Errorf("produce was never refused under a 64-byte max_retained_bytes; the rest of this " +
			"scenario cannot prove that offsets survive a block (retained-bytes lane cadence is " +
			"RETAINED_BYTES_INTERVAL_MS on the broker)")
	}

	// THE assertion: the cursor still moves, and can still be read.
	if err := cl.CommitUncommittedOffsets(ctxFor(t, 30*time.Second)); err != nil {
		t.Fatalf("OffsetCommit for a storage-blocked tenant: %v", err)
	}
	off := committedOffsets(t, admin, group)
	if off[fmt.Sprintf("%s/0", topic)] != 2 {
		t.Fatalf("OffsetFetch for a storage-blocked tenant read %v, want offset 2", off)
	}
	t.Logf("blocked tenant committed and fetched %v", off)
}
