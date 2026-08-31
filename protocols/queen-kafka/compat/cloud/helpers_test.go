// Package cloud is the queen-kafka CLOUD acceptance suite: real Kafka clients
// against a real cell, where every request crosses the proxy.
//
// The suite asserts nothing about Kafka semantics — compat/go does that, over
// 200 cases. What it asserts is the things that only become true when a facade
// is put behind a control plane: that two tenants are two tenants on the Kafka
// wire, that a narrow credential is narrow and SAYS SO, that offsets survive
// the gates a Cloud tenant meets, and that the traffic is billed.
//
// Every credential here is a Queen api key presented as the SASL/PLAIN
// password. That is the whole of the client-side change: the tenant of a Kafka
// connection is the tenant of its password, and nothing else.
package cloud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// ---------------------------------------------------------------- environment

func env(k string) string { return strings.TrimSpace(os.Getenv(k)) }

// needEnv skips LOUDLY. A scenario that cannot run must say which variable
// would have let it, or a rig that silently proves less than it claims looks
// exactly like one that proves everything.
func needEnv(t *testing.T, k, why string) string {
	t.Helper()
	v := env(k)
	if v == "" {
		t.Skipf("%s is unset: %s", k, why)
	}
	return v
}

func bootstrap(t *testing.T) string {
	t.Helper()
	b := env("QKC_BOOTSTRAP")
	if b == "" {
		t.Fatal("QKC_BOOTSTRAP is unset; run rig-cloud.sh or source its rig.env")
	}
	return b
}

// runID makes every topic and group of one run distinct, so a rig kept up
// across runs never has a previous run's committed offsets answering for this
// one's.
func runID() string {
	if v := env("RUN_ID"); v != "" {
		return v
	}
	return fmt.Sprintf("%d", time.Now().Unix())
}

func uniq(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%s", prefix, runID(), hex.EncodeToString(b[:]))
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// ------------------------------------------------------------------- clients

// client dials the facade as one tenant. `opts` are appended, so a caller can
// add a consumer group, a transactional id or a fetch budget.
//
// RequestRetries(2) rather than the default 20: several scenarios here are
// meant to FAIL, and a client that retries a refusal twenty times turns a
// two-second assertion into a two-minute one.
func client(t *testing.T, key string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(bootstrap(t)),
		// The user half of PLAIN is a label and nothing else — the facade reads
		// the password. A recognisable one makes a facade log line readable.
		kgo.SASL(plain.Auth{User: "qkc", Pass: key}.AsMechanism()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(2),
		kgo.RetryTimeout(20 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// txnClient is `client` without `DisableIdempotentWrite`, which franz-go
// refuses to combine with a transactional id — a transaction IS an idempotent
// session, so the two options contradict each other.
func txnClient(t *testing.T, key, txnID string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(bootstrap(t)),
		kgo.SASL(plain.Auth{User: "qkc", Pass: key}.AsMechanism()),
		kgo.TransactionalID(txnID),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(2),
		kgo.RetryTimeout(20 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient (transactional): %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// createTopic asks for a topic through the Kafka wire and returns the raw
// per-topic result, so a caller can read the error MESSAGE and not only the
// code. `-1` partitions is KIP-464's "I do not care", which is what a modern
// AdminClient sends and what the facade answers with its real width.
func createTopic(t *testing.T, cl *kgo.Client, name string) kmsg.CreateTopicsResponseTopic {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = name
	topic.NumPartitions = -1
	topic.ReplicationFactor = -1
	req.Topics = append(req.Topics, topic)
	req.TimeoutMillis = 20000

	raw, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("CreateTopics(%s) never got an answer: %v", name, err)
	}
	resp := raw.(*kmsg.CreateTopicsResponse)
	if len(resp.Topics) != 1 {
		t.Fatalf("CreateTopics(%s) answered %d topics, not 1", name, len(resp.Topics))
	}
	return resp.Topics[0]
}

// mustCreateTopic is createTopic where the creation is setup and not the
// assertion. TOPIC_ALREADY_EXISTS (36) is success: the topic is there, which is
// all a caller of this wanted.
func mustCreateTopic(t *testing.T, cl *kgo.Client, name string) {
	t.Helper()
	got := createTopic(t, cl, name)
	if got.ErrorCode != 0 && got.ErrorCode != 36 {
		t.Fatalf("CreateTopics(%s) = %d %q", name, got.ErrorCode, msg(got.ErrorMessage))
	}
}

func msg(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// produce writes one record per value, all to partition 0 so that a consumer
// reading one partition reads all of them.
func produce(t *testing.T, cl *kgo.Client, topic string, values ...string) {
	t.Helper()
	recs := make([]*kgo.Record, 0, len(values))
	for _, v := range values {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(v)})
	}
	if err := cl.ProduceSync(ctxFor(t, 60*time.Second), recs...).FirstErr(); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

// consumeN reads until it has n records or the deadline passes. It returns what
// it got, so a caller can assert on a SHORT read as easily as on a full one.
func consumeN(ctx context.Context, cl *kgo.Client, n int) []string {
	var got []string
	for len(got) < n && ctx.Err() == nil {
		f := cl.PollFetches(ctx)
		f.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
}

// topicNames is what Metadata reports for a client that asked for ALL topics —
// which for a Kafka client is the whole visible universe, and is therefore the
// isolation assertion in its most direct form.
func topicNames(t *testing.T, cl *kgo.Client) map[string]bool {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = nil // nil, not empty: "every topic you have"
	raw, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	out := map[string]bool{}
	for _, tp := range raw.(*kmsg.MetadataResponse).Topics {
		if tp.Topic != nil {
			out[*tp.Topic] = true
		}
	}
	return out
}

// committedOffsets is OffsetFetch for one group, flattened to
// "topic/partition" -> offset. It is what `kafka-consumer-groups --describe`
// reads, and it is served out of the `qk:group:` KV space through the proxy.
func committedOffsets(t *testing.T, cl *kgo.Client, group string) map[string]int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	req.Topics = nil // every topic this group has committed for
	raw, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("OffsetFetch(%s): %v", group, err)
	}
	resp := raw.(*kmsg.OffsetFetchResponse)
	if resp.ErrorCode != 0 {
		t.Fatalf("OffsetFetch(%s) top-level error %d", group, resp.ErrorCode)
	}
	out := map[string]int64{}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode == 0 && p.Offset >= 0 {
				out[fmt.Sprintf("%s/%d", tp.Topic, p.Partition)] = p.Offset
			}
		}
	}
	return out
}

// ------------------------------------------------------------ the cell around

// psql runs one statement through the shim rig-cloud.sh wrote. A shim and not a
// Postgres driver on purpose: this suite's whole dependency set is franz-go,
// and a rig that needs a database driver to check a metering row is a rig
// somebody will not run.
func psql(t *testing.T, shimVar, sql string) string {
	t.Helper()
	shim := needEnv(t, shimVar, "the rig writes it; without it this scenario cannot read the cell")
	out, err := exec.Command(shim, sql).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", shimVar, err, out)
	}
	return strings.TrimSpace(string(out))
}

// setLimitOverride parks a per-cluster limit delta and takes it off again at the
// end of the test, whatever the test did. Every knob a scenario here bends is
// bent through this, so a failure cannot leave the next scenario running under
// somebody else's cap.
func setLimitOverride(t *testing.T, cluster, jsonOrNULL string) {
	t.Helper()
	arg := "NULL"
	if jsonOrNULL != "NULL" {
		arg = "'" + jsonOrNULL + "'::jsonb"
	}
	psql(t, "QKC_PSQL", fmt.Sprintf(
		"SELECT queen_proxy.set_limit_override('%s'::uuid, %s)", cluster, arg))
	// The proxy caches cluster rows; QUEEN_PROXY_RECONCILE_MS is 2s in the rig.
	time.Sleep(3 * time.Second)
}

// httpThroughProxy is a raw HTTP call to the proxy with an api key, for the two
// assertions that are about the PROXY's own answer rather than about Kafka: the
// console read of the smart mirror, and the plain KV batch that must still be
// gated.
func httpThroughProxy(t *testing.T, method, path, key, body string) (int, string) {
	t.Helper()
	base := needEnv(t, "QKC_PROXY_URL", "this scenario reads the proxy directly")
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctxFor(t, 30*time.Second), method, base+path, rdr)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 8192)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil || sb.Len() > 1<<20 {
			break
		}
	}
	return resp.StatusCode, sb.String()
}

// facadeLog is the facade's own log, for the two claims no client can see from
// the outside: which QUEEN_URL branch was taken, and whether a credential's
// tenant was resolved from /auth/me.
func facadeLog(t *testing.T) string {
	t.Helper()
	path := needEnv(t, "QKC_FACADE_LOG", "the facade's own lines cannot be read without it")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// record is the suite's measurement channel. The three OUT items of the design
// are MEASURED and printed, never asserted as a target: they are decisions
// somebody made, and the rig's job is to report what they cost today.
func record(t *testing.T, what, value string) {
	t.Helper()
	t.Logf("MEASURED  %-38s %s", what, value)
}
