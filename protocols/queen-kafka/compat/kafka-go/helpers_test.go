// Package compat drives a running queen-kafka facade with segmentio/kafka-go,
// the other big pure-Go Kafka client, and asserts what a real client observes:
// metadata, produce (every codec kafka-go ships), consume through a consumer
// GROUP, offset commit and resume across a restarted member, ListOffsets bounds,
// seek on a partition reader, auto-topic-creation, and — when the rig hands it a
// TLS/SASL listener — one produce+consume through SASL/PLAIN over TLS.
//
// Nothing here starts a stack. `compat/rig.sh` does that for the franz-go suite;
// this directory's `run.sh` assumes a stack is already up and reads every
// address from the environment, so it can be wired into rig.sh later without
// changing a line of Go.
//
//	QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:19092 ./run.sh
//
// # WHY kafka-go IS A DIFFERENT TEST FROM franz-go
//
// kafka-go carries TWO protocol stacks and uses both at once, which is exactly
// what makes it interesting here:
//
//   - `Writer` (and the `Client` request helpers) go through the newer
//     `protocol` package over a `Transport`, which negotiates each API down from
//     the client's compiled-in maximum to the broker's advertised maximum.
//   - `Reader` — including everything a consumer GROUP does — goes through the
//     older `Conn`, which does NOT negotiate freely. It offers a short hardcoded
//     list per API (`conn.go`: fetch v2/v5/v10, produce v2/v3/v7, metadata
//     v1/v6, joinGroup v1/v2) and for four of the group APIs no list at all:
//     FindCoordinator v0, SyncGroup v0, Heartbeat v0, LeaveGroup v0,
//     OffsetCommit **v2** and OffsetFetch **v1**, written straight into the
//     request with no negotiation.
//
// The second half matters because `apiVersionMap.negotiate` (conn.go:72) only
// ever compares against the broker's MaxVersion — it never reads MinVersion. A
// broker whose floor rises above one of those hardcoded numbers gets an
// out-of-window request from kafka-go and, against this facade, answers by
// closing the connection (compat/ERRORS.md). queen-kafka advertises
// OffsetCommit 2-6 and OffsetFetch 1-7, so kafka-go sits EXACTLY on both floors.
// `TestApiVersionsAndNegotiation` pins that, and the wire sniffer below prints
// what was actually negotiated rather than what the docs claim.
//
// WHAT IS THE CLIENT'S FAULT, NOT THE FACADE'S
//
//   - A `&kafka.Writer{}` composite literal defaults `RequiredAcks` to
//     `RequireNone` (acks=0). `kafka.NewWriter(cfg)` rewrites 0 to RequireAll
//     (writer.go:508). The facade writes NO response frame at all for acks=0, so
//     that Writer cannot see an error and reports offsets it invented itself.
//     Every Writer here sets RequiredAcks explicitly; `TestProduceAcksZero`
//     documents the trap without asserting on the invented offsets.
//   - kafka-go has no idempotent producer and never sends InitProducerId, so the
//     fatal INIT_PRODUCER_ID failure that kills a default Java producer against
//     this facade simply cannot happen here. Nothing to disable.
//   - `Message.Partition` is documented read-only on write, so pinning a record
//     to a partition is done the way kafka-go intends: with a `Balancer`. The
//     one here reads the target out of the key (`p<n>-<seq>`), which keeps the
//     ordering assertions honest without abusing a read-only field.
//   - Every consumer group formation costs 3s server-side
//     (QUEEN_KAFKA_GROUP_JOIN_DELAY_MS, Kafka's group.initial.rebalance.delay.ms).
//     The rig runs the default; the deadlines here are sized for it.
//
// GOWORK=off is not optional inside this repository: the root go.work does not
// list this module, so a bare `go test` refuses to build it. run.sh sets it, and
// also passes -count=1, without which Go silently replays a cached PASS.
package compat

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	defaultBootstrap  = "127.0.0.1:19092"
	defaultPartitions = 8
)

// ---------------------------------------------------------------------------
// environment
// ---------------------------------------------------------------------------

func bootstrap() string {
	if v := os.Getenv("QUEEN_KAFKA_BOOTSTRAP"); v != "" {
		return v
	}
	return defaultBootstrap
}

// The width the facade auto-creates a topic with (its
// QUEEN_KAFKA_DEFAULT_PARTITIONS). Read rather than hardcoded so the same
// assertions hold against a deployment that runs the 1024 default.
func topicWidth(t *testing.T) int {
	t.Helper()
	v := os.Getenv("QUEEN_KAFKA_PARTITIONS")
	if v == "" {
		return defaultPartitions
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("QUEEN_KAFKA_PARTITIONS=%q is not a partition count", v)
	}
	return n
}

// runID makes every topic and group name in a run unique, so reruns against a
// kept stack never collide. Convention shared with the other compat suites
// (rdk-$RUN, ck-$RUN, kjs-$RUN); ours is kgo-$RUN.
var runID = func() string {
	if v := os.Getenv("RUN_ID"); v != "" {
		return v
	}
	return strconv.FormatInt(time.Now().Unix(), 10)
}()

func topicName(kind string) string { return fmt.Sprintf("kgo-%s-%s", kind, runID) }
func groupName(kind string) string { return fmt.Sprintf("kgo-g-%s-%s", kind, runID) }

// ---------------------------------------------------------------------------
// reporting: `=== ` headers, one `  ok  ` / `  FAIL` line per assertion, and a
// final RESULT line from TestMain. Mirrors the script suites so a human reading
// two runs side by side reads the same shape.
// ---------------------------------------------------------------------------

var failures struct {
	sync.Mutex
	n int
}

func section(t *testing.T, format string, a ...any) {
	t.Helper()
	fmt.Printf("=== %s\n", fmt.Sprintf(format, a...))
}

func okf(t *testing.T, format string, a ...any) {
	t.Helper()
	fmt.Printf("  ok   %s\n", fmt.Sprintf(format, a...))
}

// failf records a failure, prints it in the shared shape and fails the test.
func failf(t *testing.T, format string, a ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, a...)
	failures.Lock()
	failures.n++
	failures.Unlock()
	fmt.Printf("  FAIL %s\n", msg)
	t.Fatal(msg)
}

// note prints something observed that is not an assertion — a client quirk, a
// codec the client declines, a version it picked.
func note(format string, a ...any) {
	fmt.Printf("  ..   %s\n", fmt.Sprintf(format, a...))
}

func TestMain(m *testing.M) {
	fmt.Printf("=== queen-kafka compat: segmentio/kafka-go\n")
	fmt.Printf("  ..   bootstrap=%s partitions=%s runID=%s\n",
		bootstrap(), os.Getenv("QUEEN_KAFKA_PARTITIONS"), runID)

	code := m.Run()

	printNegotiated()

	failures.Lock()
	n := failures.n
	failures.Unlock()
	if code == 0 && n == 0 {
		fmt.Printf("RESULT: PASS\n")
	} else {
		if n == 0 {
			n = 1 // a panic or a t.Fatal that did not go through failf
		}
		fmt.Printf("RESULT: FAIL (%d)\n", n)
	}
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// the wire sniffer
//
// kafka-go has no protocol debug stream — no equivalent of librdkafka's
// debug=protocol or the Java NetworkClient logger. So rather than ASSUME which
// version it negotiated, every connection this suite opens is wrapped in a
// net.Conn that parses the 8-byte request header of each frame the client
// writes: [int32 size][int16 api_key][int16 api_version][int32 correlation_id].
// That is the client's own byte stream, which is the strongest form of the
// evidence requirement.
// ---------------------------------------------------------------------------

var seen struct {
	sync.Mutex
	m map[int16]map[int16]int // apiKey -> apiVersion -> count
}

func recordAPI(key, version int16) {
	seen.Lock()
	defer seen.Unlock()
	if seen.m == nil {
		seen.m = map[int16]map[int16]int{}
	}
	if seen.m[key] == nil {
		seen.m[key] = map[int16]int{}
	}
	seen.m[key][version]++
}

var apiNames = map[int16]string{
	0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
	8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator", 11: "JoinGroup",
	12: "Heartbeat", 13: "LeaveGroup", 14: "SyncGroup", 15: "DescribeGroups",
	16: "ListGroups", 17: "SaslHandshake", 18: "ApiVersions", 19: "CreateTopics",
	20: "DeleteTopics", 22: "InitProducerId", 32: "DescribeConfigs",
	36: "SaslAuthenticate", 47: "OffsetDelete", 60: "DescribeCluster",
}

func apiName(k int16) string {
	if n, ok := apiNames[k]; ok {
		return n
	}
	return fmt.Sprintf("api(%d)", k)
}

func printNegotiated() {
	seen.Lock()
	defer seen.Unlock()
	fmt.Printf("=== API versions kafka-go ACTUALLY sent (sniffed off its own socket writes)\n")
	keys := make([]int, 0, len(seen.m))
	for k := range seen.m {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, k := range keys {
		vs := seen.m[int16(k)]
		vers := make([]int, 0, len(vs))
		for v := range vs {
			vers = append(vers, int(v))
		}
		sort.Ints(vers)
		parts := make([]string, 0, len(vers))
		for _, v := range vers {
			parts = append(parts, fmt.Sprintf("v%d x%d", v, vs[int16(v)]))
		}
		fmt.Printf("  ..   %-16s %s\n", apiName(int16(k)), strings.Join(parts, ", "))
	}
}

// sniffConn parses complete Kafka request frames out of the byte stream the
// client writes, tolerating partial and coalesced writes.
type sniffConn struct {
	net.Conn
	mu  sync.Mutex
	buf []byte
}

func (c *sniffConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.buf = append(c.buf, p...)
	for len(c.buf) >= 4 {
		size := int(binary.BigEndian.Uint32(c.buf[0:4]))
		if size < 4 || len(c.buf) < 4+size {
			break
		}
		frame := c.buf[4 : 4+size]
		if len(frame) >= 4 {
			recordAPI(
				int16(binary.BigEndian.Uint16(frame[0:2])),
				int16(binary.BigEndian.Uint16(frame[2:4])),
			)
		}
		c.buf = c.buf[4+size:]
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func sniffDial(ctx context.Context, network, address string) (net.Conn, error) {
	d := &net.Dialer{Timeout: 10 * time.Second}
	c, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &sniffConn{Conn: c}, nil
}

// ---------------------------------------------------------------------------
// clients
// ---------------------------------------------------------------------------

// transport is the sniffing Transport used by every Writer and Client. Each
// caller gets its own so an idle-timeout reaper in one test cannot close a
// connection another is using.
func transport() *kafka.Transport {
	return &kafka.Transport{
		Dial:     sniffDial,
		ClientID: "queen-kafka-compat-kafka-go",
	}
}

// dialer is the sniffing Dialer used by every Reader (the Conn stack).
func dialer() *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  "queen-kafka-compat-kafka-go",
		DialFunc:  sniffDial,
	}
}

func client() *kafka.Client {
	return &kafka.Client{
		Addr:      kafka.TCP(bootstrap()),
		Timeout:   30 * time.Second,
		Transport: transport(),
	}
}

// newWriter is the ONLY way this suite builds a Writer: acks is always explicit
// (see the package prose on the RequireNone default) and the transport always
// sniffs.
func newWriter(topic string, acks kafka.RequiredAcks, opts ...func(*kafka.Writer)) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(bootstrap()),
		Topic:                  topic,
		Balancer:               &keyPinnedBalancer{},
		RequiredAcks:           acks,
		AllowAutoTopicCreation: true,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           30 * time.Second,
		Transport:              transport(),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// keyPinnedBalancer routes a record by the "p<n>-" prefix of its key, so a test
// can assert per-partition ORDER without setting Message.Partition, which
// kafka-go documents as read-only on write. A key without the prefix falls back
// to kafka-go's own Hash balancer.
type keyPinnedBalancer struct{ fallback kafka.Hash }

func (b *keyPinnedBalancer) Balance(msg kafka.Message, partitions ...int) int {
	k := string(msg.Key)
	if strings.HasPrefix(k, "p") {
		if i := strings.IndexByte(k, '-'); i > 1 {
			if n, err := strconv.Atoi(k[1:i]); err == nil {
				for _, p := range partitions {
					if p == n {
						return p
					}
				}
			}
		}
	}
	return b.fallback.Balance(msg, partitions...)
}

// ---------------------------------------------------------------------------
// TLS / SASL lane (M5), enabled only when the rig exports both variables
// ---------------------------------------------------------------------------

func tlsBootstrap() string  { return os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP") }
func saslToken() string     { return os.Getenv("QUEEN_KAFKA_SASL_TOKEN") }
func tlsCACertPath() string { return os.Getenv("QUEEN_KAFKA_TLS_CA") }

// tlsConfig trusts the rig's self-signed certificate when QUEEN_KAFKA_TLS_CA
// points at it, and pins ServerName explicitly. Go sends no SNI for an IP
// literal, and the facade's SNI capture reads the SNI, so an unset ServerName
// makes the facade log sni="" — setting it is what exercises the M5 path
// (compat/go/m5_test.go sets it for the same reason).
func tlsConfig(t *testing.T, serverName string) *tls.Config {
	t.Helper()
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if p := tlsCACertPath(); p != "" {
		pem, err := os.ReadFile(p)
		if err != nil {
			failf(t, "reading QUEEN_KAFKA_TLS_CA %s: %v", p, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			failf(t, "QUEEN_KAFKA_TLS_CA %s holds no PEM certificate", p)
		}
		cfg.RootCAs = pool
	} else {
		cfg.InsecureSkipVerify = true
	}
	return cfg
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func ctxWith(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

// waitForTopic makes sure a topic exists at the expected width before a test
// leans on it.
//
// WHICH kafka-go CALL CREATES A TOPIC, AND WHICH DOES NOT — measured, and the
// distinction cost this suite a 30-second red before it was understood:
//
//   - `Dialer.LookupPartitions` goes through `Conn.ReadPartitions`, which sends
//     Metadata **v6 with AllowAutoTopicCreation: true** hardcoded (conn.go:987).
//     The facade creates the topic. This is the call used below.
//   - `Client.Metadata` sends Metadata **v8 with AllowAutoTopicCreation left
//     false** — `kafka.MetadataRequest` has no field for it at all
//     (metadata.go:14, and the round trip at metadata.go:43 sets only
//     TopicNames). The facade honours the false and answers
//     UNKNOWN_TOPIC_OR_PARTITION forever. That is the facade doing the right
//     thing at a version where the wire CAN say no; PLAN_QUEEN_KAFKA.md's
//     "auto-create cannot be refused" deviation is specifically about Metadata
//     v0-v3, which have no such field.
//
// So: creation goes through the Dialer, confirmation through the Client, and
// both halves are asserted.
func waitForTopic(t *testing.T, topic string, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	cl := client()
	var last string
	for time.Now().Before(deadline) {
		ctx, cancel := ctxWith(t, 15*time.Second)
		parts, err := dialer().LookupPartitions(ctx, "tcp", bootstrap(), topic)
		cancel()
		if err != nil {
			last = err.Error()
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if len(parts) < want {
			last = fmt.Sprintf("LookupPartitions saw %d partitions, want %d", len(parts), want)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		// Confirm through the other stack too, so a topic that exists only in
		// the Conn path's view never passes.
		ctx2, cancel2 := ctxWith(t, 15*time.Second)
		resp, err := cl.Metadata(ctx2, &kafka.MetadataRequest{Topics: []string{topic}})
		cancel2()
		if err != nil {
			last = err.Error()
			time.Sleep(300 * time.Millisecond)
			continue
		}
		for _, tp := range resp.Topics {
			if tp.Name != topic {
				continue
			}
			if tp.Error != nil {
				last = tp.Error.Error()
				break
			}
			if len(tp.Partitions) >= want {
				return
			}
			last = fmt.Sprintf("%d partitions, want %d", len(tp.Partitions), want)
		}
		time.Sleep(300 * time.Millisecond)
	}
	failf(t, "topic %s never reached %d partitions: %s", topic, want, last)
}

// payload / header round-trip fixtures. Deliberately not ASCII-only: the facade
// stores values inside a base64 envelope, so a byte that is not valid UTF-8 is
// the assertion that actually proves the envelope is byte-exact.
func payloadFor(part, seq int) []byte {
	b := []byte(fmt.Sprintf("kgo|%s|p%d|s%d|", runID, part, seq))
	return append(b, 0x00, 0xff, 0xfe, 0xc3, 0x28, byte(seq%251))
}

func headersFor(part, seq int) []kafka.Header {
	return []kafka.Header{
		{Key: "trace", Value: []byte(fmt.Sprintf("%s/%d/%d", runID, part, seq))},
		{Key: "binary", Value: []byte{0x00, 0x01, 0xff, 0xfe}},
		{Key: "empty", Value: []byte{}},
	}
}

func headerMap(hs []kafka.Header) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}
