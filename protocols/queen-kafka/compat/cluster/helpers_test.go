// Package cluster is the live acceptance suite for queen-kafka CLUSTER MODE:
// two or three facades that unmodified Kafka clients address as one cluster,
// each optionally in front of its own Queen broker of the same HA deployment.
//
// It proves the two defects the design names, and it proves them the only way
// they can be proved — with real clients against running processes:
//
//	DOUBLE DELIVERY, a routing defect. Every facade used to answer
//	FindCoordinator with itself, so one group formed twice and each generation
//	assigned every partition. Fixed by rendezvous ownership over a shared live
//	set, and NOT_COORDINATOR at a non-owner.
//
//	OFFSET REWIND, a write defect. An offset commit used to be an
//	unconditional upsert, so the loser of a race silently overwrote the winner
//	(50 became 16). Fixed by a compare-and-set fence on the commit path.
//
// Nothing here starts a stack. `rig-cluster.sh` does that — one Postgres, two
// meshed Queen brokers, three clustered facades, one facade with the cluster
// config absent, and two independent single-node facades — and then runs
// `run.sh` against it. Every address is an environment variable, so the suite
// also runs against a stack that is already up:
//
//	QUEEN_KAFKA_NODES=1@host:9092,2@host:9093,3@host:9094 ./run.sh
//
// GOWORK=off is not optional inside this repository: the root go.work does not
// list this module and a bare `go test` refuses to build it. run.sh sets it.
package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ------------------------------------------------------------------ the stack

// facade is one queen-kafka process this suite can address.
type facade struct {
	// The node id it was configured with, or singleNodeID for a facade whose
	// cluster config is absent.
	id   int32
	addr string
}

// The identity a facade with no QUEEN_KAFKA_NODE_ID advertises, and the one id
// cluster mode reserves and will never hand out (handlers/metadata.rs,
// SINGLE_NODE_ID). Seeing it in a clustered Metadata response would mean the
// facade fell out of cluster mode.
const singleNodeID int32 = 0

var (
	nodes      []facade // QUEEN_KAFKA_NODES, the clustered facades
	singleAddr string   // QUEEN_KAFKA_SINGLE, cluster config absent
	splitAddrs []string // QUEEN_KAFKA_SPLIT, two independent single-node facades
	partitions int32
	ttl        time.Duration
	joinDelay  time.Duration
	killCmd    string
	stopCmd    string
	startCmd   string
	logDir     string
	runID      string
)

// failer is what parseEnv reports through. It is not testing.TB, because
// testing.TB cannot be implemented outside the testing package and TestMain has
// no *testing.T to hand it — the same parser has to serve both.
type failer interface {
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func parseEnv(tb failer) {
	spec := os.Getenv("QUEEN_KAFKA_NODES")
	if spec == "" {
		tb.Fatal("QUEEN_KAFKA_NODES is unset; see run.sh")
	}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		id, addr, ok := strings.Cut(entry, "@")
		if !ok {
			tb.Fatalf("QUEEN_KAFKA_NODES entry %q is not <id>@<host>:<port>", entry)
		}
		n, err := strconv.Atoi(id)
		if err != nil {
			tb.Fatalf("QUEEN_KAFKA_NODES entry %q: %v", entry, err)
		}
		nodes = append(nodes, facade{id: int32(n), addr: addr})
	}
	singleAddr = os.Getenv("QUEEN_KAFKA_SINGLE")
	if v := os.Getenv("QUEEN_KAFKA_SPLIT"); v != "" {
		splitAddrs = strings.Split(v, ",")
	}
	partitions = int32(envInt(tb, "QUEEN_KAFKA_PARTITIONS", 8))
	ttl = time.Duration(envInt(tb, "QUEEN_KAFKA_TTL_MS", 10000)) * time.Millisecond
	joinDelay = time.Duration(envInt(tb, "QUEEN_KAFKA_JOIN_DELAY_MS", 3000)) * time.Millisecond
	killCmd = os.Getenv("QUEEN_KAFKA_KILL_CMD")
	stopCmd = os.Getenv("QUEEN_KAFKA_STOP_CMD")
	startCmd = os.Getenv("QUEEN_KAFKA_START_CMD")
	logDir = os.Getenv("QUEEN_KAFKA_LOGDIR")
	runID = os.Getenv("RUN_ID")
	if runID == "" {
		runID = strconv.FormatInt(time.Now().Unix(), 10)
	}
}

func envInt(tb failer, key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		tb.Fatalf("%s=%q is not a positive integer", key, v)
	}
	return n
}

// addrs is the bootstrap list of every clustered facade.
func addrs() []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.addr)
	}
	return out
}

func nodeByID(id int32) (facade, bool) {
	for _, n := range nodes {
		if n.id == id {
			return n, true
		}
	}
	return facade{}, false
}

// takeoverBudget is how long a group may take to move after its owner dies:
// the registry TTL has to run out before the dead node leaves every view, and
// then the group re-forms behind the facade's join delay. Everything else in
// the suite that waits on a membership change waits this long, so raising
// QUEEN_KAFKA_CLUSTER_TTL_MS in a rig never needs an edit here.
func takeoverBudget() time.Duration { return 2*ttl + 4*joinDelay + 20*time.Second }

// TestMain refuses to run a suite against a stack that is not there, and
// refuses to run it against a cluster that has not converged: a facade that
// cannot see its peers answers COORDINATOR_NOT_AVAILABLE to everything, and
// every assertion below would fail for a reason that is not a finding.
func TestMain(m *testing.M) {
	fatal := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "cluster: "+format+"\n", args...)
		os.Exit(1)
	}
	parseEnv(fatalTB{fatal})

	for _, addr := range append(append(addrs(), splitAddrs...), singleAddr) {
		if addr == "" {
			continue
		}
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			fatal("cannot reach a facade at %s: %v\nStart the rig (compat/cluster/rig-cluster.sh) "+
				"or point QUEEN_KAFKA_NODES at one.", addr, err)
		}
		_ = c.Close()
	}
	if err := waitConverged(60 * time.Second); err != nil {
		fatal("%v", err)
	}
	if logDir != "" {
		markLogs()
	}
	os.Exit(m.Run())
}

type fatalTB struct{ f func(string, ...any) }

func (f fatalTB) Fatalf(format string, args ...any) { f.f(format, args...) }
func (f fatalTB) Fatal(args ...any)                 { f.f("%s", fmt.Sprint(args...)) }

// waitConverged blocks until every clustered facade lists every clustered
// facade. One implementation, used at boot and again after a node restart, so
// "the cluster has converged" cannot mean two different things in one run.
func waitConverged(budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			ids, err := brokerIDs(n.addr)
			if err != nil {
				ok, last = false, fmt.Sprintf("node %d at %s: %v", n.id, n.addr, err)
				break
			}
			if len(ids) != len(nodes) {
				ok, last = false, fmt.Sprintf("node %d at %s lists %v, want %d nodes", n.id, n.addr, ids, len(nodes))
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("the cluster never converged within %s: %s", budget, last)
}

// brokerIDs is the cheap half of a Metadata: who does this facade say is live.
func brokerIDs(addr string) ([]int32, error) {
	k, err := dialRaw("probe", addr)
	if err != nil {
		return nil, err
	}
	defer k.Close()
	req := kmsg.NewPtrMetadataRequest()
	resp, err := k.do(req, 6)
	if err != nil {
		return nil, err
	}
	md := resp.(*kmsg.MetadataResponse)
	out := make([]int32, 0, len(md.Brokers))
	for _, b := range md.Brokers {
		out = append(out, b.NodeID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// ------------------------------------------------------------ raw connections

// rawConn is one TCP socket speaking Kafka at exactly the version asked for.
//
// franz-go's kgo.Client is deliberately not used to carry these: it routes a
// group request to the coordinator it discovered, which is precisely the
// routing under test — "send this JoinGroup to a node that is NOT the owner"
// is unexpressible through a client whose whole job is to avoid doing that.
// The technique is compat/differential/wire.go's, trimmed to what this suite
// needs.
type rawConn struct {
	label string
	c     net.Conn
	corr  int32
	f     *kmsg.RequestFormatter
}

func dialRaw(label, addr string) (*rawConn, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s (%s): %w", label, addr, err)
	}
	return &rawConn{
		label: label,
		c:     c,
		f:     kmsg.NewRequestFormatter(kmsg.FormatterClientID("qk-cluster-" + runID)),
	}, nil
}

func mustDial(t *testing.T, label, addr string) *rawConn {
	t.Helper()
	k, err := dialRaw(label, addr)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(k.Close)
	return k
}

func (k *rawConn) Close() {
	if k.c != nil {
		_ = k.c.Close()
	}
}

func (k *rawConn) do(req kmsg.Request, version int16) (kmsg.Response, error) {
	req.SetVersion(version)
	k.corr++
	buf := k.f.AppendRequest(nil, req, k.corr)
	if err := k.c.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, err
	}
	if _, err := k.c.Write(buf); err != nil {
		return nil, fmt.Errorf("write %T v%d to %s: %w", req, version, k.label, err)
	}
	if err := k.c.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return nil, err
	}
	var sz [4]byte
	if _, err := io.ReadFull(k.c, sz[:]); err != nil {
		return nil, fmt.Errorf("read size from %s: %w", k.label, err)
	}
	n := int32(binary.BigEndian.Uint32(sz[:]))
	if n < 4 || n > 100<<20 {
		return nil, fmt.Errorf("%s answered a %d byte frame", k.label, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(k.c, body); err != nil {
		return nil, fmt.Errorf("read body from %s: %w", k.label, err)
	}
	resp := req.ResponseKind()
	resp.SetVersion(version)
	// The first four bytes are the correlation id; only one request is ever in
	// flight on one of these sockets, so it is read past rather than matched.
	payload := body[4:]
	// ApiVersions is the one API whose response header is v0 even when the body
	// is flexible; nothing here asks for it, but the rule is cheap to keep.
	if resp.IsFlexible() && resp.Key() != 18 {
		rest, err := skipTags(payload)
		if err != nil {
			return nil, fmt.Errorf("%s response header tags: %w", k.label, err)
		}
		payload = rest
	}
	if err := resp.ReadFrom(payload); err != nil {
		return nil, fmt.Errorf("decode %T v%d from %s: %w", resp, version, k.label, err)
	}
	return resp, nil
}

func (k *rawConn) must(t *testing.T, req kmsg.Request, version int16) kmsg.Response {
	t.Helper()
	resp, err := k.do(req, version)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return resp
}

// skipTags walks the tag buffer at the head of a flexible response header.
func skipTags(b []byte) ([]byte, error) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 {
		return nil, fmt.Errorf("bad tag count")
	}
	b = b[adv:]
	for i := uint64(0); i < n; i++ {
		if _, adv = binary.Uvarint(b); adv <= 0 {
			return nil, fmt.Errorf("bad tag key")
		}
		b = b[adv:]
		size, adv := binary.Uvarint(b)
		if adv <= 0 || uint64(len(b[adv:])) < size {
			return nil, fmt.Errorf("bad tag size")
		}
		b = b[adv+int(size):]
	}
	return b, nil
}

// --------------------------------------------------------------- the requests

// view is what one facade says the cluster looks like.
type view struct {
	from       string
	brokers    map[int32]string // node id -> host:port
	controller int32
	clusterID  string
	leaders    map[int32]int32 // partition -> leader node id, for one topic
	replicas   map[int32][]int32
	isr        map[int32][]int32
	epochs     map[int32]int32
}

func (v view) ids() []int32 {
	out := make([]int32, 0, len(v.brokers))
	for id := range v.brokers {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// metadataView asks one facade for the cluster and (optionally) one topic.
// Version 7 is the workhorse: it is the lowest that carries every field the
// assertions read (cluster id at v2, offline replicas at v5, leader epoch at
// v7) and it is not flexible, so a decode failure is about the facade and not
// about tag buffers.
func metadataView(t *testing.T, label, addr, topic string) view {
	t.Helper()
	k := mustDial(t, label, addr)
	req := kmsg.NewPtrMetadataRequest()
	if topic != "" {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = &topic
		req.Topics = append(req.Topics, rt)
		req.AllowAutoTopicCreation = true
	}
	md := k.must(t, req, 7).(*kmsg.MetadataResponse)

	v := view{
		from:       label,
		brokers:    map[int32]string{},
		controller: md.ControllerID,
		leaders:    map[int32]int32{},
		replicas:   map[int32][]int32{},
		isr:        map[int32][]int32{},
		epochs:     map[int32]int32{},
	}
	if md.ClusterID != nil {
		v.clusterID = *md.ClusterID
	}
	for _, b := range md.Brokers {
		v.brokers[b.NodeID] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}
	for _, mt := range md.Topics {
		if mt.Topic == nil || *mt.Topic != topic {
			continue
		}
		if mt.ErrorCode != 0 {
			t.Fatalf("%s: metadata for %s: error code %d", label, topic, mt.ErrorCode)
		}
		for _, p := range mt.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("%s: metadata for %s/%d: error code %d", label, topic, p.Partition, p.ErrorCode)
			}
			v.leaders[p.Partition] = p.Leader
			v.replicas[p.Partition] = p.Replicas
			v.isr[p.Partition] = p.ISR
			v.epochs[p.Partition] = p.LeaderEpoch
		}
	}
	return v
}

// coordinator is one FindCoordinator answer.
type coordinator struct {
	errCode int16
	nodeID  int32
	addr    string
}

func (c coordinator) String() string {
	return fmt.Sprintf("err=%d node=%d at %s", c.errCode, c.nodeID, c.addr)
}

// findCoordinator asks ONE facade who coordinates a group. v1 is used because
// it is the lowest with CoordinatorType, and the type is what separates a group
// lookup from the transactional one the facade refuses.
func findCoordinator(t *testing.T, label, addr, group string) coordinator {
	t.Helper()
	k := mustDial(t, label, addr)
	return findCoordinatorOn(t, k, group)
}

func findCoordinatorOn(t *testing.T, k *rawConn, group string) coordinator {
	t.Helper()
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = group
	req.CoordinatorType = 0
	resp := k.must(t, req, 1).(*kmsg.FindCoordinatorResponse)
	return coordinator{
		errCode: resp.ErrorCode,
		nodeID:  resp.NodeID,
		addr:    fmt.Sprintf("%s:%d", resp.Host, resp.Port),
	}
}

// joinGroupErr sends a JoinGroup to one named facade and returns only its error
// code. v2 is the last non-flexible version and carries no instance id, which
// is the shape every client in the matrix speaks against this facade.
func joinGroupErr(t *testing.T, label, addr, group string) int16 {
	t.Helper()
	k := mustDial(t, label, addr)
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = group
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 10000
	req.MemberID = ""
	req.ProtocolType = "consumer"
	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	// A consumer subscription of version 0 with no topics and no userdata: the
	// smallest thing a coordinator would accept, because this request is only
	// ever meant to be REFUSED before the coordinator sees it.
	proto.Metadata = []byte{0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}
	req.Protocols = append(req.Protocols, proto)
	resp := k.must(t, req, 2).(*kmsg.JoinGroupResponse)
	return resp.ErrorCode
}

// commitSimple writes one offset as a SIMPLE CONSUMER — generation -1, empty
// member id — through one named facade, and returns the per-partition error.
//
// The simple-consumer path is deliberate: it is the one that has no membership
// to be fenced by, so it isolates the NODE checks (the ownership guard and the
// CAS fence) from the coordinator's own generation check. It is also exactly
// the shape that produced the measured 50-then-16 rewind.
//
// v2 because that is the floor segmentio/kafka-go's Conn path writes without
// negotiating, and the advertised range's lower bound is load-bearing for it.
func commitSimple(t *testing.T, label, addr, group, topic string, partition int32, offset int64) int16 {
	t.Helper()
	k := mustDial(t, label, addr)
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.Generation = -1
	req.MemberID = ""
	req.RetentionTimeMillis = -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = partition
	rp.Offset = offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp := k.must(t, req, 2).(*kmsg.OffsetCommitResponse)
	for _, rtr := range resp.Topics {
		for _, p := range rtr.Partitions {
			if p.Partition == partition {
				return p.ErrorCode
			}
		}
	}
	t.Fatalf("%s: OffsetCommit answered nothing for %s/%d", label, topic, partition)
	return 0
}

// committed reads a group's committed offsets for one topic through one named
// facade. OffsetFetch is deliberately NOT gated by ownership (it is a read of
// shared state and its answer is identical at every node), so this doubles as
// the assertion that a non-owner still serves it.
func committed(t *testing.T, label, addr, group, topic string) map[int32]int64 {
	t.Helper()
	k := mustDial(t, label, addr)
	return committedOn(t, k, label, group, topic)
}

func committedOn(t *testing.T, k *rawConn, label, group, topic string) map[int32]int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < partitions; p++ {
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = append(req.Topics, rt)
	resp := k.must(t, req, 1).(*kmsg.OffsetFetchResponse)
	out := map[int32]int64{}
	for _, rtr := range resp.Topics {
		for _, p := range rtr.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("%s: OffsetFetch %s/%d: error code %d", label, topic, p.Partition, p.ErrorCode)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out
}

// fetchRaw reads one partition through one named facade, whatever Metadata said
// leads it. It returns the error code, the high watermark and the undecoded
// record batches — a substring search over those is enough to prove a record
// arrived without a batch parser, because nothing here produces compressed.
func fetchRaw(t *testing.T, label, addr, topic string, partition int32, at int64) (int16, int64, []byte) {
	t.Helper()
	k := mustDial(t, label, addr)
	req := kmsg.NewPtrFetchRequest()
	req.ReplicaID = -1
	req.MaxWaitMillis = 3000
	req.MinBytes = 1
	req.MaxBytes = 10 << 20
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition = partition
	rp.FetchOffset = at
	rp.PartitionMaxBytes = 10 << 20
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	// v6 is the facade's advertised ceiling (no fetch sessions, and no
	// follower fetch, which is what makes the isr_nodes decision safe).
	resp := k.must(t, req, 6).(*kmsg.FetchResponse)
	for _, rtr := range resp.Topics {
		for _, p := range rtr.Partitions {
			if p.Partition == partition {
				return p.ErrorCode, p.HighWatermark, p.RecordBatches
			}
		}
	}
	t.Fatalf("%s: Fetch answered nothing for %s/%d", label, topic, partition)
	return 0, 0, nil
}

// endOffset asks one facade for a partition's log end, whatever leads it.
func endOffset(t *testing.T, label, addr, topic string, partition int32) (int16, int64) {
	t.Helper()
	k := mustDial(t, label, addr)
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.Timestamp = -1 // latest
	rp.MaxNumOffsets = 1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp := k.must(t, req, 2).(*kmsg.ListOffsetsResponse)
	for _, rtr := range resp.Topics {
		for _, p := range rtr.Partitions {
			if p.Partition == partition {
				return p.ErrorCode, p.Offset
			}
		}
	}
	t.Fatalf("%s: ListOffsets answered nothing for %s/%d", label, topic, partition)
	return 0, 0
}

// ------------------------------------------------------------ franz-go client

// newClient builds a client with the two defaults every test here shares:
// idempotence off, because InitProducerId is deliberately not implemented, and
// a manual partitioner, because every produce in this suite names its partition
// and the assertions are about the facade's mapping, not a partitioner's hash.
func newClient(t *testing.T, bootstrap []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(bootstrap...),
		kgo.DisableIdempotentWrite(),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(20),
		// The retry window has to outlast a takeover: a client that gives up
		// after 5 s of NOT_COORDINATOR would turn the design's documented
		// blackout into a test failure.
		kgo.RetryTimeout(takeoverBudget()),
	}
	if os.Getenv("COMPAT_VERBOSE") != "" {
		base = append(base, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// eagerGroup is the option set every group test here shares. Eager (range, then
// round-robin) rather than franz-go's cooperative-sticky default: eager revokes
// the whole assignment on every rebalance, which is what actually exercises the
// coordinator's generation handling, and it is what the older clients in the
// compatibility matrix speak.
func eagerGroup(group, topic string, extra ...kgo.Opt) []kgo.Opt {
	return append([]kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.RangeBalancer(), kgo.RoundRobinBalancer()),
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(time.Second),
	}, extra...)
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// A topic and a group name unique to this run of this test. Both are durable —
// a topic is a Queen queue and a group's offsets live in Queen KV — so a re-run
// must never inherit the previous one's state.
func newName(t *testing.T, kind string) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '-'
		}
	}, t.Name())
	return fmt.Sprintf("qkc-%s-%s-%s-%d", kind, name, runID, time.Now().UnixNano()%1e6)
}

// seed produces perPartition records to every partition of a fresh topic
// through the given bootstrap, and returns the topic and the total.
func seed(t *testing.T, bootstrap []string, perPartition int) (string, int) {
	t.Helper()
	topic := newName(t, "t")
	cl := newClient(t, bootstrap, kgo.RequiredAcks(kgo.AllISRAcks()))
	ensureTopic(t, cl, topic)

	var recs []*kgo.Record
	for p := int32(0); p < partitions; p++ {
		for i := 0; i < perPartition; i++ {
			recs = append(recs, &kgo.Record{
				Topic:     topic,
				Partition: p,
				Key:       []byte(fmt.Sprintf("p%d-%d", p, i)),
				Value:     []byte(fmt.Sprintf("v-%d-%d", p, i)),
			})
		}
	}
	results := cl.ProduceSync(ctxFor(t, 120*time.Second), recs...)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("seed produce: %v", err)
	}
	return topic, len(recs)
}

// ensureTopic makes the topic exist and asserts its width, so a later failure
// is never "the topic was still being created".
func ensureTopic(t *testing.T, cl *kgo.Client, topic string) {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	req.Topics = append(req.Topics, rt)
	req.AllowAutoTopicCreation = true
	resp, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("Metadata(%s): %v", topic, err)
	}
	md := resp.(*kmsg.MetadataResponse)
	for _, mt := range md.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			if mt.ErrorCode != 0 {
				t.Fatalf("topic %s: metadata error code %d", topic, mt.ErrorCode)
			}
			if got := int32(len(mt.Partitions)); got != partitions {
				t.Fatalf("topic %s: %d partitions, want %d", topic, got, partitions)
			}
			return
		}
	}
	t.Fatalf("topic %s is not in the metadata response", topic)
}

// ------------------------------------------------------------------- counting

// seen is the record ledger every consumption assertion is made against: which
// member saw which key, and how many times.
type seen struct {
	mu     sync.Mutex
	byKey  map[string]int
	byPart map[int32]map[string]bool // partition -> set of member labels
	total  int
}

func newSeen() *seen {
	return &seen{byKey: map[string]int{}, byPart: map[int32]map[string]bool{}}
}

// add takes a partition and a key rather than a client's record type, so
// franz-go and segmentio/kafka-go are judged by one implementation of "every
// record once, no partition shared".
func (s *seen) add(member string, partition int32, key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[string(key)]++
	if s.byPart[partition] == nil {
		s.byPart[partition] = map[string]bool{}
	}
	s.byPart[partition][member] = true
	s.total++
}

func (s *seen) uniqueKeys() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byKey)
}

func (s *seen) totalRecords() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// duplicates lists the keys delivered more than once, with their counts.
func (s *seen) duplicates() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for k, n := range s.byKey {
		if n > 1 {
			out[k] = n
		}
	}
	return out
}

// sharedPartitions lists the partitions more than one member ever read from —
// the direct measurement of the double-delivery defect.
func (s *seen) sharedPartitions() map[int32][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[int32][]string{}
	for p, members := range s.byPart {
		if len(members) > 1 {
			names := make([]string, 0, len(members))
			for m := range members {
				names = append(names, m)
			}
			sort.Strings(names)
			out[p] = names
		}
	}
	return out
}

func (s *seen) partitionsOf(member string) []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int32
	for p, members := range s.byPart {
		if members[member] {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ------------------------------------------------------------- the group body

// runMembers is THE suite body: N members of one group, each with its own
// bootstrap list, all consuming and committing until every record has been
// seen once or the budget runs out. It returns the ledger and the still-open
// clients, so the caller can read each member's generation before closing them.
//
// The cluster acceptance and the single-node regression call this same
// function with different bootstrap lists and nothing else, which is the point:
// "cluster mode changes nothing a client can see except the broker list" is a
// claim about behaviour, and the honest way to test it is to run one body twice.
func runMembers(t *testing.T, group, topic string, total int, bootstraps [][]string) (*seen, []*kgo.Client) {
	t.Helper()
	ledger := newSeen()
	clients := make([]*kgo.Client, len(bootstraps))
	deadline := time.Now().Add(takeoverBudget())
	var wg sync.WaitGroup
	for i, boot := range bootstraps {
		member := fmt.Sprintf("m%d@%s", i+1, strings.Join(boot, "+"))
		cl := newClient(t, boot, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
		clients[i] = cl
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ledger.uniqueKeys() < total {
				ctx, cancel := context.WithDeadline(context.Background(), deadline)
				fs := cl.PollRecords(ctx, 64)
				cancel()
				var batch []*kgo.Record
				fs.EachRecord(func(r *kgo.Record) {
					ledger.add(member, r.Partition, r.Key)
					batch = append(batch, r)
				})
				if len(batch) == 0 {
					continue
				}
				// Committed as we go, so the offset sampler has something to
				// watch and so a failure is about the offsets a real client
				// wrote rather than about one commit at the end.
				cctx, ccancel := context.WithDeadline(context.Background(), deadline)
				if err := cl.CommitRecords(cctx, batch...); err != nil {
					// A commit that loses a rebalance is legitimate and the
					// client re-commits. What matters is the final state, and
					// that is asserted by the caller.
					t.Logf("%s: commit of %d records: %v", member, len(batch), err)
				}
				ccancel()
			}
		}()
	}
	wg.Wait()
	return ledger, clients
}

// assertOneDelivery is the other half of the shared body: every record once,
// no partition read by two members, every partition covered.
func assertOneDelivery(t *testing.T, ledger *seen, total int, members []string) {
	t.Helper()
	if got := ledger.uniqueKeys(); got != total {
		t.Errorf("the group delivered %d distinct keys, want %d", got, total)
	}
	if dups := ledger.duplicates(); len(dups) != 0 {
		t.Errorf("%d keys were delivered more than once (the double-delivery defect): %v",
			len(dups), firstFew(dups, 5))
	}
	if got := ledger.totalRecords(); got != total {
		t.Errorf("the group delivered %d records for %d distinct keys: at-least-once is allowed on a "+
			"failover, but nothing failed over here", got, total)
	}
	if shared := ledger.sharedPartitions(); len(shared) != 0 {
		t.Errorf("%d partitions were read by more than one member: %v", len(shared), shared)
	}
	covered := map[int32]bool{}
	for _, member := range members {
		ps := ledger.partitionsOf(member)
		t.Logf("%s read partitions %v", member, ps)
		if len(ps) == 0 {
			t.Errorf("%s read nothing: the assignment did not reach it", member)
		}
		for _, p := range ps {
			covered[p] = true
		}
	}
	if len(covered) != int(partitions) {
		t.Errorf("the members between them covered %d partitions, want %d", len(covered), partitions)
	}
}

// assertOneGeneration reads each member's own view of the group.
//
// It is the assertion no wire probe can make: independent coordinators cannot
// hand one generation number to members that never spoke to each other, and
// they cannot hand out distinct member ids either. Two coordinators would each
// be at generation 1 with its own member — which is exactly what the measured
// two-facade experiment produced, and exactly what looks fine from inside
// either client.
//
// kgo.Client.GroupMetadata returns (MEMBER ID, generation) — not the group
// name; there is no group getter, and the group is what the caller passed to
// ConsumerGroup in the first place.
func assertOneGeneration(t *testing.T, clients []*kgo.Client) {
	t.Helper()
	// Sampled with a short retry rather than once: the last member to finish
	// consuming can still be a heartbeat behind the generation the others are
	// already in, and a stale read of a converging group is not a finding. What
	// would be a finding — two generations that never converge — outlives this.
	var gens map[int32][]string
	deadline := time.Now().Add(4 * joinDelay)
	for {
		gens = map[int32][]string{}
		for i, cl := range clients {
			member, gen := cl.GroupMetadata()
			if member == "" || gen < 0 {
				t.Errorf("member %d is not in the group (member id %q, generation %d)", i+1, member, gen)
				continue
			}
			gens[gen] = append(gens[gen], member)
		}
		if len(gens) == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(gens) != 1 {
		t.Errorf("the %d members report %d different generations: %v", len(clients), len(gens), gens)
		return
	}
	for gen, ids := range gens {
		seen := map[string]bool{}
		for _, id := range ids {
			if seen[id] {
				t.Errorf("two members share the member id %q in generation %d", id, gen)
			}
			seen[id] = true
		}
		t.Logf("all %d members are in generation %d with distinct member ids %v", len(clients), gen, ids)
	}
}

// ----------------------------------------------------- the committed-offset watch

// offsetWatch samples a group's committed offsets while it consumes and records
// any partition whose committed offset went BACKWARDS. That is the measured
// defect stated as a monitor: the 50-then-16 rewind was invisible at the time
// it happened and only showed up as re-delivery later.
type offsetWatch struct {
	mu         sync.Mutex
	high       map[int32]int64
	regression []string
	samples    int
	stop       chan struct{}
	done       chan struct{}
}

// watchOffsets samples through the given facade — which the caller should make
// a NON-owner where it can, since OffsetFetch is the one group API cluster mode
// deliberately serves everywhere.
func watchOffsets(t *testing.T, label, addr, group, topic string, every time.Duration) *offsetWatch {
	t.Helper()
	w := &offsetWatch{high: map[int32]int64{}, stop: make(chan struct{}), done: make(chan struct{})}
	k := mustDial(t, label, addr)
	go func() {
		defer close(w.done)
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
			req := kmsg.NewPtrOffsetFetchRequest()
			req.Group = group
			rt := kmsg.NewOffsetFetchRequestTopic()
			rt.Topic = topic
			for p := int32(0); p < partitions; p++ {
				rt.Partitions = append(rt.Partitions, p)
			}
			req.Topics = append(req.Topics, rt)
			resp, err := k.do(req, 1)
			if err != nil {
				// A sampler that fails is not a finding by itself: the socket
				// may have been closed under it by a facade that died on
				// purpose. Recorded as a gap, not as a regression.
				continue
			}
			w.mu.Lock()
			w.samples++
			for _, rtr := range resp.(*kmsg.OffsetFetchResponse).Topics {
				for _, p := range rtr.Partitions {
					if p.ErrorCode != 0 || p.Offset < 0 {
						continue
					}
					if prev, ok := w.high[p.Partition]; ok && p.Offset < prev {
						w.regression = append(w.regression, fmt.Sprintf(
							"%s/%d: committed offset went %d -> %d", rtr.Topic, p.Partition, prev, p.Offset))
					} else {
						w.high[p.Partition] = p.Offset
					}
				}
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *offsetWatch) finish() (samples int, regressions []string, high map[int32]int64) {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[int32]int64{}
	for k, v := range w.high {
		out[k] = v
	}
	return w.samples, append([]string(nil), w.regression...), out
}

// ------------------------------------------------------------ node death rigging

func killNode(t *testing.T, id int32) {
	t.Helper()
	if killCmd == "" {
		t.Fatal("QUEEN_KAFKA_KILL_CMD is unset")
	}
	out, err := exec.Command(killCmd, strconv.Itoa(int(id))).CombinedOutput()
	t.Logf("kill node %d: %s", id, strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("kill node %d: %v", id, err)
	}
}

// stopNode SIGTERMs a facade and waits for it to be gone. It is the DEPLOY
// counterpart of killNode: the process gets to run its shutdown, which in
// cluster mode means handing its registry row back. The distinction is the
// whole subject of the rolling-restart scenario, so the two never share a
// command.
func stopNode(t *testing.T, id int32) {
	t.Helper()
	if stopCmd == "" {
		t.Fatal("QUEEN_KAFKA_STOP_CMD is unset")
	}
	out, err := exec.Command(stopCmd, strconv.Itoa(int(id))).CombinedOutput()
	t.Logf("stop node %d: %s", id, strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("stop node %d: %v", id, err)
	}
}

func startNode(t *testing.T, id int32) {
	t.Helper()
	if startCmd == "" {
		t.Fatal("QUEEN_KAFKA_START_CMD is unset")
	}
	out, err := exec.Command(startCmd, strconv.Itoa(int(id))).CombinedOutput()
	t.Logf("start node %d: %s", id, strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("start node %d: %v", id, err)
	}
}

// ------------------------------------------------------------------- log scan

var (
	logMarks     = map[string]int64{}
	restarted    = map[string]bool{}
	restartedMu  sync.Mutex
	ansiEscape   = regexp.MustCompile("\x1b\\[[0-9;]*m")
	facadeLogs   = []string{"node-1", "node-2", "node-3", "single", "split-a", "split-b"}
	levelPattern = regexp.MustCompile(`\b(WARN|ERROR)\b`)
)

// markLogs records how long each facade log already is, so the scan at the end
// reads only what this suite caused. Boot noise from before the first test —
// a node that ticked once before its peers registered, most of all — is not
// this suite's to judge.
func markLogs() {
	for _, name := range facadeLogs {
		if fi, err := os.Stat(logPath(name)); err == nil {
			logMarks[name] = fi.Size()
		}
	}
}

func logPath(name string) string { return logDir + "/" + name + ".log" }

func noteRestart(name string) {
	restartedMu.Lock()
	defer restartedMu.Unlock()
	restarted[name] = true
}

func wasRestarted(name string) bool {
	restartedMu.Lock()
	defer restartedMu.Unlock()
	return restarted[name]
}

// allowed is the closed list of WARN/ERROR lines this suite expects to see, and
// the reason each one is not a finding. Anything else fails the run.
type allowance struct {
	substr string
	why    string
	// onlyAfterRestart: a line that is correct for a facade that has just come
	// back and is not correct for one that has been up all along.
	onlyAfterRestart bool
}

var allowances = []allowance{
	{
		substr: "only node in its cluster registry",
		why: "a facade that has just (re)started can tick twice before its peers' rows " +
			"are in its own view; it is the misconfiguration warning, and it is transient here",
		onlyAfterRestart: true,
	},
	{
		substr: "id is taken back",
		why: "the crash-restart scenario restarts a node INSIDE its own TTL on purpose, so the " +
			"replacement waits its predecessor's row out and says so; that line is what proves " +
			"it adopted the id instead of exiting on it",
		onlyAfterRestart: true,
	},
	{
		substr: "id is taken over",
		why: "the same, for a predecessor whose row outlives the watch instead of expiring " +
			"inside it: never refreshed is never alive, and the take-over is fenced",
		onlyAfterRestart: true,
	},
}

// scanFacadeLogs fails the run on any WARN or ERROR a facade emitted during the
// suite that is not on the allow-list above. It is the assertion nothing else
// can make: a fenced commit, a lost node id, an unreachable registry and a
// facade alone in its cluster are all correct-looking on the wire and loud only
// in the log.
func scanFacadeLogs(t *testing.T) {
	t.Helper()
	if logDir == "" {
		t.Skip("QUEEN_KAFKA_LOGDIR is unset: nothing to scan")
	}
	bad := 0
	for _, name := range facadeLogs {
		body, err := os.ReadFile(logPath(name))
		if err != nil {
			continue // a facade this rig did not start
		}
		if mark := logMarks[name]; mark > 0 && int64(len(body)) > mark {
			body = body[mark:]
		} else if mark > 0 {
			continue
		}
		for _, raw := range strings.Split(string(body), "\n") {
			line := ansiEscape.ReplaceAllString(raw, "")
			if !levelPattern.MatchString(line) {
				continue
			}
			ok := false
			for _, a := range allowances {
				if !strings.Contains(line, a.substr) {
					continue
				}
				if a.onlyAfterRestart && !wasRestarted(name) {
					break
				}
				ok = true
				t.Logf("%s: allowed (%s): %s", name, a.why, line)
				break
			}
			if !ok {
				bad++
				t.Errorf("%s: unexpected WARN/ERROR: %s", name, line)
			}
		}
	}
	if bad == 0 {
		t.Logf("no unexpected WARN or ERROR in %d facade logs", len(facadeLogs))
	}
}

// containsAll reports whether every needle appears in the haystack. Used on
// undecoded record batches, where the values are literal because nothing here
// produces compressed.
func containsAll(hay []byte, needles ...string) bool {
	for _, n := range needles {
		if !bytes.Contains(hay, []byte(n)) {
			return false
		}
	}
	return true
}
