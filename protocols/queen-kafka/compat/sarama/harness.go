package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ------------------------------------------------------------------- verdicts

type runner struct {
	fails  int
	checks int
}

func (r *runner) section(name string) { fmt.Printf("\n=== %s\n", name) }

func (r *runner) ok(format string, a ...any) {
	r.checks++
	fmt.Printf("  ok   %s\n", fmt.Sprintf(format, a...))
}

func (r *runner) fail(format string, a ...any) {
	r.checks++
	r.fails++
	fmt.Printf("  FAIL %s\n", fmt.Sprintf(format, a...))
}

func (r *runner) check(cond bool, format string, a ...any) bool {
	if cond {
		r.ok(format, a...)
	} else {
		r.fail(format, a...)
	}
	return cond
}

// info records something observed rather than asserted — the failure mode of a
// deliberately unsupported call, a latency, a negotiated version.
func (r *runner) info(format string, a ...any) {
	fmt.Printf("  ---  %s\n", fmt.Sprintf(format, a...))
}

// deadline runs fn and fails if it has not returned within d. A hang is a
// result: the check is recorded and the suite carries on rather than wedging.
// The goroutine is abandoned on timeout, deliberately — a client stuck inside a
// blocking call has no way back out.
func (r *runner) deadline(what string, d time.Duration, fn func() error) bool {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("panic: %v", p)
			}
		}()
		done <- fn()
	}()
	select {
	case err := <-done:
		if err != nil {
			return r.check(false, "%s: %v", what, err)
		}
		return r.check(true, "%s", what)
	case <-time.After(d):
		return r.check(false, "%s: HUNG, no return within %s", what, d)
	}
}

// -------------------------------------------------------------- the wire tap
//
// sarama's own debug stream never prints the version of a request it sends
// (broker.go logs only "Completed ApiVersionsRequest V%d"), and its per-API
// metrics are keyed by api key alone. So the only honest way to record what
// sarama NEGOTIATED is to read the bytes it wrote: every Kafka request frame is
// a 4-byte big-endian length followed by int16 api_key, int16 api_version. This
// dialer is installed through Config.Net.Proxy.Dialer, which sarama calls before
// it wraps the connection in TLS — so the tap reads cleartext frames on a
// plaintext listener, and is switched off for a TLS one.

type wireTap struct {
	mu    sync.Mutex
	seen  map[int16]map[int16]int
	bytes map[int16]int64 // request bytes per api key, so a codec can be shown to have compressed
	next  *wireTap        // forwards to the global tap
}

var globalTap = &wireTap{seen: map[int16]map[int16]int{}, bytes: map[int16]int64{}}

func newTap() *wireTap {
	return &wireTap{seen: map[int16]map[int16]int{}, bytes: map[int16]int64{}, next: globalTap}
}

func (t *wireTap) note(key, ver int16, n int) {
	t.mu.Lock()
	if t.seen[key] == nil {
		t.seen[key] = map[int16]int{}
	}
	t.seen[key][ver]++
	t.bytes[key] += int64(n)
	t.mu.Unlock()
	if t.next != nil {
		t.next.note(key, ver, n)
	}
}

// requests counts every frame the tap saw for one api key, across versions.
// Added for M7 F3, whose claim is about a COUNT: sarama sent 51 InitProducerId
// requests on 51 connections before the key was advertised and sends one now.
func (t *wireTap) requests(key int16) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.seen[key] {
		n += c
	}
	return n
}

func (t *wireTap) requestBytes(key int16) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bytes[key]
}

// lines renders the tap as "Fetch v6 x214" rows, sorted by api key.
func (t *wireTap) lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]int, 0, len(t.seen))
	for k := range t.seen {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		vers := make([]int, 0, len(t.seen[int16(k)]))
		for v := range t.seen[int16(k)] {
			vers = append(vers, int(v))
		}
		sort.Ints(vers)
		parts := make([]string, 0, len(vers))
		for _, v := range vers {
			parts = append(parts, fmt.Sprintf("v%d x%d", v, t.seen[int16(k)][int16(v)]))
		}
		out = append(out, fmt.Sprintf("%-16s (key %2d) %s", apiName(int16(k)), k, strings.Join(parts, ", ")))
	}
	return out
}

func (t *wireTap) report() {
	for _, l := range t.lines() {
		fmt.Printf("  ---  %s\n", l)
	}
}

func apiName(k int16) string {
	names := map[int16]string{
		0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
		8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator",
		11: "JoinGroup", 12: "Heartbeat", 13: "LeaveGroup", 14: "SyncGroup",
		15: "DescribeGroups", 16: "ListGroups", 17: "SaslHandshake",
		18: "ApiVersions", 19: "CreateTopics", 20: "DeleteTopics",
		22: "InitProducerId", 24: "AddPartitionsToTxn", 25: "AddOffsetsToTxn",
		26: "EndTxn", 28: "TxnOffsetCommit", 29: "DescribeAcls", 30: "CreateAcls",
		31: "DeleteAcls", 32: "DescribeConfigs", 33: "AlterConfigs",
		36: "SaslAuthenticate", 37: "CreatePartitions", 42: "DeleteGroups",
		44: "IncrementalAlterConfigs", 47: "OffsetDelete", 60: "DescribeCluster",
	}
	if n, ok := names[k]; ok {
		return n
	}
	return fmt.Sprintf("api%d", k)
}

type tapDialer struct {
	inner *net.Dialer
	tap   *wireTap
	sniff bool // false on a TLS listener: the bytes would be ciphertext
}

func (d *tapDialer) Dial(network, addr string) (net.Conn, error) {
	c, err := d.inner.Dial(network, addr)
	if err != nil || !d.sniff {
		return c, err
	}
	return &tapConn{Conn: c, tap: d.tap}, nil
}

type tapConn struct {
	net.Conn
	tap *wireTap
	mu  sync.Mutex
	buf []byte
}

func (c *tapConn) Write(p []byte) (int, error) {
	c.feed(p)
	return c.Conn.Write(p)
}

// feed accumulates the outbound byte stream and pulls complete request frames
// out of it. sarama writes one frame per Write today, but framing on a byte
// stream is the only assumption that stays true.
func (c *tapConn) feed(p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, p...)
	for {
		if len(c.buf) < 4 {
			return
		}
		n := int(binary.BigEndian.Uint32(c.buf[:4]))
		if n < 4 || len(c.buf) < 4+n {
			return
		}
		frame := c.buf[4 : 4+n]
		c.tap.note(
			int16(binary.BigEndian.Uint16(frame[0:2])),
			int16(binary.BigEndian.Uint16(frame[2:4])),
			4+n,
		)
		c.buf = c.buf[4+n:]
	}
}

// ------------------------------------------------------------ sarama's logger
//
// sarama logs into two package-level loggers. Captured rather than silenced:
// the decisive line in a failure ("client/metadata got error from broker ...",
// "kafka: broker did not respond") is in there and nowhere else.

type logSink struct {
	mu    sync.Mutex
	lines []string
}

var saramaLog = &logSink{}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.lines = append(s.lines, strings.TrimRight(string(p), "\n"))
	s.mu.Unlock()
	if os.Getenv("SARAMA_VERBOSE") != "" {
		fmt.Printf("  [sarama] %s", p)
	}
	return len(p), nil
}

func (s *logSink) mark() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lines)
}

// since returns the log lines written after mark(), matching any of substrs
// (all of them when substrs is empty), newest last, capped at n.
func (s *logSink) since(from int, n int, substrs ...string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, l := range s.lines[min(from, len(s.lines)):] {
		if len(substrs) > 0 {
			hit := false
			for _, sub := range substrs {
				if strings.Contains(l, sub) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		out = append(out, l)
	}
	if len(out) > n {
		out = out[len(out)-n:]
	}
	return out
}

func init() {
	sarama.Logger = log.New(saramaLog, "", 0)
	sarama.DebugLogger = log.New(saramaLog, "", 0)
}

// ------------------------------------------------------------ config builders

type cfgOpts struct {
	clientID       string
	version        sarama.KafkaVersion
	apiVersionsReq bool // Config.ApiVersionsRequest; false is the trap the suite pins
	tap            *wireTap
	tlsConf        *tls.Config
	saslUser       string
	saslPassword   string
	saslV0         bool
}

func newConfig(o cfgOpts) *sarama.Config {
	c := sarama.NewConfig()
	c.ClientID = o.clientID
	if o.version != (sarama.KafkaVersion{}) {
		c.Version = o.version
	}
	c.ApiVersionsRequest = o.apiVersionsReq

	// Idempotence WORKS since M7 F3 (InitProducerId key 22, advertised v0..=4).
	// It stays off in every scenario but `edges`, which turns it on and asserts
	// the send lands, so the rest of the suite keeps measuring the
	// non-idempotent path. sarama's default is already false; stated for the
	// record so a future default flip cannot move the suite silently.
	c.Producer.Idempotent = false
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	// Strict per-partition order without an idempotent producer is
	// max.in.flight=1 on any broker, Kafka included. Not a facade requirement.
	c.Net.MaxOpenRequests = 1

	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.MaxWaitTime = 250 * time.Millisecond
	c.Consumer.Group.Session.Timeout = 10 * time.Second   // inside the facade's 6s..300s window
	c.Consumer.Group.Heartbeat.Interval = 2 * time.Second // must divide the session timeout
	c.Consumer.Group.Rebalance.Timeout = 60 * time.Second

	c.Metadata.AllowAutoTopicCreation = true
	c.Metadata.Retry.Max = 5
	c.Metadata.Retry.Backoff = 300 * time.Millisecond
	c.Metadata.RefreshFrequency = 0 // no background refresh: keep the tap legible

	c.Net.DialTimeout = 10 * time.Second
	c.Net.ReadTimeout = 30 * time.Second
	c.Net.WriteTimeout = 30 * time.Second

	if o.tlsConf != nil {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = o.tlsConf
	}
	if o.saslPassword != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		c.Net.SASL.User = o.saslUser
		c.Net.SASL.Password = o.saslPassword
		// sarama's DEFAULT is SASLHandshakeV0, and its own Validate() refuses to
		// combine that with ApiVersionsRequest — so a sarama user reaching for
		// SASL is pushed to choose. V1 is the choice that keeps the handshake,
		// and the handshake is what clamps the versions (see the file header).
		if o.saslV0 {
			c.Net.SASL.Version = sarama.SASLHandshakeV0
		} else {
			c.Net.SASL.Version = sarama.SASLHandshakeV1
		}
	}

	tap := o.tap
	if tap == nil {
		tap = newTap()
	}
	c.Net.Proxy.Enable = true
	c.Net.Proxy.Dialer = &tapDialer{
		inner: &net.Dialer{Timeout: c.Net.DialTimeout, KeepAlive: c.Net.KeepAlive},
		tap:   tap,
		sniff: o.tlsConf == nil,
	}
	return c
}

// certPool builds a pool from a PEM file, for the --m5 listener's self-signed
// certificate. Returns nil when the path is empty or unreadable, and the caller
// then falls back to InsecureSkipVerify with a note.
func certPool(path string) *x509.CertPool {
	if path == "" {
		return nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(pem) {
		return nil
	}
	return p
}

func saramaModuleVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, d := range bi.Deps {
		if d.Path == "github.com/IBM/sarama" {
			return d.Version
		}
	}
	return "unknown (not in build info)"
}

// clampAvailable reports whether the sarama this binary was built against has
// restrictApiVersion — that is, whether it clamps its request versions to what
// the broker advertised. It landed in v1.46.0; every release below sends what
// Config.Version names and needs Config.Version <= 1.0.0 against this facade.
// Checked by version rather than by reflection because the function is
// unexported, and the boundary is the thing worth stating anyway.
func clampAvailable() bool {
	v := saramaModuleVersion()
	var major, minor int
	if _, err := fmt.Sscanf(strings.TrimPrefix(v, "v"), "%d.%d", &major, &minor); err != nil {
		return false
	}
	return major > 1 || (major == 1 && minor >= 46)
}
