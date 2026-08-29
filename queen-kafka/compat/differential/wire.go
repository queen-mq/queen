package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ---------------------------------------------------------------- connections

// conn is a raw Kafka connection: one TCP socket, requests written at exactly
// the version asked for and responses handed back undecoded as well as
// decoded. franz-go's kgo.Client is deliberately NOT used to carry requests —
// it rewrites the version of everything it sends to the highest the broker
// advertises, which would make "ask at v4", "ask at an absurd version" and the
// whole version-boundary half of this suite untestable.
type conn struct {
	label string
	addr  string
	c     net.Conn
	corr  int32
	f     *kmsg.RequestFormatter
}

var connSeq atomic.Int64

func dial(label, addr string) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s (%s): %w", label, addr, err)
	}
	n := connSeq.Add(1)
	return &conn{
		label: label,
		addr:  addr,
		c:     c,
		f:     kmsg.NewRequestFormatter(kmsg.FormatterClientID(fmt.Sprintf("qk-diff-%d", n))),
	}, nil
}

func (k *conn) Close() {
	if k.c != nil {
		_ = k.c.Close()
	}
}

// send writes a request at the given version and returns without reading.
// Split from recv so a scenario can hold a request open — a follower's
// SyncGroup that must not be answered before the leader syncs is only
// observable if the runner can look at the socket while it is still pending.
func (k *conn) send(req kmsg.Request, version int16) error {
	req.SetVersion(version)
	k.corr++
	buf := k.f.AppendRequest(nil, req, k.corr)
	if err := k.c.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	if _, err := k.c.Write(buf); err != nil {
		return fmt.Errorf("write %T v%d to %s: %w", req, version, k.label, err)
	}
	return nil
}

// recvFrame reads one response frame and returns its correlation id and the
// bytes after it.
func (k *conn) recvFrame(timeout time.Duration) (int32, []byte, error) {
	if err := k.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return 0, nil, err
	}
	var sz [4]byte
	if _, err := io.ReadFull(k.c, sz[:]); err != nil {
		return 0, nil, fmt.Errorf("read size from %s: %w", k.label, err)
	}
	n := int32(binary.BigEndian.Uint32(sz[:]))
	if n < 4 || n > 100<<20 {
		return 0, nil, fmt.Errorf("%s answered a %d byte frame", k.label, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(k.c, body); err != nil {
		return 0, nil, fmt.Errorf("read body from %s: %w", k.label, err)
	}
	return int32(binary.BigEndian.Uint32(body[:4])), body[4:], nil
}

// recvRaw reads one response frame and returns the body with the response
// header stripped.
func (k *conn) recvRaw(flexibleHeader bool, timeout time.Duration) ([]byte, error) {
	_, body, err := k.recvFrame(timeout)
	if err != nil {
		return nil, err
	}
	if flexibleHeader {
		rest, err := skipTags(body)
		if err != nil {
			return nil, fmt.Errorf("%s response header tags: %w", k.label, err)
		}
		body = rest
	}
	return body, nil
}

// recv reads the response to the request last sent on this connection.
func (k *conn) recv(req kmsg.Request, timeout time.Duration) (kmsg.Response, []byte, error) {
	resp := req.ResponseKind()
	resp.SetVersion(req.GetVersion())
	// ApiVersions is the one API whose response header is v0 even when the
	// body is flexible: a client has to be able to read it before it knows
	// anything about the broker.
	flexHeader := resp.IsFlexible() && resp.Key() != 18
	body, err := k.recvRaw(flexHeader, timeout)
	if err != nil {
		return nil, nil, err
	}
	if err := resp.ReadFrom(body); err != nil {
		return nil, body, fmt.Errorf("decode %T v%d from %s: %w", resp, req.GetVersion(), k.label, err)
	}
	return resp, body, nil
}

func (k *conn) do(req kmsg.Request, version int16) (kmsg.Response, []byte, error) {
	if err := k.send(req, version); err != nil {
		return nil, nil, err
	}
	return k.recv(req, 20*time.Second)
}

func (k *conn) doT(req kmsg.Request, version int16, timeout time.Duration) (kmsg.Response, []byte, error) {
	if err := k.send(req, version); err != nil {
		return nil, nil, err
	}
	return k.recv(req, timeout)
}

// pending is a request in flight on its own connection.
type pending struct {
	k   *conn
	req kmsg.Request
}

func (k *conn) begin(req kmsg.Request, version int16) (*pending, error) {
	if err := k.send(req, version); err != nil {
		return nil, err
	}
	return &pending{k: k, req: req}, nil
}

// answered reports whether a response is already sitting on the socket, without
// consuming it and without blocking for longer than d.
func (p *pending) answered(d time.Duration) (bool, error) {
	if err := p.k.c.SetReadDeadline(time.Now().Add(d)); err != nil {
		return false, err
	}
	one := make([]byte, 1)
	n, err := p.k.c.Read(one)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return false, nil
		}
		return false, err
	}
	if n == 1 {
		// Put the byte back by stitching it in front of the socket.
		p.k.c = &prefixConn{Conn: p.k.c, pre: one}
		return true, nil
	}
	return false, nil
}

func (p *pending) wait(timeout time.Duration) (kmsg.Response, []byte, error) {
	return p.k.recv(p.req, timeout)
}

// prefixConn hands back bytes that answered() had to consume to see that a
// response had arrived.
type prefixConn struct {
	net.Conn
	pre []byte
}

func (p *prefixConn) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

// ------------------------------------------------------------------- varints

func appendUvarint(dst []byte, u uint64) []byte { return binary.AppendUvarint(dst, u) }

func appendVarint(dst []byte, i int32) []byte {
	return appendUvarint(dst, uint64(uint32(i<<1^i>>31)))
}

func appendVarlong(dst []byte, i int64) []byte {
	return appendUvarint(dst, uint64(i<<1^i>>63))
}

func appendVarintBytes(dst []byte, b []byte, null bool) []byte {
	if null {
		return appendVarint(dst, -1)
	}
	dst = appendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

type reader struct {
	src []byte
	err error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.src) < n {
		r.fail("wanted %d bytes, %d left", n, len(r.src))
		return nil
	}
	b := r.src[:n]
	r.src = r.src[n:]
	return b
}

func (r *reader) i8() int8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return int8(b[0])
}
func (r *reader) i16() int16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}
func (r *reader) i32() int32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}
func (r *reader) i64() int64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	u, n := binary.Uvarint(r.src)
	if n <= 0 {
		r.fail("malformed uvarint")
		return 0
	}
	r.src = r.src[n:]
	return u
}

func (r *reader) varint() int32  { u := r.uvarint(); return int32(uint32(u>>1) ^ uint32(-int32(u&1))) }
func (r *reader) varlong() int64 { u := r.uvarint(); return int64(u>>1) ^ -int64(u&1) }

func (r *reader) varintBytes() ([]byte, bool) {
	n := r.varint()
	if n < 0 {
		return nil, true
	}
	return r.take(int(n)), false
}

// skipTags consumes a flexible-version tag buffer from the front of b.
func skipTags(b []byte) ([]byte, error) {
	r := &reader{src: b}
	n := r.uvarint()
	for i := uint64(0); i < n; i++ {
		r.uvarint() // tag key
		size := r.uvarint()
		r.take(int(size))
	}
	return r.src, r.err
}

// -------------------------------------------------------------- record batches

type header struct {
	key     string
	val     []byte
	valNull bool
}

type record struct {
	key     []byte
	keyNull bool
	val     []byte
	valNull bool
	ts      int64
	headers []header
}

type parsedBatch struct {
	firstOffset     int64
	leaderEpoch     int32
	magic           int8
	crcOK           bool
	attributes      int16
	codec           int8
	lastOffsetDelta int32
	firstTS         int64
	maxTS           int64
	producerID      int64
	producerEpoch   int16
	firstSequence   int32
	numRecords      int32
	records         []record
	// offsets of the decoded records, firstOffset + offsetDelta
	offsets []int64
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	codecNone int8 = 0
	codecGzip int8 = 1
	codecLZ4  int8 = 3
	codecZstd int8 = 4
)

func codecName(c int8) string {
	switch c {
	case codecNone:
		return "none"
	case codecGzip:
		return "gzip"
	case 2:
		return "snappy"
	case codecLZ4:
		return "lz4"
	case codecZstd:
		return "zstd"
	}
	return fmt.Sprintf("codec(%d)", c)
}

func compressBlock(codec int8, raw []byte) ([]byte, error) {
	switch codec {
	case codecNone:
		return raw, nil
	case codecGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(raw); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case codecZstd:
		w, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
		out := w.EncodeAll(raw, nil)
		_ = w.Close()
		return out, nil
	}
	return nil, fmt.Errorf("this runner does not compress %s", codecName(codec))
}

func decompressBlock(codec int8, raw []byte) ([]byte, error) {
	switch codec {
	case codecNone:
		return raw, nil
	case codecGzip:
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	case codecZstd:
		zr, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return zr.DecodeAll(raw, nil)
	}
	return nil, fmt.Errorf("this runner does not decompress %s", codecName(codec))
}

// buildBatch encodes a v2 RecordBatch by hand: the length and the CRC-32C are
// patched in afterwards, which is what a producer client does.
func buildBatch(recs []record, codec int8) ([]byte, error) {
	if len(recs) == 0 {
		return nil, fmt.Errorf("an empty batch")
	}
	firstTS := recs[0].ts
	maxTS := recs[0].ts
	var body []byte
	for i, rc := range recs {
		if rc.ts > maxTS {
			maxTS = rc.ts
		}
		var one []byte
		one = append(one, 0) // record attributes
		one = appendVarlong(one, rc.ts-firstTS)
		one = appendVarint(one, int32(i))
		one = appendVarintBytes(one, rc.key, rc.keyNull)
		one = appendVarintBytes(one, rc.val, rc.valNull)
		one = appendVarint(one, int32(len(rc.headers)))
		for _, h := range rc.headers {
			one = appendVarintBytes(one, []byte(h.key), false)
			one = appendVarintBytes(one, h.val, h.valNull)
		}
		body = appendVarint(body, int32(len(one)))
		body = append(body, one...)
	}
	block, err := compressBlock(codec, body)
	if err != nil {
		return nil, err
	}

	var b []byte
	b = binary.BigEndian.AppendUint64(b, 0)          // first offset
	b = binary.BigEndian.AppendUint32(b, 0)          // length, patched below
	b = binary.BigEndian.AppendUint32(b, ^uint32(0)) // partition leader epoch -1
	b = append(b, 2)                                 // magic
	b = binary.BigEndian.AppendUint32(b, 0)          // crc, patched below
	b = binary.BigEndian.AppendUint16(b, uint16(codec))
	b = binary.BigEndian.AppendUint32(b, uint32(int32(len(recs)-1)))
	b = binary.BigEndian.AppendUint64(b, uint64(firstTS))
	b = binary.BigEndian.AppendUint64(b, uint64(maxTS))
	b = binary.BigEndian.AppendUint64(b, ^uint64(0)) // producer id -1
	b = binary.BigEndian.AppendUint16(b, ^uint16(0)) // producer epoch -1
	b = binary.BigEndian.AppendUint32(b, ^uint32(0)) // first sequence -1
	b = binary.BigEndian.AppendUint32(b, uint32(len(recs)))
	b = append(b, block...)

	binary.BigEndian.PutUint32(b[8:12], uint32(len(b)-12))
	binary.BigEndian.PutUint32(b[17:21], crc32.Checksum(b[21:], crcTable))
	return b, nil
}

// parseBatches walks the concatenated record batches a fetch response carries.
// A truncated trailing batch (the broker filled maxBytes mid-batch) is normal
// and is reported rather than treated as corruption.
func parseBatches(raw []byte) ([]parsedBatch, bool, error) {
	var out []parsedBatch
	truncated := false
	for len(raw) > 0 {
		if len(raw) < 12 {
			truncated = true
			break
		}
		length := int32(binary.BigEndian.Uint32(raw[8:12]))
		total := 12 + int(length)
		if length < 0 || total > len(raw) {
			truncated = true
			break
		}
		chunk := raw[:total]
		raw = raw[total:]

		b := parsedBatch{}
		r := &reader{src: chunk}
		b.firstOffset = r.i64()
		r.i32() // length
		b.leaderEpoch = r.i32()
		b.magic = r.i8()
		gotCRC := uint32(r.i32())
		b.crcOK = crc32.Checksum(chunk[21:], crcTable) == gotCRC
		b.attributes = r.i16()
		b.codec = int8(b.attributes & 0x7)
		b.lastOffsetDelta = r.i32()
		b.firstTS = r.i64()
		b.maxTS = r.i64()
		b.producerID = r.i64()
		b.producerEpoch = r.i16()
		b.firstSequence = r.i32()
		b.numRecords = r.i32()
		if r.err != nil {
			return out, truncated, r.err
		}
		if b.magic != 2 {
			return out, truncated, fmt.Errorf("a magic %d batch: this runner only reads v2", b.magic)
		}
		block, err := decompressBlock(b.codec, r.src)
		if err != nil {
			return out, truncated, err
		}
		rr := &reader{src: block}
		for i := int32(0); i < b.numRecords && rr.err == nil && len(rr.src) > 0; i++ {
			size := rr.varint()
			one := rr.take(int(size))
			if rr.err != nil {
				break
			}
			r2 := &reader{src: one}
			var rc record
			r2.i8() // record attributes
			delta := r2.varlong()
			offDelta := r2.varint()
			rc.ts = b.firstTS + delta
			rc.key, rc.keyNull = r2.varintBytes()
			rc.val, rc.valNull = r2.varintBytes()
			nh := r2.varint()
			for h := int32(0); h < nh && r2.err == nil; h++ {
				hk, _ := r2.varintBytes()
				hv, hnull := r2.varintBytes()
				var v []byte
				if !hnull {
					v = append([]byte{}, hv...)
				}
				rc.headers = append(rc.headers, header{key: string(hk), val: v, valNull: hnull})
			}
			if r2.err != nil {
				return out, truncated, fmt.Errorf("record %d: %w", i, r2.err)
			}
			b.records = append(b.records, rc)
			b.offsets = append(b.offsets, b.firstOffset+int64(offDelta))
		}
		if rr.err != nil {
			return out, truncated, rr.err
		}
		out = append(out, b)
	}
	return out, truncated, nil
}

// ------------------------------------------------------------------ err names

var errNames = map[int16]string{
	0: "NONE", 1: "OFFSET_OUT_OF_RANGE", 2: "CORRUPT_MESSAGE",
	3: "UNKNOWN_TOPIC_OR_PARTITION", 4: "INVALID_FETCH_SIZE",
	5: "LEADER_NOT_AVAILABLE", 6: "NOT_LEADER_OR_FOLLOWER",
	7: "REQUEST_TIMED_OUT", 8: "BROKER_NOT_AVAILABLE",
	9: "REPLICA_NOT_AVAILABLE", 10: "MESSAGE_TOO_LARGE",
	12: "OFFSET_METADATA_TOO_LARGE", 13: "NETWORK_EXCEPTION",
	14: "COORDINATOR_LOAD_IN_PROGRESS", 15: "COORDINATOR_NOT_AVAILABLE",
	16: "NOT_COORDINATOR", 17: "INVALID_TOPIC_EXCEPTION",
	18: "RECORD_LIST_TOO_LARGE", 21: "INVALID_REQUIRED_ACKS",
	22: "ILLEGAL_GENERATION", 23: "INCONSISTENT_GROUP_PROTOCOL",
	24: "INVALID_GROUP_ID", 25: "UNKNOWN_MEMBER_ID",
	26: "INVALID_SESSION_TIMEOUT", 27: "REBALANCE_IN_PROGRESS",
	28: "INVALID_COMMIT_OFFSET_SIZE", 29: "TOPIC_AUTHORIZATION_FAILED",
	30: "GROUP_AUTHORIZATION_FAILED", 32: "INVALID_TIMESTAMP",
	33: "UNSUPPORTED_SASL_MECHANISM", 34: "ILLEGAL_SASL_STATE",
	35: "UNSUPPORTED_VERSION", 36: "TOPIC_ALREADY_EXISTS",
	37: "INVALID_PARTITIONS", 42: "INVALID_REQUEST",
	43: "UNSUPPORTED_FOR_MESSAGE_FORMAT", 45: "OUT_OF_ORDER_SEQUENCE_NUMBER",
	47: "INVALID_PRODUCER_EPOCH", 56: "KAFKA_STORAGE_ERROR",
	58: "SASL_AUTHENTICATION_FAILED", 59: "UNKNOWN_PRODUCER_ID",
	74: "FENCED_LEADER_EPOCH", 75: "UNKNOWN_LEADER_EPOCH",
	76: "UNSUPPORTED_COMPRESSION_TYPE", 78: "OFFSET_NOT_AVAILABLE",
	79: "MEMBER_ID_REQUIRED", 81: "GROUP_MAX_SIZE_REACHED",
	82: "FENCED_INSTANCE_ID", 87: "UNKNOWN_TOPIC_ID",
	// M7 F2's two: the pair DeleteGroups answers with, and the reason both
	// brokers agree on them is that they are Kafka's own rule (a group with
	// members is not deletable) and Kafka's own answer for a name it has never
	// seen.
	39: "INVALID_REPLICA_ASSIGNMENT", 40: "INVALID_CONFIG",
	68: "NON_EMPTY_GROUP", 69: "GROUP_ID_NOT_FOUND",
	89: "THROTTLING_QUOTA_EXCEEDED",
}

func errName(code int16) string {
	if n, ok := errNames[code]; ok {
		return fmt.Sprintf("%d/%s", code, n)
	}
	return fmt.Sprintf("%d/UNKNOWN_ERROR_NAME", code)
}
