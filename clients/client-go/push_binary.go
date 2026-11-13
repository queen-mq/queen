package queen

// The binary push body: an alternative to the JSON body of POST /api/v1/push.
//
//	"QPB1" | u8 flags | [u32 raw_len, when zstd] | body
//	body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
//	       | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
//
// All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
// bytes, 2 = answer with the counts only, 4 = every item carries its
// transaction id (empty = the broker mints it). No trace id. Each payload is
// one JSON document.
//
// With flag 8 (EncodePushFrames) a group is the messages as the broker stores
// them, compressed here once:
//
//	body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
//	z:     one zstd frame of n frames: u32 body_len | u8 flags | u8[16] message id
//	       | u16 len, transactionId | [u16 len, headers block, flag 8] | payload
//
// The message id is a version-7 UUID minted by the client; a message without
// its own transaction id uses the id's text. A frame's flags are 0, or 8 when
// the message carries headers (a cluster at catalogue version 8): the block is
// `u16 count | (u8 len, name | u16 len, value) x count`, sorted by name. The
// broker checks every frame and keeps the compressed bytes as they are, so it
// does not compress them again. The body without flag 8 has no room for
// headers: a push whose messages carry some goes out framed, or as JSON.
//
// A broker that does not know this body answers 400; a client sends it only
// when ClientConfig.BinaryPush is set, so by default a push is the JSON
// request every broker knows.
//
// The binary transaction body (EncodeTransactionFrames), an alternative to the
// JSON body of POST /api/v1/transaction, carries the pushes the same way:
//
//	"QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above
//
// head is the JSON transaction body without its push operations: the acks in
// "operations", "requiredLeases", "kv", "timers". flags: 2 = the answer leaves
// out the pushes' and the acks' results and counts them ("pushed",
// "duplicate", "acked"); the riders' results stay. In the answer the head's
// operations come first and the groups' messages after them, in the order
// written; the riders follow, as in the JSON body.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

const (
	txnBinaryMagic        = "QTB1"
	txnBinaryShort   byte = 2
	pushBinaryMagic       = "QPB1"
	pushBinaryZstd   byte = 1
	pushBinaryShort  byte = 2
	pushBinaryTxn    byte = 4
	pushBinaryFrames byte = 8
)

// rawRequestBody is a request body sent as these bytes, not JSON-encoded.
type rawRequestBody []byte

// PushBatch is one partition's messages for EncodePushBinary.
type PushBatch struct {
	Queue     string
	Partition string
	Payloads  [][]byte // each one JSON document
	// TransactionIDs is nil, or one transaction id per payload ("" = the broker mints it).
	TransactionIDs []string
	// Headers is nil, or one map per payload (nil or empty: none). Only the
	// framed bodies carry headers (EncodePushFrames, EncodeTransactionFrames);
	// EncodePushBinary refuses a batch that has some.
	Headers []map[string]string
}

// hasHeaders reports whether any message of the batch carries headers.
func (b *PushBatch) hasHeaders() bool {
	for _, h := range b.Headers {
		if len(h) > 0 {
			return true
		}
	}
	return false
}

// addHeaders records the headers of the payload just appended. Headers stays
// nil while no message of the batch has any, so a batch without headers is the
// one it was before they existed.
func (b *PushBatch) addHeaders(h map[string]string) {
	if b.Headers == nil {
		if len(h) == 0 {
			return
		}
		b.Headers = make([]map[string]string, len(b.Payloads)-1, len(b.Payloads))
	}
	b.Headers = append(b.Headers, h)
}

// PushCounts is the short answer of a binary push.
type PushCounts struct {
	Count     int `json:"count"`
	Queued    int `json:"queued"`
	Duplicate int `json:"duplicate"`
	Error     int `json:"error"`
}

var (
	pushZstdOnce sync.Once
	pushZstdEnc  *zstd.Encoder
)

func pushZstdEncoder() *zstd.Encoder {
	pushZstdOnce.Do(func() {
		// Single segment: every frame declares its content size. Left to itself
		// EncodeAll writes none for under 256 bytes, and the broker refuses a
		// group whose frame does not declare it.
		pushZstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithSingleSegment(true))
	})
	return pushZstdEnc
}

// EncodePushBinary encodes batches as a binary push body. compress: the body is
// zstd-compressed. short: the broker answers with PushCounts instead of one
// object per message. It has no room for headers: a batch with some is refused
// (EncodePushFrames carries them).
func EncodePushBinary(batches []PushBatch, compress, short bool) ([]byte, error) {
	size := 4
	withTxn := false
	for i := range batches {
		b := &batches[i]
		if len(b.Queue) > 0xffff || len(b.Partition) > 0xffff {
			return nil, fmt.Errorf("queue or partition name longer than 65535 bytes")
		}
		if b.hasHeaders() {
			return nil, fmt.Errorf("the binary push body without frames carries no headers: use EncodePushFrames")
		}
		if b.TransactionIDs != nil {
			if len(b.TransactionIDs) != len(b.Payloads) {
				return nil, fmt.Errorf("%d transaction ids for %d payloads", len(b.TransactionIDs), len(b.Payloads))
			}
			withTxn = true
			for _, t := range b.TransactionIDs {
				if len(t) > 0xffff {
					return nil, fmt.Errorf("transaction id longer than 65535 bytes")
				}
				size += len(t)
			}
		}
		size += 8 + len(b.Queue) + len(b.Partition) + 2*len(b.Payloads)
		for _, p := range b.Payloads {
			size += 4 + len(p)
		}
	}
	var flags byte
	if short {
		flags |= pushBinaryShort
	}
	if withTxn {
		flags |= pushBinaryTxn
	}
	head := len(pushBinaryMagic) + 1
	body := make([]byte, head, head+size)
	copy(body, pushBinaryMagic)
	body[head-1] = flags
	body = binary.LittleEndian.AppendUint32(body, uint32(len(batches)))
	for i := range batches {
		b := &batches[i]
		body = binary.LittleEndian.AppendUint16(body, uint16(len(b.Queue)))
		body = append(body, b.Queue...)
		body = binary.LittleEndian.AppendUint16(body, uint16(len(b.Partition)))
		body = append(body, b.Partition...)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(b.Payloads)))
		for j, p := range b.Payloads {
			if withTxn {
				txn := ""
				if b.TransactionIDs != nil {
					txn = b.TransactionIDs[j]
				}
				body = binary.LittleEndian.AppendUint16(body, uint16(len(txn)))
				body = append(body, txn...)
			}
			body = binary.LittleEndian.AppendUint32(body, uint32(len(p)))
			body = append(body, p...)
		}
	}
	if !compress {
		return body, nil
	}
	raw := body[head:]
	out := make([]byte, 0, head+4+len(raw)/2)
	out = append(out, pushBinaryMagic...)
	out = append(out, flags|pushBinaryZstd)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(raw)))
	return pushZstdEncoder().EncodeAll(raw, out), nil
}

// EncodePushFrames encodes batches as a binary push body whose groups are the
// messages framed as the broker stores them and compressed once, here. short:
// the broker answers with PushCounts instead of one object per message.
func EncodePushFrames(batches []PushBatch, short bool) ([]byte, error) {
	flags := pushBinaryFrames
	if short {
		flags |= pushBinaryShort
	}
	out := make([]byte, 0, 1024)
	out = append(out, pushBinaryMagic...)
	out = append(out, flags)
	return appendFrameGroups(out, batches)
}

// EncodeTransactionFrames encodes a binary transaction body: head, the JSON
// transaction body without its push operations, then batches as groups of
// messages framed as the broker stores them and compressed once, here. short:
// the broker counts the pushes and the acks in its answer instead of rendering
// one result for each.
func EncodeTransactionFrames(head []byte, batches []PushBatch, short bool) ([]byte, error) {
	var flags byte
	if short {
		flags |= txnBinaryShort
	}
	out := make([]byte, 0, 1024+len(head))
	out = append(out, txnBinaryMagic...)
	out = append(out, flags)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(head)))
	out = append(out, head...)
	return appendFrameGroups(out, batches)
}

// appendFrameGroups appends `u32 groups | per group: u16 len, queue | u16 len,
// partition | u32 n | u32 z_len | z` to out: each batch's messages framed as
// the broker stores them, with a version-7 id minted here, as one zstd frame.
// A message with headers gets flag 8 and its block after the transaction id.
func appendFrameGroups(out []byte, batches []PushBatch) ([]byte, error) {
	out = binary.LittleEndian.AppendUint32(out, uint32(len(batches)))
	var raw []byte
	var text [36]byte
	for i := range batches {
		b := &batches[i]
		if len(b.Queue) > 0xffff || len(b.Partition) > 0xffff {
			return nil, fmt.Errorf("queue or partition name longer than 65535 bytes")
		}
		if b.TransactionIDs != nil && len(b.TransactionIDs) != len(b.Payloads) {
			return nil, fmt.Errorf("%d transaction ids for %d payloads", len(b.TransactionIDs), len(b.Payloads))
		}
		if b.Headers != nil && len(b.Headers) != len(b.Payloads) {
			return nil, fmt.Errorf("%d header maps for %d payloads", len(b.Headers), len(b.Payloads))
		}
		size := 0
		for _, p := range b.Payloads {
			size += 4 + 1 + 16 + 2 + 36 + len(p)
		}
		raw = raw[:0]
		if cap(raw) < size {
			raw = make([]byte, 0, size)
		}
		for j, p := range b.Payloads {
			id, err := uuid.NewV7()
			if err != nil {
				return nil, err
			}
			var txn []byte
			if b.TransactionIDs != nil && b.TransactionIDs[j] != "" {
				if len(b.TransactionIDs[j]) > 0xffff {
					return nil, fmt.Errorf("transaction id longer than 65535 bytes")
				}
				txn = []byte(b.TransactionIDs[j])
			} else {
				encodeUUIDText(text[:], id)
				txn = text[:]
			}
			var block []byte
			if b.Headers != nil {
				var ok bool
				if block, ok = headerBlock(b.Headers[j]); !ok {
					return nil, fmt.Errorf("headers that a frame cannot carry (a name over 255 bytes, a value or the block over 65535, or not UTF-8)")
				}
			}
			var flags byte
			bodyLen := 1 + 16 + 2 + len(txn) + len(p)
			if block != nil {
				flags = frameFlagHeaders
				bodyLen += 2 + len(block)
			}
			raw = binary.LittleEndian.AppendUint32(raw, uint32(bodyLen))
			raw = append(raw, flags)
			raw = append(raw, id[:]...)
			raw = binary.LittleEndian.AppendUint16(raw, uint16(len(txn)))
			raw = append(raw, txn...)
			if block != nil {
				raw = binary.LittleEndian.AppendUint16(raw, uint16(len(block)))
				raw = append(raw, block...)
			}
			raw = append(raw, p...)
		}
		out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Queue)))
		out = append(out, b.Queue...)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Partition)))
		out = append(out, b.Partition...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Payloads)))
		at := len(out)
		out = append(out, 0, 0, 0, 0)
		out = pushZstdEncoder().EncodeAll(raw, out)
		binary.LittleEndian.PutUint32(out[at:], uint32(len(out)-at-4))
	}
	return out, nil
}

// encodeUUIDText writes the 36-byte lower-case hyphenated text of id into dst.
func encodeUUIDText(dst []byte, id uuid.UUID) {
	const hex = "0123456789abcdef"
	p := 0
	for i, c := range id {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			dst[p] = '-'
			p++
		}
		dst[p] = hex[c>>4]
		dst[p+1] = hex[c&0x0f]
		p += 2
	}
}

// ParsePushCounts decodes the short answer of a binary push.
func ParsePushCounts(b []byte) (PushCounts, error) {
	var c PushCounts
	err := json.Unmarshal(b, &c)
	return c, err
}

// binaryPushBody is items as a binary push body, when every item can travel
// in one: no trace id. Consecutive items of one partition share a group, so
// the order of the items is the order on the wire.
func binaryPushBody(items []PushItem, compress bool) ([]byte, bool) {
	var batches []PushBatch
	for i := range items {
		it := &items[i]
		if it.TraceID != "" {
			return nil, false
		}
		// Headers travel only in frames: the plain binary body has no room
		// for them, and the JSON body carries them.
		if len(it.Headers) > 0 && !compress {
			return nil, false
		}
		payload, err := json.Marshal(it.Payload)
		if err != nil {
			return nil, false
		}
		n := len(batches)
		if n == 0 || batches[n-1].Queue != it.Queue || batches[n-1].Partition != it.Partition {
			batches = append(batches, PushBatch{Queue: it.Queue, Partition: it.Partition, TransactionIDs: []string{}})
			n++
		}
		batches[n-1].Payloads = append(batches[n-1].Payloads, payload)
		batches[n-1].TransactionIDs = append(batches[n-1].TransactionIDs, it.TransactionID)
		batches[n-1].addHeaders(it.Headers)
	}
	if compress {
		// Compressed = the messages framed and compressed here, kept by the broker as they are.
		body, err := EncodePushFrames(batches, false)
		return body, err == nil
	}
	body, err := EncodePushBinary(batches, false, false)
	return body, err == nil
}

// transactionFrames is a transaction as a binary body, when its pushes can
// travel in one: there is at least one, and none carries a trace id. The acks
// and the links stay in the JSON head; consecutive pushed items of one
// partition share a group, so the pushes keep their order.
//
// The broker writes the head's operations before the groups' messages, so a
// link that comes after a push into the same partition would land before it:
// such a transaction goes as JSON, in the order the caller wrote it.
//
// order is nil when the broker's answer lists the operations as the caller
// added them (every ack before every push). Otherwise order[k] is the place,
// among the caller's operations, of the k-th result the broker sends: it lists
// the head's acks first, then the pushes.
func transactionFrames(ops []Operation, leases []string, kv []KVOp, timers []TimerOp) (body []byte, order []int, ok bool) {
	head := transactionRequest{Operations: []Operation{}, RequiredLeases: leases, KV: kv, Timers: timers}
	var batches []PushBatch
	var ackAt, pushAt []int
	pushed := map[[2]string]bool{}
	flat := 0
	for i := range ops {
		op := &ops[i]
		if op.Type != "push" {
			if op.Type == "link" && pushed[[2]string{op.Queue, op.Partition}] {
				return nil, nil, false
			}
			head.Operations = append(head.Operations, *op)
			ackAt = append(ackAt, flat)
			flat++
			continue
		}
		for j := range op.Items {
			it := &op.Items[j]
			if it.TraceID != "" {
				return nil, nil, false
			}
			payload, err := json.Marshal(it.Payload)
			if err != nil {
				return nil, nil, false
			}
			n := len(batches)
			if n == 0 || batches[n-1].Queue != it.Queue || batches[n-1].Partition != it.Partition {
				batches = append(batches, PushBatch{Queue: it.Queue, Partition: it.Partition, TransactionIDs: []string{}})
				n++
			}
			batches[n-1].Payloads = append(batches[n-1].Payloads, payload)
			batches[n-1].TransactionIDs = append(batches[n-1].TransactionIDs, it.TransactionID)
			batches[n-1].addHeaders(it.Headers)
			pushed[[2]string{it.Queue, it.Partition}] = true
			pushAt = append(pushAt, flat)
			flat++
		}
	}
	if len(batches) == 0 {
		return nil, nil, false
	}
	headJSON, err := json.Marshal(head)
	if err != nil {
		return nil, nil, false
	}
	body, err = EncodeTransactionFrames(headJSON, batches, false)
	if err != nil {
		return nil, nil, false
	}
	order = append(ackAt, pushAt...)
	inOrder := true
	for k, at := range order {
		if at != k {
			inOrder = false
			break
		}
	}
	if inOrder {
		order = nil
	}
	return body, order, true
}

// restoreResultOrder puts the results of a binary transaction back in the
// order the caller added its operations in, and gives each its own index
// again. The riders' results, after the operations', are already in place.
func restoreResultOrder(resp *TransactionResponse, order []int) {
	if resp == nil || order == nil || len(resp.Results) < len(order) {
		return
	}
	out := make([]json.RawMessage, len(resp.Results))
	copy(out[len(order):], resp.Results[len(order):])
	for k, at := range order {
		out[at] = resp.Results[k]
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(resp.Results[k], &fields); err != nil {
			continue
		}
		fields["index"] = json.RawMessage(fmt.Sprintf("%d", at))
		if b, err := json.Marshal(fields); err == nil {
			out[at] = b
		}
	}
	resp.Results = out
}
