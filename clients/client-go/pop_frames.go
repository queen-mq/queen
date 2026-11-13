package queen

// The framed pop answer: what a broker sends for a pop with `frames=true`.
// The messages come as the stored records, each still the zstd frame the
// producer (or the broker) made, and are unpacked here.
//
//	"QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
//	u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
//	per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
//	per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes
//
// All integers little-endian. A record's bytes are `count` frames:
//
//	u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
//	| u16 len, transactionId | [u16 len, producer, flag 2]
//	| [u16 len, headers block, flag 8] | payload
//
// Message i of a record has offset base_offset+i and belongs to the delivery
// when that lies in the claim's start..=end. The JSON object in front holds what
// the JSON answer has beside its messages: the conflation echo and the
// autopilot's choice.
//
// A broker answers JSON instead whenever a record is encrypted (flag 4), holds
// links (flag 16) or headers (flag 8); the headers block is read here all the
// same, so a broker that one day frames them is understood.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const popFramesMagic = "QPF1"

// The flags of a stored frame (the broker's frames.rs).
const (
	frameFlagTrace     byte = 1
	frameFlagProducer  byte = 2
	frameFlagEncrypted byte = 4
	frameFlagHeaders   byte = 8
	frameFlagLink      byte = 16
)

// PopFrame is one message of a framed pop answer. Payload is the message's
// JSON document; it points into the answer's buffer.
type PopFrame struct {
	ID              [16]byte
	TransactionID   string
	TraceID         []byte // 16 bytes, or nil
	ProducerSub     string
	Payload         []byte
	PartitionID     uint64
	Partition       string
	Offset          uint64
	CreatedAtMicros int64
	DeliveryAttempt uint32
	// Headers are the message's headers (frame flag 8), nil when it has none.
	Headers map[string]string
}

// IDString is the message id as lower-case hyphenated text.
func (f *PopFrame) IDString() string {
	var b [36]byte
	encodeUUIDText(b[:], f.ID)
	return string(b[:])
}

// PopFrames is a decoded framed pop answer.
type PopFrames struct {
	Queue         string
	LeaseID       string
	ConsumerGroup string
	Messages      []PopFrame
	// Extra is the answer's own keys as one JSON object (the conflation echo,
	// the autopilot's choice); empty when the broker sent none.
	Extra []byte
}

// IsPopFrames reports whether a pop answer is the framed one.
func IsPopFrames(b []byte) bool {
	return len(b) >= len(popFramesMagic)+1 && string(b[:len(popFramesMagic)]) == popFramesMagic
}

var (
	popZstdOnce sync.Once
	popZstdDec  *zstd.Decoder
)

func popZstdDecoder() *zstd.Decoder {
	popZstdOnce.Do(func() {
		popZstdDec, _ = zstd.NewReader(nil)
	})
	return popZstdDec
}

type frameReader struct {
	b   []byte
	p   int
	bad bool
}

func (r *frameReader) take(n int) []byte {
	if r.bad || n < 0 || len(r.b)-r.p < n {
		r.bad = true
		return nil
	}
	s := r.b[r.p : r.p+n]
	r.p += n
	return s
}

func (r *frameReader) u8() byte {
	if s := r.take(1); s != nil {
		return s[0]
	}
	return 0
}

func (r *frameReader) u16() int {
	if s := r.take(2); s != nil {
		return int(binary.LittleEndian.Uint16(s))
	}
	return 0
}

func (r *frameReader) u32() uint32 {
	if s := r.take(4); s != nil {
		return binary.LittleEndian.Uint32(s)
	}
	return 0
}

func (r *frameReader) u64() uint64 {
	if s := r.take(8); s != nil {
		return binary.LittleEndian.Uint64(s)
	}
	return 0
}

func (r *frameReader) text() string { return string(r.take(r.u16())) }

// DecodePopFrames unpacks a framed pop answer into its messages, in delivery
// order.
func DecodePopFrames(b []byte) (*PopFrames, error) {
	if !IsPopFrames(b) {
		return nil, fmt.Errorf("not a framed pop answer")
	}
	r := &frameReader{b: b, p: len(popFramesMagic) + 1}
	var extra []byte
	switch version := b[len(popFramesMagic)]; version {
	case 0:
	case 1:
		extra = r.take(int(r.u32()))
	default:
		return nil, fmt.Errorf("framed pop answer: unknown version %d", version)
	}
	out := &PopFrames{Extra: extra, Queue: r.text(), LeaseID: r.text(), ConsumerGroup: r.text()}
	claims := r.u32()
	for c := uint32(0); c < claims && !r.bad; c++ {
		pid := r.u64()
		partition := r.text()
		start, end := r.u64(), r.u64()
		attempt := r.u32()
		records := r.u32()
		for i := uint32(0); i < records && !r.bad; i++ {
			base := r.u64()
			created := int64(r.u64())
			count := r.u32()
			codec := r.u8()
			raw := r.take(int(r.u32()))
			if r.bad {
				break
			}
			if codec == 1 {
				var err error
				if raw, err = popZstdDecoder().DecodeAll(raw, nil); err != nil {
					return nil, fmt.Errorf("framed pop answer: zstd: %w", err)
				}
			} else if codec != 0 {
				return nil, fmt.Errorf("framed pop answer: unknown codec %d", codec)
			}
			fr := &frameReader{b: raw}
			for j := uint32(0); j < count; j++ {
				body := &frameReader{b: fr.take(int(fr.u32()))}
				if fr.bad {
					return nil, fmt.Errorf("framed pop answer: a record is shorter than its %d frames", count)
				}
				flags := body.u8()
				m := PopFrame{PartitionID: pid, Partition: partition, Offset: base + uint64(j), CreatedAtMicros: created, DeliveryAttempt: attempt}
				copy(m.ID[:], body.take(16))
				if flags&frameFlagTrace != 0 {
					m.TraceID = body.take(16)
				}
				m.TransactionID = body.text()
				if flags&frameFlagProducer != 0 {
					m.ProducerSub = body.text()
				}
				if flags&frameFlagHeaders != 0 && !body.bad {
					h, ok := headersOfBlock(body.take(body.u16()))
					if !ok {
						return nil, fmt.Errorf("framed pop answer: a frame's headers do not decode")
					}
					m.Headers = h
				}
				// An encrypted payload, or a link (its payload is the address of
				// the message it stands for), is never answered framed.
				if body.bad || flags&(frameFlagEncrypted|frameFlagLink) != 0 {
					return nil, fmt.Errorf("framed pop answer: a frame is malformed, encrypted or a link")
				}
				m.Payload = body.b[body.p:]
				if m.Offset >= start && m.Offset <= end {
					out.Messages = append(out.Messages, m)
				}
			}
		}
	}
	if r.bad {
		return nil, fmt.Errorf("framed pop answer: truncated")
	}
	return out, nil
}

// messagesOfFrames is a framed answer's messages as the client hands them to
// a consumer: the same fields the JSON answer fills.
func messagesOfFrames(f *PopFrames) []*Message {
	messages := make([]*Message, 0, len(f.Messages))
	pid, pidText := ^uint64(0), ""
	for i := range f.Messages {
		m := &f.Messages[i]
		if m.PartitionID != pid {
			pid, pidText = m.PartitionID, strconv.FormatUint(m.PartitionID, 10)
		}
		msg := &Message{
			TransactionID: m.TransactionID,
			PartitionID:   pidText,
			LeaseID:       f.LeaseID,
			Partition:     m.Partition,
			CreatedAt:     time.UnixMicro(m.CreatedAtMicros).UTC().Format("2006-01-02T15:04:05.000Z"),
			ProducerSub:   m.ProducerSub,
			Headers:       m.Headers,
			Offset:        int64(m.Offset),
			ConsumerGroup: f.ConsumerGroup,
		}
		// As the JSON answer: a payload that is not an object leaves Data nil.
		var data map[string]interface{}
		if json.Unmarshal(m.Payload, &data) == nil {
			msg.Data = data
		}
		messages = append(messages, msg)
	}
	return messages
}

// popRequest performs one pop. With ClientConfig.FramedPop the request
// carries frames=true and a framed answer is unpacked here: its messages come
// back in framed, and result holds the answer's own keys. Otherwise, or when
// the broker answered JSON (one that does not know the parameter, a conflating
// group, an encrypted queue), result is the decoded JSON answer and isFramed
// is false: the caller parses the messages from it as it always did.
func (hc *HttpClient) popRequest(ctx context.Context, path string, timeoutMs int, affinityKey string, opts ...RequestOption) (result map[string]interface{}, framed []*Message, isFramed bool, err error) {
	if !hc.config.FramedPop {
		result, err = hc.Get(ctx, path, timeoutMs, affinityKey, opts...)
		return result, nil, false, err
	}
	if strings.Contains(path, "?") {
		path += "&frames=true"
	} else {
		path += "?frames=true"
	}
	raw, err := hc.GetRawWith(ctx, path, timeoutMs, affinityKey, opts...)
	if err != nil {
		return nil, nil, false, err
	}
	if !IsPopFrames(raw) {
		result, err = parseBody(raw)
		return result, nil, false, err
	}
	f, err := DecodePopFrames(raw)
	if err != nil {
		return nil, nil, false, err
	}
	if len(f.Extra) > 0 {
		if err := json.Unmarshal(f.Extra, &result); err != nil {
			return nil, nil, false, fmt.Errorf("framed pop answer: its own keys: %w", err)
		}
	}
	return result, messagesOfFrames(f), true, nil
}
