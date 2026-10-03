package core

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// jsonWords + jsonEvent: copied VERBATIM from goload (ref/goload-pe-build/
// main.go), so the three systems carry the same ~256 B JSON event payload.

// jsonWords is a fixed vocabulary of random lowercase words for -payload-json.
var jsonWords = func() []string {
	rng := rand.New(rand.NewSource(7))
	w := make([]string, 3000)
	for i := range w {
		b := make([]byte, 2+rng.Intn(8))
		for k := range b {
			b[k] = byte('a' + rng.Intn(26))
		}
		w[i] = string(b)
	}
	return w
}()

// jsonEvent builds one realistic event payload of roughly `size` JSON bytes:
// distinct ids, timestamps, a phone number and a price, then random-word text
// to reach the size (nothing is padded with repeated bytes).
func jsonEvent(rng *rand.Rand, size int) map[string]interface{} {
	uuid := func() string {
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rng.Uint32(), rng.Intn(1<<16), rng.Intn(1<<16), rng.Intn(1<<16), rng.Int63n(1<<48))
	}
	types := []string{"message.created", "message.delivered", "reservation.updated", "rate.changed", "conversation.assigned"}
	chans := []string{"whatsapp", "email", "sms", "booking", "expedia", "airbnb"}
	ev := map[string]interface{}{
		"id":             uuid(),
		"type":           types[rng.Intn(len(types))],
		"tenantId":       fmt.Sprintf("t-%04d", 1+rng.Intn(300)),
		"propertyId":     1000 + rng.Intn(99000),
		"conversationId": uuid(),
		"channel":        chans[rng.Intn(len(chans))],
		"createdAt":      time.Unix(1758500000+rng.Int63n(86400), rng.Int63n(1e9)).UTC().Format("2006-01-02T15:04:05.000Z"),
		"from":           fmt.Sprintf("+39%010d", rng.Int63n(10000000000)),
		"price":          float64(4000+rng.Intn(86000)) / 100,
		"currency":       "EUR",
	}
	base, _ := json.Marshal(ev)
	var sb strings.Builder
	for len(base)+sb.Len()+10 < size {
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(jsonWords[rng.Intn(len(jsonWords))])
	}
	ev["text"] = sb.String()
	return ev
}

// ---------------------------------------------------------------------------
// Pool + stamping (goload -payload-json -e2e, plus "src").

// PoolBatches is the number of pre-marshaled unit-sized batches rotated per
// unit (goload: 64).
const PoolBatches = 64

// PayloadPool holds PoolBatches x maxUnit pre-marshaled events. Every unit
// takes the next batch; message j of the unit is event j of that batch with
// `{"ts":<scheduled unix µs>,"src":<loader index>,` spliced in front.
type PayloadPool struct {
	raws    [][][]byte // [PoolBatches][maxUnit]: marshaled events, "{...}"
	cum     [][]int    // cum[b][j] = sum over i<j of len(raws[b][i])-1
	srcPart []byte     // `,"src":<n>,`
	idx     atomic.Uint64
	// AvgRaw is the mean marshaled event size (bytes) before the splice.
	AvgRaw float64
	// AvgStamped is the mean message size (bytes) as sent.
	AvgStamped float64
}

// NewPayloadPool pre-marshals PoolBatches*maxUnit events of ~size bytes.
func NewPayloadPool(seed int64, size, maxUnit, src int) *PayloadPool {
	if maxUnit < 1 {
		maxUnit = 1
	}
	rng := rand.New(rand.NewSource(seed))
	p := &PayloadPool{
		raws:    make([][][]byte, PoolBatches),
		cum:     make([][]int, PoolBatches),
		srcPart: []byte(`,"src":` + strconv.Itoa(src) + `,`),
	}
	total, n := 0, 0
	for b := range p.raws {
		p.raws[b] = make([][]byte, maxUnit)
		p.cum[b] = make([]int, maxUnit+1)
		for j := range p.raws[b] {
			raw, err := json.Marshal(jsonEvent(rng, size))
			if err != nil || len(raw) < 2 || raw[0] != '{' {
				panic("payload: bad event")
			}
			p.raws[b][j] = raw
			p.cum[b][j+1] = p.cum[b][j] + len(raw) - 1
			total += len(raw)
			n++
		}
	}
	p.AvgRaw = float64(total) / float64(n)
	hdr := len(`{"ts":`) + len(strconv.FormatInt(time.Now().UnixMicro(), 10)) + len(p.srcPart)
	p.AvgStamped = p.AvgRaw - 1 + float64(hdr)
	return p
}

// Stamp returns the n payloads of one unit scheduled at schedMicros (unix µs).
// One slab allocation per unit; the pool is only read.
func (p *PayloadPool) Stamp(schedMicros int64, n int) [][]byte {
	b := p.idx.Add(1) % PoolBatches
	raws := p.raws[b]
	if n > len(raws) {
		n = len(raws)
	}
	var hb [48]byte
	h := append(hb[:0], `{"ts":`...)
	h = strconv.AppendInt(h, schedMicros, 10)
	h = append(h, p.srcPart...)
	slab := make([]byte, n*len(h)+p.cum[b][n])
	out := make([][]byte, n)
	off := 0
	for j := 0; j < n; j++ {
		r := raws[j]
		l := len(h) + len(r) - 1
		m := slab[off : off+l : off+l]
		copy(m, h)
		copy(m[len(h):], r[1:])
		out[j] = m
		off += l
	}
	return out
}

// Warm returns an event WITHOUT "ts" (warm-up messages are never e2e samples
// and never counted as load).
func (p *PayloadPool) Warm() []byte {
	r := p.raws[0][0]
	out := make([]byte, len(r))
	copy(out, r)
	return out
}

// ParseStamp reads the leading `{"ts":<µs>,"src":<n>,` of a load message by
// scanning bytes (no JSON decoding on the hot path). ok=false for anything
// that does not start with {"ts": (warm-up messages). src is -1 when a ts is
// present without a src.
func ParseStamp(v []byte) (ts int64, src int, ok bool) {
	if len(v) < 8 || v[0] != '{' || v[1] != '"' || v[2] != 't' || v[3] != 's' || v[4] != '"' || v[5] != ':' {
		return 0, 0, false
	}
	i := 6
	for ; i < len(v) && v[i] >= '0' && v[i] <= '9'; i++ {
		ts = ts*10 + int64(v[i]-'0')
	}
	if i == 6 || i >= len(v) || v[i] != ',' {
		return 0, 0, false
	}
	i++
	if len(v) < i+8 || v[i] != '"' || v[i+1] != 's' || v[i+2] != 'r' || v[i+3] != 'c' || v[i+4] != '"' || v[i+5] != ':' {
		return ts, -1, true
	}
	i += 6
	j := i
	for ; i < len(v) && v[i] >= '0' && v[i] <= '9'; i++ {
		src = src*10 + int(v[i]-'0')
	}
	if i == j {
		return ts, -1, true
	}
	return ts, src, true
}
