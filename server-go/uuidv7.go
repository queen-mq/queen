package main

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// UUIDv7 generator — time-ordered UUIDs, matching lib/queen.hpp generate_uuidv7().
// Message IDs must be commit/time-ordered because the pop cursor is (created_at, id):
// a monotonic sub-millisecond counter guarantees within-ms ordering, mirroring the
// C++ per-thread sequence. A single mutex is plenty for a spike (the hot path is PG).
var (
	uuidMu   sync.Mutex
	uuidLast uint64
	uuidSeq  uint16
	uuidRand [8]byte
)

const hexDigits = "0123456789abcdef"

func generateUUIDv7() string {
	uuidMu.Lock()
	nowMs := uint64(time.Now().UnixMilli())
	if nowMs <= uuidLast {
		uuidSeq++
	} else {
		uuidLast = nowMs
		uuidSeq = 0
	}
	ms := uuidLast
	seq := uuidSeq
	// fresh randomness for the tail
	_, _ = rand.Read(uuidRand[:])
	tail := binary.BigEndian.Uint64(uuidRand[:])
	uuidMu.Unlock()

	var b [16]byte
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	// version 7 + high bits of sequence
	b[6] = 0x70 | byte((seq>>8)&0x0f)
	b[7] = byte(seq)
	// variant (10xxxxxx) + random
	b[8] = 0x80 | byte(tail>>58)&0x3f
	b[9] = byte(tail >> 50)
	b[10] = byte(tail >> 42)
	b[11] = byte(tail >> 34)
	b[12] = byte(tail >> 26)
	b[13] = byte(tail >> 18)
	b[14] = byte(tail >> 10)
	b[15] = byte(tail >> 2)

	// 8-4-4-4-12 hex with dashes
	var out [36]byte
	pos := 0
	for i := 0; i < 16; i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[pos] = '-'
			pos++
		}
		out[pos] = hexDigits[b[i]>>4]
		out[pos+1] = hexDigits[b[i]&0x0f]
		pos += 2
	}
	return string(out[:])
}
