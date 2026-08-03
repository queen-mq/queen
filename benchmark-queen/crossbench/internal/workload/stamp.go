package workload

import (
	"encoding/json"
	"errors"
	"strconv"
)

// Stamp is the per-event correctness token. It is assigned by the producer and
// carried forward UNCHANGED through every hop, so the same seq is what the
// verifier sees at all three stages.
//
// TS is UnixMicro of the producer's SCHEDULED instant (not its send instant):
// end-to-end latency measured from it is coordinated-omission corrected, and
// micros survive a JSON float64 round-trip exactly (nanos would not).
type Stamp struct {
	Prop int
	Flow Flow
	Seq  int64
	TS   int64
}

// The wire format is JSON, on purpose: it is what the July run used, and it is
// what a Postgres-backed broker actually has to parse and store. Changing it to
// a binary frame would quietly make the DB-backed systems look better.
//
// The four stamp fields are always emitted FIRST, in this fixed order, so the
// harness can pull them with a small scanner instead of unmarshalling the whole
// document (including flow B's ~2KB rates array) on every one of the 6R
// deliveries per second. Fidelity on the wire, cheap on the loader.

var errFastPath = errors.New("stamp: fast path miss")

// EncodeIngress builds a flow-A or flow-B ingress payload. rates is appended
// verbatim as the `rates` field when non-nil (flow B); pass nil for flow A.
func EncodeIngress(s Stamp, rates []byte) []byte {
	b := make([]byte, 0, 64+len(rates))
	b = appendStamp(b, s)
	if len(rates) > 0 {
		b = append(b, `,"rates":`...)
		b = append(b, rates...)
	}
	return append(b, '}')
}

// EncodeDerived builds the payload a stage re-publishes. It carries the stamp
// forward and drops the padding: the derived hop's job is to preserve identity
// and order, not to re-transmit the rates blob.
func EncodeDerived(s Stamp) []byte {
	return append(appendStamp(make([]byte, 0, 64), s), '}')
}

func appendStamp(b []byte, s Stamp) []byte {
	b = append(b, `{"prop":`...)
	b = strconv.AppendInt(b, int64(s.Prop), 10)
	b = append(b, `,"flow":"`...)
	b = append(b, string(s.Flow)...)
	b = append(b, `","seq":`...)
	b = strconv.AppendInt(b, s.Seq, 10)
	b = append(b, `,"ts":`...)
	b = strconv.AppendInt(b, s.TS, 10)
	return b
}

// DecodeStamp extracts the stamp from a payload. It tries the ordered fast path
// first and falls back to a full unmarshal, so a broker that re-serialises the
// document (reordering keys, adding fields) still decodes correctly — just
// slower. Callers that care can watch SlowDecodes.
func DecodeStamp(p []byte) (Stamp, error) {
	if s, err := fastStamp(p); err == nil {
		return s, nil
	}
	slowDecodes.Add(1)
	var m struct {
		Prop *int    `json:"prop"`
		Flow *string `json:"flow"`
		Seq  *int64  `json:"seq"`
		TS   *int64  `json:"ts"`
	}
	if err := json.Unmarshal(p, &m); err != nil {
		return Stamp{}, err
	}
	if m.Prop == nil || m.Flow == nil || m.Seq == nil || m.TS == nil {
		return Stamp{}, errors.New("stamp: missing field")
	}
	return Stamp{Prop: *m.Prop, Flow: Flow(*m.Flow), Seq: *m.Seq, TS: *m.TS}, nil
}

// fastStamp reads `{"prop":N,"flow":"X","seq":N,"ts":N` with no allocation.
// Any deviation returns errFastPath and the caller falls back.
func fastStamp(p []byte) (Stamp, error) {
	var s Stamp
	i := 0
	var err error

	if i, err = expect(p, i, `{"prop":`); err != nil {
		return s, err
	}
	var prop int64
	if prop, i, err = readInt(p, i); err != nil {
		return s, err
	}
	s.Prop = int(prop)

	if i, err = expect(p, i, `,"flow":"`); err != nil {
		return s, err
	}
	start := i
	for i < len(p) && p[i] != '"' {
		i++
	}
	if i >= len(p) || i == start {
		return s, errFastPath
	}
	s.Flow = Flow(p[start:i])
	i++ // closing quote

	if i, err = expect(p, i, `,"seq":`); err != nil {
		return s, err
	}
	if s.Seq, i, err = readInt(p, i); err != nil {
		return s, err
	}

	if i, err = expect(p, i, `,"ts":`); err != nil {
		return s, err
	}
	if s.TS, _, err = readInt(p, i); err != nil {
		return s, err
	}
	return s, nil
}

func expect(p []byte, i int, lit string) (int, error) {
	if i+len(lit) > len(p) || string(p[i:i+len(lit)]) != lit {
		return i, errFastPath
	}
	return i + len(lit), nil
}

func readInt(p []byte, i int) (int64, int, error) {
	start := i
	if i < len(p) && p[i] == '-' {
		i++
	}
	for i < len(p) && p[i] >= '0' && p[i] <= '9' {
		i++
	}
	if i == start {
		return 0, i, errFastPath
	}
	v, err := strconv.ParseInt(string(p[start:i]), 10, 64)
	if err != nil {
		return 0, i, errFastPath
	}
	return v, i, nil
}

// RatesPad builds the ~targetBytes `rates` JSON array that makes flow-B
// payloads realistic. Built once per run and shared: it is padding, not data.
func RatesPad(targetBytes int) []byte {
	b := make([]byte, 0, targetBytes+64)
	b = append(b, '[')
	approx, day := 0, 1
	for approx < targetBytes {
		if day > 1 {
			b = append(b, ',')
		}
		b = append(b, `{"d":"2026-`...)
		b = appendPad2(b, 1+(day/28)%12)
		b = append(b, '-')
		b = appendPad2(b, 1+day%28)
		b = append(b, `","p":`...)
		b = strconv.AppendFloat(b, 100.0+float64(day%400), 'f', 1, 64)
		b = append(b, `,"a":`...)
		b = strconv.AppendInt(b, int64(day%12), 10)
		b = append(b, '}')
		approx += 36
		day++
	}
	return append(b, ']')
}

func appendPad2(b []byte, v int) []byte {
	if v < 10 {
		b = append(b, '0')
	}
	return strconv.AppendInt(b, int64(v), 10)
}

// StampFromMap reads a stamp out of an already-decoded JSON object.
//
// Some clients hand the application a parsed map rather than the raw bytes
// (QueenMQ's Go client decodes into map[string]interface{} before the adapter
// ever sees it). Re-marshalling that map just to run DecodeStamp would charge
// the adapter for work its client already did, so read the map directly.
// JSON numbers decode to float64; prop, seq and ts are all integer-valued and
// well inside float64's exact range.
func StampFromMap(d map[string]interface{}) (Stamp, error) {
	pf, ok1 := d["prop"].(float64)
	sf, ok2 := d["seq"].(float64)
	tf, ok3 := d["ts"].(float64)
	fl, ok4 := d["flow"].(string)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return Stamp{}, errors.New("stamp: missing or mistyped field in decoded payload")
	}
	return Stamp{Prop: int(pf), Flow: Flow(fl), Seq: int64(sf), TS: int64(tf)}, nil
}
