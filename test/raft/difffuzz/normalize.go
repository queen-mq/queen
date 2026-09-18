package main

// Normalization: what two different brokers are ALLOWED to disagree about.
//
// Side A and side B mint their own message ids, lease ids, partition ids and
// timestamps, so a byte comparison of two answers is always red and says
// nothing. Normalization removes exactly that freedom and nothing else:
//
//   ids (uuid-shaped strings)  ->  <id:N>, numbered per side in order of first
//                                  appearance. This keeps the IDENTITY RELATION:
//                                  if A returns the same lease id twice and B
//                                  returns two different ones, the normalized
//                                  forms differ, which is the bug. A blanket
//                                  <id> would hide it.
//   timestamps (ISO 8601)      ->  <ts>. Two brokers cannot be asked to stamp
//                                  the same wall clock. Anything that depends on
//                                  the ORDER of timestamps belongs to
//                                  test/raft/checker, which reads a run log and
//                                  can reason per side.
//   volatile keys              ->  dropped by name (VolatileKeys below).
//
// Everything else — every status, every count, every offset, every deliveryAttempt,
// every key that only one side emits — survives and is compared.

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	// ISO 8601 / RFC3339 with or without fractional seconds and zone, plus the
	// "YYYY-MM-DD HH:MM:SS" spelling Postgres renders in some views.
	tsRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$`)
)

// VolatileKeys are dropped wherever they appear. They are node-local or
// wall-clock-derived by construction, so comparing them would only ever produce
// noise. Each entry needs a reason: this list is where a real difference goes to
// hide, and the review lens for §13.4 is "what did you drop and why".
var VolatileKeys = map[string]string{
	"lagSeconds":    "derived from the wall clock at read time",
	"ageSeconds":    "derived from the wall clock at read time",
	"uptime":        "process lifetime",
	"uptimeSeconds": "process lifetime",
	"serverTime":    "wall clock",
	"generatedAt":   "wall clock",
	"tookMs":        "latency",
	"durationMs":    "latency",
	"newExpiresAt":  "lease expiry is now + leaseSeconds, stamped by each broker's planner (D5)",
	"expiresAt":     "same",
	"storageBytes":  "physical size differs by construction between postgres and raft (I8)",
	"retainedBytes": "same",
	"diskBytes":     "same",
}

// Normalizer holds one side's symbol table.
type Normalizer struct {
	ids  map[string]string
	next int
	// SortArrays canonicalizes array ORDER as well. Used for the final views,
	// where the broker does not promise an order (a list of queues, a KV page);
	// never used for a response, where order is part of the answer.
	//
	// LIMIT, on purpose: substitution happens BEFORE the sort, so an element
	// whose only distinguishing field is an id sorts by its symbol, i.e. by the
	// order the side emitted it. That is exact when the element carries a stable
	// key too (transactionId, queue name, KV key), which every view compared in
	// run.go does. A future view without one needs a sort key declared here, not
	// a blanket <id>.
	SortArrays bool
}

func NewNormalizer() *Normalizer { return &Normalizer{ids: map[string]string{}} }

// Normalize decodes and normalizes a raw JSON body. Numbers are kept as
// json.Number so that 1 and 1.0 stay distinguishable and an int64 offset does
// not go through a float64.
func (n *Normalizer) Normalize(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return n.value(v), nil
}

func (n *Normalizer) value(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		// SORTED, not map order: the symbol table numbers ids in order of first
		// appearance, so walking a body in Go's randomized map order would give
		// the same body two different numberings on two runs — every pop answer
		// would "diverge" at $.messages[0].id. (Found by the selftest, which is
		// what it is for.)
		for _, k := range sortedKeys(t) {
			if _, drop := VolatileKeys[k]; drop {
				continue
			}
			out[k] = n.value(t[k])
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, e := range t {
			out = append(out, n.value(e))
		}
		if n.SortArrays {
			sort.SliceStable(out, func(i, j int) bool { return Canonical(out[i]) < Canonical(out[j]) })
		}
		return out
	case string:
		return n.str(t)
	default:
		return v
	}
}

func (n *Normalizer) str(s string) string {
	switch {
	case uuidRe.MatchString(s):
		if sym, ok := n.ids[s]; ok {
			return sym
		}
		sym := "<id:" + strconv.Itoa(n.next) + ">"
		n.next++
		n.ids[s] = sym
		return sym
	case tsRe.MatchString(s):
		return "<ts>"
	default:
		return s
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Canonical renders a normalized value with sorted object keys, so that two
// values are equal exactly when their canonical text is equal.
func Canonical(v any) string {
	var b strings.Builder
	writeCanonical(&b, v)
	return b.String()
}

func writeCanonical(b *strings.Builder, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			q, _ := json.Marshal(k)
			b.Write(q)
			b.WriteByte(':')
			writeCanonical(b, t[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, e)
		}
		b.WriteByte(']')
	case nil:
		b.WriteString("null")
	default:
		q, err := json.Marshal(t)
		if err != nil {
			b.WriteString(strconv.Quote(fmt.Sprint(t)))
			return
		}
		b.Write(q)
	}
}
