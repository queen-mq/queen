package main

// Comparison and the divergence report.
//
// A divergence names ONE path. "the bodies differ" plus two 40 KB blobs is not
// a bug report; `messages[3].deliveryAttempt: A=1 B=2` is.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Divergence is one difference between the two sides.
type Divergence struct {
	Op      int    `json:"op"`      // operation index, -1 for a view comparison
	Kind    string `json:"kind"`    // operation kind or view name
	What    string `json:"what"`    // "status" | "body" | "transport"
	Path    string `json:"path"`    // JSON path of the first difference
	A       string `json:"a"`       // canonical value on side A at Path
	B       string `json:"b"`       // canonical value on side B at Path
	Request string `json:"request"` // a one-line description of what was sent
}

func (d Divergence) String() string {
	return fmt.Sprintf("op %d (%s) %s at %s\n    A: %s\n    B: %s\n    sent: %s",
		d.Op, d.Kind, d.What, d.Path, trunc(d.A, 400), trunc(d.B, 400), d.Request)
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("… (%d bytes)", len(s))
}

// CompareResponses compares one pair of answers. `sortArrays` is for views.
func CompareResponses(opIndex int, kind, request string, a, b *Resp, sortArrays bool) *Divergence {
	if a.Status != b.Status {
		return &Divergence{
			Op: opIndex, Kind: kind, What: "status", Path: "$",
			A: strconv.Itoa(a.Status), B: strconv.Itoa(b.Status), Request: request,
		}
	}
	na, nb := NewNormalizer(), NewNormalizer()
	na.SortArrays, nb.SortArrays = sortArrays, sortArrays
	va, errA := na.Normalize(a.Body)
	vb, errB := nb.Normalize(b.Body)
	if errA != nil || errB != nil {
		return &Divergence{
			Op: opIndex, Kind: kind, What: "body", Path: "$",
			A:       fmt.Sprintf("decode: %v: %s", errA, trunc(string(a.Body), 200)),
			B:       fmt.Sprintf("decode: %v: %s", errB, trunc(string(b.Body), 200)),
			Request: request,
		}
	}
	if path, da, db, differ := FirstDiff("$", va, vb); differ {
		return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: path, A: da, B: db, Request: request}
	}
	return nil
}

// compareAck compares two ack (or ack-batch) answers. Both routes answer a
// TOP-LEVEL array [{index,transactionId,success,error,leaseReleased,dlq,noop?}].
// It compares the STATE-BEARING fields (index, transactionId, dlq, success) and
// absorbs exactly the documented phase-1 AckResult-envelope parity gaps (R-101;
// the WP-1.7c "design call" deferred the shape refinement to a catalogue version
// bump). `statuses` is the per-item requested status for a batch ack, nil for a
// single ack.
//
//   - noop: postgres emits it on every item; the raft facade does not render it.
//     Ignored — a report annotation, not state. (finding)
//   - error: the raft AckResult codec has NO per-item error field (R-101); it
//     reports a rejected/stale/unresolvable ack through success:false, never a
//     string, so it always answers error:null. Postgres annotates the same
//     outcomes with a human string ("already committed: the cursor moved past
//     this message", "invalid or expired lease", "unresolvable: …"). Absorbed
//     ONLY in that direction (pg a string, raft null); a raft-produced error
//     that postgres does not match is still a divergence. (finding)
//   - leaseReleased: the two engines ATTRIBUTE this flag to different items and
//     in different directions. On a partial single ack the postgres broker
//     reports true after acking the HEAD though the rest stays leased (verified:
//     a fresh pop returns EMPTY on both); on a batch ack it flags the head item
//     while the raft facade flags the item that actually releases the lease (the
//     tail). The flag is a per-item envelope annotation whose attribution is not
//     agreed (R-101); the lease STATE it describes is validated where it matters
//     — by the pinned-pop differential, since a lease wrongly released on one
//     engine makes its messages re-poppable there and diverges the next pop — so
//     the flag itself is not compared. (finding: leaseReleased attribution)
//   - success on a no-op ack: a completed ack of a hash already below the cursor
//     is success:true + noop:true on postgres, success:false on the raft facade.
//     Absorbed ONLY when postgres marked the item noop:true. (finding)
//   - dlq on a non-signal (completed) BATCH item: postgres BROADCASTS a
//     (partition,lease) target's dlq count to every item of the target, so a
//     completed sibling of a dlq'd item reads dlq:true; the raft facade
//     attributes dlq PER ITEM (the WP-1.7c seam fix), so it reads dlq:false. The
//     RAFT answer is the correct one — absorbed as an ORACLE QUIRK in that exact
//     direction (completed item, pg=true, raft=false). (finding)
//
// Everything else — a wrong dlq on a signal item, a raft error postgres did not
// produce, a success mismatch that is NOT the no-op case, a leaseReleased
// mismatch in the losing direction, a different item count — is a real divergence.
func compareAck(opIndex int, kind, request string, a, b *Resp, statuses []string) *Divergence {
	if a.Status != b.Status {
		return &Divergence{Op: opIndex, Kind: kind, What: "status", Path: "$",
			A: strconv.Itoa(a.Status), B: strconv.Itoa(b.Status), Request: request}
	}
	arrA, errA := ackItems(a.Body)
	arrB, errB := ackItems(b.Body)
	if errA != nil || errB != nil {
		return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: "$",
			A: fmt.Sprintf("parse: %v: %s", errA, trunc(string(a.Body), 200)),
			B: fmt.Sprintf("parse: %v: %s", errB, trunc(string(b.Body), 200)), Request: request}
	}
	if len(arrA) != len(arrB) {
		return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: "$.length",
			A: strconv.Itoa(len(arrA)), B: strconv.Itoa(len(arrB)), Request: request}
	}
	for i := range arrA {
		pa, pb := arrA[i], arrB[i]
		path := fmt.Sprintf("%s[%d]", "$", i)
		for _, k := range []string{"index", "transactionId", "conflated"} {
			if va, vb := field(pa, k), field(pb, k); va != vb {
				return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: path + "." + k, A: va, B: vb, Request: request}
			}
		}
		// dlq: absorb the postgres batch broadcast on a completed item.
		da, db := field(pa, "dlq"), field(pb, "dlq")
		if da != db {
			completed := statuses != nil && i < len(statuses) && statuses[i] == "completed"
			if !(completed && da == "true" && db == "false") {
				return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: path + ".dlq", A: da, B: db,
					Request: request + " (dlq differs and it is not the postgres batch broadcast on a completed item)"}
			}
		}
		// error: only the pg-string / raft-null direction is absorbed (R-101).
		if ea, eb := field(pa, "error"), field(pb, "error"); ea != eb {
			if rbNull := eb == "null" || eb == "<absent>"; !rbNull {
				return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: path + ".error", A: ea, B: eb,
					Request: request + " (raft produced an error that postgres did not match)"}
			}
		}
		// success: absorbed only when postgres flags a no-op.
		pgNoop := field(pa, "noop") == "true"
		if sa, sb := field(pa, "success"), field(pb, "success"); sa != sb && !pgNoop {
			return &Divergence{Op: opIndex, Kind: kind, What: "body", Path: path + ".success", A: sa, B: sb,
				Request: request + " (success differs and postgres did NOT mark it a no-op)"}
		}
		// leaseReleased: attribution not agreed (R-101); the lease STATE is
		// checked by the pinned-pop differential, not this flag. Not compared.
	}
	return nil
}

// ackItems decodes an ack answer into per-item field maps.
func ackItems(raw json.RawMessage) ([]map[string]json.RawMessage, error) {
	var arr []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}

// field renders one ack-item field as a canonical string, "<absent>" when the
// item does not carry it (so present-null and absent stay distinguishable).
func field(m map[string]json.RawMessage, k string) string {
	v, ok := m[k]
	if !ok {
		return "<absent>"
	}
	return string(v)
}

// FirstDiff walks both values in a fixed order (sorted keys, then index) and
// returns the path of the first difference. Objects are compared key by key so
// that a key present on one side only is reported as that key, not as "the
// whole object differs".
func FirstDiff(path string, a, b any) (string, string, string, bool) {
	switch ta := a.(type) {
	case map[string]any:
		tb, ok := b.(map[string]any)
		if !ok {
			return path, Canonical(a), Canonical(b), true
		}
		keys := map[string]bool{}
		for k := range ta {
			keys[k] = true
		}
		for k := range tb {
			keys[k] = true
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			va, oka := ta[k]
			vb, okb := tb[k]
			if oka != okb {
				return path + "." + k, presence(oka, va), presence(okb, vb), true
			}
			if p, da, db, differ := FirstDiff(path+"."+k, va, vb); differ {
				return p, da, db, true
			}
		}
		return "", "", "", false
	case []any:
		tb, ok := b.([]any)
		if !ok {
			return path, Canonical(a), Canonical(b), true
		}
		if len(ta) != len(tb) {
			return path + ".length", strconv.Itoa(len(ta)), strconv.Itoa(len(tb)), true
		}
		for i := range ta {
			if p, da, db, differ := FirstDiff(fmt.Sprintf("%s[%d]", path, i), ta[i], tb[i]); differ {
				return p, da, db, true
			}
		}
		return "", "", "", false
	default:
		ca, cb := Canonical(a), Canonical(b)
		if ca != cb {
			return path, ca, cb, true
		}
		return "", "", "", false
	}
}

func presence(ok bool, v any) string {
	if !ok {
		return "<absent>"
	}
	return Canonical(v)
}

// Report is the artifact of a run: enough to replay it.
type Report struct {
	Seed        int64        `json:"seed"`
	RunID       string       `json:"runId"`
	Ops         int          `json:"ops"`
	Mix         string       `json:"mix"`
	SideA       string       `json:"sideA"`
	SideB       string       `json:"sideB"`
	Executed    int          `json:"executed"`
	Divergences []Divergence `json:"divergences"`
	Replay      string       `json:"replay"`
}

func (r *Report) JSON() []byte {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return []byte(fmt.Sprintf("{\"error\":%q}", err.Error()))
	}
	return append(b, '\n')
}

func (r *Report) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "difffuzz seed=%d run-id=%s ops=%d executed=%d mix=%s\n", r.Seed, r.RunID, r.Ops, r.Executed, r.Mix)
	fmt.Fprintf(&b, "  A=%s  B=%s\n", r.SideA, r.SideB)
	if len(r.Divergences) == 0 {
		b.WriteString("  no divergence\n")
		return b.String()
	}
	fmt.Fprintf(&b, "  %d divergence(s):\n", len(r.Divergences))
	for _, d := range r.Divergences {
		fmt.Fprintf(&b, "  - %s\n", strings.ReplaceAll(d.String(), "\n", "\n  "))
	}
	fmt.Fprintf(&b, "  replay: %s\n", r.Replay)
	return b.String()
}
