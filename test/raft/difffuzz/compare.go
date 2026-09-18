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
