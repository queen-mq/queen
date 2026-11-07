package feed

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
)

// The page decode. encoding/json scans its whole input on every Unmarshal
// call, so a page is decoded in one call (answer, entries and payloads
// together), not one call per level. A large page is split at its entries
// by a light scan and its parts are decoded on several cores; whatever the
// scan does not expect sends the page to the plain decode, which then says
// what is wrong with it. Entries come out in log order either way.

// wireEntry is one entry as the broker writes it, its payload decoded with it.
type wireEntry struct {
	Position      int64       `json:"position"`
	Kind          string      `json:"kind"`
	Queue         string      `json:"queue"`
	Partition     string      `json:"partition"`
	PartitionID   string      `json:"partitionId"`
	Offset        int64       `json:"offset"`
	ID            string      `json:"id"`
	TransactionID string      `json:"transactionId"`
	Payload       interface{} `json:"payload"`
	TS            string      `json:"ts"`
}

type wireAnswer struct {
	Applied    *int64      `json:"applied"`
	CaughtUp   bool        `json:"caughtUp"`
	Entries    []wireEntry `json:"entries"`
	Next       int64       `json:"next"`
	ResolvedTS string      `json:"resolvedTs"`
}

// splitPartBytes: the least a decoding goroutine takes, in bytes of the page.
// A stream reads the broker's default pages (1000 entries, 1 MiB at most),
// so the split goes by bytes, not entries: a page of large messages holds
// few of them.
const splitPartBytes = 64 << 10

// splitMinBytes: a smaller page is decoded on one core (a split costs more than it gives).
const splitMinBytes = 2 * splitPartBytes

// splitMinEntries: the fewest entries a decoding goroutine takes.
const splitMinEntries = 8

func decodePage(body []byte) (Page, error) {
	if len(body) >= splitMinBytes {
		if workers := runtime.GOMAXPROCS(0); workers > 1 {
			if p, ok := decodeSplit(body, workers); ok {
				return p, nil
			}
		}
	}
	var a wireAnswer
	if err := json.Unmarshal(body, &a); err != nil {
		return Page{}, fmt.Errorf("feed: the answer is not a feed page: %w", err)
	}
	return pageOf(&a, a.Entries), nil
}

func pageOf(a *wireAnswer, es []wireEntry) Page {
	p := Page{Next: a.Next, ResolvedTS: a.ResolvedTS, CaughtUp: a.CaughtUp, Applied: a.Applied, Entries: make([]Entry, len(es))}
	for i := range es {
		e := &es[i]
		p.Entries[i] = Entry{
			Position: e.Position, Kind: e.Kind, Queue: e.Queue, Partition: e.Partition, PartitionID: e.PartitionID,
			Offset: e.Offset, ID: e.ID, TransactionID: e.TransactionID, Data: e.Payload, TS: e.TS, CreatedAt: e.TS,
		}
	}
	return p
}

// decodeSplit decodes the answer without its entries, and the entries in
// parts, one goroutine each. ok false: decode the page the plain way.
func decodeSplit(body []byte, workers int) (Page, bool) {
	spans, arr, ok := entrySpans(body)
	if !ok || len(spans) < 2*splitMinEntries {
		return Page{}, false
	}
	if most := len(body) / splitPartBytes; workers > most {
		workers = most
	}
	if most := len(spans) / splitMinEntries; workers > most {
		workers = most
	}
	rest := make([]byte, 0, arr[0]+2+len(body)-arr[1])
	rest = append(append(append(rest, body[:arr[0]]...), '[', ']'), body[arr[1]:]...)
	var a wireAnswer
	if err := json.Unmarshal(rest, &a); err != nil {
		return Page{}, false
	}
	out := make([]wireEntry, len(spans))
	per := (len(spans) + workers - 1) / workers
	failed := make([]bool, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo, hi := w*per, (w+1)*per
		if hi > len(spans) {
			hi = len(spans)
		}
		if lo >= hi {
			break
		}
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			// The part as one JSON array: its entries and the commas between them.
			s, e := spans[lo][0], spans[hi-1][1]
			buf := make([]byte, 0, e-s+2)
			buf = append(append(append(buf, '['), body[s:e]...), ']')
			part := out[lo:lo:hi]
			if json.Unmarshal(buf, &part) != nil || len(part) != hi-lo {
				failed[w] = true
			}
		}(w, lo, hi)
	}
	wg.Wait()
	for _, f := range failed {
		if f {
			return Page{}, false
		}
	}
	return pageOf(&a, out), true
}

func isSpace(c byte) bool { return c == ' ' || c == '\n' || c == '\r' || c == '\t' }

func skipSpace(b []byte, i int) int {
	for i < len(b) && isSpace(b[i]) {
		i++
	}
	return i
}

// skipString: past the closing quote of the string at b[i] ('"'); -1 if it never closes.
func skipString(b []byte, i int) int {
	for i++; i < len(b); {
		switch b[i] {
		case '\\':
			i += 2
		case '"':
			return i + 1
		default:
			i++
		}
	}
	return -1
}

// skipValue: past the JSON value at b[i]; -1 if it never ends. It does not
// validate: what it splits is then decoded, and so checked, by encoding/json.
func skipValue(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	switch b[i] {
	case '"':
		return skipString(b, i)
	case '{', '[':
		depth := 0
		for i < len(b) {
			switch b[i] {
			case '"':
				if i = skipString(b, i); i < 0 {
					return -1
				}
				continue
			case '{', '[':
				depth++
			case '}', ']':
				if depth--; depth == 0 {
					return i + 1
				}
			}
			i++
		}
		return -1
	default:
		for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' && !isSpace(b[i]) {
			i++
		}
		return i
	}
}

// entrySpans finds the answer's "entries" array: each entry's span, and the array's.
func entrySpans(b []byte) (spans [][2]int, arr [2]int, ok bool) {
	i := skipSpace(b, 0)
	if i >= len(b) || b[i] != '{' {
		return nil, arr, false
	}
	i++
	for {
		i = skipSpace(b, i)
		if i >= len(b) || b[i] != '"' {
			return nil, arr, false
		}
		ke := skipString(b, i)
		if ke < 0 {
			return nil, arr, false
		}
		key := b[i+1 : ke-1]
		i = skipSpace(b, ke)
		if i >= len(b) || b[i] != ':' {
			return nil, arr, false
		}
		i = skipSpace(b, i+1)
		if string(key) == "entries" {
			if i >= len(b) || b[i] != '[' {
				return nil, arr, false
			}
			arr[0] = i
			j := skipSpace(b, i+1)
			if j < len(b) && b[j] == ']' {
				arr[1] = j + 1
				return spans, arr, true
			}
			for {
				e := skipValue(b, j)
				if e < 0 {
					return nil, arr, false
				}
				spans = append(spans, [2]int{j, e})
				j = skipSpace(b, e)
				if j >= len(b) {
					return nil, arr, false
				}
				if b[j] == ']' {
					arr[1] = j + 1
					return spans, arr, true
				}
				if b[j] != ',' {
					return nil, arr, false
				}
				j = skipSpace(b, j+1)
			}
		}
		e := skipValue(b, i)
		if e < 0 {
			return nil, arr, false
		}
		i = skipSpace(b, e)
		if i >= len(b) || b[i] != ',' {
			return nil, arr, false
		}
		i++
	}
}
