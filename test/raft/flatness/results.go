package main

// The RESULTS table: the file a regime run writes and the comparator reads.
//
// One run of one regime against one store state produces one file. The
// comparison of §13.6 is then a comparison of two FILES — an empty store and a
// preloaded one — which means a flatness result can be re-judged later, by a
// newer comparator, without re-running anything. That is the point: the 300 M
// preload is hours of work, and "we compared it at the time" is not evidence.
//
// FORMAT (text, deliberately boring, greppable, diffable):
//
//	# regime=A20k state=preloaded host=vm-164.90.215.224 commit=abc1234
//	# started=2026-09-17T09:00:00Z duration_s=3600 topology=raft3
//	# command=goload -openloop -rate 20000 -partitions 1000000 …
//	p50_ms              18.9
//	p99_ms              41.2
//	p999_ms            120.0
//	ack_rtt_ms          22.4
//	cpu_cores            3.10
//	disk_mbps          118.0
//	durable_point_ms     4.8
//	snapshot_build_s    41.0
//	rss_start_mb      1024.0
//	rss_end_mb        1061.0
//
// Rules:
//   - `# key=value` lines are the header; everything else is `metric value`.
//   - Unknown metrics are KEPT and compared if both sides have them; a metric
//     that only one side has is an ERROR, never a silent skip (the two runs
//     would not be comparable).
//   - Values are plain float64. Units live in the metric NAME (`_ms`, `_mb`,
//     `_mbps`, `_s`, `cores`), because a unit column is a unit that drifts.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Results is one run's table.
type Results struct {
	Path    string
	Header  map[string]string
	Metrics map[string]float64
	Order   []string // metric names in file order, for stable printing
}

func NewResults() *Results {
	return &Results{Header: map[string]string{}, Metrics: map[string]float64{}}
}

// Required header keys. A RESULTS file without them cannot be quoted in
// RAFT_STATUS.md (§0.3: numbers come with the host, the commit and the command).
var RequiredHeader = []string{"regime", "state", "host", "commit"}

// ParseResults reads one table. It is strict about the header and about number
// parsing, and tolerant about blank lines and spacing.
func ParseResults(path string, r io.Reader) (*Results, error) {
	res := NewResults()
	res.Path = path
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		if strings.HasPrefix(text, "#") {
			for _, kv := range splitHeader(strings.TrimPrefix(text, "#")) {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					continue
				}
				res.Header[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
			continue
		}
		fields := strings.Fields(text)
		if len(fields) < 2 {
			return nil, fmt.Errorf("%s:%d: %q is not `metric value`", path, line, text)
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: metric %q has a non-numeric value %q", path, line, fields[0], fields[1])
		}
		if _, dup := res.Metrics[fields[0]]; dup {
			return nil, fmt.Errorf("%s:%d: metric %q appears twice", path, line, fields[0])
		}
		res.Metrics[fields[0]] = v
		res.Order = append(res.Order, fields[0])
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	var missing []string
	for _, k := range RequiredHeader {
		if res.Header[k] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: the header does not carry %s "+
			"(a number without its host and commit cannot go into RAFT_STATUS.md)", path, strings.Join(missing, ", "))
	}
	if len(res.Metrics) == 0 {
		return nil, fmt.Errorf("%s: no metrics", path)
	}
	return res, nil
}

// splitHeader turns `# a=1 b="two words" c=3` into its pairs. Quoting matters
// for `command=…`, which is the field a reader most wants to copy.
func splitHeader(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
		case r == ' ' && !inQuote:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func LoadResults(path string) (*Results, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseResults(path, f)
}

// Write renders a table in the canonical format, so the runner and the tests
// produce byte-identical files.
func (r *Results) Write(w io.Writer) error {
	keys := make([]string, 0, len(r.Header))
	for k := range r.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var head []string
	for _, k := range keys {
		v := r.Header[k]
		if strings.ContainsRune(v, ' ') {
			v = `"` + v + `"`
		}
		head = append(head, k+"="+v)
	}
	if _, err := fmt.Fprintf(w, "# %s\n", strings.Join(head, " ")); err != nil {
		return err
	}
	names := r.Order
	if len(names) != len(r.Metrics) {
		names = names[:0]
		for k := range r.Metrics {
			names = append(names, k)
		}
		sort.Strings(names)
	}
	for _, n := range names {
		if _, err := fmt.Fprintf(w, "%-20s %g\n", n, r.Metrics[n]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Results) String() string {
	var b strings.Builder
	_ = r.Write(&b)
	return b.String()
}
