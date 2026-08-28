// Command differential runs the same wire-level scenarios against the
// queen-kafka facade and against a real single-node Apache Kafka, and diffs
// what a client can OBSERVE: error codes, offsets, watermarks, response shapes.
//
// It is the M6 oracle from PLAN_QUEEN_KAFKA.md. What it deliberately does not
// diff: timings, node ids, hosts and ports, throttle values, cluster ids and
// anything the broker generates freely (member ids, coordinator identity). A
// facade that answered those identically would be lying about being itself.
//
//	queen-kafka/compat/differential/rig-diff.sh run          # stack + runner
//	QK_FACADE=host:port QK_KAFKA=host:port go run .          # against a live pair
//	... -run group                                           # one scenario
//
// Exit status is 1 when a divergence appears that is not in the deliberate
// deviation table at the bottom of this file, so the runner can become a gate
// later without being one today.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ------------------------------------------------------------------ recording

type observation struct {
	key  string
	val  string
	info bool
}

// recorder collects one target's answers for one scenario. Keys are stable
// strings; every key a scenario records against one broker it must also record
// against the other, or the diff reports the absence — which is itself a
// finding, and the reason nothing here is recorded conditionally on success.
type recorder struct {
	obs   []observation
	index map[string]int
}

func newRecorder() *recorder { return &recorder{index: map[string]int{}} }

func (r *recorder) put(key, val string, info bool) {
	if i, ok := r.index[key]; ok {
		r.obs[i].val = val
		return
	}
	r.index[key] = len(r.obs)
	r.obs = append(r.obs, observation{key: key, val: val, info: info})
}

// add records an observation that is diffed.
func (r *recorder) add(key, format string, args ...any) {
	r.put(key, fmt.Sprintf(format, args...), false)
}

// info records an observation that is printed side by side but never diffed:
// something legitimately broker-specific (a node id, a count of supported
// APIs) that is still worth having in the report.
func (r *recorder) info(key, format string, args ...any) {
	r.put(key, fmt.Sprintf(format, args...), true)
}

// bad records a failure. It is diffed like anything else: an error on one side
// and an answer on the other is the loudest divergence there is.
func (r *recorder) bad(key string, err error) {
	r.put(key, "ERROR: "+err.Error(), false)
}

// ------------------------------------------------------------------- scenarios

type target struct {
	label string // "facade" or "kafka"
	addr  string
}

func (t *target) dial() (*conn, error) { return dial(t.label, t.addr) }

type runctx struct {
	target *target
	rec    *recorder
	runID  string
	baseTS int64
	parts  int32
}

// topic returns a per-run topic name, identical on both brokers so the two
// recordings line up, distinct between runs so a second run against a stack
// that is still up starts from empty partitions.
func (c *runctx) topic(suffix string) string {
	return fmt.Sprintf("dt-%s-%s", c.runID, suffix)
}

func (c *runctx) group(suffix string) string {
	return fmt.Sprintf("dg-%s-%s", c.runID, suffix)
}

type scenario struct {
	name string
	desc string
	run  func(c *runctx)
}

var scenarios []scenario

// ---------------------------------------------------------------------- diff

type divergence struct {
	scenario string
	key      string
	facade   string
	kafka    string
	verdict  string
	why      string
}

func main() {
	facadeAddr := envOr("QK_FACADE", "127.0.0.1:29192")
	kafkaAddr := envOr("QK_KAFKA", "127.0.0.1:29092")
	only := flag.String("run", "", "regexp: only scenarios whose name matches")
	quiet := flag.Bool("quiet", false, "print divergences only, not every observation")
	flag.Parse()

	runID := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond)%100000000)
	// One base timestamp for both targets: record timestamps are then
	// directly comparable between the two brokers instead of being noise.
	baseTS := time.Now().UnixMilli() - 60_000

	facade := &target{label: "facade", addr: facadeAddr}
	kafka := &target{label: "kafka", addr: kafkaAddr}

	fmt.Printf("queen-kafka M6 differential\n")
	fmt.Printf("  facade  %s\n  kafka   %s\n  run id  %s\n  base ts %d\n\n",
		facadeAddr, kafkaAddr, runID, baseTS)

	var filter *regexp.Regexp
	if *only != "" {
		var err error
		filter, err = regexp.Compile(*only)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad -run regexp: %v\n", err)
			os.Exit(2)
		}
	}

	// A fixed order rather than the order the files happened to register in.
	// It is load-bearing in one place: the metadata scenario asks about
	// __consumer_offsets, which on Kafka does not exist until a group has
	// committed, so the group scenario has to have run.
	order := map[string]int{
		"produce-consume": 0, "listoffsets": 1, "fetch-bounds": 2,
		"group": 3, "metadata": 4, "apiversions": 5, "extras": 6,
	}
	sort.SliceStable(scenarios, func(i, j int) bool {
		oi, oki := order[scenarios[i].name]
		oj, okj := order[scenarios[j].name]
		if !oki || !okj {
			return oki && !okj
		}
		return oi < oj
	})

	var divs []divergence
	ran := 0
	for _, s := range scenarios {
		if filter != nil && !filter.MatchString(s.name) {
			continue
		}
		ran++
		fmt.Printf("=========================================================== %s\n", s.name)
		if s.desc != "" {
			fmt.Printf("            %s\n", s.desc)
		}
		recs := map[string]*recorder{}
		for _, t := range []*target{facade, kafka} {
			r := newRecorder()
			recs[t.label] = r
			c := &runctx{target: t, rec: r, runID: runID, baseTS: baseTS, parts: 8}
			func() {
				defer func() {
					if p := recover(); p != nil {
						r.bad("panic", fmt.Errorf("%v", p))
					}
				}()
				s.run(c)
			}()
		}
		divs = append(divs, report(s.name, recs["facade"], recs["kafka"], *quiet)...)
		fmt.Println()
	}

	if ran == 0 {
		fmt.Fprintln(os.Stderr, "no scenario matched")
		os.Exit(2)
	}

	fmt.Printf("=========================================================== SUMMARY\n")
	counts := map[string]int{}
	for _, d := range divs {
		counts[d.verdict]++
	}
	nReview := counts["REVIEW"]
	fmt.Printf("%d divergence(s): %d deliberate, %d accepted as harmless, %d to classify by hand\n\n",
		len(divs), counts["DELIBERATE"], counts["ACCEPTED"], nReview)
	for _, want := range []string{"REVIEW", "DELIBERATE", "ACCEPTED"} {
		for _, d := range divs {
			if d.verdict != want {
				continue
			}
			fmt.Printf("[%s] %s / %s\n", d.verdict, d.scenario, d.key)
			fmt.Printf("        facade = %s\n", d.facade)
			fmt.Printf("        kafka  = %s\n", d.kafka)
			if d.why != "" {
				fmt.Printf("        why    = %s\n", d.why)
			}
		}
	}
	if nReview > 0 {
		os.Exit(1)
	}
}

func report(name string, f, k *recorder, quiet bool) []divergence {
	if f == nil || k == nil {
		panic("a scenario recorded nothing for one of the targets")
	}
	// The union of the keys, in the order the facade recorded them, then any
	// the facade never produced.
	var keys []string
	seen := map[string]bool{}
	for _, o := range f.obs {
		keys = append(keys, o.key)
		seen[o.key] = true
	}
	var extra []string
	for _, o := range k.obs {
		if !seen[o.key] {
			extra = append(extra, o.key)
		}
	}
	sort.Strings(extra)
	keys = append(keys, extra...)

	var divs []divergence
	for _, key := range keys {
		fo, fok := get(f, key)
		ko, kok := get(k, key)
		switch {
		case fok && kok && fo.info:
			if !quiet {
				fmt.Printf("  i %-52s facade=%s kafka=%s\n", key, fo.val, ko.val)
			}
		case fok && kok && fo.val == ko.val:
			if !quiet {
				fmt.Printf("  = %-52s %s\n", key, fo.val)
			}
		default:
			fv, kv := "<not recorded>", "<not recorded>"
			if fok {
				fv = fo.val
			}
			if kok {
				kv = ko.val
			}
			verdict, why := classify(name, key)
			fmt.Printf("  %s %-52s\n        facade = %s\n        kafka  = %s\n",
				map[string]string{"DELIBERATE": "d", "ACCEPTED": "a", "REVIEW": "!"}[verdict], key, fv, kv)
			divs = append(divs, divergence{
				scenario: name, key: key, facade: fv, kafka: kv, verdict: verdict, why: why,
			})
		}
	}
	return divs
}

func get(r *recorder, key string) (observation, bool) {
	i, ok := r.index[key]
	if !ok {
		return observation{}, false
	}
	return r.obs[i], true
}

// ------------------------------------------------------- deliberate deviations

// The list from the queen-kafka README and the crate's own comments, as
// patterns over "scenario/key". A divergence that matches one of these is
// reported as DELIBERATE and does not fail the run; everything else lands in
// REVIEW and does.
var deliberate = []classification{
	{regexp.MustCompile(`^listoffsets/ts\.concrete\.`),
		"documented: ListOffsets with a concrete timestamp answers -1 with no error"},
	{regexp.MustCompile(`^listoffsets/nonexistent\.`),
		"documented minor: a concrete-timestamp probe on a topic that does not exist is answered as if it did"},
	{regexp.MustCompile(`^metadata/internal(_autocreate)?\.`),
		"documented: __-prefixed topics are hidden and refused everywhere"},
	{regexp.MustCompile(`^metadata/alltopics\.has_consumer_offsets$`),
		"documented: __-prefixed topics are hidden; the facade has no __consumer_offsets to hide either"},
	{regexp.MustCompile(`^extras/initproducerid\.`),
		"non-goal in PLAN_QUEEN_KAFKA.md: no transactions, no EOS, so InitProducerId is not implemented"},
}

type classification struct {
	pat *regexp.Regexp
	why string
}

// Divergences the M6 pass looked at and judged harmless: real differences in
// what a client sees, none of which changes what a client can DO. They are
// listed here rather than left in REVIEW so that a later run fails only on
// something new — and each one carries the reason it was let through, so the
// judgement can be argued with instead of being invisible.
var accepted = []classification{
	{regexp.MustCompile(`^produce-consume/\w+\.fetch\.batch0\.(codec|last_offset_delta|max_timestamp|num_records_field)$`),
		"the facade re-batches on the fetch path (two produced batches come back as one) and does not re-compress; " +
			"batch boundaries and the wire codec are the broker's choice in Kafka too, and every record, key, header, " +
			"timestamp and offset is identical"},
	{regexp.MustCompile(`^(listoffsets/v5\.latest|metadata/known_v9\.p0)\.leader_epoch$`),
		"the facade has no leader epochs to report; -1 is Kafka's own 'unknown epoch' sentinel and clients skip " +
			"epoch validation on it rather than misbehaving"},
	{regexp.MustCompile(`^extras/produce_v2\.`),
		"Produce v2 is below the advertised floor (versions.rs offers 3-9) and the facade closes the connection, " +
			"which is what Kafka itself does with a version it does not know — metadata_v99 closes identically on " +
			"both sides. Kafka 3.9 merely still supports v2"},
	{regexp.MustCompile(`^produce-consume/\w+\.produce1\.log_start_offset$`),
		"the produce answer carries no log start offset (-1, the field's own default in Kafka's schema and the " +
			"Java client's) because a push reports offsets and statuses and no lower bound: filling it in would " +
			"cost a bounds probe per produce, on the write path, for a field only the idempotent producer reads " +
			"— and that producer is refused outright (handlers::produce::refuse). The fetch path reports the real " +
			"log start on both sides, which is where a client looks for it"},
}

func classify(scenarioName, key string) (string, string) {
	full := scenarioName + "/" + key
	for _, d := range deliberate {
		if d.pat.MatchString(full) {
			return "DELIBERATE", d.why
		}
	}
	for _, a := range accepted {
		if a.pat.MatchString(full) {
			return "ACCEPTED", a.why
		}
	}
	return "REVIEW", ""
}

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
