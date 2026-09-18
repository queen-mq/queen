package main

// Configuration and CLI surface.
//
// Two rules shape everything here:
//
//  1. A run is reproducible from its SEED plus this config and nothing else.
//     The seed selects the operation sequence; the config selects the shape of
//     the world the sequence runs in. A failing seed becomes a regression
//     fixture (§13.4), and a fixture is only worth keeping if replaying it
//     replays the same operations.
//
//  2. Names are per-run by default. The broker deduplicates transaction ids for
//     `dedupWindowSeconds` (3600 by default) and a consumer group carries a
//     cursor no DELETE resets, so fixed names would make the second run inside
//     an hour see its pushes refused as duplicates and its pops start from
//     somebody else's cursor — green once, red for an hour. `-run-id` pins the
//     suffix when a fixture has to be replayed against a fresh pair of brokers.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config is the whole run.
type Config struct {
	URLA    string // side A: the oracle, a postgres broker (test/run.sh `single`)
	URLB    string // side B: the system under test, a raft broker (`raft1`)
	NameA   string
	NameB   string
	Tenant  string
	Token   string
	Timeout time.Duration

	Seed    int64
	Ops     int
	RunID   string
	Mix     Mix
	Queues  int
	Parts   int
	Groups  int
	DupRate int // percent of pushes that deliberately reuse a live transactionId

	Namespace  string // KV namespace and queue namespace prefix for this run
	OutDir     string // where a divergence report is written ("" = stdout only)
	StopOnDiff bool
	Verbose    bool
}

// Mix is the operation mix: weights by operation kind. Weights are integers so
// that a mix is quotable in a bug report and stable across Go versions.
type Mix map[OpKind]int

func (m Mix) String() string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[OpKind(k)]))
	}
	return strings.Join(parts, ",")
}

func (m Mix) total() int {
	t := 0
	for _, w := range m {
		t += w
	}
	return t
}

// DefaultMix is the FIRST mix of §13.4: push, pop, ack. The remaining kinds are
// declared in ops.go with weight 0 and a stub planner, so `-mix txn=10` names a
// real kind and fails loudly ("not implemented") instead of being a typo the
// flag parser accepts.
func DefaultMix() Mix {
	return Mix{OpPush: 50, OpPop: 30, OpAck: 20}
}

// ParseMix reads "push=50,pop=30,ack=20".
func ParseMix(s string) (Mix, error) {
	m := Mix{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("mix item %q is not kind=weight", item)
		}
		kind := OpKind(strings.TrimSpace(k))
		if !KnownKind(kind) {
			return nil, fmt.Errorf("unknown operation kind %q (known: %s)", k, strings.Join(AllKindNames(), " "))
		}
		w, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || w < 0 {
			return nil, fmt.Errorf("mix weight for %q must be a non-negative integer, got %q", k, v)
		}
		if w > 0 {
			m[kind] = w
		}
	}
	if m.total() == 0 {
		return nil, errors.New("mix has no operation with a positive weight")
	}
	return m, nil
}

// Validate refuses a config that cannot produce a meaningful comparison.
func (c *Config) Validate() error {
	if c.URLA == "" || c.URLB == "" {
		return errors.New("both -a and -b are required (side A = postgres oracle, side B = raft)")
	}
	if c.URLA == c.URLB {
		return errors.New("-a and -b point at the same broker: a run would compare a broker with itself")
	}
	if c.Ops <= 0 {
		return errors.New("-ops must be > 0")
	}
	if c.Queues <= 0 || c.Parts <= 0 || c.Groups <= 0 {
		return errors.New("-queues, -partitions and -groups must be > 0")
	}
	if c.DupRate < 0 || c.DupRate > 100 {
		return errors.New("-dup-rate is a percentage in [0,100]")
	}
	if c.Mix == nil || c.Mix.total() == 0 {
		return errors.New("empty operation mix")
	}
	for kind := range c.Mix {
		if !Implemented(kind) {
			return fmt.Errorf("operation kind %q is a documented stub in this skeleton (ops.go); implement it before giving it weight", kind)
		}
	}
	return nil
}

// ParseFlags builds a Config from argv. It is separate from main so that the
// tests can exercise the flag surface without running a fuzz.
func ParseFlags(args []string, stderr *os.File) (*Config, error) {
	fs := flag.NewFlagSet("difffuzz", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &Config{}
	var mixStr string
	fs.StringVar(&c.URLA, "a", "http://localhost:6632", "side A base URL: the postgres broker (the oracle)")
	fs.StringVar(&c.URLB, "b", "http://localhost:7632", "side B base URL: the raft broker (system under test)")
	fs.StringVar(&c.NameA, "name-a", "postgres", "label for side A in reports")
	fs.StringVar(&c.NameB, "name-b", "raft", "label for side B in reports")
	fs.StringVar(&c.Tenant, "tenant", "", "x-queen-tenant header (empty: untenanted)")
	fs.StringVar(&c.Token, "token", "", "bearer token (empty: auth off)")
	fs.DurationVar(&c.Timeout, "timeout", 30*time.Second, "per-request deadline")
	fs.Int64Var(&c.Seed, "seed", 0, "random seed; 0 picks one from the clock and prints it")
	fs.IntVar(&c.Ops, "ops", 200, "number of operations in the sequence")
	fs.StringVar(&c.RunID, "run-id", "", "suffix for queue/group/txn names (default: derived from the seed and the clock)")
	fs.StringVar(&mixStr, "mix", DefaultMix().String(), "operation mix, kind=weight,...")
	fs.IntVar(&c.Queues, "queues", 3, "number of queues in the world")
	fs.IntVar(&c.Parts, "partitions", 4, "partitions per queue")
	fs.IntVar(&c.Groups, "groups", 2, "consumer groups")
	fs.IntVar(&c.DupRate, "dup-rate", 15, "percent of pushes that deliberately reuse a live transactionId")
	fs.StringVar(&c.Namespace, "namespace", "difffuzz", "namespace prefix for queues and the KV namespace")
	fs.StringVar(&c.OutDir, "out", "", "directory for the divergence report (default: stdout only)")
	fs.BoolVar(&c.StopOnDiff, "stop-on-diff", true, "stop at the first divergence (false: keep going and collect)")
	fs.BoolVar(&c.Verbose, "v", false, "print every operation")
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	m, err := ParseMix(mixStr)
	if err != nil {
		return nil, err
	}
	c.Mix = m
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	if c.RunID == "" {
		c.RunID = fmt.Sprintf("%d-%d", c.Seed&0xffffff, time.Now().Unix()%100000)
	}
	return c, c.Validate()
}

const usage = `difffuzz — differential fuzzer for the raft storage class (PLAN_RAFT.md §13.4)

Runs one seeded random operation sequence over the wire against TWO brokers —
side A a postgres broker (the oracle, D22) and side B a raft broker — and
compares every normalized response and the final views.

  difffuzz -a http://localhost:6632 -b http://localhost:7632 -seed 12345 -ops 500

A failing seed is the bug report: rerun it with the same -seed, -ops, -mix and
-run-id and the same sequence replays.

Flags:
`
