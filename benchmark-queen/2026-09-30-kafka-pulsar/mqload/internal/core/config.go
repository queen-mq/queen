package core

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config holds the workload flags common to kload and pload (SPEC.md §2, §4).
type Config struct {
	Rate        float64 // offered msg/s for THIS process (0 = no producers)
	Ramp        time.Duration
	Duration    time.Duration // whole producing time, ramp included
	Report      time.Duration
	Drain       time.Duration
	StartFile   string
	StartAt     int64 // unix ms
	MaxInflight int   // in-flight UNITS; over it a unit is shed

	Mode     string // batch | keyed
	Batch    int
	BatchMax int

	Topics       int
	Topic        string // topic name (T=1) or prefix (<prefix>-t<i>)
	Partitions   int
	Entities     int
	Dist         string // rr | rotate | scatter | zipf
	Active       int
	ActivePolicy string // rotate | scatter (with -dist rr and -active N)
	ZipfS        float64
	TopicDist    string // rr | zipf
	TopicZipfS   float64
	Payload      int

	Consumers   int
	ConsOffset  int
	ConsTotal   int
	Poll        int
	ProcUs      float64
	Ack         string // async | sync | none
	AckInflight int

	LoaderIndex int
	Loaders     int
	Create      bool
	CreateOnly  bool
	Warm        bool
	Out         string
	Tag         string
	Seed        int64
	LocalSrc    string
	Pprof       string

	// Producer sharding (set by pload's -producer-shard, never by kload): this
	// process produces only to the partitions p with p % ShardCount ==
	// ShardIndex, and the picker walks only that subset. 0 = off.
	ShardIndex int
	ShardCount int

	localSrc map[int]bool
	fs       *flag.FlagSet
}

// secDuration is a time.Duration flag that also accepts a bare number of
// seconds ("60" == "60s"), so run scripts can pass either form.
type secDuration struct{ d *time.Duration }

func (s secDuration) String() string {
	if s.d == nil {
		return "0s"
	}
	return s.d.String()
}

func (s secDuration) Set(v string) error {
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		*s.d = time.Duration(f * float64(time.Second))
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	*s.d = d
	return nil
}

// DurationVar registers a duration flag that accepts "60", "60s", "1m", "500ms".
func DurationVar(fs *flag.FlagSet, p *time.Duration, name string, def time.Duration, usage string) {
	*p = def
	fs.Var(secDuration{p}, name, usage+" (a Go duration, or a bare number of seconds)")
}

// Register adds the common flags to fs.
func (c *Config) Register(fs *flag.FlagSet) {
	c.fs = fs
	fs.Float64Var(&c.Rate, "rate", 0, "offered msg/s for THIS process, open loop (0 = no producers)")
	DurationVar(fs, &c.Ramp, "ramp", 10*time.Second, "linear ramp of the offered rate from 0 (goload's F^-1 schedule)")
	DurationVar(fs, &c.Duration, "duration", 70*time.Second, "whole producing time, ramp included")
	DurationVar(fs, &c.Report, "report", 10*time.Second, "window line every this long (from the start instant)")
	DurationVar(fs, &c.Drain, "drain", 0, "after -duration, consumers keep consuming this long")
	fs.StringVar(&c.StartFile, "start-file", "", "start barrier: after READY, poll this file every 100 ms; its content = the start instant (unix ms)")
	fs.Int64Var(&c.StartAt, "start-at", 0, "start barrier: start instant (unix ms)")
	fs.IntVar(&c.MaxInflight, "max-inflight", 5000, "in-flight UNITS; a unit scheduled while the cap is full is shed (offered+shed, never sent)")
	fs.StringVar(&c.Mode, "mode", "batch", "batch (a unit = -batch msgs of one entity back to back) | keyed (a unit = -batch..-batch-max msgs of one entity; the client batches across entities)")
	fs.IntVar(&c.Batch, "batch", 0, "messages per unit (default 100 in batch mode, 1 in keyed mode)")
	fs.IntVar(&c.BatchMax, "batch-max", 0, "if > -batch: unit size drawn uniformly in [batch, batch-max]")
	fs.IntVar(&c.Topics, "topics", 1, "number of topics T (names: -topic if T=1, else <topic>-t<i>)")
	fs.StringVar(&c.Topic, "topic", "bench", "topic name (T=1) or prefix")
	fs.IntVar(&c.Partitions, "partitions", 200, "partitions per topic (Pulsar: 0 = non-partitioned topic)")
	fs.IntVar(&c.Entities, "entities", 0, "entities per topic (0 = one entity per partition, entity e IS partition e; >0 = message key e<id>)")
	fs.StringVar(&c.Dist, "dist", "rr", "entity picker: rr | rotate | scatter | zipf")
	fs.IntVar(&c.Active, "active", 0, "rotate/scatter: distinct entities per second (goload -active-partitions)")
	fs.StringVar(&c.ActivePolicy, "active-policy", "rotate", "with -dist rr and -active N: rotate | scatter (goload -active-policy)")
	fs.Float64Var(&c.ZipfS, "zipf-s", 1.1, "Zipf exponent over entities (-dist zipf)")
	fs.StringVar(&c.TopicDist, "topic-dist", "rr", "topic picker: rr | zipf")
	fs.Float64Var(&c.TopicZipfS, "topic-zipf-s", 1.0, "Zipf exponent over topics (-topic-dist zipf)")
	fs.IntVar(&c.Payload, "payload", 256, "target JSON event size in bytes (goload's jsonEvent)")
	fs.IntVar(&c.Consumers, "consumers", 4, "consumers in this process (0 = none)")
	fs.IntVar(&c.ConsOffset, "cons-offset", 0, "global index of this process's first consumer")
	fs.IntVar(&c.ConsTotal, "cons-total", 0, "consumers across all processes (0 = -consumers)")
	fs.IntVar(&c.Poll, "poll", 1000, "max records per poll (Kafka PollRecords; Pulsar: drained from the receive channel per loop)")
	fs.Float64Var(&c.ProcUs, "proc-us", 0, "simulated processing time per message, µs (accumulated, slept in >= 1 ms chunks)")
	fs.StringVar(&c.Ack, "ack", "async", "Kafka offset commit per poll: async | sync | none (Pulsar: Ack per message, none = never ack)")
	fs.IntVar(&c.AckInflight, "ack-inflight", 256, "cap on in-flight async commits per process (full = block, never shed)")
	fs.IntVar(&c.LoaderIndex, "loader-index", 0, "this process's index (0..loaders-1): the \"src\" in every message and the rr start offset")
	fs.IntVar(&c.Loaders, "loaders", 1, "number of load processes sharing the topics")
	fs.BoolVar(&c.Create, "create", true, "create the topics (idempotent: existing topics are checked, not recreated)")
	fs.BoolVar(&c.CreateOnly, "create-only", false, "create (or wait for) the topics, optionally -warm, then exit")
	fs.BoolVar(&c.Warm, "warm", false, "after the topics are ready: write ONE message without \"ts\" into every partition (and consume it: Pulsar)")
	fs.StringVar(&c.Out, "out", "", "write the final numbers + config as JSON to this file")
	fs.StringVar(&c.Tag, "tag", "", "free-form run tag (echoed in the header and JSON)")
	fs.Int64Var(&c.Seed, "seed", 0, "RNG seed for payloads, phases and pickers (0 = time based); the process seed is seed+loader_index")
	fs.StringVar(&c.Pprof, "pprof", "", "serve net/http/pprof on this address (e.g. 127.0.0.1:6060); empty = off")
	fs.StringVar(&c.LocalSrc, "local-src", "", "loader indices running on THIS host, e.g. 0-2 or 0,3,6 (e2e_local = messages from these srcs; default: this process only)")
}

// IsSet reports whether the flag was given on the command line.
func (c *Config) IsSet(name string) bool {
	set := false
	c.fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// Finalize applies mode-dependent defaults and validates.
func (c *Config) Finalize() error {
	switch c.Mode {
	case "batch":
		if c.Batch <= 0 {
			c.Batch = 100
		}
	case "keyed":
		if c.Batch <= 0 {
			c.Batch = 1
		}
	default:
		return fmt.Errorf("-mode %q: want batch|keyed", c.Mode)
	}
	if c.BatchMax != 0 && c.BatchMax < c.Batch {
		return fmt.Errorf("-batch-max %d < -batch %d", c.BatchMax, c.Batch)
	}
	if c.Topics < 1 {
		return fmt.Errorf("-topics must be >= 1")
	}
	if c.Partitions < 0 || c.Entities < 0 {
		return fmt.Errorf("-partitions and -entities must be >= 0")
	}
	if c.Loaders < 1 || c.LoaderIndex < 0 || c.LoaderIndex >= c.Loaders {
		return fmt.Errorf("need 0 <= -loader-index (%d) < -loaders (%d)", c.LoaderIndex, c.Loaders)
	}
	if c.Consumers < 0 || c.ConsOffset < 0 {
		return fmt.Errorf("-consumers and -cons-offset must be >= 0")
	}
	if c.ConsTotal <= 0 {
		c.ConsTotal = c.ConsOffset + c.Consumers
	}
	if c.Consumers > 0 && c.ConsOffset+c.Consumers > c.ConsTotal {
		return fmt.Errorf("-cons-offset %d + -consumers %d > -cons-total %d", c.ConsOffset, c.Consumers, c.ConsTotal)
	}
	switch c.Ack {
	case "async", "sync", "none":
	default:
		return fmt.Errorf("-ack %q: want async|sync|none", c.Ack)
	}
	if c.Poll < 1 {
		c.Poll = 1
	}
	if c.AckInflight < 1 {
		c.AckInflight = 1
	}
	if c.MaxInflight < 1 {
		return fmt.Errorf("-max-inflight must be >= 1")
	}
	if c.Rate < 0 {
		return fmt.Errorf("-rate must be >= 0")
	}
	if c.Ramp > c.Duration {
		c.Ramp = c.Duration
	}
	if c.Report <= 0 {
		c.Report = 10 * time.Second
	}
	if c.Payload < 16 {
		c.Payload = 16
	}
	c.localSrc = map[int]bool{c.LoaderIndex: true}
	if c.LocalSrc != "" {
		m, err := parseIntSet(c.LocalSrc)
		if err != nil {
			return fmt.Errorf("-local-src: %v", err)
		}
		c.localSrc = m
	}
	return nil
}

// parseIntSet parses "0-2,5,7" into a set.
func parseIntSet(s string) (map[int]bool, error) {
	m := map[int]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			return nil, err
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				return nil, err
			}
		}
		for i := a; i <= b; i++ {
			m[i] = true
		}
	}
	return m, nil
}

// Space is the entity space per topic: -entities, else -partitions (min 1).
func (c *Config) Space() uint64 {
	if c.Entities > 0 {
		return uint64(c.Entities)
	}
	if c.Partitions > 0 {
		return uint64(c.Partitions)
	}
	return 1
}

// Sharded reports whether this process produces to a partition shard only.
func (c *Config) Sharded() bool { return c.ShardCount > 1 && c.Entities == 0 && c.Partitions > 0 }

// ShardPartitions returns the partitions {p : p % n == i} of a P-partition
// topic, ascending (producer sharding: process i of n).
func ShardPartitions(i, n, P int) []int {
	var out []int
	for p := i; p < P; p += n {
		out = append(out, p)
	}
	return out
}

// PickSpace is the space the picker walks: the entity space, or with
// producer sharding the number of partitions of this process's shard.
func (c *Config) PickSpace() uint64 {
	if c.Sharded() {
		return uint64(len(ShardPartitions(c.ShardIndex, c.ShardCount, c.Partitions)))
	}
	return c.Space()
}

// ConsumerPartitions returns the partitions of topic t that consumer ci owns
// when consumers split partitions like a Kafka group (Pulsar failover and
// exclusive): the consumers reading t per the §2 rule are ranked (cons-total
// >= T: members ci = t, t+T, t+2T, ... with rank ci/T; cons-total < T: ci is
// t's only reader) and partition p belongs to rank p % members. With T=1 this
// is {p : p % cons-total == ci}. Every partition of every topic is owned by
// exactly one consumer; consumers ranked >= P own nothing.
func (c *Config) ConsumerPartitions(ci, t int) []int {
	n, rank := 1, 0
	if c.ConsTotal >= c.Topics {
		if ci%c.Topics != t {
			return nil
		}
		n, rank = c.MembersOfTopic(t), ci/c.Topics
	} else if t%c.ConsTotal != ci {
		return nil
	}
	var out []int
	for p := rank; p < c.Partitions; p += n {
		out = append(out, p)
	}
	return out
}

// Keyed reports whether messages carry the key e<id> (-entities > 0).
func (c *Config) Keyed() bool { return c.Entities > 0 }

// MaxUnit is the largest unit size in messages.
func (c *Config) MaxUnit() int {
	if c.BatchMax > c.Batch {
		return c.BatchMax
	}
	return c.Batch
}

// AvgUnit is the mean unit size in messages.
func (c *Config) AvgUnit() float64 {
	if c.BatchMax > c.Batch {
		return float64(c.Batch+c.BatchMax) / 2
	}
	return float64(c.Batch)
}

// UnitsPerSec is the offered unit rate at full rate.
func (c *Config) UnitsPerSec() float64 { return c.Rate / c.AvgUnit() }

// TopicName returns the name of topic i.
func (c *Config) TopicName(i int) string {
	if c.Topics == 1 {
		return c.Topic
	}
	return fmt.Sprintf("%s-t%d", c.Topic, i)
}

// TopicNames returns all topic names.
func (c *Config) TopicNames() []string {
	out := make([]string, c.Topics)
	for i := range out {
		out[i] = c.TopicName(i)
	}
	return out
}

// ConsumerTopics is the SPEC §2 assignment rule: if cons-total >= T, consumer
// ci reads topic ci % T; else it reads {t : t % cons-total == ci}.
func ConsumerTopics(ci, consTotal, topics int) []int {
	if consTotal >= topics {
		return []int{ci % topics}
	}
	var ts []int
	for t := ci; t < topics; t += consTotal {
		ts = append(ts, t)
	}
	return ts
}

// ConsumerTopics applies the rule with this config.
func (c *Config) ConsumerTopics(ci int) []int { return ConsumerTopics(ci, c.ConsTotal, c.Topics) }

// SharedByTopic reports whether consumers share per-topic groups/subscriptions
// (cons-total >= T) rather than each owning a topic slice.
func (c *Config) SharedByTopic() bool { return c.ConsTotal >= c.Topics }

// MembersOfTopic is how many consumers (across all processes) read topic t
// when cons-total >= T.
func (c *Config) MembersOfTopic(t int) int {
	if c.ConsTotal < c.Topics {
		return 1
	}
	return (c.ConsTotal - t + c.Topics - 1) / c.Topics
}

// IsLocalSrc reports whether messages from loader index src were produced on
// this host (e2e_local: clock-skew free).
func (c *Config) IsLocalSrc(src int) bool { return c.localSrc[src] }

// LocalSrcList returns the local srcs, sorted.
func (c *Config) LocalSrcList() []int {
	out := make([]int, 0, len(c.localSrc))
	for s := range c.localSrc {
		out = append(out, s)
	}
	sort.Ints(out)
	return out
}

// Flags returns every flag of the set with its effective value.
func (c *Config) Flags() map[string]string {
	m := map[string]string{}
	c.fs.VisitAll(func(f *flag.Flag) { m[f.Name] = f.Value.String() })
	m["batch"] = strconv.Itoa(c.Batch)
	m["cons-total"] = strconv.Itoa(c.ConsTotal)
	return m
}

// ProcessSeed returns the effective seed of this process.
func (c *Config) ProcessSeed() int64 {
	if c.Seed == 0 {
		return time.Now().UnixNano() ^ int64(os.Getpid())<<20 ^ int64(c.LoaderIndex)
	}
	return c.Seed + int64(c.LoaderIndex)
}
