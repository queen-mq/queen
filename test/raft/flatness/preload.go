package main

// The bulk preloader's CLI and plan (PLAN_RAFT.md §13.6, step 1):
//
//	Preload 300 million messages over 1 million partitions with a 1-hour dedup
//	window and retention off (a bulk loader in test/raft/flatness).
//
// This file implements the CLI, the validation, the plan and the estimate. The
// SEND LOOP is a documented stub (WP-0.7): a loader that pushes 300 M messages
// is a thing to write against a broker that exists and to measure while it
// runs, not to ship unexercised — and a half-written loader that silently loads
// 12 M instead of 300 M would poison every flatness number after it.
//
// What the CLI already decides, and why it matters more than the loop:
//
//   - RESUMABILITY. 300 M messages is hours. A preload that cannot resume is a
//     preload that gets restarted from zero after every hiccup, and in practice
//     never finishes. The state file records the last completed partition, and
//     `-resume` continues from it.
//   - THE SHAPE. 300 M over 1 M partitions is 300 per partition. The loader
//     walks partitions, not messages, so a resume is exact and the load is
//     spread the way the regimes expect.
//   - THE SETTINGS IT IMPOSES. Retention off and a 1-hour dedup window are not
//     the broker's defaults; the preloader configures them and PRINTS what it
//     configured. A preload that silently ran with retention on deletes the
//     very volume the test is about.

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// PreloadConfig is the whole preload.
type PreloadConfig struct {
	URL          string
	Tenant       string
	Token        string
	QueuePrefix  string
	Queues       int
	Partitions   int   // total, spread evenly over the queues
	Messages     int64 // total
	PayloadBytes int
	Batch        int
	Concurrency  int
	RateMsgsSec  int // 0 = as fast as the broker accepts
	DedupWindowS int
	RetentionOff bool
	StateFile    string
	Resume       bool
	AssumeRate   int // for the estimate only, msgs/s
	DryRun       bool
}

// Plan is what a preload would do, in numbers a reviewer can check.
type Plan struct {
	Messages             int64
	Queues               int
	Partitions           int
	MessagesPerPartition int64
	PartitionsPerQueue   int
	Batches              int64
	PayloadBytes         int64
	EstimatedSeconds     float64
	Configure            map[string]any
	Warnings             []string
}

func (c *PreloadConfig) Validate() error {
	if c.URL == "" {
		return errors.New("-url is required (the broker to preload)")
	}
	if c.Messages <= 0 {
		return errors.New("-messages must be > 0")
	}
	if c.Partitions <= 0 || c.Queues <= 0 {
		return errors.New("-queues and -partitions must be > 0")
	}
	if c.Partitions < c.Queues {
		return errors.New("-partitions is the TOTAL number of partitions and must be >= -queues")
	}
	if c.Messages < int64(c.Partitions) {
		return fmt.Errorf("-messages (%d) is smaller than -partitions (%d): some partitions would stay empty, "+
			"which is not the shape §13.6 asks for", c.Messages, c.Partitions)
	}
	if c.Batch <= 0 || c.Batch > 10000 {
		return errors.New("-batch must be in [1, 10000]")
	}
	if c.Concurrency <= 0 || c.Concurrency > 512 {
		return errors.New("-concurrency must be in [1, 512]")
	}
	if c.PayloadBytes < 0 {
		return errors.New("-payload-bytes must be >= 0")
	}
	if c.Resume && c.StateFile == "" {
		return errors.New("-resume needs -state to know where it stopped")
	}
	return nil
}

func (c *PreloadConfig) Plan() (*Plan, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	perPartition := c.Messages / int64(c.Partitions)
	p := &Plan{
		Messages:             c.Messages,
		Queues:               c.Queues,
		Partitions:           c.Partitions,
		MessagesPerPartition: perPartition,
		PartitionsPerQueue:   c.Partitions / c.Queues,
		Batches:              int64(math.Ceil(float64(c.Messages) / float64(c.Batch))),
		PayloadBytes:         c.Messages * int64(c.PayloadBytes),
		Configure: map[string]any{
			"retentionEnabled":   !c.RetentionOff,
			"dedupWindowSeconds": c.DedupWindowS,
		},
	}
	rate := c.RateMsgsSec
	if rate == 0 {
		rate = c.AssumeRate
	}
	if rate > 0 {
		p.EstimatedSeconds = float64(c.Messages) / float64(rate)
	}
	if !c.RetentionOff {
		p.Warnings = append(p.Warnings, "retention is ON: §13.6 preloads with retention OFF, or the volume "+
			"the test is about is deleted while it loads")
	}
	if c.DedupWindowS < 3600 {
		p.Warnings = append(p.Warnings, fmt.Sprintf("dedup window is %ds: §13.6 says one hour (3600), and the "+
			"window is what keeps the hash lists alive (D10)", c.DedupWindowS))
	}
	if c.Messages%int64(c.Partitions) != 0 {
		p.Warnings = append(p.Warnings, fmt.Sprintf("%d messages do not divide evenly over %d partitions: "+
			"%d partitions get one extra", c.Messages, c.Partitions, c.Messages%int64(c.Partitions)))
	}
	if c.StateFile == "" {
		p.Warnings = append(p.Warnings, "no -state file: a preload of this size that cannot resume gets "+
			"restarted from zero after any hiccup")
	}
	return p, nil
}

func (p *Plan) Text(c *PreloadConfig) string {
	var b strings.Builder
	fmt.Fprintf(&b, "preload plan (PLAN_RAFT.md §13.6 step 1)\n")
	fmt.Fprintf(&b, "  target        %s   tenant=%q\n", c.URL, c.Tenant)
	fmt.Fprintf(&b, "  messages      %s over %s partitions in %d queue(s)\n",
		human(p.Messages), human(int64(p.Partitions)), p.Queues)
	fmt.Fprintf(&b, "  shape         %d message(s) per partition, %d partition(s) per queue\n",
		p.MessagesPerPartition, p.PartitionsPerQueue)
	fmt.Fprintf(&b, "  batching      %s push(es) of %d, concurrency %d\n", human(p.Batches), c.Batch, c.Concurrency)
	fmt.Fprintf(&b, "  payload       %d B each, %s total (before framing, hashes and per-message overhead)\n",
		c.PayloadBytes, humanBytes(p.PayloadBytes))
	fmt.Fprintf(&b, "  configure     %v\n", p.Configure)
	if p.EstimatedSeconds > 0 {
		rate := c.RateMsgsSec
		src := "-rate"
		if rate == 0 {
			rate, src = c.AssumeRate, "-assume-rate"
		}
		fmt.Fprintf(&b, "  estimate      %s at %s msgs/s (%s) — an ESTIMATE, not a promise\n",
			dur(p.EstimatedSeconds), human(int64(rate)), src)
	} else {
		fmt.Fprintf(&b, "  estimate      none: pass -rate or -assume-rate to get one\n")
	}
	if c.StateFile != "" {
		fmt.Fprintf(&b, "  resume        state file %s (records the last completed partition)\n", c.StateFile)
	}
	for _, w := range p.Warnings {
		fmt.Fprintf(&b, "  WARNING       %s\n", w)
	}
	return b.String()
}

func human(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.3gG", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.4gM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.4gk", float64(n)/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func humanBytes(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v := float64(n)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.4g %s", v, units[i])
}

func dur(seconds float64) string {
	return time.Duration(seconds * float64(time.Second)).Round(time.Second).String()
}

// ParseCount reads 300m / 1g / 1_000_000 / 300000000.
func ParseCount(s string) (int64, error) {
	s = strings.TrimSpace(strings.ReplaceAll(s, "_", ""))
	if s == "" {
		return 0, errors.New("empty count")
	}
	mult := int64(1)
	switch last := s[len(s)-1]; last {
	case 'k', 'K':
		mult, s = 1_000, s[:len(s)-1]
	case 'm', 'M':
		mult, s = 1_000_000, s[:len(s)-1]
	case 'g', 'G':
		mult, s = 1_000_000_000, s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a count (try 300m, 1g, 1000000)", s)
	}
	if v < 0 {
		return 0, errors.New("count must be >= 0")
	}
	return int64(v * float64(mult)), nil
}

// ParsePreloadFlags builds the config. Separate from main so the tests can
// exercise the surface.
func ParsePreloadFlags(args []string, stderr *os.File) (*PreloadConfig, error) {
	fs := flag.NewFlagSet("preload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &PreloadConfig{}
	var messages, partitions string
	fs.StringVar(&c.URL, "url", "http://localhost:6632", "broker to preload")
	fs.StringVar(&c.Tenant, "tenant", "", "x-queen-tenant header")
	fs.StringVar(&c.Token, "token", "", "bearer token")
	fs.StringVar(&c.QueuePrefix, "queue-prefix", "flatness", "queue name prefix")
	fs.IntVar(&c.Queues, "queues", 10, "number of queues the partitions are spread over")
	fs.StringVar(&partitions, "partitions", "1m", "TOTAL partitions (accepts 1m, 500k)")
	fs.StringVar(&messages, "messages", "300m", "TOTAL messages (accepts 300m, 1g)")
	fs.IntVar(&c.PayloadBytes, "payload-bytes", 256, "payload size per message")
	fs.IntVar(&c.Batch, "batch", 500, "messages per push")
	fs.IntVar(&c.Concurrency, "concurrency", 16, "concurrent pushers")
	fs.IntVar(&c.RateMsgsSec, "rate", 0, "cap in messages/s (0: as fast as the broker accepts)")
	fs.IntVar(&c.DedupWindowS, "dedup-window", 3600, "dedupWindowSeconds to configure (§13.6: one hour)")
	fs.BoolVar(&c.RetentionOff, "retention-off", true, "configure retention OFF while preloading (§13.6)")
	fs.StringVar(&c.StateFile, "state", "", "state file for -resume (records the last completed partition)")
	fs.BoolVar(&c.Resume, "resume", false, "continue from the state file instead of starting over")
	fs.IntVar(&c.AssumeRate, "assume-rate", 200000, "rate used for the time ESTIMATE only")
	fs.BoolVar(&c.DryRun, "dry-run", false, "print the plan and exit")
	fs.Usage = func() {
		fmt.Fprint(stderr, preloadUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	var err error
	if c.Messages, err = ParseCount(messages); err != nil {
		return nil, fmt.Errorf("-messages: %w", err)
	}
	n, err := ParseCount(partitions)
	if err != nil {
		return nil, fmt.Errorf("-partitions: %w", err)
	}
	c.Partitions = int(n)
	return c, c.Validate()
}

const preloadUsage = `flatness preload — the bulk preloader of PLAN_RAFT.md §13.6

Loads a store to the volume the flatness test needs: by default 300 million
messages over 1 million partitions, dedup window 3600 s, retention off.

  flatness preload -url http://vm:6632 -dry-run
  flatness preload -url http://vm:6632 -messages 300m -partitions 1m -state /root/raft/preload.state

WP-0.7 ships the CLI, the validation, the plan and the estimate; the send loop
is a documented stub, so anything but -dry-run refuses.

Flags:
`
