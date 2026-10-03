package main

import (
	"flag"
	"fmt"
	"testing"

	"mqload/internal/core"
)

func layout(t *testing.T, subType string, topics, parts, total int) *consumers {
	t.Helper()
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	cfg := &core.Config{}
	cfg.Register(fs)
	pc := &pulsarFlags{}
	pc.register(fs)
	if err := fs.Parse([]string{"-topics", fmt.Sprint(topics), "-partitions", fmt.Sprint(parts), "-cons-total", fmt.Sprint(total),
		"-consumers", "1", "-sub-type", subType}); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := pc.finalize(cfg); err != nil {
		t.Fatal(err)
	}
	r, err := core.NewRun(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	return &consumers{run: r, pc: pc}
}

// failover/exclusive: every <topic>-partition-<p> is subscribed by exactly one
// consumer across all processes, and the printed totals agree; shared and
// key_shared subscribe whole topics.
func TestConsumerLayout(t *testing.T) {
	for _, tc := range []struct {
		sub                  string
		topics, parts, total int
		wantRegs, wantIdle   int
		wantWhole            bool
	}{
		{"failover", 1, 100000, 297, 100000, 0, false},
		{"failover", 1, 200, 297, 200, 97, false},
		{"exclusive", 10, 1000, 297, 10000, 0, false},
		{"failover", 1000, 100, 297, 100000, 0, false},
		{"key_shared", 1, 48, 297, 297 * 48, 0, true},
		{"shared", 10, 100, 297, 297 * 100, 0, true},
		{"failover", 3, 0, 5, 5, 0, true}, // non-partitioned topics: whole topics
	} {
		c := layout(t, tc.sub, tc.topics, tc.parts, tc.total)
		seen := map[string]int{}
		regs := 0
		for ci := 0; ci < tc.total; ci++ {
			ts := c.topicsOf(ci)
			regs += registrations(c.run.Cfg, ts, c.sliced())
			for _, name := range ts {
				seen[name]++
			}
		}
		total, idle := c.totals()
		if total != tc.wantRegs || regs != tc.wantRegs || idle != tc.wantIdle {
			t.Errorf("%+v: registrations %d (summed %d) idle %d, want %d / %d", tc, total, regs, idle, tc.wantRegs, tc.wantIdle)
		}
		if tc.wantWhole {
			if c.sliced() {
				t.Errorf("%+v: must subscribe whole topics", tc)
			}
			continue
		}
		if len(seen) != tc.topics*tc.parts {
			t.Fatalf("%+v: %d distinct partition topics subscribed, want %d", tc, len(seen), tc.topics*tc.parts)
		}
		for name, n := range seen {
			if n != 1 {
				t.Fatalf("%+v: %s subscribed %d times", tc, name, n)
			}
		}
	}
}
