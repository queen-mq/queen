package feedrt

import (
	"context"
	"errors"
	"fmt"
)

// Handle is a running feed stream: one runner, or the shards of one in this process.
type Handle interface {
	// Stop stops after the cycle in hand, commits what is pending and gives the lock back.
	Stop(ctx context.Context) error
	// Done is closed once it stopped (Stop, its Context, a poison message, a broker without the feed).
	Done() <-chan struct{}
	// Metrics is a snapshot of the metrics; it never waits for the runner,
	// so a stream function may call it.
	Metrics() Metrics
	// State is the live state of one step in this process. It waits for the
	// runner's next network wait: never call it from inside a stream function.
	State(step string) StepState
}

// StepState reads one step's live state: Get(key) {found, value} and List(limit) {items, truncated, total}.
type StepState interface {
	Get(key string) map[string]interface{}
	List(limit int) map[string]interface{}
}

var _ Handle = (*Runner)(nil)
var _ Handle = (*ShardGroup)(nil)

// ShardGroup is run({ queryId, shards: N }): the N shards of one feed stream
// in this process, one Runner each. Each shard has its own lock: the same
// code started in several processes runs each shard in the first process that
// takes its lock, and keeps it as a standby in the others.
type ShardGroup struct {
	broker  Broker
	stream  *Compiled
	opts    RunOptions
	count   int
	runners []*Runner
}

// NewShardGroup checks the options of a group of shards.
func NewShardGroup(b Broker, stream *Compiled, opts RunOptions, shards int) (*ShardGroup, error) {
	if shards < 2 || shards > 4096 {
		return nil, fmt.Errorf("run({ shards }): shards is a whole number from 2 to 4096, got %d", shards)
	}
	if opts.Shard != nil {
		return nil, errors.New("run(): give shards (all of them here) or shard (one), not both")
	}
	return &ShardGroup{broker: b, stream: stream, opts: opts, count: shards}, nil
}

// Shards is how many shards the stream runs in.
func (g *ShardGroup) Shards() int { return g.count }

// Runners is the shards' runners.
func (g *ShardGroup) Runners() []*Runner { return append([]*Runner(nil), g.runners...) }

// Start starts the shards one at a time: the first shard writes the layout the others then find.
func (g *ShardGroup) Start(ctx context.Context) error {
	for i := 0; i < g.count; i++ {
		o := g.opts
		o.Shard = &ShardSpec{Index: i, Count: g.count}
		r, err := NewRunner(g.broker, g.stream, o)
		if err != nil {
			return err
		}
		g.runners = append(g.runners, r)
	}
	for _, r := range g.runners {
		if err := r.Start(ctx); err != nil {
			for _, x := range g.runners {
				_ = x.Stop(context.Background())
			}
			return err
		}
	}
	return nil
}

// Stop stops every shard.
func (g *ShardGroup) Stop(ctx context.Context) error {
	errs := make(chan error, len(g.runners))
	for _, r := range g.runners {
		go func(r *Runner) { errs <- r.Stop(ctx) }(r)
	}
	var first error
	for range g.runners {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Done is closed once every shard stopped.
func (g *ShardGroup) Done() <-chan struct{} {
	c := make(chan struct{})
	go func() {
		for _, r := range g.runners {
			<-r.Done()
		}
		close(c)
	}()
	return c
}

// Metrics is the sums, and every shard's. Like a runner's, it never waits
// for the runners: it can be called from anywhere, a stream function too.
func (g *ShardGroup) Metrics() Metrics {
	out := Metrics{Group: true, Shards: g.count}
	for _, r := range g.runners {
		m := r.Metrics()
		out.PerShard = append(out.PerShard, m)
		if m.Role == "active" {
			out.Running++
		}
		out.Cycles += m.Cycles
		out.EntriesRead += m.EntriesRead
		out.Emitted += m.Emitted
		out.Outputs += m.Outputs
		out.Effects += m.Effects
		out.Commits += m.Commits
		out.Conflicts += m.Conflicts
		out.Errors += m.Errors
		out.Retries += m.Retries
		out.Poisoned += m.Poisoned
		out.Skipped += m.Skipped
		out.DeadLettered += m.DeadLettered
		out.Late += m.Late
		out.Ticks += m.Ticks
		out.Takeovers += m.Takeovers
		out.Snapshots += m.Snapshots
		out.Replays += m.Replays
		out.ReplayEntries += m.ReplayEntries
		out.TableRows += m.TableRows
		out.PendingOutputs += m.PendingOutputs
		out.Open += m.Open
		out.StateKeys += m.StateKeys
	}
	return out
}

type groupState struct {
	g    *ShardGroup
	step string
}

// State is the live state of one step over the shards this process runs. As
// a runner's State, it waits for each shard's next network wait: never call it
// from inside a stream function.
func (g *ShardGroup) State(step string) StepState { return groupState{g: g, step: step} }

func (s groupState) Get(key string) map[string]interface{} {
	for _, r := range s.g.runners {
		v := r.State(s.step).Get(key)
		if v["found"] == true || v["error"] != nil {
			out := copyMap(v)
			idx := 0
			if r.shard != nil {
				idx = r.shard.Index
			}
			out["shard"] = float64(idx)
			return out
		}
	}
	return map[string]interface{}{"found": false}
}

func (s groupState) List(limit int) map[string]interface{} {
	if limit <= 0 {
		limit = 100
	}
	items := []interface{}{}
	truncated := false
	for _, r := range s.g.runners {
		part := r.State(s.step).List(limit)
		if part["error"] != nil {
			return part
		}
		if l, ok := part["items"].([]interface{}); ok {
			items = append(items, l...)
		}
		truncated = truncated || part["truncated"] == true
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return map[string]interface{}{"items": items, "truncated": truncated || more}
}
