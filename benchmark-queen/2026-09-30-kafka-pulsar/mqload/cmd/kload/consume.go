package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"mqload/internal/core"
)

// groupState tracks one consumer group this process has members in.
type groupState struct {
	name     string
	topics   []string
	total    int64 // partitions over its topics
	expected int   // members across ALL processes
	local    int   // members in this process

	assigned   atomic.Int64 // partitions held by the local members
	lastChange atomic.Int64 // unix ns of the last local assignment change
	assignEv   atomic.Int64
	revokeEv   atomic.Int64
	lostEv     atomic.Int64

	fullSince time.Time
	// group-wide view (DescribeGroups / DescribeConsumerGroups)
	remoteFull     bool
	remoteState    string
	remoteMembers  int
	remoteAssigned int
}

type kconsumer struct {
	ci   int
	gs   *groupState
	cl   *kgo.Client
	cs   *core.ConsumerStats
	proc *core.ProcSim
	sess *kgo.GroupTransactSession // -txn workers
}

type consumers struct {
	run    *core.Run
	kc     *kafkaFlags
	adm    *admin
	groups map[string]*groupState
	order  []*groupState
	list   []*kconsumer

	ctx       context.Context
	cancel    context.CancelFunc
	ackCtx    context.Context
	ackCancel context.CancelFunc
	stopping  atomic.Bool
	wg        sync.WaitGroup
	stableS   float64

	// -txn (txn.go): the topic these members read and their group prefix (default: the -topic names), how many
	// members this process runs and their global indices, extra client options (read_committed), and the
	// transaction workers' settings (nil = plain consumers / readers)
	topicNames []string
	n, offset  int
	total      int
	extraOpts  []kgo.Opt
	worker     *workerCfg
	readerTxn  *core.TxnStats
}

func newConsumers(run *core.Run, kc *kafkaFlags, adm *admin) *consumers {
	cfg := run.Cfg
	c := &consumers{run: run, kc: kc, adm: adm, groups: map[string]*groupState{},
		topicNames: cfg.TopicNames(), n: cfg.Consumers, offset: cfg.ConsOffset, total: cfg.ConsTotal}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.ackCtx, c.ackCancel = context.WithCancel(context.Background())
	return c
}

func countParts(m map[string][]int32) int64 {
	var n int64
	for _, ps := range m {
		n += int64(len(ps))
	}
	return n
}

// start creates one franz-go client per consumer (a group member each) and its
// poll loop. Groups per SPEC §2: <prefix>-g-t<i> per topic when cons-total >=
// T, else <prefix>-g-c<ci> per consumer owning its topic slice.
func (c *consumers) start(ctx context.Context) error {
	cfg := c.run.Cfg
	names := c.topicNames
	prefix := cfg.Topic
	if len(names) == 1 {
		prefix = names[0] // -txn: <topic>-in / <topic>-out groups
	}
	for i := 0; i < c.n; i++ {
		ci := c.offset + i
		tidx := core.ConsumerTopics(ci, c.total, len(names))
		gname, expected := fmt.Sprintf("%s-g-c%d", prefix, ci), 1
		if c.total >= len(names) {
			members := (c.total - tidx[0] + len(names) - 1) / len(names)
			gname, expected = fmt.Sprintf("%s-g-t%d", prefix, tidx[0]), members
		}
		gs := c.groups[gname]
		if gs == nil {
			gs = &groupState{name: gname, expected: expected, total: int64(len(tidx) * cfg.Partitions)}
			for _, t := range tidx {
				gs.topics = append(gs.topics, names[t])
			}
			c.groups[gname] = gs
			c.order = append(c.order, gs)
		}
		gs.local++
		kcn := &kconsumer{ci: ci, gs: gs, cs: c.run.NewConsumerStats(), proc: core.NewProcSim(cfg.ProcUs)}
		opts := append(c.kc.baseOpts(fmt.Sprintf("kload-%d-c%d", cfg.LoaderIndex, ci)),
			kgo.ConsumerGroup(gname),
			kgo.ConsumeTopics(gs.topics...),
			kgo.DisableAutoCommit(),
			kgo.FetchMaxWait(c.kc.fetchMaxWait),
			kgo.FetchMinBytes(int32(c.kc.fetchMinBytes)),
			kgo.FetchMaxPartitionBytes(int32(c.kc.fetchMaxPartBytes)),
			kgo.SessionTimeout(c.kc.sessionTimeout),
			// a new group starts at the log start: nothing produced after READY
			// can be skipped (warm-up records carry no "ts": not counted)
			kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
			kgo.Balancers(kgo.CooperativeStickyBalancer()),
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				if n := countParts(m); n > 0 {
					gs.assigned.Add(n)
					gs.lastChange.Store(time.Now().UnixNano())
				}
				gs.assignEv.Add(1)
			}),
			kgo.OnPartitionsRevoked(func(rctx context.Context, cl *kgo.Client, m map[string][]int32) {
				if n := countParts(m); n > 0 {
					gs.assigned.Add(-n)
					gs.lastChange.Store(time.Now().UnixNano())
					gs.revokeEv.Add(1)
				}
				// commit what was polled before the partitions move (sync, as
				// franz-go requires in a revoke); not while shutting down. Never
				// for -txn workers: their offsets commit only inside a transaction
				// (the GroupTransactSession aborts the open one on a revoke)
				if c.worker == nil && cfg.Ack != "none" && !c.stopping.Load() {
					_ = cl.CommitUncommittedOffsets(rctx)
				}
			}),
			kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				if n := countParts(m); n > 0 {
					gs.assigned.Add(-n)
					gs.lastChange.Store(time.Now().UnixNano())
					gs.lostEv.Add(1)
				}
			}),
		)
		if c.kc.groupProtocol == "consumer" {
			opts = append(opts, kgo.ServerSideBalancer()) // KIP-848: broker-side "uniform" assignor
		}
		opts = append(opts, c.extraOpts...)
		if c.worker != nil {
			sess, err := kgo.NewGroupTransactSession(append(opts, c.worker.opts(c.kc, ci)...)...)
			if err != nil {
				return err
			}
			kcn.sess, kcn.cl = sess, sess.Client()
			c.list = append(c.list, kcn)
			c.wg.Add(1)
			go c.workerLoop(kcn)
			continue
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return err
		}
		kcn.cl = cl
		c.list = append(c.list, kcn)
		c.wg.Add(1)
		go c.loop(kcn)
	}
	if len(c.list) > 0 {
		what := "consumers"
		if c.worker != nil {
			what = "txn workers (GroupTransactSession)"
		}
		fmt.Printf("  [group] %d %s in %d groups (%s ... ), protocol %s\n", len(c.list), what, len(c.order), c.order[0].name, c.kc.groupProtocol)
	}
	return nil
}

// loop is the closed-loop consumer: poll, e2e per record, -proc-us, commit.
func (c *consumers) loop(k *kconsumer) {
	defer c.wg.Done()
	cfg := c.run.Cfg
	ctx := c.ctx
	for {
		fs := k.cl.PollRecords(ctx, cfg.Poll)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			k.cs.PopErr(fmt.Errorf("%s/%d: %w", t, p, err))
		})
		now := time.Now().UnixMicro()
		load, warm := 0, 0
		var offs map[string]map[int32]kgo.EpochOffset
		if cfg.Ack != "none" {
			offs = make(map[string]map[int32]kgo.EpochOffset, 1)
		}
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			for _, r := range p.Records {
				if k.cs.Observe(r.Value, now) {
					load++
				} else {
					warm++
				}
			}
			if offs != nil {
				last := p.Records[len(p.Records)-1]
				m := offs[p.Topic]
				if m == nil {
					m = make(map[int32]kgo.EpochOffset, 4)
					offs[p.Topic] = m
				}
				m[p.Partition] = kgo.EpochOffset{Epoch: last.LeaderEpoch, Offset: last.Offset + 1}
			}
		})
		if load+warm == 0 {
			k.cs.Empty()
			continue
		}
		k.cs.Polled(load, warm)
		if c.readerTxn != nil {
			c.readerTxn.ReaderSaw()
		}
		k.proc.Add(load + warm)
		switch cfg.Ack {
		case "async":
			// the process-wide cap on in-flight commits: block, never shed
			select {
			case c.run.AckSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			t := time.Now()
			n := load
			k.cl.CommitOffsets(c.ackCtx, offs, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				<-c.run.AckSem
				if err == nil {
					err = commitRespErr(resp)
				}
				if err != nil && c.ackCtx.Err() != nil {
					return // cut off at shutdown: not an error of the system under test
				}
				k.cs.AckDone(n, time.Since(t), err)
			})
		case "sync":
			t := time.Now()
			var cerr error
			k.cl.CommitOffsetsSync(c.ackCtx, offs, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				if err == nil {
					err = commitRespErr(resp)
				}
				cerr = err
			})
			if cerr == nil || c.ackCtx.Err() == nil {
				k.cs.AckDone(load, time.Since(t), cerr)
			}
		}
	}
}

func commitRespErr(resp *kmsg.OffsetCommitResponse) error {
	if resp == nil {
		return nil
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return fmt.Errorf("commit %s/%d: %w", t.Topic, p.Partition, err)
			}
		}
	}
	return nil
}

// waitStable returns once every group is stable: all its partitions assigned
// (by the local members, or group-wide via DescribeGroups /
// DescribeConsumerGroups when members live in other processes) and no
// assignment change for -stable-wait.
func (c *consumers) waitStable(ctx context.Context) error {
	if len(c.list) == 0 {
		return nil
	}
	t0 := time.Now()
	var lastDescribe, lastLog time.Time
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(lastDescribe) >= time.Second {
			lastDescribe = time.Now()
			c.describeGroups(ctx)
		}
		allStable := true
		var waiting []string
		for _, gs := range c.order {
			// Group-wide view for every group, local-only ones too: a local
			// member can hold every partition while the coordinator's target
			// already moved some elsewhere (KIP-848 reconciles at the
			// heartbeat cadence; classic cooperative rebalances in two phases).
			full := gs.remoteFull && (gs.expected != gs.local || gs.assigned.Load() == gs.total)
			if !full {
				gs.fullSince = time.Time{}
			} else if gs.fullSince.IsZero() {
				gs.fullSince = time.Now()
			}
			lc := time.Unix(0, gs.lastChange.Load())
			if !(full && time.Since(lc) >= c.kc.stableWait && time.Since(gs.fullSince) >= c.kc.stableWait) {
				allStable = false
				if len(waiting) < 3 {
					waiting = append(waiting, fmt.Sprintf("%s: local members %d/%d hold %d/%d partitions (group-wide %s members=%d assigned=%d)",
						gs.name, gs.local, gs.expected, gs.assigned.Load(), gs.total, gs.remoteState, gs.remoteMembers, gs.remoteAssigned))
				}
			}
		}
		if allStable {
			break
		}
		if time.Since(t0) > c.kc.groupTimeout {
			return fmt.Errorf("groups not stable after -group-timeout %v: %s", c.kc.groupTimeout, strings.Join(waiting, "; "))
		}
		if time.Since(lastLog) >= 2*time.Second {
			lastLog = time.Now()
			fmt.Printf("  [group] t=+%.1fs waiting: %s\n", time.Since(t0).Seconds(), strings.Join(waiting, "; "))
		}
		time.Sleep(100 * time.Millisecond)
	}
	var ev [3]int64
	var held int64
	for _, gs := range c.order {
		ev[0] += gs.assignEv.Load()
		ev[1] += gs.revokeEv.Load()
		ev[2] += gs.lostEv.Load()
		held += gs.assigned.Load()
	}
	c.stableS = time.Since(t0).Seconds()
	fmt.Printf("  [group] STABLE after %.1fs: %d groups, %d local members holding %d partitions, assignEv=%d revokeEv=%d lostEv=%d\n",
		c.stableS, len(c.order), len(c.list), held, ev[0], ev[1], ev[2])
	c.run.SetInfo("group_stable_s", c.stableS)
	c.run.SetInfo("group_protocol", c.kc.groupProtocol)
	c.run.SetInfo("groups", len(c.order))
	return nil
}

// describeGroups refreshes the group-wide view of every group: KIP-848 =
// ConsumerGroupDescribe (Stable, epochs settled, every member's assignment ==
// its target), classic = DescribeGroups (Stable); both: the expected member
// count across all processes and every partition assigned.
func (c *consumers) describeGroups(ctx context.Context) {
	names := make([]string, 0, len(c.order))
	for _, gs := range c.order {
		names = append(names, gs.name)
	}
	sort.Strings(names)
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if c.kc.groupProtocol == "consumer" {
		ds, _ := c.adm.adm.DescribeConsumerGroups(dctx, names...)
		for _, n := range names {
			gs := c.groups[n]
			d, ok := ds[n]
			gs.remoteFull = false
			if !ok || d.Err != nil {
				gs.remoteState = "?"
				continue
			}
			assigned, settled := 0, d.AssignmentEpoch == d.Epoch
			for _, m := range d.Members {
				for _, ps := range m.Assignment {
					assigned += len(ps)
				}
				if m.MemberEpoch != d.Epoch || !sameSet(m.Assignment, m.TargetAssignment) {
					settled = false
				}
			}
			gs.remoteState, gs.remoteMembers, gs.remoteAssigned = d.State, len(d.Members), assigned
			gs.remoteFull = settled && d.State == "Stable" && len(d.Members) == gs.expected && int64(assigned) == gs.total
		}
		return
	}
	ds, _ := c.adm.adm.DescribeGroups(dctx, names...)
	for _, n := range names {
		gs := c.groups[n]
		d, ok := ds[n]
		gs.remoteFull = false
		if !ok || d.Err != nil {
			gs.remoteState = "?"
			continue
		}
		assigned := 0
		for _, ps := range d.AssignedPartitions() {
			assigned += len(ps)
		}
		gs.remoteState, gs.remoteMembers, gs.remoteAssigned = d.State, len(d.Members), assigned
		gs.remoteFull = d.State == "Stable" && len(d.Members) == gs.expected && int64(assigned) == gs.total
	}
}

func sameSet[S ~map[string]map[int32]struct{}](a, b S) bool {
	if len(a) != len(b) {
		return false
	}
	for t, ps := range a {
		if len(b[t]) != len(ps) {
			return false
		}
		for p := range ps {
			if _, ok := b[t][p]; !ok {
				return false
			}
		}
	}
	return true
}

// stop ends the poll loops, lets in-flight async commits land (up to 5 s),
// then closes every client (leaving the groups) in parallel.
func (c *consumers) stop() {
	c.stopping.Store(true)
	c.cancel()
	c.wg.Wait()
	dl := time.Now().Add(5 * time.Second)
	for len(c.run.AckSem) > 0 && time.Now().Before(dl) {
		time.Sleep(5 * time.Millisecond)
	}
	c.ackCancel()
	var wg sync.WaitGroup
	for _, k := range c.list {
		wg.Add(1)
		go func(k *kconsumer) {
			defer wg.Done()
			if k.sess != nil {
				k.sess.Close()
				return
			}
			k.cl.Close()
		}(k)
	}
	wg.Wait()
}
