// Package runner drives one CM-BENCH run: it wires the workload to a broker
// adapter, runs the twelve consumer stages and the two producers, reports at
// 1 Hz, and verifies at the end.
//
// The stage logic lives here, written ONCE against broker.Broker, so every
// system under test executes byte-identical application code. Any behavioural
// difference between two runs is then the system's, not the harness's.
package runner

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

// errAborted is returned by a handler whose work was cut short by shutdown. The
// batch is deliberately NOT acked: nothing was applied, the system redelivers,
// and the verifier stays clean. A half-applied batch would read as a gap.
var errAborted = errors.New("stage: aborted before completion")

// Stage owns one consumer stream for the duration of a run.
type Stage struct {
	Def      workload.Stage
	Rec      *workload.Recorder
	Counters *workload.Counters
	Stats    *workload.StageCounters

	// lanes bounds concurrent in-work messages for this stage. It is the
	// Little's law figure from SPEC.md §2 with headroom, and it is the same
	// number for every system: the workload demands it, nobody gets to demand
	// less.
	lanes chan struct{}
}

// NewStage prepares a stage with a lane budget.
func NewStage(def workload.Stage, rec *workload.Recorder, c *workload.Counters,
	sc *workload.StageCounters, lanes int) *Stage {
	if lanes < 1 {
		lanes = 1
	}
	return &Stage{Def: def, Rec: rec, Counters: c, Stats: sc, lanes: make(chan struct{}, lanes)}
}

// Run consumes until ctx is done. It blocks.
func (s *Stage) Run(ctx context.Context, b broker.Broker, batchSize, prefetch int) error {
	opts := broker.ConsumeOpts{
		Lanes:     cap(s.lanes),
		BatchSize: batchSize,
		Prefetch:  prefetch,
		Stats:     s.Stats,
	}
	return b.Consume(ctx, s.Def.Topic, s.Def.Group, opts, func(ctx context.Context, batch *broker.Batch) error {
		return s.handle(ctx, b, batch)
	})
}

// handle is the application: do the work, then commit it in order.
//
//	PHASE 1  simulated work, all messages of the batch concurrently, bounded by
//	         the stage's lane budget, joined on a barrier.
//	PHASE 2  ordered commit — one batched re-publish to the SAME key with the
//	         slice in seq order, then the recording lines in seq order.
//
// This is the July shape unchanged (concurrent batch processing, ordered
// commit). It is a real pattern and, more to the point, changing it would make
// this campaign's Queen numbers incomparable with the certified July run.
func (s *Stage) handle(ctx context.Context, b broker.Broker, batch *broker.Batch) error {
	n := len(batch.Msgs)
	if n == 0 {
		return nil
	}
	s.Stats.Delivered.Add(int64(n))
	s.Counters.Delivered.Add(int64(n))
	entry := time.Now().UnixMicro()
	for i := range batch.Msgs {
		s.Stats.ObserveAge(entry - batch.Msgs[i].Stamp.TS)
	}
	for i := range batch.Msgs {
		if batch.Msgs[i].Redelivery {
			s.Stats.Dups.Add(1)
		}
	}

	// PHASE 1 — work barrier.
	tBarrier := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	var aborted bool
	var abortMu sync.Mutex
	for range batch.Msgs {
		d := s.workDuration()
		go func() {
			defer wg.Done()
			select {
			case s.lanes <- struct{}{}:
			case <-ctx.Done():
				abortMu.Lock()
				aborted = true
				abortMu.Unlock()
				return
			}
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				abortMu.Lock()
				aborted = true
				abortMu.Unlock()
			}
			<-s.lanes
		}()
	}
	wg.Wait()
	s.Stats.ObserveBarrier(time.Since(tBarrier).Microseconds())
	if aborted || ctx.Err() != nil {
		return errAborted
	}

	// PHASE 2 — ordered commit.
	if !s.Def.Terminal() {
		payloads := make([][]byte, 0, n)
		for _, m := range batch.Msgs {
			payloads = append(payloads, workload.EncodeDerived(m.Stamp))
		}
		// One batched publish to the property's single output lane: the slice
		// order IS the seq order, so per-property total order survives the hop
		// in one round trip instead of n. (July: serial per-message derived
		// publishes at congested RTT made the cycle seconds long.)
		tPush := time.Now()
		if err := b.PublishBatch(ctx, s.Def.OutTopic, batch.Key, payloads); err != nil {
			s.Counters.PushErr.Add(int64(n))
			return err // no ack: the batch redelivers whole
		}
		s.Stats.ObservePush(time.Since(tPush).Microseconds())
		s.Counters.Derived.Add(int64(n))
	}

	now := time.Now().UnixMicro()
	for _, m := range batch.Msgs {
		s.Rec.Write(m.Stamp.Prop, m.Stamp.Seq)
		if s.Def.Terminal() {
			// Only terminal stages observe end-to-end latency; counting an
			// intermediate hop too would double-count the same event.
			s.Counters.ObserveE2E(m.Stamp.Flow, now-m.Stamp.TS)
		}
	}
	s.Stats.Processed.Add(int64(n))
	s.Counters.Processed.Add(int64(n))
	return nil
}

func (s *Stage) workDuration() time.Duration {
	lo, hi := s.Def.WorkMinMs, s.Def.WorkMaxMs
	if hi <= lo {
		return time.Duration(lo) * time.Millisecond
	}
	return time.Duration(lo+rand.IntN(hi-lo+1)) * time.Millisecond
}
