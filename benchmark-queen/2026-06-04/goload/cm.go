// cm.go — "channel manager" realistic application workload for QueenMQ.
//
// Models a hotel channel-manager pipeline over 1000 properties, with two flows
// that must each preserve TOTAL ORDER PER PROPERTY end to end:
//
//	Flow A (availability):
//	  OTA events --push--> cm-avail        (single push, partition = property)
//	    stage-A (group cm-db): sleep 10-20ms/msg (DB update), then
//	    --push--> cm-ota-sync              (single push, same property partition)
//	      fan-out: 5 groups ota-1..ota-5, each sleep 30ms/msg (final stage).
//
//	Flow B (prices):
//	  PMS jobs --push--> cm-prices         (single push, ~2KB rates payload)
//	    stage-B (group cm-cal): sleep 10ms/msg, then
//	    --push--> cm-ota-prices            (single push, same property partition)
//	      fan-out: 5 groups otap-1..otap-5, each sleep 30ms/msg (final stage).
//
// Every event is stamped {prop,flow,seq,ts}; seq is per-(property,flow)
// monotonic. Derived pushes CARRY FORWARD prop/flow/seq/ts. Each consumer
// stage appends "prop,seq" lines to <logdir>/<queue>_<group>.log; a built-in
// verifier then proves, per (file,property), that the de-duplicated seq stream
// is 1..max with no holes below the highest delivered seq (missing tail seqs =
// in-flight at cutoff, not a loss) and is non-decreasing in first-occurrence
// order (ordering violation otherwise).
//
// Producers are OPEN-LOOP pacers (wall-clock owed + optional ramp). Per-property
// push order is guaranteed by routing each property to a single ordered pusher
// (per-partition FIFO), so seq order == broker arrival order. Consumers are
// bounded worker pools that lease (AutoAck=false) and ack the full batch ASYNC
// through a shared bounded semaphore.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// Topology constants.
const (
	cmQAvail     = "cm-avail"
	cmQPrices    = "cm-prices"
	cmQOtaSync   = "cm-ota-sync"
	cmQOtaPrices = "cm-ota-prices"
	cmGrpDB      = "cm-db"
	cmGrpCal     = "cm-cal"
	cmOtaFanout  = 5 // ota-1..ota-5 and otap-1..otap-5
)

// ---------------------------------------------------------------------------
// log writer (per stage-per-group), buffered, flushed on shutdown.
// ---------------------------------------------------------------------------

type cmLog struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

func newCMLog(dir, queue, group string) (*cmLog, error) {
	f, err := os.Create(filepath.Join(dir, queue+"_"+group+".log"))
	if err != nil {
		return nil, err
	}
	return &cmLog{w: bufio.NewWriterSize(f, 1<<20), f: f}, nil
}

// write appends one "prop,seq\n" line. Safe from all workers in the group.
func (l *cmLog) write(prop int, seq int64) {
	var b [32]byte
	n := 0
	n += copy(b[n:], strconv.Itoa(prop))
	b[n] = ','
	n++
	n += copy(b[n:], strconv.FormatInt(seq, 10))
	b[n] = '\n'
	n++
	l.mu.Lock()
	_, _ = l.w.Write(b[:n])
	l.mu.Unlock()
}

func (l *cmLog) close() {
	l.mu.Lock()
	_ = l.w.Flush()
	_ = l.f.Close()
	l.mu.Unlock()
}

// ---------------------------------------------------------------------------
// payloads + extraction
// ---------------------------------------------------------------------------

func cmPart(prop int) string { return "p" + strconv.Itoa(prop) }

// cmStamp is the per-event correctness stamp carried through every hop.
// ts is UnixMicro (exactly representable as float64 through a JSON round-trip,
// unlike UnixNano) so end-to-end latency stays exact to the microsecond.
func cmPayloadA(prop int, seq, tsMicros int64) map[string]interface{} {
	return map[string]interface{}{"prop": prop, "flow": "A", "seq": seq, "ts": tsMicros}
}

func cmPayloadB(prop int, seq, tsMicros int64, rates []interface{}) map[string]interface{} {
	return map[string]interface{}{"prop": prop, "flow": "B", "seq": seq, "ts": tsMicros, "rates": rates}
}

func cmPayloadDerived(prop int, flow string, seq, tsMicros int64) map[string]interface{} {
	return map[string]interface{}{"prop": prop, "flow": flow, "seq": seq, "ts": tsMicros}
}

// cmExtract pulls the stamp out of a consumed message. JSON numbers decode to
// float64; prop/seq/ts are all integer-valued and within float64's exact range.
func cmExtract(d map[string]interface{}) (prop int, flow string, seq, ts int64, ok bool) {
	pf, ok1 := d["prop"].(float64)
	sf, ok2 := d["seq"].(float64)
	tf, ok3 := d["ts"].(float64)
	fl, ok4 := d["flow"].(string)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0, "", 0, 0, false
	}
	return int(pf), fl, int64(sf), int64(tf), true
}

// cmRatesPad builds a ~targetBytes rates array to make flow-B payloads realistic.
func cmRatesPad(targetBytes int) []interface{} {
	var rates []interface{}
	approx, day := 0, 1
	for approx < targetBytes {
		rates = append(rates, map[string]interface{}{
			"d": fmt.Sprintf("2026-%02d-%02d", 1+(day/28)%12, 1+day%28),
			"p": 100.0 + float64(day%400),
			"a": day % 12,
		})
		approx += 36
		day++
	}
	return rates
}

// ---------------------------------------------------------------------------
// async full-batch ack, consumer-group aware
// ---------------------------------------------------------------------------

// cmDispatchAck acks an entire leased pop batch as `completed` on a goroutine,
// bounded by a shared semaphore (honest blocking backpressure — an ack is never
// shed). Unlike main.go's dispatchAck it threads the consumer GROUP into
// AckOptions: the seg broker resolves an ack's cursor per (partition, group) and
// defaults a missing group to __QUEUE_MODE__, so a group workload MUST send it.
// The goroutine runs under ackCtx (not the run ctx) so an in-flight ack can still
// land during teardown; ackWg tracks it for the drain. Failures are counted, not
// retried (lease expiry -> redeliver is the real behavior).
func cmDispatchAck(ctx, ackCtx context.Context, q *queen.Queen, msgs []*queen.Message,
	group string, ackSem chan struct{}, ackWg *sync.WaitGroup, acked, ackErr *int64) {
	select {
	case ackSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	ackWg.Add(1)
	go func(batch []*queen.Message) {
		defer ackWg.Done()
		defer func() { <-ackSem }()
		n := int64(len(batch))
		resp, err := q.Ack(ackCtx, batch, true, queen.AckOptions{ConsumerGroup: group})
		if err != nil {
			if ackCtx.Err() == nil {
				atomic.AddInt64(ackErr, n)
			}
			return
		}
		var ok int64
		for _, r := range resp {
			if r.Success {
				ok++
			}
		}
		atomic.AddInt64(acked, ok)
		if ok < n {
			atomic.AddInt64(ackErr, n-ok)
		}
	}(msgs)
}

// ---------------------------------------------------------------------------
// open-loop pacer (single scheduler goroutine per flow)
// ---------------------------------------------------------------------------

// cmPacer offers `rate` events/s on a wall-clock schedule (so a late wake still
// offers everything owed — the offered rate can't silently sag). With rampSec>0
// the density ramps 0->full over the first rampSec seconds (same math as
// runOpenLoopMode). emit(sched) is called once per owed event with its CO-correct
// scheduled instant; onShed(n) accounts a per-wake backlog beyond maxCatchUp.
func cmPacer(ctx context.Context, rate, rampSec int, emit func(sched time.Time), onShed func(n int64)) {
	if rate <= 0 {
		<-ctx.Done()
		return
	}
	rps := float64(rate)
	minTick, maxTick := 250*time.Microsecond, 1*time.Millisecond
	step := time.Duration(float64(time.Second) / rps)
	tickEvery := step
	if tickEvery > maxTick {
		tickEvery = maxTick
	}
	if tickEvery < minTick {
		tickEvery = minTick
	}
	const maxCatchUp = 8192
	ramp := float64(rampSec)
	base := time.Now()
	tk := time.NewTicker(tickEvery)
	defer tk.Stop()
	var k int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		el := time.Since(base).Seconds()
		var cum float64
		if ramp <= 0 || el >= ramp {
			cum = rps * (el - maxf(ramp, 0)/2)
		} else {
			cum = rps * el * el / (2 * ramp)
		}
		owed := int64(cum) + 1 - k
		if owed <= 0 {
			continue
		}
		if owed > maxCatchUp {
			bulk := owed - maxCatchUp
			owed = maxCatchUp
			k += bulk
			onShed(bulk)
		}
		for n := int64(0); n < owed; n++ {
			var schedSec float64
			kf := float64(k)
			if ramp > 0 && kf < rps*ramp/2 {
				schedSec = math.Sqrt(2 * kf * ramp / rps)
			} else {
				schedSec = kf/rps + maxf(ramp, 0)/2
			}
			sched := base.Add(time.Duration(schedSec * float64(time.Second)))
			k++
			emit(sched)
		}
	}
}

// ---------------------------------------------------------------------------
// producers — one flow, sharded ordered pushers
// ---------------------------------------------------------------------------

type cmPushJob struct {
	prop int
	seq  int64
	ts   int64
}

// cmRunProducer schedules `rate` events/s for one flow and pushes them SINGLY
// (batch 1 — intentionally stressing request rate). Per-property push order (=
// seq order = broker arrival order) is guaranteed by routing property p to
// shard p%shards: the single pacer assigns seq and enqueues under no contention
// (one scheduler goroutine), and each shard has ONE pusher draining its channel
// in FIFO order. A shed (channel full) does NOT consume a seq, so shedding never
// manufactures a gap. Push errors retry (client also retries w/ dedup) so a
// successfully-scheduled seq is never lost except at ctx-cancel (a tail seq).
func cmRunProducer(ctx context.Context, wg *sync.WaitGroup, q *queen.Queen,
	queue, flow string, rate, rampSec, properties, shards, chanCap int,
	seqCounter []int64, ratesPad []interface{},
	produced, pushShed, pushErr, pushRetry *int64) {

	shardCh := make([]chan cmPushJob, shards)
	for i := range shardCh {
		shardCh[i] = make(chan cmPushJob, chanCap)
	}

	for s := 0; s < shards; s++ {
		wg.Add(1)
		go func(ch chan cmPushJob) {
			defer wg.Done()
			for job := range ch {
				if ctx.Err() != nil {
					continue // drain-drop after shutdown (tail seqs -> in-flight)
				}
				var payload map[string]interface{}
				if flow == "A" {
					payload = cmPayloadA(job.prop, job.seq, job.ts)
				} else {
					payload = cmPayloadB(job.prop, job.seq, job.ts, ratesPad)
				}
				part := cmPart(job.prop)
				for {
					_, err := q.Queue(queue).Partition(part).Push(payload).Execute(ctx)
					if err == nil {
						atomic.AddInt64(produced, 1)
						break
					}
					if ctx.Err() != nil {
						atomic.AddInt64(pushErr, 1)
						break
					}
					atomic.AddInt64(pushRetry, 1)
					time.Sleep(3 * time.Millisecond)
				}
			}
		}(shardCh[s])
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			for i := range shardCh {
				close(shardCh[i])
			}
		}()
		cmPacer(ctx, rate, rampSec, func(sched time.Time) {
			prop := rand.Intn(properties)
			ts := sched.UnixMicro()
			next := seqCounter[prop] + 1
			ch := shardCh[prop%shards]
			select {
			case ch <- cmPushJob{prop: prop, seq: next, ts: ts}:
				seqCounter[prop] = next
			default:
				atomic.AddInt64(pushShed, 1)
			}
		}, func(n int64) { atomic.AddInt64(pushShed, n) })
	}()
}

// cmWarmup pre-pushes seq 0 for every property on one ingress flow at bounded
// concurrency. Purpose: create + dedup-hydrate all partitions OUTSIDE the rated
// window. Cold mass-creation under load is a measured wedge: log_push_multi_v1
// creates missing partitions inside the bundle transaction, so first contact
// with thousands of partitions serializes every overlapping bundle on the
// creator's xid (observed 2026-07-23: 190 backends on Lock:transactionid,
// commit/s 1186 -> ~10, total stall). The warm wave is convoy-free by
// construction — each partition is pushed exactly once, so no two transactions
// ever contend on the same uncreated partition. Warm pushes retry until
// success: a lost seq 0 would be a verifier gap, not a shed. Paced seqs start
// at 1 (counter init 0), so seq 0 is warmup's alone; re-running against a
// non-fresh DB outside the dedup window would re-deliver seq 0, which the
// verifier flags — VM runs are on fresh DBs, local reruns use -queue-prefix.
func cmWarmup(ctx context.Context, q *queen.Queen, queue, flow string, properties, conc int,
	ratesPad []interface{}, produced, pushRetry *int64) error {
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed int64
	for p := 0; p < properties; p++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go func(prop int) {
			defer wg.Done()
			defer func() { <-sem }()
			var payload map[string]interface{}
			ts := time.Now().UnixMicro()
			if flow == "A" {
				payload = cmPayloadA(prop, 0, ts)
			} else {
				payload = cmPayloadB(prop, 0, ts, ratesPad)
			}
			part := cmPart(prop)
			for attempt := 0; ; attempt++ {
				_, err := q.Queue(queue).Partition(part).Push(payload).Execute(ctx)
				if err == nil {
					atomic.AddInt64(produced, 1)
					return
				}
				if ctx.Err() != nil || attempt >= 40 {
					atomic.AddInt64(&failed, 1)
					return
				}
				atomic.AddInt64(pushRetry, 1)
				time.Sleep(250 * time.Millisecond)
			}
		}(p)
	}
	wg.Wait()
	if n := atomic.LoadInt64(&failed); n > 0 {
		return fmt.Errorf("warmup %s: %d/%d properties failed seq-0 push", queue, n, properties)
	}
	return nil
}

// ---------------------------------------------------------------------------
// consumers
// ---------------------------------------------------------------------------

// cmIntermediateStage — OPERATOR DIRECTIVE (Alice): multi-partition batches,
// per-message work CONCURRENT, one bulk ack per batch. Order is preserved by
// the parallel-work + ORDERED-EMISSION pattern:
//
//	PHASE 1 — every message's simulated work (sleep) runs concurrently, bounded
//	  by the stage's shared executor semaphore; a barrier waits for ALL.
//	  Batch wall-time ≈ max(sleep), not sum — a 100-msg batch at 30ms/msg
//	  costs ~30ms, not 3s.
//	PHASE 2 — side-effects are emitted IN SEQ ORDER PER PARTITION: the batch's
//	  messages are grouped by partition (pop already yields each partition's
//	  frames in seq order); one goroutine per partition emits its derived
//	  pushes + order-log lines serially. Different partitions emit in
//	  parallel (they are independent properties). The partition's lease is
//	  held until the bulk ack, so no other consumer can interleave.
//
// This mirrors a real channel manager: independent updates of one property are
// PROCESSED concurrently but APPLIED (forwarded) in order.
func cmIntermediateStage(ctx, ackCtx context.Context, wg, ackWg *sync.WaitGroup, q *queen.Queen,
	inQueue, group, outQueue string, popWorkers int, workSem chan struct{},
	sleepMinMs, sleepMaxMs, popBatch, popTimeout int,
	popWait bool, popPartitions int, lg *cmLog, ackSem chan struct{},
	processed, derived, acked, ackErr, pushErr, pushRetry *int64) {

	for w := 0; w < popWorkers; w++ {
		wg.Add(1)
		go func(rng *rand.Rand) {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := q.Queue(inQueue).Group(group)
				if popPartitions > 1 {
					qb = qb.Partitions(popPartitions)
				}
				if popWait {
					qb = qb.Wait(true).TimeoutMillis(popTimeout)
				} else {
					qb = qb.Wait(false)
				}
				msgs, err := qb.Batch(popBatch).AutoAck(false).Pop(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if len(msgs) == 0 {
					if !popWait {
						time.Sleep(2 * time.Millisecond)
					}
					continue
				}

				// PHASE 1 — concurrent simulated work with completion barrier.
				// Durations drawn by the popper (rng is not goroutine-safe).
				var workWg sync.WaitGroup
				workWg.Add(len(msgs))
				aborted := false
				for range msgs {
					d := sleepMinMs
					if sleepMaxMs > sleepMinMs {
						d = sleepMinMs + rng.Intn(sleepMaxMs-sleepMinMs+1)
					}
					go func(d time.Duration) {
						defer workWg.Done()
						select {
						case workSem <- struct{}{}:
						case <-ctx.Done():
							return
						}
						time.Sleep(d)
						<-workSem
					}(time.Duration(d) * time.Millisecond)
				}
				workWg.Wait()
				if ctx.Err() != nil {
					aborted = true
				}
				if aborted {
					// No emission, no ack: the lease expires and the whole batch
					// redelivers — nothing half-applied, verifier stays clean.
					return
				}

				// PHASE 2 — ordered emission per partition, partitions in parallel.
				order := make([]string, 0, popPartitions)
				byPart := make(map[string][]*queen.Message, popPartitions)
				for _, m := range msgs {
					if _, seen := byPart[m.PartitionID]; !seen {
						order = append(order, m.PartitionID)
					}
					byPart[m.PartitionID] = append(byPart[m.PartitionID], m)
				}
				var emitWg sync.WaitGroup
				for _, pid := range order {
					emitWg.Add(1)
					go func(batch []*queen.Message) {
						defer emitWg.Done()
						// 1 property = 1 partition on BOTH queues, so this
						// group targets ONE output partition: emit it as a
						// SINGLE batched push — the array order IS the seq
						// order and the server appends it atomically, so
						// total order per property is preserved with one
						// round-trip instead of len(batch). (Measured: serial
						// per-message derived pushes at congested RTT made
						// the pop cycle seconds long.)
						payloads := make([]interface{}, 0, len(batch))
						type stamp struct {
							prop int
							seq  int64
						}
						stamps := make([]stamp, 0, len(batch))
						part := ""
						for _, m := range batch {
							prop, flow, seq, ts, ok := cmExtract(m.Data)
							if !ok {
								continue
							}
							payloads = append(payloads, cmPayloadDerived(prop, flow, seq, ts))
							stamps = append(stamps, stamp{prop, seq})
							part = cmPart(prop)
						}
						if len(payloads) == 0 {
							return
						}
						for {
							_, e := q.Queue(outQueue).Partition(part).Push(payloads).Execute(ctx)
							if e == nil {
								atomic.AddInt64(derived, int64(len(payloads)))
								break
							}
							if ctx.Err() != nil {
								atomic.AddInt64(pushErr, int64(len(payloads)))
								return
							}
							atomic.AddInt64(pushRetry, 1)
							time.Sleep(3 * time.Millisecond)
						}
						for _, s := range stamps {
							lg.write(s.prop, s.seq)
							atomic.AddInt64(processed, int64(1))
						}
					}(byPart[pid])
				}
				emitWg.Wait()
				if ctx.Err() != nil {
					return
				}
				cmDispatchAck(ctx, ackCtx, q, msgs, group, ackSem, ackWg, acked, ackErr)
			}
		}(rand.New(rand.NewSource(rand.Int63())))
	}
}

// cmIntermediateStageTargeted — intermediate stage via TARGETED pops with
// static partition ownership (same sweep pattern as cmFinalStageTargeted;
// see its header for the measured wildcard-scan rationale — at high single-
// push rates the allocator UPDATE churn on log_partitions makes ANY wildcard
// candidate scan read dead-version mountains: measured 35ms/scan, ~20 PG
// cores at 400 scans/s). Each non-empty batch is processed asynchronously
// (lease-guarded), work runs concurrently under the stage executor, then the
// batch's derived messages are emitted as ONE batched push to the property's
// output partition and the batch is bulk-acked.
func cmIntermediateStageTargeted(ctx, ackCtx context.Context, wg, ackWg *sync.WaitGroup, q *queen.Queen,
	inQueue, group, outQueue string, popWorkers int, workSem chan struct{},
	sleepMinMs, sleepMaxMs, popBatch, properties, sweepFloorMs int,
	lg *cmLog, ackSem chan struct{},
	processed, derived, acked, ackErr, pushErr, pushRetry *int64) {

	for w := 0; w < popWorkers; w++ {
		wg.Add(1)
		go func(w int, rng *rand.Rand) {
			defer wg.Done()
			var procWg sync.WaitGroup
			defer procWg.Wait()
			// Durations are drawn by the sweeper (rng is not goroutine-safe)
			// and handed to the async processor with the batch.
			for ctx.Err() == nil {
				sweepStart := time.Now()
				for p := w; p < properties; p += popWorkers {
					if ctx.Err() != nil {
						return
					}
					msgs, err := q.Queue(inQueue).Partition(cmPart(p)).Group(group).
						Batch(popBatch).Wait(false).AutoAck(false).Pop(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						time.Sleep(5 * time.Millisecond)
						continue
					}
					if len(msgs) == 0 {
						continue
					}
					durs := make([]time.Duration, len(msgs))
					for i := range durs {
						d := sleepMinMs
						if sleepMaxMs > sleepMinMs {
							d = sleepMinMs + rng.Intn(sleepMaxMs-sleepMinMs+1)
						}
						durs[i] = time.Duration(d) * time.Millisecond
					}
					procWg.Add(1)
					go func(batch []*queen.Message, durs []time.Duration) {
						defer procWg.Done()
						var workWg sync.WaitGroup
						workWg.Add(len(batch))
						for i := range batch {
							go func(d time.Duration) {
								defer workWg.Done()
								select {
								case workSem <- struct{}{}:
								case <-ctx.Done():
									return
								}
								time.Sleep(d)
								<-workSem
							}(durs[i])
						}
						workWg.Wait()
						if ctx.Err() != nil {
							return // no emission, no ack: batch redelivers whole
						}
						// Ordered emission: ONE batched push (array order =
						// seq order) to the property's output partition.
						payloads := make([]interface{}, 0, len(batch))
						type stamp struct {
							prop int
							seq  int64
						}
						stamps := make([]stamp, 0, len(batch))
						part := ""
						for _, m := range batch {
							prop, flow, seq, ts, ok := cmExtract(m.Data)
							if !ok {
								continue
							}
							payloads = append(payloads, cmPayloadDerived(prop, flow, seq, ts))
							stamps = append(stamps, stamp{prop, seq})
							part = cmPart(prop)
						}
						if len(payloads) > 0 {
							for {
								_, e := q.Queue(outQueue).Partition(part).Push(payloads).Execute(ctx)
								if e == nil {
									atomic.AddInt64(derived, int64(len(payloads)))
									break
								}
								if ctx.Err() != nil {
									atomic.AddInt64(pushErr, int64(len(payloads)))
									return
								}
								atomic.AddInt64(pushRetry, 1)
								time.Sleep(3 * time.Millisecond)
							}
							for _, s := range stamps {
								lg.write(s.prop, s.seq)
								atomic.AddInt64(processed, 1)
							}
						}
						cmDispatchAck(ctx, ackCtx, q, batch, group, ackSem, ackWg, acked, ackErr)
					}(msgs, durs)
				}
				if d := time.Duration(sweepFloorMs)*time.Millisecond - time.Since(sweepStart); d > 0 {
					select {
					case <-time.After(d):
					case <-ctx.Done():
					}
				}
			}
		}(w, rand.New(rand.NewSource(rand.Int63())))
	}
}

// cmFinalStage: lease a single-partition batch, sleep per message, log, record
// end-to-end latency (now - producer-stamped ts), async-ack. This is where the
// e2e latency distribution is measured (CO-correct: ts is the producer's
// SCHEDULED instant, not its actual send time).
func cmFinalStage(ctx, ackCtx context.Context, wg, ackWg *sync.WaitGroup, q *queen.Queen,
	inQueue, group string, popWorkers int, workSem chan struct{}, sleepMs, popBatch, popTimeout int,
	popWait bool, popPartitions int, lg *cmLog, ackSem chan struct{}, lat *olHist,
	total, groupCount, acked, ackErr *int64) {

	for w := 0; w < popWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := q.Queue(inQueue).Group(group)
				if popPartitions > 1 {
					qb = qb.Partitions(popPartitions)
				}
				if popWait {
					qb = qb.Wait(true).TimeoutMillis(popTimeout)
				} else {
					qb = qb.Wait(false)
				}
				msgs, err := qb.Batch(popBatch).AutoAck(false).Pop(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if len(msgs) == 0 {
					if !popWait {
						time.Sleep(2 * time.Millisecond)
					}
					continue
				}
				// PHASE 1 — concurrent simulated work (operator directive): all
				// sleeps run in parallel under the shared executor semaphore.
				var workWg sync.WaitGroup
				workWg.Add(len(msgs))
				for range msgs {
					go func() {
						defer workWg.Done()
						select {
						case workSem <- struct{}{}:
						case <-ctx.Done():
							return
						}
						time.Sleep(time.Duration(sleepMs) * time.Millisecond)
						<-workSem
					}()
				}
				workWg.Wait()
				if ctx.Err() != nil {
					return // no logs, no ack: batch redelivers whole
				}

				// PHASE 2 — ordered recording (pop order = seq order per
				// partition). No side-effect pushes here, so a single cheap
				// in-order loop suffices; e2e latency includes the work time.
				nowMicros := time.Now().UnixMicro()
				for _, m := range msgs {
					prop, _, seq, ts, ok := cmExtract(m.Data)
					if !ok {
						continue
					}
					if e2e := nowMicros - ts; e2e > 0 {
						lat.record(e2e)
					}
					lg.write(prop, seq)
					atomic.AddInt64(total, 1)
					atomic.AddInt64(groupCount, 1)
				}
				cmDispatchAck(ctx, ackCtx, q, msgs, group, ackSem, ackWg, acked, ackErr)
			}
		}()
	}
}

// cmFinalStageTargeted — final fan-out consumption via TARGETED pops with
// static partition ownership: worker w owns partitions {w, w+W, ...} and
// sweeps them continuously. Rationale (measured 2026-07-23, pg_stat_statements
// track=all): at 1000 partitions/queue the WILDCARD candidate scan costs
// ~12ms/pop, and the near-empty final queues drove 2.5k scans/s ≈ 30 PG cores
// of pure candidate selection, congesting the push path and collapsing the
// whole pipeline. A targeted pop skips candidate selection entirely
// (~0.15ms). Ordering is stronger than the wildcard path: one partition has
// exactly ONE owner, batches are processed serially per partition, and the
// broker lease guards the async processing window (a re-pop of a partition
// whose batch is still in flight returns empty until the bulk ack lands).
// The sweep floor bounds empty-pop cost at low rates; at the target rate
// batches arrive full and the floor never engages.
func cmFinalStageTargeted(ctx, ackCtx context.Context, wg, ackWg *sync.WaitGroup, q *queen.Queen,
	inQueue, group string, popWorkers int, workSem chan struct{}, sleepMs, popBatch, properties, sweepFloorMs int,
	lg *cmLog, ackSem chan struct{}, lat *olHist,
	total, groupCount, acked, ackErr *int64) {

	process := func(msgs []*queen.Message) {
		var workWg sync.WaitGroup
		workWg.Add(len(msgs))
		for range msgs {
			go func() {
				defer workWg.Done()
				select {
				case workSem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				<-workSem
			}()
		}
		workWg.Wait()
		if ctx.Err() != nil {
			return // no logs, no ack: lease expires, batch redelivers whole
		}
		nowMicros := time.Now().UnixMicro()
		for _, m := range msgs {
			prop, _, seq, ts, ok := cmExtract(m.Data)
			if !ok {
				continue
			}
			if e2e := nowMicros - ts; e2e > 0 {
				lat.record(e2e)
			}
			lg.write(prop, seq)
			atomic.AddInt64(total, 1)
			atomic.AddInt64(groupCount, 1)
		}
		cmDispatchAck(ctx, ackCtx, q, msgs, group, ackSem, ackWg, acked, ackErr)
	}

	for w := 0; w < popWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var procWg sync.WaitGroup
			defer procWg.Wait()
			for ctx.Err() == nil {
				sweepStart := time.Now()
				for p := w; p < properties; p += popWorkers {
					if ctx.Err() != nil {
						return
					}
					msgs, err := q.Queue(inQueue).Partition(cmPart(p)).Group(group).
						Batch(popBatch).Wait(false).AutoAck(false).Pop(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						time.Sleep(5 * time.Millisecond)
						continue
					}
					if len(msgs) == 0 {
						continue
					}
					procWg.Add(1)
					go func(batch []*queen.Message) {
						defer procWg.Done()
						process(batch)
					}(msgs)
				}
				// Sweep floor: don't hammer near-empty partitions. At the
				// target rate sweeps take longer than the floor anyway.
				if d := time.Duration(sweepFloorMs)*time.Millisecond - time.Since(sweepStart); d > 0 {
					select {
					case <-time.After(d):
					case <-ctx.Done():
					}
				}
			}
		}(w)
	}
}

// ---------------------------------------------------------------------------
// verifier
// ---------------------------------------------------------------------------

type cmPropAcc struct {
	seen     map[int64]struct{}
	maxFirst int64 // highest first-occurrence seq seen so far (order check)
	maxSeen  int64
	dups     int64
}

type cmStageResult struct {
	file     string
	flow     string
	msgs     int64
	unique   int64
	dups     int64
	gaps     int64 // holes below the highest delivered seq = real losses
	viols    int64 // first-occurrence order violations
	inflight int64 // producedMax - maxSeen, summed over props (tail in-flight, OK)
	props    int
	pass     bool
	fail     bool
}

// cmVerifyFile streams one <queue>_<group>.log. produced maps prop -> the
// ground-truth max seq assigned for this file's flow (from produced.meta), used
// only to report the in-flight tail; it never affects pass/fail. baseSeq is the
// first seq every property is expected to deliver (0 with -warmup, else 1).
func cmVerifyFile(path, flow string, produced map[int]int64, ackErr, baseSeq int64) (cmStageResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return cmStageResult{file: filepath.Base(path), flow: flow}, err
	}
	defer f.Close()

	accs := make(map[int]*cmPropAcc)
	res := cmStageResult{file: filepath.Base(path), flow: flow}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		comma := strings.IndexByte(line, ',')
		if comma <= 0 {
			continue
		}
		prop, e1 := strconv.Atoi(line[:comma])
		seq, e2 := strconv.ParseInt(line[comma+1:], 10, 64)
		if e1 != nil || e2 != nil {
			continue
		}
		res.msgs++
		a := accs[prop]
		if a == nil {
			a = &cmPropAcc{seen: make(map[int64]struct{})}
			accs[prop] = a
		}
		if _, dup := a.seen[seq]; dup {
			a.dups++
			res.dups++
			continue // redelivery: dedup by first occurrence for order
		}
		a.seen[seq] = struct{}{}
		res.unique++
		if seq < a.maxFirst {
			res.viols++ // first occurrence of a lower seq after a higher one
		}
		if seq > a.maxFirst {
			a.maxFirst = seq
		}
		if seq > a.maxSeen {
			a.maxSeen = seq
		}
	}
	if err := sc.Err(); err != nil {
		return res, err
	}

	res.props = len(accs)
	for prop, a := range accs {
		// Real losses: any seq in [baseSeq,maxSeen] not delivered — a HIGHER
		// seq arrived, so the missing one wasn't merely in-flight. All seen
		// seqs are distinct and in [baseSeq,maxSeen], so the expected span is
		// maxSeen-baseSeq+1 and the shortfall vs |seen| is the loss count.
		res.gaps += a.maxSeen - baseSeq + 1 - int64(len(a.seen))
		// Tail in-flight: seqs produced but not yet delivered here (the highest
		// few at cutoff). Reported, never a failure.
		if pm, ok := produced[prop]; ok && pm > a.maxSeen {
			res.inflight += pm - a.maxSeen
		}
	}

	// Verdict: a gap below the frontier is ALWAYS a failure. An ordering
	// violation fails UNLESS acks failed during the run (a redelivery can
	// legitimately reorder), in which case it is reported but not fatal.
	res.fail = res.gaps > 0 || (res.viols > 0 && ackErr == 0)
	res.pass = !res.fail
	return res, nil
}

// cmLoadMeta reads produced.meta: a "# ackErr=<n> base=<0|1> ..." header plus
// "flow prop maxseq" lines. Returns per-flow prop->maxseq maps, the run's
// ackErr, and the first expected seq per property (0 with -warmup, else 1;
// old metas without the token default to 1).
func cmLoadMeta(dir string) (map[int]int64, map[int]int64, int64, int64) {
	pa, pb := map[int]int64{}, map[int]int64{}
	var ackErr int64
	baseSeq := int64(1)
	f, err := os.Open(filepath.Join(dir, "produced.meta"))
	if err != nil {
		return pa, pb, 0, baseSeq
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			for _, tok := range strings.Fields(line) {
				if v, ok := strings.CutPrefix(tok, "ackErr="); ok {
					ackErr, _ = strconv.ParseInt(v, 10, 64)
				}
				if v, ok := strings.CutPrefix(tok, "base="); ok {
					baseSeq, _ = strconv.ParseInt(v, 10, 64)
				}
			}
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 3 {
			continue
		}
		prop, _ := strconv.Atoi(fs[1])
		mx, _ := strconv.ParseInt(fs[2], 10, 64)
		switch fs[0] {
		case "A":
			pa[prop] = mx
		case "B":
			pb[prop] = mx
		}
	}
	return pa, pb, ackErr, baseSeq
}

// cmVerify runs the verifier over every stage log in dir and prints a verdict
// table. Returns true on overall PASS. ackErr is the run's ack-failure count
// (0 in -verify-only mode unless present in produced.meta). prefix matches the
// -queue-prefix the run used (so the file names line up).
func cmVerify(dir, prefix string, ackErrOverride int64, haveOverride bool) bool {
	pa, pb, metaAckErr, baseSeq := cmLoadMeta(dir)
	ackErr := metaAckErr
	if haveOverride {
		ackErr = ackErrOverride
	}

	type spec struct {
		queue, group, flow string
	}
	specs := []spec{{prefix + cmQAvail, cmGrpDB, "A"}}
	for i := 1; i <= cmOtaFanout; i++ {
		specs = append(specs, spec{prefix + cmQOtaSync, "ota-" + strconv.Itoa(i), "A"})
	}
	specs = append(specs, spec{prefix + cmQPrices, cmGrpCal, "B"})
	for i := 1; i <= cmOtaFanout; i++ {
		specs = append(specs, spec{prefix + cmQOtaPrices, "otap-" + strconv.Itoa(i), "B"})
	}

	fmt.Printf("\n=== VERIFIER (dir=%s, ackErr=%d) ===\n", dir, ackErr)
	fmt.Printf("%-26s %10s %10s %8s %8s %8s %10s  %s\n",
		"stage(queue_group)", "msgs", "unique", "dups", "gaps", "viols", "inflight", "verdict")
	overall := true
	var tMsgs, tUniq, tDups, tGaps, tViol, tInfl int64
	for _, s := range specs {
		produced := pa
		if s.flow == "B" {
			produced = pb
		}
		path := filepath.Join(dir, s.queue+"_"+s.group+".log")
		r, err := cmVerifyFile(path, s.flow, produced, ackErr, baseSeq)
		if err != nil {
			fmt.Printf("%-26s  ERROR: %v\n", s.queue+"_"+s.group, err)
			overall = false
			continue
		}
		verdict := "PASS"
		if r.fail {
			verdict = "FAIL"
			overall = false
		} else if r.viols > 0 {
			verdict = "PASS(viol~redeliv)"
		}
		fmt.Printf("%-26s %10d %10d %8d %8d %8d %10d  %s\n",
			s.queue+"_"+s.group, r.msgs, r.unique, r.dups, r.gaps, r.viols, r.inflight, verdict)
		tMsgs += r.msgs
		tUniq += r.unique
		tDups += r.dups
		tGaps += r.gaps
		tViol += r.viols
		tInfl += r.inflight
	}
	fmt.Printf("%-26s %10d %10d %8d %8d %8d %10d\n", "TOTAL", tMsgs, tUniq, tDups, tGaps, tViol, tInfl)
	if overall {
		fmt.Printf("VERDICT: PASS  (0 gaps below frontier, %d order-violations, %d dups, %d in-flight at cutoff)\n",
			tViol, tDups, tInfl)
	} else {
		fmt.Printf("VERDICT: FAIL  (gaps=%d order-violations=%d — see per-stage rows above)\n", tGaps, tViol)
	}
	return overall
}

// ---------------------------------------------------------------------------
// entry point
// ---------------------------------------------------------------------------

func cmDeriveWorkers(rate int, sleepMs float64) int {
	w := int(math.Ceil(float64(rate) * sleepMs / 1000.0 * 1.5))
	if w < 4 {
		w = 4
	}
	return w
}

// cmDerivePopWorkers sizes a stage's POP-worker count (each worker owns one
// pop→work-barrier→ordered-emit→bulk-ack cycle). cycle ≈ pop RTT + max sleep +
// per-partition emission + ack dispatch ≈ ~150ms; 2x headroom.
func cmDerivePopWorkers(rate, popBatch int) int {
	w := int(math.Ceil(float64(rate) / float64(popBatch) * 0.15 * 2.0))
	if w < 8 {
		w = 8
	}
	return w
}

func runCMMode(args []string) {
	fs := flag.NewFlagSet("goload-cm", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:6640", "broker base URL")
	properties := fs.Int("properties", 1000, "number of properties (= partitions per queue): p0..p{N-1}")
	rateEvents := fs.Int("rate-events", 0, "TOTAL end-to-end events/s entering the system, split 50/50 flow A/B (required, >0)")
	rampSec := fs.Int("ramp-sec", 0, "linear ramp of the offered rate from 0 to full over N seconds (0 = full from t=0)")
	durationSec := fs.Int("duration", 0, "run duration seconds (0 = run until SIGINT)")
	popWait := fs.Bool("pop-wait", true, "consumers long-poll (Wait=true): empty pops park server-side instead of spinning")
	popTimeout := fs.Int("pop-timeout", 2000, "pop long-poll timeout ms (used when -pop-wait)")
	popBatch := fs.Int("pop-batch", 100, "max messages per pop (operator directive: 100)")
	popPartitions := fs.Int("pop-partitions", 10, "multi-partition pop cap (operator directive: 10). ORDER-SAFE: one property lives in one partition, a pop claims each partition exclusively (lease held to the bulk ack), and phase-2 emission is serial per partition — multi-partition batches only mix DIFFERENT properties")
	workersDB := fs.Int("workers-db", 0, "stage-A (cm-db) EXECUTOR cap: max concurrent in-work messages (0 = derive from rate)")
	workersCal := fs.Int("workers-cal", 0, "stage-B (cm-cal) EXECUTOR cap (0 = derive from rate)")
	workersOTA := fs.Int("workers-ota", 0, "PER-GROUP executor cap for the 10 final OTA groups (0 = derive from rate)")
	pushCostMs := fs.Int("push-cost-ms", 20, "estimated per-message SYNC derived-push cost (incl. broker fusion hold ~15ms) folded into the cm-db/cm-cal worker derivation; the derived push blocks the worker so it counts toward service time")
	pushShards := fs.Int("push-shards", 64, "ordered pusher shards per flow (bounds concurrent single-pushes per flow; property->shard is fixed so per-property order holds)")
	pushChanCap := fs.Int("push-chan-cap", 1024, "per-shard producer channel capacity (full => shed, no seq consumed)")
	dbSleepMin := fs.Int("db-sleep-min", 10, "stage-A per-message sleep min ms")
	dbSleepMax := fs.Int("db-sleep-max", 20, "stage-A per-message sleep max ms (uniform)")
	calSleep := fs.Int("cal-sleep", 10, "stage-B per-message sleep ms")
	otaSleep := fs.Int("ota-sleep", 30, "final OTA per-message sleep ms (both fan-outs)")
	dedupWindow := fs.Int("dedup-window", 300, "dedupWindowSeconds set on all 4 queues at t=0")
	completedRet := fs.Int("completed-retention", 300, "completedRetentionSeconds on all 4 queues")
	leaseTime := fs.Int("lease-time", 60, "leaseTime seconds on all 4 queues")
	payloadB := fs.Int("payload-b", 2048, "flow-B rates payload target bytes (~2KB)")
	reportSec := fs.Int("report", 1, "report interval seconds")
	idleConns := fs.Int("idle-conns", 2048, "MaxIdleConnsPerHost")
	ackInflight := fs.Int("ack-inflight", 1024, "cap on concurrently in-flight async acks (shared across all stages)")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms")
	logDir := fs.String("logdir", "/root/cmlogs", "directory for per-stage logs + produced.meta")
	queuePrefix := fs.String("queue-prefix", "", "prefix on all 4 queue names — isolates a run's queues/cursors so repeated LOCAL runs don't inherit each other's backlog (leave empty for the canonical cm-* names)")
	warmup := fs.Bool("warmup", true, "pre-push seq 0 for every property on both ingress flows at bounded concurrency, then drain the pipeline, BEFORE the pacer starts — creates+hydrates all partitions outside the rated window (a channel manager's properties pre-exist; cold mass-creation under rated load is a measured broker wedge)")
	warmupConc := fs.Int("warmup-conc", 96, "concurrent warm pushes per flow during -warmup")
	finalTargeted := fs.Bool("final-targeted", true, "final OTA stages use TARGETED pops with static partition ownership instead of wildcard pops (measured: the wildcard candidate scan at 1000 partitions costs ~12ms/pop and near-empty final queues drove ~30 PG cores of scan; targeted is ~0.15ms)")
	interTargeted := fs.Bool("intermediate-targeted", true, "intermediate stages (cm-db / cm-cal) also use TARGETED pops with static partition ownership (measured: allocator-update churn on log_partitions bloats the wildcard candidate scan to ~35ms at high single-push rates)")
	sweepFloorMs := fs.Int("sweep-floor-ms", 250, "with -final-targeted: minimum wall time per full partition sweep (bounds empty-pop rate at low load)")
	verifyOnly := fs.Bool("verify-only", false, "skip the run: verify existing logs in -logdir and exit")
	_ = fs.String("mode", "cm", "run mode")
	_ = fs.Parse(args)

	if *verifyOnly {
		ok := cmVerify(*logDir, *queuePrefix, 0, false)
		if !ok {
			os.Exit(1)
		}
		return
	}

	if *rateEvents <= 0 {
		fmt.Println("goload -mode cm: -rate-events must be > 0 (total e2e events/s)")
		os.Exit(2)
	}
	if *properties <= 0 {
		fmt.Println("goload -mode cm: -properties must be > 0")
		os.Exit(2)
	}
	if err := os.MkdirAll(*logDir, 0o755); err != nil {
		fmt.Printf("cannot create logdir %s: %v\n", *logDir, err)
		os.Exit(1)
	}

	rateA := *rateEvents / 2
	rateB := *rateEvents - rateA
	// Executor caps (concurrent in-work messages per stage): rate × sleep × 1.5.
	// The derived push no longer sits in the executor's service time — emission
	// happens per-partition-parallel AFTER the work barrier (pushCostMs only
	// informs the pop-cycle estimate below).
	if *workersDB == 0 {
		*workersDB = cmDeriveWorkers(rateA, float64(*dbSleepMin+*dbSleepMax)/2)
	}
	if *workersCal == 0 {
		*workersCal = cmDeriveWorkers(rateB, float64(*calSleep))
	}
	if *workersOTA == 0 {
		*workersOTA = cmDeriveWorkers(rateA, float64(*otaSleep)) // per group; rateA≈rateB
	}
	_ = pushCostMs
	// Pop workers per stage (each owns pop→barrier→emit→ack cycles).
	popWDB := cmDerivePopWorkers(rateA, *popBatch)
	popWCal := cmDerivePopWorkers(rateB, *popBatch)
	popWOTA := cmDerivePopWorkers(rateA, *popBatch) // per group
	// Executor semaphores (shared per stage; OTA groups each get their own).
	semDB := make(chan struct{}, *workersDB)
	semCal := make(chan struct{}, *workersCal)
	if *pushShards > *properties {
		*pushShards = *properties
	}

	// Actual (optionally prefixed) queue names.
	qAvail := *queuePrefix + cmQAvail
	qPrices := *queuePrefix + cmQPrices
	qOtaSync := *queuePrefix + cmQOtaSync
	qOtaPrices := *queuePrefix + cmQOtaPrices

	fmt.Printf("goload -mode cm -> %s properties=%d rate-events=%d (A=%d B=%d) ramp=%ds duration=%ds\n",
		*url, *properties, *rateEvents, rateA, rateB, *rampSec, *durationSec)
	fmt.Printf("  executors: db=%d cal=%d ota=%d/group | pop-workers: db=%d cal=%d ota=%d/group x%d groups | push-shards=%d/flow pop-batch=%d pop-partitions=%d pop-wait=%v\n",
		*workersDB, *workersCal, *workersOTA, popWDB, popWCal, popWOTA, cmOtaFanout*2, *pushShards, *popBatch, *popPartitions, *popWait)
	fmt.Printf("  queues: dedupWindow=%ds completedRetention=%ds leaseTime=%ds | logdir=%s\n",
		*dedupWindow, *completedRet, *leaseTime, *logDir)

	q, err := queen.New(queen.ClientConfig{
		URL:                 *url,
		TimeoutMillis:       *timeoutMs,
		MaxIdleConnsPerHost: *idleConns,
		RetryAttempts:       3, // idempotent via dedupWindow; keeps scheduled seqs from being lost on transient errors
	})
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure all 4 queues once at t=0.
	for _, qn := range []string{qAvail, qPrices, qOtaSync, qOtaPrices} {
		cfgCtx, cfgCancel := context.WithTimeout(ctx, 10*time.Second)
		_, cerr := q.GetHttpClient().Post(cfgCtx, "/api/v1/configure", map[string]interface{}{
			"queue": qn,
			"options": map[string]interface{}{
				"retentionEnabled":          true,
				"completedRetentionSeconds": *completedRet,
				"retentionSeconds":          0,
				"leaseTime":                 *leaseTime,
				"dedupWindowSeconds":        *dedupWindow,
			},
		})
		cfgCancel()
		if cerr != nil {
			fmt.Printf("[configure] WARNING %s: %v\n", qn, cerr)
		}
	}
	fmt.Printf("[configure] %s %s %s %s ready\n", qAvail, qPrices, qOtaSync, qOtaPrices)

	// Log writers (12: 1 db + 5 ota + 1 cal + 5 otap).
	logs := map[string]*cmLog{}
	openLog := func(queue, group string) *cmLog {
		l, e := newCMLog(*logDir, queue, group)
		if e != nil {
			fmt.Printf("cannot open log %s_%s: %v\n", queue, group, e)
			os.Exit(1)
		}
		logs[queue+"_"+group] = l
		return l
	}
	dbLog := openLog(qAvail, cmGrpDB)
	calLog := openLog(qPrices, cmGrpCal)
	otaLogs := make([]*cmLog, cmOtaFanout)
	otapLogs := make([]*cmLog, cmOtaFanout)
	for i := 0; i < cmOtaFanout; i++ {
		otaLogs[i] = openLog(qOtaSync, "ota-"+strconv.Itoa(i+1))
		otapLogs[i] = openLog(qOtaPrices, "otap-"+strconv.Itoa(i+1))
	}

	// Counters.
	var producedA, producedB, pushShed, pushErr, pushRetry int64
	var dbTotal, calTotal, dbDerived, calDerived int64
	var otaTotal, otapTotal, acked, ackErr int64
	otaN := make([]int64, cmOtaFanout)
	otapN := make([]int64, cmOtaFanout)
	lat := newOLHist()

	// Per-(property) seq high-water marks, owned by each flow's single pacer.
	seqA := make([]int64, *properties)
	seqB := make([]int64, *properties)
	ratesPad := cmRatesPad(*payloadB)

	// Shared async-ack plumbing.
	ackSem := make(chan struct{}, *ackInflight)
	var ackWg sync.WaitGroup
	ackCtx, ackCancel := context.WithCancel(context.Background())
	defer ackCancel()

	var wg sync.WaitGroup

	// Consumers first (so their group subscriptions exist before production;
	// subscriptionMode defaults to "all" so this is belt-and-suspenders).
	if *interTargeted {
		cmIntermediateStageTargeted(ctx, ackCtx, &wg, &ackWg, q, qAvail, cmGrpDB, qOtaSync,
			popWDB, semDB, *dbSleepMin, *dbSleepMax, *popBatch, *properties, *sweepFloorMs,
			dbLog, ackSem, &dbTotal, &dbDerived, &acked, &ackErr, &pushErr, &pushRetry)
		cmIntermediateStageTargeted(ctx, ackCtx, &wg, &ackWg, q, qPrices, cmGrpCal, qOtaPrices,
			popWCal, semCal, *calSleep, *calSleep, *popBatch, *properties, *sweepFloorMs,
			calLog, ackSem, &calTotal, &calDerived, &acked, &ackErr, &pushErr, &pushRetry)
	} else {
		cmIntermediateStage(ctx, ackCtx, &wg, &ackWg, q, qAvail, cmGrpDB, qOtaSync,
			popWDB, semDB, *dbSleepMin, *dbSleepMax, *popBatch, *popTimeout, *popWait, *popPartitions,
			dbLog, ackSem, &dbTotal, &dbDerived, &acked, &ackErr, &pushErr, &pushRetry)
		cmIntermediateStage(ctx, ackCtx, &wg, &ackWg, q, qPrices, cmGrpCal, qOtaPrices,
			popWCal, semCal, *calSleep, *calSleep, *popBatch, *popTimeout, *popWait, *popPartitions,
			calLog, ackSem, &calTotal, &calDerived, &acked, &ackErr, &pushErr, &pushRetry)
	}
	for i := 0; i < cmOtaFanout; i++ {
		if *finalTargeted {
			cmFinalStageTargeted(ctx, ackCtx, &wg, &ackWg, q, qOtaSync, "ota-"+strconv.Itoa(i+1),
				popWOTA, make(chan struct{}, *workersOTA), *otaSleep, *popBatch, *properties, *sweepFloorMs,
				otaLogs[i], ackSem, lat, &otaTotal, &otaN[i], &acked, &ackErr)
			cmFinalStageTargeted(ctx, ackCtx, &wg, &ackWg, q, qOtaPrices, "otap-"+strconv.Itoa(i+1),
				popWOTA, make(chan struct{}, *workersOTA), *otaSleep, *popBatch, *properties, *sweepFloorMs,
				otapLogs[i], ackSem, lat, &otapTotal, &otapN[i], &acked, &ackErr)
		} else {
			cmFinalStage(ctx, ackCtx, &wg, &ackWg, q, qOtaSync, "ota-"+strconv.Itoa(i+1),
				popWOTA, make(chan struct{}, *workersOTA), *otaSleep, *popBatch, *popTimeout, *popWait, *popPartitions,
				otaLogs[i], ackSem, lat, &otaTotal, &otaN[i], &acked, &ackErr)
			cmFinalStage(ctx, ackCtx, &wg, &ackWg, q, qOtaPrices, "otap-"+strconv.Itoa(i+1),
				popWOTA, make(chan struct{}, *workersOTA), *otaSleep, *popBatch, *popTimeout, *popWait, *popPartitions,
				otapLogs[i], ackSem, lat, &otapTotal, &otapN[i], &acked, &ackErr)
		}
	}

	// Let subscriptions establish.
	time.Sleep(300 * time.Millisecond)

	if *warmup {
		fmt.Printf("[warmup] seq-0 push for %d properties x 2 flows (conc=%d/flow)...\n", *properties, *warmupConc)
		t0 := time.Now()
		werr := make(chan error, 2)
		go func() {
			werr <- cmWarmup(ctx, q, qAvail, "A", *properties, *warmupConc, nil, &producedA, &pushRetry)
		}()
		go func() {
			werr <- cmWarmup(ctx, q, qPrices, "B", *properties, *warmupConc, ratesPad, &producedB, &pushRetry)
		}()
		for i := 0; i < 2; i++ {
			if err := <-werr; err != nil {
				fmt.Printf("[warmup] FAILED: %v\n", err)
				os.Exit(1)
			}
		}
		fmt.Printf("[warmup] ingress done in %.1fs, draining pipeline...\n", time.Since(t0).Seconds())
		// Drain: the warm wave flowing through creates + hydrates both derived
		// queues' partitions and seeds all 12 group cursors, so the rated
		// window starts from a warm, empty system.
		wantI := int64(*properties)
		wantF := int64(cmOtaFanout * *properties)
		deadline := time.Now().Add(4 * time.Minute)
		for {
			db, cal := atomic.LoadInt64(&dbTotal), atomic.LoadInt64(&calTotal)
			ota, otap := atomic.LoadInt64(&otaTotal), atomic.LoadInt64(&otapTotal)
			if db >= wantI && cal >= wantI && ota >= wantF && otap >= wantF {
				break
			}
			if time.Now().After(deadline) {
				fmt.Printf("[warmup] DRAIN TIMEOUT: db=%d/%d cal=%d/%d ota=%d/%d otap=%d/%d\n",
					db, wantI, cal, wantI, ota, wantF, otap, wantF)
				os.Exit(1)
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("[warmup] complete in %.1fs — starting rated load\n", time.Since(t0).Seconds())
	}

	cmRunProducer(ctx, &wg, q, qAvail, "A", rateA, *rampSec, *properties, *pushShards, *pushChanCap,
		seqA, nil, &producedA, &pushShed, &pushErr, &pushRetry)
	cmRunProducer(ctx, &wg, q, qPrices, "B", rateB, *rampSec, *properties, *pushShards, *pushChanCap,
		seqB, ratesPad, &producedB, &pushShed, &pushErr, &pushRetry)

	// Reporter.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer t.Stop()
		prev := make([]int64, olNumBuckets)
		cur := make([]int64, olNumBuckets)
		diff := make([]int64, olNumBuckets)
		var lpa, lpb, ldb, lcal, lota, lotap, lack int64
		minOf := func(s []int64) int64 {
			m := s[0]
			for _, v := range s[1:] {
				if v < m {
					m = v
				}
			}
			return m
		}
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				secs := float64(*reportSec)
				pa, pb := atomic.LoadInt64(&producedA), atomic.LoadInt64(&producedB)
				db, cal := atomic.LoadInt64(&dbTotal), atomic.LoadInt64(&calTotal)
				ota, otap := atomic.LoadInt64(&otaTotal), atomic.LoadInt64(&otapTotal)
				ack := atomic.LoadInt64(&acked)
				dbd, cald := atomic.LoadInt64(&dbDerived), atomic.LoadInt64(&calDerived)
				lat.snapshot(cur)
				for i := range diff {
					diff[i] = cur[i] - prev[i]
					prev[i] = cur[i]
				}
				var minOta, minOtap int64
				sn := make([]int64, cmOtaFanout)
				for i := range otaN {
					sn[i] = atomic.LoadInt64(&otaN[i])
				}
				minOta = minOf(sn)
				for i := range otapN {
					sn[i] = atomic.LoadInt64(&otapN[i])
				}
				minOtap = minOf(sn)
				e2eCompleted := (float64(ota-lota) + float64(otap-lotap)) / float64(cmOtaFanout) / secs
				fmt.Printf("[%s] prodA=%6.0f prodB=%6.0f | db=%6.0f cal=%6.0f ota=%7.0f otap=%7.0f acked=%7.0f e2e=%6.0f/s | p50=%6.2f p99=%7.2f ms | lag avail=%d prices=%d otaSync=%d otaPrices=%d | ackErr=%d pushErr=%d shed=%d gor=%d\n",
					time.Now().UTC().Format("15:04:05"),
					float64(pa-lpa)/secs, float64(pb-lpb)/secs,
					float64(db-ldb)/secs, float64(cal-lcal)/secs,
					float64(ota-lota)/secs, float64(otap-lotap)/secs,
					float64(ack-lack)/secs, e2eCompleted,
					olPercentile(diff, 0.50), olPercentile(diff, 0.99),
					pa-db, pb-cal, dbd-minOta, cald-minOtap,
					atomic.LoadInt64(&ackErr), atomic.LoadInt64(&pushErr), atomic.LoadInt64(&pushShed),
					runtime.NumGoroutine())
				lpa, lpb, ldb, lcal, lota, lotap, lack = pa, pb, db, cal, ota, otap, ack
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if *durationSec > 0 {
		go func() { time.Sleep(time.Duration(*durationSec) * time.Second); cancel() }()
	}
	select {
	case <-sigCh:
		fmt.Println("\n[signal] stopping...")
		cancel()
	case <-ctx.Done():
	}
	close(stop)

	// Wait for producers + consumers to unwind (bounded).
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		fmt.Println("[shutdown] worker drain timed out (15s) — proceeding")
	}
	// Drain in-flight async acks, then release ackCtx.
	drainAsyncAcks(true, &ackWg, ackCancel)

	// Flush logs.
	for _, l := range logs {
		l.close()
	}

	// produced.meta: ground-truth seq high-water per (flow,prop) + run ackErr.
	metaBase := int64(1)
	if *warmup {
		metaBase = 0
	}
	writeMeta(*logDir, seqA, seqB, atomic.LoadInt64(&ackErr),
		atomic.LoadInt64(&producedA), atomic.LoadInt64(&producedB), metaBase)

	_ = q.Close(context.Background())

	fmt.Printf("\n[final] producedA=%d producedB=%d | db=%d cal=%d ota=%d otap=%d | acked=%d ackErr=%d pushErr=%d pushRetry=%d shed=%d\n",
		atomic.LoadInt64(&producedA), atomic.LoadInt64(&producedB),
		atomic.LoadInt64(&dbTotal), atomic.LoadInt64(&calTotal),
		atomic.LoadInt64(&otaTotal), atomic.LoadInt64(&otapTotal),
		atomic.LoadInt64(&acked), atomic.LoadInt64(&ackErr),
		atomic.LoadInt64(&pushErr), atomic.LoadInt64(&pushRetry), atomic.LoadInt64(&pushShed))

	// Built-in verifier (uses the run's real ackErr).
	ok := cmVerify(*logDir, *queuePrefix, atomic.LoadInt64(&ackErr), true)
	if !ok {
		os.Exit(1)
	}
}

// writeMeta persists the per-(flow,property) seq high-water marks (only props
// that produced anything) plus a header with the run's ackErr, totals, and the
// first expected seq (base=0 with -warmup, else 1 — the verifier's gap span).
func writeMeta(dir string, seqA, seqB []int64, ackErr, producedA, producedB, baseSeq int64) {
	f, err := os.Create(filepath.Join(dir, "produced.meta"))
	if err != nil {
		fmt.Printf("[meta] WARNING cannot write produced.meta: %v\n", err)
		return
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()
	fmt.Fprintf(w, "# ackErr=%d producedA=%d producedB=%d base=%d ts=%d\n", ackErr, producedA, producedB, baseSeq, time.Now().Unix())
	for prop, mx := range seqA {
		if mx > 0 {
			fmt.Fprintf(w, "A %d %d\n", prop, mx)
		}
	}
	for prop, mx := range seqB {
		if mx > 0 {
			fmt.Fprintf(w, "B %d %d\n", prop, mx)
		}
	}
}
