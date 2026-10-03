package core

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// maxCatchUp bounds the units a worker offers individually per wake (goload).
const maxCatchUp = 4096

// Pacer is goload's open-loop pacer (ref/goload-pe-build/main.go,
// runOpenLoopMode), ported with its schedule math unchanged. It paces UNITS.
//
// W workers (~50 units/s each, at most 64) each own a fixed schedule anchored
// at t0 plus a random phase in [0, step), so the W schedules interleave. On
// every wake (ticker clamped to [250µs, 1ms]) a worker computes from the WALL
// CLOCK how many scheduled instants are due, so a late wake still offers
// everything it owes: with ramp R the cumulative schedule is
// F(t) = rps·t²/(2R) for t < R, then rps·(t − R/2), and unit k is scheduled at
// F⁻¹(k) — the coordinated-omission-correct latency baseline. At most
// maxCatchUp owed units are offered individually per wake; any further backlog
// is bulk-counted as offered+shed and the schedule jumps forward.
//
// Launch must NEVER block (it tries the in-flight semaphore non-blockingly and
// sheds): a blocking launch would silently turn the pacer into a closed-loop
// worker pool.
type Pacer struct {
	UnitsPerSec float64
	Ramp        time.Duration
	// End stops offering: instants scheduled after End are never offered.
	// Zero = run until ctx is done.
	End  time.Time
	Seed int64
	// Launch offers the unit scheduled at sched, from worker w.
	Launch func(w int, sched time.Time)
	// Bulk counts n owed units of worker w as offered+shed (never sent).
	Bulk func(w int, n int64)
}

// PacerWorkers is goload's W = min(64, units/s / 50 + 1).
func PacerWorkers(unitsPerSec float64) int {
	W := int(unitsPerSec/50) + 1
	if W > 64 {
		W = 64
	}
	if W < 1 {
		W = 1
	}
	return W
}

// maxf: float max (goload).
func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// Run starts the workers at t0 and returns when they are done (End reached
// or ctx cancelled).
func (p *Pacer) Run(ctx context.Context, t0 time.Time) {
	if p.UnitsPerSec <= 0 {
		return
	}
	W := PacerWorkers(p.UnitsPerSec)
	perWorkerRPS := p.UnitsPerSec / float64(W)
	// Ticker cadence: clamp the per-worker spacing into [minTick, maxTick].
	//   - maxTick caps how coarse the wake is so that at LOW rates each unit
	//     is launched within ~maxTick of its scheduled instant.
	//   - minTick floors it so we don't spin a sub-ms ticker at very HIGH
	//     rates; there the catch-up loop launches several units per wake.
	minTick := 250 * time.Microsecond
	maxTick := 1 * time.Millisecond
	step := time.Duration(float64(time.Second) / perWorkerRPS)
	tickEvery := step
	if tickEvery > maxTick {
		tickEvery = maxTick
	}
	if tickEvery < minTick {
		tickEvery = minTick
	}
	ramp := p.Ramp.Seconds()
	rng := rand.New(rand.NewSource(p.Seed))
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		// Random phase in [0, step) so the W schedules interleave.
		offset := time.Duration(rng.Int63n(int64(step) + 1))
		base := t0.Add(offset)
		wg.Add(1)
		go func(w int, base time.Time) {
			defer wg.Done()
			tk := time.NewTicker(tickEvery)
			defer tk.Stop()
			var k int64 // number of units scheduled by this worker so far
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
				}
				now := time.Now()
				last := false
				if !p.End.IsZero() && !now.Before(p.End) {
					now, last = p.End, true
				}
				if now.Before(base) {
					if last {
						return
					}
					continue
				}
				// targetK = # of scheduled instants with schedTime <= now.
				el := now.Sub(base).Seconds()
				var cum float64
				if ramp <= 0 || el >= ramp {
					cum = perWorkerRPS * (el - maxf(ramp, 0)/2)
				} else {
					cum = perWorkerRPS * el * el / (2 * ramp)
				}
				targetK := int64(cum) + 1
				owed := targetK - k
				if owed > 0 {
					indiv := owed
					var bulk int64
					if owed > maxCatchUp {
						indiv = maxCatchUp
						bulk = owed - indiv
					}
					for n := int64(0); n < indiv; n++ {
						// F⁻¹(k): during the ramp k = rps·t²/(2R) ⇒ t = √(2kR/rps);
						// after it t = k/rps + R/2.
						var schedSec float64
						kf := float64(k)
						if ramp > 0 && kf < perWorkerRPS*ramp/2 {
							schedSec = math.Sqrt(2 * kf * ramp / perWorkerRPS)
						} else {
							schedSec = kf/perWorkerRPS + maxf(ramp, 0)/2
						}
						sched := base.Add(time.Duration(schedSec * float64(time.Second)))
						k++
						p.Launch(w, sched)
					}
					if bulk > 0 {
						// Backlog beyond the per-wake cap: owed, but the rig is
						// already saturated, so count them as offered+shed and
						// jump the schedule forward.
						k += bulk
						p.Bulk(w, bulk)
					}
				}
				if last {
					return
				}
			}
		}(w, base)
	}
	wg.Wait()
}
