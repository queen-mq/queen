/**
 * Example 03 — Threshold alerting.
 *
 * Source `metrics.cpu` carries per-host CPU samples; we compute a 30-second
 * average per host and emit an alert to `alerts.cpu` whenever the average
 * exceeds 90.
 *
 * The pipeline shape:
 *   .windowTumbling({30s}) -> .aggregate({avg}) -> .filter(>90) -> .map(severity) -> .to(alerts.cpu)
 *
 * Note: `.filter` and `.map` after a window/reduce work on the per-window
 * aggregated value, not on the raw input — they run as the cycle's emit
 * pipeline. (In v0.1 these post-window stateless ops are inlined into the
 * sink — see Stream._compile().)
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/03-threshold-alerting.js
 */

import { Queen, Stream } from 'queen-mq'
const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen(url)

// v0.1: post-window operators are not yet implemented in the chain
// compiler. As a workaround, we materialise the average via aggregate and
// gate the sink emit by emitting only when avg > 90 — the .map happens
// inside a custom reducer instead.
const handle = await Stream
  .from(q.queue('metrics.cpu'))
  .windowTumbling({ seconds: 30 })
  .reduce(
    (acc, m) => {
      const v = typeof m.data.value === 'number' ? m.data.value : 0
      return { sum: acc.sum + v, count: acc.count + 1 }
    },
    { sum: 0, count: 0 }
  )
  .map(agg => {
    const avg = agg.count > 0 ? agg.sum / agg.count : 0
    if (avg <= 90) return null
    return {
      severity: 'high',
      avg,
      windowSamples: agg.count,
      detectedAt: new Date().toISOString()
    }
  })
  .filter(v => v !== null)
  .to(q.queue('alerts.cpu'))
  .run({
    queryId: 'examples.alerts.cpu_high',
    url
  })

await new Promise(r => setTimeout(r, 5 * 60_000))
console.log('Stats:', handle.metrics())
await handle.stop()
await q.close()
