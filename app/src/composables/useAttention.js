// What needs you — ONE rule, used by the Overview's issue list, the sidebar's
// marks, the Queues list and the Consumer groups list, so no two places on
// screen can disagree about the same queue.
//
//   a consumer group     judged by consumerGroupSeverity (useSeverity): its
//                        oldest unconsumed message 5 min old is bad, 1 min is
//                        warn; a Dead group (the broker's word for one that
//                        has never consumed) is mute, not an alert.
//   a queue              the worst of its live groups; and warn when it holds
//                        messages that no live group reads.
//
// The Overview adds one rule of its own, ack failures across the tenant: that
// one needs the hour's ack counts, which only the Overview fetches.
//
// Pure: no Vue, no '@/' imports, so test/attention.test.js pins it directly.
import { consumerGroupSeverity } from './useSeverity.js'

const RANK = { bad: 2, warn: 1 }

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/** A consumer group's verdict: 'bad' | 'warn' | 'ok' | 'mute' (Dead). */
export function groupAttention(g) {
  return consumerGroupSeverity({ state: g?.state, maxTimeLag: g?.maxTimeLag })
}

/**
 * The queues that need you, in the order they were given, each as
 *   { name, sev: 'bad'|'warn', reason: 'lag'|'noReader', lag, pending, deadOnly }
 * `lag` is the worst live group's age in seconds; `deadOnly` says the queue has
 * groups, but none of them has ever consumed.
 */
export function queueAttention(queues = [], groups = []) {
  const byQueue = new Map()
  for (const g of groups || []) {
    const q = g?.queueName
    if (!q) continue
    if (!byQueue.has(q)) byQueue.set(q, [])
    byQueue.get(q).push(g)
  }
  const out = []
  for (const q of queues || []) {
    if (!q?.name) continue
    const pending = num(q.messages?.pending) || 0
    const all = byQueue.get(q.name) || []
    const live = all.filter((g) => groupAttention(g) !== 'mute')
    let sev = null
    let lag = 0
    for (const g of live) {
      const s = groupAttention(g)
      lag = Math.max(lag, num(g.maxTimeLag) || 0)
      if ((RANK[s] || 0) > (RANK[sev] || 0)) sev = s
    }
    if (sev === 'bad' || sev === 'warn') {
      out.push({ name: q.name, sev, reason: 'lag', lag, pending, deadOnly: false })
    } else if (pending > 0 && live.length === 0) {
      out.push({ name: q.name, sev: 'warn', reason: 'noReader', lag, pending, deadOnly: all.length > 0 })
    }
  }
  return out
}

/** Worst severity and how many: { sev: 'bad'|'warn'|null, count }. */
export function summarize(sevs = []) {
  let sev = null
  let count = 0
  for (const s of sevs) {
    if (s !== 'bad' && s !== 'warn') continue
    count += 1
    if ((RANK[s] || 0) > (RANK[sev] || 0)) sev = s
  }
  return { sev, count }
}
