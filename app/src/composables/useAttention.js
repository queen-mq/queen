// What needs you — ONE rule, used by the Overview's issue list, the sidebar's
// marks, the Queues list and the Consumer groups list, so no two places on
// screen can disagree about the same queue.
//
//   a consumer group     judged by consumerGroupSeverity (useSeverity): its
//                        oldest unconsumed message past the queue's second
//                        lag line is bad, past the first is warn (5 min and
//                        1 min unless Settings moved them); a Dead group (the
//                        broker's word for one that has never consumed) is
//                        mute, not an alert.
//   a Kafka group        (kind 'kafka', read through the Kafka facade) the
//                        same, while it is there and for a grace after its
//                        last member left (groupActivity; 15 min unless
//                        Settings moved it). Past the grace, or with no
//                        presence to date it by, it is inactive: mute. Its
//                        cursors cannot say whether it reads — Kafka clients
//                        commit offsets and take no lease — so the broker's
//                        Dead says nothing about it.
//   a queue              the worst of its live groups; and warn when it holds
//                        messages that nothing is set up to read: no live
//                        group, no named group at all, not a topic the Kafka
//                        facade created, and Settings does not say no reader
//                        is expected there.
//
// Why a named group, in any state, takes the "no reader" alarm away: it is a
// subscription somebody made, and its own row tells its story — behind, never
// consumed, inactive. The queue's pending count (unconsumed by its slowest
// cursor) is then some group's backlog, not unread messages, and calling it
// unread is how a queue read by Kafka clients came to say "No reader" at two
// seconds of lag. A topic the Kafka facade created is never read in queue
// mode, and keeps its messages for whoever subscribes next, as Kafka topics
// do: nobody on it yet is not news. What is left is a queue only queue mode
// reads, where a queue-mode cursor that has never consumed is no reader
// (`deadOnly`).
//
// `linesFor` is the settings store's lookup, queue name → the lines in force
// for that queue (and its flags). Without one every queue is judged by the
// tenant's lines.
//
// The Overview adds one rule of its own, ack failures across the tenant: that
// one needs the hour's ack counts, which only the Overview fetches.
//
// Pure: no Vue, no '@/' imports, so test/attention.test.js pins it directly.
import { formatSpan } from './settingsDoc.js'
import { consumerGroupSeverity, groupActivity } from './useSeverity.js'

const RANK = { bad: 2, warn: 1 }

/** The synthetic group of the groupless pop. Every other name is a subscription. */
const QUEUE_MODE = '__QUEUE_MODE__'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

// Not a function when a caller hands groupAttention straight to Array.map,
// which passes the index here.
const linesOf = (linesFor, queue) => (typeof linesFor === 'function' ? linesFor(queue) : undefined)

/** A consumer group's verdict: 'bad' | 'warn' | 'ok' | 'mute' (Dead, or an inactive Kafka group). */
export function groupAttention(g, linesFor) {
  const lines = linesOf(linesFor, g?.queueName)
  if (groupActivity(g, lines) === 'inactive') return 'mute'
  return consumerGroupSeverity({ state: g?.state, maxTimeLag: g?.maxTimeLag }, lines)
}

/**
 * What a group's row says, one step finer than its verdict: groupAttention's,
 * except a Kafka group with no live member — 'inactive' past the grace, and
 * 'empty' inside it while its lag is fine (its consumers stopped five minutes
 * ago: that is not "Stable"). Behind is behind, members or not.
 *
 * @returns {'bad'|'warn'|'ok'|'mute'|'inactive'|'empty'}
 */
export function groupState(g, linesFor) {
  const activity = groupActivity(g, linesOf(linesFor, g?.queueName))
  if (activity === 'inactive') return 'inactive'
  const sev = groupAttention(g, linesFor)
  return activity === 'idle' && sev === 'ok' ? 'empty' : sev
}

/**
 * A Kafka group with no live member in a few words, for its row and its
 * detail: when it was last seen — on the broker's clock, so `inactiveSeconds`
 * and never this browser's — or why it cannot be dated. '' for a live group
 * and for a native one.
 */
export function groupPresence(g, linesFor) {
  const activity = groupActivity(g, linesOf(linesFor, g?.queueName))
  if (activity === null || activity === 'live') return ''
  const away = num(g.inactiveSeconds)
  if (away !== null) return `last seen ${formatSpan(away)} ago`
  // Alive, with members that no longer subscribe to this queue; else a facade
  // that reports no presence at all (an older broker, or presence turned off).
  return g.state === 'Empty' && num(g.liveMembers) === 0 && !g.lastSeenAt
    ? 'not subscribed by its members'
    : 'no member data'
}

/**
 * The queues that need you, in the order they were given, each as
 *   { name, sev: 'bad'|'warn', reason: 'lag'|'noReader', lag, pending, deadOnly }
 * `lag` is the worst live group's age in seconds; `deadOnly` says the queue has
 * a queue-mode cursor, but it has never consumed.
 */
export function queueAttention(queues = [], groups = [], linesFor) {
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
    const live = all.filter((g) => groupAttention(g, linesFor) !== 'mute')
    let sev = null
    let lag = 0
    for (const g of live) {
      const s = groupAttention(g, linesFor)
      lag = Math.max(lag, num(g.maxTimeLag) || 0)
      if ((RANK[s] || 0) > (RANK[sev] || 0)) sev = s
    }
    const named = all.some((g) => g.name !== QUEUE_MODE)
    if (sev === 'bad' || sev === 'warn') {
      out.push({ name: q.name, sev, reason: 'lag', lag, pending, deadOnly: false })
    } else if (pending > 0 && live.length === 0 && !named && q.kafkaTopic !== true
      && !linesOf(linesFor, q.name)?.noReaderOk) {
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
