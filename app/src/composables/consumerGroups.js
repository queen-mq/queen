// The Consumer groups tab, as data: the words a group's verdict is said in,
// which groups the tab shows (the queue picked in the list, else the list's
// namespace; the tab's search; "Lagging only") and in what order.
//
// Every verdict is useAttention's (groupState, groupAttention): nothing here
// draws a line of its own, so the grid's word, the detail's word and the
// queue list's mark cannot disagree about the same group.
//
// Pure: no Vue, no '@/' imports, so test/consumer-groups.test.js pins it.
import { inNamespace } from './navigation.js'
import { byLagInactiveLast, groupAttention, groupState } from './useAttention.js'

export const QUEUE_MODE = '__QUEUE_MODE__'

/**
 * A group's verdict as the page says it, by groupState: its mark (`.g`), its
 * word and its tone. Mute is two states with one shape: a native group that
 * has never consumed, and a Kafka group that is inactive — which may well
 * have consumed, so it is never called the first.
 */
export const VERDICT = {
  bad: { glyph: 'bad', word: 'Stuck', tone: 'bad' },
  warn: { glyph: 'warn', word: 'Lagging', tone: 'warn' },
  mute: { glyph: 'idle', word: 'Never consumed', tone: '' },
  inactive: { glyph: 'idle', word: 'Inactive', tone: '' },
  empty: { glyph: 'ok', word: 'No members', tone: '' },
  ok: { glyph: 'ok', word: 'Stable', tone: '' },
}

/** The verdict of one group, in the page's words. */
export const verdictOf = (g, linesFor) => VERDICT[groupState(g, linesFor)] || VERDICT.ok

/** The name a person reads: the synthetic queue-mode group is "Queue mode". */
export const groupLabel = (g) => (g?.name === QUEUE_MODE ? 'Queue mode' : String(g?.name ?? '') || '—')

/** Past its queue's lag lines: the row that says Lagging or Stuck. */
export function isBehind(g, linesFor) {
  const sev = groupAttention(g, linesFor)
  return sev === 'warn' || sev === 'bad'
}

/**
 * The groups the tab shows, in the order given:
 *   queue        the picked queue's groups only; '' for every queue
 *   namespace    with no queue picked, the queues of this namespace only
 *                (null for all; '' is the default namespace, a real one).
 *                `metaOf(queueName)` gives the queue's listing row; a queue the
 *                listing does not know is in no namespace.
 *   search       a group's name, or its queue's ("queue mode" finds those)
 *   laggingOnly  only the groups past their lag lines (isBehind)
 */
export function visibleGroups(groups = [], {
  queue = '', namespace = null, metaOf = () => undefined, search = '', laggingOnly = false, linesFor,
} = {}) {
  const term = String(search ?? '').trim().toLowerCase()
  return (groups || []).filter((g) => {
    if (!g) return false
    if (queue) {
      if (g.queueName !== queue) return false
    } else if (namespace !== null && namespace !== undefined) {
      const meta = metaOf(g.queueName)
      if (!meta || !inNamespace(meta, namespace)) return false
    }
    if (term && ![groupLabel(g), g.name, g.queueName].some((v) => typeof v === 'string' && v.toLowerCase().includes(term))) return false
    if (laggingOnly && !isBehind(g, linesFor)) return false
    return true
  })
}

/**
 * The lagging-partition rows (GET /consumer-groups/lagging, snake_case) the
 * tab shows: the same scope and search as its groups.
 */
export function visiblePartitions(rows = [], { queue = '', namespace = null, metaOf = () => undefined, search = '' } = {}) {
  const term = String(search ?? '').trim().toLowerCase()
  return (rows || []).filter((p) => {
    if (!p) return false
    if (queue) {
      if (p.queue_name !== queue) return false
    } else if (namespace !== null && namespace !== undefined) {
      const meta = metaOf(p.queue_name)
      if (!meta || !inNamespace(meta, namespace)) return false
    }
    const label = groupLabel({ name: p.consumer_group })
    return !term || [label, p.consumer_group, p.queue_name].some((v) => typeof v === 'string' && v.toLowerCase().includes(term))
  })
}

// Worst first: behind beats caught up, caught up beats never consumed, and an
// inactive Kafka group (whose lag grows for as long as nobody runs it) goes last.
const STATE_RANK = { bad: 0, warn: 1, ok: 2, empty: 2, mute: 3, inactive: 4 }
const num = (v) => {
  const x = Number(v)
  return v === null || v === undefined || v === '' || !Number.isFinite(x) ? 0 : x
}
const byName = (a, b) => groupLabel(a).localeCompare(groupLabel(b)) || String(a.queueName ?? '').localeCompare(String(b.queueName ?? ''))

export const SORTS = [
  { value: 'health', label: 'Worst first' },
  { value: 'lag', label: 'Time lag' },
  { value: 'members', label: 'Partitions' },
  { value: 'name', label: 'Name' },
]

/** The groups in the tab's order; `groups` is not modified. */
export function sortGroups(groups = [], sortBy = 'health', linesFor) {
  const list = [...(groups || [])].filter(Boolean)
  if (sortBy === 'name') return list.sort(byName)
  if (sortBy === 'members') return list.sort((a, b) => num(b.members) - num(a.members) || byName(a, b))
  if (sortBy === 'lag') return byLagInactiveLast(list, linesFor)
  const rank = (g) => STATE_RANK[groupState(g, linesFor)] ?? 2
  return list.sort((a, b) => rank(a) - rank(b) || num(b.maxTimeLag) - num(a.maxTimeLag) || byName(a, b))
}

/** "12 groups", or "3 of 12 groups" while the search or the box narrows them. */
export function groupCount(shown, total) {
  const noun = (n) => (n === 1 ? 'group' : 'groups')
  return shown === total ? `${total.toLocaleString('en-US')} ${noun(total)}` : `${shown.toLocaleString('en-US')} of ${total.toLocaleString('en-US')} ${noun(total)}`
}
