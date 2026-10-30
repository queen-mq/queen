// The Workload list, as data: every namespace — or every task, by the page's
// Group by — one line each, by name (the empty key, the real group of the
// queues without one, last), and the whole tenant above them. A click is the
// page's own drill-down into that group, on the right; the list itself never
// changes under it, and queues are not listed in it.
//
// The groups come from the queue list, as the page's own picker did, plus any
// key the workload rows carry that the list does not (a namespace quiet in
// the list but busy in the window), and the one the page is drilled into.
//
// A group's mark is the worst of its queues by useAttention's queueAttention —
// the rule the Queues list, the sidebar and the Overview share. Without the
// consumer groups there is no honest mark: `groups` null draws none rather
// than calling every queue with pending "unread". Its number is the pending
// messages of its queues, as the Queues list counts them.
//
// Pure: no Vue, no '@/' imports, so test/workload-side.test.js pins it.
import { queueAttention } from './useAttention.js'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/** "(no namespace)" / "(no task)" for the empty key. */
export const groupName = (key, groupBy) => key || (groupBy === 'task' ? '(no task)' : '(no namespace)')

/** The "all" row and the page's word for no pick: "All namespaces" / "All tasks". */
export const rootName = (groupBy) => (groupBy === 'task' ? 'All tasks' : 'All namespaces')

/** The Group by choices. */
export const GROUP_BY = [
  { value: 'namespace', label: 'Namespace' },
  { value: 'task', label: 'Task' },
]

/**
 * @returns {{
 *   root: { label, on, pending },
 *   rows: Array<{ key, label, sev, why, count, pending, on }>,
 *   groups: number, queues: number,
 * }}
 * `rows` are the groups the search names (the group itself, or one of its
 * queues); `groups` and `queues` count them all, whatever the search.
 */
export function workloadList({
  queues = [], groups = null, groupBy = 'namespace', focus = null, keys = [], search = '', linesFor,
} = {}) {
  const term = String(search || '').trim().toLowerCase()
  const byTask = groupBy === 'task'
  const keyOf = (q) => (byTask ? q.task : q.namespace) ?? ''
  const attention = groups
    ? new Map(queueAttention(queues, groups, linesFor).map((a) => [a.name, a]))
    : new Map()

  const nodes = new Map()
  const node = (key) => {
    if (!nodes.has(key)) nodes.set(key, { key, queues: [] })
    return nodes.get(key)
  }
  for (const k of keys || []) node(k ?? '')
  if (focus !== null && focus !== undefined) node(focus)
  let queueCount = 0
  for (const q of queues || []) {
    if (!q?.name) continue
    queueCount += 1
    node(keyOf(q)).queues.push({ name: q.name, sev: attention.get(q.name)?.sev || null, pending: num(q.messages?.pending) })
  }

  const sevOf = (list) => (list.some((q) => q.sev === 'bad') ? 'bad' : list.some((q) => q.sev === 'warn') ? 'warn' : null)
  const pendingOf = (list) => {
    const known = list.filter((q) => q.pending !== null)
    return known.length ? known.reduce((s, q) => s + q.pending, 0) : null
  }
  const all = [...nodes.values()].map((n) => {
    const needs = n.queues.filter((q) => q.sev).length
    const label = groupName(n.key, groupBy)
    return {
      key: n.key,
      label,
      sev: sevOf(n.queues),
      why: needs ? `${needs} ${needs === 1 ? 'queue needs' : 'queues need'} attention` : '',
      count: n.queues.length,
      pending: pendingOf(n.queues),
      on: focus === n.key,
      shown: !term || label.toLowerCase().includes(term) || n.queues.some((q) => q.name.toLowerCase().includes(term)),
    }
  })
  const flat = [...nodes.values()].flatMap((n) => n.queues)
  return {
    root: { label: rootName(groupBy), on: focus === null || focus === undefined, pending: pendingOf(flat) },
    rows: all
      .filter((n) => n.shown)
      .map(({ shown: _shown, ...rest }) => rest)
      .sort((a, b) => (a.key === '' ? 1 : b.key === '' ? -1 : a.key.localeCompare(b.key))),
    groups: nodes.size,
    queues: queueCount,
  }
}
