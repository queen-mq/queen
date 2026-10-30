// Settings' list and the section it opens: the tenant's lines first, then
// every queue that has its own (settingsDoc.js, the stored document as the
// settings store reads it). Nothing is judged here: a section says which of
// its lines are its own and which come from below it. Pure:
// test/settings-side.test.js.

import {
  LINES, QUEUE_FLAGS, QUEUE_LINES, formatLine, formatSpan, lineGroups, summarizeQueue,
} from './settingsDoc.js'

const set = (v) => v !== undefined && v !== null

/** How many of the tenant's lines the document moves off the built-in table. */
export function movedCount(defaults) {
  const d = defaults && typeof defaults === 'object' ? defaults : {}
  return LINES.filter((m) => set(d[m.key])).length
}

/**
 * The queues with their own settings, by name: `{ name, summary, exists }`.
 * `known` is the set of queue names on the cluster, or null while it is not
 * read: then no queue is said not to exist.
 */
export function ownQueueEntries(queues, known = null) {
  const list = queues instanceof Map ? [...queues] : []
  return list
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, own]) => ({ name, summary: summarizeQueue(own), exists: known ? known.has(name) : true }))
}

/** The entries whose name holds `term`, case-blind; all of them for a blank one. */
export function matchEntries(entries, term = '') {
  const t = String(term || '').trim().toLowerCase()
  const list = Array.isArray(entries) ? entries : []
  return t ? list.filter((e) => String(e?.name || '').toLowerCase().includes(t)) : list
}

const said = (meta, value) => (!set(value) ? '—'
  : meta.unit === 's' ? formatSpan(value) : formatLine(meta, value))

/**
 * One queue's lines as they are in force: its own where it has one, the
 * tenant's (`tenantLines`, built-in with the document's defaults) otherwise.
 * `{ groups: [{ name, rows: [{ key, label, help, own, text }] }], flags, hasOwn }`.
 */
export function queueLinesInForce(own, tenantLines) {
  const mine = own && typeof own === 'object' ? own : {}
  const base = tenantLines && typeof tenantLines === 'object' ? tenantLines : {}
  return {
    groups: lineGroups(QUEUE_LINES).map((g) => ({
      name: g.name,
      rows: g.lines.map((meta) => {
        const isOwn = set(mine[meta.key])
        return {
          key: meta.key,
          label: meta.label,
          help: meta.help,
          own: isOwn,
          text: said(meta, isOwn ? mine[meta.key] : base[meta.key]),
        }
      }),
    })),
    flags: QUEUE_FLAGS.map((f) => ({ key: f.key, label: f.label, help: f.help, on: mine[f.key] === true })),
    hasOwn: QUEUE_LINES.some((m) => set(mine[m.key])) || QUEUE_FLAGS.some((f) => mine[f.key] === true),
  }
}

/** How many lines and flags a queue holds of its own. */
export function ownCount(own) {
  const mine = own && typeof own === 'object' ? own : {}
  return QUEUE_LINES.filter((m) => set(mine[m.key])).length + QUEUE_FLAGS.filter((f) => mine[f.key] === true).length
}

/**
 * The tenant's lines as they are in force, for a reader who may not move
 * them: the document's value where it moves one, the built-in (`builtIn`,
 * useSeverity THRESHOLDS) elsewhere. `{ groups: [{ name, rows }], moved }`,
 * rows as queueLinesInForce's.
 */
export function tenantLinesInForce(defaults, builtIn) {
  const d = defaults && typeof defaults === 'object' ? defaults : {}
  const base = builtIn && typeof builtIn === 'object' ? builtIn : {}
  const groups = lineGroups(LINES).map((g) => ({
    name: g.name,
    rows: g.lines.map((meta) => {
      const own = set(d[meta.key])
      return { key: meta.key, label: meta.label, help: meta.help, own, text: said(meta, own ? d[meta.key] : base[meta.key]) }
    }),
  }))
  return { groups, moved: movedCount(d) }
}

/**
 * The "Add a queue" menu. `names` are the cluster's queues, `taken` the ones
 * with settings of their own (a Map or a Set). Offered: the others whose
 * name holds `term`, by name, at most `limit` (`more` counts the rest).
 * `typed` is the typed name when no queue has it, so a queue can have its
 * lines before its first push; `own` is the typed name when that queue
 * already has its own, to open instead.
 */
export function queueChoices(names, taken, term = '', limit = 200) {
  const t = String(term || '').trim()
  const lower = t.toLowerCase()
  const has = (n) => Boolean(taken && typeof taken.has === 'function' && taken.has(n))
  const free = [...new Set((Array.isArray(names) ? names : []).filter((n) => typeof n === 'string' && n !== ''))]
    .filter((n) => !has(n))
  const match = free.filter((n) => !lower || n.toLowerCase().includes(lower)).sort((a, b) => a.localeCompare(b))
  return {
    rows: match.slice(0, limit),
    more: Math.max(0, match.length - limit),
    typed: t && !has(t) && !free.includes(t) ? t : '',
    own: t && has(t) ? t : '',
  }
}
