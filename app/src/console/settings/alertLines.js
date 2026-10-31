// Settings › Alerts: the lines of the console's settings document
// (composables/settingsDoc.js, KV queen.console / settings) in plain words,
// typed as a person says them ("90 s", "5 min", "2 h", "1%"), and what a save
// changes: the rows of its diff, the document it stores, and which queues go
// on or leave the Overview because of it.
//
// A draft is `{ tenant: { key: text }, queues: { name: { key: text | bool } } }`:
// only what was typed. A line nobody touched shows what is stored, so a save
// by somebody else moves it in place, and a typed field is left alone.
// A blank field means "inherit": the built-in line for the tenant's lines,
// the tenant's line for a queue's.
//
// Pure: no Vue, no '@/' imports, so test/settings-page.test.js runs it as is.
import {
  LINES, LINE_META, QUEUE_FLAGS, QUEUE_LINES, readSettings, resolveLines, withDefaults, withQueue,
} from '../../composables/settingsDoc.js'
import { THRESHOLDS } from '../../composables/useSeverity.js'
import { groupAttention, queueAttention } from '../../composables/useAttention.js'
import { acksText } from '../../composables/queueOps.js'

export const DOC_PATH = '/api/v1/kv/queen.console/settings'

// ---- words -----------------------------------------------------------------

/** The tenant's lines under four headings, each with what it measures. */
export const GROUPS = Object.freeze([
  { id: 'lag', title: 'Consumer lag', about: 'how long a group’s oldest message has waited' },
  { id: 'backlog', title: 'Backlog', about: 'what waits, as the time the acks take to clear it' },
  { id: 'acks', title: 'Ack failures', about: 'the share of acks that failed in the last hour' },
  { id: 'nodes', title: 'Nodes', about: 'a node’s memory and CPU' },
])

/**
 * Every line of the catalogue, in words. `what` sits under its heading on the
 * tenant's table; `label` names it alone, on a queue's row; `lead` comes
 * before the field ("over 1 min").
 */
export const WORDS = Object.freeze({
  lagWarnSeconds: { group: 'lag', what: 'Needs you', label: 'Lag needs you', lead: 'over' },
  lagBadSeconds: { group: 'lag', what: 'Failing', label: 'Lag failing', lead: 'over' },
  inactiveAfterSeconds: { group: 'lag', what: 'A Kafka group with no member is inactive', label: 'Kafka group inactive', lead: 'after' },
  backlogWarnSeconds: { group: 'backlog', what: 'Needs you', label: 'Backlog needs you', lead: 'at' },
  backlogBadSeconds: { group: 'backlog', what: 'Failing', label: 'Backlog failing', lead: 'at' },
  ackWarnRate: { group: 'acks', what: 'Needs you', label: 'Ack failures need you', lead: 'at' },
  ackBadRate: { group: 'acks', what: 'Failing', label: 'Acks failing', lead: 'at' },
  memWarnShare: { group: 'nodes', what: 'Memory needs you', label: 'Memory needs you', lead: 'at' },
  memBadShare: { group: 'nodes', what: 'Memory is failing', label: 'Memory failing', lead: 'at' },
  cpuWarnShare: { group: 'nodes', what: 'CPU needs you', label: 'CPU needs you', lead: 'at' },
  noReaderOk: { label: 'No reader is expected' },
})

/** The tenant's lines as headed groups: `[{ ...group, lines: [meta] }]`. */
export function tenantGroups() {
  return GROUPS.map((g) => ({ ...g, lines: LINES.filter((m) => WORDS[m.key]?.group === g.id) }))
}

// ---- a line as text --------------------------------------------------------

const SECONDS_MAX = 31_536_000 // a year, settingsDoc's own bound
const UNIT_S = {
  s: 1, sec: 1, secs: 1, second: 1, seconds: 1,
  m: 60, min: 60, mins: 60, minute: 60, minutes: 60,
  h: 3600, hr: 3600, hrs: 3600, hour: 3600, hours: 3600,
  d: 86400, day: 86400, days: 86400,
}

/** A stored line as it is typed back: '90 s', '5 min', '2 h', '1%'. '' for none. */
export function lineText(meta, v) {
  if (v === undefined || v === null || !Number.isFinite(v)) return ''
  if (meta.unit === 's') {
    if (v >= 3600 && v % 3600 === 0) return `${v / 3600} h`
    if (v >= 60 && v % 60 === 0) return `${v / 60} min`
    return `${v} s`
  }
  return `${Number((v * 100).toFixed(4))}%`
}

/**
 * Typed text as a line: null for a blank field, NaN for text that is not one.
 * Seconds take a unit (a bare number is seconds); a share is a percentage,
 * with or without its sign. A range is not checked here (lineErrors does).
 */
export function parseLine(meta, text) {
  const s = String(text ?? '').trim().toLowerCase()
  if (s === '') return null
  if (meta.unit === 's') {
    const m = /^(\d+(?:\.\d+)?)\s*([a-z]*)$/.exec(s)
    if (!m) return NaN
    const k = m[2] ? UNIT_S[m[2]] : 1
    if (!k) return NaN
    const v = Number(m[1]) * k
    const r = Math.round(v)
    return Math.abs(v - r) < 1e-9 ? r : NaN
  }
  const m = /^(\d+(?:\.\d+)?)\s*%?$/.exec(s)
  return m ? Number(m[1]) / 100 : NaN
}

const same = (a, b) => (a === undefined || a === null ? b === undefined || b === null
  : b !== undefined && b !== null && Math.abs(a - b) < 1e-9)

/**
 * `{ key: message }` for one scope's own lines. `values` holds what the
 * scope sets ({ key: number | null | NaN }, null = blank); `base` is what a
 * blank inherits (the built-in table, or the tenant's lines for a queue).
 * A line with a partner has to stay under it (attention before failing); the
 * clash is said on the field that was typed in (`typed`, a Set of keys; all
 * of `values` when absent), on the lower one when both were.
 */
export function lineErrors(base, values, perQueue = false, typed = null) {
  const errors = {}
  const merged = { ...base }
  const metas = perQueue ? QUEUE_LINES : LINES
  const isTyped = (k) => (typed ? typed.has(k) : Object.hasOwn(values, k) && values[k] !== null)
  for (const meta of metas) {
    if (!Object.hasOwn(values, meta.key)) continue
    const v = values[meta.key]
    if (v === null) continue
    if (Number.isNaN(v)) {
      errors[meta.key] = meta.unit === 's' ? 'Write a number and its unit, like 90 s, 5 min or 2 h.' : 'Write a percentage, like 5%.'
      continue
    }
    const ok = meta.unit === 's' ? Number.isInteger(v) && v >= 1 && v <= SECONDS_MAX : v > 0 && v <= 1
    if (!ok) { errors[meta.key] = meta.unit === 's' ? 'Between 1 s and a year.' : 'Above 0%, up to 100%.'; continue }
    merged[meta.key] = v
  }
  for (const meta of metas) {
    const up = meta.below
    if (!up || errors[meta.key] || errors[up] || !metas.some((m) => m.key === up)) continue
    if (merged[meta.key] < merged[up]) continue
    const low = lineText(meta, merged[meta.key])
    const high = lineText(LINE_META[up], merged[up])
    const lowTyped = isTyped(meta.key) && values[meta.key] !== null
    if (!lowTyped && isTyped(up)) {
      errors[up] = values[up] === null || values[up] === undefined
        ? `Removing it leaves the failing line at ${high}, under the line that needs you, ${low}.`
        : `Has to be over the line that needs you, ${low}.`
    } else errors[meta.key] = `Has to be under the failing line, ${high}.`
  }
  return errors
}

// ---- the draft over the stored document -------------------------------------

const has = (o, k) => !!o && Object.hasOwn(o, k)

/** One scope's own lines as the draft has them: stored, unless typed over. */
function ownValues(stored, typed, metas) {
  const out = {}
  for (const m of metas) {
    if (has(typed, m.key)) out[m.key] = parseLine(m, typed[m.key])
    else if (stored?.[m.key] !== undefined) out[m.key] = stored[m.key]
  }
  return out
}
const numbersOnly = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => typeof v === 'number' && Number.isFinite(v)))

/** The queues with lines of their own, stored or typed, by name. */
export function queueNames(settings, draft) {
  const names = new Set(settings.queues.keys())
  for (const [q, typed] of Object.entries(draft?.queues || {})) if (typed && Object.keys(typed).length) names.add(q)
  return [...names].sort((a, b) => a.localeCompare(b))
}

/**
 * Everything the view needs from the stored value and the draft:
 *   tenant   { values, errors, inForce }    inForce: built-in with the draft's valid lines
 *   queues   [{ name, stored, values, flags, errors, inForce }]
 *   rows     the diff: [{ scope: '' | queue, key, flag, before, after }]
 *   bad      how many fields cannot be saved as typed
 */
export function readDraft(raw, draft) {
  const settings = readSettings(raw)
  const typedTenant = draft?.tenant || {}
  const tValues = ownValues(settings.defaults, typedTenant, LINES)
  const tErrors = lineErrors(THRESHOLDS, tValues, false, new Set(Object.keys(typedTenant)))
  const tValid = Object.fromEntries(Object.entries(numbersOnly(tValues)).filter(([k]) => !tErrors[k]))
  const tInForce = { ...THRESHOLDS, ...tValid }
  const rows = []
  for (const m of LINES) {
    if (!has(typedTenant, m.key) || tErrors[m.key]) continue
    const before = settings.defaults[m.key]
    const after = tValues[m.key] ?? undefined
    if (!same(before, after)) rows.push({ scope: '', key: m.key, flag: false, before, after })
  }
  const queues = []
  for (const name of queueNames(settings, draft)) {
    const stored = settings.queues.get(name) || {}
    const typed = draft?.queues?.[name] || {}
    const values = ownValues(stored, typed, QUEUE_LINES)
    const errors = lineErrors(tInForce, values, true, new Set(Object.keys(typed)))
    const flags = {}
    for (const f of QUEUE_FLAGS) {
      if (has(typed, f.key)) flags[f.key] = typed[f.key] === true
      else if (stored[f.key] === true) flags[f.key] = true
    }
    queues.push({ name, stored, values, flags, errors, inForce: { ...tInForce, ...numbersOnly(values) } })
    for (const m of QUEUE_LINES) {
      if (!has(typed, m.key) || errors[m.key]) continue
      const before = stored[m.key]
      const after = values[m.key] ?? undefined
      if (!same(before, after)) rows.push({ scope: name, key: m.key, flag: false, before, after })
    }
    for (const f of QUEUE_FLAGS) {
      if (!has(typed, f.key)) continue
      const before = stored[f.key] === true
      if (before !== (typed[f.key] === true)) rows.push({ scope: name, key: f.key, flag: true, before, after: typed[f.key] === true })
    }
  }
  const bad = Object.keys(tErrors).length + queues.reduce((n, q) => n + Object.keys(q.errors).length, 0)
  return { settings, tenant: { values: tValues, errors: tErrors, inForce: tInForce }, queues, rows, bad }
}

/**
 * The value to store: the stored one with only the scopes the diff names
 * replaced (withDefaults / withQueue keep every field this console does not
 * know). A scope nobody changed is not rewritten, even if it was typed in.
 */
export function nextDocument(raw, read) {
  let doc = raw ?? null
  const scopes = new Set(read.rows.map((r) => r.scope))
  if (scopes.has('')) doc = withDefaults(doc, numbersOnly(read.tenant.values))
  for (const q of read.queues) {
    if (!scopes.has(q.name)) continue
    const own = numbersOnly(q.values)
    for (const [k, on] of Object.entries(q.flags)) if (on) own[k] = true
    doc = withQueue(doc, q.name, own)
  }
  return doc
}

/** A diff row's two sides as words: '90 s', 'built-in 1 min', 'the tenant’s 1 min', 'yes'. */
export function sideText(row, side, read) {
  const v = row[side]
  if (row.flag) return v ? 'yes' : 'no'
  const meta = LINE_META[row.key]
  if (v !== undefined && v !== null) return lineText(meta, v)
  if (!row.scope) return `built-in ${lineText(meta, THRESHOLDS[meta.key])}`
  // A queue's blank inherits the tenant's line: as it is now before, as the draft makes it after.
  const tenant = side === 'before' ? { ...THRESHOLDS, ...read.settings.defaults } : read.tenant.inForce
  return `the tenant’s ${lineText(meta, tenant[meta.key])}`
}

/** The PUT a save sends, as the dialog shows it before it runs. */
export function putCall(body, max = 700) {
  const json = JSON.stringify(body)
  if (json.length <= max) return { call: `PUT ${DOC_PATH} ${json}`, long: false }
  const rest = JSON.stringify({ ...body, value: '…' }).replace('"…"', '{…}')
  return { call: `PUT ${DOC_PATH} ${rest}`, long: true }
}

// ---- what a save moves on the Overview --------------------------------------

/** The lookup the console judges a queue by, for one stored value. */
export function linesForDoc(raw) {
  const s = readSettings(raw)
  const tenant = Object.freeze(resolveLines(THRESHOLDS, s))
  const own = new Map([...s.queues.keys()].map((q) => [q, Object.freeze(resolveLines(THRESHOLDS, s, q))]))
  return (q) => own.get(q) || tenant
}

const fmt = (n) => Math.round(Number(n) || 0).toLocaleString('en-US')

/** Seconds as the console says them: "38 s", "12 min", "2 h 5 min" (console/useOverview span). */
export function spanWords(seconds) {
  const s = Math.max(0, Math.round(Number(seconds) || 0))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  const h = Math.floor(s / 3600); const mm = Math.round((s % 3600) / 60)
  if (h < 24) return mm ? `${h} h ${mm} min` : `${h} h`
  const d = Math.floor(s / 86400); const hh = Math.round((s % 86400) / 3600)
  return hh ? `${d} d ${hh} h` : `${d} d`
}

const waiting = (n) => `${fmt(n)} waiting message${Math.round(Number(n) || 0) === 1 ? '' : 's'}`

/** Why a queue is on the Overview, as the end of a sentence. */
export function issueWords(i) {
  if (!i) return ''
  if (i.reason === 'lag') return `its slowest group is ${spanWords(i.lag)} behind`
  if (i.reason === 'acks') return `${acksText(i.ops)} in the last hour`
  return `nothing reads its ${waiting(i.pending)}`
}

/**
 * Where one queue stands now, by the lines in force (`linesFor`), for the
 * head of its block: `{ text, tone }`, tone '' | 'amber' | 'red'. `row` is
 * its row of the queues listing; null when the cluster has no such queue.
 */
export function queueNow(row, groups, linesFor, ops) {
  if (!row || !Array.isArray(groups)) return { text: '', tone: '' }
  const mine = groups.filter((g) => g?.queueName === row.name)
  const [i] = queueAttention([row], mine, linesFor, ops)
  if (i) {
    const tone = i.sev === 'bad' ? 'red' : 'amber'
    if (i.reason === 'lag') return { text: `slowest group ${spanWords(i.lag)} behind`, tone }
    if (i.reason === 'acks') return { text: `${acksText(i.ops)} in the last hour`, tone }
    return { text: `nothing reads its ${waiting(i.pending)}`, tone }
  }
  const live = mine.filter((g) => groupAttention(g, linesFor) !== 'mute')
  const pending = Number(row.messages?.pending) || 0
  if (!live.length) return { text: pending ? `${waiting(pending)}, no group reads it` : 'no group reads it', tone: '' }
  const lag = Math.max(0, ...live.map((g) => Number(g.maxTimeLag) || 0))
  return { text: lag > 0 ? `slowest group ${spanWords(lag)} behind` : 'every group caught up', tone: '' }
}

const and = (names) => (names.length < 2 ? names.join('') : `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`)
const listed = (items) => {
  const names = items.map((i) => `\`${i.name}\``)
  return items.length <= 3 ? and(names) : `${names.slice(0, 2).join(', ')} and ${items.length - 2} more`
}

/**
 * The queues that go on the Overview, leave it or change colour when the
 * lines move from `beforeRaw` to `afterRaw`, as sentences (names in
 * backticks, for mono). Judged by the one rule (useAttention.queueAttention)
 * on the numbers the console holds now.
 */
export function overviewMoves(queues, groups, ops, beforeRaw, afterRaw) {
  if (!Array.isArray(queues) || !queues.length || !Array.isArray(groups)) return []
  const before = new Map(queueAttention(queues, groups, linesForDoc(beforeRaw), ops).map((i) => [i.name, i]))
  const after = new Map(queueAttention(queues, groups, linesForDoc(afterRaw), ops).map((i) => [i.name, i]))
  const on = []; const off = []; const red = []; const amber = []
  for (const [name, i] of after) {
    const b = before.get(name)
    if (!b) on.push(i)
    else if (b.sev !== i.sev) (i.sev === 'bad' ? red : amber).push(i)
  }
  for (const [name, i] of before) if (!after.has(name)) off.push(i)
  const out = []
  if (on.length === 1) out.push(`${listed(on)} goes on the Overview: ${issueWords(on[0])}.`)
  else if (on.length) out.push(`${listed(on)} go on the Overview.`)
  if (off.length) out.push(`${listed(off)} ${off.length === 1 ? 'leaves' : 'leave'} the Overview.`)
  if (red.length) out.push(`${listed(red)} ${red.length === 1 ? 'turns' : 'turn'} from amber to red.`)
  if (amber.length) out.push(`${listed(amber)} ${amber.length === 1 ? 'turns' : 'turn'} from red to amber.`)
  return out
}

/** A sentence with `mono` parts, as spans: [{ t, mono }]. */
export function parts(s) {
  return String(s).split('`').map((t, i) => ({ t, mono: i % 2 === 1 })).filter((p) => p.t)
}
