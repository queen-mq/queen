// The State page's words and rules: KV keys, locks and semaphores, timers.
// PURE (no Vue, no API, no '@/' imports), so `node --test` reaches every rule
// here; the reads and the writes live in ../useStatePage.js.
//
// Three rules worth knowing before changing anything:
//
//  1. A VALUE IS FLAGGED ONLY PAST A LINE. The broker refuses a value whose
//     compact JSON is over QUEEN_KV_MAX_VALUE_BYTES (64 KiB by default,
//     server/src/rsm/planner/kv.rs MAX_VALUE_BYTES_DEFAULT), so the line is
//     three quarters of that: a write that grows such a value is close to
//     being refused. Amber only when it will not fix itself, that is when the
//     key has no expiry; a key that expires is flagged in grey words.
//  2. EXPIRY IS THE BROKER'S VERDICT. A row's `expired` comes from the broker's
//     own clock (useKvView.js, rule 3); this file never decides it from
//     `expiresAt` and the browser's clock.
//  3. EACH NUMBER ONCE. The summary line carries the totals (keys, bytes,
//     locks, timers); the namespace list carries one count per namespace;
//     nothing else on the page repeats them.
import { parseBrokerInstant } from '../../composables/useTimers.js'
import { valueBytes } from '../../composables/useKvView.js'

/** The broker's default ceiling on one value's compact JSON. */
export const VALUE_CAP = 65_536
/** Past this share of the ceiling a value is flagged. */
export const NEAR_SHARE = 0.75
/** The broker's namespace rule (server/src/rsm/planner/kv.rs check_namespace). */
export const NAMESPACE_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/
/** The broker's key ceiling, in bytes (MAX_KEY_BYTES). */
export const KEY_MAX_BYTES = 512
/** Keys per page. */
export const PAGE = 50

const ENCODER = new TextEncoder()
const utf8 = (s) => ENCODER.encode(String(s)).length
const enc = encodeURIComponent

// ---- words ----------------------------------------------------------------

export const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
export const plural = (n, one, many = `${one}s`) => (n === 1 ? one : many)
export const clip = (s, n = 120) => {
  const t = String(s ?? '')
  return t.length > n ? `${t.slice(0, n - 1)}…` : t
}

/** 1024-based, the console's own rule: `161 KB`, `1.9 MB`. */
export function bytes(b) {
  if (b == null || !Number.isFinite(Number(b))) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = Number(b)
  let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}

/**
 * The console's span (console/useOverview.js), here so node --test can reach
 * it. Rounded once, then split, so 1 d 23 h 59 min reads `2 d`, never `1 d 24 h`.
 */
export function span(seconds) {
  const s = Math.max(0, Math.round(seconds))
  if (s < 60) return `${s} s`
  const mins = Math.round(s / 60)
  if (mins < 60) return `${mins} min`
  if (mins < 24 * 60) {
    const h = Math.floor(mins / 60)
    const mm = mins % 60
    return mm ? `${h} h ${mm} min` : `${h} h`
  }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24)
  const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}

/** A broker stamp (six fractional digits) → epoch ms, or null. */
export function msOf(v) {
  const d = parseBrokerInstant(v)
  return d ? d.getTime() : null
}

/** `in 4 min`, `4 min ago`, `now`. */
export function inOrAgo(ms, now = Date.now()) {
  if (ms == null) return ''
  const d = ms - now
  if (Math.abs(d) < 1000) return 'now'
  return d > 0 ? `in ${span(d / 1000)}` : `${span(-d / 1000)} ago`
}

/** `just now`, `4 min ago`. */
export function ago(ms, now = Date.now()) {
  if (ms == null) return ''
  const d = now - ms
  return d < 1000 ? 'just now' : `${span(d / 1000)} ago`
}

/** A timer's time against now: `in 4 min`, `4 min late`, `now`. */
export function fires(ms, now = Date.now()) {
  if (ms == null) return ''
  const d = ms - now
  if (Math.abs(d) < 1000) return 'now'
  return d > 0 ? `in ${span(d / 1000)}` : `${span(-d / 1000)} late`
}

// ---- values ---------------------------------------------------------------

/**
 * A string that is base64 of bytes that are not text: binary, or encrypted.
 * Narrow on purpose: long, the base64 alphabet only, upper and lower case and
 * a digit (a lowercase hex id or a word is not it), and bytes that do not
 * decode as UTF-8 text.
 */
export function looksBinary(s) {
  if (typeof s !== 'string' || s.length < 64) return false
  if (!/^[A-Za-z0-9+/_-]+={0,2}$/.test(s)) return false
  if (!/[A-Z]/.test(s) || !/[a-z]/.test(s) || !/[0-9]/.test(s)) return false
  let raw
  try { raw = atob(s.replace(/-/g, '+').replace(/_/g, '/')) } catch { return false }
  const b = new Uint8Array(raw.length)
  for (let i = 0; i < raw.length; i++) b[i] = raw.charCodeAt(i)
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(b)
    // Text, but full of control characters, is not text either.
    return /[\u0000-\u0008\u000e-\u001f]/.test(text)
  } catch {
    return true
  }
}

/**
 * One value as the page shows it: `{kind, size, preview, text}`.
 *   json    an object, an array, a number, a boolean or null: `text` is pretty
 *   text    a JSON string: `text` is the string itself
 *   binary  a string that is base64 of bytes (looksBinary): not shown
 *   none    the row carries no value (a keysOnly page, which this page never asks for)
 */
export function valueView(row) {
  if (!row || typeof row !== 'object' || !('value' in row)) return { kind: 'none', size: null, preview: '', text: '' }
  const v = row.value
  const size = valueBytes(row)
  if (typeof v === 'string') {
    if (looksBinary(v)) return { kind: 'binary', size, preview: '', text: v }
    return { kind: 'text', size, preview: clip(JSON.stringify(v), 160), text: v }
  }
  const compact = JSON.stringify(v)
  const pretty = v !== null && typeof v === 'object' ? JSON.stringify(v, null, 2) : compact
  return { kind: 'json', size, preview: clip(compact, 160), text: pretty }
}

/**
 * A value past the line (rule 1), or null: `{tone, badge, long}`.
 * `tone` is 'amber' when the key never expires, '' when it does. The words
 * name no number: the page's flag line says the limit once.
 */
export function sizeFlag(size, { expires = false, cap = VALUE_CAP } = {}) {
  if (!Number.isFinite(size) || size < cap * NEAR_SHARE) return null
  return {
    tone: expires ? '' : 'amber',
    badge: 'Near the limit for one value',
    long: 'A write that makes it bigger is refused. ' +
      (expires ? 'The key expires, so it goes by itself.' : 'The key never expires, so it stays until someone changes it.'),
  }
}

/** The page's rows, in the broker's order (name order). */
export function keyRows(page) {
  const rows = Array.isArray(page?.rows) ? page.rows : []
  return rows.map((r) => {
    const v = valueView(r)
    const expiresMs = msOf(r.expiresAt)
    return {
      key: r.key,
      raw: r,
      kind: v.kind,
      preview: v.preview,
      text: v.text,
      size: v.size,
      flag: sizeFlag(v.size, { expires: expiresMs != null }),
      version: Number.isFinite(r.version) ? r.version : null,
      updatedMs: msOf(r.updatedAt),
      expiresMs,
      expired: r.expired === true,
    }
  })
}

/** The Expires cell: `in 6 d 23 h`, `expired`, or '' for a key that never expires. */
export function expiresText(row, now = Date.now()) {
  if (!row) return ''
  if (row.expired) return 'expired'
  return row.expiresMs == null ? '' : inOrAgo(row.expiresMs, now)
}

/**
 * The flag line over a page, `{tone, badge, text}`: how many values on it are
 * past the line. Amber when one of them never expires.
 */
export function flagLine(rows, cap = VALUE_CAP) {
  const flagged = (Array.isArray(rows) ? rows : []).filter((r) => r.flag)
  if (!flagged.length) return null
  const stays = flagged.some((r) => r.flag.tone === 'amber')
  const n = flagged.length
  const them = n === 1 ? 'it' : 'them'
  return {
    tone: stays ? 'amber' : '',
    badge: `${n === 1 ? 'One value' : `${fmt(n)} values`} near ${bytes(cap)}`,
    text: `${n === 1 ? 'That is' : 'Each is'} close to the broker's default limit for one value: a write that makes ${them} bigger is refused.` +
      (stays ? '' : ` ${n === 1 ? 'It expires' : 'They expire'}, so ${n === 1 ? 'it goes' : 'they go'} by ${n === 1 ? 'itself' : 'themselves'}.`),
  }
}

// ---- the summary line -----------------------------------------------------

const cap1 = (s) => (s ? s[0].toUpperCase() + s.slice(1) : s)

/**
 * The page's one line of totals, each number once (rule 3).
 *   kv      {keys, bytes, namespaces} | {unavailable} | null while the namespaces are read
 *   locks   {locks, semaphores} | {unavailable} | null while they are read
 *   timers  {count, queues, more} | {reading, total} | {unavailable} | null
 * A family the broker does not serve is left out of the line: its view says why.
 */
export function summaryLine({ kv = null, locks = null, timers = null } = {}) {
  let first
  if (!kv) first = 'Reading the KV namespaces…'
  else if (kv.unavailable) first = 'KV is not available on this broker.'
  else if (!kv.namespaces || !kv.keys) first = 'Nothing is kept in KV yet.'
  else {
    first = `${fmt(kv.keys)} ${plural(kv.keys, 'key')} in ${fmt(kv.namespaces)} ${plural(kv.namespaces, 'namespace')}` +
      (kv.bytes != null ? `, ${bytes(kv.bytes)} in all.` : '.')
  }
  const lockOff = Boolean(locks?.unavailable)
  const timerOff = Boolean(timers?.unavailable)
  if (lockOff && timerOff) return first
  let l = null
  if (locks && !lockOff) {
    const n = locks.locks || 0
    const m = locks.semaphores || 0
    if (!n && !m) l = 'no locks are held'
    else {
      const parts = []
      if (n) parts.push(`${fmt(n)} ${plural(n, 'lock')}`)
      if (m) parts.push(`${fmt(m)} ${plural(m, 'semaphore')}`)
      l = `${parts.join(' and ')} ${n + m === 1 ? 'is' : 'are'} held`
    }
  }
  let t = null
  if (timers && !timers.reading && !timerOff) {
    if (!timers.count) t = 'no timers are waiting'
    else {
      const one = timers.count === 1 && !timers.more
      t = `${timers.more ? 'over ' : ''}${fmt(timers.count)} ${plural(timers.count, 'timer')} ${one ? 'waits' : 'wait'} in ${fmt(timers.queues)} ${plural(timers.queues, 'queue')}`
    }
  }
  const queues = timers?.total ? ` of ${fmt(timers.total)} ${plural(timers.total, 'queue')}` : ''
  if (lockOff) return `${first} ${t ? `${cap1(t)}.` : `Reading the timers${queues}…`}`
  if (timerOff) return `${first} ${l ? `${cap1(l)}.` : 'Reading the locks…'}`
  let second
  if (l && t) second = `${cap1(l)}, and ${t}.`
  else if (l) second = `${cap1(l)}. Reading the timers${queues}…`
  else if (t) second = `Reading the locks. ${cap1(t)}.`
  else second = `Reading the locks and the timers${queues}…`
  return `${first} ${second}`
}

// ---- locks ----------------------------------------------------------------

const byteOrder = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const finiteMin = (xs) => { const f = xs.filter(Number.isFinite); return f.length ? Math.min(...f) : null }

/**
 * Permits (useLocks.permitsOf) → one row per lock name, by name: a lock has one
 * holder, a semaphore one per permit held. Rows that are not permits (`foreign`)
 * are counted apart: they are in the namespace and are not locks.
 */
export function lockRows(permits) {
  const by = new Map()
  let other = 0
  for (const p of Array.isArray(permits) ? permits : []) {
    if (!p || p.foreign) { other += 1; continue }
    let g = by.get(p.name)
    if (!g) { g = { name: p.name, permits: [] }; by.set(p.name, g) }
    g.permits.push(p)
  }
  const rows = [...by.values()].map((g) => {
    const holders = g.permits
      .map((p) => ({
        key: p.key,
        owner: p.owner,
        slot: p.slot,
        token: p.token,
        sinceMs: msOf(p.since),
        renewedMs: msOf(p.renewedAt),
        endsMs: msOf(p.expiresAt),
        lifetime: (() => {
          const a = msOf(p.renewedAt); const b = msOf(p.expiresAt)
          return a != null && b != null && b > a ? Math.round((b - a) / 1000) : null
        })(),
      }))
      .sort((a, b) => (a.slot ?? 0) - (b.slot ?? 0))
    const shared = g.permits.some((p) => p.shared)
    return {
      name: g.name,
      kind: shared ? 'semaphore' : 'lock',
      holders,
      sinceMs: finiteMin(holders.map((h) => h.sinceMs)),
      endsMs: finiteMin(holders.map((h) => h.endsMs)),
    }
  }).sort((a, b) => byteOrder(a.name, b.name))
  return { rows, other }
}

/** The summary's lock part: `{locks, semaphores}`. */
export function lockTotals(rows) {
  const list = Array.isArray(rows) ? rows : []
  return {
    locks: list.filter((r) => r.kind === 'lock').length,
    semaphores: list.filter((r) => r.kind === 'semaphore').length,
  }
}

/** Lease left, never below zero: `21 s`, or `ending` once it is past. */
export function leaseLeft(endsMs, now = Date.now()) {
  if (endsMs == null) return '–'
  const d = endsMs - now
  return d < 1000 ? 'ending' : span(d / 1000)
}

// ---- timers ---------------------------------------------------------------

/**
 * The sweep's answers → one list, the next to fire first.
 * `results` is `[{queue, rows, truncated, error}]` (one per queue read).
 */
export function timerRows(results) {
  const out = []
  const failed = []
  let more = false
  const queues = new Set()
  for (const r of Array.isArray(results) ? results : []) {
    if (!r) continue
    if (r.error) { failed.push({ queue: r.queue, error: r.error }); continue }
    if (r.truncated) more = true
    for (const t of Array.isArray(r.rows) ? r.rows : []) {
      queues.add(r.queue)
      out.push({
        id: `${r.queue}\u0000${t.timerKey}`,
        queue: r.queue,
        key: t.timerKey,
        partition: t.partition || '',
        atMs: msOf(t.deliverAt),
        attempts: Number.isFinite(t.attempts) ? t.attempts : 0,
        claimed: t.claimed === true,
        lastError: t.lastError || '',
        encrypted: t.encrypted === true,
        raw: t,
      })
    }
  }
  out.sort((a, b) => (a.atMs ?? Infinity) - (b.atMs ?? Infinity) || byteOrder(a.queue, b.queue) || byteOrder(a.key, b.key))
  const perQueue = new Map()
  for (const t of out) perQueue.set(t.queue, (perQueue.get(t.queue) || 0) + 1)
  return { rows: out, count: out.length, queues: queues.size, more, failed, perQueue }
}

// ---- the writes -----------------------------------------------------------

/** `{ok, value, compact, error}`; the error in the parser's words, without its prefix. */
export function parseJson(text) {
  const t = String(text ?? '').trim()
  if (!t) return { ok: false, value: undefined, compact: '', error: 'it is empty' }
  try {
    const value = JSON.parse(t)
    return { ok: true, value, compact: JSON.stringify(value), error: '' }
  } catch (e) {
    return { ok: false, value: undefined, compact: '', error: String(e?.message || 'it does not parse').replace(/^JSON\.parse: /, '') }
  }
}

const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)

/**
 * What an edit changes, as rows `{k, from, to, field}`: field by field when
 * both values are JSON objects (`plan: "pro" → "business"`), else the whole
 * value clipped. A field that appears or goes is `–` on the other side.
 */
export function valueChanges(oldV, newV, max = 6) {
  if (isObj(oldV) && isObj(newV)) {
    const keys = [...new Set([...Object.keys(oldV), ...Object.keys(newV)])].sort()
    const rows = []
    for (const k of keys) {
      const a = k in oldV ? JSON.stringify(oldV[k]) : null
      const b = k in newV ? JSON.stringify(newV[k]) : null
      if (a !== b) rows.push({ k, from: a == null ? '–' : clip(a, 40), to: b == null ? '–' : clip(b, 40), field: true })
    }
    if (rows.length > max) return [...rows.slice(0, max), { k: `and ${fmt(rows.length - max)} more fields`, from: '', to: '', field: false, note: true }]
    return rows
  }
  return [{ k: 'Value', from: clip(JSON.stringify(oldV), 44), to: clip(JSON.stringify(newV), 44), field: false }]
}

/** The lifetime a key was last written with, in seconds, or null (no expiry). */
export function lifetimeOf(row) {
  const a = msOf(row?.updatedAt)
  const b = msOf(row?.expiresAt)
  if (b == null) return null
  if (a == null || b <= a) return null
  return Math.max(1, Math.round((b - a) / 1000))
}

export const kvPath = (ns, key) => `/api/v1/kv/${enc(ns)}/${enc(key)}`

/** A write's body as the dialog prints it: the value clipped, the rest exact. */
export function bodyText(body, valueCompact) {
  if (!body) return ''
  const rest = Object.entries(body).filter(([k]) => k !== 'value').map(([k, v]) => `${JSON.stringify(k)}:${JSON.stringify(v)}`)
  const head = 'value' in body ? [`"value":${clip(valueCompact ?? JSON.stringify(body.value), 140)}`] : []
  return `{${[...head, ...rest].join(',')}}`
}

/**
 * Edit one key. `asText` edits a text value as plain text (sent as a JSON
 * string). The key keeps the lifetime it was written with, counted from this
 * write; `ifUnchanged` sends the version it was read at as `expect`.
 */
export function editPlan({ ns, row, text, asText = false, ifUnchanged = true, cap = VALUE_CAP } = {}) {
  const key = row?.key || ''
  const parsed = asText ? { ok: true, value: String(text ?? ''), compact: JSON.stringify(String(text ?? '')), error: '' } : parseJson(text)
  const oldCompact = JSON.stringify(row?.value)
  const oldSize = valueBytes(row)
  const life = lifetimeOf(row)
  const expiry = life ? { ttlSeconds: life } : { forever: true }
  const call = (body, compact) => `PUT ${kvPath(ns, key)} ${body ? bodyText(body, compact) : '…'}`
  if (!parsed.ok) {
    return { ok: false, same: false, error: `Not valid JSON yet: ${parsed.error}.`, call: call(null), changes: [], effect: '', body: null }
  }
  const size = utf8(parsed.compact)
  const same = parsed.compact === oldCompact
  const tooBig = size > cap
  const body = { value: parsed.value, ...expiry }
  if (ifUnchanged && Number.isFinite(row?.version)) body.expect = row.version
  const changes = same ? [] : valueChanges(row?.value, parsed.value)
  if (!same && size !== oldSize) changes.push({ k: 'Size', from: bytes(oldSize), to: bytes(size), field: false, size: true })
  const effect = [
    `${key} gets the new value; steps read it from their next read.`,
    life ? `It keeps its ${span(life)} lifetime, counted from this write.` : 'It never expires, as now.',
    ifUnchanged ? 'If anything wrote it after you opened it, nothing is written.' : 'Anything written to it since you opened it is replaced.',
  ].join(' ')
  return {
    ok: !same && !tooBig,
    same,
    error: tooBig ? `It is ${bytes(size)}: over ${bytes(cap)}, the broker's default limit for one value.` : '',
    call: call(body, parsed.compact),
    changes,
    effect,
    body,
  }
}

export const EXPIRIES = [
  { value: 'never', label: 'Never', seconds: 0 },
  { value: '1h', label: '1 h', seconds: 3600 },
  { value: '1d', label: '1 day', seconds: 86_400 },
  { value: '7d', label: '7 days', seconds: 604_800 },
]

/** Put a new key: `absent` sends `expect: 0`, so an existing key is left alone. */
export function putPlan({ ns = '', key = '', text = '', expiry = 'never', absent = true, cap = VALUE_CAP } = {}) {
  const n = String(ns).trim()
  const k = String(key)
  const parsed = parseJson(text)
  const exp = EXPIRIES.find((e) => e.value === expiry) || EXPIRIES[0]
  // What is wrong with what was typed; a field still empty is not a problem
  // yet, and the effect line asks for it.
  const problems = []
  if (n && !NAMESPACE_RE.test(n)) problems.push('A namespace is lower case letters, digits, dots, dashes and underscores, up to 64, and starts with a letter or a digit.')
  if (k.includes('\u0000')) problems.push('A key cannot hold a NUL character.')
  else if (utf8(k) > KEY_MAX_BYTES) problems.push(`The key is ${fmt(utf8(k))} bytes; the broker takes ${KEY_MAX_BYTES} at most.`)
  if (!parsed.ok && String(text ?? '').trim()) problems.push(`Not valid JSON yet: ${parsed.error}.`)
  const size = parsed.ok ? utf8(parsed.compact) : 0
  if (parsed.ok && size > cap) problems.push(`The value is ${bytes(size)}: over ${bytes(cap)}, the broker's default limit for one value.`)
  const body = parsed.ok ? { value: parsed.value, ...(exp.seconds ? { ttlSeconds: exp.seconds } : { forever: true }) } : null
  if (body && absent) body.expect = 0
  const ready = Boolean(n && k && parsed.ok && !problems.length)
  const where = n && k ? kvPath(n, k) : `/api/v1/kv/${n ? enc(n) : '…'}/${k ? enc(k) : '…'}`
  let effect = !n || !k ? 'Name the namespace and the key, and give it a value.' : 'Give it a value.'
  if (n && k && parsed.ok) {
    effect = (absent ? `It adds ${k} to ${n}` : `It writes ${k} in ${n}`) +
      (exp.seconds ? `, and the broker removes it ${exp.label} after the write.` : '; it stays until someone deletes it.') +
      (absent ? ' If the key exists, nothing is written.' : ' An existing value is replaced.')
  }
  return { ok: ready, problems, size, body, call: `PUT ${where} ${body ? bodyText(body, parsed.compact) : '…'}`, effect }
}

/**
 * Delete one key, only at the version it was read at. An expired row is
 * deleted plainly: a delete with `expect` answers `absent` on it and leaves
 * the row for the sweeper.
 */
export function deletePlan({ ns, row } = {}) {
  const key = row?.key || ''
  const body = row?.expired === true || !Number.isFinite(row?.version) ? null : { expect: row.version }
  return { body, call: `DELETE ${kvPath(ns, key)}${body ? ` ${JSON.stringify(body)}` : ''}` }
}

/**
 * The broker's answer to a put or a delete, as a toast: `{text, sub, tone}`.
 * A lost precondition is a 200 with `applied: false` and a reason, never an
 * error: the words say what is stored now.
 */
export function writeAnswer(op, data, { key = '', ns = '', status = 200 } = {}) {
  const d = data && typeof data === 'object' ? data : {}
  const said = `The broker answered ${status}`
  if (d.applied === true) {
    if (op === 'delete') return { text: `Deleted ${key}`, sub: `${said}: it is gone from ${ns}.`, tone: '' }
    return { text: `Saved ${key}`, sub: `${said}: ${ns}, version ${fmt(d.version)}.`, tone: '' }
  }
  if (d.applied === false) {
    const v = Number.isFinite(d.version) && d.version > 0 ? `, at version ${fmt(d.version)}` : ''
    if (d.reason === 'exists') return { text: `Not written: ${key} exists`, sub: `${said}${v}; nothing changed. Untick "Only if absent" to replace it.`, tone: '' }
    if (d.reason === 'version') return { text: `Not ${op === 'delete' ? 'deleted' : 'written'}: ${key} changed since you opened it`, sub: `${said}${v}; nothing changed. Open it again to see what it holds now.`, tone: '' }
    if (d.reason === 'absent') return { text: `Not ${op === 'delete' ? 'deleted' : 'written'}: ${key} is not there`, sub: `${said}: someone deleted it, or it expired.`, tone: '' }
    return { text: `Not ${op === 'delete' ? 'deleted' : 'written'}`, sub: `${said} with "${d.reason || 'no reason'}".`, tone: 'red' }
  }
  return { text: 'Sent, with no verdict', sub: `${said}, with nothing this page can read as applied or not. Look at the key again.`, tone: 'red' }
}
