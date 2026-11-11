// Scoped API keys on Settings › Access: what a key's grants say, the form that
// edits them, and the clients it refuses. Plain functions over the payloads
// the page reads — proxy/src/console.rs list_keys ({ scoped, grants, limits,
// token_ttl_max_s }) and list_blocks ({ key, sub, reason, at }) — so the
// words in the table and the body the dialogs send come from one place.
// Pure: test/scoped-keys.test.js.
//
// A grant names ONE queue, KV namespace or timers queue (a pattern), maybe
// its partition, group or key, and what may be done there. A pattern is a
// name, `prefix*`, `*`, or holds `{sub}` once: the entity a client token is
// for. The proxy checks every rule again (crates/queen-protocol/src/scope.rs,
// Template::parse); these mirror them so the form says what is wrong before
// anything is sent.

const list = value => (Array.isArray(value) ? value : [])

export const SUB = '{sub}'

export const GRANT_KINDS = Object.freeze([
  {
    kind: 'queue', name: 'Queue', field: 'queue', fields: ['partition', 'group'],
    ops: [{ op: 'produce', can: 'push' }, { op: 'consume', can: 'pop and acknowledge' }],
  },
  {
    kind: 'kv', name: 'KV', field: 'namespace', fields: ['key'],
    ops: [{ op: 'read', can: 'read' }, { op: 'write', can: 'write and delete' }],
  },
  {
    kind: 'timers', name: 'Timers', field: 'timers queue', fields: ['partition', 'key'],
    ops: [{ op: 'read', can: 'list and peek' }, { op: 'write', can: 'schedule and cancel' }],
  },
])

const kindOf = kind => GRANT_KINDS.find(k => k.kind === kind)

/** The surface a grant names: 'queue', 'kv', 'timers', or '' for none or several. */
export function grantKind(grant) {
  const named = GRANT_KINDS.filter(k => typeof grant?.[k.kind] === 'string')
  return named.length === 1 ? named[0].kind : ''
}

/** One grant in one line: "queue telemetry · partition {sub} · produce". */
export function grantText(grant) {
  const kind = grantKind(grant)
  if (!kind) return 'a grant this page cannot read'
  const parts = [`${kind} ${grant[kind]}`]
  for (const f of ['partition', 'group', 'key']) if (grant[f]) parts.push(`${f} ${grant[f]}`)
  const ops = list(grant.ops)
  parts.push(ops.length ? ops.join(', ') : 'nothing')
  return parts.join(' · ')
}

/** A key's grants for a table cell: the first, and how many more. */
export function grantsSummary(grants) {
  const all = list(grants)
  if (!all.length) return 'nothing'
  return all.length === 1 ? grantText(all[0]) : `${grantText(all[0])} · +${all.length - 1} more`
}

/** Does any grant name `{sub}`, so each client reaches only its own names? */
export const namesSub = grants => list(grants).some(g =>
  ['queue', 'kv', 'timers', 'partition', 'group', 'key'].some(f => String(g?.[f] ?? '').includes(SUB)))

const SUB_CHAR = /^[A-Za-z0-9._@+-]$/
const bytes = s => new TextEncoder().encode(s).length

/** Why a pattern is refused, or '' when the proxy takes it. */
export function patternProblem(field, raw) {
  const s = String(raw ?? '')
  if (!s) return `The ${field} is empty: write a name, a prefix ending in *, or *.`
  if (bytes(s) > 256) return `The ${field} is longer than 256 bytes.`
  if (s === '*') return ''
  const prefix = s.endsWith('*')
  const body = prefix ? s.slice(0, -1) : s
  if (body.includes('*')) return `The ${field} ${s}: a * may only end it.`
  const at = body.indexOf(SUB)
  if (at < 0) return /[{}]/.test(body) ? `The ${field} ${s}: { and } appear only as {sub}.` : ''
  if (/[{}]/.test(body.slice(0, at)) || /[{}]/.test(body.slice(at + SUB.length))) {
    return `The ${field} ${s}: {sub} may appear once, and { and } nowhere else.`
  }
  const next = body.slice(at + SUB.length, at + SUB.length + 1)
  if (prefix && (!next || SUB_CHAR.test(next))) {
    return `The ${field} ${s}: put a separator such as / or : between {sub} and *, as in {sub}/*, or one client reaches another's names.`
  }
  return ''
}

/** Why a grant is refused, or ''. */
export function grantProblem(grant) {
  const kind = grantKind(grant)
  if (!kind) return 'Pick a queue, a KV namespace or a timers queue.'
  const def = kindOf(kind)
  const named = patternProblem(def.field, grant[kind])
  if (named) return named
  for (const f of ['partition', 'group', 'key']) {
    if (grant[f] == null) continue
    if (!def.fields.includes(f)) return `A ${kind} grant has no ${f}.`
    const p = patternProblem(f, grant[f])
    if (p) return p
  }
  const ops = list(grant.ops)
  if (!ops.length) return `Pick what it may do: ${def.ops.map(o => o.can).join(', or ')}.`
  const bad = ops.find(o => !def.ops.some(d => d.op === o))
  return bad ? `A ${kind} grant takes ${def.ops.map(o => o.op).join(' and ')}, not ${bad}.` : ''
}

// ---------------------------------------------------------------------------
// The form: one row per grant, every field a string, empty = not named.
// ---------------------------------------------------------------------------

export const blankRow = (kind = 'queue') => ({
  kind, name: '', partition: '', group: '', key: '', ops: [kindOf(kind)?.ops[0].op || 'produce'],
})

/** A grant as the form edits it. */
export function rowOf(grant) {
  const kind = grantKind(grant) || 'queue'
  return {
    kind,
    name: String(grant?.[kind] ?? ''),
    partition: String(grant?.partition ?? ''),
    group: String(grant?.group ?? ''),
    key: String(grant?.key ?? ''),
    ops: [...list(grant?.ops)],
  }
}

/** A row as the proxy takes it: only the fields its kind has, trimmed, empty ones left out. */
export function grantOf(row) {
  const def = kindOf(row?.kind) || GRANT_KINDS[0]
  const grant = { [def.kind]: String(row?.name ?? '').trim() }
  for (const f of def.fields) {
    const v = String(row?.[f] ?? '').trim()
    if (v) grant[f] = v
  }
  grant.ops = def.ops.map(o => o.op).filter(op => list(row?.ops).includes(op))
  return grant
}

/** A row switched to another kind: the name stays, the ops become the new kind's first. */
export function switchKind(row, kind) {
  const def = kindOf(kind)
  if (!def) return row
  const ops = list(row?.ops).filter(op => def.ops.some(o => o.op === op))
  return { ...row, kind, ops: ops.length ? ops : [def.ops[0].op] }
}

// ---------------------------------------------------------------------------
// Limits (requests per second, per node) and the longest token a key mints.
// ---------------------------------------------------------------------------

export const RPS_CEILING = 1_000_000

/** Why a typed limit is refused, or ''. Empty is no limit. */
export function rpsProblem(raw) {
  const s = String(raw ?? '').trim()
  if (!s) return ''
  const n = Number(s)
  return Number.isInteger(n) && n >= 1 && n <= RPS_CEILING ? '' : `A limit is a whole number of requests per second, 1 to ${RPS_CEILING.toLocaleString('en-US')}, or empty for none.`
}

/** The limits to send: the typed rates over what the key had, so a burst set
 * through the API stays; a cleared rate takes its burst with it. */
export function limitsOf(before, keyRps, subRps) {
  const out = { ...(before && typeof before === 'object' ? before : {}) }
  for (const [rps, burst, raw] of [['key_rps', 'key_burst', keyRps], ['sub_rps', 'sub_burst', subRps]]) {
    const s = String(raw ?? '').trim()
    if (s) out[rps] = Number(s)
    else { delete out[rps]; delete out[burst] }
  }
  return out
}

export const DEFAULT_TOKEN_TTL_MAX_S = 86_400

export const TOKEN_CEILINGS = Object.freeze([
  { s: 900, name: '15 minutes' },
  { s: 3600, name: '1 hour' },
  { s: 86_400, name: '1 day' },
  { s: 7 * 86_400, name: '7 days' },
  { s: 30 * 86_400, name: '30 days' },
])

/** "1 day", "10 minutes", "90 seconds": how long a ceiling is. */
export function lengthOf(seconds) {
  const s = Number(seconds)
  if (!Number.isFinite(s) || s <= 0) return ''
  const named = TOKEN_CEILINGS.find(c => c.s === s)
  if (named) return named.name
  const unit = (n, one) => `${n} ${one}${n === 1 ? '' : 's'}`
  if (s % 86_400 === 0) return unit(s / 86_400, 'day')
  if (s % 3600 === 0) return unit(s / 3600, 'hour')
  if (s % 60 === 0) return unit(s / 60, 'minute')
  return unit(s, 'second')
}

/** The choices for a key's ceiling: the usual ones, plus the key's own if it is another. */
export function ceilingChoices(current) {
  const s = Number(current)
  if (!Number.isFinite(s) || s <= 0 || TOKEN_CEILINGS.some(c => c.s === s)) return TOKEN_CEILINGS
  return [...TOKEN_CEILINGS, { s, name: lengthOf(s) }].sort((a, b) => a.s - b.s)
}

export const ROTATE_GRACES = Object.freeze([
  { s: 0, name: 'Now' },
  { s: 3600, name: 'In 1 hour' },
  { s: 86_400, name: 'In 1 day' },
  { s: 7 * 86_400, name: 'In 7 days' },
])

// ---------------------------------------------------------------------------
// Blocked clients.
// ---------------------------------------------------------------------------

/** Is this an entity name a token can carry (and a block can name)? */
export const validSub = raw => {
  const s = String(raw ?? '')
  return s.length > 0 && bytes(s) <= 128 && [...s].every(c => SUB_CHAR.test(c))
}

export const BLOCK_REASON_MAX = 200

/** The block list with each key's name, newest first. A block of a key no
 * longer listed keeps its id as its name. */
export function blockRows(blocks, keys) {
  const names = new Map(list(keys).map(k => [k?.id, k?.name]))
  return list(blocks)
    .map(b => ({ ...b, keyName: names.get(b?.key) || String(b?.key ?? '') }))
    .sort((a, b) => String(b.at).localeCompare(String(a.at)) || String(a.sub).localeCompare(String(b.sub)))
}
