// The `where` language in the console (PLAN_ROUTING.md §9): the string form
// read the way the broker reads it, so the New queue form can say what is
// wrong with a where, and try it on a source's last messages, before the
// broker sees either. Fields `queue`, `partition`, `transactionId`, `data`,
// `data.a.b` and `headers.<name>`; ops `=` `!=` `<` `<=` `>` `>=` `in`
// `exists` `prefix` `!prefix`; conditions joined by `and` only.
//
// A mirror of server/src/rsm/route/cond.rs (lex, parse_text, check, holds,
// to_text) with the route's rules (no `offset` or `position`). It refuses
// only what the broker refuses. The two limits, 16 conditions and 64 values in
// one `in`, are defaults an operator can raise (QUEEN_WHERE_MAX_CONDITIONS,
// QUEEN_WHERE_MAX_IN), so they are left to the broker.
//
// Pure: no Vue, no '@/' imports (test/routing.test.js).

export const MAX_FIELD_DEPTH = 16
const HEADER_KEY = /^[a-z0-9._-]{1,64}$/
const JSON_NUMBER = /^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$/
const OPS = {
  '=': '=', '==': '=', '!=': '!=', '<>': '!=', '<': '<', '<=': '<=', '>': '>', '>=': '>=',
  in: 'in', exists: 'exists', prefix: 'prefix', '!prefix': '!prefix',
}
const COMPARE = new Set(['<', '<=', '>', '>='])
const q = (s) => JSON.stringify(String(s))

/** An op as the broker spells it ('==' is '=', '<>' is '!='); throws on an unknown one. */
export function parseOp(raw) {
  const op = OPS[String(raw ?? '').trim().toLowerCase()]
  if (!op) throw new Error(`where: unknown op ${q(raw)} (=, !=, <, <=, >, >=, in, exists, prefix, !prefix)`)
  return op
}

/** A field by name: `{ kind: 'queue'|'partition'|'transactionId'|'data'|'header', path?, key? }`; throws on a bad one. */
export function parseField(raw) {
  const s = String(raw ?? '')
  if (s === 'queue' || s === 'partition' || s === 'transactionId') return { kind: s }
  if (s === 'offset' || s === 'position') throw new Error(`where: ${q(s)} is known only once a message is stored; use it in the feed`)
  if (s === 'data') return { kind: 'data', path: [] }
  if (s.startsWith('headers.')) {
    const key = s.slice('headers.'.length)
    if (!HEADER_KEY.test(key)) throw new Error(`where: bad header name ${q(key)} (1 to 64 bytes of a-z, 0-9, '.', '_' or '-')`)
    return { kind: 'header', key }
  }
  if (!s.startsWith('data.')) {
    throw new Error(`where: unknown field ${q(s)} (queue, partition, transactionId, data, data.<path> or headers.<name>)`)
  }
  const path = s.slice('data.'.length).split('.')
  if (path.some((p) => p === '') || path.length > MAX_FIELD_DEPTH) throw new Error(`where: bad field path ${q(s)}`)
  return { kind: 'data', path }
}

/** One condition, its value checked against its op (cond.rs `check`). */
function check(fieldRaw, opRaw, value) {
  const field = parseField(fieldRaw)
  const op = parseOp(opRaw)
  if (op === 'in' && !Array.isArray(value)) throw new Error(`where: in on ${q(fieldRaw)} needs an array`)
  if ((op === 'prefix' || op === '!prefix') && typeof value !== 'string') throw new Error(`where: ${opRaw} on ${q(fieldRaw)} needs a string`)
  if (COMPARE.has(op) && typeof value !== 'number' && typeof value !== 'string') {
    throw new Error(`where: ${opRaw} on ${q(fieldRaw)} needs a number or a string`)
  }
  return { field: String(fieldRaw), f: field, op, value }
}

// ---- the string form ----------------------------------------------------------

const SPACE = /\s/u
const WORD_START = /[\p{Alphabetic}\p{N}_]/u
const WORD_REST = /[\p{Alphabetic}\p{N}_.-]/u

function lex(s) {
  const b = Array.from(s)
  const out = []
  let i = 0
  while (i < b.length) {
    const c = b[i]
    if (SPACE.test(c)) { i++; continue }
    if ('()[],'.includes(c)) { out.push({ k: c }); i++; continue }
    if (c === "'" || c === '"') {
      i++
      let v = ''
      for (;;) {
        if (i >= b.length) throw new Error('where: a string is not closed')
        if (b[i] === c) {
          // A doubled quote is the quote itself ('it''s').
          if (b[i + 1] === c) { v += c; i += 2; continue }
          i++
          break
        }
        if (b[i] === '\\' && i + 1 < b.length) { v += b[i + 1]; i += 2; continue }
        v += b[i]
        i++
      }
      out.push({ k: 'str', v })
      continue
    }
    if ('=<>!'.includes(c)) {
      let op = c
      const n = b[i + 1]
      if ((n === '=' || n === '>') && !(c === '>' && n === '>')) { op += n; i += 2 } else i++
      if (op === '!') {
        const start = i
        while (i < b.length && /[A-Za-z]/.test(b[i])) i++
        const w = b.slice(start, i).join('')
        if (w.toLowerCase() === 'prefix') { out.push({ k: 'op', v: '!prefix' }); continue }
        throw new Error(`where: unexpected '!' before ${q(w)}`)
      }
      out.push({ k: 'op', v: op })
      continue
    }
    if (c === '-' || (c >= '0' && c <= '9')) {
      const start = i
      i++
      while (i < b.length && /[0-9.eE+-]/.test(b[i])) i++
      const t = b.slice(start, i).join('')
      const v = Number(t)
      if (!JSON_NUMBER.test(t) || !Number.isFinite(v)) throw new Error(`where: ${q(t)} is not a number`)
      out.push({ k: 'num', v })
      continue
    }
    if (WORD_START.test(c)) {
      const start = i
      while (i < b.length && WORD_REST.test(b[i])) i++
      out.push({ k: 'word', v: b.slice(start, i).join('') })
      continue
    }
    throw new Error(`where: unexpected ${q(c)}`)
  }
  return out
}

/** A value token: a string, a number, true, false or null; undefined for anything else. */
function scalar(t) {
  if (!t) return undefined
  if (t.k === 'str' || t.k === 'num') return t.v
  if (t.k === 'word') {
    const w = t.v.toLowerCase()
    if (w === 'true') return true
    if (w === 'false') return false
    if (w === 'null') return null
  }
  return undefined
}

const said = (t) => (t.k === 'str' || t.k === 'word' || t.k === 'op' ? q(t.v) : t.k === 'num' ? String(t.v) : q(t.k))

/**
 * The string form, `cond (and cond)*`, where a cond is `field op value` or
 * `field exists`, a value is a quoted string, a number, true, false or null,
 * and `in` takes `(v, v, ...)`. → `{ conds, error }`: the conditions (none for
 * an empty string: every message), or null and the broker's sentence.
 */
export function parseWhere(text) {
  const s = typeof text === 'string' ? text : ''
  try {
    const toks = lex(s)
    const out = []
    let i = 0
    if (!toks.length) return { conds: out, error: null }
    for (;;) {
      if (toks[i]?.k !== 'word') throw new Error(`where: a condition starts with a field, at token ${i + 1}`)
      const field = toks[i].v
      i++
      const o = toks[i]
      if (o?.k !== 'op' && o?.k !== 'word') throw new Error(`where: ${q(field)} needs an operator`)
      const op = o.v
      i++
      const kind = parseOp(op)
      let value
      if (kind === 'exists') value = null
      else if (kind === 'in') {
        const close = toks[i]?.k === '(' ? ')' : toks[i]?.k === '[' ? ']' : null
        if (!close) throw new Error(`where: in on ${q(field)} needs (value, value, ...)`)
        i++
        const vals = []
        for (;;) {
          if (toks[i]?.k === close && !vals.length) { i++; break }
          const v = scalar(toks[i])
          if (v === undefined) throw new Error(`where: in on ${q(field)}: a value, at token ${i + 1}`)
          vals.push(v)
          i++
          if (toks[i]?.k === ',') i++
          else if (toks[i]?.k === close) { i++; break }
          else throw new Error(`where: in on ${q(field)}: ',' or the closing bracket`)
        }
        value = vals
      } else {
        value = scalar(toks[i])
        if (value === undefined) throw new Error(`where: ${q(field)} ${op} needs a value`)
        i++
      }
      out.push(check(field, op, value))
      const t = toks[i]
      if (!t) break
      if (t.k === 'word' && t.v.toLowerCase() === 'and') { i++; continue }
      if (t.k === 'word' && t.v.toLowerCase() === 'or') throw new Error('where: only AND joins conditions; use in (...) for alternatives')
      throw new Error(`where: expected 'and', found ${said(t)}`)
    }
    return { conds: out, error: null }
  } catch (e) {
    return { conds: null, error: e.message }
  }
}

// ---- the list form, as the string the console shows ----------------------------

function valueText(v) {
  if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`
  if (Array.isArray(v)) return `(${v.map(valueText).join(', ')})`
  return JSON.stringify(v === undefined ? null : v)
}

/**
 * A where as one string, from either form: a string is itself (trimmed), a
 * list of `{field, op, value}` or `[field, op, value]` is written as the
 * broker writes it back (cond.rs to_text). Anything else is ''.
 */
export function whereOf(value) {
  if (typeof value === 'string') return value.trim()
  if (!Array.isArray(value)) return ''
  return value.map((c) => {
    const [field, op, v] = Array.isArray(c) ? c : [c?.field, c?.op, c?.value]
    const o = OPS[String(op ?? '').trim().toLowerCase()] || String(op ?? '')
    return o === 'exists' ? `${field} exists` : `${field} ${o} ${valueText(v)}`
  }).join(' and ')
}

// ---- evaluation ---------------------------------------------------------------

const own = (o, k) => Object.prototype.hasOwnProperty.call(o, k)

/** What a field reads on a message as GET /api/v1/messages answers it; undefined when it lacks it. */
function fieldValue(f, m) {
  switch (f.kind) {
    case 'queue': return typeof m.queue === 'string' ? m.queue : undefined
    case 'partition': return typeof m.partition === 'string' ? m.partition : undefined
    case 'transactionId': return typeof m.transactionId === 'string' ? m.transactionId : undefined
    case 'header': {
      const h = m.headers
      return h && typeof h === 'object' && own(h, f.key) && typeof h[f.key] === 'string' ? h[f.key] : undefined
    }
    case 'data': {
      let cur = own(m, 'data') ? m.data : m.payload
      for (const key of f.path) {
        if (Array.isArray(cur)) cur = /^\d+$/.test(key) ? cur[Number(key)] : undefined
        else if (cur && typeof cur === 'object') cur = own(cur, key) ? cur[key] : undefined
        else return undefined
        if (cur === undefined) return undefined
      }
      return cur
    }
    default: return undefined
  }
}

function deepEqual(a, b) {
  if (a === b) return true
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a)) return a.length === b.length && a.every((x, i) => deepEqual(x, b[i]))
  const ka = Object.keys(a)
  return ka.length === Object.keys(b).length && ka.every((k) => own(b, k) && deepEqual(a[k], b[k]))
}

/** Two numbers compare as numbers; strings and the rest as JSON values. */
const same = (a, b) => (typeof a === 'number' && typeof b === 'number' ? a === b : deepEqual(a, b))

function order(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  return null
}

/**
 * Whether one condition holds for message `m`. A field the message lacks
 * holds for no op, `!=` included, as SQL's comparisons with NULL.
 */
export function holds(c, m) {
  if (!c || !m || typeof m !== 'object') return false
  const v = fieldValue(c.f, m)
  if (v === undefined) return false
  switch (c.op) {
    case 'exists': return true
    case '=': return same(v, c.value)
    case '!=': return !same(v, c.value)
    case 'in': return Array.isArray(c.value) && c.value.some((x) => same(v, x))
    case 'prefix': return typeof v === 'string' && typeof c.value === 'string' && v.startsWith(c.value)
    case '!prefix': return typeof v === 'string' && typeof c.value === 'string' && !v.startsWith(c.value)
    default: {
      const o = order(v, c.value)
      if (o === null) return false
      return c.op === '<' ? o < 0 : c.op === '<=' ? o <= 0 : c.op === '>' ? o > 0 : o >= 0
    }
  }
}

/** Whether every condition holds (none: every message). */
export const whereHolds = (conds, m) => Array.isArray(conds) && conds.every((c) => holds(c, m))

/**
 * A field's value on a message, read as a condition reads it (a reading
 * queue's `partitionBy`, say): undefined when the message lacks it or the
 * field is not one.
 */
export function readField(raw, m) {
  if (!m || typeof m !== 'object') return undefined
  try { return fieldValue(parseField(raw), m) } catch { return undefined }
}
