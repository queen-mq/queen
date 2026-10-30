// Traces' list: the tenant's trace names, from GET /api/v1/traces/names (the
// page's one read). The broker answers them in name order, and the list keeps
// that order: a list that reorders itself moves rows under the pointer. The
// search narrows it; Enter opens an exact name, listed or not.
// Pure: test/traces-side.test.js.

/** How many names the page asks for. The broker walks every name either way. */
export const TRACE_NAMES_LIMIT = 200

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const time = (v) => {
  if (typeof v !== 'string' || !v) return null
  const t = Date.parse(v)
  return Number.isFinite(t) ? t : null
}

/**
 * GET /api/v1/traces/names → `{ rows, total }`, or null when the answer holds
 * no list (that is not "no names"). A row: `{ name, traces, messages,
 * lastSeen }`, lastSeen in ms or null. `total` counts every name the tenant
 * has, never fewer than the rows.
 */
export function readTraceNames(payload) {
  const list = payload?.trace_names
  if (!Array.isArray(list)) return null
  const rows = list
    .filter((n) => n && typeof n.trace_name === 'string' && n.trace_name)
    .map((n) => ({
      name: n.trace_name,
      traces: num(n.trace_count),
      messages: num(n.message_count),
      lastSeen: time(n.last_seen),
    }))
  const total = num(payload.total)
  return { rows, total: total !== null && total >= rows.length ? total : rows.length }
}

const byName = (a, b) => a.name.localeCompare(b.name)

/**
 * The list as drawn: the names by name, narrowed by what is typed in the
 * search box.
 *
 *   rows  the names to list
 *   ask   the typed name when neither the list nor the page holds it
 *         exactly: a row that searches it (the box searches exact names, as
 *         the page's own box did)
 *   open  the name open on the right when the list does not hold it, so the
 *         column always says what the page shows
 */
export function traceNameList(rows, { term = '', current = '' } = {}) {
  const list = Array.isArray(rows) ? rows : []
  const typed = String(term || '').trim()
  const needle = typed.toLowerCase()
  const sorted = [...list].sort(byName)
  const has = (name) => list.some((r) => r.name === name)
  return {
    rows: needle ? sorted.filter((r) => r.name.toLowerCase().includes(needle)) : sorted,
    ask: typed && !has(typed) && typed !== current ? typed : null,
    open: current && !has(current) ? current : null,
  }
}
