// KV browser rules — PURE functions, no Vue, no DOM (the useTimers.js and
// useWorkload.js convention), so `npm test` can assert them without a browser.
//
// Five of them are the ones that can be wrong in a way nobody notices:
//
//  1. THE BODY IS THE QUERY. `POST /api/v1/resources/kv/list` takes its cursor
//     in the body because a cursor IS a key, and a key in a query string
//     (`?after=wh.deliver:promotion-publication:b15f…`) is the leak
//     PLAN_KV_TIMERS.md §5.5 forbids, through four components' access logs. So
//     the body is built here, once, rather than assembled at three call sites:
//     an `after: ""` that reached the wire would be read as "no cursor" by the
//     stored procedure — which is right — while an `after: null` in a query
//     string would have been the four literal characters `null`, which is not.
//
//  2. THE CONSOLE'S DEFAULTS ARE NOT THE API'S, and this file sends the
//     console's EXPLICITLY. `includeExpired` is FALSE on the wire when nobody
//     asks — the route matches its stored procedure and §5.7, so a caller that
//     omits it is not handed rows every other KV read treats as absent — and
//     showing them is a decision of THIS PAGE (PLAN_DASHBOARD_ACTIONS.md §2.5
//     D5), sent on every call. Why the page must show them: an expired row
//     is still an OCCUPIED one — it holds its space until the sweeper takes it
//     — and it is counted by the namespace figure in the selector
//     (kv_namespaces_v1 counts every row). It is NOT in the header's `≈ N live
//     keys`, which the sweeper computes over live rows only
//     (026_kv_sweeper.sql kv_usage_step_v1): the two numbers on screen count
//     different populations, which is exactly why each names its own.
//     `keysOnly` is never sent: the drawer's value comes from the list page
//     itself, and the stored procedure's 4 MiB page budget makes that affordable.
//
//  3. EXPIRY IS THE BROKER'S VERDICT, NEVER THIS BROWSER'S CLOCK. Every row
//     carries `expired`, computed in SQL against ONE `now()` per page — the
//     database's clock, the same one the sweeper reads (024_kv.sql
//     kv_live_v1). Nothing here re-decides it from `expiresAt`: a laptop three
//     minutes fast would otherwise grey a key that every reader still sees,
//     which is the one lie this page exists not to tell. The formatter below
//     renders how long is left, and only that.
//
//  4. "ago", NOT "overdue". useTimers.formatDeliverIn says "2m overdue"
//     because a timer that has passed its instant is LATE — it is still going
//     to be delivered. A KV key that has passed its expiry is not late: it is
//     already absent from every read, and what remains is a row the sweeper
//     has not collected yet. Same shape, opposite meaning, so the two
//     formatters stay apart and say so.
//
//  5. THE SIZE COLUMN IS MEASURED HERE, THE PAGE TOTAL IN POSTGRES. `bytes` on
//     the response is `octet_length(value::text)` summed over the page, and
//     jsonb's text form spaces its separators (`{"a": 1}`) where
//     JSON.stringify does not (`{"a":1}`). A per-row size measured in the
//     browser is therefore a byte or two under the server's count for the same
//     row, and the column that renders it says so in its title rather than
//     pretending the two numbers are the same measurement.
import { parseBrokerInstant } from './useTimers.js'

/** The stored procedure's per-page byte budget (024_kv.sql C_MAX_READ_BYTES). */
export const KV_PAGE_BUDGET_BYTES = 4 * 1024 * 1024

/**
 * The `POST /api/v1/resources/kv/list` body for one page.
 *
 * Optional fields are OMITTED rather than sent empty: `prefix: ""` and
 * `after: ""` both mean "not given" to the stored procedure, but a body that
 * carries them makes the request log read as though a filter were applied.
 * `limit` is passed straight through — the SP clamps it to 1..1000 and never
 * rejects it, so there is no second place for that number to live.
 */
export function kvListBody({ namespace, prefix = '', after = null, limit = null } = {}) {
  const body = { namespace: typeof namespace === 'string' ? namespace : '', includeExpired: true }
  if (typeof prefix === 'string' && prefix !== '') body.prefix = prefix
  if (typeof after === 'string' && after !== '') body.after = after
  if (Number.isFinite(limit)) body.limit = limit
  return body
}

const ENCODER = new TextEncoder()

/**
 * One row's value as bytes, or null when the row carries no value at all.
 *
 * `'value' in row` and not `row.value != null`: a stored JSON `null` is a
 * legitimate value four bytes long, and the ABSENCE of the field is what a
 * `keysOnly` page answers. Confusing the two would print "—" over a key that
 * really does hold `null`.
 */
export function valueBytes(row) {
  if (!row || typeof row !== 'object' || !('value' in row)) return null
  let text
  try {
    text = JSON.stringify(row.value)
  } catch {
    return null
  }
  if (typeof text !== 'string') return null
  return ENCODER.encode(text).length
}

/** Coarse span, seconds up to a minute and never more precision than the unit. */
const span = (ms) => {
  const sec = Math.floor(ms / 1000)
  if (sec < 60) return `${sec}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m`
  if (sec < 86_400) return `${Math.floor(sec / 3600)}h`
  return `${Math.floor(sec / 86_400)}d`
}

/**
 * The "Expires" column: `never` / `in 4h` / `12m ago`.
 *
 * `never` is the honest word for a NULL `expiresAt` — the key has no TTL and
 * nothing will remove it but a delete — and it is distinct from `—`, which is
 * what an unparsable stamp renders as. `now` is passed in rather than read off
 * the clock so a whole page is rendered against ONE instant, the load, which
 * is what the freshness stamp above the table claims.
 */
export function formatExpiry(expiresAt, now = Date.now()) {
  if (expiresAt === null || expiresAt === undefined || expiresAt === '') return 'never'
  const date = parseBrokerInstant(expiresAt)
  if (!date) return '—'
  const delta = date.getTime() - now
  if (Math.abs(delta) < 1000) return 'now'
  return delta > 0 ? `in ${span(delta)}` : `${span(-delta)} ago`
}

const EXPIRED_TITLE =
  'Past its expiry and still stored: every read already treats this key as absent, and the ' +
  'sweeper has not collected the row yet. It still occupies space and still counts in this ' +
  'namespace’s exact key count, which is why this page shows it instead of hiding it.'
const LIVE_TITLE = 'Readable right now — a get or a getPrefix on this namespace returns it.'

/**
 * The "State" column: what the BROKER said about this row, in two words.
 *
 * Two states and not three: "expiring soon" would be this browser deciding
 * something the database decides (point 3 in the header), and the Expires
 * column already says how long is left.
 */
export function describeKvState(row) {
  if (row && row.expired === true) {
    return { label: 'expired', note: 'awaiting sweep', tone: 'warn', title: EXPIRED_TITLE }
  }
  return { label: 'live', note: '', tone: 'mute', title: LIVE_TITLE }
}

/**
 * What the foot of the page can honestly say about its own end, or `''`.
 *
 * The one case worth a sentence is a page that came back SHORT of the row
 * limit and is still `truncated`: the stored procedure stopped on the 4 MiB
 * byte budget, keeping the row that straddles it. Without the sentence a
 * namespace of fat values looks like a broken pager — "I asked for 100 and got
 * 7, and there is still a Next" — and the operator lowers a limit that was
 * never the binding constraint.
 */
export function describePageEnd({ rowCount = 0, truncated = false, limit = 0, prefixed = false } = {}) {
  if (!Number.isFinite(rowCount) || rowCount <= 0) return ''
  if (truncated) {
    return Number.isFinite(limit) && limit > 0 && rowCount < limit
      ? 'this page stopped on the 4 MiB byte budget, not on the row limit'
      : ''
  }
  return prefixed ? 'end of the prefix' : 'end of the namespace'
}

/**
 * `{namespaces:[{namespace, keys}]}` → the selector's rows, IN THE ORDER THE
 * BROKER GAVE THEM (byte order, `COLLATE "C"`), which is the order the list
 * itself walks. Re-sorting them in the browser would put the selector and the
 * pages it opens in two different orders for no gain.
 *
 * The bare array is accepted too: that is what `queen.kv_namespaces_v1`
 * returns, and a direct-to-broker debugging session sees it unwrapped.
 */
export function namespaceOptions(payload) {
  const raw = Array.isArray(payload) ? payload : (payload && Array.isArray(payload.namespaces) ? payload.namespaces : null)
  if (!raw) return []
  const rows = []
  for (const entry of raw) {
    const namespace = typeof entry?.namespace === 'string' ? entry.namespace : null
    if (!namespace) continue
    const keys = Number.isFinite(entry?.keys) ? entry.keys : null
    // The count is EXACT (a count(*) at the moment of the call), so it is
    // rendered in full with separators rather than abbreviated to "1.2M": the
    // approximate figure on this page is the header's, and only the header's.
    const label = keys === null
      ? namespace
      : `${namespace} · ${keys.toLocaleString('en-US')} ${keys === 1 ? 'key' : 'keys'}`
    rows.push({ namespace, keys, label })
  }
  return rows
}

/**
 * Is the sweeper's tenant-wide footprint a MEASUREMENT, or the absence of one?
 *
 * `kvRows` / `kvBytes` ride on the queue listing and come from the sweeper's
 * five-minute snapshot (`queen.kv_usage`). A cell that has not run the usage
 * phase yet — it runs every `QUEEN_KV_USAGE_EVERY_MS`, and it can be switched
 * off for the process — simply has no row, and the broker does NOT distinguish
 * that from an empty tenant on the wire: `handlers/queues.rs` sends `kvRows: 0`
 * for a missing snapshot and omits the four fields only when the read itself
 * failed. So a freshly booted cell holding 30k keys answers `kvRows: 0`, and
 * the header renders a confident `≈ 0 live keys` next to a selector that says
 * `30,000 in perf` — two numbers on one strip, one of them certainly wrong.
 *
 * The namespace listing is the witness, and this page has already fetched it:
 * it is exact and it counts every row. A zero it contradicts is not a
 * measurement, so the page prints nothing at all — `—` would be a claim of its
 * own, and the honest rendering of "this cell has not measured yet" is silence.
 * A genuinely empty tenant still shows `≈ 0`, because then the two figures
 * agree.
 */
export function sweeperUsageIsMeasured(kvRows, namespaces) {
  if (!Number.isFinite(kvRows)) return false
  if (kvRows > 0) return true
  const known = (Array.isArray(namespaces) ? namespaces : [])
    .reduce((n, o) => n + (Number.isFinite(o?.keys) ? o.keys : 0), 0)
  return known === 0
}

/**
 * The broker's own sentence for a refusal it minted, or null.
 *
 * The KV handlers answer `{error:<code>, reason:<identifier>, detail:<human>}`
 * (handlers/kv.rs err()), while api/errors.js describeApiError only knows the
 * proxy's `{error:<message>, code:<code>}` envelope — so on a 400
 * `kv_bad_namespace` the sentence that names what to fix is in a field the
 * shared formatter never reads. Views call this first and fall back to
 * describeApiError, exactly as views/Timers.vue does for the count route.
 */
export function kvRefusalText(err) {
  const body = err && typeof err.body === 'object' ? err.body : null
  if (!body) return null
  if (typeof body.detail === 'string' && body.detail) return body.detail
  if (typeof body.reason === 'string' && body.reason) return body.reason
  return null
}
