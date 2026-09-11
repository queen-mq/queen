import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  KV_PAGE_BUDGET_BYTES,
  describeKvState,
  describePageEnd,
  formatExpiry,
  kvListBody,
  kvRefusalText,
  namespaceOptions,
  sweeperUsageIsMeasured,
  valueBytes,
} from '../src/composables/useKvView.js'

// The KV browser's pure half (PLAN_DASHBOARD_ACTIONS.md §2.5). Everything here
// is a rule about what reaches the wire or what a cell renders — the parts that
// can be wrong for weeks without anybody noticing, because the page still
// paints.

// ---------------------------------------------------------------------------
// The request body
// ---------------------------------------------------------------------------

test('the first page asks for a namespace and nothing else', () => {
  const body = kvListBody({ namespace: 'orders', limit: 100 })
  assert.deepEqual(body, { namespace: 'orders', includeExpired: true, limit: 100 })
  // Not `prefix: ''`, not `after: null`: the SP reads both as absent, but a
  // body that carries them makes the request read as a filtered one.
  assert.equal('prefix' in body, false)
  assert.equal('after' in body, false)
})

test('includeExpired is sent on every page, and it is true', () => {
  // D5: an expired row still counts in the namespace figure beside the list, so
  // a page that hid them would contradict the count it prints. The ROUTE
  // defaults to false — it matches its stored procedure and §5.7, so a caller
  // who did not ask is not handed rows every other KV read treats as absent —
  // which is exactly why showing them has to be said here, on every call.
  for (const body of [
    kvListBody({ namespace: 'orders' }),
    kvListBody({ namespace: 'orders', prefix: 'wh.', after: 'wh.a', limit: 250 }),
  ]) {
    assert.equal(body.includeExpired, true)
  }
})

test('an empty cursor is not a cursor', () => {
  // Under COLLATE "C" every key sorts after the empty string, so an `after: ""`
  // that was taken as a cursor would answer an empty first page forever. The SP
  // NULLIFs it; this never sends it.
  assert.equal('after' in kvListBody({ namespace: 'orders', after: '' }), false)
  assert.equal('after' in kvListBody({ namespace: 'orders', after: null }), false)
  assert.equal(kvListBody({ namespace: 'orders', after: 'wh.deliver:b15f' }).after, 'wh.deliver:b15f')
})

test('the prefix travels verbatim — it is a byte range, not a search box', () => {
  // `%` and `_` are ordinary bytes to `starts_with` (024_kv.sql), and a
  // trailing space is a legal byte in a key: trimming or escaping either here
  // would silently answer a different question than the operator asked.
  assert.equal(kvListBody({ namespace: 'g', prefix: '100%_' }).prefix, '100%_')
  assert.equal(kvListBody({ namespace: 'g', prefix: 'order ' }).prefix, 'order ')
})

test('a limit is passed through for the stored procedure to clamp', () => {
  assert.equal(kvListBody({ namespace: 'orders', limit: 250 }).limit, 250)
  // 1..1000 is clamped server-side and never rejected, so nothing here
  // second-guesses it — but a missing limit is omitted rather than guessed.
  assert.equal(kvListBody({ namespace: 'orders', limit: 5000 }).limit, 5000)
  assert.equal('limit' in kvListBody({ namespace: 'orders' }), false)
  assert.equal('limit' in kvListBody({ namespace: 'orders', limit: Number.NaN }), false)
})

test('the page budget the sentences quote is the stored procedure\'s', () => {
  assert.equal(KV_PAGE_BUDGET_BYTES, 4 * 1024 * 1024)
})

// ---------------------------------------------------------------------------
// Sizes
// ---------------------------------------------------------------------------

test('a value is measured in bytes, and a stored null is a value', () => {
  assert.equal(valueBytes({ value: { a: 1 } }), 7)          // {"a":1}
  assert.equal(valueBytes({ value: 'hi' }), 4)              // "hi"
  assert.equal(valueBytes({ value: null }), 4)              // null — four bytes, not "absent"
  assert.equal(valueBytes({ value: '€' }), 5)               // three UTF-8 bytes plus two quotes
  // A keysOnly page carries no `value` field at all, and THAT is the absence.
  assert.equal(valueBytes({ key: 'k', version: 3 }), null)
  assert.equal(valueBytes(null), null)
})

// ---------------------------------------------------------------------------
// Expiry
// ---------------------------------------------------------------------------

test('no expiry is "never", an unreadable one is an em dash', () => {
  assert.equal(formatExpiry(null), 'never')
  assert.equal(formatExpiry(undefined), 'never')
  assert.equal(formatExpiry(''), 'never')
  assert.equal(formatExpiry('not a date'), '—')
})

test('expiry reads forwards and backwards from one instant', () => {
  const now = Date.parse('2026-09-11T10:00:00Z')
  assert.equal(formatExpiry('2026-09-11T14:00:00Z', now), 'in 4h')
  assert.equal(formatExpiry('2026-09-11T10:00:30Z', now), 'in 30s')
  // Past is "ago", never "overdue": an expired key is not late, it is already
  // gone from every read (the opposite of useTimers.formatDeliverIn).
  assert.equal(formatExpiry('2026-09-11T09:48:00Z', now), '12m ago')
  assert.equal(formatExpiry('2026-09-08T10:00:00Z', now), '3d ago')
  assert.equal(formatExpiry('2026-09-11T10:00:00.500Z', now), 'now')
})

test('the KV stamp — microseconds, UTC, a literal Z — parses', () => {
  // `queen.kv_list_v1` renders both stamps with
  // `to_char(… AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, the house
  // convention: six fractional digits and a Z, whatever the broker session's
  // TimeZone happens to be (pinned in server/tests/kv_console_list.rs).
  const now = Date.parse('2026-09-11T06:36:21.185Z') + 45_000
  assert.equal(formatExpiry('2026-09-11T06:36:21.185976Z', now), '45s ago')
  // The offset forms still parse, and that is deliberate: a cell running an
  // older broker answers `…+00:00` from the raw timestamptz this render
  // replaced, and an offset that is not UTC is still an instant, not a wall
  // clock. Reading them costs nothing; refusing them would blank the column.
  assert.equal(formatExpiry('2026-09-11T06:36:21.185976+00:00', now), '45s ago')
  assert.equal(formatExpiry('2026-09-11T08:36:21.185976+02:00', now), '45s ago')
})

// ---------------------------------------------------------------------------
// Row state
// ---------------------------------------------------------------------------

test('state comes off the row, never off the browser clock', () => {
  const expired = describeKvState({ expired: true, expiresAt: '2099-01-01T00:00:00+00:00' })
  assert.equal(expired.label, 'expired')
  assert.equal(expired.note, 'awaiting sweep')
  assert.equal(expired.tone, 'warn')
  // The broker said expired even though the stamp is decades away (a clock this
  // browser cannot see is the authority) — and the page still says expired.
  assert.match(expired.title, /sweeper/)

  assert.equal(describeKvState({ expired: false }).label, 'live')
  assert.equal(describeKvState({}).label, 'live')
  assert.equal(describeKvState(null).label, 'live')
})

// ---------------------------------------------------------------------------
// The foot of the page
// ---------------------------------------------------------------------------

test('a full page that is truncated says nothing — Next says it', () => {
  assert.equal(describePageEnd({ rowCount: 100, truncated: true, limit: 100 }), '')
})

test('a short truncated page names the byte budget', () => {
  // Seven fat rows out of a hundred asked for, and there is still a Next: the
  // 4 MiB budget stopped the page, and without this sentence the pager looks
  // broken and the operator lowers a limit that was never binding.
  assert.equal(
    describePageEnd({ rowCount: 7, truncated: true, limit: 100 }),
    'this page stopped on the 4 MiB byte budget, not on the row limit',
  )
})

test('the last page names what it is the end of', () => {
  assert.equal(describePageEnd({ rowCount: 12, truncated: false, limit: 100 }), 'end of the namespace')
  assert.equal(
    describePageEnd({ rowCount: 12, truncated: false, limit: 100, prefixed: true }),
    'end of the prefix',
  )
  // An empty page has its own empty state; the pager adds nothing.
  assert.equal(describePageEnd({ rowCount: 0, truncated: false, limit: 100 }), '')
  assert.equal(describePageEnd(), '')
})

// ---------------------------------------------------------------------------
// The namespace selector
// ---------------------------------------------------------------------------

test('namespaces keep the broker\'s byte order and carry exact counts', () => {
  const rows = namespaceOptions({
    namespaces: [
      { namespace: 'Billing', keys: 1 },
      { namespace: 'a-graph', keys: 27_413 },
      { namespace: 'orders', keys: 0 },
    ],
  })
  // "B" before "a": COLLATE "C" is byte order, and the list walks it in the
  // same order, so the selector must not re-sort into locale order.
  assert.deepEqual(rows.map(r => r.namespace), ['Billing', 'a-graph', 'orders'])
  assert.equal(rows[0].label, 'Billing · 1 key')
  assert.equal(rows[1].label, 'a-graph · 27,413 keys')
  assert.equal(rows[2].label, 'orders · 0 keys')
})

test('the bare array the stored procedure returns is accepted too', () => {
  assert.equal(namespaceOptions([{ namespace: 'orders', keys: 3 }])[0].keys, 3)
  // A row without a namespace is not a namespace; a body that is not a listing
  // is an empty selector, never a crash on a page whose job is to render.
  assert.deepEqual(namespaceOptions({ namespaces: [{ keys: 3 }, null, 'orders'] }), [])
  assert.deepEqual(namespaceOptions(null), [])
  assert.deepEqual(namespaceOptions({}), [])
  assert.equal(namespaceOptions([{ namespace: 'orders' }])[0].label, 'orders')
})

// ---------------------------------------------------------------------------
// The tenant footprint on the scope strip
// ---------------------------------------------------------------------------

test('a sweeper zero the namespace listing contradicts is not a measurement', () => {
  // The broker sends `kvRows: 0` both for "this tenant holds nothing" and for
  // "queen.kv_usage has no row for this tenant yet" (handlers/queues.rs omits
  // the field only when the READ failed), and the usage phase runs every five
  // minutes and can be switched off. Printing `≈ 0 live keys` beside a selector
  // that says 30,000 is the one reading an operator files as a bug.
  const ns = [{ namespace: 'perf', keys: 30_000 }]
  assert.equal(sweeperUsageIsMeasured(0, ns), false)
  // A real zero is still printed: with nothing in any namespace the two figures
  // agree, and silence there would hide a working measurement.
  assert.equal(sweeperUsageIsMeasured(0, []), true)
  assert.equal(sweeperUsageIsMeasured(0, [{ namespace: 'orders', keys: 0 }]), true)
  // Any positive snapshot is a measurement, whatever the selector says — the
  // two count different populations (live rows vs every row) and are allowed to
  // disagree; only the zero is unreadable.
  assert.equal(sweeperUsageIsMeasured(27_000, ns), true)
  assert.equal(sweeperUsageIsMeasured(1, ns), true)
  // `null` is "the broker did not say", which the strip already renders as
  // nothing at all.
  assert.equal(sweeperUsageIsMeasured(null, ns), false)
  assert.equal(sweeperUsageIsMeasured(undefined, null), false)
})

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

test('a broker refusal speaks for itself', () => {
  const bad = {
    status: 400,
    code: null,
    body: {
      error: 'kv_bad_request',
      reason: 'kv_bad_namespace',
      detail: 'namespace \'Orders!\' does not match ^[a-z0-9][a-z0-9._-]{0,63}$',
    },
  }
  assert.match(kvRefusalText(bad), /does not match/)
  // The stable identifier when there is no human half, and nothing at all when
  // the envelope is the proxy's — that one describeApiError already handles.
  assert.equal(kvRefusalText({ status: 400, body: { error: 'kv_bad_request', reason: 'kv_bad_body' } }), 'kv_bad_body')
  assert.equal(kvRefusalText({ status: 403, code: 'feature_gated', body: null }), null)
  assert.equal(kvRefusalText(null), null)
})
