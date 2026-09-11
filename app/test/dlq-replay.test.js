// Replaying a dead letter from the console (PLAN_DASHBOARD_ACTIONS.md §2.3).
//
// The rule under test is the one the button was withdrawn for in 8d357fa4: a
// replay control may only report what the broker actually said about the row.
// Three of the five answers are traps —
//
//   200 `duplicate`  nothing was written AND nothing was removed: the row is
//                    still dead-lettered, and the ids in `replayedAs` name a
//                    frame that was never stored
//   404 `gone`       not a failure: the row was already replayed or purged, and
//                    the list on screen is simply stale
//   404 anything else  the ROUTE is missing (old broker, route_blocked, the SPA
//                    fallback) and says nothing at all about the row
//
// — so `replayVerdict` is the single mapping every entry point renders, and
// `removeRow` is the only thing allowed to drop a row from the table.
//
// The wire pinned here comes from server/src/handlers/messages.rs
// (`move_response`, `move_failed_response`, `handle_dlq_replay`'s 400/404
// bodies) and reaches the mapper as api/httpClient.js delivers it: a resolved
// `{data}` for 2xx, an `ApiError` carrying `status`, `code` and the parsed
// `body` for everything else.
//
// The request half answers to a SECOND wire. The proxy in front admits a named
// replay destination against the tenant's plan caps (gateway.rs
// `admit_replay_dest`), and the registry it asks can only admit a (queue,
// partition) PAIR — so a body naming one half is refused with a 400 that never
// reaches the broker. `replayRequest` therefore names both halves or names
// none, and the row supplies whichever half the operator left alone.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { ApiError } from '../src/api/errors.js'
import {
  dlqRowKey,
  replayRequest,
  replayTransactionId,
  replayVerdict,
} from '../src/composables/useDlqReplay.js'

/** One dead-letter row as `get_dlq_messages_v1` returns it (010_log_admin.sql). */
const row = (extra = {}) => ({
  id: '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
  transactionId: 'order-8891',
  partitionId: '0198f2bb-2c40-7aa1-9d51-0f3c2b7a1e44',
  queue: 'orders.created',
  partition: 'eu-1',
  consumerGroup: 'billing',
  errorMessage: 'handler threw',
  retryCount: 3,
  ...extra,
})

/** A 200 body in the shape `move_response` emits. */
const moved = (result, extra = {}) => ({
  data: {
    success: true,
    result,
    queue: 'orders.created',
    partition: 'eu-1',
    consumerGroup: 'billing',
    dlqId: row().id,
    originalTransactionId: 'order-8891',
    replayedAs: {
      index: 0,
      message_id: '0198f3aa-7b21-7c9d-8e14-5b7c9d0a1f22',
      transaction_id: `dlq:${row().id}`,
      queueName: 'orders.created',
      // `move_body` answers the zero-uuid sentinel on a duplicate: the frame
      // it minted was never stored, and the copy already in the log carries its
      // own id inside a segment blob no route reads back.
      message_id: result === 'duplicate'
        ? '00000000-0000-0000-0000-000000000000'
        : '0198f3aa-7b21-7c9d-8e14-5b7c9d0a1f22',
      status: result === 'duplicate' ? 'duplicate' : 'queued',
      offset: 4711,
    },
    // The SP deletes the source row only when it moved it (016_messages).
    dlqRowRemoved: result !== 'duplicate',
    ...extra,
  },
})

/** An ApiError as api/httpClient.js mints one: message from `error`, body parsed. */
const apiError = (status, body, { code = null, retryAfter = null } = {}) =>
  new ApiError(
    (body && typeof body === 'object' && body.error) || 'Request failed',
    { status, code, retryAfter, body, path: '/api/v1/dlq/x/replay' },
  )

// ---------------------------------------------------------------------------
// The two verdicts that mean the row is gone from queen.log_dlq
// ---------------------------------------------------------------------------

test('moved: the only answer that is a success, and it names the destination', () => {
  const v = replayVerdict(moved('moved'))
  assert.equal(v.kind, 'success')
  assert.equal(v.removeRow, true)
  // The row is verified gone, so there is nothing to go back to the broker for.
  assert.equal(v.refresh, false)
  assert.match(v.title, /orders\.created\/eu-1/)
  assert.match(v.detail, /offset 4711/)
  assert.match(v.detail, /dlq:0198f2c1/)
  // The two caveats the plan requires on screen: it appends, and the age clock
  // restarts at the destination.
  assert.match(v.detail, /does not restore/)
  assert.match(v.detail, /age starts again/)
  assert.deepEqual(v.target, {
    queue: 'orders.created',
    partition: 'eu-1',
    offset: 4711,
    transactionId: `dlq:${row().id}`,
    messageId: '0198f3aa-7b21-7c9d-8e14-5b7c9d0a1f22',
  })
})

test('duplicate: nothing was written, so nothing is removed and the row stays', () => {
  const v = replayVerdict(moved('duplicate'))
  // Not a success and not a "problem solved" either. The transaction id a
  // replay uses is derivable from the row id the listing publishes, so the
  // broker cannot prove the copy in the window is this dead letter — it keeps
  // the record, and the row has to stay on screen with it.
  assert.equal(v.kind, 'warning')
  assert.equal(v.removeRow, false)
  assert.equal(v.refresh, false)
  assert.match(v.title, /already holds this transaction id/)
  assert.match(v.detail, /Nothing was written and nothing was removed/)
  assert.match(v.detail, /offset 4711/)
  assert.match(v.detail, /dlq:0198f2c1/)
  // The trap: `replayedAs.message_id` belongs to a frame the broker did not
  // store. A view that rendered it would name a message that does not exist.
  assert.equal(v.target.messageId, null)
  assert.equal(v.target.offset, 4711)
})

test('offset 0 is a position, an absent offset is not', () => {
  const head = moved('moved')
  head.data.replayedAs.offset = 0
  assert.match(replayVerdict(head).detail, /at offset 0/)
  assert.equal(replayVerdict(head).target.offset, 0)

  const none = moved('moved')
  none.data.replayedAs.offset = null
  assert.doesNotMatch(replayVerdict(none).detail, /offset/)
  assert.equal(replayVerdict(none).target.offset, null)
})

// ---------------------------------------------------------------------------
// A 2xx is not a verdict
// ---------------------------------------------------------------------------

test('an unknown result never removes the row', () => {
  const v = replayVerdict({ data: { success: true, result: 'relocated' } })
  assert.equal(v.kind, 'error')
  assert.equal(v.removeRow, false)
  assert.equal(v.refresh, true)
  assert.match(v.title, /relocated/)
})

test('success:false and an empty body are failures, not quiet successes', () => {
  const refused = replayVerdict({ data: { success: false, result: 'moved' } })
  assert.equal(refused.kind, 'error')
  assert.equal(refused.removeRow, false)

  for (const answer of [{}, { data: null }, { data: 'ok' }, { data: [] }, null, undefined]) {
    const v = replayVerdict(answer)
    assert.equal(v.kind, 'error', `${JSON.stringify(answer)} must not pass`)
    assert.equal(v.removeRow, false)
    assert.match(v.title, /no verdict/)
  }
})

// ---------------------------------------------------------------------------
// 404, three ways
// ---------------------------------------------------------------------------

test('gone (id-addressed route): a warning, the row goes, the list reloads', () => {
  const v = replayVerdict(apiError(404, {
    success: false,
    result: 'gone',
    dlqId: row().id,
    error: 'Message not found',
    message: 'No dead-letter row with this id — it was already replayed or purged',
  }))
  // Never an error toast: a second click, another operator or the sweeper got
  // there first, and nothing was written by this attempt.
  assert.equal(v.kind, 'warning')
  assert.equal(v.removeRow, true)
  assert.equal(v.refresh, true)
  assert.match(v.title, /Already replayed or purged/)
})

test('gone (address-addressed route): the historical not-found shape counts too', () => {
  // POST /api/v1/messages/:pid/:txid/retry keeps its own 404 body — no
  // `result` field — and it means exactly the same thing.
  const v = replayVerdict(apiError(404, {
    success: false,
    partitionId: row().partitionId,
    transactionId: 'order-8891',
    error: 'Message not found',
    message: 'No dead-letter row for this address. Only dead-lettered messages can be replayed',
  }))
  assert.equal(v.kind, 'warning')
  assert.equal(v.removeRow, true)
})

test('a 404 that is about the ROUTE says nothing about the row', () => {
  // Three ways a cell says "not served here": the proxy's route_blocked, a
  // broker that predates the route (bare 404, no body), and the SPA fallback
  // the HTTP client reports as not_an_api_response.
  const blocked = apiError(404, { error: 'not found', code: 'route_blocked' }, { code: 'route_blocked' })
  const bare = apiError(404, '')
  const fallback = new ApiError('Endpoint does not exist on this broker', {
    status: 200, code: 'not_an_api_response', path: '/api/v1/dlq/x/replay',
  })

  for (const err of [blocked, bare, fallback]) {
    const v = replayVerdict(err)
    assert.equal(v.removeRow, false, 'a missing route must never drop a dead-letter row')
    assert.equal(v.refresh, false)
    assert.match(v.title, /not available here/)
    assert.match(v.detail, /untouched/)
    // The ONLY verdict the page may remember for the cluster epoch
    // (stores/routeSupport.js) and hide the button on.
    assert.equal(v.unavailable, true)
  }
})

test('only the missing-route 404 is remembered — `gone` is a 404 too', () => {
  // The trap this flag exists for: `routeSupport.guard` keys off any
  // missing-route error, and a page that guarded the call would disable replay
  // for the rest of the session after the first already-purged row.
  const gone = replayVerdict(apiError(404, {
    success: false,
    result: 'gone',
    error: 'Message not found',
  }))
  assert.ok(!gone.unavailable, 'an already-purged row must not take the button away')
  for (const v of [
    replayVerdict(moved('moved')),
    replayVerdict(moved('duplicate')),
    replayVerdict(apiError(500, { success: false, error: 'boom', dlqRowRemoved: false })),
    replayVerdict(apiError(403, { error: 'forbidden' }, { code: 'forbidden' })),
  ]) {
    assert.ok(!v.unavailable)
  }
})

// ---------------------------------------------------------------------------
// The refusals
// ---------------------------------------------------------------------------

test('400 points at the Advanced field that caused it', () => {
  const queueField = replayVerdict(apiError(400, {
    success: false,
    error: 'queue override must be a non-empty name',
    message: 'The replay body is optional; when present it may name a queue and/or a partition to move the message to',
  }))
  assert.equal(queueField.kind, 'error')
  assert.equal(queueField.field, 'queue')
  assert.equal(queueField.removeRow, false)
  assert.match(queueField.detail, /non-empty name\. Nothing was replayed/)

  const partitionField = replayVerdict(apiError(400, {
    success: false,
    error: 'partition override must be a non-empty name',
  }))
  assert.equal(partitionField.field, 'partition')

  // A malformed body belongs to neither field, and must not be pinned on one.
  const badBody = replayVerdict(apiError(400, {
    success: false,
    error: 'bad body: expected {} or {"queue":"...","partition":"..."}',
  }))
  assert.equal(badBody.field, null)
})

test('a plan refusal is worded as a plan refusal, not as a role refusal', () => {
  // The proxy applies the PUSH blocks to the replay routes, because a replay
  // grows retained bytes exactly like a push. "Not permitted for your role"
  // would send a storage-blocked tenant to the wrong person.
  const blocked = replayVerdict(apiError(403, { error: 'storage quota exceeded', code: 'storage_quota_exceeded' }, { code: 'storage_quota_exceeded' }))
  assert.equal(blocked.kind, 'error')
  assert.equal(blocked.removeRow, false)
  assert.match(blocked.detail, /Storage quota exceeded/)
  assert.match(blocked.detail, /untouched/)

  // A real role refusal still gets the product's shared sentence.
  const forbidden = replayVerdict(apiError(403, { error: 'forbidden', code: 'forbidden' }, { code: 'forbidden' }))
  assert.match(forbidden.detail, /Not permitted for your role/)

  // 429 keeps its Retry-After, and the two sentences do not run together.
  const limited = replayVerdict(apiError(429, { error: 'rate limited' }, { retryAfter: 30 }))
  assert.match(limited.detail, /retry in 30s\. Nothing was replayed/)
})

test('500: the row is untouched, and the verdict says so', () => {
  const v = replayVerdict(apiError(500, {
    success: false,
    error: 'dlq move failed: deadlock detected',
    dlqRowRemoved: false,
    message: 'Nothing was replayed — the dead-letter row is untouched',
  }))
  assert.equal(v.kind, 'error')
  assert.equal(v.removeRow, false)
  assert.equal(v.refresh, false)
  assert.match(v.detail, /deadlock detected\. Nothing was replayed/)
  assert.match(v.detail, /safely be sent again/)
})

test('500 with no stated row state: the outcome is unknown, and it says so', () => {
  // `dlqRowRemoved` absent or null = the broker never learned what happened: a
  // transport error with no SQLSTATE (the connection can be lost AFTER a
  // single-statement transaction committed) or a verdict it could not read.
  // Claiming "the row is untouched" there would be a guess.
  for (const body of [
    { success: false, error: 'dlq move failed: connection closed', dlqRowRemoved: null },
    { success: false, error: 'dlq move returned an unknown verdict: {"result":"parked"}' },
  ]) {
    const v = replayVerdict(apiError(500, body))
    assert.equal(v.kind, 'error')
    assert.equal(v.removeRow, false, 'an unknown outcome must never drop a row')
    assert.equal(v.refresh, true, 'the list is the only way to find out what happened')
    assert.match(v.title, /outcome is unknown/)
    assert.match(v.detail, /if the row is gone, the move happened/)
    assert.doesNotMatch(v.detail, /safely be sent again/)
  }
})

test('503: push maintenance refuses the move, and the row is untouched', () => {
  // A move cannot be spooled the way a push is — the spool carries frames, not
  // the removal of a dead-letter record — so the broker refuses it outright
  // while the switch is on. That is a state of the cell, not of this row.
  const v = replayVerdict(apiError(503, {
    success: false,
    result: 'maintenance',
    error: 'push maintenance is on',
    dlqRowRemoved: false,
    message: 'Push maintenance is on, so nothing may be written to the log. The dead-letter row is untouched; replay it once maintenance is off',
  }))
  assert.equal(v.kind, 'warning')
  assert.equal(v.removeRow, false)
  assert.equal(v.refresh, false)
  assert.match(v.title, /Push maintenance is on/)
  assert.match(v.detail, /untouched/)
  assert.match(v.detail, /once the maintenance/)
})

test('no answer at all: the outcome is unknown, so nothing is claimed', () => {
  // A move that commits and then loses its response looks exactly like one that
  // never ran. The row stays, and the list is reloaded to find out.
  const v = replayVerdict(new ApiError('Network error', { status: 0, path: '/api/v1/dlq/x/replay' }))
  assert.equal(v.kind, 'error')
  assert.equal(v.removeRow, false)
  assert.equal(v.refresh, true)
  assert.match(v.title, /outcome is unknown/)
  assert.match(v.detail, /Cannot reach the API/)
})

// ---------------------------------------------------------------------------
// The request the button sends
// ---------------------------------------------------------------------------

test('an untouched Advanced section sends an empty body', () => {
  const r = replayRequest(row())
  assert.deepEqual(r.body, {})
  assert.equal(r.namesDestination, false, 'nothing to admit: the broker resolves the row’s own address')
  // Both halves fall back to the source row, which is what the confirm modal
  // has to name — the broker resolves the same way.
  assert.equal(r.queue, 'orders.created')
  assert.equal(r.partition, 'eu-1')
  assert.equal(r.moved, false)

  // Blank and whitespace are the same as untouched: sending `""` would ask the
  // broker to provision a queue nobody can name (and it refuses, with a 400).
  assert.deepEqual(replayRequest(row(), { queue: '', partition: '   ' }).body, {})
  assert.equal(replayRequest(row(), { queue: '', partition: '   ' }).namesDestination, false)
})

test('either half filled sends BOTH halves — a destination is admitted as a pair', () => {
  // The coordination this pins: the proxy admits a named replay destination
  // against the plan's caps through the registry (gateway.rs
  // `admit_replay_dest`), and the registry takes (queue, partition) pairs only.
  // A body naming one half is refused 400 before the broker sees it, so the
  // omitted half is filled in HERE from the row — with the same value the
  // handler would have resolved it to.
  const queueOnly = replayRequest(row(), { queue: 'orders.retry' })
  assert.deepEqual(queueOnly.body, { queue: 'orders.retry', partition: 'eu-1' })
  assert.equal(queueOnly.partition, 'eu-1', 'an omitted half keeps the source partition')
  assert.equal(queueOnly.namesDestination, true)
  assert.equal(queueOnly.moved, true)

  const partitionOnly = replayRequest(row(), { partition: ' eu-2 ' })
  assert.deepEqual(partitionOnly.body, { queue: 'orders.created', partition: 'eu-2' })
  assert.equal(partitionOnly.queue, 'orders.created')
  assert.equal(partitionOnly.moved, true)

  // Both typed: trimmed, and sent exactly as typed.
  const both = replayRequest(row(), { queue: ' orders.retry ', partition: ' eu-2 ' })
  assert.deepEqual(both.body, { queue: 'orders.retry', partition: 'eu-2' })

  // Typing the row's own address back in is not a move, but it is still a named
  // destination — the pair travels, and the proxy admits the pair it already
  // carries.
  const same = replayRequest(row(), { queue: 'orders.created', partition: 'eu-1' })
  assert.deepEqual(same.body, { queue: 'orders.created', partition: 'eu-1' })
  assert.equal(same.namesDestination, true)
  assert.equal(same.moved, false)

  // A half the ROW cannot supply is the one case that still goes out alone: the
  // refusal then names the missing half, under the input that has to fill it,
  // where dropping the override would replay the message somewhere nobody asked
  // for — silently.
  assert.deepEqual(
    replayRequest(row({ partition: null }), { queue: 'orders.retry' }).body,
    { queue: 'orders.retry' },
  )
  assert.deepEqual(
    replayRequest(row({ queue: undefined }), { partition: 'eu-2' }).body,
    { partition: 'eu-2' },
  )
})

// ---------------------------------------------------------------------------
// Which row the page is talking about
// ---------------------------------------------------------------------------

test('a row is identified by its DLQ row id, never by the transaction id alone', () => {
  // The collision this exists for: queen.log_dlq holds one record PER CONSUMER
  // GROUP for the same frame, so these two rows are two different records that
  // share every address field the old key was built from. A surface keyed on
  // `transactionId` resolves both to whichever one the list happened to put
  // first — and its Replay button then moves that consumer group's record
  // instead of the one that was clicked (§1.3).
  const billing = row({ id: 'row-billing', consumerGroup: 'billing' })
  const ledger = row({ id: 'row-ledger', consumerGroup: 'ledger' })
  assert.equal(billing.transactionId, ledger.transactionId)
  assert.notEqual(dlqRowKey(billing), dlqRowKey(ledger))
  assert.equal(dlqRowKey(billing), 'row-billing')

  // The id is the address `POST /api/v1/dlq/:id/replay` takes, so the identity
  // and the thing the replay sends are the same string.
  assert.equal(replayRequest(billing).id, dlqRowKey(billing))
})

test('a listing with no row id falls back to transaction id PLUS group', () => {
  // A broker older than the `id` key in get_dlq_messages_v1 cannot be replayed
  // by id at all, but its rows still have to be told apart on screen — and the
  // pair that separates them is the one the DLQ table itself is keyed by.
  const billing = row({ id: undefined, consumerGroup: 'billing' })
  const ledger = row({ id: undefined, consumerGroup: 'ledger' })
  assert.notEqual(dlqRowKey(billing), dlqRowKey(ledger))
  assert.equal(dlqRowKey(billing), 'order-8891::billing')

  // Whitespace and blanks are not identities.
  assert.equal(dlqRowKey({ id: '  ' , transactionId: 'tx', consumerGroup: '' }), 'tx::')
  assert.equal(dlqRowKey({}), null)
  assert.equal(dlqRowKey(null), null)
  assert.equal(dlqRowKey(undefined), null)
})

test('the DLQ view selects and replays by the row identity, and purges by the address', () => {
  // The behaviour needs a DOM this suite does not have, so the contract between
  // the two files is pinned the way push.test.js pins the Autocomplete's: the
  // view imports the helper, the table/drawer/selection are keyed on it, and
  // `msgKey` survives ONLY where delete_message_v1's every-group semantics are
  // the truth.
  const view = readFileSync(new URL('../src/views/DeadLetter.vue', import.meta.url), 'utf8')

  assert.match(view, /import \{ dlqRowKey,/)
  assert.match(view, /const rowKey = dlqRowKey/)
  // Row key, selection, the drawer's row, and the stale-selection drop.
  assert.match(view, /:key="rowKey\(msg\)"/)
  assert.match(view, /rowKey\(msg\) === selectedKey/)
  assert.match(view, /find\(m => rowKey\(m\) === selectedKey\.value\)/)
  assert.match(view, /rows\.some\(m => rowKey\(m\) === selectedKey\.value\)/)
  // Nothing resolves a row by the message address any more.
  assert.doesNotMatch(view, /msgKey\(m\) === selectedKey\.value/)
  assert.doesNotMatch(view, /selectedKey\.value === msgKey/)

  // The purge keeps it, deliberately: one call deletes every consumer group's
  // record under the address, so its in-flight flag, its error line and its
  // suppression set are all address-wide.
  assert.match(view, /deleting\.value\.has\(msgKey\(msg\)\)/)
  assert.match(view, /rowErrors\.value\.get\(msgKey\(msg\)\)/)
  assert.match(view, /purged\.value\.has\(msgKey\(m\)\)/)
})

test('the transaction id is the row id, every time', () => {
  // Deterministic by construction: this is what makes a second replay a
  // `duplicate` instead of a second copy of the message.
  assert.equal(replayTransactionId('abc'), 'dlq:abc')
  assert.equal(replayRequest(row()).transactionId, `dlq:${row().id}`)
  // A list that carries no row id cannot be replayed by id at all — the button
  // has nothing to address.
  assert.equal(replayRequest(row({ id: undefined })).transactionId, null)
  assert.equal(replayRequest(row({ id: undefined })).id, null)
})
