// Pushing a message from the console (PLAN_DASHBOARD_ACTIONS.md §2.1).
//
// The rules worth a test are the ones about not calling a push a success it was
// not: `pushVerdict`, which reads the broker's PER-ITEM status instead of the
// HTTP status; which ids that status entitles the modal to print; what the
// textarea is allowed to send; how an overloaded proxy refusal is worded; and
// which filters the Messages list has to drop afterwards or it answers a good
// push with an empty table. They live in @/composables/usePushVerdict as pure
// functions precisely so they can be asserted here rather than by opening a
// modal and squinting at it — the two that cannot (a blur, a transport) are
// pinned through the source and through `createApiClient` at the end.
//
// The wire this file pins comes from crates/queen-protocol/src/push.rs
// (`PushStatus`, `PushResult`) and server/src/handlers/data.rs
// (`render_push_results`): a top-level array, mixed snake/camel casing, and an
// `offset` key that is ABSENT rather than null when the broker allocated none.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { ApiError } from '../src/api/errors.js'
import { createApiClient } from '../src/api/httpClient.js'
import {
  ACCEPTED_STATUSES,
  MAX_PAYLOAD_TEXT_BYTES,
  describePushRefusal,
  filtersForPushedMessage,
  isEncryptedEnvelope,
  offsetLine,
  parsePayload,
  payloadToText,
  pushVerdict,
  showsIds,
  showsMessageId,
  worstResult,
} from '../src/composables/usePushVerdict.js'

/** One result in the shape `render_push_results` emits. */
const result = (status, extra = {}) => ({
  index: 0,
  message_id: '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
  transaction_id: 'order-8891',
  queueName: 'orders.created',
  status,
  ...extra,
})

// ---------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------

test('verdict: only `queued` is a success', () => {
  assert.equal(pushVerdict([result('queued')]).kind, 'success')

  // The three the old dashboard would have painted green on a 201, and the two
  // reasons each one is not: nothing was written, and nothing is consumable.
  assert.equal(pushVerdict([result('duplicate')]).kind, 'warning')
  assert.equal(pushVerdict([result('buffered')]).kind, 'warning')
  assert.equal(pushVerdict([result('error')]).kind, 'error')
  assert.equal(pushVerdict([result('failed')]).kind, 'error')
})

test('verdict: each status says what happened to the message', () => {
  // A duplicate must not read as "stored": the ids that come back are the
  // pre-existing message's, and the operator has to know the difference.
  const duplicate = pushVerdict([result('duplicate')])
  assert.match(duplicate.title, /Duplicate/)
  assert.match(duplicate.detail, /dedup window/)
  assert.match(duplicate.detail, /stored nothing/)

  // Buffered is accepted but NOT in the queue — the distinction the spool
  // exists to make.
  const buffered = pushVerdict([result('buffered')])
  assert.match(buffered.detail, /spool/)
  assert.match(buffered.detail, /consume/)

  // `failed` is the only status that means the message is gone.
  assert.match(pushVerdict([result('failed')]).detail, /holding nothing/)
})

test('verdict: buffered and failed do not blame maintenance on their own', () => {
  // `handle_push` spools the items whose DB transaction failed and re-labels
  // them `buffered` / `failed` with a 201 (RUSTFIX item 1) while maintenance is
  // OFF, so a verdict that states "push maintenance is on" as a fact sends the
  // operator to a switch that is not the cause. Both details word the
  // disjunction the broker actually produces.
  const buffered = pushVerdict([result('buffered')]).detail
  assert.match(buffered, /maintenance/)
  assert.match(buffered, /database/)
  // And a buffered message keeps only its transaction id through the spool.
  assert.match(buffered, /transaction id/)

  const failed = pushVerdict([result('failed')]).detail
  assert.match(failed, /database/)
  assert.match(failed, /spool/)
  assert.doesNotMatch(failed, /maintenance is on/)
})

test('verdict: an unknown status is never a success', () => {
  // A newer broker inventing a sixth status must leave the modal open and the
  // push unconfirmed, not fall through to a green box.
  const verdict = pushVerdict([result('deferred')])
  assert.equal(verdict.kind, 'error')
  assert.match(verdict.title, /deferred/)
  assert.match(verdict.detail, /cannot/)
})

test('verdict: no answer is a failure, not a quiet success', () => {
  for (const answer of [[], null, undefined, {}, 'ok', 201]) {
    const verdict = pushVerdict(answer)
    assert.equal(verdict.kind, 'error', `${JSON.stringify(answer)} must not pass`)
    assert.match(verdict.title, /no result/)
  }
})

test('verdict: a mixed array is judged by its WORST item', () => {
  // The console pushes one item, so this is defensive — but reading results[0]
  // would let a queued first item hide an errored second one.
  assert.equal(pushVerdict([result('queued'), result('error', { index: 1 })]).kind, 'error')
  assert.equal(pushVerdict([result('queued'), result('duplicate', { index: 1 })]).kind, 'warning')
  assert.equal(pushVerdict([result('duplicate'), result('failed', { index: 1 })]).kind, 'error')
  assert.equal(pushVerdict([result('queued'), result('queued', { index: 1 })]).kind, 'success')
})

test('verdict: a multi-item answer says which item it is describing', () => {
  const verdict = pushVerdict([result('queued'), result('error', { index: 1 })])
  assert.match(verdict.detail, /2 results/)
  assert.match(verdict.detail, /item 1/)
  // A single result is the normal case and says nothing about batches.
  assert.doesNotMatch(pushVerdict([result('queued')]).detail, /results;/)
})

test('verdict: the item the ids come from is the one the verdict is about', () => {
  const bad = result('error', { index: 1, message_id: 'm2', transaction_id: 't2' })
  assert.equal(worstResult([result('queued'), bad]), bad)
  // Ties go to the earliest item, so the first of two duplicates is reported.
  const first = result('duplicate')
  assert.equal(worstResult([first, result('duplicate', { index: 1 })]), first)
  assert.equal(worstResult([]), null)
  assert.equal(worstResult(null), null)
})

test('verdict: an absent offset is not offset 0', () => {
  // `render_push_results` OMITS the key when the broker allocated none
  // (buffered, failed, error), as does every broker older than the field.
  // Rendering 0 there would claim the head of the partition, so the renderer
  // asks `offsetLine`, which answers null for absent AND for an explicit null.
  assert.equal(offsetLine(result('buffered')), null)
  assert.equal(offsetLine(result('queued', { offset: null })), null)
  assert.equal(offsetLine(result('queued', { offset: 0 })), 0)
  assert.equal(offsetLine(result('queued', { offset: 41 })), 41)
  assert.equal(offsetLine(null), null)
  assert.equal(offsetLine(undefined), null)
})

test('verdict: ids are rendered only where the broker took the message', () => {
  // PushStatus::accepted() (crates/queen-protocol/src/push.rs). A message id
  // printed beside "the message is lost" names a message that does not exist.
  assert.deepEqual(ACCEPTED_STATUSES, ['queued', 'duplicate', 'buffered'])
  for (const status of ACCEPTED_STATUSES) assert.equal(showsIds(status), true, status)
  for (const status of ['error', 'failed', 'deferred', undefined, null, '']) {
    assert.equal(showsIds(status), false, `${status} must show no ids`)
  }
})

test('verdict: a buffered result shows no message id', () => {
  // The spool record carries no message id (`WriteEvent`, file_buffer.rs) and
  // the drain mints a fresh one per replayed event (`build_segment`), so the id
  // the broker returns beside `buffered` will never identify a row. The
  // transaction id IS spooled, and is the handle the verdict points at.
  assert.equal(showsMessageId('buffered'), false)
  assert.equal(showsMessageId('queued'), true)
  assert.equal(showsMessageId('duplicate'), true)
  for (const status of ['error', 'failed', undefined]) {
    assert.equal(showsMessageId(status), false, `${status} must show no message id`)
  }
})

// ---------------------------------------------------------------------------
// The payload
// ---------------------------------------------------------------------------

test('payload: every JSON value is a payload, not just an object', () => {
  // The broker stores `payload` verbatim as any JSON value
  // (webdoc reference/http/push), so the form must accept all six shapes.
  const cases = [
    ['{"id": 1}', { id: 1 }],
    ['[1, 2, 3]', [1, 2, 3]],
    ['"a string"', 'a string'],
    ['42', 42],
    ['-1.5e3', -1500],
    ['true', true],
    ['false', false],
    ['null', null],
  ]
  for (const [text, value] of cases) {
    const parsed = parsePayload(text)
    assert.equal(parsed.ok, true, `${text} must parse`)
    assert.deepEqual(parsed.value, value)
  }
})

test('payload: surrounding whitespace is not an error', () => {
  const parsed = parsePayload('\n  {"id": 1}\n\n')
  assert.equal(parsed.ok, true)
  assert.deepEqual(parsed.value, { id: 1 })
})

test('payload: empty is refused with a sentence that names the shapes', () => {
  for (const text of ['', '   \n ', null, undefined]) {
    const parsed = parsePayload(text)
    assert.equal(parsed.ok, false)
    assert.match(parsed.message, /empty/)
    // The most common first mistake is typing bare text, so the refusal shows
    // what a legal payload looks like instead of only saying "invalid".
    assert.match(parsed.message, /"string"/)
  }
})

test('payload: invalid JSON is refused with the parser’s own reason', () => {
  const parsed = parsePayload('order-17')
  assert.equal(parsed.ok, false)
  assert.match(parsed.message, /not valid JSON/i)
  // And the fix for the commonest case: unquoted text.
  assert.match(parsed.message, /quoted/)

  assert.equal(parsePayload('{"a": 1,}').ok, false)
  assert.equal(parsePayload("{'a': 1}").ok, false)
  assert.equal(parsePayload('{"a": 1').ok, false)
})

test('payload: over the cap is refused as the DASHBOARD’s limit, not the broker’s', () => {
  // A 413 from the proxy names a plan limit; this one is ours, and saying so
  // is what stops it being read as "the cluster refuses payloads this size".
  const big = `"${'a'.repeat(MAX_PAYLOAD_TEXT_BYTES)}"`
  const parsed = parsePayload(big)
  assert.equal(parsed.ok, false)
  assert.match(parsed.message, /the form stops at/)
  assert.match(parsed.message, /not the broker/)

  // Just under it still parses, so the cap is a ceiling and not a mood.
  const ok = `"${'a'.repeat(MAX_PAYLOAD_TEXT_BYTES - 3)}"`
  assert.equal(parsePayload(ok).ok, true)
})

test('payload: the cap counts BYTES, so multi-byte text cannot slip past it', () => {
  // '𝄞' is 4 UTF-8 bytes and 2 JS characters: a length-based cap would accept
  // roughly twice the payload it promised to stop.
  const text = `"${'𝄞'.repeat(MAX_PAYLOAD_TEXT_BYTES / 4 + 1)}"`
  assert.ok(text.length < MAX_PAYLOAD_TEXT_BYTES, 'fewer characters than the cap')
  assert.equal(parsePayload(text).ok, false)
})

// ---------------------------------------------------------------------------
// A copy of an existing message
// ---------------------------------------------------------------------------

test('copy: the textarea round-trips the stored payload VALUE', () => {
  // The value, not the bytes: httpClient already parsed the response, so a
  // number the double cannot hold was rewritten before this ever ran.
  assert.equal(payloadToText(JSON.parse('{"id":9007199254740993}')), '{\n  "id": 9007199254740992\n}')

  for (const value of [{ id: 1 }, [1, 2], 'a string', 42, true, null]) {
    const parsed = parsePayload(payloadToText(value))
    assert.equal(parsed.ok, true)
    assert.deepEqual(parsed.value, value)
  }
})

test('copy: a payload that IS a JSON string stays a string', () => {
  // The drawer's display formatter parses a double-encoded payload so it reads
  // nicely. Doing that here would push an OBJECT where the original carried a
  // STRING — a copy that is not a copy.
  const stored = '{"a":1}'
  const parsed = parsePayload(payloadToText(stored))
  assert.equal(parsed.ok, true)
  assert.equal(parsed.value, stored)
  assert.equal(typeof parsed.value, 'string')
})

test('copy: no payload at all leaves the field empty, and null fills it with null', () => {
  assert.equal(payloadToText(undefined), '')
  assert.equal(payloadToText(null), 'null')
})

// ---------------------------------------------------------------------------
// The refusals that never reach the broker
// ---------------------------------------------------------------------------

test('refusal: a plan block is not a role problem', () => {
  // "Not permitted for your role" is the wrong advice for every 403 that is
  // about the CLUSTER: no role change helps a storage-blocked, quota-exhausted
  // or suspended one. `describeApiError` now words those itself; these three
  // keep their own sentence because the push path knows the rest of the story
  // — that nothing was written, and what has to fall back under a cap before
  // the next attempt can land.
  const storage = new ApiError('storage quota exceeded; pushes blocked', {
    status: 403, code: 'storage_quota_exceeded',
  })
  assert.match(describePushRefusal(storage), /Storage quota/)
  assert.doesNotMatch(describePushRefusal(storage), /role/)

  const hold = new ApiError('pushes blocked (billing hold)', {
    status: 403, code: 'push_blocked',
  })
  assert.match(describePushRefusal(hold), /billing hold/)

  const suspended = new ApiError('cluster suspended', {
    status: 403, code: 'cluster_suspended',
  })
  assert.match(describePushRefusal(suspended), /suspended/)
  assert.doesNotMatch(describePushRefusal(suspended), /role/)

  const gated = new ApiError('feature not in plan', {
    status: 403, code: 'feature_gated',
  })
  assert.match(describePushRefusal(gated), /plan/)
  assert.doesNotMatch(describePushRefusal(gated), /role/)
})

test('refusal: `quota_exceeded` is OVERLOADED, so the proxy keeps the wording', () => {
  // One code, four emitters (proxy/src/errors.rs + gateway.rs). On the push
  // path `enforce_produce` -> `admit_pairs` returns it for the REGISTRY caps as
  // well as for the monthly quota, and a form that invites pushing to a name
  // the cluster does not carry yet trips the queue cap first. Asserting one
  // meaning here would tell an operator at max_queues to come back next month.
  //
  // The sentence now arrives through `describeApiError`, which prefixes the
  // proxy's own words with the subject they are missing ("The plan on this
  // cluster refused it: …"): a bare "queue limit reached (20)" was fine in the
  // modal, which has a heading, and said nothing about WHO refused when the
  // same error reached the global toast. What must not change is that the cap
  // the proxy named survives verbatim.
  const queues = new ApiError('queue limit reached (20)', {
    status: 403, code: 'quota_exceeded',
  })
  assert.equal(describePushRefusal(queues), 'The plan on this cluster refused it: queue limit reached (20)')

  const partitions = new ApiError('partition limit reached (50)', {
    status: 403, code: 'quota_exceeded',
  })
  assert.match(describePushRefusal(partitions), /partition limit reached \(50\)$/)

  const monthly = new ApiError(
    'monthly message quota (monthly_msgs_quota) exhausted; pushes blocked until the next calendar month',
    { status: 403, code: 'quota_exceeded' },
  )
  assert.match(describePushRefusal(monthly), /monthly message quota/)
  assert.match(describePushRefusal(monthly), /next calendar month/)

  // A proxy that sends the code with no sentence still must not read as a role
  // problem, and must not name a cap it did not name.
  const bare = new ApiError('', { status: 403, code: 'quota_exceeded' })
  assert.equal(describePushRefusal(bare), 'The plan on this cluster refused it')
  assert.doesNotMatch(describePushRefusal(bare), /role/)
  assert.doesNotMatch(describePushRefusal(bare), /month/)

  // Same for the envelope whose `error` field carries the CODE rather than a
  // sentence (the shape the broker's own handlers use): repeating it would put
  // "quota_exceeded" on screen as if it were English.
  const coded = new ApiError('quota_exceeded', { status: 403, code: 'quota_exceeded' })
  assert.equal(describePushRefusal(coded), 'The plan on this cluster refused it')
})

test('refusal: a real 403 keeps the shell’s wording', () => {
  const forbidden = new ApiError('forbidden', { status: 403, code: 'forbidden' })
  assert.equal(describePushRefusal(forbidden), 'Not permitted for your role on this cluster')
})

test('refusal: 429 keeps the Retry-After the shell already renders', () => {
  const limited = new ApiError('message rate limit exceeded', {
    status: 429, code: 'rate_limited', retryAfter: 12,
  })
  assert.match(describePushRefusal(limited), /retry in 12s/)
})

test('refusal: a 413 repeats the cap the proxy named', () => {
  // The proxy says which cap and by how much; nothing this function could
  // invent about a limit it cannot read would be more useful.
  const tooBig = new ApiError('item 0: payload 900000 bytes exceeds max_payload_bytes (262144)', {
    status: 413, code: 'payload_too_large',
  })
  assert.match(describePushRefusal(tooBig), /max_payload_bytes \(262144\)/)
})

// ---------------------------------------------------------------------------
// A payload the broker could not decrypt
// ---------------------------------------------------------------------------

test('copy: an undecryptable envelope is recognised, and a plain payload is not', () => {
  // With no key (or the wrong one) `decrypt_payload_bytes` returns None and the
  // detail route answers the STORED bytes — the {encrypted,iv,authTag} object
  // itself (server/src/handlers/messages.rs). Copying that would push the
  // envelope as a plaintext payload; on an encrypted queue the broker then
  // wraps it again, and the message decrypts to an envelope.
  assert.equal(isEncryptedEnvelope({
    encrypted: 'k0Yb3Q==', iv: 'YWJjZGVmZ2hpamts', authTag: 'zzzz',
  }), true)

  // A payload of the operator's own that merely carries one of those keys is a
  // payload, and refusing to copy it would be the opposite mistake.
  assert.equal(isEncryptedEnvelope({ encrypted: true, data: 1 }), false)
  assert.equal(isEncryptedEnvelope({ encrypted: 'x', iv: 'y' }), false)
  assert.equal(isEncryptedEnvelope({ encrypted: 'x', iv: 'y', authTag: 'z', queue: 'q' }), false)
  assert.equal(isEncryptedEnvelope({ encrypted: 'x', iv: 'y', authTag: 42 }), false)
  for (const value of [null, undefined, 'encrypted', 42, [1, 2, 3], []]) {
    assert.equal(isEncryptedEnvelope(value), false, `${JSON.stringify(value)} is not an envelope`)
  }
})

// ---------------------------------------------------------------------------
// The 500 that carries results
// ---------------------------------------------------------------------------

test('transport: a 500 that carries the results array still reports the item status', () => {
  // `buffer_all` answers 500 with the SAME per-item array when a spool write
  // failed, and that is one of the two ways `failed` — "the message is lost" —
  // ever reaches the screen. It survives three layers: ky pre-parsing
  // `error.data`, httpClient passing a non-string through `errorData`, and
  // ApiError keeping it as `body`. A change to any one of them would silently
  // downgrade the sentence to the generic "Server error (HTTP 500)" toast.
  const lost = [{
    index: 0,
    message_id: '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
    transaction_id: 'order-8891',
    queueName: 'orders.created',
    status: 'failed',
  }]
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async () => new Response(JSON.stringify(lost), {
      status: 500, headers: { 'content-type': 'application/json' },
    }),
  })

  return client.post('/api/v1/push', { items: [{ queue: 'orders.created', payload: { id: 1 } }] })
    .then(
      () => assert.fail('a 500 must reject'),
      (err) => {
        assert.ok(Array.isArray(err.body), 'the results array survives as ApiError.body')
        const verdict = pushVerdict(err.body)
        assert.equal(verdict.kind, 'error')
        assert.match(verdict.title, /message is lost/)
        // And the modal reads the array rather than describing the status code.
        assert.notEqual(describePushRefusal(err), verdict.title)
      },
    )
})

// ---------------------------------------------------------------------------
// Where the message is written
// ---------------------------------------------------------------------------

// The queue field is the one place in this form that decides WHERE the message
// goes, and it is a shared widget whose default is to discard uncommitted text
// (right for the eleven filter rows, wrong here: the typed name is the write
// target). The behaviour itself needs a DOM, which this suite does not have —
// so pin the contract between the two files the way theme.test.js pins the
// pre-paint script: the opt-in exists, it is off by default, and the push form
// asks for it.
test('the push form commits the queue name it was typed, not the one before it', () => {
  const widget = readFileSync(
    new URL('../src/components/Autocomplete.vue', import.meta.url), 'utf8',
  )
  // Leaving the field runs the opt-in-aware handler, never `cancel` directly.
  assert.match(widget, /@blur="onLeave"/)
  assert.match(widget, /@keydown\.tab="onLeave"/)
  // Off by default, so the filter rows keep discarding half-typed text.
  assert.match(widget, /commitOnBlur:\s*\{\s*type:\s*Boolean,\s*default:\s*false/)
  // And what it applies is the TYPED text, not the highlighted row: on a blur
  // nobody chose that row.
  assert.match(widget, /const commitTyped\s*=/)
  assert.match(widget, /commitOnBlur\s*&&\s*props\.allowCustom\)\s*\{\s*commitTyped\(\)/)

  const modal = readFileSync(
    new URL('../src/components/PushMessageModal.vue', import.meta.url), 'utf8',
  )
  assert.match(modal, /commit-on-blur/)
  // And the filter vocabulary stays out of a form whose only verb is "push".
  assert.match(modal, /custom-hint="press Enter to push to this queue name"/)
  assert.match(modal, /:reset-label="null"/)
  assert.doesNotMatch(modal, /filter by this name/)
})

// ---------------------------------------------------------------------------
// The list the operator lands on afterwards
// ---------------------------------------------------------------------------

const NOW = '2026-09-11T15:04'

test('after a push, the list window is moved to now', () => {
  // `filterTo` is set ONCE, at page load, and the list SP scans up to
  // `date_trunc('minute', to) + 1 minute` — so a page open for two minutes
  // refreshes into a window that ends before the pushed message exists.
  const next = filtersForPushedMessage(
    { to: '2026-09-11T14:31', status: '', queue: '', partition: '' },
    { queue: 'orders.created', partition: 'Default' },
    NOW,
  )
  assert.equal(next.to, NOW)

  // An open-ended window already covers it and is left alone.
  const open = filtersForPushedMessage(
    { to: '', status: '', queue: '', partition: '' },
    { queue: 'orders.created', partition: 'Default' },
    NOW,
  )
  assert.equal(open.to, '')
})

test('after a push, a filter that cannot match a fresh message is dropped', () => {
  // A just-pushed message is `pending`: under Completed or Dead Letter the
  // refreshed table is empty however right the push went.
  for (const status of ['completed', 'dead_letter', 'processing']) {
    const next = filtersForPushedMessage(
      { to: NOW, status, queue: '', partition: '' },
      { queue: 'orders.created', partition: 'Default' },
      NOW,
    )
    assert.equal(next.status, '', `${status} must not survive a push`)
  }
  // Pending matches it, and no filter at all matches everything: both stay.
  for (const status of ['pending', '']) {
    const next = filtersForPushedMessage(
      { to: NOW, status, queue: '', partition: '' },
      { queue: 'orders.created', partition: 'Default' },
      NOW,
    )
    assert.equal(next.status, status)
  }
})

test('after a push, the queue and partition filters follow the message', () => {
  const next = filtersForPushedMessage(
    { to: NOW, status: '', queue: 'orders.created', partition: 'shard-1' },
    { queue: 'orders.shipped', partition: 'Default' },
    NOW,
  )
  assert.equal(next.queue, 'orders.shipped')
  assert.equal(next.partition, 'Default')

  // A list showing every queue already shows the new row: narrowing it to the
  // pushed queue would hide rows the operator asked to see.
  const wide = filtersForPushedMessage(
    { to: NOW, status: '', queue: '', partition: '' },
    { queue: 'orders.shipped', partition: 'Default' },
    NOW,
  )
  assert.equal(wide.queue, '')
  assert.equal(wide.partition, '')

  // And a filter that already names the pushed queue is not touched.
  const same = filtersForPushedMessage(
    { to: NOW, status: 'pending', queue: 'orders.shipped', partition: 'Default' },
    { queue: 'orders.shipped', partition: 'Default' },
    NOW,
  )
  assert.deepEqual(same, { to: NOW, status: 'pending', queue: 'orders.shipped', partition: 'Default' })
})
