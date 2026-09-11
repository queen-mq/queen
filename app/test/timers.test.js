import { test } from 'node:test'
import assert from 'node:assert/strict'
import zlib from 'node:zlib'

import { decompress } from 'fzstd'

import { createApiClient } from '../src/api/httpClient.js'
import { timerAddr, timerQueueAddr } from '../src/api/timerPath.js'
import {
  TIMER_PAYLOAD_BUDGET_BYTES,
  decodeTimerPayload,
  describeCancel,
  formatDeliverIn,
  moveUnwinds,
  normalizeBrokerStamp,
  parseBrokerInstant,
  rowsForQueue,
  timerListParams,
  timerUsageIsMeasured,
  zstdDeclaredSize,
  zstdWindowSize,
} from '../src/composables/useTimers.js'

// A real zstd frame, produced once with Node 24's
// `zlib.zstdCompressSync(Buffer.from(PAYLOAD_TEXT))` and checked in as base64 —
// the same bytes a client that compresses before scheduling puts in
// `payload` with `payloadZstd:true`. Checked in rather than generated at test
// time so this asserts fzstd against a FIXED frame: a test that compresses and
// decompresses with two halves of the same run can pass while the decoder in
// the bundle cannot read what real clients send.
const PAYLOAD_TEXT =
  '{"order":"A-1177","retryOf":"wh.deliver:promotion-publication","attempt":3,"note":"zstd frame produced by node:zlib zstdCompressSync"}'
const PAYLOAD_ZSTD_B64 =
  'KLUv/SCGnQMA4gcaG3Bp28AmDNvLLRQ46Hc737UqHpyOx2Ff9v/fEosv1xSWRfm1HRzFlARqrjuNOqmLyeV3tCVOTvaSOF573VEA369pdzp6WkxRzDPcst62/JD1wBRNgAFxfEJouJWPjiDHYQiOZ03eKwICAEdUwHxQoA=='
const PAYLOAD_PLAIN_B64 = Buffer.from(PAYLOAD_TEXT, 'utf8').toString('base64')

// ---------------------------------------------------------------------------
// The address of a timer
// ---------------------------------------------------------------------------

test('a timer key is percent-encoded into the path, `/` included', () => {
  // The wildcard segment (`*timerKey`) would swallow an unencoded `/`, so that
  // one keeps working by accident and hides the bug until a key carries `?`,
  // `#` or a space. client-js pins the same shape
  // (clients/client-js/test-v2/kv-unit/timerWire.test.js).
  assert.equal(timerAddr('reminders', 'order/9f1'), '/api/v1/timers/reminders/order%2F9f1')
  assert.equal(timerAddr('re minders', 'a b'), '/api/v1/timers/re%20minders/a%20b')
  assert.equal(timerAddr('q', 'why?#frag'), '/api/v1/timers/q/why%3F%23frag')
  assert.equal(timerAddr('cödä', 'ké'), '/api/v1/timers/c%C3%B6d%C3%A4/k%C3%A9')
  // The two colons of a real key are legal in a path segment and survive.
  assert.equal(
    timerAddr('orders', 'wh.deliver:promotion-publication'),
    '/api/v1/timers/orders/wh.deliver%3Apromotion-publication',
  )
  assert.equal(timerQueueAddr('a/b'), '/api/v1/timers/a%2Fb')

  // A key taken verbatim off a list row round-trips to the peek and the cancel:
  // whatever the SP returned is what goes back on the wire.
  const row = { queue: 'orders', timerKey: 'retry:A-1177/2' }
  assert.equal(timerAddr(row.queue, row.timerKey), '/api/v1/timers/orders/retry%3AA-1177%2F2')
  assert.equal(decodeURIComponent(timerAddr(row.queue, row.timerKey).split('/').pop()), row.timerKey)
})

// ---------------------------------------------------------------------------
// Instants
// ---------------------------------------------------------------------------

test('the microsecond fraction is truncated by US, not by the engine', () => {
  // V8 truncates the extra digits itself, so asserting the parsed Date measures
  // the RUNTIME: this test is what fails if the normalisation is deleted.
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00.123456Z'), '2026-09-11T10:00:00.123Z')
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00.123456789Z'), '2026-09-11T10:00:00.123Z')
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00.123456+02:00'), '2026-09-11T10:00:00.123+02:00')
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00.123456+0200'), '2026-09-11T10:00:00.123+0200')
  // Three digits or fewer are already legal and are left alone.
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00.123Z'), '2026-09-11T10:00:00.123Z')
  assert.equal(normalizeBrokerStamp('2026-09-11T10:00:00Z'), '2026-09-11T10:00:00Z')
  assert.equal(normalizeBrokerStamp(1789120800123), 1789120800123)
})

test('the broker stamp parses, microseconds and all', () => {
  // to_char(… 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') — six fractional digits, three
  // more than the ECMAScript date-time format specifies.
  const d = parseBrokerInstant('2026-09-11T10:00:00.123456Z')
  assert.equal(d.toISOString(), '2026-09-11T10:00:00.123Z')
  assert.equal(parseBrokerInstant('2026-09-11T10:00:00Z').toISOString(), '2026-09-11T10:00:00.000Z')
  assert.equal(parseBrokerInstant(1789120800123).getTime(), 1789120800123)
  assert.equal(parseBrokerInstant(new Date(0)).getTime(), 0)

  for (const empty of [null, undefined, '', 'not a date']) {
    assert.equal(parseBrokerInstant(empty), null)
  }
})

test('deliverAt is relative in both directions — and the past is "overdue"', () => {
  const now = Date.parse('2026-09-11T10:00:00.000Z')
  const at = (ms) => new Date(now + ms).toISOString().replace('Z', '000Z')

  assert.equal(formatDeliverIn(at(4_000), now), 'in 4s')
  assert.equal(formatDeliverIn(at(180_000), now), 'in 3m')
  assert.equal(formatDeliverIn(at(2 * 3_600_000), now), 'in 2h')
  assert.equal(formatDeliverIn(at(3 * 86_400_000), now), 'in 3d')

  // Not "3m ago": nothing has happened yet. The sweeper has not reached it.
  assert.equal(formatDeliverIn(at(-180_000), now), '3m overdue')
  assert.equal(formatDeliverIn(at(-12_000), now), '12s overdue')

  assert.equal(formatDeliverIn(at(500), now), 'due now')
  assert.equal(formatDeliverIn(at(-500), now), 'due now')
  assert.equal(formatDeliverIn(null, now), '—')
})

// ---------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------

test('fzstd reads the checked-in frame, byte for byte', () => {
  const stored = Buffer.from(PAYLOAD_ZSTD_B64, 'base64')
  const out = decompress(new Uint8Array(stored))
  assert.equal(Buffer.from(out).toString('utf8'), PAYLOAD_TEXT)
})

test('an uncompressed payload decodes to its text', () => {
  const r = decodeTimerPayload({ payload: PAYLOAD_PLAIN_B64, payloadZstd: false, encrypted: false })
  assert.equal(r.state, 'text')
  assert.equal(r.text, PAYLOAD_TEXT)
  assert.equal(r.bytes, Buffer.byteLength(PAYLOAD_TEXT))
  assert.equal(r.storedBytes, r.bytes)
})

test('payloadZstd is a FLAG on `payload`, and the drawer decompresses it', () => {
  const r = decodeTimerPayload({ payload: PAYLOAD_ZSTD_B64, payloadZstd: true, encrypted: false })
  assert.equal(r.state, 'text')
  assert.equal(r.text, PAYLOAD_TEXT)
  assert.equal(r.bytes, Buffer.byteLength(PAYLOAD_TEXT))
  // Stored is the compressed size: the two together are the only way the
  // console can say whether the client's compression bought anything.
  assert.equal(r.storedBytes, Buffer.from(PAYLOAD_ZSTD_B64, 'base64').length)
  assert.ok(r.storedBytes < r.bytes)
})

test('an encrypted payload is an envelope and is never decoded', () => {
  // Encryption is OUTERMOST (handlers/timers.rs prepare_schedule), so even a
  // payload that would decode is left alone: peek promises the bytes as
  // stored, and the key is the broker's.
  const r = decodeTimerPayload({ payload: PAYLOAD_PLAIN_B64, payloadZstd: false, encrypted: true })
  assert.equal(r.state, 'encrypted')
  assert.equal(r.text, '')
})

test('empty, absent, and undecodable payloads each say which they are', () => {
  assert.equal(decodeTimerPayload({ payload: '' }).state, 'empty')
  // `{found:false}` carries no payload at all, and neither does a list row.
  assert.equal(decodeTimerPayload({ found: false, queue: 'q', timerKey: 'k' }).state, 'absent')
  assert.equal(decodeTimerPayload(null).state, 'absent')

  const notZstd = decodeTimerPayload({ payload: PAYLOAD_PLAIN_B64, payloadZstd: true })
  assert.equal(notZstd.state, 'error')
  assert.match(notZstd.error, /zstd/)

  // Bytes that are not UTF-8 are binary, not a string of replacement marks.
  const binary = decodeTimerPayload({ payload: Buffer.from([0xff, 0xfe, 0x00, 0x01]).toString('base64') })
  assert.equal(binary.state, 'binary')
  assert.equal(binary.bytes, 4)
})

// ---------------------------------------------------------------------------
// The decode budget — one hostile payload must not take the tab with it
// ---------------------------------------------------------------------------

test('the zstd frame header is read before anything is decompressed', () => {
  const declared = (n) => zstdDeclaredSize(new Uint8Array(zlib.zstdCompressSync(Buffer.alloc(n, 0x41))))
  assert.equal(declared(0), 0)
  assert.equal(declared(1), 1)
  // The 2-byte form stores the size MINUS 256, which is where an off-by-256
  // would hide.
  assert.equal(declared(255), 255)
  assert.equal(declared(256), 256)
  assert.equal(declared(70_000), 70_000)
  assert.equal(declared(64 * 1024 * 1024), 64 * 1024 * 1024)

  // Not a zstd frame, and a frame that declares nothing (every streaming
  // compressor): both are "unknown", and the running total bounds those.
  assert.equal(zstdDeclaredSize(new Uint8Array([1, 2, 3, 4, 5])), null)
  assert.equal(zstdDeclaredSize(new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58])), null)
  assert.equal(zstdDeclaredSize(new Uint8Array([])), null)
})

test('a payload that expands past the budget is refused, not decompressed', () => {
  // 2 067 stored bytes for 64 MB out, measured with this decoder: the stored
  // bytes are capped by the broker at 1 MiB and the expansion is not, so a
  // producer chooses how much memory the operator's tab is asked for.
  const bomb = zlib.zstdCompressSync(Buffer.alloc(64 * 1024 * 1024, 0))
  assert.ok(bomb.length < 4096, 'the fixture is the amplification being tested')

  const started = Date.now()
  const r = decodeTimerPayload({ payload: bomb.toString('base64'), payloadZstd: true })
  assert.equal(r.state, 'too_large')
  assert.equal(r.bytes, 64 * 1024 * 1024)          // what the frame CLAIMS
  assert.equal(r.storedBytes, bomb.length)
  assert.equal(r.error, null)
  assert.ok(Date.now() - started < 1000, 'refused on the header, not after decoding')

  // A frame just under the budget still decodes: the guard is a ceiling, not a
  // ban on large payloads.
  const ok = zlib.zstdCompressSync(Buffer.alloc(TIMER_PAYLOAD_BUDGET_BYTES - 1024, 0x41))
  const fine = decodeTimerPayload({ payload: ok.toString('base64'), payloadZstd: true })
  assert.equal(fine.state, 'text')
  assert.equal(fine.bytes, TIMER_PAYLOAD_BUDGET_BYTES - 1024)
})

test('a frame that declares no size is bounded by the running total', async () => {
  // What a streaming compressor produces: no Frame_Content_Size in the header,
  // so the header check cannot help and the decode itself has to stop.
  const stream = zlib.createZstdCompress()
  const chunks = []
  stream.on('data', (c) => chunks.push(c))
  const done = new Promise((resolve) => stream.on('end', resolve))
  stream.end(Buffer.alloc(64 * 1024 * 1024, 0))
  stream.resume()
  await done
  const frame = Buffer.concat(chunks)
  assert.equal(zstdDeclaredSize(new Uint8Array(frame)), null)

  const r = decodeTimerPayload({ payload: frame.toString('base64'), payloadZstd: true })
  assert.equal(r.state, 'too_large')
  assert.equal(r.bytes, null)                      // nothing was ever claimed
  assert.equal(r.storedBytes, frame.length)
  // NOT an error: the frame is fine, this console just will not hold it.
  assert.equal(r.error, null)
})

/**
 * A frame with a chosen Window_Descriptor and `blocks` RLE blocks of 128 KiB.
 *
 * Hand-built because no compressor will produce it: the point is a frame whose
 * OUTPUT is small and whose declared WINDOW is enormous, which is exactly the
 * shape a hostile producer writes by hand and the shape both of the other two
 * guards are blind to.
 */
function windowFrame(exponent, mantissa, blocks = 0) {
  const out = [0x28, 0xb5, 0x2f, 0xfd, 0x00, (exponent << 3) | mantissa]
  const BLK = 128 * 1024
  for (let i = 0; i < blocks; i += 1) {
    // Block_Header: last(1) · type RLE=1 (2) · size(21), little-endian.
    const h = (i === blocks - 1 ? 1 : 0) | (1 << 1) | (BLK << 3)
    out.push(h & 0xff, (h >> 8) & 0xff, (h >> 16) & 0xff, 0x41)
  }
  return Buffer.from(out)
}

test('the window a frame asks the decoder to HOLD is read too', () => {
  // windowLog = 10 + exponent, plus mantissa eighths of it.
  assert.equal(zstdWindowSize(windowFrame(0, 0)), 1024)
  assert.equal(zstdWindowSize(windowFrame(11, 0)), 2 * 1024 * 1024)
  assert.equal(zstdWindowSize(windowFrame(13, 4)), 8 * 1024 * 1024 + 4 * 1024 * 1024)
  // The maximum the field can express: `1 << (10 + 31)` would count mod 32 and
  // answer 512 bytes, so this is the assertion that pins `2 **`.
  assert.equal(zstdWindowSize(windowFrame(31, 7)), 2 ** 41 + (2 ** 41 / 8) * 7)
  // fzstd's own ceiling is ~2 GiB, so the largest window it will ACCEPT is the
  // one that actually hurts.
  assert.equal(zstdWindowSize(windowFrame(20, 7)), 2_013_265_920)

  // Single_Segment carries no Window_Descriptor: the window is the content size.
  const single = new Uint8Array(zlib.zstdCompressSync(Buffer.alloc(70_000, 0x41)))
  assert.equal((single[4] >> 5) & 1, 1, 'the fixture is a single-segment frame')
  assert.equal(zstdWindowSize(single), 70_000)

  assert.equal(zstdWindowSize(new Uint8Array([1, 2, 3, 4, 5, 6])), null)
  assert.equal(zstdWindowSize(new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00])), null)
  assert.equal(zstdWindowSize(null), null)

  // And what real clients send stays well under the budget: node's default
  // level declares 2 MB, level 19 declares exactly 8 MB (admitted, the check is
  // a ceiling). Only `--ultra -22` territory is refused.
  assert.ok(zstdWindowSize(new Uint8Array(zlib.zstdCompressSync(Buffer.alloc(64 * 1024 * 1024, 0)))) <= TIMER_PAYLOAD_BUDGET_BYTES)
})

test('a frame with a huge window is refused even when its output is tiny', () => {
  // 134 stored bytes, 4 MB of output — under the declared-size check (nothing
  // is declared) and under the running total (4 MB < 8 MB) — but fzstd
  // allocates the WINDOW and copyWithin()s all of it once per block: measured
  // at 3.3 s and 1.9 GB on the main thread before this guard existed.
  const bomb = windowFrame(20, 7, 32)
  assert.ok(bomb.length < 200, 'the fixture is the amplification being tested')
  assert.equal(zstdDeclaredSize(new Uint8Array(bomb)), null)

  const started = Date.now()
  const r = decodeTimerPayload({ payload: bomb.toString('base64'), payloadZstd: true })
  assert.equal(r.state, 'too_large')
  assert.equal(r.bytes, null)            // the frame claimed no size, only a window
  assert.equal(r.storedBytes, bomb.length)
  assert.equal(r.error, null)
  assert.ok(Date.now() - started < 500, 'refused on the header, not after allocating 1.9 GB')

  // The six-byte header alone — no blocks at all — used to take the 1.9 GB and
  // then report itself as an undecodable payload.
  const header = decodeTimerPayload({ payload: windowFrame(20, 7).toString('base64'), payloadZstd: true })
  assert.equal(header.state, 'too_large')

  // A window inside the budget is still decoded: the guard is a ceiling.
  const fine = decodeTimerPayload({ payload: windowFrame(11, 0, 1).toString('base64'), payloadZstd: true })
  assert.equal(fine.state, 'text')
  assert.equal(fine.bytes, 128 * 1024)
})

// ---------------------------------------------------------------------------
// What reaches the wire, and what reaches the screen
// ---------------------------------------------------------------------------

test('the first page omits `after` — a null cursor would travel as the string "null"', () => {
  const first = timerListParams(null, 100)
  assert.ok(!('after' in first), 'ky serialises {after:null} as after=null, which the SP takes as a cursor')
  assert.deepEqual(first, { limit: 100 })
  assert.deepEqual(timerListParams(undefined, 50), { limit: 50 })

  // A real cursor travels byte for byte — it is a key, and COLLATE "C" makes
  // every byte of it load-bearing.
  const key = 'wh.deliver:promotion-publication:b15f6d46/#? é'
  assert.deepEqual(timerListParams(key, 250), { after: key, limit: 250 })
  // The empty string is a legal timer key and therefore a legal cursor.
  assert.deepEqual(timerListParams('', 100), { after: '', limit: 100 })
})

test('rows belong to the queue they were fetched for, or there are none', () => {
  const data = { rows: [{ timerKey: 'a' }], truncated: false, nextAfter: null }
  assert.deepEqual(rowsForQueue('orders', 'orders', data), data.rows)
  // The pick has moved on and the answer has not landed: the previous queue's
  // timers under the new queue's name is the lie this prevents.
  assert.deepEqual(rowsForQueue('orders', 'payments', data), [])
  assert.deepEqual(rowsForQueue(null, 'orders', data), [])
  assert.deepEqual(rowsForQueue('orders', 'orders', null), [])
  assert.deepEqual(rowsForQueue('orders', 'orders', {}), [])
  assert.deepEqual(rowsForQueue('orders', 'orders', { rows: 'nope' }), [])
})

test('a move only walks the cursor stack back while it still owns the walk', () => {
  // The marks the view takes off useKeysetPager: two REPLACED-not-mutated refs,
  // so identity is what tells "untouched" from "someone else moved it".
  const cursors = ['a']
  const tail = { truncated: true, nextAfter: 'b' }
  const moved = { cursors, tail }
  const untouched = { cursors, tail }

  // The page landed: there is nothing to undo, whatever else happened.
  assert.equal(moveUnwinds({ landed: true, seqAtMove: 3, seqNow: 3, movedMark: moved, nowMark: untouched }), false)

  // The plain failure this undo exists for — the move's own load (one token)
  // came back empty-handed and nothing else touched the stack.
  assert.equal(moveUnwinds({ landed: false, seqAtMove: 3, seqNow: 4, movedMark: moved, nowMark: untouched }), true)

  // A reload that refused to send at all (a stable gated verdict returns before
  // taking a token): nothing is in flight, so the undo is still safe.
  assert.equal(moveUnwinds({ landed: false, seqAtMove: 3, seqNow: 3, movedMark: moved, nowMark: untouched }), true)

  // THE RACE. Refresh pressed during a Next starts a second load, which
  // supersedes the move's own and is still fetching the MOVED cursor: it has
  // written nothing, so both marks are identical and identity alone would
  // happily walk the stack back underneath it. The token is what sees it.
  assert.equal(moveUnwinds({ landed: false, seqAtMove: 3, seqNow: 5, movedMark: moved, nowMark: untouched }), false)
  assert.equal(moveUnwinds({ landed: false, seqAtMove: 0, seqNow: 9, movedMark: moved, nowMark: untouched }), false)

  // A load that already LANDED, or a reset: the refs were replaced, and the
  // marks catch it even when the token count looks like ours alone.
  assert.equal(
    moveUnwinds({
      landed: false, seqAtMove: 3, seqNow: 4,
      movedMark: moved,
      nowMark: { cursors: ['a'], tail },
    }),
    false,
    'an equal-looking cursor array is a different walk',
  )
  assert.equal(
    moveUnwinds({
      landed: false, seqAtMove: 3, seqNow: 4,
      movedMark: moved,
      nowMark: { cursors, tail: { truncated: true, nextAfter: 'b' } },
    }),
    false,
    'a tail recorded by another load is another load’s page',
  )

  // Nothing to compare against, or a counter that is not a counter: leave the
  // walk alone. "Page 3" over page 2 costs one Previous; an unwind under a live
  // load mislabels its rows with no way to tell.
  assert.equal(moveUnwinds({ landed: false, seqAtMove: 3, seqNow: 4, movedMark: moved, nowMark: null }), false)
  assert.equal(moveUnwinds({ landed: false, seqAtMove: undefined, seqNow: 4, movedMark: moved, nowMark: untouched }), false)
  assert.equal(moveUnwinds(), false)
})

test('a tenant total the page contradicts is not a measurement', () => {
  const rows = [{ timerKey: 'a' }]
  // handlers/queues.rs sends 0 for a tenant the sweeper has not reached yet
  // (kv_usage_snapshot -> unwrap_or), so a zero over a full page is certainly
  // wrong and the chip says nothing at all.
  assert.equal(timerUsageIsMeasured(0, rows), false)
  // A genuinely empty tenant: the two figures agree, so `≈ 0` is honest.
  assert.equal(timerUsageIsMeasured(0, []), true)
  assert.equal(timerUsageIsMeasured(4321, rows), true)
  assert.equal(timerUsageIsMeasured(4321, []), true)
  // Omitted entirely (the read failed): nothing to render.
  assert.equal(timerUsageIsMeasured(null, []), false)
  assert.equal(timerUsageIsMeasured(undefined, rows), false)
})

test('the cancel carries the txn echo, on a DELETE, through the real transport', async () => {
  // The cancel route reads exactly one query parameter and the SP hands it back
  // on `absent` "so the check needs no second API" (025_log_timers.sql). Two
  // assumptions hold that up and both are asserted here rather than assumed:
  // that a config `params` reaches the search string on a DELETE, and that the
  // already-encoded path is not re-encoded on the way out.
  let seen = null
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async (request) => {
      seen = request.url
      return new Response(
        JSON.stringify({ ok: false, status: 'absent', queue: 'orders', timerKey: 'retry:A-1177', txn: 'txn-9' }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      )
    },
  })

  const res = await client.delete(timerAddr('orders', 'retry:A-1177'), { params: { txn: 'txn-9' } })
  assert.equal(seen, 'https://queen.test/api/v1/timers/orders/retry%3AA-1177?txn=txn-9')

  // And the verdict that comes back names it, which is the whole point of
  // sending it: without the echo this sentence degrades to "the timer's txn".
  const verdict = describeCancel(res.data, { queue: 'orders', timerKey: 'retry:A-1177' })
  assert.equal(verdict.status, 'absent')
  assert.match(verdict.sentence, /txn "txn-9"/)
})

// ---------------------------------------------------------------------------
// Cancel verdicts — rendered as the SP issued them
// ---------------------------------------------------------------------------

test('the cancel verdict is the stored procedure\'s, not the button\'s', () => {
  const ctx = { queue: 'orders', timerKey: 'retry:A-1177' }

  const cancelled = describeCancel(
    { ok: true, status: 'cancelled', queue: 'orders', timerKey: 'retry:A-1177', txn: 'txn-9' },
    ctx,
  )
  assert.equal(cancelled.status, 'cancelled')
  assert.equal(cancelled.ok, true)
  assert.equal(cancelled.tone, 'ok')
  assert.match(cancelled.sentence, /will not fire/)

  const tooLate = describeCancel({ ok: false, status: 'too_late', queue: 'orders', timerKey: 'retry:A-1177' }, ctx)
  assert.equal(tooLate.tone, 'warn')
  assert.match(tooLate.sentence, /NOT cancelled/)
  // too_late means "a claim is live right now", NOT "this delivery is
  // unstoppable": log_timers_fail_v1 pushes claimed_until out and NULLs the
  // token, so a failed attempt puts the row back where a cancel takes it
  // (025_log_timers.sql: "a cancel during a backoff SUCCEEDS"). A sentence
  // promising finality sends the operator away from the retry that works.
  assert.match(tooLate.sentence, /backoff/)
  assert.doesNotMatch(tooLate.sentence, /nothing can now stop/i)
  assert.doesNotMatch(tooLate.sentence, /cannot be stopped/i)

  // No tombstone: absent may mean already delivered, and the echoed txn is how
  // the caller checks without a second API call (§4.4).
  const absent = describeCancel(
    { ok: false, status: 'absent', queue: 'orders', timerKey: 'retry:A-1177', txn: 'txn-9' },
    ctx,
  )
  assert.equal(absent.tone, 'warn')
  assert.match(absent.sentence, /already been delivered/)
  assert.match(absent.sentence, /txn-9/)

  // A status this build has never seen is shown, not swallowed.
  const unknown = describeCancel({ ok: false, status: 'quiesced' }, ctx)
  assert.equal(unknown.status, 'quiesced')
  assert.match(unknown.sentence, /quiesced/)

  const nothing = describeCancel(null, ctx)
  assert.equal(nothing.status, null)
  assert.match(nothing.sentence, /still pending/)
})
