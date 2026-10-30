// The Dead letter tab's "Why they failed", pinned: the broker's signatures
// matched back to the page's rows for the full text and the table's filter.
// Stage, 2026-10-30: 8,625 rows of one error, shown as 25 copies cut off.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { errorExample, midCut, sigPrefix, whyRows, whyScope } from '../src/composables/dlqWhy.js'

const FULL = 'fetch: resolve booking review cursor scope: bookingcom: hotel_id not configured on connection (authDetails.hotelId / externalId missing)'
const SIG = FULL.slice(0, 120)
const rows = [
  { errorMessage: FULL },
  { errorMessage: 'timeout after 3000 ms calling 10.0.0.12' },
  { errorMessage: '' },
]

test('sigPrefix is the text before the first placeholder', () => {
  assert.equal(sigPrefix('timeout after <n> ms calling <n>.<n>.<n>.<n>'), 'timeout after')
  assert.equal(sigPrefix('order <id> not found'), 'order')
  assert.equal(sigPrefix(SIG), SIG)
  assert.equal(sigPrefix(null), '')
})

test('a signature finds its full error on the page', () => {
  assert.equal(errorExample(SIG, rows), FULL, 'the 120-character cut matches the row it came from')
  assert.equal(errorExample('timeout after <n> ms calling <n>.<n>.<n>.<n>', rows), 'timeout after 3000 ms calling 10.0.0.12', 'numbers folded out still match by the head')
  assert.equal(errorExample('code <n>', [{ errorMessage: 'code 7 at step 2' }]), null, 'a head under 12 characters must equal the whole error')
  assert.equal(errorExample('payment declined by issuer <n>', [{ errorMessage: 'payment declined by issuer 51' }]), 'payment declined by issuer 51')
  assert.equal(errorExample('(no message)', rows), '', 'the broker\'s word for an empty error matches an empty one')
  assert.equal(errorExample('nothing like it on this page', rows), null)
})

test('midCut keeps the head and the tail of a long id', () => {
  assert.equal(midCut('poll-28cade7a-0726-59e1-bc2a-ae36cf0f8e92', 28), 'poll-28cade7a-…-ae36cf0f8e92')
  assert.equal(midCut('short-id', 28), 'short-id')
  assert.equal(midCut('poll-28cade7a-0726-59e1-bc2a-ae36cf0f8e92', 28).length, 28)
})

test('with signatures, the rows are the broker\'s, with the page\'s full text where a row has it', () => {
  const why = { sample: 200, rowsNow: 8625, signatures: [{ text: SIG, n: 180, share: 0.9 }, { text: 'gone <id>', n: 20, share: 0.1 }] }
  const out = whyRows(why, [], rows)
  assert.deepEqual(out[0], { key: SIG, text: FULL, cut: false, n: 180, share: 0.9, filter: FULL })
  assert.deepEqual(out[1], { key: 'gone <id>', text: 'gone <id>', cut: false, n: 20, share: 0.1, filter: null })
})

test('without signatures, the rows are the page\'s own errors', () => {
  const groups = [{ error: 'a', count: 3, share: 0.75 }, { error: 'b', count: 1, share: 0.25 }]
  assert.deepEqual(whyRows(null, groups, rows).map((r) => [r.text, r.n, r.filter]), [['a', 3, 'a'], ['b', 1, 'b']])
})

test('the scope says what was counted', () => {
  const t = (iso) => iso.slice(11, 16)
  assert.equal(whyScope({ sample: 200, rowsNow: 8625, signatures: [{}], oldest: '2026-10-30T13:24:37Z', newest: '2026-10-30T13:57:47Z' }, 25, t),
    'the newest 200 of 8,625 in dead letter · 13:24 to 13:57')
  assert.equal(whyScope({ sample: 40, rowsNow: 40, signatures: [{}] }, 25, t), 'all 40 in dead letter')
  assert.equal(whyScope(null, 25, t), '25 messages on this page')
  assert.equal(whyScope(null, 1, t), '1 message on this page')
})
