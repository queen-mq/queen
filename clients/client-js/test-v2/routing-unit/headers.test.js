/**
 * Message headers (PLAN_ROUTING.md §6, the routing contract §1): a map of
 * strings on every message, set on push and read back by pop, fetch and the
 * feed.
 *
 * What is at stake: an envelope's `headers` reach the broker on every push
 * path (direct, buffered, transaction; JSON, and the framed binary body with
 * frame flag 8 and the broker's own block), a push without headers is the
 * request it always was, a bare payload that happens to have a `headers`
 * field stays a payload, and the messages the client hands back carry them.
 *
 * Same style as wire-unit/: a real node:http server records the bytes.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { zstdDecompressSync, zstdCompressSync } from 'node:zlib'

import { Queen } from '../../client-v2/index.js'
import { checkHeaders } from '../../client-v2/utils/validation.js'
import { headersBlock, encodePushFrames, encodePushBinary } from '../../client-v2/http/pushBinary.js'
import { decodePopFrames } from '../../client-v2/http/popFrames.js'
import { FeedReader, conditionHolds, whereCondition } from '../../client-v2/feed/Feed.js'
import { withWireServer } from '../wire-unit/_wireServer.js'

const queued = (n = 1) => ({ body: JSON.stringify(Array.from({ length: n }, () => ({ status: 'queued' }))) })
const pushes = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/push')
const transactions = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/transaction')
const jsonOf = (hit) => JSON.parse(hit.raw.toString())

async function withQueen(config, answer, run) {
  await withWireServer(answer, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1, ...config })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

/** The pairs of a headers block, in its order: u16 count | (u8 len, name | u16 len, value) x count. */
function blockPairs(block) {
  let at = 0
  const n = block.readUInt16LE(at); at += 2
  const pairs = []
  for (let i = 0; i < n; i++) {
    const kl = block[at]; at += 1
    const k = block.subarray(at, at + kl).toString(); at += kl
    const vl = block.readUInt16LE(at); at += 2
    pairs.push([k, block.subarray(at, at + vl).toString()]); at += vl
  }
  assert.equal(at, block.length, 'bytes after the last pair')
  return pairs
}

/** The frames of one framed group (u32 body_len | flags | id | txn | [headers, flag 8] | payload). */
function frames(z) {
  const raw = zstdDecompressSync(z)
  const out = []
  let p = 0
  while (p < raw.length) {
    const body = raw.subarray(p + 4, p + 4 + raw.readUInt32LE(p))
    p += 4 + body.length
    const flags = body[0]
    let at = 17
    const tl = body.readUInt16LE(at); at += 2
    const txn = body.subarray(at, at + tl).toString(); at += tl
    let headers = null
    if (flags & 8) {
      const hl = body.readUInt16LE(at); at += 2
      headers = blockPairs(body.subarray(at, at + hl)); at += hl
    }
    out.push({ flags, txn, headers, payload: body.subarray(at).toString() })
  }
  return out
}

/** The groups of a framed body after its magic and flags: u32 groups | per group: queue | partition | n | z. */
function framedGroups(bytes) {
  let at = 0
  const u16 = () => { at += 2; return bytes.readUInt16LE(at - 2) }
  const u32 = () => { at += 4; return bytes.readUInt32LE(at - 4) }
  const take = (n) => { at += n; return bytes.subarray(at - n, at) }
  const groups = []
  for (let g = u32(); g > 0; g--) {
    const queue = take(u16()).toString()
    const partition = take(u16()).toString()
    const count = u32()
    groups.push({ queue, partition, count, frames: frames(take(u32())) })
  }
  assert.equal(at, bytes.length, 'bytes after the last group')
  return groups
}

describe('headers — checkHeaders', () => {
  it('answers a copy of the headers, and null for none', () => {
    const given = { type: 'order.paid', 'trace_id-2': 'x' }
    const got = checkHeaders(given)
    assert.deepEqual(got, given)
    assert.notEqual(got, given, 'a copy: a later change to the object does not reach a buffered push')
    assert.equal(checkHeaders(undefined), null)
    assert.equal(checkHeaders(null), null)
    assert.equal(checkHeaders({}), null)
  })

  it('refuses what the broker refuses, naming the header', () => {
    assert.throws(() => checkHeaders({ Type: 'x' }), /header name "Type": 1 to 64 bytes of a-z, 0-9/)
    assert.throws(() => checkHeaders({ 'a b': 'x' }), /header name "a b"/)
    assert.throws(() => checkHeaders({ ['k'.repeat(65)]: 'x' }), /1 to 64 bytes/)
    assert.throws(() => checkHeaders({ 'queen-route': 'x' }), /names starting with "queen-" are the broker's/)
    assert.throws(() => checkHeaders({ n: 1 }), /header "n": the value must be a string, got 1/)
    assert.throws(() => checkHeaders(['a']), /headers must be an object of strings/)
    assert.throws(() => checkHeaders('a=b', 'link'), /^Error: link: headers must be an object/)
    assert.deepEqual(checkHeaders({ ['k'.repeat(64)]: '' }), { ['k'.repeat(64)]: '' })
  })
})

describe('headers — push (JSON)', () => {
  it('sends an envelope\'s headers on its item', async () => {
    await withQueen({}, () => queued(2), async (queen, hits) => {
      await queen.queue('orders').partition('order-1').push([
        { data: { a: 1 }, transactionId: 't1', headers: { type: 'order.paid', traceparent: '00-abc-01' } },
        { payload: { a: 2 }, transactionId: 't2', headers: { type: 'order.refunded' } }
      ])

      const { items } = jsonOf(pushes(hits)[0])
      assert.deepEqual(items[0], {
        queue: 'orders', partition: 'order-1', payload: { a: 1 }, transactionId: 't1',
        headers: { type: 'order.paid', traceparent: '00-abc-01' }
      })
      assert.deepEqual(items[1].headers, { type: 'order.refunded' })
    })
  })

  it('a push without headers is the request it always was; {} and null are none', async () => {
    await withQueen({}, () => queued(3), async (queen, hits) => {
      await queen.queue('orders').push([{ data: 1 }, { data: 2, headers: {} }, { data: 3, headers: null }])

      for (const item of jsonOf(pushes(hits)[0]).items) {
        assert.deepEqual(Object.keys(item), ['queue', 'partition', 'payload', 'transactionId'])
      }
    })
  })

  it('a bare object with a headers field is its own payload, as with partition', async () => {
    await withQueen({}, () => queued(), async (queen, hits) => {
      // An HTTP request log: its headers are data, and not valid header names either.
      const log = { url: '/x', headers: { 'Content-Type': 'text/plain' } }
      await queen.queue('requests').push(log)

      const [item] = jsonOf(pushes(hits)[0]).items
      assert.deepEqual(item.payload, log)
      assert.equal(item.headers, undefined)
    })
  })

  it('refuses bad headers before any request', async () => {
    await withQueen({}, () => queued(), async (queen, hits) => {
      assert.throws(() => queen.queue('orders').push({ data: 1, headers: { Type: 'x' } }), /header name "Type"/)
      assert.throws(() => queen.queue('orders').push({ data: 1, headers: { 'queen-x': 'y' } }), /"queen-" are the broker's/)
      assert.equal(hits.length, 0)
    })
  })

  it('a buffered push carries its headers to the flush', async () => {
    await withQueen({}, () => queued(2), async (queen, hits) => {
      const q = queen.queue('orders').buffer({ messageCount: 10, timeMillis: 60000 })
      await q.push({ data: { a: 1 }, headers: { type: 'a' } })
      await queen.queue('orders').buffer({ messageCount: 10, timeMillis: 60000 }).push({ data: { a: 2 } })
      await queen.flushAllBuffers()

      const { items } = jsonOf(pushes(hits)[0])
      assert.deepEqual(items.map(i => i.headers), [{ type: 'a' }, undefined])
    })
  })
})

describe('headers — the binary push body', () => {
  it('binaryPush alone: a push with headers is JSON (the plain body has no room for them)', async () => {
    await withQueen({ binaryPush: true }, () => queued(2), async (queen, hits) => {
      await queen.queue('orders').partition('p1').push([{ data: 1 }, { data: 2, headers: { type: 'x' } }])

      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/json')
      assert.deepEqual(jsonOf(hit).items[1].headers, { type: 'x' })
    })
  })

  it('binaryPush alone: a push without headers stays binary', async () => {
    await withQueen({ binaryPush: true }, () => queued(), async (queen, hits) => {
      await queen.queue('orders').partition('p1').push([{ data: 1, headers: {} }])

      assert.equal(pushes(hits)[0].headers['content-type'], 'application/x-queen-push')
    })
  })

  it('with pushCompression: frame flag 8, the block sorted by name after the transaction id', async () => {
    await withQueen({ binaryPush: true, pushCompression: true }, () => queued(3), async (queen, hits) => {
      await queen.queue('orders').partition('p1').push([
        { data: { a: 1 }, transactionId: 't1', headers: { zeta: 'z', alpha: 'ä€', 'b.c_d-e': '' } },
        { data: { a: 2 }, transactionId: 't2' },
        { data: { a: 3 }, transactionId: 't3', headers: {} }
      ])

      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/x-queen-push')
      assert.equal(hit.raw.subarray(0, 5).toString('latin1'), 'QPB1\x08')
      const [group] = framedGroups(hit.raw.subarray(5))
      assert.equal(group.count, 3)
      assert.deepEqual(group.frames.map(f => f.flags), [8, 0, 0], 'no flag for no headers, {} included')
      assert.deepEqual(group.frames[0].headers, [['alpha', 'ä€'], ['b.c_d-e', ''], ['zeta', 'z']])
      assert.deepEqual(group.frames.map(f => f.txn), ['t1', 't2', 't3'])
      assert.deepEqual(group.frames.map(f => f.payload), ['{"a":1}', '{"a":2}', '{"a":3}'])
    })
  })

  it('the block is the bytes the broker writes: u16 count | (u8 len, name | u16 len, value) x count', () => {
    const block = Buffer.from(headersBlock({ b: 'é', a: 'xy' }))
    assert.deepEqual([...block], [2, 0, 1, 0x61, 2, 0, 0x78, 0x79, 1, 0x62, 2, 0, 0xc3, 0xa9])
    assert.equal(headersBlock({}), null)
    assert.equal(headersBlock(null), null)
  })

  it('encodePushBinary refuses headers instead of dropping them; encodePushFrames carries them', async () => {
    const batch = { queue: 'q', partition: 'p', payloads: ['1'], headers: [{ k: 'v' }] }
    await assert.rejects(encodePushBinary([batch]), /carries no headers/)
    const bytes = Buffer.from(await encodePushFrames([batch]))
    assert.deepEqual(framedGroups(bytes.subarray(5))[0].frames[0].headers, [['k', 'v']])
  })
})

describe('headers — transactions', () => {
  const ok = (results = []) => () => ({ body: JSON.stringify({ transactionId: 'tx', success: true, results }) })

  it('a push in a transaction carries its envelope\'s headers (JSON)', async () => {
    await withQueen({}, ok(), async (queen, hits) => {
      await queen.transaction()
        .queue('out').partition('p1').push([{ data: { n: 1 }, transactionId: 'a', headers: { type: 't' } }, { data: { n: 2 } }])
        .commit()

      const [op] = jsonOf(transactions(hits)[0]).operations
      assert.deepEqual(op.items[0].headers, { type: 't' })
      assert.equal('headers' in op.items[1], false)
    })
  })

  it('a binary transaction frames the headers like the framed push', async () => {
    await withQueen({ binaryPush: true, pushCompression: true }, ok([{ index: 0 }, { index: 1 }]), async (queen, hits) => {
      await queen.transaction()
        .queue('out').partition('p1').push([{ data: { n: 1 }, transactionId: 'a', headers: { type: 't' } }, { data: { n: 2 }, transactionId: 'b' }])
        .commit()

      const raw = transactions(hits)[0].raw
      assert.equal(raw.subarray(0, 4).toString('latin1'), 'QTB1')
      const headLength = raw.readUInt32LE(5)
      const [group] = framedGroups(raw.subarray(9 + headLength))
      assert.deepEqual(group.frames.map(f => [f.flags, f.txn, f.headers]), [[8, 'a', [['type', 't']]], [0, 'b', null]])
    })
  })

  it('refuses bad headers in a transaction push before any request', async () => {
    await withQueen({}, ok(), async (queen, hits) => {
      assert.throws(() => queen.transaction().queue('out').push([{ data: 1, headers: { 'Bad Name': 'x' } }]), /header name "Bad Name"/)
      assert.equal(hits.length, 0)
    })
  })
})

describe('headers — timers', () => {
  const scheduled = () => ({ body: JSON.stringify({ results: [{ ok: true, status: 'scheduled' }] }) })
  const timerCalls = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/timers')

  it('a schedule carries the fired message\'s headers, standalone and in a transaction', async () => {
    const answer = (hit) => (hit.url === '/api/v1/timers' ? scheduled() : { body: JSON.stringify({ transactionId: 'tx', success: true, results: [] }) })
    await withQueen({}, answer, async (queen, hits) => {
      await queen.timer('reminders').key('order-9').delay('30m').payload({ orderId: 9 }).headers({ type: 'reminder' }).schedule()
      await queen.transaction()
        .timer('reminders').key('order-10').delay('30m').payload({ orderId: 10 }).headers({ type: 'reminder', 'trace.id': 'x' }).schedule()
        .commit()
      await queen.timer('reminders').key('order-11').delay('30m').payload({ orderId: 11 }).schedule()

      const [first, plain] = timerCalls(hits).map(jsonOf)
      assert.deepEqual(first.operations[0].headers, { type: 'reminder' })
      assert.equal('headers' in plain.operations[0], false, 'no headers: the op it always was')
      const [tx] = transactions(hits).map(jsonOf)
      assert.deepEqual(tx.timers[0].headers, { type: 'reminder', 'trace.id': 'x' })
    })
  })

  it('refuses bad timer headers when they are set', () => {
    const queen = new Queen({ url: 'http://127.0.0.1:1', handleSignals: false })
    assert.throws(() => queen.timer('reminders').key('k').headers({ 'queen-fire': 'x' }), /^Error: timer: header name "queen-fire"/)
    assert.throws(() => queen.timer('reminders').key('k').headers({ n: 1 }), /the value must be a string/)
    return queen.close()
  })
})

describe('headers — reads', () => {
  const POPPED = {
    id: '01000000-0000-7000-8000-000000000000', transactionId: 't1', traceId: null, data: { a: 1 }, producerSub: null,
    headers: { type: 'order.paid' }, createdAt: '2026-11-12T10:00:00.000Z', partitionId: '7', partition: 'p1',
    leaseId: 'lease-1', consumerGroup: 'billing', deliveryAttempt: 1, offset: 4
  }
  const PLAIN = { ...POPPED, transactionId: 't2', offset: 5 }
  delete PLAIN.headers

  it('pop() and consume() hand the JSON answer\'s headers on, and none when a message has none', async () => {
    const popAnswer = { body: JSON.stringify({ success: true, messages: [POPPED, PLAIN], partitionsClaimed: 1 }) }
    const ackAnswer = { body: JSON.stringify([{ index: 0, success: true }, { index: 1, success: true }]) }
    await withQueen({}, (hit) => (hit.url.startsWith('/api/v1/pop') ? popAnswer : ackAnswer), async (queen) => {
      const messages = await queen.queue('orders').group('billing').batch(2).pop()
      assert.deepEqual(messages.map(m => m.headers), [{ type: 'order.paid' }, undefined])

      const seen = []
      await queen.queue('orders').group('billing').batch(2).limit(2).consume(async (batch) => {
        for (const m of batch) seen.push(m.headers)
      })
      assert.deepEqual(seen, [{ type: 'order.paid' }, undefined])
    })
  })

  it('a framed pop reads a frame\'s headers block (flag 8), and refuses a link frame (flag 16)', async () => {
    const u16 = (n) => { const b = Buffer.alloc(2); b.writeUInt16LE(n); return b }
    const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
    const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
    const text = (s) => Buffer.concat([u16(Buffer.byteLength(s)), Buffer.from(s)])
    const frame = (flags, txn, extra, payload) => {
      const id = Buffer.alloc(16); id[6] = 0x70; id[8] = 0x80
      const body = Buffer.concat([Buffer.of(flags), id, text(txn), extra, Buffer.from(payload)])
      return Buffer.concat([u32(body.length), body])
    }
    const answer = (frames) => {
      const raw = zstdCompressSync(Buffer.concat(frames))
      return Buffer.concat([
        Buffer.from('QPF1'), Buffer.of(1), u32(2), Buffer.from('{}'),
        text('orders'), text('lease-1'), text('billing'), u32(1),
        u64(7), text('p1'), u64(0), u64(9), u32(1), u32(1),
        u64(0), u64(1_700_000_000_000_000n), u32(frames.length), Buffer.of(1), u32(raw.length), raw
      ])
    }
    const block = Buffer.from(headersBlock({ type: 'order.paid', region: 'eu' }))
    const got = await decodePopFrames(answer([
      frame(8, 't1', Buffer.concat([u16(block.length), block]), '{"a":1}'),
      frame(0, 't2', Buffer.alloc(0), '{"a":2}')
    ]))
    assert.deepEqual(got.messages[0].headers, { region: 'eu', type: 'order.paid' })
    assert.deepEqual(got.messages[0].data, { a: 1 })
    assert.equal('headers' in got.messages[1], false)

    await assert.rejects(decodePopFrames(answer([frame(16, 't3', Buffer.alloc(0), Buffer.alloc(24).toString('latin1'))])), /a frame is a link/)
  })

  it('fetch() keeps a record\'s headers', async () => {
    const answer = {
      body: JSON.stringify({
        entries: [{
          highWatermark: 2, logStartOffset: 0, partition: 'p1', queue: 'orders',
          records: [
            { headers: { type: 'x' }, offset: 0, payload: { a: 1 }, transactionId: 't1', ts: '2026-11-12T10:00:00.000000Z' },
            { offset: 1, payload: { a: 2 }, transactionId: 't2', ts: '2026-11-12T10:00:00.000000Z' }
          ]
        }]
      })
    }
    await withQueen({}, () => answer, async (queen) => {
      const read = await queen.queue('orders').fetch(['p1'])
      const records = read.get('p1').records
      assert.deepEqual(records[0], { offset: 0, transactionId: 't1', payload: { a: 1 }, ts: '2026-11-12T10:00:00.000000Z', headers: { type: 'x' } })
      assert.equal('headers' in records[1], false)
    })
  })

  it('a feed entry carries its headers, and a where on headers.<name> holds on them', async () => {
    const entry = (n, headers) => ({
      ...(headers ? { headers } : {}), id: `m${n}`, kind: 'push', offset: n, partition: 'c1', partitionId: 'uuid-1',
      payload: { n }, position: n, queue: 'orders', transactionId: `t${n}`, ts: '2026-11-12T10:00:00.000000Z'
    })
    const http = {
      async post() {
        return { applied: 2, caughtUp: true, entries: [entry(1, { type: 'order.paid' }), entry(2)], next: 3, resolvedTs: '2026-11-12T10:00:01.000000Z' }
      }
    }
    const reader = new FeedReader(http, () => http, 'http', { from: 'earliest', follow: false })
    const { entries } = await reader.read()
    assert.deepEqual(entries[0].headers, { type: 'order.paid' })
    assert.equal('headers' in entries[1], false)

    const paid = whereCondition('headers.type', '=', 'order.paid')
    assert.equal(conditionHolds(paid, entries[0]), true)
    assert.equal(conditionHolds(paid, entries[1]), false)
    // A header the message lacks holds for no op, != included, as on the broker.
    assert.equal(conditionHolds(whereCondition('headers.type', '!=', 'x'), entries[1]), false)
    assert.equal(conditionHolds(whereCondition('headers.type', 'exists'), entries[0]), true)
    assert.equal(conditionHolds(whereCondition('headers.constructor', 'exists'), entries[0]), false, 'own names only')
  })
})
