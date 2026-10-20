/**
 * queue.fetch() and queue.changedSince() against a fake broker that answers
 * POST /api/v1/fetch and POST /api/v1/partitions/changed the way
 * server/src/rsm/facade/real/phase2/reads.rs does: entries answered in order,
 * each entry's byte budget and the whole call's ceiling (the first record of an
 * entry read whatever it weighs), a record cap per entry, OFFSET_OUT_OF_RANGE
 * below logStartOffset and above the high watermark, UNKNOWN_TOPIC_OR_PARTITION
 * for a queue that does not exist, discovery pages with a `next` cursor exactly
 * when a page is full, BAD_CURSOR for a cursor it did not issue.
 *
 * A real node:http server records every request, so the bodies the client sends
 * are asserted on the socket.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'

const MiB = 1024 * 1024

class FakeBroker {
  constructor({ recordsPerEntry = 10_000 } = {}) {
    this.queues = new Map() // queue -> Map(partition -> { records, logStart, created })
    this.recordsPerEntry = recordsPerEntry
    this.hits = []
    this.safeTimes = []      // answered in turn by partitions/changed, then the last one
    this.afterFetch = null   // (broker) => void, run after every fetch answer
    this.created = 0
  }

  queue(name) {
    if (!this.queues.has(name)) this.queues.set(name, new Map())
    return this.queues.get(name)
  }

  write(queue, partition, payloads, { lastWriteAt = '2026-10-20T10:00:00.000000Z' } = {}) {
    const q = this.queue(queue)
    if (!q.has(partition)) q.set(partition, { records: [], logStart: 0, created: this.created++, lastWriteAt })
    const p = q.get(partition)
    for (const payload of payloads) {
      const offset = p.records.length === 0 ? p.logStart : p.records[p.records.length - 1].offset + 1
      p.records.push({ offset, transactionId: `${partition}-${offset}`, payload, ts: '2026-10-20T10:00:00.000000Z' })
    }
    p.lastWriteAt = lastWriteAt
  }

  /** Retention: everything below `offset` is gone. */
  trim(queue, partition, offset) {
    const p = this.queue(queue).get(partition)
    p.records = p.records.filter(r => r.offset >= offset)
    p.logStart = offset
  }

  fetch(body) {
    if (body.entries.length > 1024) return [400, { error: 'too many entries' }]
    const ceiling = body.maxBytes ?? null
    let used = 0
    const entries = body.entries.map(e => {
      const out = { queue: e.queue, partition: e.partition ?? 'Default', records: [], highWatermark: 0, logStartOffset: 0 }
      const q = this.queues.get(e.queue)
      if (!q) return { ...out, error: 'UNKNOWN_TOPIC_OR_PARTITION' }
      const p = q.get(out.partition)
      if (!p) return out
      const last = p.records.length ? p.records[p.records.length - 1].offset : p.logStart - 1
      out.highWatermark = last + 1
      out.logStartOffset = p.logStart
      if (e.offset < p.logStart || e.offset > out.highWatermark) return { ...out, error: 'OFFSET_OUT_OF_RANGE' }
      if (ceiling !== null && used >= ceiling) return out
      const budget = Math.min(e.maxBytes ?? MiB, ceiling === null ? Infinity : ceiling - used)
      let spent = 0
      for (const r of p.records) {
        if (r.offset < e.offset) continue
        if (out.records.length >= this.recordsPerEntry) break
        const weight = Math.max(JSON.stringify(r.payload).length, 1)
        if (out.records.length > 0 && spent + weight > budget) break
        spent += weight
        out.records.push(r)
      }
      used += spent
      return out
    })
    return [200, { entries }]
  }

  changed(body) {
    if (body.entries.length > 1024) return [400, { error: 'too many entries' }]
    const safeTime = this.safeTimes.length > 1 ? this.safeTimes.shift() : (this.safeTimes[0] ?? '2026-10-20T12:00:00.000000Z')
    const entries = body.entries.map(e => {
      const q = this.queues.get(e.queue)
      if (!q) return { queue: e.queue, error: 'UNKNOWN_TOPIC_OR_PARTITION' }
      let from = 0
      if (e.after) {
        const m = /^p\|(\d+)$/.exec(e.after)
        if (!m) return { queue: e.queue, error: 'BAD_CURSOR' }
        from = Number(m[1]) + 1
      }
      const limit = Math.min(Math.max(e.limit ?? 1000, 1), 1000)
      const all = [...q.entries()].sort((a, b) => a[1].created - b[1].created)
      const partitions = []
      let last = null
      for (const [name, p] of all) {
        if (p.created < from) continue
        if (e.since && p.lastWriteAt < e.since) continue
        const lastOffset = p.records.length ? p.records[p.records.length - 1].offset : p.logStart - 1
        partitions.push({ name, id: `id-${name}`, lastOffset, logStart: p.logStart, lastWriteAt: p.lastWriteAt })
        last = p.created
        if (partitions.length >= limit) break
      }
      return { queue: e.queue, partitions, next: partitions.length >= limit ? `p|${last}` : null }
    })
    return [200, { safeTime, safeTimeDegraded: false, entries }]
  }

  async run(test) {
    const server = createServer((req, res) => {
      let raw = ''
      req.on('data', c => { raw += c })
      req.on('end', () => {
        const body = raw ? JSON.parse(raw) : null
        this.hits.push({ method: req.method, url: req.url, body })
        let answer
        if (req.method === 'POST' && req.url === '/api/v1/fetch') {
          answer = this.fetch(body)
          this.afterFetch?.(this)
        } else if (req.method === 'POST' && req.url === '/api/v1/partitions/changed') {
          answer = this.changed(body)
        } else {
          answer = [404, { error: 'not found' }]
        }
        res.writeHead(answer[0], { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(answer[1]))
      })
    })
    await new Promise(r => server.listen(0, '127.0.0.1', r))
    const queen = new Queen({ url: `http://127.0.0.1:${server.address().port}`, handleSignals: false })
    try {
      await test(queen)
    } finally {
      await queen.close()
      await new Promise(r => server.close(r))
    }
  }

  fetches() {
    return this.hits.filter(h => h.url === '/api/v1/fetch')
  }
}

const payloads = (n, from = 0) => Array.from({ length: n }, (_, i) => ({ n: from + i }))
const offsetsOf = (part) => part.records.map(r => r.offset)

describe('queue.fetch()', () => {
  it('reads every record of each partition, from offset 0, in one call when they fit', async () => {
    const broker = new FakeBroker()
    broker.write('tf.events', 'trace-1', payloads(3))
    broker.write('tf.events', 'trace-2', [{ a: 1 }])
    await broker.run(async (queen) => {
      const read = await queen.queue('tf.events').fetch(['trace-1', 'trace-2'])
      assert.deepEqual([...read.keys()], ['trace-1', 'trace-2'])
      const t1 = read.get('trace-1')
      assert.deepEqual(t1.records.map(r => r.payload), payloads(3))
      assert.deepEqual(t1.records[0], { offset: 0, transactionId: 'trace-1-0', payload: { n: 0 }, ts: '2026-10-20T10:00:00.000000Z' })
      assert.equal(t1.partition, 'trace-1')
      assert.equal(t1.highWatermark, 3)
      assert.equal(t1.logStartOffset, 0)
      assert.equal(t1.nextOffset, 3)
      assert.deepEqual(read.get('trace-2').records.map(r => r.payload), [{ a: 1 }])

      const fetches = broker.fetches()
      assert.equal(fetches.length, 1)
      assert.deepEqual(fetches[0].body, {
        entries: [
          { queue: 'tf.events', partition: 'trace-1', offset: 0, maxBytes: 8 * MiB },
          { queue: 'tf.events', partition: 'trace-2', offset: 0, maxBytes: 8 * MiB }
        ],
        maxBytes: 8 * MiB
      })
    })
  })

  it('asks again from one past the last record until the high watermark of the first answer', async () => {
    const broker = new FakeBroker({ recordsPerEntry: 3 })
    broker.write('q', 'p', payloads(10))
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch('p')
      assert.deepEqual(offsetsOf(read.get('p')), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      assert.deepEqual(broker.fetches().map(h => h.body.entries[0].offset), [0, 3, 6, 9])
      assert.equal(read.get('p').nextOffset, 10)
    })
  })

  it('does not chase a partition written during the read', async () => {
    const broker = new FakeBroker({ recordsPerEntry: 2 })
    broker.write('q', 'p', payloads(4))
    broker.afterFetch = (b) => b.write('q', 'p', [{ late: true }])
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(['p'])
      // The first answer said the partition ended at 4: offsets 0..3 are read,
      // and the call stops there although the partition kept growing.
      assert.deepEqual(offsetsOf(read.get('p')), [0, 1, 2, 3])
      assert.equal(broker.fetches().length, 2)
      assert.equal(read.get('p').nextOffset, 4)
    })
  })

  it('asks for at most 1000 partitions per call', async () => {
    const broker = new FakeBroker()
    const names = Array.from({ length: 2500 }, (_, i) => `p-${i}`)
    for (const name of names) broker.write('q', name, [{ name }])
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(names)
      assert.deepEqual(broker.fetches().map(h => h.body.entries.length), [1000, 1000, 500])
      assert.equal(read.size, 2500)
      for (const name of names) assert.deepEqual(read.get(name).records.map(r => r.payload), [{ name }])
    })
  })

  it('asks again for the partitions the call ceiling left empty', async () => {
    const broker = new FakeBroker()
    const fat = 'x'.repeat(100)
    for (const name of ['a', 'b', 'c']) broker.write('q', name, [fat, fat])
    await broker.run(async (queen) => {
      // Each record weighs 102 bytes: one call carries two of them at most.
      const read = await queen.queue('q').fetch(['a', 'b', 'c'], { maxBytes: 250 })
      for (const name of ['a', 'b', 'c']) assert.deepEqual(offsetsOf(read.get(name)), [0, 1])
      assert.ok(broker.fetches().length > 1)
      assert.ok(broker.fetches().every(h => h.body.maxBytes === 250))
    })
  })

  it('reads from logStartOffset when retention deleted the start', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'p', payloads(10))
    broker.trim('q', 'p', 6)
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(['p'])
      assert.deepEqual(offsetsOf(read.get('p')), [6, 7, 8, 9])
      assert.equal(read.get('p').logStartOffset, 6)
      assert.deepEqual(broker.fetches().map(h => h.body.entries[0].offset), [0, 6])
    })
  })

  it('starts each partition at its offset in options.offsets', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'a', payloads(5))
    broker.write('q', 'b', payloads(5))
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(['a', 'b'], { offsets: { a: 3 } })
      assert.deepEqual(offsetsOf(read.get('a')), [3, 4])
      assert.deepEqual(offsetsOf(read.get('b')), [0, 1, 2, 3, 4])
      const again = await queen.queue('q').fetch(['a'], { offsets: new Map([['a', read.get('a').nextOffset]]) })
      assert.deepEqual(again.get('a').records, [])
      assert.equal(again.get('a').nextOffset, 5)
    })
  })

  it('answers no records for a missing queue, an unwritten partition and an offset past the end', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'p', payloads(2))
    await broker.run(async (queen) => {
      const missing = await queen.queue('nope').fetch(['p'])
      assert.deepEqual(missing.get('p').records, [])
      const read = await queen.queue('q').fetch(['never', 'p'], { offsets: { p: 7 } })
      assert.deepEqual(read.get('never').records, [])
      assert.deepEqual(read.get('p').records, [])
      assert.equal(read.get('p').nextOffset, 7)
      assert.equal(read.get('p').highWatermark, 2)
    })
  })

  it('throws on an error marker it does not know', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'p', payloads(1))
    broker.fetch = () => [200, { entries: [{ queue: 'q', partition: 'p', records: [], highWatermark: 1, logStartOffset: 0, error: 'SOMETHING_NEW' }] }]
    await broker.run(async (queen) => {
      await assert.rejects(queen.queue('q').fetch(['p']), /SOMETHING_NEW/)
    })
  })

  it('stops when a call reads nothing although the partition is not at its end', async () => {
    const broker = new FakeBroker()
    broker.fetch = () => [200, { entries: [{ queue: 'q', partition: 'p', records: [], highWatermark: 5, logStartOffset: 0 }] }]
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(['p'])
      assert.deepEqual(read.get('p').records, [])
      assert.equal(broker.fetches().length, 1)
    })
  })

  it('asks once for a partition named twice and refuses a bad name or offset before any call', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'p', payloads(1))
    await broker.run(async (queen) => {
      const read = await queen.queue('q').fetch(['p', 'p'])
      assert.equal(read.size, 1)
      assert.equal(broker.fetches()[0].body.entries.length, 1)
      await assert.rejects(queen.queue('q').fetch(['']), /non-empty string/)
      await assert.rejects(queen.queue('q').fetch(['p'], { offsets: { p: -1 } }), /non-negative integer/)
      await assert.rejects(queen.queue('q').fetch(['p'], { maxBytes: 0 }), /positive integer/)
      assert.equal(broker.fetches().length, 1)
    })
  })
})

describe('queue.changedSince()', () => {
  it('follows the page cursor to the end of the pass, in creation order', async () => {
    const broker = new FakeBroker()
    for (let i = 0; i < 2300; i++) broker.write('tf.events', `t-${i}`, [{ i }])
    await broker.run(async (queen) => {
      const { partitions, safeTime } = await queen.queue('tf.events').changedSince('2026-10-20T09:00:00Z')
      assert.equal(partitions.length, 2300)
      assert.equal(partitions[0].name, 't-0')
      assert.equal(partitions[2299].name, 't-2299')
      assert.deepEqual(partitions[0], { name: 't-0', id: 'id-t-0', lastOffset: 0, logStart: 0, lastWriteAt: '2026-10-20T10:00:00.000000Z' })
      assert.equal(safeTime, '2026-10-20T12:00:00.000000Z')
      const bodies = broker.hits.map(h => h.body.entries[0])
      assert.deepEqual(bodies.map(b => b.after ?? null), [null, 'p|999', 'p|1999'])
      assert.ok(bodies.every(b => b.since === '2026-10-20T09:00:00Z' && b.limit === 1000 && b.queue === 'tf.events'))
    })
  })

  it('lists only the partitions written at or after since', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'old', [1], { lastWriteAt: '2026-10-20T08:00:00.000000Z' })
    broker.write('q', 'new', [1], { lastWriteAt: '2026-10-20T10:30:00.000000Z' })
    await broker.run(async (queen) => {
      const since = new Date('2026-10-20T10:00:00Z')
      const { partitions } = await queen.queue('q').changedSince(since)
      assert.deepEqual(partitions.map(p => p.name), ['new'])
      assert.equal(broker.hits[0].body.entries[0].since, '2026-10-20T10:00:00.000Z')
      await queen.queue('q').changedSince(since.getTime())
      assert.equal(broker.hits[1].body.entries[0].since, '2026-10-20T10:00:00.000Z')
    })
  })

  it('without since lists every partition, and sends no since', async () => {
    const broker = new FakeBroker()
    broker.write('q', 'a', [1], { lastWriteAt: '2020-01-01T00:00:00.000000Z' })
    await broker.run(async (queen) => {
      const { partitions } = await queen.queue('q').changedSince()
      assert.deepEqual(partitions.map(p => p.name), ['a'])
      assert.equal('since' in broker.hits[0].body.entries[0], false)
    })
  })

  it('answers the smallest safeTime of its pages', async () => {
    const broker = new FakeBroker()
    for (let i = 0; i < 1500; i++) broker.write('q', `p-${i}`, [1])
    broker.safeTimes = ['2026-10-20T12:00:05.000000Z', '2026-10-20T12:00:01.000000Z']
    await broker.run(async (queen) => {
      const { safeTime } = await queen.queue('q').changedSince(null)
      assert.equal(safeTime, '2026-10-20T12:00:01.000000Z')
    })
  })

  it('answers no partitions for a queue that does not exist, and throws on BAD_CURSOR', async () => {
    const broker = new FakeBroker()
    await broker.run(async (queen) => {
      const { partitions, safeTime } = await queen.queue('ghost').changedSince('2026-10-20T00:00:00Z')
      assert.deepEqual(partitions, [])
      assert.equal(safeTime, '2026-10-20T12:00:00.000000Z')
    })
    const bad = new FakeBroker()
    bad.changed = () => [200, { safeTime: '2026-10-20T12:00:00.000000Z', safeTimeDegraded: false, entries: [{ queue: 'q', error: 'BAD_CURSOR' }] }]
    await bad.run(async (queen) => {
      await assert.rejects(queen.queue('q').changedSince(null), /BAD_CURSOR/)
    })
  })

  it('refuses a since that is not a time before any call', async () => {
    const broker = new FakeBroker()
    await broker.run(async (queen) => {
      await assert.rejects(queen.queue('q').changedSince(new Date('nope')), /invalid Date/)
      await assert.rejects(queen.queue('q').changedSince({}), /ISO-8601/)
      assert.equal(broker.hits.length, 0)
    })
  })
})
