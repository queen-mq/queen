/**
 * Reads by offset and partition discovery, as two queue methods:
 *
 *   const read = await queen.queue('orders').fetch(['customer-42', 'customer-7'])
 *   read.get('customer-42').records   // [{ offset, transactionId, payload, ts }, ...]
 *
 *   const { partitions, safeTime } = await queen.queue('orders').changedSince(since)
 *
 * `fetch` is `POST /api/v1/fetch` and `changedSince` is
 * `POST /api/v1/partitions/changed`. Neither takes a lease, moves a cursor or
 * needs an ack: two readers asking the same question get the same answer, and
 * no consumer group notices them. Each is served from the answering node's own
 * applied state.
 *
 * Both do the loops a caller of the raw routes has to write:
 *
 * - fetch asks for at most 1000 partitions per call, and asks again from one
 *   past the last record until each partition reaches the high watermark its
 *   first answer reported. A partition whose start was deleted by retention is
 *   read from its oldest kept record (an offset below `logStartOffset` answers
 *   OFFSET_OUT_OF_RANGE and no records, so a loop that does not restart there
 *   silently reads nothing).
 * - changedSince follows the page cursor (`next`) until the broker says the
 *   pass is over, and answers the smallest `safeTime` of its pages.
 */

// The broker refuses more than 1024 entries in one call; 1000 leaves room.
const ENTRIES_PER_CALL = 1000

// Bytes of payload one call may carry: each entry's budget and the ceiling of
// the whole call. 8 MiB is the most the broker reads for one entry.
const DEFAULT_MAX_BYTES = 8 * 1024 * 1024

// Partitions per discovery page: the broker's default and its ceiling.
const CHANGED_PAGE = 1000

const OFFSET_OUT_OF_RANGE = 'OFFSET_OUT_OF_RANGE'
const UNKNOWN_TOPIC_OR_PARTITION = 'UNKNOWN_TOPIC_OR_PARTITION'

// Every call of one read goes to the same node: a follower answers from what it
// has applied, so two nodes can disagree about where a partition ends.
const readAffinity = (queue) => `${queue}:*:__READ__`

function partitionNames(partitions) {
  const list = typeof partitions === 'string' ? [partitions] : partitions
  if (!Array.isArray(list)) {
    throw new Error('fetch: partitions must be a partition name or an array of names')
  }
  const names = []
  const seen = new Set()
  for (const p of list) {
    const name = typeof p === 'number' && Number.isInteger(p) ? String(p) : p
    if (typeof name !== 'string' || name.length === 0) {
      throw new Error(`fetch: a partition name must be a non-empty string, got ${JSON.stringify(p)}`)
    }
    if (!seen.has(name)) {
      seen.add(name)
      names.push(name)
    }
  }
  return names
}

function startOffsets(offsets) {
  if (offsets == null) return new Map()
  const entries = offsets instanceof Map ? [...offsets.entries()] : Object.entries(offsets)
  const out = new Map()
  for (const [name, offset] of entries) {
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new Error(`fetch: offsets['${name}'] must be a non-negative integer, got ${JSON.stringify(offset)}`)
    }
    out.set(String(name), offset)
  }
  return out
}

function positiveInteger(value, what) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${what} must be a positive integer, got ${JSON.stringify(value)}`)
  }
  return value
}

/**
 * Every record of `partitions`, read by offset: from each partition's offset in
 * `options.offsets` (default 0, the oldest record still kept) up to where the
 * partition ended when it was first read.
 *
 * Answers a Map, in the order the partitions were named, of
 * `{ partition, records, highWatermark, logStartOffset, nextOffset }`:
 * - `records`: `{ offset, transactionId, payload, ts }` in offset order. `ts` is
 *   the time the push that wrote the record was stored.
 * - `highWatermark`: the next offset the partition will assign;
 *   `logStartOffset`: the oldest one retention kept.
 * - `nextOffset`: where to start next time to read only what is new.
 *
 * A partition nobody has written, or of a queue that does not exist, answers no
 * records. So does an offset past the end of the partition (`nextOffset` above
 * `highWatermark` says so: the partition was deleted and written again).
 */
export async function fetchPartitions(httpClient, queue, partitions, options = {}) {
  if (!queue) throw new Error('Queue name is required for fetch')
  const names = partitionNames(partitions)
  const offsets = startOffsets(options.offsets)
  const maxBytes = positiveInteger(options.maxBytes ?? DEFAULT_MAX_BYTES, 'fetch: maxBytes')

  const state = new Map()
  for (const name of names) {
    state.set(name, {
      partition: name,
      records: [],
      highWatermark: 0,
      logStartOffset: 0,
      nextOffset: offsets.get(name) ?? 0,
      // The high watermark of the first answer: the read ends there.
      end: null
    })
  }

  let pending = names
  while (pending.length > 0) {
    const again = []
    for (let i = 0; i < pending.length; i += ENTRIES_PER_CALL) {
      const chunk = pending.slice(i, i + ENTRIES_PER_CALL)
      const body = {
        entries: chunk.map(name => ({
          queue,
          partition: name,
          offset: state.get(name).nextOffset,
          maxBytes
        })),
        maxBytes
      }
      const answer = await httpClient.post('/api/v1/fetch', body, null, readAffinity(queue))
      const entries = answer?.entries
      if (!Array.isArray(entries) || entries.length !== chunk.length) {
        throw new Error(`fetch ${queue}: the broker answered ${Array.isArray(entries) ? entries.length : 'no'} entries for ${chunk.length} asked`)
      }

      let delivered = 0
      // Entries that came back empty while short of their end: the call's byte
      // ceiling was spent by the entries before them.
      const starved = []
      chunk.forEach((name, j) => {
        const entry = entries[j]
        const s = state.get(name)
        s.highWatermark = entry.highWatermark ?? s.highWatermark
        s.logStartOffset = entry.logStartOffset ?? s.logStartOffset
        if (entry.error) {
          if (entry.error === OFFSET_OUT_OF_RANGE && s.nextOffset < s.logStartOffset) {
            // Retention deleted what was asked for: go on from the oldest kept.
            s.nextOffset = s.logStartOffset
            if (s.end === null) s.end = s.highWatermark
            if (s.nextOffset < s.end) again.push(name)
            return
          }
          if (entry.error === OFFSET_OUT_OF_RANGE || entry.error === UNKNOWN_TOPIC_OR_PARTITION) {
            return
          }
          throw new Error(`fetch ${queue}/${name}: the broker answered ${entry.error}`)
        }
        if (s.end === null) s.end = s.highWatermark
        const records = Array.isArray(entry.records) ? entry.records : []
        if (records.length > 0) {
          delivered += records.length
          for (const r of records) {
            s.records.push({ offset: r.offset, transactionId: r.transactionId, payload: r.payload, ts: r.ts })
          }
          s.nextOffset = records[records.length - 1].offset + 1
          if (s.nextOffset < s.end) again.push(name)
        } else if (s.nextOffset < s.end) {
          starved.push(name)
        }
      })
      // An empty entry is asked again only when its call read something: a
      // call that read nothing at all would answer the same next time.
      if (delivered > 0) again.push(...starved)
    }
    pending = again
  }

  const out = new Map()
  for (const name of names) {
    const { partition, records, highWatermark, logStartOffset, nextOffset } = state.get(name)
    out.set(name, { partition, records, highWatermark, logStartOffset, nextOffset })
  }
  return out
}

function sinceIso(since) {
  if (since == null) return null
  if (since instanceof Date) {
    if (Number.isNaN(since.getTime())) throw new Error('changedSince: since is an invalid Date')
    return since.toISOString()
  }
  if (typeof since === 'number') {
    const d = new Date(since)
    if (Number.isNaN(d.getTime())) throw new Error(`changedSince: since ${since} is not a time`)
    return d.toISOString()
  }
  if (typeof since === 'string' && since.length > 0) return since
  throw new Error(`changedSince: since must be a Date, epoch milliseconds or an ISO-8601 string, got ${JSON.stringify(since)}`)
}

/**
 * The partitions of the queue written at or after `since`, in the order they
 * were created: `{ partitions, safeTime }`. Each partition is
 * `{ name, id, lastOffset, logStart, lastWriteAt }`; `lastOffset` is the offset
 * of its last record (-1 when none).
 *
 * `since` is a Date, epoch milliseconds or an ISO-8601 string; null or absent
 * lists every partition of the queue. A queue that does not exist has none.
 *
 * `safeTime` is the broker's time up to which every write is counted in this
 * answer: pass it as the next `since` to see only what changed after this
 * call. It comes from the broker's clock, never compare it with the local one.
 */
export async function partitionsChangedSince(httpClient, queue, since) {
  if (!queue) throw new Error('Queue name is required for changedSince')
  const from = sinceIso(since)
  const partitions = []
  let safeTime = null
  let after = null
  for (;;) {
    const entry = { queue, limit: CHANGED_PAGE }
    if (from !== null) entry.since = from
    if (after !== null) entry.after = after
    const answer = await httpClient.post('/api/v1/partitions/changed', { entries: [entry] }, null, readAffinity(queue))
    // Pages served by different nodes: the smallest is the one that holds for
    // all of them. The broker spells every one the same way, so the strings
    // compare in time order.
    if (typeof answer?.safeTime === 'string' && (safeTime === null || answer.safeTime < safeTime)) {
      safeTime = answer.safeTime
    }
    const result = answer?.entries?.[0]
    if (!result) throw new Error(`changedSince ${queue}: the broker answered no entry`)
    if (result.error === UNKNOWN_TOPIC_OR_PARTITION) break
    if (result.error) throw new Error(`changedSince ${queue}: the broker answered ${result.error}`)
    for (const p of result.partitions || []) {
      partitions.push({
        name: p.name,
        id: p.id ?? null,
        lastOffset: p.lastOffset,
        logStart: p.logStart,
        lastWriteAt: p.lastWriteAt
      })
    }
    if (!result.next) break
    after = result.next
  }
  return { partitions, safeTime }
}
