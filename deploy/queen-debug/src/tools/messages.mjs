// Read messages and dead letters. Nothing here takes a lease or moves a cursor.

import { BrokerError } from '../broker.mjs'
import { segment, withQuery } from '../routes.mjs'
import { isHiddenQueue, parseWhere, whereHolds, whereOf } from '../rules.mjs'
import { freshnessNotes, readTriage } from '../state.mjs'

const FEED_PAGE = 1_000
const FEED_MAX_PAGES = 40
const FEED_BUDGET_MS = 10_000
const LIST_PAGE = 1_000
const NO_FOLLOWER_NOTE = 'The log search needs a follower and none answered, so this read the message lists instead, newest first.'
const HIDDEN_NOTE = 'Message bodies and headers are hidden. An operator turns them on with QUEEN_DEBUG_READ_MESSAGES.'
const ALL_TIME = '1970-01-01T00:00:00Z'

const FIELD_REF = /\b(?:data|headers)(?:\.[\w-]+)*/gi
const MESSAGE_FIELDS = [
  'transactionId', 'queue', 'partition', 'partitionId', 'namespace', 'task', 'status', 'offset', 'position',
  'createdAt', 'ts', 'failedAt', 'leaseExpiresAt', 'retryCount', 'errorMessage', 'consumerGroup', 'consumerGroups',
  'busStatus', 'traceId', 'id', 'messageId', 'data', 'payload', 'headers',
]

const STATUS_WORDS = {
  pending: 'Waiting to be consumed.',
  processing: 'A consumer holds it under a lease.',
  completed: 'Consumed.',
  dead_letter: 'It failed its last retry and is in the dead-letter queue.',
}

export function messageTools({ cluster, guard }) {
  const hiddenNote = () => (guard.readMessages ? [] : [HIDDEN_NOTE])

  function refuseHiddenWhere(where) {
    for (const field of String(where ?? '').match(FIELD_REF) ?? []) {
      if (guard.refusesField(field)) {
        throw new RangeError(`"${field}" is a masked field: searching on it would reveal its value`)
      }
    }
  }

  /** What a person debugging needs from a message row, without the storage detail. */
  function messageRow(m) {
    const row = guard.message(m)
    return Object.fromEntries(MESSAGE_FIELDS.filter((f) => row[f] !== undefined && row[f] !== null).map((f) => [f, row[f]]))
  }

  /** The feed, page by page, until enough matches or the time budget ends. */
  async function scanFeed({ queues, partitions, where, since, limit }) {
    const started = Date.now()
    const found = []
    let from = 'earliest'
    let source = null
    let done = false
    let applied = null
    let pages = 0
    while (pages < FEED_MAX_PAGES && found.length < limit && Date.now() - started < FEED_BUDGET_MS) {
      const body = { from, where, maxRecords: FEED_PAGE }
      if (queues?.length) body.queues = queues
      if (partitions?.length) body.partitions = partitions
      if (since) body.since = since
      const answer = await cluster.read('/api/v1/feed', { method: 'POST', body, relay: true, timeoutMs: 8_000 })
      pages += 1
      source = answer.source
      found.push(...(answer.data?.entries || []))
      applied = answer.data?.applied ?? applied
      if (answer.data?.caughtUp) {
        done = true
        break
      }
      if (answer.data?.next === undefined || answer.data.next === from) break
      from = answer.data.next
    }
    return { via: 'feed', found: found.slice(0, limit), complete: done, reachedPosition: from, applied, source, more: found.length > limit }
  }

  async function queuesMatching(pattern) {
    if (pattern && !/[*?]/.test(pattern)) return [pattern]
    const { data } = await cluster.read('/api/v1/resources/queues', { timeoutMs: 8_000 })
    const names = (data?.queues ?? []).map((q) => q.name).filter((n) => n && !isHiddenQueue(n))
    if (!pattern) return names
    const re = new RegExp(`^${pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`)
    return names.filter((n) => re.test(n))
  }

  /**
   * The same search over the message lists, judged here by the console's copy
   * of the broker's where rules. The broker serves the feed from followers
   * only, so when the one node left was the leader this is the way to search.
   */
  async function scanLists({ queues, partitions, where, since, limit }) {
    const { conds, error } = parseWhere(whereOf(where))
    if (error) throw new RangeError(error)
    const started = Date.now()
    const found = []
    let scanned = 0
    let complete = true
    let source = null
    const names = await queuesMatching(queues?.[0])
    outer: for (const queue of names) {
      for (let offset = 0; ; offset += LIST_PAGE) {
        if (Date.now() - started > FEED_BUDGET_MS) {
          complete = false
          break outer
        }
        let answer
        try {
          answer = await cluster.read(withQuery('/api/v1/messages', {
            queue,
            partition: partitions?.[0],
            from: since ?? ALL_TIME,
            limit: LIST_PAGE,
            offset: offset || undefined,
          }), { timeoutMs: 8_000 })
        } catch (e) {
          // Past the broker's deepest page: the rest of this queue is unread.
          if (e instanceof BrokerError && e.kind === 'bad_request' && offset > 0) {
            complete = false
            break
          }
          throw e
        }
        source = answer.source
        const rows = answer.data?.messages ?? []
        scanned += rows.length
        found.push(...rows.filter((m) => whereHolds(conds, m)))
        if (found.length > limit) {
          complete = false
          break outer
        }
        if (rows.length < LIST_PAGE) break
      }
    }
    return { via: 'list', found: found.slice(0, limit), complete, scanned, source, more: found.length > limit }
  }

  async function search(request) {
    try {
      return await scanFeed(request)
    } catch (e) {
      if (!(e instanceof BrokerError) || !['unavailable', 'wrong_node'].includes(e.kind)) throw e
      return scanLists(request)
    }
  }

  const searchedWords = (scan) => {
    if (scan.via === 'list') {
      return scan.complete ? `all ${scan.scanned} messages this node lists` : `the newest ${scan.scanned} messages. Narrow it with queue, partition or since.`
    }
    return scan.complete ? 'the whole log this node holds' : `up to position ${scan.reachedPosition} of ${scan.applied ?? '?'}. Narrow it with queue, partition or since.`
  }
  const scanNotes = (scan) => (scan?.via === 'list' ? [NO_FOLLOWER_NOTE] : [])

  const getMessage = {
    name: 'get_message',
    title: 'What happened to one message',
    description: 'Finds one message by transaction ID and says what happened to it: status, each consumer group\'s progress, last error, retries and trace. Give the queue. Give the partition too when you know it: the search is faster.',
    inputSchema: {
      type: 'object',
      properties: {
        transactionId: { type: 'string' },
        queue: { type: 'string', description: 'The queue. Without it every queue is searched.' },
        partition: { type: 'string', description: 'The partition name.' },
        partitionId: { type: 'string', description: 'The partition id from another answer: skips the search.' },
        since: { type: 'string', description: 'ISO-8601. Search only messages created after this.' },
      },
      required: ['transactionId'],
      additionalProperties: false,
    },
    async run({ transactionId, queue, partition, partitionId, since }) {
      let pid = partitionId
      let located = null
      let scan = null
      if (!pid) {
        scan = await search({
          queues: queue ? [queue] : undefined,
          partitions: partition ? [partition] : undefined,
          where: [{ field: 'transactionId', op: '=', value: transactionId }],
          since,
          limit: 5,
        })
        located = scan.found[0] ?? null
        if (!located) {
          return {
            found: false,
            transactionId,
            searched: searchedWords(scan),
            meaning: scan.complete ? 'No message has this id. It was never pushed here, or retention removed it.' : 'Not found in the part searched.',
            source: scan.source,
            notes: [...scanNotes(scan), ...freshnessNotes(scan.source)],
          }
        }
        pid = located.partitionId
      }

      const base = `/api/v1/messages/${segment(pid, 'partition')}/${segment(transactionId, 'transaction')}`
      let m
      let source
      try {
        ;({ data: m, source } = await cluster.read(base, { timeoutMs: 8_000 }))
      } catch (e) {
        if (!(e instanceof BrokerError) || e.kind !== 'not_found') throw e
        // The broker stops looking a partition up by uuid after 2M partitions,
        // so a 404 here does not mean the message is gone.
        if (located) {
          return {
            found: true,
            partial: true,
            meaning: 'The log holds this message, but the broker did not return its status, so whether each group consumed it is unknown.',
            message: messageRow(located),
            source: scan.source,
            notes: [...scanNotes(scan), ...hiddenNote(), ...freshnessNotes(scan.source)],
          }
        }
        return { found: false, transactionId, partitionId: pid, meaning: 'No message has this id in that partition.' }
      }

      let trace = []
      try {
        const { data } = await cluster.read(`/api/v1/traces/${segment(pid, 'partition')}/${segment(transactionId, 'transaction')}`, { timeoutMs: 5_000 })
        trace = (data?.events || data?.traces || []).map(traceRow)
      } catch (e) {
        if (!(e instanceof BrokerError)) throw e
      }
      const row = messageRow(m)
      const others = scan && scan.found.length > 1 ? scan.found.slice(1).map((x) => ({ queue: x.queue, partition: x.partition, partitionId: x.partitionId })) : undefined
      return {
        found: true,
        meaning: STATUS_WORDS[row.status] ?? undefined,
        message: row,
        trace: trace.length ? trace : undefined,
        sameIdElsewhere: others,
        source,
        notes: [...scanNotes(scan), ...hiddenNote(), ...freshnessNotes(source)],
      }
    },
  }

  const listMessages = {
    name: 'list_messages',
    title: 'List messages',
    description: 'Messages newest first, by queue, partition, status and time. The window defaults to the last hour, so pass from for older ones. Nothing is leased or moved.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        partition: { type: 'string' },
        namespace: { type: 'string' },
        task: { type: 'string' },
        status: { type: 'string', enum: ['pending', 'processing', 'completed', 'dead_letter'] },
        from: { type: 'string', description: 'ISO-8601, inclusive. Default one hour ago.' },
        to: { type: 'string', description: 'ISO-8601. Default now.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 20.' },
        offset: { type: 'integer', minimum: 0, description: 'Rows to skip.' },
      },
      additionalProperties: false,
    },
    async run({ limit = 20, offset = 0, ...filters }) {
      const { data, source } = await cluster.read(withQuery('/api/v1/messages', { ...filters, limit, offset: offset || undefined }), { timeoutMs: 15_000 })
      const rows = (data?.messages || []).map(messageRow)
      return {
        count: rows.length,
        nextOffset: rows.length === limit ? offset + limit : undefined,
        messages: rows,
        source,
        notes: [...hiddenNote(), ...freshnessNotes(source)],
      }
    },
  }

  const fetchMessages = {
    name: 'fetch_messages',
    title: 'Read a partition by offset',
    description: 'Reads a partition\'s log in order from an offset or a time, like the SDKs\' fetch. Nothing is leased or moved, so it is safe on a live queue.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        partition: { type: 'string' },
        offset: { type: 'integer', minimum: 0, description: 'First offset. Default: the start of the log.' },
        from: { type: 'string', description: 'ISO-8601. Start at the first message appended at or after this time.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 20.' },
      },
      required: ['queue', 'partition'],
      additionalProperties: false,
    },
    async run({ queue, partition, offset, from, limit = 20 }) {
      let start = offset
      if (from !== undefined) {
        const { data } = await cluster.read('/api/v1/fetch/offsets', {
          method: 'POST',
          body: { entries: [{ queue, partition, timestamp: from }] },
          timeoutMs: 6_000,
        })
        const e = data?.entries?.[0]
        if (e?.error) return { error: e.error, queue, partition }
        start = e?.offset ?? e?.highWatermark ?? 0
      }
      if (start === undefined) {
        const { data } = await cluster.read('/api/v1/fetch/offsets', {
          method: 'POST',
          body: { entries: [{ queue, partition, timestamp: ALL_TIME }] },
          timeoutMs: 6_000,
        })
        const e = data?.entries?.[0]
        if (e?.error) return { error: e.error, queue, partition }
        start = e?.logStartOffset ?? 0
      }
      const { data, source } = await cluster.read('/api/v1/fetch', {
        method: 'POST',
        body: { entries: [{ queue, partition, offset: start, maxBytes: 1024 * 1024 }] },
        timeoutMs: 8_000,
      })
      const e = data?.entries?.[0] ?? {}
      if (e.error) return { error: e.error, queue, partition, offset: start, logStartOffset: e.logStartOffset, highWatermark: e.highWatermark }
      const records = (e.records || []).slice(0, limit)
      const last = records.at(-1)?.offset
      return {
        queue,
        partition,
        fromOffset: start,
        logStartOffset: e.logStartOffset,
        highWatermark: e.highWatermark,
        count: records.length,
        nextOffset: last !== undefined && last + 1 < e.highWatermark ? last + 1 : undefined,
        records: records.map(messageRow),
        source,
        notes: [...hiddenNote(), ...freshnessNotes(source)],
      }
    },
  }

  const findMessages = {
    name: 'find_messages',
    title: 'Search messages',
    description: 'Searches the log across queues by transaction ID, a payload field or a header. where uses the broker\'s language, for example: data.orderId = 9137 and headers.type = \'paid\'. Ops: = != < <= > >= in exists prefix. It reads the log in order, so narrow it with queue or since when you can.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string', description: 'A queue name or a pattern, like orders*. Default every queue.' },
        partition: { type: 'string' },
        transactionId: { type: 'string' },
        where: { type: 'string', description: 'A condition in the broker\'s where language.' },
        since: { type: 'string', description: 'ISO-8601. Only messages created at or after this.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 20.' },
      },
      additionalProperties: false,
    },
    async run({ queue, partition, transactionId, where, since, limit = 20 }) {
      if (!transactionId && !where) throw new RangeError('give transactionId or where')
      refuseHiddenWhere(where)
      // The language has `and` and no parentheses; a doubled quote is a quote.
      const byId = transactionId ? `transactionId = '${String(transactionId).replace(/'/g, "''")}'` : null
      const condition = [byId, where].filter(Boolean).join(' and ')
      const scan = await search({
        queues: queue ? [queue] : undefined,
        partitions: partition ? [partition] : undefined,
        where: condition,
        since,
        limit,
      })
      return {
        count: scan.found.length,
        complete: scan.complete,
        searched: searchedWords(scan),
        messages: scan.found.map(messageRow),
        moreMatches: scan.more || undefined,
        source: scan.source,
        notes: [...scanNotes(scan), ...hiddenNote(), ...freshnessNotes(scan.source)],
      }
    },
  }

  function traceRow(e) {
    const out = guard.message({ data: e.data, message_payload: e.message_payload })
    return {
      at: e.createdAt ?? e.created_at,
      type: e.eventType ?? e.event_type,
      queue: e.queue_name ?? undefined,
      partition: e.partition_name ?? undefined,
      partitionId: e.partitionId ?? e.partition_id ?? undefined,
      group: e.consumerGroup ?? e.consumer_group ?? undefined,
      transactionId: e.transactionId ?? e.transaction_id ?? undefined,
      names: e.names ?? undefined,
      data: out.data,
    }
  }

  const followTrace = {
    name: 'follow_trace',
    title: 'Follow a business flow',
    description: 'Follows one trace across queues, for apps that record traces. Without a name it lists the trace names, most recent first.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'The trace name, like order-9137.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 50.' },
        offset: { type: 'integer', minimum: 0 },
      },
      additionalProperties: false,
    },
    async run({ name, limit = 50, offset = 0 }) {
      if (!name) {
        const { data, source } = await cluster.read(withQuery('/api/v1/traces/names', { limit, offset: offset || undefined }), { timeoutMs: 8_000 })
        return { total: data?.total, names: data?.trace_names ?? [], source, notes: freshnessNotes(source) }
      }
      const { data, source } = await cluster.read(
        withQuery(`/api/v1/traces/by-name/${segment(name, 'trace')}`, { limit, offset: offset || undefined }),
        { timeoutMs: 8_000 },
      )
      const events = (data?.events || data?.traces || []).map(traceRow)
      return {
        name,
        total: data?.total,
        events,
        nextOffset: events.length === limit ? offset + limit : undefined,
        source,
        notes: [...hiddenNote(), ...freshnessNotes(source)],
      }
    },
  }

  const dlqSummary = {
    name: 'dlq_summary',
    title: 'Why messages failed',
    description: 'Dead letters grouped by error, with ids and numbers taken out so the same failure counts once. For one queue, or every queue that has dead letters.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string', description: 'Leave out for every queue with dead letters.' },
        sample: { type: 'integer', minimum: 1, maximum: 1000, description: 'Newest rows to read per queue. Default 200.' },
      },
      additionalProperties: false,
    },
    async run({ queue, sample = 200 }) {
      let names = queue ? [queue] : null
      let totals = null
      const notes = []
      if (!names) {
        const t = await readTriage(cluster)
        const withDead = t.queues
          .map((q) => ({ name: q.name, deadLetter: Number(q.messages?.deadLetter) || 0 }))
          .filter((q) => q.deadLetter > 0)
          .sort((a, b) => b.deadLetter - a.deadLetter)
        totals = { queues: withDead.length, messages: withDead.reduce((n, q) => n + q.deadLetter, 0) }
        names = withDead.slice(0, 5).map((q) => q.name)
        if (withDead.length > 5) notes.push(`Showing the 5 queues with the most dead letters, of ${withDead.length}.`)
      }
      const queues = []
      let source = null
      for (const name of names) {
        const a = await cluster.read(withQuery('/api/v1/analytics/dlq-signatures', { queue: name, limit: sample }), { timeoutMs: 8_000 })
        source = a.source
        queues.push({
          queue: name,
          deadLetters: a.data?.rowsNow,
          sampled: a.data?.sample,
          groups: a.data?.groups,
          oldest: a.data?.oldest,
          newest: a.data?.newest,
          retryCounts: a.data?.retryCounts,
          byDay: a.data?.byDay,
          errors: (a.data?.signatures || []).map((s) => ({ count: s.n, share: s.share, error: guard.text(s.text) })),
        })
      }
      return { totals: totals ?? undefined, queues, source: source ?? undefined, notes: [...notes, ...freshnessNotes(source)] }
    },
  }

  const listDlq = {
    name: 'list_dlq',
    title: 'List dead letters',
    description: 'The messages in the dead-letter queue, newest first, with the error, retry count and failing group.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        group: { type: 'string', description: 'Only this consumer group.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 20.' },
        offset: { type: 'integer', minimum: 0 },
      },
      additionalProperties: false,
    },
    async run({ queue, group, limit = 20, offset = 0 }) {
      const { data, source } = await cluster.read(withQuery('/api/v1/dlq', { queue, consumerGroup: group, limit, offset: offset || undefined }), { timeoutMs: 15_000 })
      const rows = (data?.messages || []).map(messageRow)
      return {
        total: data?.total,
        count: rows.length,
        nextOffset: offset + rows.length < (data?.total ?? 0) ? offset + rows.length : undefined,
        messages: rows,
        source,
        notes: [...hiddenNote(), ...freshnessNotes(source)],
      }
    },
  }

  return [getMessage, listMessages, fetchMessages, findMessages, followTrace, dlqSummary, listDlq]
}
