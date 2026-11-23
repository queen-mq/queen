// Application state: KV, timers, locks, streams and connectors. The broker
// answers these only with a quorum, so each one says so instead of hanging.

import { BrokerError } from '../broker.mjs'
import { keyPath, segment, withQuery } from '../routes.mjs'
import { LOCKS_NAMESPACE, decodeTimerPayload, locksListBody, permitsOf } from '../rules.mjs'
import { NEEDS_QUORUM, freshnessNotes, readFeed } from '../state.mjs'

const TIMEOUT_MS = 5_000
const HIDDEN_NOTE = 'Values are hidden. An operator turns them on with QUEEN_DEBUG_READ_MESSAGES.'

// The Prometheus families that say how an S3 sink is doing, per node.
const S3_FAMILIES = ['queen_s3_lag_seconds', 'queen_s3_safe_lag_seconds', 'queen_s3_records_written_total', 'queen_s3_records_lost_total']

function s3Lines(text, node) {
  const out = []
  for (const line of String(text).split('\n')) {
    if (!S3_FAMILIES.some((f) => line.startsWith(f))) continue
    const at = line.lastIndexOf(' ')
    if (at > 0) out.push({ node, metric: line.slice(0, at), value: Number(line.slice(at + 1)) })
  }
  return out
}

export function stateTools({ cluster, guard }) {
  /** Runs a read that needs a quorum, or says plainly that there is none. */
  async function withQuorum(fn) {
    if (!(await cluster.hasQuorum())) return { ...NEEDS_QUORUM }
    try {
      return await fn()
    } catch (e) {
      if (e instanceof BrokerError && (e.kind === 'unavailable' || e.kind === 'timeout')) {
        return { ...NEEDS_QUORUM, detail: e.message }
      }
      throw e
    }
  }

  const hiddenNote = () => (guard.readMessages ? [] : [HIDDEN_NOTE])

  const readKv = {
    name: 'read_kv',
    title: 'Read KV state',
    description: 'Application state in KV. With no namespace it lists the namespaces. With a namespace it lists keys. With a key it reads that key, with its version and expiry. Needs a quorum.',
    inputSchema: {
      type: 'object',
      properties: {
        namespace: { type: 'string' },
        key: { type: 'string' },
        prefix: { type: 'string', description: 'Only keys starting with this.' },
        after: { type: 'string', description: 'Continue after this key.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 50.' },
      },
      additionalProperties: false,
    },
    run({ namespace, key, prefix, after, limit = 50 }) {
      return withQuorum(async () => {
        if (key !== undefined && namespace === undefined) throw new RangeError('a key needs its namespace')
        if (namespace === undefined) {
          const { data, source } = await cluster.read('/api/v1/resources/kv/namespaces', { timeoutMs: TIMEOUT_MS })
          return { namespaces: data?.namespaces ?? [], source }
        }
        if (key !== undefined) {
          const { data, source } = await cluster.read(`/api/v1/kv/${segment(namespace, 'namespace')}/${keyPath(key)}`, { timeoutMs: TIMEOUT_MS })
          if (!data?.found) return { found: false, namespace, key, source }
          return {
            found: true, namespace, key: data.key, version: data.version, updatedAt: data.updatedAt, expiresAt: data.expiresAt,
            value: guard.value(data.value), source, notes: hiddenNote(),
          }
        }
        const body = { namespace, includeExpired: false, limit }
        if (prefix) body.prefix = prefix
        if (after) body.after = after
        const { data, source } = await cluster.read('/api/v1/resources/kv/list', { method: 'POST', body, timeoutMs: TIMEOUT_MS })
        return {
          namespace,
          count: data?.rows?.length ?? 0,
          nextAfter: data?.truncated ? data.nextAfter : undefined,
          rows: (data?.rows || []).map((r) => ({ key: r.key, version: r.version, updatedAt: r.updatedAt, expiresAt: r.expiresAt, value: guard.value(r.value) })),
          source,
          notes: hiddenNote(),
        }
      })
    },
  }

  const listTimers = {
    name: 'list_timers',
    title: 'List timers',
    description: 'Timers waiting to fire on a queue, or one timer by key, with when it fires, attempts and last error. Needs a quorum.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        timerKey: { type: 'string' },
        after: { type: 'string', description: 'Continue after this key.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 50.' },
      },
      required: ['queue'],
      additionalProperties: false,
    },
    run({ queue, timerKey, after, limit = 50 }) {
      return withQuorum(async () => {
        const q = segment(queue, 'queue')
        const row = (t) => ({
          timerKey: t.timerKey, partition: t.partition, deliverAt: t.deliverAt, attempts: t.attempts, claimed: t.claimed,
          lastError: t.lastError ? guard.text(t.lastError) : null, txn: t.txn, createdAt: t.createdAt,
        })
        if (timerKey !== undefined) {
          const { data, source } = await cluster.read(`/api/v1/timers/${q}/${keyPath(timerKey)}`, { timeoutMs: TIMEOUT_MS })
          if (data?.found === false) return { found: false, queue, timerKey, source }
          let payload
          if (guard.readMessages) {
            const d = decodeTimerPayload(data)
            if (d.state === 'ok' || d.state === 'text' || d.text) {
              try {
                payload = guard.value(JSON.parse(d.text))
              } catch {
                payload = guard.text(d.text)
              }
            } else {
              payload = `[${d.state}]`
            }
          }
          return { found: true, queue, timer: { ...row(data), payload }, source, notes: hiddenNote() }
        }
        const { data, source } = await cluster.read(withQuery(`/api/v1/timers/${q}`, { limit, after }), { timeoutMs: TIMEOUT_MS })
        return {
          queue,
          count: data?.rows?.length ?? 0,
          nextAfter: data?.truncated ? data.nextAfter : undefined,
          timers: (data?.rows || []).map(row),
          source,
        }
      })
    },
  }

  const listLocks = {
    name: 'list_locks',
    title: 'List locks',
    description: 'Who holds each lock and semaphore right now, since when, and when it expires if nobody renews it. Needs a quorum.',
    inputSchema: {
      type: 'object',
      properties: {
        prefix: { type: 'string', description: 'Only lock names starting with this.' },
        limit: { type: 'integer', minimum: 1, maximum: 500, description: 'Default 100.' },
      },
      additionalProperties: false,
    },
    run({ prefix, limit = 100 }) {
      return withQuorum(async () => {
        const { data, source } = await cluster.read('/api/v1/resources/kv/list', {
          method: 'POST',
          body: locksListBody({ prefix, limit }),
          timeoutMs: TIMEOUT_MS,
        })
        const permits = permitsOf(data?.rows)
        return {
          namespace: LOCKS_NAMESPACE,
          count: permits.length,
          more: data?.truncated || undefined,
          locks: permits.map((p) => ({ name: p.name, slot: p.slot, semaphore: p.shared || undefined, owner: p.owner, since: p.since, renewedAt: p.renewedAt, expiresAt: p.expiresAt, notAPermit: p.foreign || undefined })),
          source,
        }
      })
    },
  }

  const streamState = {
    name: 'stream_state',
    title: 'Stream state',
    description: 'Running streams, both kinds: registered queries, with any whose windows nobody is closing, and feed streams with their status and how far behind they are. Needs a quorum.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'A query id: also list its partitions untouched for untouchedMinutes.' },
        untouchedMinutes: { type: 'integer', minimum: 0, maximum: 10080, description: 'Default 5.' },
      },
      additionalProperties: false,
    },
    run({ query, untouchedMinutes = 5 }) {
      return withQuorum(async () => {
        const [{ data, source }, feed] = await Promise.all([
          cluster.read('/streams/v1/queries', { timeoutMs: TIMEOUT_MS }),
          readFeed(cluster),
        ])
        let idlePartitions
        if (query) {
          const { data: p } = await cluster.read('/streams/v1/state/partitions', {
            method: 'POST',
            body: { query_id: query, untouched_for_ms: untouchedMinutes * 60_000, limit: 100 },
            timeoutMs: TIMEOUT_MS,
          })
          idlePartitions = p
        }
        return {
          overdueAfterMs: data?.overdue_after_ms,
          queries: data?.queries ?? [],
          idlePartitions,
          feedStreams: feed.rows.map((r) => ({
            name: r.name, status: r.status, needsAttention: r.needs ? true : undefined, reads: r.source ?? undefined, sink: r.sinkText ?? undefined,
            shards: r.shardCount, behindSeconds: r.lag, openWindows: r.open, runsOn: r.hosts, failed: r.failed || undefined,
          })),
          source,
        }
      })
    },
  }

  const connectorStatus = {
    name: 'connector_status',
    title: 'Connector status',
    description: 'Postgres connectors with their state, and S3 sink lag and losses per node. Passwords are never returned. The Postgres part needs a quorum.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    async run() {
      const { nodes } = await cluster.health()
      const reachable = nodes.filter((n) => n.reachable)
      const s3 = (await Promise.all(reachable.map(async (n) => {
        try {
          return s3Lines(await cluster.readOn(n, '/metrics/prometheus', { timeoutMs: TIMEOUT_MS }), n.name)
        } catch (e) {
          if (e instanceof BrokerError) return []
          throw e
        }
      }))).flat()

      const pg = await withQuorum(async () => {
        const { data, source } = await cluster.read('/api/v1/connectors', { timeoutMs: TIMEOUT_MS })
        return { connectors: data?.connectors ?? [], source, notes: freshnessNotes(source) }
      })
      return { postgres: pg, s3: s3.length ? s3 : undefined }
    },
  }

  return [readKv, listTimers, listLocks, streamState, connectorStatus]
}
