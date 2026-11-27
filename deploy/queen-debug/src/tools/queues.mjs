// Which queue is in trouble, which group is behind, and why.

import { thin } from '../answer.mjs'
import { BrokerError } from '../broker.mjs'
import { segment, withQuery } from '../routes.mjs'
import { ackFailureSeverity, byLagInactiveLast, groupAttention, groupPresence, groupState } from '../rules.mjs'
import { freshnessNotes, readGroups, readLines, readTriage } from '../state.mjs'

const STUCK_WINDOW_MINUTES = 15
const CHECK_MS = 2_000
const QUEUE_MODE = '__QUEUE_MODE__'

const windowOf = (minutes) => {
  const to = new Date()
  return { from: new Date(to.getTime() - minutes * 60_000).toISOString(), to: to.toISOString() }
}
const sum = (rows, field) => rows.reduce((total, r) => total + (Number(r?.[field]) || 0), 0)
const groupLabel = (name) => (name === QUEUE_MODE ? 'queue mode (no group)' : name)

function groupRow(g, linesFor) {
  return {
    name: groupLabel(g.name),
    queue: g.queueName,
    attention: groupAttention(g, linesFor),
    state: groupState(g, linesFor),
    brokerState: g.state,
    kind: g.kind,
    behindSeconds: g.maxTimeLag ?? 0,
    behindMessages: g.totalLag ?? 0,
    partitionsBehind: g.partitionsWithLag ?? 0,
    partitions: g.partitionCursors ?? undefined,
    subscriptionMode: g.subscriptionMode ?? undefined,
    filtered: g.filtered || undefined,
    where: g.where ?? undefined,
    presence: groupPresence(g, linesFor) || undefined,
  }
}

function triageRow(r) {
  return {
    name: r.name,
    namespace: r.namespace || undefined,
    severity: r.sev,
    reason: r.reason,
    pending: r.pending,
    processing: r.processing,
    behindSeconds: r.lag,
    pendingChange: r.delta ?? undefined,
    groups: r.groupCount,
    liveReaders: r.readerCount,
    groupsBehind: r.affected.length ? r.affected.map((g) => groupLabel(g.name)) : undefined,
  }
}

async function queueOps(cluster, queue, minutes) {
  try {
    const { data } = await cluster.read(withQuery('/api/v1/analytics/queue-ops', { queue, ...windowOf(minutes) }), { timeoutMs: 8_000 })
    return data
  } catch (e) {
    if (e instanceof BrokerError) return null
    throw e
  }
}

async function groupOnQueue(cluster, name, queue) {
  try {
    const { data } = await cluster.read(`/api/v1/consumer-groups/${segment(name, 'group')}`, { timeoutMs: 10_000 })
    return data?.[queue] ?? null
  } catch (e) {
    if (e instanceof BrokerError) return null
    throw e
  }
}

const consumedBy = (detail) => sum(detail?.partitions ?? [], 'totalConsumed')

/**
 * What held this group back, from the group's own partitions. The queue's
 * counters add up every group, so `moved`, this group's own cursor measured
 * twice, is what says whether it is progressing.
 */
function groupCauses({ g, detail, moved, queueOptions, ops, linesFor }) {
  const causes = []
  const label = groupLabel(g.name)
  const partitions = detail?.partitions ?? []
  const behind = partitions.filter((p) => (p.offsetLag ?? 0) > 0)
  const leased = behind.filter((p) => p.leaseActive)
  const free = behind.filter((p) => !p.leaseActive)
  const oldest = Math.max(0, ...behind.map((p) => p.timeLagSeconds ?? 0))
  const pops = ops ? sum(ops, 'popMessages') : null
  const empties = ops ? sum(ops, 'popEmpty') : null
  const acked = ops ? sum(ops, 'ackSuccess') : null
  const failed = ops ? sum(ops, 'ackFailed') : null
  const checked = moved === null ? '' : ` Nothing was consumed during a ${CHECK_MS / 1000} s check.`

  if (g.kind === 'kafka' && (g.liveMembers === 0 || g.state === 'Empty')) {
    causes.push({
      code: 'kafka_group_empty', severity: 'bad', group: label,
      title: `Kafka group ${label} has no live member`,
      evidence: groupPresence(g, linesFor) || 'No member is subscribed to this queue.',
      action: 'Start the consumers. A Kafka group takes no lease, so nothing else moves it.',
    })
    return causes
  }

  if (moved > 0 && behind.length) {
    causes.push({
      code: 'behind_but_moving', severity: 'warn', group: label,
      title: `${label} is behind and still moving`,
      evidence: `${sum(behind, 'offsetLag')} messages behind, oldest ${oldest}s. ${moved} consumed during a ${CHECK_MS / 1000} s check.`,
      action: 'Add consumers, or spread the work over more partitions: one worker serves one partition at a time.',
    })
  } else {
    if (leased.length) {
      const workers = [...new Set(leased.map((p) => p.workerId).filter(Boolean))]
      causes.push({
        code: 'worker_holds_lease',
        severity: 'bad',
        group: label,
        title: `A worker holds ${leased.length} partition(s) of ${label} and is not finishing`,
        evidence: `Lease time ${queueOptions?.leaseTime ?? '?'}s. Oldest message ${oldest}s old. Workers: ${workers.slice(0, 3).join(', ') || 'unknown'}. Partitions: ${leased.slice(0, 5).map((p) => p.partition).join(', ')}.${checked}`,
        action: 'Check that worker: it is slow, hung or dead. If it died, the messages come back by themselves when the lease ends.',
      })
    }
    if (free.length) {
      const held = `${free.length} partition(s) have messages and no lease.`
      if (empties > 0 && !pops) {
        causes.push({
          code: 'pops_come_back_empty', severity: 'bad', group: label,
          title: `Consumers ask, and get nothing, while ${label} is behind`,
          evidence: `${held} ${empties} empty pops in ${STUCK_WINDOW_MINUTES} min. delayedProcessing ${queueOptions?.delayedProcessing ?? 0}s, windowBuffer ${queueOptions?.windowBuffer ?? 0}s, retryDelay ${queueOptions?.retryDelay ?? 0} ms${g.where ? `, filter: ${g.where}` : ''}.`,
          action: 'Check the group name on the consumer, its filter, and the delay settings of the queue.',
        })
      } else {
        causes.push({
          code: 'nobody_reading', severity: 'bad', group: label,
          title: `Nothing is reading ${label} now`,
          evidence: `${held} Oldest message ${oldest}s old.${checked}${pops === null ? '' : ` This queue saw ${pops} popped by any group in ${STUCK_WINDOW_MINUTES} min.`}`,
          action: 'Check that the consumers of this group are running and can reach the broker.',
        })
      }
    }
  }

  if (failed > 0) {
    const sev = ackFailureSeverity({ failed, succeeded: acked }, linesFor(g.queueName))
    causes.push({
      code: 'acks_failing', severity: sev || 'warn', group: label,
      title: 'Consumers are failing messages on this queue',
      evidence: `${failed} failed and ${acked} successful acks in ${STUCK_WINDOW_MINUTES} min. Retry limit ${queueOptions?.retryLimit ?? '?'}.`,
      action: 'Read why they fail.',
      next: { tool: 'dlq_summary', args: { queue: g.queueName } },
    })
  }
  return causes
}

export function queueTools({ cluster }) {
  const listQueues = {
    name: 'list_queues',
    title: 'List queues, worst first',
    description: 'Every queue with the console\'s verdict, those needing attention first: pending, behind, failing acks or no reader. Also lists ephemeral queues.',
    inputSchema: {
      type: 'object',
      properties: {
        filter: { type: 'string', enum: ['all', 'attention'], description: 'attention keeps only queues with a problem. Default all.' },
        namespace: { type: 'string', description: 'Only this namespace.' },
        search: { type: 'string', description: 'Only names holding this text.' },
        limit: { type: 'integer', minimum: 1, maximum: 500, description: 'Default 50.' },
      },
      additionalProperties: false,
    },
    async run({ filter = 'all', namespace, search, limit = 50 }) {
      const t = await readTriage(cluster)
      const term = search?.toLowerCase()
      const rows = t.rows.filter((r) => (filter === 'all' || r.sev) && (!namespace || r.namespace === namespace) && (!term || r.name.toLowerCase().includes(term)))
      let ephemeral
      try {
        const { data } = await cluster.read('/api/v1/ephemeral/queues', { timeoutMs: 4_000 })
        if (data?.queues?.length) ephemeral = { count: data.count, cellBytes: data.cellBytes, queues: data.queues.slice(0, 20) }
      } catch (e) {
        if (!(e instanceof BrokerError)) throw e
      }
      return {
        total: t.rows.length,
        matching: rows.length,
        queues: rows.slice(0, limit).map(triageRow),
        ephemeral,
        source: t.source,
        notes: [...t.notes, ...freshnessNotes(t.source)],
      }
    },
  }

  const queueDetail = {
    name: 'queue_detail',
    title: 'One queue in detail',
    description: 'One queue: settings, message counts, its groups, and its partitions with the most pending first.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        partitions: { type: 'integer', minimum: 0, maximum: 200, description: 'Partitions to list. Default 20.' },
      },
      required: ['queue'],
      additionalProperties: false,
    },
    async run({ queue, partitions = 20 }) {
      const path = withQuery(`/api/v1/status/queues/${segment(queue, 'queue')}`, partitions === 0 ? { includePartitions: 'false' } : {})
      const [{ data, source }, groupsRead, lines] = await Promise.all([
        cluster.read(path, { timeoutMs: 10_000 }),
        readGroups(cluster),
        readLines(cluster),
      ])
      const rows = (data.partitions || []).slice().sort((a, b) => (Number(b.messages?.pending ?? b.pending) || 0) - (Number(a.messages?.pending ?? a.pending) || 0))
      return {
        queue: data.queue,
        totals: data.totals,
        groups: groupsRead.groups.filter((g) => g.queueName === queue).map((g) => groupRow(g, lines.linesFor)),
        partitionCount: data.totals?.partitions ?? rows.length,
        partitions: rows.slice(0, partitions),
        partitionsCut: data.partitionsTruncated || undefined,
        statsAge: data.statsAge ?? undefined,
        source,
        notes: [...lines.notes, ...freshnessNotes(source)],
      }
    },
  }

  const queueHistory = {
    name: 'queue_history',
    title: 'A queue over time',
    description: 'Pushes, pops, acks, failures, backlog and lag over a time window, for one queue or all of them. Use it to find when a problem started.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string', description: 'Leave out for all queues.' },
        minutes: { type: 'integer', minimum: 1, maximum: 10080, description: 'How far back. Default 60.' },
      },
      additionalProperties: false,
    },
    async run({ queue, minutes = 60 }) {
      const { data, source } = await cluster.read(withQuery('/api/v1/analytics/queue-ops', { queue, ...windowOf(minutes) }), { timeoutMs: 10_000 })
      const series = data?.series || []
      const fields = ['pushMessages', 'popMessages', 'ackSuccess', 'ackFailed', 'popEmpty', 'fetchMessages', 'transactions']
      const totals = Object.fromEntries(fields.map((f) => [f, sum(series, f)]))
      totals.maxLagMs = series.reduce((m, r) => Math.max(m, Number(r.maxLagMs) || 0), 0)

      // One point per bucket, added up across queues when several match.
      const byBucket = new Map()
      for (const r of series) {
        const row = byBucket.get(r.bucket) ?? { bucket: r.bucket, push: 0, pop: 0, ackOk: 0, ackFailed: 0, maxLagMs: 0 }
        row.push += Number(r.pushMessages) || 0
        row.pop += Number(r.popMessages) || 0
        row.ackOk += Number(r.ackSuccess) || 0
        row.ackFailed += Number(r.ackFailed) || 0
        row.maxLagMs = Math.max(row.maxLagMs, Number(r.maxLagMs) || 0)
        byBucket.set(r.bucket, row)
      }
      const points = [...byBucket.values()].sort((a, b) => String(a.bucket).localeCompare(String(b.bucket)))
      return {
        queue: queue ?? 'all',
        timeRange: data?.timeRange,
        bucketMinutes: data?.bucketMinutes,
        totals,
        series: thin(points, 30),
        backlog: thin(data?.backlog || [], 30),
        nodesMissing: data?.nodesMissing?.length ? data.nodesMissing : undefined,
        source,
        notes: freshnessNotes(source),
      }
    },
  }

  const listGroups = {
    name: 'list_groups',
    title: 'List consumer groups',
    description: 'Every consumer group with how far behind it is and the console\'s verdict, the furthest behind first.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string', description: 'Only groups reading this queue.' },
        filter: { type: 'string', enum: ['all', 'attention'], description: 'attention keeps only groups with a problem. Default all.' },
        limit: { type: 'integer', minimum: 1, maximum: 500, description: 'Default 50.' },
      },
      additionalProperties: false,
    },
    async run({ queue, filter = 'all', limit = 50 }) {
      const [{ groups, source }, lines] = await Promise.all([readGroups(cluster), readLines(cluster)])
      const mine = groups.filter((g) => !queue || g.queueName === queue)
      const sorted = byLagInactiveLast(mine, lines.linesFor)
      const rows = sorted.map((g) => groupRow(g, lines.linesFor)).filter((r) => filter === 'all' || ['bad', 'warn'].includes(r.attention))
      return { total: mine.length, matching: rows.length, groups: rows.slice(0, limit), source, notes: [...lines.notes, ...freshnessNotes(source)] }
    },
  }

  const groupDetail = {
    name: 'group_detail',
    title: 'One consumer group in detail',
    description: 'One group, per queue and partition: messages behind, seconds behind, whether a lease is active, and which worker holds it. The partitions furthest behind come first.',
    inputSchema: {
      type: 'object',
      properties: {
        group: { type: 'string' },
        queue: { type: 'string', description: 'Only this queue.' },
        limit: { type: 'integer', minimum: 1, maximum: 500, description: 'Partitions per queue. Default 30.' },
      },
      required: ['group'],
      additionalProperties: false,
    },
    async run({ group, queue, limit = 30 }) {
      const { data, source } = await cluster.read(`/api/v1/consumer-groups/${segment(group, 'group')}`, { timeoutMs: 15_000 })
      const queues = Object.entries(data || {})
        .filter(([name]) => !queue || name === queue)
        .map(([name, d]) => {
          const parts = (d.partitions || []).slice().sort((a, b) => (b.timeLagSeconds ?? 0) - (a.timeLagSeconds ?? 0) || (b.offsetLag ?? 0) - (a.offsetLag ?? 0))
          return {
            queue: name,
            kind: d.kind,
            filtered: d.filtered || undefined,
            where: d.where ?? undefined,
            liveMembers: d.liveMembers ?? undefined,
            partitionCount: parts.length,
            behindMessages: sum(parts, 'offsetLag'),
            partitionsLeased: parts.filter((p) => p.leaseActive).length,
            partitions: parts.slice(0, limit).map((p) => ({
              partition: p.partition,
              behindMessages: p.offsetLag,
              behindSeconds: p.timeLagSeconds,
              leaseActive: p.leaseActive,
              worker: p.workerId ?? undefined,
              consumed: p.totalConsumed,
            })),
          }
        })
      return { group, queues, source, notes: freshnessNotes(source) }
    },
  }

  const whyStuck = {
    name: 'why_stuck',
    title: 'Why a queue is not moving',
    description: 'Finds why a queue or consumer group is not making progress: no quorum, no reader, a worker holding a lease, nobody polling, polls coming back empty, or failing messages. Gives the evidence and what to do.',
    inputSchema: {
      type: 'object',
      properties: {
        queue: { type: 'string' },
        group: { type: 'string', description: 'Only this consumer group.' },
      },
      required: ['queue'],
      additionalProperties: false,
    },
    async run({ queue, group }) {
      const quorum = await cluster.hasQuorum()
      const path = withQuery(`/api/v1/status/queues/${segment(queue, 'queue')}`, { includePartitions: 'false' })
      const [{ data: status, source }, triage, opsData] = await Promise.all([
        cluster.read(path, { timeoutMs: 10_000 }),
        readTriage(cluster),
        queueOps(cluster, queue, STUCK_WINDOW_MINUTES),
      ])
      const lines = { linesFor: triage.linesFor, notes: triage.notes }
      const options = status.queue?.config ?? {}
      const counts = status.totals?.messages ?? {}
      const ops = opsData?.series?.filter((r) => r.queueName === queue) ?? null
      const verdict = triage.rows.find((r) => r.name === queue)

      const all = triage.groups.filter((g) => g.queueName === queue)
      const named = group === undefined ? all : all.filter((g) => g.name === group || groupLabel(g.name) === group)
      if (group !== undefined && !named.length) {
        return { error: 'not_found', message: `Group "${group}" does not read ${queue}.`, groups: all.map((g) => groupLabel(g.name)) }
      }

      const causes = []
      if (!quorum) {
        causes.push({
          code: 'no_quorum', severity: 'critical',
          title: 'The cluster has no quorum',
          evidence: 'No node reports a working leader, so pops and acks cannot commit on any queue.',
          action: 'Fix the cluster first.',
          next: { tool: 'diagnose', args: {} },
        })
      }

      // The console's own verdict decides "nobody reads this".
      if (verdict?.reason === 'noReader') {
        causes.push({
          code: 'no_reader', severity: verdict.sev,
          title: 'No consumer group reads this queue',
          evidence: `${verdict.pending ?? counts.pending} messages pending${verdict.deadOnly ? ', and its only cursor has never consumed' : ''}.`,
          action: 'Start a consumer. A new group skips older messages unless it subscribes with mode all.',
        })
      } else if (verdict?.reason === 'feedDown') {
        causes.push({
          code: 'feed_stream_down', severity: verdict.sev,
          title: 'The feed stream that reads this queue is down',
          evidence: `${verdict.pending ?? counts.pending} messages pending, and no feed stream on it is running.`,
          action: 'Restart the stream.',
          next: { tool: 'stream_state', args: {} },
        })
      }

      const first = await Promise.all(named.map((g) => groupOnQueue(cluster, g.name, queue)))
      let second = first
      if (quorum && first.some((d) => d?.partitions?.some((p) => (p.offsetLag ?? 0) > 0))) {
        await new Promise((resolve) => setTimeout(resolve, CHECK_MS))
        second = await Promise.all(named.map((g) => groupOnQueue(cluster, g.name, queue)))
      }
      named.forEach((g, i) => {
        const moved = first[i] && second[i] && second !== first ? consumedBy(second[i]) - consumedBy(first[i]) : null
        causes.push(...groupCauses({ g, detail: second[i] ?? first[i], moved, queueOptions: options, ops, linesFor: lines.linesFor }))
      })

      if ((options.delayedProcessing ?? 0) > 0 || (options.windowBuffer ?? 0) > 0) {
        causes.push({
          code: 'delivery_is_delayed', severity: 'info',
          title: 'This queue holds messages back on purpose',
          evidence: `delayedProcessing ${options.delayedProcessing ?? 0}s, windowBuffer ${options.windowBuffer ?? 0}s.`,
        })
      }
      if ((counts.deadLetter ?? 0) > 0 && !causes.some((c) => c.code === 'acks_failing')) {
        causes.push({
          code: 'dead_letters', severity: 'info',
          title: `${counts.deadLetter} messages are in the dead-letter queue`,
          evidence: 'They left the queue after their last retry.',
          next: { tool: 'dlq_summary', args: { queue } },
        })
      }

      const rank = { critical: 3, bad: 2, warn: 1, info: 0 }
      causes.sort((a, b) => (rank[b.severity] ?? 0) - (rank[a.severity] ?? 0))
      const moving = !causes.some((c) => c.severity !== 'info')
      return {
        queue,
        verdict: moving ? 'not_stuck' : causes[0].code,
        summary: moving ? 'Nothing is holding this queue back.' : causes[0].title,
        causes,
        messages: counts,
        settings: {
          leaseTime: options.leaseTime, retryLimit: options.retryLimit, retryDelay: options.retryDelay,
          delayedProcessing: options.delayedProcessing, windowBuffer: options.windowBuffer,
        },
        groups: named.map((g) => groupRow(g, lines.linesFor)),
        lastMinutes: ops ? {
          minutes: STUCK_WINDOW_MINUTES,
          pushed: sum(ops, 'pushMessages'), popped: sum(ops, 'popMessages'), emptyPops: sum(ops, 'popEmpty'),
          acked: sum(ops, 'ackSuccess'), failed: sum(ops, 'ackFailed'),
        } : undefined,
        source,
        notes: [...lines.notes, ...freshnessNotes(source)],
      }
    },
  }

  return [whyStuck, listQueues, queueDetail, queueHistory, listGroups, groupDetail]
}
