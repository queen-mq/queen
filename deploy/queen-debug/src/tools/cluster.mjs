// Is the cluster healthy, and if not, what do you do about it.

import { thin } from '../answer.mjs'
import { BrokerError } from '../broker.mjs'
import { PROBE_KEY, PROBE_NAMESPACE, withQuery } from '../routes.mjs'
import { brokerStatus, cellIssues, hostReading } from '../rules.mjs'
import { freshnessNotes, readTriage } from '../state.mjs'

const RECOVERY = 'https://queenmq.com/manage/recovery/'
const RUNBOOK = {
  start: { title: 'Start with a write', url: `${RECOVERY}#start-with-a-write` },
  replace: { title: 'Replace a node', url: `${RECOVERY}#replace-a-node` },
  majority: { title: 'A majority is down, the disks are fine', url: `${RECOVERY}#a-majority-is-down-the-disks-are-fine` },
  disk: { title: 'A disk is full', url: `${RECOVERY}#a-disk-is-full` },
  skip: { title: 'Every node stopped on the same entry', url: `${RECOVERY}#every-node-stopped-on-the-same-entry` },
}

// The three failures that show only in a node's log.
const LOG_MARKERS = [
  { re: /refused this node's QUEEN_RAFT_TOKEN/, code: 'raft_token_refused', title: 'The node has a different QUEEN_RAFT_TOKEN', action: 'Give it the same token as the others and restart it.', runbook: null },
  { re: /APPLY FAILED/, code: 'apply_failed', title: 'The node stopped on a log entry it cannot apply', action: 'If every node stopped on the same entry, follow the runbook to skip it.', runbook: RUNBOOK.skip },
  { re: /\bFATAL\b/, code: 'fatal', title: 'The node stopped on a fatal error', action: 'If the line names a damaged file, replace the node.', runbook: RUNBOOK.replace },
]

const RECENT_EXIT_MS = 60 * 60 * 1000
const SEVERITY_RANK = { critical: 3, bad: 2, warn: 1, info: 0 }
const byRank = (a, b) => (SEVERITY_RANK[b.severity] ?? 0) - (SEVERITY_RANK[a.severity] ?? 0)

const windowOf = (minutes) => {
  const to = new Date()
  return { from: new Date(to.getTime() - minutes * 60_000).toISOString(), to: to.toISOString() }
}

/** One node's minute: each reading is `{avg, last, min, max}` over its samples. */
function nodePoint(p) {
  const m = p?.metrics ?? {}
  const avg = (v) => (typeof v?.avg === 'number' ? v.avg : null)
  const round = (v, digits = 1) => (v === null ? null : Number(v.toFixed(digits)))
  const user = avg(m.cpu?.user_us)
  const sys = avg(m.cpu?.system_us)
  const rss = avg(m.memory?.rss_bytes)
  const used = avg(m.disk?.used_bytes)
  const total = avg(m.disk?.total_bytes)
  return {
    at: p?.timestamp,
    // Stored as percent x 100 of one core.
    cpuPctOfOneCore: user === null && sys === null ? null : round(((user ?? 0) + (sys ?? 0)) / 100),
    memoryMb: rss === null ? null : Math.round(rss / 1_048_576),
    diskUsedPct: used !== null && total ? round((used / total) * 100) : null,
    connections: (m.connections?.broker?.max ?? 0) + (m.connections?.proxy?.max ?? 0),
    commitsPerSec: round(avg(m.work?.commits_per_sec), 2),
    transactionsPerSec: round(avg(m.work?.transactions_per_sec), 2),
    appliedLagMax: m.raft?.applied_lag?.max ?? null,
  }
}

const majority = (voters) => Math.floor(voters / 2) + 1
const isDown = (n) => !n.reachable

function exitFinding(n, now = Date.now()) {
  const exit = n.pod?.lastExit
  if (!exit) return null
  const at = exit.at ? ` at ${exit.at}` : ''
  if (exit.code === 75) {
    return n.reachable
      ? { severity: 'info', code: 'snapshot_restart', node: n.name, title: `${n.name} restarted to load a snapshot`, evidence: `Exit code 75${at}.`, action: 'Nothing to do.' }
      : null
  }
  // A pod keeps its last exit for as long as it runs. A node that came back and
  // is healthy needs nothing; a recent crash is still worth a look.
  if (n.ready && exit.code !== 0) {
    const ended = Date.parse(exit.at ?? '')
    const recent = Number.isFinite(ended) && now - ended < RECENT_EXIT_MS
    return {
      severity: recent ? 'warn' : 'info',
      code: 'restarted',
      node: n.name,
      title: `${n.name} restarted and is healthy again`,
      evidence: `Last exit code ${exit.code} (${exit.reason ?? 'Error'})${at}, ${n.pod.restarts} restarts.`,
      action: recent ? 'If it happens again, read the log of the run that died.' : 'Nothing to do.',
      next: recent ? { tool: 'node_logs', args: { node: n.name, previous: true } } : undefined,
    }
  }
  // Only Kubernetes' own word: exit 137 is any SIGKILL, not only the memory limit.
  if (exit.reason === 'OOMKilled') {
    return { severity: 'bad', code: 'out_of_memory', node: n.name, title: `${n.name} was killed for memory`, evidence: `Exit code ${exit.code} (OOMKilled)${at}, ${n.pod.restarts} restarts.`, action: 'Check the memory limit and what grew before the kill.', next: { tool: 'node_logs', args: { node: n.name, previous: true } } }
  }
  if (exit.code === 139) {
    return { severity: 'bad', code: 'node_crashed', node: n.name, title: `${n.name} crashed with no message`, evidence: `Exit code 139${at}, ${n.pod.restarts} restarts.`, action: 'Replace the node.', runbook: RUNBOOK.replace, next: { tool: 'node_logs', args: { node: n.name, previous: true } } }
  }
  // 143 is a stop somebody asked for, and node_down already says it is away.
  if (exit.code !== 0 && exit.code !== 143 && !n.reachable) {
    const how = exit.code === 137 ? 'killed by SIGKILL' : exit.reason ?? 'Error'
    return { severity: 'bad', code: 'node_exited', node: n.name, title: `${n.name} exited with code ${exit.code}`, evidence: `${how}${at}, ${n.pod.restarts} restarts.`, action: 'Read the log of the run that died.', next: { tool: 'node_logs', args: { node: n.name, previous: true } } }
  }
  return null
}

async function logFindings(cluster, sick) {
  const kube = cluster.kube
  if (!kube) return []
  const out = []
  await Promise.all(sick.slice(0, 3).map(async (n) => {
    const reads = [{ previous: true }, { previous: false }]
    for (const opts of reads) {
      let text
      try {
        text = await kube.logs(n.pod.name, { container: n.pod.container, tailLines: 200, ...opts })
      } catch {
        continue
      }
      const lines = text.split('\n')
      for (const m of LOG_MARKERS) {
        const line = lines.findLast((l) => m.re.test(l))
        if (!line) continue
        out.push({ severity: 'bad', code: m.code, node: n.name, title: `${n.name}: ${m.title}`, evidence: 'Found in its log.', action: m.action, runbook: m.runbook ?? undefined, next: { tool: 'node_logs', args: { node: n.name, previous: opts.previous, grep: m.re.source.replace(/\\b/g, '') } } })
        return
      }
    }
  }))
  return out
}

function nodeLine(n) {
  const h = hostReading({ host: n.hostStats })
  return {
    name: n.name,
    nodeId: n.nodeId ?? undefined,
    state: n.reachable ? n.state : 'unreachable',
    ready: n.ready,
    term: n.term ?? undefined,
    applied: n.applied ?? undefined,
    commit: n.commit ?? undefined,
    lagMs: n.lagMs ?? undefined,
    behindEntries: n.lagEntries ?? undefined,
    version: n.version ?? undefined,
    cpu: h.cpu.share === null ? undefined : Number(h.cpu.share.toFixed(3)),
    memory: h.mem.share === null ? undefined : Number(h.mem.share.toFixed(3)),
    diskUsedPct: h.disk.usedPct === null ? undefined : Number(h.disk.usedPct.toFixed(1)),
    writesRefused: h.disk.refused || undefined,
    pod: n.pod ? { phase: n.pod.phase, ready: n.pod.ready, restarts: n.pod.restarts, waiting: n.pod.waiting ?? undefined, lastExit: n.pod.lastExit ?? undefined } : undefined,
  }
}

async function clusterFindings(cluster, view) {
  const { nodes, voters, members } = view
  const findings = []
  const up = nodes.filter((n) => n.reachable).length
  const anyReady = nodes.some((n) => n.ready)

  if (up === 0) {
    findings.push({
      severity: 'critical', code: 'no_broker_answers', title: 'No broker answers',
      evidence: `0 of ${nodes.length} nodes reached.`,
      action: 'Read why the nodes are down, then bring them back.',
      runbook: RUNBOOK.majority,
      next: nodes[0]?.pod ? { tool: 'node_logs', args: { node: nodes[0].name, previous: true } } : undefined,
    })
  } else if (!anyReady) {
    const failed = nodes.filter((n) => n.reachable && n.applyFailure)
    if (failed.length && failed.length === up) {
      findings.push({
        severity: 'critical', code: 'apply_stopped_everywhere', title: 'Every node stopped on the same log entry',
        evidence: failed.map((n) => `${n.name}: ${String(n.applyFailure).slice(0, 160)}`).join(' | '),
        action: 'Follow the runbook to skip the entry. A person must decide this.',
        runbook: RUNBOOK.skip,
      })
    } else if (up < majority(voters)) {
      findings.push({
        severity: 'critical', code: 'no_quorum', title: 'A majority of the nodes is down',
        evidence: `${up} of ${voters} voters answer, ${majority(voters)} are needed. Nothing can commit. Down: ${nodes.filter(isDown).map((n) => n.name).join(', ')}.`,
        action: 'Bring the down nodes back on their disks. Never start them empty.',
        runbook: RUNBOOK.majority,
      })
    } else {
      findings.push({
        severity: 'critical', code: 'no_ready_node', title: 'No node reports a working leader',
        evidence: `${up} of ${voters} voters answer, none is ready. An election may be running.`,
        action: 'Look again in a few seconds. If it stays, test a write and read the nodes\' logs.',
        runbook: RUNBOOK.start,
      })
    }
  }

  // The console's own findings on each node: disk, memory, CPU, lag, heartbeat.
  const status = members ? brokerStatus(members) : null
  if (status) {
    for (const issue of cellIssues(status)) {
      if (issue.key === 'cell:quorum' && findings.some((f) => f.severity === 'critical')) continue
      const disk = issue.kind === 'disk'
      findings.push({
        severity: issue.sev,
        code: issue.kind ?? 'quorum',
        node: issue.name,
        title: issue.title,
        evidence: issue.why,
        action: disk ? 'Grow the disk, or free space on it.' : undefined,
        runbook: disk ? RUNBOOK.disk : undefined,
      })
    }
  }

  for (const n of nodes) {
    if (n.reachable && n.applyFailure && anyReady) {
      findings.push({ severity: 'bad', code: 'apply_stopped', node: n.name, title: `${n.name} stopped applying the log`, evidence: String(n.applyFailure).slice(0, 200), action: 'Replace the node, starting with its removal.', runbook: RUNBOOK.replace })
    }
    if (n.reachable && !n.ready && anyReady && !n.applyFailure) {
      findings.push({ severity: 'warn', code: 'node_not_ready', node: n.name, title: `${n.name} is not ready`, evidence: `State ${n.state}, applied ${n.applied ?? '?'} of ${n.commit ?? '?'}. Kubernetes sends it no traffic.`, action: 'If `applied` stays behind, replace the node.', runbook: RUNBOOK.replace })
    }
    if (isDown(n) && anyReady) {
      findings.push({ severity: 'bad', code: 'node_down', node: n.name, title: `${n.name} is down`, evidence: n.pod ? `Pod ${n.pod.phase}${n.pod.waiting ? `, ${n.pod.waiting}` : ''}, ${n.pod.restarts} restarts.` : 'It does not answer.', action: 'The others still commit. Read why it stopped.', next: n.pod ? { tool: 'node_logs', args: { node: n.name, previous: true } } : undefined })
    }
    const e = exitFinding(n)
    if (e) findings.push(e)
  }

  const versions = [...new Set(nodes.map((n) => n.version).filter(Boolean))]
  if (versions.length > 1) {
    findings.push({ severity: 'warn', code: 'mixed_versions', title: 'The nodes run different versions', evidence: versions.join(', '), action: 'Normal during a rolling upgrade. Otherwise finish it.' })
  }

  // Only a node that is unwell now: an old crash line must not raise an alarm.
  const sick = nodes.filter((n) => n.pod && (!n.reachable || !n.ready))
  findings.push(...await logFindings(cluster, sick))
  return findings
}

export function clusterTools({ config, cluster, guard }) {
  const diagnose = {
    name: 'diagnose',
    title: 'Diagnose the cluster',
    description: 'Start here. Says whether the cluster is healthy, lists what is wrong, worst first, with the evidence, the recovery step and the next tool to call. Works with nodes down and with no quorum.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    async run() {
      const view = await cluster.view()
      const findings = await clusterFindings(cluster, view)
      const notes = []
      if (view.raftRoutesRefused) notes.push('This token cannot read the raft routes, so node detail comes from /health only.')
      if (view.podsError) notes.push(`Kubernetes could not be read: ${view.podsError}`)
      if (!cluster.kube) notes.push('Not running in Kubernetes: no pod state, exit codes or logs.')

      let queues = null
      if (view.nodes.some((n) => n.reachable)) {
        try {
          const t = await readTriage(cluster)
          const flagged = t.rows.filter((r) => r.sev)
          for (const r of flagged.slice(0, 5)) {
            const why = { lag: 'A consumer group is behind', acks: 'Acks are failing', noReader: 'It holds messages nobody reads', feedDown: 'The feed stream reading it is down' }[r.reason] ?? 'It needs attention'
            const acks = r.reason === 'acks' && r.ops ? `, ${r.ops.ackFailed} of ${r.ops.ackFailed + r.ops.ackSuccess} acks failed in the last hour` : ''
            findings.push({ severity: r.sev, code: `queue_${r.reason}`, queue: r.name, title: `${r.name}: ${why}`, evidence: `${r.pending ?? '?'} pending${r.lag ? `, oldest ${r.lag}s behind` : ''}${acks}.`, next: { tool: 'why_stuck', args: { queue: r.name } } })
          }
          queues = { total: t.rows.length, needingAttention: flagged.length, shown: Math.min(flagged.length, 5) }
          notes.push(...t.notes, ...freshnessNotes(t.source))
        } catch (e) {
          if (!(e instanceof BrokerError)) throw e
          notes.push(`Queues could not be read: ${e.message}`)
        }
      }

      findings.sort(byRank)
      const worst = findings.find((f) => f.severity !== 'info')?.severity
      const verdict = worst === 'critical' ? 'critical' : worst ? 'degraded' : 'healthy'
      const up = view.nodes.filter((n) => n.reachable).length
      return {
        verdict,
        summary: `${up} of ${view.voters} nodes answer. ${findings.filter((f) => f.severity !== 'info').length} problems found.`,
        findings,
        nodes: view.nodes.map(nodeLine),
        queues: queues ?? undefined,
        notes: notes.length ? notes : undefined,
      }
    },
  }

  const nodeDetail = {
    name: 'node_detail',
    title: 'One node in detail',
    description: 'One node: raft state, indexes, lag, CPU, memory, disk, store, version, and in Kubernetes its pod, restarts, last exit code and recent events.',
    inputSchema: {
      type: 'object',
      properties: { node: { type: 'string', description: 'Pod name, hostname, raft node id or address.' } },
      required: ['node'],
      additionalProperties: false,
    },
    async run({ node }) {
      const { node: n, nodes } = await cluster.findNode(node)
      if (!n) return { error: 'not_found', message: `No node "${node}".`, nodes: nodes.map((x) => x.name) }
      let events
      if (cluster.kube && n.pod) {
        try {
          events = await cluster.kube.events(n.pod.name)
        } catch (e) {
          events = { error: e.message }
        }
      }
      const h = hostReading({ host: n.hostStats })
      return {
        ...nodeLine(n),
        address: n.httpAddr ?? undefined,
        leaderId: n.leaderId ?? undefined,
        quorumAckMs: n.quorumAckMs ?? undefined,
        heartbeatAgeMs: n.heartbeatAgeMs ?? undefined,
        uptimeSeconds: n.uptimeSeconds ?? undefined,
        peersReachIt: n.peersReach,
        peerError: n.peerError ?? undefined,
        applyFailure: n.applyFailure ?? undefined,
        applySkipped: n.applySkipped ?? undefined,
        host: n.hostStats ? { cpu: h.cpu, memory: h.mem, disk: h.disk } : undefined,
        store: n.store ?? undefined,
        log: n.log ?? undefined,
        work: n.work ?? undefined,
        podDetail: n.pod ?? undefined,
        events,
      }
    },
  }

  const nodeLogs = {
    name: 'node_logs',
    title: 'A node\'s logs',
    description: 'A broker pod\'s log, newest last, in Kubernetes. Set previous to read the run before the last crash. Numbers, ids and emails are hidden unless the operator turned on message reading.',
    inputSchema: {
      type: 'object',
      properties: {
        node: { type: 'string', description: 'Pod name, hostname, raft node id or address.' },
        previous: { type: 'boolean', description: 'The run before the last restart.' },
        level: { type: 'string', enum: ['all', 'warn', 'error'], description: 'Keep only this level and worse. Default all.' },
        grep: { type: 'string', description: 'Keep only lines holding this text, in any case.' },
        tail: { type: 'integer', minimum: 1, maximum: 2000, description: 'Lines to read from the end. Default 200.' },
        sinceMinutes: { type: 'integer', minimum: 1, maximum: 10080, description: 'Only the last N minutes.' },
      },
      required: ['node'],
      additionalProperties: false,
    },
    async run({ node, previous = false, level = 'all', grep, tail = 200, sinceMinutes }) {
      if (!cluster.kube) {
        return {
          unavailable: 'not_in_kubernetes',
          message: 'This server reads logs through Kubernetes. Run one of these yourself on the machine that hosts the node.',
          commands: [`docker logs --tail ${tail} <container>`, `kubectl logs ${previous ? '--previous ' : ''}--tail ${tail} <pod>`],
        }
      }
      const { node: n, nodes } = await cluster.findNode(node)
      if (!n?.pod) return { error: 'not_found', message: `No pod for node "${node}".`, nodes: nodes.map((x) => x.name) }
      const text = await cluster.kube.logs(n.pod.name, { container: n.pod.container, previous, tailLines: tail, sinceSeconds: sinceMinutes ? sinceMinutes * 60 : undefined })
      let lines = text.split('\n').filter(Boolean)
      const read = lines.length
      if (level !== 'all') {
        const re = level === 'error' ? /\b(ERROR|FATAL|PANIC)\b/i : /\b(WARN|WARNING|ERROR|FATAL|PANIC)\b/i
        lines = lines.filter((l) => re.test(l))
      }
      if (grep) {
        const wanted = grep.toLowerCase()
        lines = lines.filter((l) => l.toLowerCase().includes(wanted))
      }
      return { node: n.name, previous, linesRead: read, linesKept: lines.length, lines: lines.map((l) => guard.text(l)) }
    },
  }

  const clusterMetrics = {
    name: 'cluster_metrics',
    title: 'Cluster metrics over time',
    description: 'Work and resources over a time window: pushes, pops, acks, failures and dead letters for the cluster, plus each node\'s CPU, memory and connections.',
    inputSchema: {
      type: 'object',
      properties: { minutes: { type: 'integer', minimum: 1, maximum: 10080, description: 'How far back. Default 60.' } },
      additionalProperties: false,
    },
    async run({ minutes = 60 }) {
      const range = windowOf(minutes)
      const [system, worker] = await Promise.all([
        cluster.read(withQuery('/api/v1/analytics/system-metrics', range), { timeoutMs: 10_000 }),
        cluster.read(withQuery('/api/v1/analytics/worker-metrics', range), { timeoutMs: 10_000 }),
      ])
      const pointFields = ['bucket', 'timestamp', 'ackPerSecond', 'ackFailed', 'pushPerSecond', 'popPerSecond', 'dlqCount', 'avgLagMs', 'maxLagMs', 'avgEventLoopLagMs', 'dbErrors']
      const pick = (p) => Object.fromEntries(pointFields.filter((f) => p?.[f] !== undefined && p?.[f] !== null).map((f) => [f, p[f]]))
      // pendingMessages here is pushes minus acks in the window, which goes
      // negative after a backlog: not how many messages are pending.
      const { pendingMessages: _windowDifference, ...totals } = worker.data?.summary ?? {}
      const oldestFirst = [...(worker.data?.timeSeries || [])].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)))
      return {
        timeRange: worker.data?.timeRange,
        bucketMinutes: worker.data?.bucketMinutes,
        totals,
        busiestQueues: (worker.data?.queues || []).slice().sort((a, b) => (b.popCount ?? 0) - (a.popCount ?? 0)).slice(0, 10),
        series: thin(oldestFirst, 24).map(pick),
        nodes: (system.data?.replicas || []).map((r) => ({ node: r.hostname, points: thin(r.timeSeries || [], 12).map(nodePoint) })),
        nodesMissing: system.data?.nodesMissing?.length ? system.data.nodesMissing : undefined,
        source: worker.source,
        notes: freshnessNotes(worker.source),
      }
    },
  }

  const writeProbe = {
    name: 'write_probe',
    title: 'Test a write',
    writes: true,
    description: 'The one tool that writes. Puts a small key that expires in 60 s through two nodes, to answer whether the cluster can still commit. An answer within a second means it commits.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    async run() {
      const { nodes } = await cluster.health({ fresh: true })
      const targets = nodes.filter((n) => n.reachable).slice(0, 2)
      if (!targets.length) return { commits: false, message: 'No node answers.' }
      const results = await Promise.all(targets.map(async (n) => {
        const started = Date.now()
        try {
          await cluster.put(n, `/api/v1/kv/${PROBE_NAMESPACE}/${PROBE_KEY}`, { value: 'probe', ttlSeconds: 60 }, 10_000)
          return { node: n.name, committed: true, ms: Date.now() - started }
        } catch (e) {
          if (!(e instanceof BrokerError)) throw e
          return { node: n.name, committed: false, ms: Date.now() - started, error: e.kind }
        }
      }))
      const commits = results.some((r) => r.committed)
      return {
        commits,
        results,
        meaning: commits
          ? 'The cluster commits. If something is wrong, look for one sick node.'
          : 'The cluster does not commit. Find which case it is with diagnose.',
        runbook: RUNBOOK.start,
      }
    },
  }

  return config.allowProbe
    ? [diagnose, nodeDetail, nodeLogs, clusterMetrics, writeProbe]
    : [diagnose, nodeDetail, nodeLogs, clusterMetrics]
}
