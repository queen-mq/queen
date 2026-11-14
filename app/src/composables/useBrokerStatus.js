// The Overview's broker strip: the machine under each node of the cell and the
// replicated log across them, from the read System's Raft source already makes
// — GET /api/v1/raft/members, whose members carry `host`
// (server/src/rsm/facade/real/phase2/admin.rs host_json):
//
//   cpuPct, cpuWindowSeconds  the process's CPU over the metrics collector's
//                             last interval, percent of ONE core (4 busy = 400)
//   cpus                      the CPUs it may use (affinity, cgroup quota)
//   rssBytes, memLimitBytes   resident memory, and the cgroup's limit or the RAM
//   anonBytes                 the part of it the process holds itself: the rest
//                             is file pages it maps (the store's LMDB file is
//                             read whole at boot), page cache the kernel drops
//                             before it kills anything. The meter shows this when
//                             the broker reports it (2.0.1), else the resident set
//   disk                      the data filesystem: usedPct, usedBytes,
//                             totalBytes, and the node's write gate (gate,
//                             highPct, lowPct, writesRefused)
//
// and `work` (dashboard/mod.rs work_json): commitsPerSec, commandsPerSec,
// transactionsPerSec over the collector's last interval (windowSeconds), and
// the connections open now on each listener (broker, proxy).
//
// A member nobody could reach, or a broker older than `host`, has none of it:
// its readings are null, which the strip says as "—", never as idle. A measure
// is summed up by its fullest node, the one that runs out first; the meters
// show every node. Pure, like useRaftCluster: test/broker-status.test.js.

import { clusterAlert, clusterView } from './useRaftCluster.js'
import {
  SEV_BAD, SEV_NONE, SEV_WARN,
  cpuShareSeverity, diskSeverity, memoryShareSeverity,
} from './useSeverity.js'
import { formatRate, memberTraffic } from './useTraffic.js'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

const RANK = { [SEV_NONE]: 0, [SEV_WARN]: 1, [SEV_BAD]: 2 }
const worst = (a, b) => ((RANK[b] || 0) > (RANK[a] || 0) ? b : a)

/** One node's readings. A share is 0..1 of what the node may use; null = not known. */
export function hostReading(member) {
  const h = (member && member.host) || {}
  const cpus = num(h.cpus)
  const cpuPct = num(h.cpuPct)
  const cpuShare = cpuPct !== null && cpus ? cpuPct / (cpus * 100) : null
  const rss = num(h.rssBytes)
  const anon = num(h.anonBytes)
  const held = anon ?? rss
  const limit = num(h.memLimitBytes)
  const memShare = held !== null && limit ? held / limit : null
  const d = h.disk && typeof h.disk === 'object' ? h.disk : null
  const disk = {
    usedPct: d ? num(d.usedPct) : null,
    usedBytes: d ? num(d.usedBytes) : null,
    totalBytes: d ? num(d.totalBytes) : null,
    gate: d ? d.gate !== false : false,
    highPct: d ? num(d.highPct) : null,
    lowPct: d ? num(d.lowPct) : null,
    refused: d ? d.writesRefused === true : false,
  }
  disk.share = disk.usedPct === null ? null : disk.usedPct / 100
  disk.sev = diskSeverity(disk)
  return {
    cpu: { pct: cpuPct, cpus, share: cpuShare, windowSeconds: num(h.cpuWindowSeconds), sev: cpuShareSeverity(cpuShare) },
    mem: { held, rss, anon, limit, share: memShare, sev: memoryShareSeverity(memShare) },
    disk,
  }
}

/** The fullest node's reading of one measure, the worst tone, and every node's for the meters. */
function summarise(nodes, key) {
  let top = null
  for (const n of nodes) {
    if (n[key].share === null) continue
    if (!top || n[key].share > top[key].share) top = n
  }
  return {
    top: top ? { name: top.name, ...top[key] } : null,
    sev: nodes.reduce((s, n) => worst(s, n[key].sev), SEV_NONE),
    per: nodes.map((n) => ({ name: n.name, unreachable: n.unreachable, ...n[key] })),
  }
}

/**
 * One node's work: its log's commits, the commands it planned and the
 * transactions it committed, a second over the collector's last interval, and
 * the connections open on it now. Null for a node nobody reached, or a broker
 * older than `work`; a rate is null until the node's first interval closes.
 */
export function memberWork(member) {
  const w = member && member.work
  if (!w || typeof w !== 'object') return null
  const raw = w.connections && typeof w.connections === 'object' ? w.connections : {}
  const byListener = {}
  for (const [k, v] of Object.entries(raw)) if (num(v) !== null) byListener[k] = num(v)
  const open = Object.values(byListener)
  return {
    windowSeconds: num(w.windowSeconds),
    commits: num(w.commitsPerSec),
    commands: num(w.commandsPerSec),
    transactions: num(w.transactionsPerSec),
    connections: open.length ? open.reduce((a, b) => a + b, 0) : null,
    byListener,
  }
}

const sumOrNull = (vs) => (vs.length ? vs.reduce((a, b) => a + b, 0) : null)

/**
 * The cell's work from its nodes'. Every node sees every commit, so the
 * commits are the node that saw the most; only a leader plans, so the
 * commands are added; transactions and connections are each node's own,
 * added. Null when no node says its work.
 */
export function cellWork(nodes) {
  const live = nodes.filter((n) => n.work)
  if (!live.length) return null
  const of = (k) => live.map((n) => n.work[k]).filter((v) => v !== null)
  const commits = of('commits')
  const commands = sumOrNull(of('commands'))
  const top = commits.length ? Math.max(...commits) : null
  const byListener = {}
  for (const n of live) for (const [k, v] of Object.entries(n.work.byListener)) byListener[k] = (byListener[k] || 0) + v
  const windows = of('windowSeconds')
  return {
    commits: top,
    commands,
    perCommit: top && commands !== null ? commands / top : null,
    transactions: sumOrNull(of('transactions')),
    connections: sumOrNull(of('connections')),
    byListener,
    nodes: live.length,
    windowSeconds: windows.length ? Math.max(...windows) : null,
  }
}

/** A rate a second, short: "0", "0.25", "2.5", "18", "1,240". */
export function perSecond(v) {
  const x = num(v)
  if (x === null) return '–'
  const a = Math.abs(x)
  if (a === 0) return '0'
  if (a >= 10) return Math.round(x).toLocaleString('en-US')
  return String(parseFloat(x.toFixed(a >= 1 ? 1 : 2)))
}

const byNodeId = (a, b) => (num(a?.nodeId) ?? Infinity) - (num(b?.nodeId) ?? Infinity)

/**
 * GET /api/v1/raft/members → the strip's model, or null when the payload is
 * not one. Nodes are in node-id order, like System's members table.
 */
export function brokerStatus(payload) {
  const view = clusterView(payload)
  if (!view) return null
  // clusterView sorts its rows the same way, so row i is member i.
  const members = payload.members.filter(Boolean).slice().sort(byNodeId)
  const nodes = view.rows.map((row, i) => ({
    name: row.hostname || (row.nodeId !== null ? `node-${row.nodeId}` : `node ${i + 1}`),
    nodeId: row.nodeId,
    state: row.state,
    role: row.role,
    isLeader: row.isLeader,
    unreachable: row.unreachable,
    lag: row.lag,
    lagSeverity: row.lagSeverity,
    heartbeatMs: row.heartbeatMs,
    heartbeatSeverity: row.heartbeatSeverity,
    // What it moved over the metrics collector's last interval (useTraffic.js).
    traffic: row.unreachable ? null : memberTraffic(members[i]),
    work: row.unreachable ? null : memberWork(members[i]),
    ...hostReading(members[i]),
  }))

  let lagMax = null
  let raftSev = view.quorum.severity || SEV_NONE
  for (const row of view.rows) {
    if (row.lag !== null) lagMax = Math.max(lagMax ?? 0, row.lag)
    raftSev = worst(worst(raftSev, row.lagSeverity), row.heartbeatSeverity)
  }
  const raft = {
    single: view.singleNode,
    voters: view.quorum.voters,
    up: view.quorum.up,
    hasQuorum: view.quorum.hasQuorum,
    leader: view.leader ? nodes[view.rows.indexOf(view.leader)]?.name || null : null,
    term: view.term,
    lagMax,
    sev: raftSev,
    alert: clusterAlert(view),
    // Bytes per second on the Raft port's wire: every node's sent, summed (a
    // byte is sent once). null when no node reports its traffic.
    wire: nodes.reduce((sum, n) => (n.traffic?.sent == null ? sum : (sum ?? 0) + n.traffic.sent), null),
  }

  const cpu = summarise(nodes, 'cpu')
  const mem = summarise(nodes, 'mem')
  const disk = summarise(nodes, 'disk')
  return {
    nodes,
    cpu,
    mem,
    disk,
    raft,
    work: cellWork(nodes),
    sev: [cpu.sev, mem.sev, disk.sev, raft.sev].reduce(worst, SEV_NONE),
  }
}

const count = (v) => Math.round(v).toLocaleString('en-US')
const share = (s) => `${Math.round(s * 100)}%`

const commitsTitle = (w) =>
  `entries committed to the log a second, over the last ${w.windowSeconds || 60} s` +
  (w.perCommit ? `, ${perSecond(w.perCommit)} commands each` : '') +
  '. One entry is one atomic commit of a batch of commands; every node sees the same.'

/** The band chip's commits a second, or null when no node says it yet. */
export function workChip(work) {
  if (!work || work.commits === null) return null
  return { id: 'commits', label: 'Commits', value: `${perSecond(work.commits)}/s`, tone: '', title: `Commits: ${commitsTitle(work)}` }
}

/**
 * "In this cluster"'s two live rows: the connections open now and the
 * commits a second. None for a broker older than `work`; the commits read
 * "–" until the first interval closes.
 */
export function workRows(work) {
  if (!work) return []
  const rows = []
  if (work.connections !== null) {
    const viaProxy = work.byListener.proxy
    rows.push({
      id: 'connections',
      label: 'Connections',
      value: count(work.connections),
      sub: viaProxy ? `${count(viaProxy)} via the proxy` : `on ${work.nodes} node${work.nodes === 1 ? '' : 's'}`,
      title: "Open now, on every node: its port (apps in the cluster and the other nodes) and the proxy's port (clients from outside)",
      to: '/cluster',
    })
  }
  rows.push({
    id: 'commits',
    label: 'Commits',
    value: perSecond(work.commits),
    sub: 'a second',
    title: commitsTitle(work).replace(/^./, (c) => c.toUpperCase()),
    to: '/cluster',
  })
  return rows
}
const ms = (v) => (v < 1000 ? `${Math.round(v)} ms` : `${(v / 1000).toFixed(1)} s`)

/**
 * What is wrong in the cell, as Overview issues, worst first:
 * `{key, sev, name, title, why, to}`, `title` the headline when it is the worst
 * one. The lines are brokerStatus's own (the top bar colours by them), so the
 * two never disagree; the snapshot rule is the only new one, from the Raft
 * port's traffic. A node nobody reached is the quorum line's, not an issue of
 * its own.
 */
export function cellIssues(status) {
  if (!status) return []
  const out = []
  const add = (key, sev, name, title, why) => {
    if (sev === SEV_WARN || sev === SEV_BAD) out.push({ key: `cell:${key}`, sev, name, title, why, to: '/system', cell: true })
  }
  const alert = status.raft.alert
  if (alert) add('quorum', alert.level, 'Raft', alert.title, `${alert.title}: ${alert.detail}`)

  // A snapshot is a whole store over the network: name the node it goes to,
  // or, when that node does not say (an older build), the one sending it.
  const live = status.nodes.filter((n) => !n.unreachable && n.traffic)
  const window = (n) => (n.traffic.windowSeconds ? `last ${n.traffic.windowSeconds} s` : 'last interval')
  const receiving = live.filter((n) => (n.traffic.snapshot?.received || 0) > 0)
  for (const n of receiving) {
    add(`${n.name}:snapshot`, SEV_WARN, n.name, `${n.name} is being sent a snapshot`,
      `Receiving the whole store · ${formatRate(n.traffic.snapshot.received)} over the ${window(n)}`)
  }
  if (!receiving.length) {
    for (const n of live.filter((x) => (x.traffic.snapshot?.sent || 0) > 0)) {
      add(`${n.name}:snapshot`, SEV_WARN, n.name, `${n.name} is sending a snapshot`,
        `Sending the whole store to a member · ${formatRate(n.traffic.snapshot.sent)} over the ${window(n)}`)
    }
  }

  for (const n of status.nodes) {
    if (n.unreachable) continue
    if (n.heartbeatMs !== null) {
      add(`${n.name}:heartbeat`, n.heartbeatSeverity, n.name, `${n.name} stopped answering the leader`,
        `The leader last heard from it ${ms(n.heartbeatMs)} ago`)
    }
    if (n.lag !== null) {
      add(`${n.name}:lag`, n.lagSeverity, n.name, `${n.name} is falling behind the leader`,
        `${count(n.lag)} log entries behind`)
    }
    const d = n.disk
    if (d.usedPct !== null) {
      add(`${n.name}:disk`, d.sev, n.name,
        d.refused ? `${n.name} refuses writes: its disk is full` : `${n.name}'s disk is nearly full`,
        d.refused ? `Disk ${Math.round(d.usedPct)}% used · writes answer 507` : `Disk ${Math.round(d.usedPct)}% used${d.gate && d.highPct !== null ? ` · stops taking writes at ${d.highPct}%` : ''}`)
    }
    if (n.mem.share !== null) add(`${n.name}:memory`, n.mem.sev, n.name, `${n.name} is short of memory`, `Memory ${share(n.mem.share)} of its limit`)
    if (n.cpu.share !== null) add(`${n.name}:cpu`, n.cpu.sev, n.name, `${n.name} is busy`, `CPU ${share(n.cpu.share)} of the cores it may use`)
  }
  return mergeShared(out).sort((a, b) => (RANK[b.sev] || 0) - (RANK[a.sev] || 0))
}

// What several nodes share, said once: the same problem, tone and words.
const PLURAL = {
  snapshot: (n) => `${n} nodes are moving a snapshot`,
  heartbeat: (n) => `${n} followers stopped answering the leader`,
  lag: (n) => `${n} followers are falling behind the leader`,
  disk: (n, i) => (/writes answer 507/.test(i.why) ? `${n} nodes refuse writes: their disks are full` : `${n} nodes' disks are nearly full`),
  memory: (n) => `${n} nodes are short of memory`,
  cpu: (n) => `${n} nodes are busy`,
}

/**
 * One row per (problem, tone, words): three nodes whose disks read "84%
 * used" are one issue naming the three, not three rows saying it again.
 * Named in full up to three nodes, counted past that. Exported for tests.
 */
export function mergeShared(issues) {
  const out = []
  const byShape = new Map()
  for (const i of issues) {
    const [node, kind] = i.key.slice('cell:'.length).split(':')
    if (!kind || !PLURAL[kind]) { out.push(i); continue }
    const shape = `${kind}|${i.sev}|${i.why}`
    const seen = byShape.get(shape)
    if (seen) { seen.nodes.push(node); continue }
    const entry = { ...i, kind, nodes: [node] }
    byShape.set(shape, entry)
    out.push(entry)
  }
  return out.map((i) => {
    if (!i.nodes || i.nodes.length < 2) return i
    const n = i.nodes.length
    return {
      ...i,
      key: `cell:${i.kind}:${i.nodes.join(',')}`,
      name: n <= 3 ? i.nodes.join(', ') : `${n} nodes`,
      title: PLURAL[i.kind](n, i),
    }
  })
}
