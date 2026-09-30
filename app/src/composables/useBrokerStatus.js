// The Overview's broker strip: the machine under each node of the cell and the
// replicated log across them, from the read System's Raft source already makes
// — GET /api/v1/raft/members, whose members carry `host`
// (server/src/rsm/facade/real/phase2/admin.rs host_json):
//
//   cpuPct, cpuWindowSeconds  the process's CPU over the metrics collector's
//                             last interval, percent of ONE core (4 busy = 400)
//   cpus                      the CPUs it may use (affinity, cgroup quota)
//   rssBytes, memLimitBytes   resident memory, and the cgroup's limit or the RAM
//   disk                      the data filesystem: usedPct, usedBytes,
//                             totalBytes, and the node's write gate (gate,
//                             highPct, lowPct, writesRefused)
//
// A member nobody could reach, or a broker older than `host`, has none of it:
// its readings are null, which the strip says as "—", never as idle. A measure
// is summed up by its fullest node, the one that runs out first; the meters
// show every node. Pure, like useRaftCluster: test/broker-status.test.js.

import { clusterView } from './useRaftCluster.js'
import {
  SEV_BAD, SEV_NONE, SEV_WARN,
  cpuShareSeverity, diskSeverity, memoryShareSeverity,
} from './useSeverity.js'

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
  const limit = num(h.memLimitBytes)
  const memShare = rss !== null && limit ? rss / limit : null
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
    mem: { rss, limit, share: memShare, sev: memoryShareSeverity(memShare) },
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
    sev: [cpu.sev, mem.sev, disk.sev, raft.sev].reduce(worst, SEV_NONE),
  }
}
