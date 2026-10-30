// System's list and tabs: "All nodes", then the cell's nodes, all from the
// GET /api/v1/raft/members read the page already makes, through brokerStatus
// (useBrokerStatus.js). Nothing here draws a line of its own: a mark is the
// worst of the severities brokerStatus and cellIssues already give, and only
// attention (warn, bad) is a mark; a healthy node has none. Pure, like
// useBrokerStatus: test/system-side.test.js.

import { cellIssues } from './useBrokerStatus.js'
import { stateChip } from './useRaftCluster.js'
import { SEV_BAD, SEV_WARN } from './useSeverity.js'

const RANK = { [SEV_WARN]: 1, [SEV_BAD]: 2 }

/** The worst of several severities: bad, warn, else ok. */
export function worstMark(sevs) {
  let out = 'ok'
  for (const s of sevs) if ((RANK[s] || 0) > (RANK[out] || 0)) out = s
  return out
}

/** A severity as a list mark: warn or bad, '' for anything that needs nobody. */
export const attention = (sev) => (sev === SEV_WARN || sev === SEV_BAD ? sev : '')

const count = (v) => Math.round(v).toLocaleString('en-US')
const capital = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s)

/** A node's severity: one nobody reached is bad; else the worst of its own readings. */
export function nodeMark(node) {
  if (node.unreachable) return SEV_BAD
  return worstMark([node.cpu?.sev, node.mem?.sev, node.disk?.sev, node.lagSeverity, node.heartbeatSeverity])
}

/** A node's part in the cluster, one word: Leader, Follower, Learner, Candidate, Unreachable. */
export function nodeRole(node) {
  if (node.unreachable) return 'Unreachable'
  if (node.role === 'learner') return 'Learner'
  return capital(stateChip(node.state).label)
}

// An Overview issue's kind (useBrokerStatus cellIssues): a merged one says
// it; else the key's last part ("cell:queen-2:disk", "cell:quorum").
const issueKind = (i) => i.kind || String(i.key || '').split(':').pop()
const aboutNode = (i, name) => (i.nodes ? i.nodes.includes(name) : i.name === name)
const RESOURCE_KINDS = new Set(['cpu', 'memory', 'disk'])

/**
 * brokerStatus(payload) → one entry per node, in its order (node id, the
 * members table's order too, so `index` is that node's row there):
 * `{ name, index, mark, role, why }`. `why` is the Overview's words for what
 * is wrong with it (cellIssues), and how far behind it is.
 */
export function nodeEntries(status) {
  if (!status || !Array.isArray(status.nodes)) return []
  const issues = cellIssues(status)
  return status.nodes.map((n, index) => {
    const mine = n.unreachable ? [] : issues.filter((i) => aboutNode(i, n.name))
    const why = n.unreachable ? ['No answer from this member'] : mine.map((i) => i.why)
    if (!n.unreachable && n.lag && !mine.some((i) => issueKind(i) === 'lag')) why.push(`${count(n.lag)} entries behind the leader`)
    return { name: n.name, index, mark: attention(nodeMark(n)), role: nodeRole(n), why: why.join('\n') }
  })
}

/**
 * The marks on the page's two tabs, for the whole cell or for one node:
 * Resources carries the CPU, memory and disk issues, Raft the quorum, the
 * followers' lag and heartbeat and a snapshot on the move (the Overview's
 * issues, cellIssues). `{ resources: { mark, why }, raft: { mark, why } }`,
 * `mark` '' when nothing needs you; `why` the issues' titles for the cell,
 * and for one node their words about it (a title may name three nodes).
 */
export function tabMarks(status, node = null) {
  const none = () => ({ resources: { mark: '', why: '' }, raft: { mark: '', why: '' } })
  if (!status || !Array.isArray(status.nodes)) return none()
  const target = node ? status.nodes.find((n) => n.name === node) : null
  if (node && !target) return none()
  const issues = cellIssues(status).filter((i) => !node || aboutNode(i, node))
  const tab = (resources) => {
    const mine = issues.filter((i) => RESOURCE_KINDS.has(issueKind(i)) === resources)
    return { mark: attention(worstMark(mine.map((i) => i.sev))), why: mine.map((i) => (node ? i.why : i.title)).join('\n') }
  }
  const out = { resources: tab(true), raft: tab(false) }
  if (target?.unreachable) out.raft = { mark: SEV_BAD, why: `${target.name} does not answer` }
  return out
}

/**
 * The system-metrics replicas of one node (its hostname is the members'
 * hostname: both are the broker's node label), or all of them with none
 * picked.
 */
export function nodeReplicas(replicas, node) {
  const list = Array.isArray(replicas) ? replicas : []
  return node ? list.filter((r) => r?.hostname === node) : list
}
