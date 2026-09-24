// The Raft cluster as System's Raft source renders it: what each member row
// says, whether the voters still make a quorum, and what the page must warn
// about. Read from two broker routes (api/index.js operator.getRaft*):
//
//   GET /api/v1/raft/members  {clusterId, leaderId, term, self, members:[…]}
//   GET /api/v1/raft/status   the answering node's member object, plus
//                             clusterId, leaderId, self, voters, singleNode
//
// A member is `{nodeId, role: voter|learner, state, reachable, …}`, `state` one
// of leader | follower | candidate | learner | shutdown | unreachable. An
// unreachable member carries only nodeId, role, raftAddr, httpAddr,
// reachable:false and error; everything else is null — unknown, never zero.
// matchIndex / lagEntries / heartbeatAgeMs are the LEADER's view of each
// follower, so the leader's own row has none of them.
//
// Every colour comes from composables/useSeverity.js; nothing here picks a
// threshold. Pure — no Vue, no store, no HTTP — so test/raft-cluster.test.js
// holds the rows against the contract's own payloads.

import { describeApiError, refusalCode } from '../api/errors.js'
import {
  SEV_BAD, SEV_NONE, SEV_WARN,
  quorumSeverity, raftHeartbeatSeverity, raftLagSeverity, storeMapSeverity,
} from './useSeverity.js'

/** Finite number or null: a missing field must never format as 0. */
const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

const RANK = { [SEV_NONE]: 0, [SEV_WARN]: 1, [SEV_BAD]: 2 }
const worst = (...tones) => tones.reduce((a, b) => ((RANK[b] || 0) > (RANK[a] || 0) ? b : a), SEV_NONE)

/**
 * A log index, in full. formatNumber abbreviates from a million up, and
 * "58.9M / 58.9M / 58.9M" would hide the very gap the applied, committed and
 * durable columns exist to show.
 */
export function formatIndex(v) {
  const x = num(v)
  return x === null ? '—' : Math.trunc(x).toLocaleString('en-US')
}

/**
 * The store's map usage. `mapUsedPct` is already a percentage — 0.038 is
 * 0.038 %, not 3.8 % (server/src/rsm/store/mod.rs MapUsage::pct) — and a small
 * one is the normal case on a 64 GiB map, so it keeps two decimals under 1 %.
 */
export function formatMapPct(v) {
  const x = num(v)
  if (x === null) return '—'
  if (x === 0) return '0%'
  if (x < 0.01) return '<0.01%'
  return `${x < 1 ? x.toFixed(2) : x.toFixed(1)}%`
}

// The leader is the one green badge: the broker reports `leader` only once the
// node has applied an entry of its own term (replicator/raft role_of), so it
// is a verified healthy state, not a title. A follower is healthy and plain.
const CHIPS = {
  leader: { cls: 'chip-ok', label: 'leader' },
  follower: { cls: 'chip-mute', label: 'follower' },
  learner: { cls: 'chip-mute', label: 'learner' },
  candidate: { cls: 'chip-warn', label: 'candidate' },
  shutdown: { cls: 'chip-bad', label: 'shutdown' },
  unreachable: { cls: 'chip-bad', label: 'unreachable' },
}

/** The badge for a member state; an unknown state is shown as sent, in plain ink. */
export function stateChip(state) {
  return CHIPS[state] || { cls: 'chip-mute', label: state ? String(state) : 'unknown' }
}

export const isUnreachable = (m) => !!m && (m.reachable === false || m.state === 'unreachable')

/** Answers and takes part: what a voter must be to count toward a majority. */
export const isUp = (m) => !!m && !isUnreachable(m) && m.state !== 'shutdown'

/**
 * Voters that answer against the voters the membership holds. Learners never
 * vote, so they are counted apart. A member with no role is counted as a
 * voter: under-counting voters would under-state the majority.
 */
export function quorumHealth(members) {
  const list = (Array.isArray(members) ? members : []).filter(Boolean)
  const voters = list.filter((m) => m.role !== 'learner')
  const up = voters.filter(isUp).length
  const total = voters.length
  const majority = total > 0 ? Math.floor(total / 2) + 1 : null
  const hasQuorum = total > 0 ? up >= majority : null
  return {
    voters: total,
    up,
    down: total - up,
    learners: list.length - total,
    majority,
    hasQuorum,
    /** Voters that can still fail before writes stop. */
    spare: hasQuorum ? up - majority : 0,
    severity: quorumSeverity({ up, voters: total }),
  }
}

/** One members-table row: the raw figures, the badges and the tones. */
export function memberRow(member, { leaderId = null, self = null } = {}) {
  const m = member || {}
  const nodeId = num(m.nodeId)
  const unreachable = isUnreachable(m)
  const state = unreachable ? 'unreachable' : (m.state || null)
  const isLeader = state === 'leader'
  const store = m.store || {}
  const log = m.log || {}
  // The leader's view of each follower: on the leader's own row, and on a row
  // nobody could reach, there is nothing to lag behind.
  const lag = isLeader || unreachable ? null : num(m.lagEntries)
  const heartbeatMs = isLeader || unreachable ? null : num(m.heartbeatAgeMs)
  const mapPct = num(store.mapUsedPct)
  return {
    key: nodeId ?? m.raftAddr ?? m.httpAddr ?? null,
    nodeId,
    hostname: m.hostname || null,
    httpAddr: m.httpAddr || null,
    raftAddr: m.raftAddr || null,
    role: m.role || null,
    state,
    chip: stateChip(state),
    isLeader,
    isSelf: m.local === true || (nodeId !== null && nodeId === num(self)),
    isNamedLeader: nodeId !== null && nodeId === num(leaderId),
    unreachable,
    error: m.error ? String(m.error) : null,
    term: num(m.term),
    lastLogIndex: num(m.lastLogIndex),
    applied: num(m.appliedIndex),
    committed: num(m.committedIndex),
    durable: num(m.durableIndex),
    match: num(m.matchIndex),
    inflight: num(m.inflight),
    lag,
    lagSeverity: raftLagSeverity(lag),
    heartbeatMs,
    heartbeatSeverity: raftHeartbeatSeverity(heartbeatMs),
    logBytes: num(log.bytes),
    logFiles: num(log.files),
    mapPct,
    mapUsed: num(store.usedBytes),
    mapBytes: num(store.mapBytes),
    readers: num(store.readersInUse),
    maxReaders: num(store.maxReaders),
    mapSeverity: storeMapSeverity(mapPct),
    version: m.version || null,
    uptimeSeconds: num(m.uptimeSeconds),
  }
}

/**
 * GET /api/v1/raft/members → the Raft source's model, or null when the
 * payload is not one. Rows are ordered by node id so a leader change never
 * reshuffles the table under the reader.
 */
export function clusterView(payload) {
  if (!payload || typeof payload !== 'object' || !Array.isArray(payload.members)) return null
  const leaderId = num(payload.leaderId)
  const self = num(payload.self)
  const rows = payload.members
    .filter(Boolean)
    .map((m) => memberRow(m, { leaderId, self }))
    .sort((a, b) => (a.nodeId ?? Infinity) - (b.nodeId ?? Infinity))
  const quorum = quorumHealth(payload.members)
  return {
    clusterId: payload.clusterId || null,
    leaderId,
    leader: rows.find((r) => r.isNamedLeader) || rows.find((r) => r.isLeader) || null,
    term: num(payload.term),
    self,
    rows,
    quorum,
    singleNode: rows.length === 1 && quorum.voters === 1,
  }
}

/**
 * What the Raft source must say before anything else, or null. One alert at a
 * time, the gravest: without a majority nothing is elected or committed;
 * with one but no leader, an election is running; with a voter down, the
 * cluster serves one failure closer to not serving.
 */
export function clusterAlert(view) {
  const q = view?.quorum
  if (!q || !q.voters) return null
  if (!q.hasQuorum) {
    return {
      level: SEV_BAD,
      title: 'Quorum lost',
      detail: `${q.up} of ${q.voters} voters reachable, ${q.majority} needed — no leader can be elected and every write is refused until a voter comes back`,
    }
  }
  if (view.leaderId === null && !view.leader) {
    return {
      level: SEV_WARN,
      title: 'No leader',
      detail: `an election is running and writes wait for it — ${q.up} of ${q.voters} voters reachable, ${q.majority} needed`,
    }
  }
  if (q.down > 0) {
    return {
      level: SEV_WARN,
      title: `${q.down} of ${q.voters} voters unreachable`,
      detail: `quorum holds with ${q.up} up and ${q.majority} needed — ${q.spare === 0 ? 'one more loss stops writes' : `${q.spare} more can fail before writes stop`}`,
    }
  }
  return null
}

/**
 * GET /api/v1/raft/status → the Replicated log card: this node's row, plus
 * where it stands in the cluster and one tone for the card's border.
 */
export function nodeView(payload) {
  if (!payload || typeof payload !== 'object') return null
  const row = memberRow(payload, { leaderId: payload.leaderId, self: payload.self })
  const nodeId = row.nodeId ?? num(payload.self)
  const leaderId = num(payload.leaderId)
  const voters = num(payload.voters)
  const singleNode = payload.singleNode === true || voters === 1
  const stateSeverity =
    row.state === 'shutdown' || row.state === 'unreachable' ? SEV_BAD
      : row.state === 'candidate' || leaderId === null ? SEV_WARN
        : SEV_NONE
  return {
    ...row,
    nodeId,
    clusterId: payload.clusterId || null,
    leaderId,
    voters,
    singleNode,
    clusterNote: singleNode
      ? 'single node, not replicated'
      : voters !== null ? `${voters} voters in the cluster` : '',
    leaderNote: leaderId === null
      ? 'no leader known'
      : leaderId === nodeId ? 'this node leads' : `leader is node ${leaderId}`,
    leaderSeverity: leaderId === null ? SEV_WARN : SEV_NONE,
    severity: worst(stateSeverity, row.mapSeverity),
  }
}

/** 503 no_leader: the broker had no leader to answer for the cluster. */
export const isNoLeaderError = (err) => refusalCode(err) === 'no_leader'

/** The sentence for a failed raft read. no_leader is a cluster state, not a fault. */
export function describeRaftFailure(err) {
  if (isNoLeaderError(err)) {
    return 'no leader is elected (503 no_leader) — writes are refused until a majority of voters elects one'
  }
  return describeApiError(err)
}
