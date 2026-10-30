// What a node moves, as System renders it: bytes per second over the network,
// to and from the disk, and over the Raft RPC port. Read from two broker
// routes:
//
//   GET /api/v1/analytics/system-metrics  per point, `network`, `disk` and
//       `raft_traffic` (its total and each channel), every leaf
//       {avg, min, max, last} in bytes per second
//   GET /api/v1/raft/members              per member, `traffic`: the metrics
//       collector's last interval, {windowSeconds, raft, network, disk}
//
// A broker before these fields, and a platform that does not report one
// (the network on macOS), leaves them out: unknown, never zero.
//
// Pure — no Vue, no store, no HTTP — so test/traffic.test.js holds it against
// the contract's own payloads.

/** Finite number or null: a missing field must never format as 0. */
const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/**
 * The Raft RPC port's channels, in the broker's order
 * (server/src/rsm/replicator/raft/traffic.rs).
 */
export const RAFT_CHANNELS = [
  { key: 'replication', label: 'Replication', note: "the leader's appends to its followers" },
  { key: 'forward', label: 'Forwarding', note: 'client commands a follower hands to the leader' },
  { key: 'snapshot', label: 'Snapshots', note: 'a whole store sent to a member too far behind' },
  { key: 'link', label: 'Standby link', note: 'a standby cluster reading this one' },
  { key: 'control', label: 'Control', note: 'votes, membership, the members view' },
]

// 1024-based, like formatBytes.
const UNITS = [
  { label: 'B/s', div: 1 },
  { label: 'KB/s', div: 1024 },
  { label: 'MB/s', div: 1024 ** 2 },
  { label: 'GB/s', div: 1024 ** 3 },
]

/** The unit a chart whose largest value is `max` bytes per second reads best in. */
export function rateUnit(max) {
  const v = num(max) ?? 0
  let unit = UNITS[0]
  for (const u of UNITS) if (v >= u.div) unit = u
  return unit
}

/** `v` bytes per second in `unit`, two decimals at most; null stays null. */
export function inUnit(v, unit) {
  const n = num(v)
  return n === null ? null : Math.round((n / unit.div) * 100) / 100
}

/** "0 B/s", "812 B/s", "12.4 KB/s", "3.21 MB/s"; an em dash when unknown. */
export function formatRate(v) {
  const n = num(v)
  if (n === null) return '—'
  const unit = rateUnit(n)
  const x = n / unit.div
  const digits = unit.div === 1 || x >= 100 ? 0 : x >= 10 ? 1 : 2
  return `${parseFloat(x.toFixed(digits))} ${unit.label}`
}

/**
 * A member's `traffic` block → its Raft table cell: what the node sent and
 * received over the Raft port, per second over the collector's last
 * interval, and each channel. null when the member says nothing (a broker
 * before this field, an unreachable member, or before its first interval).
 */
export function memberTraffic(member) {
  const t = member?.traffic
  const raft = t?.raft
  if (!raft || typeof raft !== 'object') return null
  const channels = RAFT_CHANNELS.map((c) => ({
    key: c.key,
    label: c.label,
    sent: num(raft[c.key]?.sentBytesPerSec),
    received: num(raft[c.key]?.receivedBytesPerSec),
  }))
  return {
    sent: num(raft.sentBytesPerSec),
    received: num(raft.receivedBytesPerSec),
    windowSeconds: num(t.windowSeconds),
    channels,
    snapshot: channels.find((c) => c.key === 'snapshot'),
    // Where the platform says (the network: Linux only); else null.
    network: t.network ? { received: num(t.network.receivedBytesPerSec), sent: num(t.network.sentBytesPerSec) } : null,
    disk: t.disk ? { read: num(t.disk.readBytesPerSec), written: num(t.disk.writtenBytesPerSec) } : null,
  }
}

/** The tooltip of a Traffic cell: the window, then each channel that moved anything. */
export function trafficTitle(traffic) {
  if (!traffic) return ''
  const window = traffic.windowSeconds ? `last ${traffic.windowSeconds} s` : 'last interval'
  const busy = traffic.channels.filter((c) => (c.sent || 0) + (c.received || 0) > 0)
  if (!busy.length) return `Nothing crossed the Raft port (${window})`
  return [
    `Raft port, ${window}:`,
    ...busy.map((c) => `${c.label}: ${formatRate(c.sent)} sent, ${formatRate(c.received)} received`),
  ].join('\n')
}
