// The Overview's three panels under what needs you: where the traffic is
// (namespaces by arrivals), the cluster underneath (each node), and what runs
// inside the broker (streams and connectors). Plain functions over what the
// page already reads, each built on the page that owns the subject, so the two
// never disagree: queuesView's namespace totals, clusterViews' node rows,
// the Streams page's stream rows and the Connectors page's row rule.
// Pure: test/overview-panels.test.js.

import { HOUR_S, namespaceGroups } from './queues/queuesView.js'
import { nodeRows, sharedGate, versionsOf } from './cluster/clusterViews.js'

export const TRAFFIC_TOP = 4

/**
 * Namespaces by arrivals over the last hour, the busiest first:
 *   { rows: [{ ns, queues, inS, ackS, share }], quiet, more, total }
 * `share` is the namespace's arrivals over the busiest one's (a bar);
 * `quiet` the namespaces with no arrival this hour; `more` the busy ones past
 * the first `top`. Rates are messages a second, the hour's average. Null when
 * the hour was not read.
 */
export function namespaceTraffic(rows = [], top = TRAFFIC_TOP) {
  const groups = namespaceGroups(rows)
  const list = groups
    .filter((g) => g.totals.inHour != null)
    .map((g) => ({
      ns: g.ns,
      queues: g.total,
      inS: g.totals.inHour / HOUR_S,
      ackS: g.totals.ackHour == null ? null : g.totals.ackHour / HOUR_S,
    }))
  if (!list.length) return null
  const busy = list.filter((x) => x.inS > 0).sort((a, b) => b.inS - a.inS || a.ns.localeCompare(b.ns))
  const max = busy.length ? busy[0].inS : 0
  return {
    rows: busy.slice(0, top).map((x) => ({ ...x, share: max > 0 ? x.inS / max : 0 })),
    more: Math.max(0, busy.length - top),
    quiet: list.length - busy.length,
    total: busy.reduce((s, x) => s + x.inS, 0),
  }
}

/**
 * Each node, as the Cluster page reads it (nodeRows over the top bar's own
 * read), with what the panel says under them: the versions (one, or which
 * differ during a roll) and where every node stops writes. Null without the
 * cell (a tenant through the proxy sees no nodes).
 */
export function clusterNodes(status, view) {
  if (!status) return null
  const rows = nodeRows(status, view).map((r) => ({
    key: r.key,
    name: r.name || `node ${r.nodeId ?? '?'}`,
    leader: r.leader,
    role: r.role,
    roleTone: r.roleTone,
    disk: r.disk.text,
    diskTone: r.disk.tone,
    memory: r.memory,
    memoryTone: r.memoryTone,
    up: r.up,
    behind: r.leader ? null : r.behind,
    behindTone: r.behindTone,
    unreachable: r.unreachable,
  }))
  return { rows, versions: versionsOf(view), gate: sharedGate(status) }
}

const LIVE = new Set(['running', 'starting', 'partial'])

/**
 * What runs inside the broker: the streams (feed streams from their KV rows,
 * queue streams from the registry, as the Streams page judges them) and the
 * connectors (the Connectors page's rows). Each part is null when it could
 * not be read (a broker without the route, a role that may not):
 *   { streams: { total, feed, queue, running, stopped, needs: [names] } | null,
 *     connectors: { total, off, needs: [names] } | null }
 */
export function insideBroker({ feedRows = null, queueStreams = null, connectorRows = null } = {}) {
  let streams = null
  if (feedRows != null || queueStreams != null) {
    const feed = feedRows || []
    const queue = queueStreams || []
    const needs = [
      ...feed.filter((r) => r.needs).map((r) => r.name),
      ...queue.filter((r) => r.needs).map((r) => r.name),
    ]
    streams = {
      total: feed.length + queue.length,
      feed: feed.length,
      queue: queue.length,
      running: feed.filter((r) => LIVE.has(r.status)).length,
      stopped: feed.filter((r) => r.status === 'stopped').length,
      needs,
    }
  }
  let connectors = null
  if (connectorRows != null) {
    connectors = {
      total: connectorRows.length,
      off: connectorRows.filter((r) => r.word === 'Off').length,
      needs: connectorRows.filter((r) => r.tone).map((r) => r.name),
    }
  }
  return { streams, connectors }
}
