// The reads several tools share, joined the way the console's stores join them.

import { BrokerError } from './broker.mjs'
import { keyPath, segment, withQuery } from './routes.mjs'
import {
  FEED_LOCK_PREFIX,
  FEED_NAMESPACE,
  SETTINGS_KEY,
  SETTINGS_NS,
  THRESHOLDS,
  buildQueueTriage,
  feedKeysToRead,
  feedStreamRows,
  isHiddenQueue,
  locksListBody,
  observePending,
  opsByQueue,
  permitsOf,
  readSettings,
  resolveLines,
  scanFeedKeys,
  setLinesReader,
  shownGroups,
  withFeedReaders,
} from './rules.mjs'

const QUORUM_READ_MS = 3_000
const FEED_KEYS_MAX = 200
const FEED_READS_AT_ONCE = 6

export const NEEDS_QUORUM = {
  unavailable: 'needs_quorum',
  message: 'The broker answers this only with a working quorum, and this cluster has none right now. Run diagnose.',
}

/** What an answer owes the reader about where it came from. */
export function freshnessNotes(source) {
  if (!source || source.ready) return []
  return [`${source.node} answered without a quorum: this is what it had applied, and it can miss recent writes.`]
}

/** The alert lines in force, from Settings, or the built-in ones with a note. */
export async function readLines(cluster) {
  const builtIn = (note) => {
    setLinesReader(() => THRESHOLDS)
    return { linesFor: () => THRESHOLDS, notes: note ? [note] : [] }
  }
  if (!(await cluster.hasQuorum())) {
    return builtIn('No quorum: judged by the built-in alert lines, because Settings cannot be read.')
  }
  try {
    const { data } = await cluster.read(`/api/v1/kv/${segment(SETTINGS_NS)}/${keyPath(SETTINGS_KEY)}`, { timeoutMs: QUORUM_READ_MS })
    const settings = readSettings(data?.found ? data.value : null)
    setLinesReader(() => resolveLines(THRESHOLDS, settings))
    return { linesFor: (queue) => resolveLines(THRESHOLDS, settings, queue), notes: [] }
  } catch (e) {
    if (!(e instanceof BrokerError)) throw e
    return builtIn('Settings could not be read: judged by the built-in alert lines.')
  }
}

/** The feed streams, which read queues with no consumer group. */
export async function readFeed(cluster) {
  if (!(await cluster.hasQuorum())) return { rows: [], known: false }
  const list = async (body) =>
    (await cluster.read('/api/v1/resources/kv/list', { method: 'POST', body, timeoutMs: QUORUM_READ_MS })).data
  try {
    const { keys } = await scanFeedKeys(list, { maxPages: 5 })
    const wanted = feedKeysToRead(keys).slice(0, FEED_KEYS_MAX)
    const values = new Map()
    for (let i = 0; i < wanted.length; i += FEED_READS_AT_ONCE) {
      await Promise.all(wanted.slice(i, i + FEED_READS_AT_ONCE).map(async (k) => {
        const { data } = await cluster.read(`/api/v1/kv/${segment(FEED_NAMESPACE)}/${keyPath(k)}`, { timeoutMs: QUORUM_READ_MS })
        if (data?.found) values.set(k, data.value)
      }))
    }
    const locks = await list(locksListBody({ prefix: FEED_LOCK_PREFIX, limit: 1000 }))
    return { rows: feedStreamRows({ keys, values, permits: permitsOf(locks?.rows), now: Date.now() }), known: true }
  } catch (e) {
    if (e instanceof BrokerError && ['no_access', 'not_found'].includes(e.kind)) return { rows: [], known: true }
    if (e instanceof BrokerError) return { rows: [], known: false }
    throw e
  }
}

export async function readGroups(cluster) {
  const { data, source } = await cluster.read('/api/v1/consumer-groups', { timeoutMs: 15_000 })
  return { groups: shownGroups(Array.isArray(data) ? data : data?.consumer_groups || []), source }
}

let lastPending = null

/** Every queue with the console's verdict on it. */
export async function readTriage(cluster) {
  const [lines, feed, listed, groupsRead, workload] = await Promise.all([
    readLines(cluster),
    readFeed(cluster),
    cluster.read('/api/v1/resources/queues', { timeoutMs: 15_000 }),
    readGroups(cluster),
    cluster.read(withQuery('/api/v1/analytics/workload', { groupBy: 'queue' }), { timeoutMs: 8_000 }).catch((e) => {
      if (e instanceof BrokerError) return null
      throw e
    }),
  ])
  const all = listed.data?.queues || (Array.isArray(listed.data) ? listed.data : [])
  const visible = all.filter((q) => q.name && q.name.trim() !== '' && !isHiddenQueue(q.name))
  const queues = withFeedReaders(visible, feed.rows)
  const ops = workload ? opsByQueue(workload.data) : null

  // Two readings of the list, up to two minutes apart, give the trend.
  const seen = observePending(lastPending, queues, Date.now())
  lastPending = seen.current

  const rows = buildQueueTriage(queues, groupsRead.groups, seen.delta, lines.linesFor, ops)
  const notes = [...lines.notes]
  if (!feed.known) notes.push('Feed streams could not be read: a queue only a feed stream reads may show as having no reader.')
  if (!ops) notes.push('The last hour of acks could not be read: queues are judged without them.')
  return { rows, queues, groups: groupsRead.groups, ops, linesFor: lines.linesFor, source: listed.source, notes }
}
