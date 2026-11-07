// Feed streams (Stream.fromFeed) on the Streams page: what they keep in KV,
// read as one row per stream. PURE (no Vue, no API, no '@/' imports), so
// `node --test` reaches every rule here; the reads live in ../useStreamsPage.js.
//
// What a feed stream keeps in the namespace `queen-feed` (client-js
// streams/runtime/FeedRunner.js and feed/about.js):
//   <queryId>                   the position record of a stream that runs whole
//   <queryId>#shard<i>of<n>     the position record of shard i of n
//   <queryId>#layout            {shards, shardBy}, when it runs in shards
//   <queryId>#about             what it reads, its steps, where its results go
//                               (written by client-js from 2.7.x; older runners
//                               write none, and the row says less)
//   <id>#snap#<sid>#<n>         the chunks of a snapshot: never read here
//   <queryId>#<n>               state in an old format: never read here
// and its lock, `queen-feed/<queryId>` (`/shard-<i>-of-<n>` for a shard), in
// `queen-locks`, held by the runner that runs it: owner `<host>:<pid>:<random>`.
//
// Four rules worth knowing before changing anything:
//
//  1. A LOCK HELD IS A RUNNER. The runner renews it while it runs, a standby
//     waits without one, and the listing has only live permits (the broker
//     judges expiry on its own clock).
//
//  2. caughtUp, NOT THE TIMES, SAYS "NOT BEHIND". The broker's clock moves only
//     with writes: a read on a quiet cluster resolves to the last write's time,
//     so `readTo` trails the wall clock there. Behind is `updatedAt - readTo`
//     only in a record that was not caught up.
//
//  3. A RUNNER WRITES ITS POSITION AT LEAST EVERY positionCommitMillis (5 s by
//     default) while the log moves, and its own writes and lock renewals move
//     it. A lock held with no write for three of those (a minute at least) is
//     a runner that does not get on: its reads fail, or a function hangs.
//
//  4. NO LOCK, AND NO STOP IN THE RECORD, IS A RUNNER THAT DIED. A crash or a
//     kill ends one without a word; a poison stop and a stop its process asked
//     for say so in the record's `stopped`. A stream with `exclusive: false`
//     takes no lock: its record's age alone says whether it runs.
import { span } from '../words.js'
import { bytes } from '../state/stateViews.js'

export const FEED_NAMESPACE = 'queen-feed'
export const FEED_LOCK_PREFIX = 'queen-feed/'
const PAGE = 1000
const MAX_PAGES = 20
const STALE_MIN_MS = 60_000
const POSITION_COMMIT_MS = 5000
// A record that was not caught up, closer than this to its feed time, is not behind:
// the clocks of two machines, and the page in hand.
const NOT_BEHIND_MS = 2000
// The same for an older runner's record, which has no caughtUp: its clock lags
// by up to one position commit on a quiet feed.
const NOT_BEHIND_OLD_MS = 15_000

const msOf = (v) => {
  if (v == null || v === '') return null
  const t = typeof v === 'number' ? v : Date.parse(v)
  return Number.isFinite(t) ? t : null
}
const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
const plural = (n, one, many = `${one}s`) => (n === 1 ? one : many)

// ---- keys -------------------------------------------------------------------

/**
 * One key of `queen-feed`: `{type: 'position', queryId, shard, id}`, `{type:
 * 'about'|'layout', queryId}`, `{type: 'chunk', id}`, or null (a key of an
 * old format, or not a feed stream's). A queryId has no `#`: the first one
 * splits the key.
 */
export function feedKeyOf(key) {
  if (typeof key !== 'string' || key === '') return null
  const snap = key.indexOf('#snap#')
  if (snap > 0) return { type: 'chunk', id: key.slice(0, snap) }
  const at = key.indexOf('#')
  if (at === -1) return { type: 'position', queryId: key, shard: null, id: key }
  if (at === 0) return null
  const queryId = key.slice(0, at)
  const rest = key.slice(at + 1)
  if (rest === 'about') return { type: 'about', queryId }
  if (rest === 'layout') return { type: 'layout', queryId }
  const m = /^shard(0|[1-9][0-9]*)of([1-9][0-9]*)$/.exec(rest)
  if (m && Number(m[1]) < Number(m[2])) return { type: 'position', queryId, shard: { index: Number(m[1]), count: Number(m[2]) }, id: key }
  return null
}

/** The `POST /api/v1/resources/kv/list` body for one page of the namespace's keys. */
export function feedListBody(after = null) {
  const body = { namespace: FEED_NAMESPACE, keysOnly: true, limit: PAGE }
  if (typeof after === 'string' && after !== '') body.after = after
  return body
}

/**
 * The cursor after a page. A page that ends in a snapshot's chunks jumps past
 * the rest of them at once: `<id>#snap$` sorts right after every `<id>#snap#…`
 * (`$` follows `#`), so a stream with thousands of chunks costs one page.
 */
export function feedAfter(rows, nextAfter) {
  const last = rows.length ? rows[rows.length - 1].key : null
  const k = feedKeyOf(last)
  if (k && k.type === 'chunk') return `${k.id}#snap$`
  return nextAfter ?? last
}

/**
 * Every key of the namespace but the chunks: `{keys: Map(key → {version,
 * updatedAt}), exact}`. `listPage(body)` answers one page (`{rows, truncated,
 * nextAfter}`); `exact` is false when `maxPages` ran out first.
 */
export async function scanFeedKeys(listPage, { maxPages = MAX_PAGES } = {}) {
  const keys = new Map()
  let after = null
  for (let n = 0; n < maxPages; n++) {
    const page = await listPage(feedListBody(after))
    const rows = Array.isArray(page?.rows) ? page.rows : []
    for (const r of rows) {
      const k = feedKeyOf(r?.key)
      if (k && k.type !== 'chunk') keys.set(r.key, { version: r.version ?? null, updatedAt: r.updatedAt ?? null })
    }
    if (page?.truncated !== true) return { keys, exact: true }
    const next = feedAfter(rows, page.nextAfter)
    if (!next || next === after) return { keys, exact: false }
    after = next
  }
  return { keys, exact: false }
}

/** The keys whose values the rows need: the records, the descriptions, the layouts. */
export function feedKeysToRead(keys) {
  return [...keys.keys()].filter((k) => {
    const t = feedKeyOf(k)?.type
    return t === 'position' || t === 'about' || t === 'layout'
  })
}

// ---- locks ------------------------------------------------------------------

/** The lock a runner holds: `queen-feed/<queryId>`, `/shard-<i>-of-<n>` for a shard. */
export function feedLockName(queryId, shard = null) {
  return shard && shard.count > 1
    ? `${FEED_LOCK_PREFIX}${queryId}/shard-${shard.index}-of-${shard.count}`
    : `${FEED_LOCK_PREFIX}${queryId}`
}

/** The stream (and shard) a lock name is of: `{queryId, shard}`, or null for another lock. */
export function lockQueryOf(name) {
  if (typeof name !== 'string' || !name.startsWith(FEED_LOCK_PREFIX)) return null
  const rest = name.slice(FEED_LOCK_PREFIX.length)
  const m = /^(.+)\/shard-(0|[1-9][0-9]*)-of-([1-9][0-9]*)$/.exec(rest)
  if (m && Number(m[2]) < Number(m[3])) return { queryId: m[1], shard: { index: Number(m[2]), count: Number(m[3]) } }
  return rest && !rest.includes('#') ? { queryId: rest, shard: null } : null
}

/** A lock owner `<host>:<pid>:<random>` (client-js Locks.js mintOwner) as `{host, pid}`. */
export function ownerParts(owner) {
  if (typeof owner !== 'string' || owner === '') return { host: null, pid: null }
  const m = /^(.*):([0-9]+):[0-9a-f]+$/.exec(owner)
  return m && m[1] !== '' ? { host: m[1], pid: Number(m[2]) } : { host: owner, pid: null }
}

// ---- one shard --------------------------------------------------------------

function totals(o) {
  if (!o || typeof o !== 'object') return null
  let n = 0
  let any = false
  for (const v of Object.values(o)) if (Number.isFinite(v)) { n += v; any = true }
  return any ? n : null
}

function behindOf(record, updatedMs) {
  if (record.caughtUp === true) return 0
  const fresh = typeof record.caughtUp === 'boolean'
  const t = Number.isFinite(record.readTo) ? record.readTo : Number.isFinite(record.clock) && record.clock > 0 ? record.clock : null
  if (t == null || updatedMs == null) return null
  const d = updatedMs - t
  return d < (fresh ? NOT_BEHIND_MS : NOT_BEHIND_OLD_MS) ? 0 : d
}

/**
 * One shard (or the whole stream) from its record and its lock:
 *   running    a runner holds the lock and writes (behindMs: how far behind, null: not known)
 *   starting   a runner holds the lock and has committed nothing yet
 *   quiet      a runner holds the lock and has written nothing for a while (rule 3)
 *   failed     it stopped on a message that kept failing (`stopped.reason` poison)
 *   stopped    its process stopped it
 *   gone       no runner, and the record does not say it stopped (rule 4)
 *   never      no runner and no record: nothing ran yet, or what ran left only its layout
 */
export function shardState({ record = null, permit = null, about = null, now = Date.now() } = {}) {
  const updatedMs = msOf(record?.updatedAt)
  const sinceMs = msOf(permit?.since)
  const endsMs = msOf(permit?.expiresAt)
  const holder = permit && (endsMs == null || endsMs > now)
    ? { owner: permit.owner ?? null, ...ownerParts(permit.owner), sinceMs, endsMs }
    : null
  const every = Number.isFinite(about?.positionCommitMillis) && about.positionCommitMillis > 0 ? about.positionCommitMillis : POSITION_COMMIT_MS
  const staleMs = Math.max(STALE_MIN_MS, 3 * every)
  const base = {
    record,
    holder,
    updatedMs,
    position: Number.isInteger(record?.position) ? record.position : null,
    open: totals(record?.open),
    held: totals(record?.held),
    stopped: record?.stopped && typeof record.stopped === 'object' ? record.stopped : null,
    behindMs: null,
    quietMs: null,
  }
  if (holder) {
    // No record yet: starting, until it has held the lock as long as a write may take.
    if (!record) {
      return sinceMs != null && now - sinceMs > staleMs ? { ...base, state: 'quiet', quietMs: now - sinceMs } : { ...base, state: 'starting' }
    }
    const last = Math.max(updatedMs ?? 0, sinceMs ?? 0)
    if (last > 0 && now - last > staleMs) return { ...base, state: 'quiet', quietMs: now - last }
    return { ...base, state: 'running', behindMs: behindOf(record, updatedMs) }
  }
  if (!record) return { ...base, state: 'never' }
  if (base.stopped?.reason === 'poison') return { ...base, state: 'failed' }
  if (base.stopped?.reason === 'stop') return { ...base, state: 'stopped' }
  if (about && about.exclusive === false && updatedMs != null && now - updatedMs <= staleMs) {
    return { ...base, state: 'running', behindMs: behindOf(record, updatedMs) }
  }
  return { ...base, state: 'gone' }
}

// ---- what the description says ----------------------------------------------

/** A where condition in words: `data.country = "IT"`. */
export function whereText(c) {
  if (!c || typeof c !== 'object') return ''
  const v = c.value === undefined ? '' : ` ${JSON.stringify(c.value)}`
  return `${c.field} ${c.op}${v}`
}

const asList = (v) => (Array.isArray(v) ? v.filter((x) => typeof x === 'string') : null)

/** What the stream reads: `{queues (null: every queue), text, where, partitions, tables, kinds}`. */
export function readsOf(about) {
  const r = about?.reads
  if (!r) return null
  const queues = asList(r.queues)
  return {
    queues,
    text: queues && queues.length ? queues.join(', ') : 'every queue',
    where: Array.isArray(r.where) ? r.where.map(whereText).filter(Boolean) : [],
    partitions: asList(r.partitions),
    tables: asList(r.tables) || [],
    kinds: asList(r.kinds),
  }
}

/** Where its results go: `{kind, queues, text}`; null without a description. */
export function sinkOf(about) {
  const s = about?.sink
  if (!s || typeof s !== 'object') return null
  if (s.kind === 'queues') {
    const queues = asList(s.queues) || []
    return { kind: 'queues', queues, text: queues.join(', ') }
  }
  if (s.kind === 'route') return { kind: 'route', queues: [], text: 'queues a function picks' }
  if (s.kind === 'foreach') return { kind: 'foreach', queues: [], text: 'a function (foreach)' }
  return null
}

/**
 * The queue names among `names` that the stream's queues name: exact names
 * and patterns (`pay*`, `?`), as the feed matches them. `null` (every queue)
 * matches nothing here: the page has no rate for "every queue".
 */
export function matchQueues(patterns, names) {
  if (!Array.isArray(patterns) || !patterns.length) return []
  const res = patterns.map((p) => new RegExp(`^${p.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`))
  return [...names].filter((n) => res.some((re) => re.test(n)))
}

/**
 * Whether a stream's description says it reads the queue `name`: the queues it
 * names (patterns too), every queue when it names none, and the queues of its
 * table joins. A `where` on the queue field is not looked at: the page says
 * "reads" for every queue the stream's read can reach.
 */
export function readsQueue(reads, name) {
  if (!reads || typeof name !== 'string') return false
  if (Array.isArray(reads.tables) && reads.tables.includes(name)) return true
  if (!Array.isArray(reads.queues) || reads.queues.length === 0) return true
  return matchQueues(reads.queues, [name]).length > 0
}

/**
 * The queue rows, each one a feed stream reads carrying `feedReaders`:
 * [{ name, status, needs }], in the streams' order. A feed stream reads
 * without a consumer group, so to every rule that looks for one the queue
 * would be one nobody reads; this is what those rules check instead (the
 * Kafka facade's `kafkaTopic` is the same kind of reader). Rows no stream
 * reads are returned as they are, and so is the list when there is no stream.
 * A stream without a description (an older runner) is not known to read
 * anything.
 */
export function withFeedReaders(queues, feedRows) {
  if (!Array.isArray(queues) || !Array.isArray(feedRows) || !feedRows.length) return queues
  const described = feedRows.filter((r) => r && r.reads)
  if (!described.length) return queues
  return queues.map((q) => {
    if (!q || typeof q.name !== 'string') return q
    const by = described
      .filter((r) => readsQueue(r.about?.reads, q.name))
      .map((r) => ({ name: r.name, status: r.status, needs: r.needs }))
    return by.length ? { ...q, feedReaders: by } : q
  })
}

const LIVE = new Set(['running', 'starting', 'partial'])

/**
 * What a queue's State cell says about the feed streams that read it:
 * `{ kind: 'feed'|'feedDown', word, tone, title }`, or null when none does. "Feed
 * stream" while one of them runs; "Feed stopped" when their processes stopped
 * them; "Feed down" when none runs and none was stopped on purpose: amber
 * (kind `feedDown`) while the queue holds messages, as useAttention flags it,
 * and grey when it is empty. `pending` is what the queue holds: no group acks
 * it, so it only goes with retention.
 */
export function feedQueueState(readers, pending = null) {
  if (!Array.isArray(readers) || !readers.length) return null
  const names = readers.map((r) => r.name)
  const who = names.length === 1 ? `the feed stream ${names[0]}` : `${names.length} feed streams (${names.slice(0, 3).join(', ')}${names.length > 3 ? ', …' : ''})`
  const held = Number.isFinite(pending) && pending > 0
    ? ` It reads without a consumer group, so nothing here is acked: the queue holds ${fmt(pending)} message${pending === 1 ? '' : 's'} until retention removes them.`
    : ' It reads without a consumer group, so nothing here is acked.'
  if (readers.some((r) => LIVE.has(r.status))) {
    return { kind: 'feed', word: 'Feed stream', tone: '', title: `Read by ${who}.${held}` }
  }
  if (readers.some((r) => r.status === 'stopped')) {
    return { kind: 'feed', word: 'Feed stopped', tone: '', title: `Read by ${who}, which its process stopped.${held}` }
  }
  // Down: it needs you while the queue holds something (useAttention's feedDown), and is a fact when it is empty.
  const title = `Read by ${who}, and nothing runs ${names.length === 1 ? 'it' : 'them'} now: see Streams.${held}`
  return Number.isFinite(pending) && pending > 0
    ? { kind: 'feedDown', word: 'Feed down', tone: 'amber', title }
    : { kind: 'feed', word: 'Feed down', tone: '', title }
}

/** The dead-letter queue (`onPoison: queue`), or null. */
export function deadLetterOf(about) {
  const p = about?.onPoison
  return p && typeof p === 'object' && typeof p.queue === 'string' ? p.queue : null
}

const secs = (ms) => (Number.isFinite(ms) ? span(ms / 1000) : null)

function windowWords(w) {
  if (!w || typeof w !== 'object') return 'window'
  let shape
  if (w.kind === 'window-tumbling') shape = `tumbling ${span(w.seconds)}`
  else if (w.kind === 'window-sliding') shape = `sliding ${span(w.size)}, every ${span(w.slide)}`
  else if (w.kind === 'window-session') shape = `session, gap ${span(w.gap)}`
  else if (w.kind === 'window-cron') shape = `every ${w.every}`
  else shape = String(w.kind || 'window').replace(/^window-/, '')
  const parts = [shape]
  if (Number(w.gracePeriod) > 0) parts.push(`grace ${span(w.gracePeriod)}`)
  if (Number.isFinite(w.idle)) parts.push(`idle ${secs(w.idle)}`)
  return parts.join(', ')
}

function reducerWords(r) {
  if (!r) return null
  if (r.kind === 'aggregate') return Array.isArray(r.fields) && r.fields.length ? `aggregate ${r.fields.join(', ')}` : 'aggregate'
  return r.kind || null
}

/**
 * One step of the description as the opened row draws it: `{kind, name,
 * label, detail, clock, open, held}`, where open and held come from the
 * records (summed over the shards). Stateless operators are one step of
 * words (`map, keyBy`).
 */
export function stepWords(step, open = {}, held = {}) {
  if (!step || typeof step !== 'object') return null
  if (step.kind === 'ops') {
    const ops = Array.isArray(step.ops) ? step.ops : []
    const where = Array.isArray(step.where) ? step.where.map(whereText).filter(Boolean) : []
    return { kind: 'ops', name: null, label: ops.join(', ') || 'operators', detail: where.join(' and ') || null, clock: null, open: null, held: null }
  }
  const name = typeof step.name === 'string' ? step.name : null
  const c = step.config || {}
  let label
  let detail
  if (step.kind === 'window') {
    label = 'window'
    detail = [windowWords(step.window), reducerWords(step.reducer)].filter(Boolean).join(' · ')
  } else if (step.kind === 'pattern') {
    label = 'pattern'
    detail = [c.within ? `within ${secs(c.within)}` : null, c.timeout ? 'with a deadline' : null].filter(Boolean).join(', ')
  } else if (step.kind === 'join') {
    label = 'join'
    detail = [Array.isArray(c.parts) && c.parts.length ? `of ${c.parts.join(', ')}` : null, c.within ? `within ${secs(c.within)}` : null].filter(Boolean).join(', ')
  } else if (step.kind === 'gather') {
    label = 'gather'
    const what = Array.isArray(c.parts) && c.parts.length ? `of ${c.parts.join(', ')}` : c.count ? 'the count from its messages' : null
    detail = [what, c.within ? `within ${secs(c.within)}` : null].filter(Boolean).join(', ')
  } else if (step.kind === 'table') {
    label = 'table'
    detail = Array.isArray(c.queues) && c.queues.length ? `of ${c.queues.join(', ')}` : null
  } else {
    label = String(step.kind || 'step')
    detail = null
  }
  const clock = step.clock === 'event' ? 'event time' : step.clock === 'feed' ? 'feed clock' : null
  const n = name && Number.isFinite(open?.[name]) ? open[name] : null
  const h = name && Number.isFinite(held?.[name]) ? held[name] : null
  return { kind: step.kind, name, label, detail: detail || null, clock, open: n, held: h }
}

// ---- one stream -------------------------------------------------------------

// Worst first: the state of a stream is the worst of its shards'.
const WORST = ['failed', 'gone', 'never', 'quiet', 'starting', 'running', 'stopped']
const NEEDS = new Set(['failed', 'gone', 'never', 'quiet'])

function sumBy(list, f) {
  let n = 0
  let any = false
  for (const x of list) { const v = f(x); if (Number.isFinite(v)) { n += v; any = true } }
  return any ? n : null
}

function addInto(into, o) {
  if (!o || typeof o !== 'object') return
  for (const [k, v] of Object.entries(o)) if (Number.isFinite(v)) into[k] = (into[k] || 0) + v
}

/** A stopped record's message in words: `the message at position 812 (orders/c3, offset 4)`. */
export function stoppedAt(s) {
  if (!s) return ''
  if (Number.isInteger(s.position)) {
    const where = s.queue ? ` (${s.queue}/${s.partition}, offset ${s.offset})` : ''
    return `the message at position ${fmt(s.position)}${where}`
  }
  if (s.result || s.step) return `the ${s.result || 'result'} of step ${s.step ?? '?'}${s.key != null ? `, key ${JSON.stringify(s.key)}` : ''}`
  return 'a message'
}

/**
 * One feed stream as the page draws it, from its keys' values (`values`:
 * Map key → value) and the lock permits (useLocks.permitsOf). The fields the
 * table shares with a queue stream's row (useStreamsPage.streamRow) have the
 * same names.
 */
export function feedStreamRow({ queryId, values, lockBy, now = Date.now(), shardCounts = [] }) {
  const about = values.get(`${queryId}#about`) ?? null
  const layout = values.get(`${queryId}#layout`) ?? null
  const count = Number.isInteger(layout?.shards) && layout.shards > 1
    ? layout.shards
    : Number.isInteger(about?.shards?.count) && about.shards.count > 1
      ? about.shards.count
      : (values.has(queryId) || !shardCounts.length ? 1 : Math.max(...shardCounts))
  const list = count > 1 ? Array.from({ length: count }, (_, i) => ({ index: i, count })) : [null]
  const shards = list.map((sh) => {
    const key = sh ? `${queryId}#shard${sh.index}of${sh.count}` : queryId
    return {
      index: sh ? sh.index : 0,
      key,
      ...shardState({ record: values.get(key) ?? null, permit: lockBy.get(feedLockName(queryId, sh)) ?? null, about, now }),
    }
  })
  const states = new Set(shards.map((s) => s.state))
  let status = WORST.find((w) => states.has(w)) || 'never'
  // Some shards stopped by their process, the others still on: part of the stream runs.
  const partial = states.has('stopped') && states.size > 1
  if (partial && !NEEDS.has(status)) status = 'partial'
  const running = shards.filter((s) => s.state === 'running')
  const behinds = running.map((s) => s.behindMs).filter((v) => v != null)
  const behindMs = behinds.length ? Math.max(...behinds) : null
  const open = {}
  const held = {}
  for (const s of shards) { addInto(open, s.record?.open); addInto(held, s.record?.held) }
  const reads = readsOf(about)
  const sink = sinkOf(about)
  const hosts = new Set(shards.map((s) => s.holder?.host).filter(Boolean))
  // One process holds a lock per shard, each with its own owner: a process is its host and pid.
  const owners = new Set(shards.filter((s) => s.holder).map((s) => (s.holder.pid != null ? `${s.holder.host}:${s.holder.pid}` : s.holder.owner)))
  const snapBytes = sumBy(shards, (s) => s.record?.snapshot?.bytes)
  const layers = sumBy(shards, (s) => (Array.isArray(s.record?.snapshot?.layers) ? s.record.snapshot.layers.length : null))
  const snapClock = Math.min(...shards.map((s) => s.record?.snapshot?.clock).filter((v) => Number.isFinite(v) && v > 0))
  const snapFrom = Math.min(...shards.map((s) => s.record?.snapshot?.position).filter(Number.isInteger))
  const skipped = sumBy(shards, (s) => (Array.isArray(s.record?.skips) ? s.record.skips.length : null))
  const lastMs = Math.max(...shards.map((s) => s.updatedMs ?? -Infinity))
  const epochs = shards.map((s) => s.record?.epoch).filter(Number.isInteger)
  const failed = shards.filter((s) => s.state === 'failed')
  return {
    kind: 'feed',
    key: `feed:${queryId}`,
    name: queryId,
    queryId,
    about,
    layout,
    shardCount: count,
    shards,
    status,
    needs: NEEDS.has(status) ? 'amber' : '',
    reads,
    sink,
    deadLetters: deadLetterOf(about),
    source: reads ? reads.text : null,
    sinkText: sink ? sink.text : null,
    behindMs,
    lag: behindMs != null ? behindMs / 1000 : 0,
    open: sumBy(Object.values(open), (v) => v),
    openBy: open,
    heldBy: held,
    windows: sumBy(Object.values(open), (v) => v),
    overdue: null,
    hosts: [...hosts],
    owners: owners.size,
    position: shards.length === 1 ? shards[0].position : null,
    lastMs: Number.isFinite(lastMs) ? lastMs : null,
    snapshot: snapBytes != null
      ? { bytes: snapBytes, layers, clockMs: Number.isFinite(snapClock) ? snapClock : null, position: Number.isFinite(snapFrom) ? snapFrom : null }
      : null,
    skipped,
    epoch: epochs.length ? Math.max(...epochs) : null,
    hash: about?.hash ?? shards.find((s) => s.record?.hash)?.record.hash ?? null,
    client: typeof about?.client === 'string' ? about.client : null,
    failed,
    now,
  }
}

/**
 * Every feed stream found in the keys: `keys` (scanFeedKeys), `values` (Map
 * key → value of feedKeysToRead), `permits` (useLocks.permitsOf of the
 * `queen-feed/` locks). Sorted by name.
 */
export function feedStreamRows({ keys, values, permits = [], now = Date.now() }) {
  const ids = new Map()
  for (const key of keys.keys()) {
    const k = feedKeyOf(key)
    if (!k || k.type === 'chunk') continue
    if (!ids.has(k.queryId)) ids.set(k.queryId, [])
    if (k.type === 'position' && k.shard) ids.get(k.queryId).push(k.shard.count)
  }
  const lockBy = new Map()
  for (const p of Array.isArray(permits) ? permits : []) {
    if (!p || p.foreign || typeof p.name !== 'string' || lockBy.has(p.name)) continue
    const k = lockQueryOf(p.name)
    if (!k) continue
    lockBy.set(p.name, p)
    // A runner that has not committed yet has a lock and no key: it is a stream too.
    if (!ids.has(k.queryId)) ids.set(k.queryId, [])
    if (k.shard) ids.get(k.queryId).push(k.shard.count)
  }
  return [...ids.entries()]
    .map(([queryId, shardCounts]) => feedStreamRow({ queryId, values, lockBy, now, shardCounts }))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
}

// ---- words --------------------------------------------------------------------

/** The Behind cell: `{text, tone}`. */
export function behindCell(row) {
  switch (row.status) {
    case 'running': return { text: row.behindMs == null ? 'running' : row.behindMs === 0 ? 'caught up' : span(row.behindMs / 1000), tone: '' }
    case 'partial': return { text: 'part stopped', tone: '' }
    case 'starting': return { text: 'starting', tone: '' }
    case 'quiet': return { text: 'no writes', tone: 'amber' }
    case 'failed': return { text: 'stopped', tone: 'amber' }
    case 'stopped': return { text: 'stopped', tone: '' }
    default: return { text: 'no runner', tone: 'amber' }
  }
}

/** The verdict's words for one stream that needs you (after its name). */
export function feedNeedsText(row) {
  if (row.status === 'failed') return 'stopped on a message it cannot process.'
  if (row.status === 'quiet') return 'holds its lock but writes nothing.'
  return 'has no runner.'
}

const ago = (ms, now) => (ms == null ? null : `${span((now - ms) / 1000)} ago`)

/**
 * Why a stream needs you, for its opened row: `{tone, text, hint}` or null.
 * Said once per stream: a shard's words name the shard.
 */
export function feedWhy(row) {
  const now = row.now ?? Date.now()
  const who = (s) => (row.shardCount > 1 ? `Shard ${s.index}` : 'It')
  if (row.status === 'failed') {
    const s = row.failed[0]
    const st = s.stopped || {}
    const more = row.failed.length > 1 ? ` ${row.failed.length - 1} more ${plural(row.failed.length - 1, 'shard')} stopped the same way.` : ''
    const at = msOf(st.at)
    const times = Number.isFinite(st.attempts) ? ` after ${st.attempts} ${plural(st.attempts, 'try', 'tries')}` : ''
    return {
      tone: 'amber',
      text: `${who(s)} stopped${at != null ? ` ${ago(at, now)}` : ''} at ${stoppedAt(st)}${times}: ${st.error || 'no error kept'}.${more}`,
      hint: "Its next start reads that message again: fix the function, or run it with onPoison: 'skip' or a dead-letter queue.",
    }
  }
  if (row.status === 'quiet') {
    const s = row.shards.find((x) => x.state === 'quiet')
    return {
      tone: 'amber',
      text: `${who(s)} holds its lock and has written nothing for ${span(s.quietMs / 1000)}${s.holder?.host ? `, on ${s.holder.host}${s.holder.pid != null ? ` (pid ${s.holder.pid})` : ''}` : ''}.`,
      hint: 'A runner that reads writes its position every few seconds: its reads fail (its log says why), or a function does not return.',
    }
  }
  if (row.status === 'gone') {
    const s = row.shards.find((x) => x.state === 'gone')
    const it = who(s) === 'It' ? 'it' : who(s).toLowerCase()
    const lead = s.updatedMs != null ? `Nothing has run ${it} for ${span((now - s.updatedMs) / 1000)}` : `Nothing runs ${it}`
    return {
      tone: 'amber',
      text: `${lead}: its lock is free, and its last commit does not say it stopped.`,
      hint: 'A crash or a kill ends a runner without a word. Start it again; a stream nobody needs any more keeps its keys until they are deleted.',
    }
  }
  if (row.status === 'never') {
    return {
      tone: 'amber',
      text: 'Nothing runs it, and it has committed nothing: only its layout or its description is here.',
      hint: 'A runner that started and failed before its first commit, or a stream removed but for those keys.',
    }
  }
  if (row.status === 'stopped') {
    const at = Math.max(...row.shards.map((s) => msOf(s.stopped?.at) ?? -Infinity))
    return { tone: '', text: `Its process stopped it${Number.isFinite(at) ? ` ${ago(at, now)}` : ''}. It goes on from its position when it starts again.`, hint: null }
  }
  if (row.status === 'partial') {
    const n = row.shards.filter((s) => s.state === 'stopped').length
    return { tone: '', text: `${n} of ${row.shardCount} shards were stopped by their process; the others run.`, hint: null }
  }
  return null
}

/**
 * Its stored state, in words: `4.2 MB in 2 layers, as of 38 s ago`. A
 * snapshot of no bytes is the state where it was empty: what is open now is
 * rebuilt at a restart by reading the feed again from that position.
 */
export function stateSavedText(row) {
  const s = row.snapshot
  if (!s) return null
  if (!s.bytes) {
    if (!row.open) return 'nothing to keep'
    return s.position != null
      ? `none written yet: a restart reads the feed again from position ${fmt(s.position)}`
      : 'none written yet: a restart reads the feed again'
  }
  const layers = Number.isFinite(s.layers) && s.layers > 0 ? ` in ${s.layers} ${plural(s.layers, 'layer')}` : ''
  const as = s.clockMs != null ? `, as of ${ago(s.clockMs, row.now ?? Date.now())}` : ''
  return `${bytes(s.bytes)}${layers}${as}`
}

/** When it last committed: `just now`, `2 min ago`, and for shards the latest of them. */
export function committedText(row) {
  if (row.lastMs == null) return 'nothing yet'
  const d = (row.now ?? Date.now()) - row.lastMs
  const when = d < 2000 ? 'just now' : `${span(d / 1000)} ago`
  const at = row.position != null ? `position ${fmt(row.position)}, ` : ''
  return row.shardCount > 1 ? `${when}, the latest of ${row.shardCount} shards` : `${at}${when}`
}

/** Where it runs, in words: `qf-run-1, pid 4242` or `4 processes on 2 hosts`. */
export function runsOnText(row) {
  if (row.shardCount === 1) {
    const h = row.shards[0].holder
    if (!h) return null
    return h.host ? `${h.host}${h.pid != null ? `, pid ${h.pid}` : ''}` : h.owner
  }
  if (!row.owners) return null
  const held = row.shards.filter((s) => s.holder)
  if (row.owners === 1) {
    const h = held[0].holder
    const where = h.host ? `${h.host}${h.pid != null ? `, pid ${h.pid}` : ''}` : h.owner
    return held.length === row.shardCount ? `${where}, all ${row.shardCount} shards` : `${where}, ${held.length} of ${row.shardCount} shards`
  }
  return `${row.owners} ${plural(row.owners, 'process', 'processes')} on ${row.hosts.length} ${plural(row.hosts.length, 'host')}`
}
