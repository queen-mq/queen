// The Dead letters page, pure: the causes of a tenant's dead letters across
// its queues, how many each holds, whether it still happens, and the words.
// No Vue, no '@/' imports (test/deadletters.test.js).
//
// The broker folds a queue's newest dead letters into error signatures
// (GET /api/v1/analytics/dlq-signatures?queue=, the newest 200, its top 8):
// uuids and long hex words become <id>, dates <date>, numbers <n>, cut at 120
// characters. errorSignature below is that fold, ported, so a row read from
// GET /api/v1/dlq finds its cause exactly. A prefix match cannot: on stage two
// causes of smartpms.channel-sync share the head "POST …/rate-plans [<n>]".

export const LIVE_MS = 10 * 60_000 // the newest within 10 min: still happening
export const SAMPLE = 200 // the broker's sample, and the most the page reads of a queue
export const REST = '\u0000rest' // the kinds past the broker's top 8

const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`
const WORDS = ['None', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten']
const word = (n) => WORDS[n] || fmt(n)
const time = (v) => {
  const t = typeof v === 'number' ? v : Date.parse(v)
  return Number.isFinite(t) ? t : null
}

// ---- the broker's fold (server/src/rsm/facade/real/phase2/admin.rs) -----------
const UUID = /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/g
const WORD = /[A-Za-z0-9_]+/g
const DATE = /[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9:.]+Z?)?/g

/** error_signature: empty → "(no message)"; uuids and 20+ hex words → <id>; dates → <date>; digit words → <n>; one space; 120 characters. */
export function errorSignature(error) {
  const t = String(error ?? '').trim()
  let s = t || '(no message)'
  s = s.replace(UUID, '<id>')
  s = s.replace(WORD, (w) => (w.length >= 20 && /^[0-9a-fA-F]+$/.test(w) ? '<id>' : w))
  s = s.replace(DATE, '<date>')
  s = s.replace(WORD, (w) => (/^[0-9]+$/.test(w) ? '<n>' : w))
  s = s.split(/\s+/).filter(Boolean).join(' ')
  return Array.from(s).slice(0, 120).join('')
}

// A row keeps its fold: the same objects come back on every recompute.
const folded = new WeakMap()
export function sigOf(row) {
  if (!row || typeof row !== 'object') return errorSignature(row)
  let s = folded.get(row)
  if (s === undefined) { s = errorSignature(row.errorMessage); folded.set(row, s) }
  return s
}

/** The read rows that died of `cause` (a REST cause takes those of none of its queue's named ones). */
export function rowsOf(cause, rows = []) {
  if (!cause) return []
  if (cause.unknown) return rows || []
  if (cause.rest) {
    const known = new Set(cause.known || [])
    return (rows || []).filter((m) => !known.has(sigOf(m)))
  }
  return (rows || []).filter((m) => sigOf(m) === cause.text)
}

/** Split `total` in proportion to `weights`, in whole numbers that add up (largest remainder). */
export function apportion(total, weights) {
  const w = (weights || []).map((x) => Math.max(0, Number(x) || 0))
  const sum = w.reduce((a, b) => a + b, 0)
  const T = Math.max(0, Math.round(Number(total) || 0))
  if (!sum || !T) return w.map(() => 0)
  const raw = w.map((x) => (x / sum) * T)
  const out = raw.map(Math.floor)
  let left = T - out.reduce((a, b) => a + b, 0)
  const by = raw.map((r, i) => [r - Math.floor(r), i]).sort((a, b) => b[0] - a[0] || a[1] - b[1])
  for (let k = 0; k < by.length && left > 0; k++, left--) out[by[k][1]]++
  return out
}

const uniq = (xs) => [...new Set(xs.filter((x) => x != null && x !== ''))]

/**
 * Every cause across the queues, queue by queue:
 *   key, queue, text (the signature; '' for the rest), cut (it hit 120
 *   characters), rest, unknown (its queue's causes could not be read), error,
 *   n (rows of the sample with it), sample, rowsNow, causes (its queue's),
 *   single, count (its share of the queue's dead letters, apportioned so a
 *   queue's causes add up to the queue), exact (every row was in the sample
 *   and the queue has not changed since), newest / oldest (ms), full (the
 *   whole error of the newest read row), groups, retries, known (the queue's
 *   named signatures, for the rest).
 *
 * @param {Array} queues  [{ name, dead }]: the queues with dead letters
 * @param {Map}   sigs    name → { data, error }: the dlq-signatures answers
 * @param {Map}   read    name → { messages }: rows read, newest first
 */
export function buildCauses(queues = [], sigs = new Map(), read = new Map()) {
  const out = []
  for (const q of queues || []) {
    const s = sigs.get(q.name)
    if (!s) continue
    const dead = Number(q.dead) || 0
    const d = s.data
    if (!d) {
      if (s.error) out.push({ key: `${q.name}\u0000?`, queue: q.name, unknown: true, error: s.error, text: '', count: dead, exact: true, groups: [], retries: [], known: [] })
      continue
    }
    const named = (Array.isArray(d.signatures) ? d.signatures : []).map((c) => ({ text: String(c.text ?? ''), n: Number(c.n) || 0 }))
    const sample = Number(d.sample) || 0
    const rowsNow = Number(d.rowsNow) || 0
    const sum = named.reduce((a, c) => a + c.n, 0)
    const kinds = sample > sum ? [...named, { text: REST, n: sample - sum, rest: true }] : named
    if (!kinds.length) continue
    const counts = apportion(dead || rowsNow, kinds.map((k) => k.n))
    const exact = sample >= rowsNow && (!dead || dead === rowsNow)
    const known = named.map((k) => k.text)
    const knownSet = new Set(known)
    const rows = read.get(q.name)?.messages || []
    const byKind = new Map()
    for (const m of rows) {
      const k = knownSet.has(sigOf(m)) ? sigOf(m) : REST
      if (!byKind.has(k)) byKind.set(k, [])
      byKind.get(k).push(m)
    }
    const single = kinds.length === 1
    kinds.forEach((k, i) => {
      const mine = byKind.get(k.text) || []
      const times = mine.map((m) => time(m.failedAt)).filter((t) => t != null)
      let newest = times.length ? Math.max(...times) : null
      let oldest = times.length ? Math.min(...times) : null
      let groups = uniq(mine.map((m) => m.consumerGroup))
      let retries = uniq(mine.map((m) => m.retryCount)).map(Number).sort((a, b) => a - b)
      if (single) {
        // The sample's own span is the cause's: rows read since may be newer.
        newest = Math.max(newest ?? -Infinity, time(d.newest) ?? -Infinity)
        if (!Number.isFinite(newest)) newest = null
        oldest = time(d.oldest) ?? oldest
        if (Array.isArray(d.groups) && d.groups.length) groups = uniq([...d.groups, ...groups])
        if (Array.isArray(d.retryCounts) && d.retryCounts.length) retries = uniq([...d.retryCounts, ...retries]).map(Number).sort((a, b) => a - b)
      }
      out.push({
        key: `${q.name}\u0000${k.text}`,
        queue: q.name,
        text: k.rest ? '' : k.text,
        cut: !k.rest && Array.from(k.text).length >= 120,
        rest: !!k.rest,
        n: k.n,
        sample,
        rowsNow,
        causes: kinds.length,
        single,
        count: counts[i],
        exact,
        newest,
        oldest,
        full: mine.length ? String(mine[0].errorMessage ?? '').trim() : '',
        groups,
        retries,
        known,
      })
    })
  }
  return out
}

/**
 * An error cut where a line may break: after , & ? = and after a / that is
 * not part of "//" (each part ends with its mark). Machine text (URLs, JSON)
 * then wraps at its joints, not at the hyphen in "channel-stage".
 */
export function breakable(text) {
  return String(text ?? '').split(/(?<=[,&?=]|(?<![/:])\/(?!\/))/)
}

/** "54 min", "7 h", "2 d": how long ago, roughly, for a list. */
export function roughly(seconds) {
  const s = Math.max(0, Math.round(seconds))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  const h = Math.round(s / 3600)
  if (h < 24) return `${h} h`
  return `${Math.round(s / 86_400)} d`
}

/** Whether it still happens, judged at `now`. */
export const isLive = (c, now = Date.now()) => !!c && !c.gone && c.newest != null && now - c.newest < LIVE_MS

/**
 * Ranks for the causes not ranked yet, after the ones that are: still
 * happening first, then the most recent, then the biggest. Ranked once, a
 * cause keeps its place: the list never re-sorts while it is looked at.
 */
export function rankNew(ranks, causes, now = Date.now()) {
  const next = new Map(ranks)
  let top = next.size ? Math.max(...next.values()) + 1 : 0
  const fresh = (causes || []).filter((c) => !next.has(c.key))
  fresh.sort((a, b) => (isLive(b, now) - isLive(a, now))
    || ((b.newest ?? -1) - (a.newest ?? -1))
    || ((b.count ?? 0) - (a.count ?? 0))
    || a.key.localeCompare(b.key))
  for (const c of fresh) next.set(c.key, top++)
  return next
}

/**
 * The causes seen so far, merged with a fresh read: a cause the read no
 * longer lists keeps its row, as none left (its queue has none, or every row
 * was read) or as out of the sample (older rows were not read).
 *
 * @param {Map}   seen    key → cause
 * @param {Array} fresh   buildCauses' answer
 * @param {Map}   sigs    name → { data, error }, to tell "none left" from "not sampled"
 * @param {Set}   dead    names of the queues that still hold dead letters
 * @param {Set}   scope   names of the queues this read covered (null: all); others stay as they were
 */
export function mergeSeen(seen, fresh, sigs = new Map(), dead = new Set(), scope = null) {
  const next = new Map()
  const keys = new Set(fresh.map((c) => c.key))
  for (const c of fresh) next.set(c.key, c)
  for (const [k, c] of seen || []) {
    if (keys.has(k)) continue
    if (scope && !scope.has(c.queue)) { next.set(k, c); continue }
    if (!dead.has(c.queue)) { next.set(k, { ...c, gone: true, dropped: false, count: 0 }); continue }
    const d = sigs.get(c.queue)?.data
    if (!d) { next.set(k, c); continue } // not read again: as it was
    const covered = (Number(d.sample) || 0) >= (Number(d.rowsNow) || 0)
    next.set(k, { ...c, gone: covered, dropped: !covered, count: covered ? 0 : null })
  }
  return next
}

// ---- words ----------------------------------------------------------------------

/**
 * The page's one line, in parts (a queue's name is mono):
 * "8,902 dead letters in 4 queues, 8 causes. One is still happening."
 */
export function summaryParts(causes = [], { total = 0, queues = 0, queue = '', now = Date.now() } = {}) {
  const shown = causes.filter((c) => !c.gone && !c.unknown)
  const unread = causes.filter((c) => c.unknown).length
  const where = queue ? [{ t: ' in ' }, { t: queue, mono: true }] : [{ t: ` in ${plural(queues, 'queue', 'queues')}` }]
  if (!total && !unread) return [{ t: 'No dead letters' }, ...(queue ? where : []), { t: '.' }]
  const head = [{ t: plural(total, 'dead letter', 'dead letters') }, ...where]
  if (!shown.length) return [...head, { t: '. Their causes could not be read.' }]
  const one = shown.length === 1
  const live = shown.filter((c) => isLive(c, now)).length
  let tail
  if (one) tail = live ? 'It is still happening.' : 'It has stopped.'
  else if (!live) tail = 'All have stopped.'
  else tail = `${word(live)} ${live === 1 ? 'is' : 'are'} still happening.`
  const miss = unread ? ` The causes of ${plural(unread, 'queue', 'queues')} could not be read.` : ''
  return [...head, { t: `, ${one ? 'all of one cause' : plural(shown.length, 'cause', 'causes')}. ${tail}${miss}` }]
}
export const summaryLine = (causes, opts) => summaryParts(causes, opts).map((p) => p.t).join('')

/** The count in the list: "8,639", "≈ 8,639" when scaled from the sample, "–" when not known. */
export function countText(c) {
  if (!c) return '–'
  if (c.count == null) return '–'
  return (c.exact || c.gone ? '' : '≈ ') + fmt(c.count)
}

/** How the count was made, under the cause's words (the queue is named just above). */
export function countedLine(c) {
  if (!c || c.gone || c.dropped || c.unknown) return ''
  if (c.exact) {
    if (c.single) return 'Every dead letter of this queue failed this way.'
    return `One of ${c.causes} causes among its ${plural(c.rowsNow, 'dead letter', 'dead letters')}, all read.`
  }
  const older = Math.max(0, c.rowsNow - c.sample)
  const head = c.n >= c.sample
    ? `Its newest ${fmt(c.sample)} dead letters all failed this way`
    : `${fmt(c.n)} of its newest ${fmt(c.sample)} dead letters failed this way`
  const tail = older ? `; the ${fmt(older)} older ones were not read, so the count is scaled from these.` : '.'
  return head + tail
}

/** "after 5 retries", "with no retry", "after 0 to 3 retries" (from retryCount). */
export function retriesWords(retries = []) {
  const r = (retries || []).filter((x) => Number.isFinite(x))
  if (!r.length) return ''
  const lo = Math.min(...r); const hi = Math.max(...r)
  if (hi === 0) return 'with no retry'
  if (lo === hi) return `after ${plural(hi, 'retry', 'retries')}`
  return `after ${fmt(lo)} to ${fmt(hi)} retries`
}

/** Which rows a replay or a discard acts on, and by which call. */
export function actionPlan(c, { dead = 0 } = {}) {
  if (!c || c.gone || c.unknown) return null
  // Every dead letter of the queue is this cause, as far as was read: one purge.
  const whole = c.single && !c.rest
  return {
    whole,
    discardCall: whole
      ? `DELETE /api/v1/dlq?queue=${encodeURIComponent(c.queue)}`
      : 'DELETE /api/v1/messages/{partitionId}/{transactionId}',
    replayCall: 'POST /api/v1/dlq/{id}/replay {}',
    dead,
  }
}

/** A fence longer than any run of backticks in `text`, so any error stays one block. */
export function fenced(text, lang = 'text') {
  const t = String(text ?? '')
  const run = (t.match(/`+/g) || []).reduce((m, r) => Math.max(m, r.length), 0)
  const f = '`'.repeat(Math.max(3, run + 1))
  return `${f}${lang}\n${t}\n${f}`
}

/** The head of a Markdown file of one cause's dead letters. */
export function markdownHead(cause, n, { where = '', at = Date.now() } = {}) {
  const q = cause?.queue || ''
  const sig = cause?.rest ? 'other causes than the queue\'s eight named ones' : (cause?.text || '')
  return [
    `# Dead letters of ${q}`,
    '',
    `${plural(n, 'dead letter', 'dead letters')} with one cause: the newest of the queue, read from GET /api/v1/dlq?queue=${encodeURIComponent(q)}&limit=${SAMPLE}${where ? ` on ${where}` : ''} at ${new Date(at).toISOString()}.`,
    '',
    'The cause, as the broker folds it (ids as <id>, numbers as <n>, dates as <date>):',
    '',
    fenced(sig),
  ].join('\n')
}

/** One file of dead letters: what the broker returned, as JSON. */
export function asJson(rows = []) {
  return JSON.stringify(rows, null, 2) + '\n'
}

/** A file name: dead-letters-<queue>-<YYYY-MM-DD>.<ext>, safe on every system. */
export function fileName(queue, ext, now = Date.now()) {
  const d = new Date(now)
  const day = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
  return `dead-letters-${String(queue || 'all').replace(/[^A-Za-z0-9._-]+/g, '_')}-${day}.${ext}`
}
