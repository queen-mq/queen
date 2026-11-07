// Numbers in words, the console's way. Pure: node tests and the pure helpers
// of every page import it (useOverview.js re-exports both).

/**
 * A per-minute count as a rate a second: the mean of the last `n` complete
 * minutes (the last one is still filling). A minute with nothing in it comes
 * as null, and counts as 0 once the series has begun: a burst fades out of
 * the rate in `n` minutes, and a queue that went quiet reads 0, not the rate
 * of its last busy minutes. The gaps before the first counted minute (a queue
 * or a broker younger than `n` minutes) are left out, as `sparkValues` leaves
 * them out of a line. null: nothing counted yet.
 */
export function perSecond(minutes, n = 5) {
  const done = (minutes || []).slice(0, -1)
  const first = done.findIndex((v) => v != null)
  if (first < 0) return null
  const last = done.slice(Math.max(first, done.length - n)).map((v) => (v == null ? 0 : v))
  return last.reduce((a, b) => a + b, 0) / last.length / 60
}

/**
 * A chart's points (one per bucket, `[t, a rate a second]`) as one mean over
 * its window: the series has a point only for the buckets that had something,
 * and the others count as 0, so a two-minute burst in an hour reads as what it
 * was over the hour, not as the burst. `bucketMs` is the buckets' size; only
 * the window's complete buckets count (the last one is still filling). null:
 * no point, and no complete bucket.
 */
export function windowMean(pts, { from, to, bucketMs, now = Date.now() } = {}) {
  const sum = (pts || []).reduce((s, p) => s + (p?.[1] || 0), 0)
  const n = Number.isFinite(from) && Number.isFinite(to) && bucketMs > 0
    ? Math.floor((Math.min(to, now) - from) / bucketMs)
    : 0
  const count = Math.max(n, (pts || []).length)
  return count > 0 ? sum / count : null
}

/**
 * A live chart's "In a second": the rate now, the mean of the complete buckets
 * of the last `spanMs` (5 minutes), the ones with nothing counted as 0, as
 * perSecond does for a minute series: a burst fades out in five minutes, and
 * a queue that started pushing ten minutes ago reads its rate, not the hour's
 * mean, which the time before it started pulls down. A series that begins
 * inside the span is averaged from its first bucket. null: no point at all.
 * `pts` are `[the bucket's middle, a rate a second]`.
 */
export function liveMean(pts, { bucketMs, now = Date.now(), spanMs = 300_000 } = {}) {
  const list = pts || []
  if (!list.length || !(bucketMs > 0)) return null
  // The bucket still filling starts here; the complete ones end here.
  const end = Math.floor(now / bucketMs) * bucketMs
  let start = end - Math.max(1, Math.round(spanMs / bucketMs)) * bucketMs
  const first = Math.min(...list.map((p) => p[0] - bucketMs / 2))
  if (first > start) start = first
  const count = Math.round((end - start) / bucketMs)
  if (count <= 0) return null
  const sum = list.filter((p) => p[0] >= start && p[0] < end).reduce((s, p) => s + (p[1] || 0), 0)
  return sum / count
}

// A rate a second: whole above 100, one decimal above 10, else two.
export function rate(v) {
  if (v == null || !Number.isFinite(v)) return '–'
  const a = Math.abs(v)
  if (a >= 100) return Math.round(v).toLocaleString('en-US')
  if (a >= 10) return v.toFixed(1)
  if (a === 0) return '0'
  return v.toFixed(2).replace(/0$/, '')
}
export function span(seconds) {
  const s = Math.max(0, Math.round(seconds))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  // Round once, then split: 16 h 59.6 min reads 17 h, never "16 h 60 min".
  if (m < 24 * 60) { const h = Math.floor(m / 60); const mm = m % 60; return mm ? `${h} h ${mm} min` : `${h} h` }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24); const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}
