// Check the streams against the same day computed in batch.
//
//   node check.mjs
//
// It reads the input queues back in the order the feed gives them (the order
// the streams saw), and for each stream works out what it must have sent:
// the same watermark (latest event time minus allowedLateness), the same
// late messages left out, every remaining message in event-time order, the
// same deadlines. Then it compares that, result by result, with what the
// streams sent. Results due after the import's last watermark (the ones
// `idle` closes later, as the feed's clock goes on) are counted apart.
import {
  client, Q, OUT, LATENESS, ANSWER_WITHIN, PR_WITHIN, BURST, topOf, log
} from './common.mjs'

const queen = client()
const started = Date.now()

/** The watermark of one step: latest event time minus the lateness; an older message is late. */
class Watermark {
  max = -Infinity
  late = 0
  accept(ts) {
    if (ts < this.max - LATENESS.ms) { this.late++; return false }
    if (ts > this.max) this.max = ts
    return true
  }
  get value() { return this.max - LATENESS.ms }
}

/** A min-heap of [due, key]. */
class Heap {
  a = []
  push(x) { const a = this.a; a.push(x); let i = a.length - 1; while (i > 0) { const p = (i - 1) >> 1; if (a[p][0] <= a[i][0]) break; [a[p], a[i]] = [a[i], a[p]]; i = p } }
  peek() { return this.a[0] }
  pop() { const a = this.a; const top = a[0]; const last = a.pop(); if (a.length) { a[0] = last; let i = 0; for (;;) { const l = 2 * i + 1; const r = l + 1; let m = i; if (l < a.length && a[l][0] < a[m][0]) m = l; if (r < a.length && a[r][0] < a[m][0]) m = r; if (m === i) break; [a[m], a[i]] = [a[i], a[m]]; i = m } } return top }
  get size() { return this.a.length }
}

const byTime = (x, y) => x.ts - y.ts || x.i - y.i
const round1 = (ms) => Math.round(ms / 6000) / 10
const iso = (ms) => new Date(ms).toISOString()
// One spelling of a time ('...:03Z' and '...:03.000Z' are the same instant).
const at = (s) => (s === undefined ? '' : iso(Date.parse(s)))

// ---- read the inputs in feed order ------------------------------------------
const wm = { unanswered: new Watermark(), prs: new Watermark(), trending: new Watermark(), bursts: new Watermark() }
const issues = []
const prs = []
const stars = new Map()
const pushes = []
let i = 0
const reader = queen.feed({ queues: [Q.issues, Q.comments, Q.prs, Q.reviews, Q.stars, Q.pushes], from: 'earliest', maxRecords: 10000 })
for (;;) {
  const page = await reader.read({ waitMillis: 0 })
  for (const e of page.entries) {
    const d = e.data
    const ts = Date.parse(d.t)
    i++
    // A pattern and a gather count only the messages their tests take: the
    // others need no time and move no watermark.
    if (e.queue === Q.issues || e.queue === Q.comments) {
      const start = e.queue === Q.issues && d.action === 'opened' && !d.bot
      const then = e.queue === Q.comments && !d.pr && !d.bot && d.actor !== d.author
      if (!start && !then) continue
      if (!wm.unanswered.accept(ts)) continue
      issues.push({ ts, i, key: `${d.repo}#${d.n}`, start, then })
    } else if (e.queue === Q.prs || e.queue === Q.reviews) {
      const part = e.queue === Q.prs && d.action === 'opened' ? 'opened' : e.queue === Q.reviews ? 'reviewed' : e.queue === Q.prs && d.merged === true ? 'merged' : null
      if (!part) continue
      if (!wm.prs.accept(ts)) continue
      prs.push({ ts, i, key: `${d.repo}#${d.n}`, part })
    } else if (e.queue === Q.stars) {
      if (!wm.trending.accept(ts)) continue
      const hour = iso(Math.floor(ts / 3600000) * 3600000)
      if (!stars.has(hour)) stars.set(hour, new Map())
      const h = stars.get(hour)
      h.set(d.repo, (h.get(d.repo) || 0) + 1)
    } else if (e.queue === Q.pushes) {
      if (!wm.bursts.accept(ts)) continue
      pushes.push({ ts, i, actor: d.actor, commits: d.commits ?? 0 })
    }
  }
  if (page.caughtUp) break
}
await reader.close?.()
log(`read ${i} input messages in ${((Date.now() - started) / 1000).toFixed(1)} s; late: ${Object.entries(wm).map(([k, w]) => `${k} ${w.late}`).join(', ')}`)

// ---- what each stream must have sent ------------------------------------------
function unansweredTruth() {
  const W = wm.unanswered.value
  const out = []
  const waits = new Map()
  const heap = new Heap()
  const expire = (t) => {
    while (heap.size && heap.peek()[0] <= t) {
      const [due, key] = heap.pop()
      const w = waits.get(key)
      if (w && w.due === due) {
        waits.delete(key)
        out.push({ issue: key, kind: 'unanswered', deadline: iso(w.t0 + ANSWER_WITHIN.ms) })
      }
    }
  }
  for (const m of issues.sort(byTime)) {
    if (m.ts > W) break
    expire(m.ts)
    const w = waits.get(m.key)
    if (m.then && w) {
      waits.delete(m.key)
      out.push({ issue: m.key, kind: 'answered', at: iso(m.ts), minutes: round1(m.ts - w.t0) })
    } else if (m.start && !w) {
      const due = m.ts + ANSWER_WITHIN.ms + 1
      waits.set(m.key, { t0: m.ts, due })
      heap.push([due, m.key])
    }
  }
  expire(W)
  return { W, out, final: (r) => (r.kind === 'answered' ? Date.parse(r.at) <= W : Date.parse(r.deadline) < W), key: (r) => `${r.issue}|${r.kind}|${at(r.at ?? r.deadline)}|${r.minutes ?? ''}` }
}

function prsTruth() {
  const W = wm.prs.value
  const out = []
  const runs = new Map()
  const heap = new Heap()
  const expire = (t) => {
    while (heap.size && heap.peek()[0] <= t) {
      const [due, key] = heap.pop()
      const r = runs.get(key)
      if (!r || r.due !== due) continue
      runs.delete(key)
      if (r.done) continue
      if (r.parts.opened !== undefined && r.parts.merged !== undefined && r.parts.reviewed === undefined) {
        out.push({ pr: key, kind: 'merged-without-review', deadline: iso(r.first + PR_WITHIN.ms), minutes: round1(r.parts.merged - r.parts.opened) })
      }
    }
  }
  for (const m of prs.sort(byTime)) {
    if (m.ts > W) break
    expire(m.ts)
    let r = runs.get(m.key)
    if (r && (r.done || r.parts[m.part] !== undefined)) continue
    if (!r) {
      r = { first: m.ts, due: m.ts + PR_WITHIN.ms + 1, parts: {} }
      runs.set(m.key, r)
      heap.push([r.due, m.key])
    }
    r.parts[m.part] = m.ts
    if (r.parts.opened !== undefined && r.parts.reviewed !== undefined && r.parts.merged !== undefined) {
      out.push({ pr: m.key, kind: 'reviewed-and-merged', at: iso(m.ts), minutes: round1(r.parts.merged - r.parts.opened) })
      runs.set(m.key, { done: true, due: r.due })
    }
  }
  expire(W)
  return { W, out, final: (r) => (r.kind === 'reviewed-and-merged' ? Date.parse(r.at) <= W : Date.parse(r.deadline) < W), key: (r) => `${r.pr}|${r.kind}|${at(r.at ?? r.deadline)}|${r.minutes}` }
}

function trendingTruth() {
  const W = wm.trending.value
  const out = []
  // The top step follows the per-repo step's watermark: an hour's top closes
  // as soon as the hour's windows closed and nothing older can come.
  const closed = [...stars.keys()].filter((h) => Date.parse(h) + 3600000 <= W).sort()
  for (const hour of closed) out.push({ hour, top: topOf([...stars.get(hour)].map(([repo, n]) => ({ repo, stars: n }))) })
  const all = (hour) => (stars.has(hour) ? { hour, top: topOf([...stars.get(hour)].map(([repo, n]) => ({ repo, stars: n }))) } : null)
  return { W, out, all, final: (r) => Date.parse(r.hour) + 3600000 <= W, key: (r) => `${r.hour}|${JSON.stringify(r.top)}` }
}

function burstsTruth() {
  const W = wm.bursts.value
  const gap = BURST.gapSeconds * 1000
  const out = []
  const open = new Map()
  // A session closes the millisecond after its last push + the gap.
  const close = (s) => { if (s.last + gap + 1 <= W && s.pushes >= BURST.minPushes) out.push({ actor: s.actor, pushes: s.pushes, commits: s.commits, from: iso(s.start), to: iso(s.last) }) }
  for (const p of pushes.sort(byTime)) {
    if (p.ts > W) break
    const s = open.get(p.actor)
    // A push exactly `gap` after the last one is still in the session.
    if (s && p.ts <= s.last + gap) {
      s.last = p.ts
      s.pushes++
      s.commits += p.commits
    } else {
      if (s) close(s)
      open.set(p.actor, { actor: p.actor, start: p.ts, last: p.ts, pushes: 1, commits: p.commits })
    }
  }
  for (const s of open.values()) close(s)
  return { W, out, final: (r) => Date.parse(r.to) + gap + 1 <= W, key: (r) => `${r.actor}|${at(r.from)}|${at(r.to)}|${r.pushes}|${r.commits}` }
}

// ---- what they sent, and the comparison -------------------------------------
async function sent(queue) {
  const rows = []
  const r = queen.feed({ queues: [queue], from: 'earliest', maxRecords: 10000 })
  for (;;) {
    const page = await r.read({ waitMillis: 0 })
    for (const e of page.entries) rows.push(e.data)
    if (page.caughtUp) break
  }
  await r.close?.()
  return rows
}

let ok = true
const report = []
for (const [name, truth, queue] of [
  ['unanswered', unansweredTruth(), OUT.unanswered],
  ['prs', prsTruth(), OUT.prs],
  ['trending', trendingTruth(), OUT.trending],
  ['bursts', burstsTruth(), OUT.bursts]
]) {
  const got = await sent(queue)
  const finalGot = got.filter(truth.final)
  const later = got.length - finalGot.length
  const want = new Map()
  for (const r of truth.out) want.set(truth.key(r), (want.get(truth.key(r)) || 0) + 1)
  const have = new Map()
  for (const r of finalGot) have.set(truth.key(r), (have.get(truth.key(r)) || 0) + 1)
  let missing = 0, extra = 0, dups = 0
  for (const [k, n] of want) if ((have.get(k) || 0) < n) missing += n - (have.get(k) || 0)
  for (const [k, n] of have) {
    if ((want.get(k) || 0) < n) extra += n - (want.get(k) || 0)
    if (n > 1) dups += n - 1
  }
  // Results past the import's watermark (idle closed them): right too, where it can be told.
  let laterWrong = 0
  if (name === 'trending') for (const r of got.filter((x) => !truth.final(x))) if (truth.key(r) !== truth.key(truth.all(r.hour) ?? {})) laterWrong++
  const pass = !missing && !extra && !dups && !laterWrong
  ok &&= pass
  const kinds = {}
  for (const r of finalGot) kinds[r.kind ?? 'top'] = (kinds[r.kind ?? 'top'] || 0) + 1
  report.push(`${pass ? 'PASS' : 'FAIL'} ${name}: ${truth.out.length} expected up to ${iso(truth.W).slice(0, 19)}, ${finalGot.length} sent (${JSON.stringify(kinds)}), missing ${missing}, wrong or extra ${extra}, duplicates ${dups}; ${later} more after it${name === 'trending' && later ? ` (${laterWrong} wrong)` : ''}`)
}
for (const line of report) log(line)
log(`${ok ? 'ALL EXACT' : 'MISMATCH'} in ${((Date.now() - started) / 1000).toFixed(1)} s`)
process.exit(ok ? 0 : 1)
