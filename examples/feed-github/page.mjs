// A live page over the running streams: http://127.0.0.1:4390
//
//   node page.mjs
//
// Everything on it comes from Queen: the streams' own state, asked over
// Stream.view (who is waiting for an answer right now, this hour's stars),
// and the results they sent, read from their output queues.
import http from 'node:http'
import fs from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { client, Stream, QUERY, OUT, log } from './common.mjs'

const PORT = Number(process.env.GH_PAGE_PORT ?? 4390)
const here = path.dirname(fileURLToPath(import.meta.url))
const queen = client()

// ---- the results, read from the output queues as they come ----------------
const results = {
  unanswered: { answered: 0, unanswered: 0, minutes: [], latest: [] },
  prs: { 'reviewed-and-merged': 0, 'merged-without-review': 0, latestUnreviewed: [] },
  trending: { hours: [] },
  bursts: { count: 0, latest: [] }
}
const keep = (list, item, n) => { list.unshift(item); if (list.length > n) list.length = n }
async function tail(queue, onRow) {
  const reader = queen.feed({ queues: [queue], from: 'earliest', waitMillis: 1000 })
  for (;;) {
    try {
      const page = await reader.read({ waitMillis: 1000 })
      for (const e of page.entries) onRow(e.data)
    } catch (err) {
      log(`[page] ${queue}: ${err.message}`)
      await new Promise((r) => setTimeout(r, 1000))
    }
  }
}
tail(OUT.unanswered, (r) => {
  const u = results.unanswered
  u[r.kind]++
  if (r.kind === 'answered') { u.minutes.push(r.minutes); if (u.minutes.length > 5000) u.minutes.shift() }
  else keep(u.latest, r, 8)
})
tail(OUT.prs, (r) => {
  results.prs[r.kind]++
  if (r.kind === 'merged-without-review') keep(results.prs.latestUnreviewed, r, 6)
})
tail(OUT.trending, (r) => { keep(results.trending.hours, r, 30) })
tail(OUT.bursts, (r) => { results.bursts.count++; keep(results.bursts.latest, r, 8) })

// ---- the live state, asked of the streams ---------------------------------
const views = Object.fromEntries(Object.entries(QUERY).map(([k, id]) => [k, Stream.view(queen, id, { timeoutMillis: 1500 })]))
async function status(name) {
  try {
    const answers = await views[name].status()
    const ms = answers.map((a) => a.metrics || {})
    const sum = (k) => ms.reduce((s, m) => s + (m[k] || 0), 0)
    const marks = ms.map((m) => m.watermark).filter(Boolean).sort()
    return {
      role: [...new Set(answers.map((a) => a.role))].join('/'),
      shards: answers.length,
      read: sum('entriesRead'), outputs: sum('outputs'), late: sum('late'), open: sum('open'),
      held: ms.reduce((s, m) => s + (m.steps || []).reduce((t, st) => t + (st.held || 0), 0), 0),
      watermark: marks[0] ?? null,
      replays: sum('replayEntries')
    }
  } catch (err) {
    return { role: 'not running', error: err.message }
  }
}
const median = (xs) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)] }

async function state() {
  const names = Object.keys(QUERY)
  const st = Object.fromEntries(await Promise.all(names.map(async (n) => [n, await status(n)])))
  // Issues waiting for an answer right now: the pattern's own state.
  let waiting = []
  try {
    const { items } = await views.unanswered.list('waiting', { limit: 6 })
    waiting = items.flatMap((it) => it.waiting.map((w) => ({ issue: it.key, since: w.msg.data.t, deadline: w.deadline })))
  } catch (_) { /* not running */ }
  // The last hour that closed, and this hour's stars so far for its top repos.
  const last = results.trending.hours[0] ?? null
  let live = []
  if (last) {
    live = await Promise.all(last.top.map(async (r) => {
      try {
        const got = await views.trending.get('perRepo', r.repo)
        const open = got.found ? got.value.windows.filter((w) => w.start > last.hour) : []
        return { repo: r.repo, lastHour: r.stars, now: open.length ? open[open.length - 1].value.stars : 0 }
      } catch (_) {
        return { repo: r.repo, lastHour: r.stars, now: null }
      }
    }))
  }
  const u = results.unanswered
  return {
    streams: st,
    unanswered: { answered: u.answered, unanswered: u.unanswered, medianMinutes: median(u.minutes), latest: u.latest, waiting },
    prs: results.prs,
    trending: { hour: last?.hour ?? null, top: live, hours: results.trending.hours.length },
    bursts: results.bursts
  }
}

http.createServer(async (req, res) => {
  if (req.url === '/api/state') {
    const body = JSON.stringify(await state())
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' })
    return res.end(body)
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
  res.end(fs.readFileSync(path.join(here, 'page.html')))
}).listen(PORT, '127.0.0.1', () => log(`page on http://127.0.0.1:${PORT}`))
