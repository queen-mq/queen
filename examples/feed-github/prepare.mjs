// The downloaded hours, slimmed to what the streams read, in the archive's
// own order (hour by hour, as GitHub recorded them: mostly in time, a few
// events late).
//
//   node prepare.mjs [data dir]     reads <data>/raw, writes <data>/events.ndjson
//
// One line per event: { q, id, t, repo, actor, ... }, q the queue it goes to.
// Kept: issues opened and closed, comments, pull requests opened and closed
// (merged or not), reviews, stars, forks and pushes.
import fs from 'node:fs'
import zlib from 'node:zlib'
import readline from 'node:readline'
import path from 'node:path'
import { DATA } from './common.mjs'

const dir = process.argv[2] ?? DATA
const hours = []
for (let h = 0; h < 24; h++) hours.push(`2025-10-01-${h}`)
for (let h = 0; h < 6; h++) hours.push(`2025-10-02-${h}`)

const bot = (login) => /\[bot\]$|-bot$|^dependabot|^renovate/i.test(login || '')

function slim(e) {
  const base = { id: e.id, t: e.created_at, repo: e.repo?.name, actor: e.actor?.login }
  const p = e.payload || {}
  switch (e.type) {
    case 'IssuesEvent':
      if (p.action !== 'opened' && p.action !== 'closed') return null
      return { q: 'gh-issues', ...base, action: p.action, n: p.issue?.number, bot: bot(base.actor) || undefined }
    case 'IssueCommentEvent':
      if (p.action !== 'created') return null
      // A comment on a pull request is an IssueCommentEvent too: issue.pull_request says so.
      return { q: 'gh-comments', ...base, n: p.issue?.number, pr: p.issue?.pull_request ? true : undefined, author: p.issue?.user?.login, bot: bot(base.actor) || undefined }
    case 'PullRequestEvent':
      if (p.action !== 'opened' && p.action !== 'closed') return null
      return { q: 'gh-prs', ...base, action: p.action, n: p.number ?? p.pull_request?.number, merged: p.action === 'closed' ? !!p.pull_request?.merged : undefined }
    case 'PullRequestReviewEvent':
      return { q: 'gh-reviews', ...base, n: p.pull_request?.number, state: p.review?.state }
    case 'WatchEvent':
      return { q: 'gh-stars', ...base }
    case 'ForkEvent':
      return { q: 'gh-forks', ...base }
    case 'PushEvent':
      return { q: 'gh-pushes', ...base, commits: p.size ?? p.distinct_size ?? undefined }
    default:
      return null
  }
}

const out = path.join(dir, 'events.ndjson')
const w = fs.createWriteStream(out)
const counts = {}
let kept = 0
let read = 0
const started = Date.now()
for (const hour of hours) {
  const file = path.join(dir, 'raw', `${hour}.json.gz`)
  if (!fs.existsSync(file)) {
    console.error(`missing ${file}: run ./fetch.sh first`)
    process.exit(1)
  }
  const rl = readline.createInterface({ input: fs.createReadStream(file).pipe(zlib.createGunzip()), crlfDelay: Infinity })
  for await (const line of rl) {
    if (!line) continue
    read++
    let e
    try { e = JSON.parse(line) } catch { continue }
    const s = slim(e)
    if (!s || !s.repo || !s.actor) continue
    for (const k of Object.keys(s)) if (s[k] === undefined) delete s[k]
    counts[s.q] = (counts[s.q] || 0) + 1
    kept++
    if (!w.write(JSON.stringify(s) + '\n')) await new Promise((r) => w.once('drain', r))
  }
  console.log(`${hour}: ${kept} kept of ${read} read`)
}
await new Promise((r) => w.end(r))
console.log(JSON.stringify({ out, read, kept, counts, seconds: Math.round((Date.now() - started) / 1000), mb: Math.round(fs.statSync(out).size / 1e6) }))
