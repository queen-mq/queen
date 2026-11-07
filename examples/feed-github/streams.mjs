// Four streams over the GitHub day, all on event time (the time GitHub
// recorded, not the time the events were pushed), in one process:
//
//   unanswered  an issue with no reply from someone else within an hour   (pattern)
//   prs         pull requests reviewed and merged, or merged unreviewed   (gather)
//   trending    stars per repo per hour, then each hour's top 10           (two windows)
//   bursts      sessions of 100+ pushes by one account, 5-minute gaps      (session window)
//
//   node streams.mjs                 all four
//   node streams.mjs --only bursts   some of them (comma separated)
//   node streams.mjs --shards 4      bursts in 4 shards, each with its own position and lock
//   GH_IDLE=off node streams.mjs     no idle: the watermark moves only with messages (for check.mjs)
//
// Each stream keeps its position in KV and takes over from it: stop this
// process (or kill it) and start it again, and nothing is lost or sent twice.
import {
  client, Stream, Q, OUT, QUERY, LATENESS, IDLE, ANSWER_WITHIN, PR_WITHIN, BURST, topOf, log
} from './common.mjs'

const args = process.argv.slice(2)
const only = args.includes('--only') ? args[args.indexOf('--only') + 1].split(',') : null
const shards = args.includes('--shards') ? Number(args[args.indexOf('--shards') + 1]) : 1
const idle = process.env.GH_IDLE === 'off' ? {} : { idle: IDLE.duration }
const idleSeconds = process.env.GH_IDLE === 'off' ? {} : { idle: IDLE.seconds }
const want = (name) => !only || only.includes(name)

const queen = client()
const runOpts = { waitMillis: 1000, onError: (err) => log(`[stream] ${err.message}`) }
const iso = (ms) => new Date(ms).toISOString()
const runners = {}

if (want('unanswered')) {
  runners.unanswered = await Stream.fromFeed(queen, { queues: [Q.issues, Q.comments], from: 'earliest' })
    .pattern({
      start: (m) => m.queue === Q.issues && m.data.action === 'opened' && !m.data.bot,
      then: (m) => m.queue === Q.comments && !m.data.pr && !m.data.bot && m.data.actor !== m.data.author,
      key: (m) => `${m.data.repo}#${m.data.n}`,
      within: ANSWER_WITHIN.duration,
      eventTime: (m) => m.data.t,
      allowedLateness: LATENESS.duration,
      ...idle,
      keep: (m) => ({ repo: m.data.repo, n: m.data.n, t: m.data.t }),
      onMatch: (issue, reply) => ({
        issue: `${issue.data.repo}#${issue.data.n}`,
        kind: 'answered',
        at: reply.data.t,
        minutes: Math.round((Date.parse(reply.data.t) - Date.parse(issue.data.t)) / 6000) / 10
      }),
      onTimeout: (issue) => ({
        issue: `${issue.data.repo}#${issue.data.n}`,
        kind: 'unanswered',
        deadline: iso(Date.parse(issue.data.t) + ANSWER_WITHIN.ms)
      })
    })
    .name('waiting')
    .to(queen.queue(OUT.unanswered))
    .run({ queryId: QUERY.unanswered, ...runOpts })
}

if (want('prs')) {
  runners.prs = await Stream.fromFeed(queen, { queues: [Q.prs, Q.reviews], from: 'earliest' })
    .gather({
      steps: {
        opened: (m) => m.queue === Q.prs && m.data.action === 'opened',
        reviewed: (m) => m.queue === Q.reviews,
        merged: (m) => m.queue === Q.prs && m.data.merged === true
      },
      key: (m) => `${m.data.repo}#${m.data.n}`,
      within: PR_WITHIN.duration,
      eventTime: (m) => m.data.t,
      allowedLateness: LATENESS.duration,
      ...idle,
      keep: (m) => ({ t: m.data.t }),
      onComplete: (got, pr) => ({
        pr,
        kind: 'reviewed-and-merged',
        at: [got.opened.data.t, got.reviewed.data.t, got.merged.data.t].sort().pop(),
        minutes: Math.round((Date.parse(got.merged.data.t) - Date.parse(got.opened.data.t)) / 6000) / 10
      }),
      // Only one kind of run that runs out of time is news: merged, never reviewed.
      onTimeout: (got, missing, pr) => (got.opened && got.merged && missing.length === 1
        ? {
            pr,
            kind: 'merged-without-review',
            // The deadline counts from the run's first part (on event time: the earliest).
            deadline: iso(Math.min(...Object.values(got).map((p) => Date.parse(p.data.t))) + PR_WITHIN.ms),
            minutes: Math.round((Date.parse(got.merged.data.t) - Date.parse(got.opened.data.t)) / 6000) / 10
          }
        : null)
    })
    .name('runs')
    .to(queen.queue(OUT.prs))
    .run({ queryId: QUERY.prs, ...runOpts })
}

if (want('trending')) {
  runners.trending = await Stream.fromFeed(queen, { queues: [Q.stars], from: 'earliest' })
    .keyBy((m) => m.data.repo)
    .windowTumbling({ seconds: 3600, eventTime: (m) => m.data.t, allowedLateness: LATENESS.seconds, ...idleSeconds })
    .aggregate({ stars: () => 1 })
    .name('perRepo')
    .map((v, ctx) => ({ repo: ctx.key, stars: v.stars }))
    // The step after runs on event time too: each result carries its hour's last millisecond.
    .keyBy(() => 'all')
    .windowTumbling({ seconds: 3600 })
    .reduce((top, r) => topOf([...top, r]), [])
    .name('top')
    .map((top, ctx) => ({ hour: ctx.windowKey, top }))
    .to(queen.queue(OUT.trending))
    .run({ queryId: QUERY.trending, ...runOpts })
}

if (want('bursts')) {
  runners.bursts = await Stream.fromFeed(queen, { queues: [Q.pushes], from: 'earliest' })
    .keyBy((m) => m.data.actor)
    .windowSession({ gap: BURST.gapSeconds, eventTime: (m) => m.data.t, allowedLateness: LATENESS.seconds, ...idleSeconds })
    .aggregate({ pushes: () => 1, commits: (v) => v.commits ?? 0 })
    .name('sessions')
    .filter((v) => v.pushes >= BURST.minPushes)
    .map((v, ctx) => ({
      actor: ctx.key,
      pushes: v.pushes,
      commits: v.commits,
      from: iso(ctx.windowStart),
      to: iso(ctx.windowEnd - BURST.gapSeconds * 1000)
    }))
    .to(queen.queue(OUT.bursts))
    .run({ queryId: QUERY.bursts, ...runOpts, ...(shards > 1 ? { shards } : {}) })
}

log(`running: ${Object.keys(runners).join(', ')}${shards > 1 ? ` (bursts in ${shards} shards)` : ''}${process.env.GH_IDLE === 'off' ? ', no idle' : ''}`)
const every = Number(process.env.GH_REPORT_MS ?? 5000)
setInterval(() => {
  const line = Object.entries(runners).map(([name, r]) => {
    const m = r.metrics()
    const ms = m.perShard ?? [m]
    const read = ms.reduce((s, x) => s + (x.entriesRead || 0), 0)
    const out = ms.reduce((s, x) => s + (x.outputs || 0), 0)
    const late = ms.reduce((s, x) => s + (x.late || 0), 0)
    const wm = ms.map((x) => x.watermark).filter(Boolean).sort()[0] ?? '-'
    const role = [...new Set(ms.map((x) => x.role))].join('/')
    return `${name} ${role} read ${read} out ${out} late ${late} wm ${wm.slice(0, 19)}`
  }).join(' | ')
  log(line)
}, every).unref()

const stop = async () => {
  log('stopping: committing and writing a snapshot')
  await Promise.all(Object.values(runners).map((r) => r.stop()))
  process.exit(0)
}
process.on('SIGINT', stop)
process.on('SIGTERM', stop)
setInterval(() => {}, 1 << 30)
