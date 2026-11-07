// What every script of the demo shares: the broker, the queue names, the
// partitions and the stream parameters (check.mjs computes the same answers
// in batch, so it reads them from here too).
import os from 'node:os'
import path from 'node:path'

const clientUrl = process.env.QUEEN_CLIENT ?? new URL('../../clients/client-js/client-v2/index.js', import.meta.url).href
export const { Queen, Stream } = await import(clientUrl)

export const QUEEN_URL = process.env.QUEEN_URL ?? 'http://127.0.0.1:17862'
// A prefix for every queue and stream, so runs can sit side by side on one broker.
export const PREFIX = process.env.GH_PREFIX ?? 'gh'
export const DATA = process.env.GH_DATA ?? path.join(os.homedir(), 'Work', 'queen-data', 'gharchive')

export const Q = {
  issues: `${PREFIX}-issues`,
  comments: `${PREFIX}-comments`,
  prs: `${PREFIX}-prs`,
  reviews: `${PREFIX}-reviews`,
  stars: `${PREFIX}-stars`,
  forks: `${PREFIX}-forks`,
  pushes: `${PREFIX}-pushes`
}
export const OUT = {
  unanswered: `${PREFIX}-out-unanswered`,
  prs: `${PREFIX}-out-prs`,
  trending: `${PREFIX}-out-trending`,
  bursts: `${PREFIX}-out-bursts`
}
export const QUERY = {
  unanswered: `${PREFIX}-unanswered`,
  prs: `${PREFIX}-prs`,
  trending: `${PREFIX}-trending`,
  bursts: `${PREFIX}-bursts`
}

// The archive's queue name ('gh-issues', ...) to this run's.
export const queueOf = (q) => `${PREFIX}-${q.slice(3)}`

// A day has about a million repos and half a million people pushing: one
// partition each is a lot for a broker on a laptop. Repos (and, for pushes,
// people) are hashed into 1,024 partitions; every stream keys by the repo or
// the person itself, and a partition keeps the order of each one in it.
export const BUCKETS = 1024
export function fnv1a32(text) {
  const bytes = Buffer.from(String(text), 'utf8')
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}
export const partitionOf = (e) => `p${fnv1a32(e.q === 'gh-pushes' ? e.actor : e.repo) % BUCKETS}`

// The stream parameters.
export const LATENESS = { duration: '2m', seconds: 120, ms: 120000 }
export const IDLE = { duration: '30s', seconds: 30 }
export const ANSWER_WITHIN = { duration: '1h', ms: 3600000 }
export const PR_WITHIN = { duration: '6h', ms: 6 * 3600000 }
export const BURST = { gapSeconds: 300, minPushes: 100 }
export const TOP = 10

/** The top repos of an hour: most stars first, then by name (the same whatever order they came in). */
export function topOf(list) {
  return [...list].sort((a, b) => b.stars - a.stars || (a.repo < b.repo ? -1 : a.repo > b.repo ? 1 : 0)).slice(0, TOP)
}

export const client = () => new Queen({ url: QUEEN_URL, handleSignals: false })
export const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a)
