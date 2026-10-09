import { supervisorQueueMetrics } from './supervisorQueueMetrics.js'

export const supervisorActivityKey = Symbol('supervisorActivity')

const value = n => typeof n === 'number' && Number.isFinite(n) && n >= 0 ? n : null

// The broker owns this history. It describes ONE named queue on the acting
// cluster, including other applications consuming it; never a pod's throughput.
export function supervisorActivity(data, queue, now = Date.now()) {
  const metrics = supervisorQueueMetrics(data, queue, now)
  const minutes = data.bucketMinutes ?? 1
  if (!Number.isInteger(minutes) || minutes < 1 || minutes > 60 || 60 % minutes) throw new Error('Invalid activity bucket width')
  const width = minutes * 60_000, end = Math.floor(now / width) * width
  const start = end - 3_600_000
  const traffic = new Map(data.series.map(row => [Date.parse(row.bucket), row]))
  const backlog = new Map((data.backlog || []).map(row => [Date.parse(row.bucket), row]))
  const points = []
  for (let at = start; at < end; at += width) {
    // The request begins one hour before read time, possibly within a bucket.
    // The partial leading bucket is as unsuitable as the still-open tail.
    const complete = at >= now - 3_600_000
    const row = complete ? traffic.get(at) : null, pending = complete ? backlog.get(at) : null
    const push = value(row?.pushPerSecond), pop = value(row?.popPerSecond)
    points.push({ at, incoming: push === null ? null : push * 60,
      delivered: pop === null ? null : pop * 60, pending: value(pending?.pending) })
  }
  const samples = (data.backlog || []).filter(row => Date.parse(row.bucket) <= now && Date.parse(row.bucket) >= now - 3_600_000)
    .sort((a, b) => Date.parse(a.bucket) - Date.parse(b.bucket))
  return { ...metrics, points, minutes, readAt: now, queue,
    pendingAt: samples.length ? Date.parse(samples.at(-1).bucket) : null,
    incoming: points.at(-1)?.incoming ?? null, delivered: points.at(-1)?.delivered ?? null,
    trafficSamples: points.filter(point => point.incoming !== null || point.delivered !== null).length,
    backlogSamples: points.filter(point => point.pending !== null).length }
}

// One reader per page: repeated queues share one request and visible cards
// cannot launch an unbounded burst. Reset on refresh, source/tenant switch and
// unmount; queued reads from the old source are rejected before reaching the API.
export function createSupervisorActivityReader(read, concurrency = 3) {
  let generation = 0, active = 0, pending = []
  const cache = new Map(), controllers = new Set()
  const aborted = () => new DOMException('Activity read superseded', 'AbortError')
  function pump() {
    while (active < concurrency && pending.length) {
      const job = pending.shift(), controller = new AbortController()
      controllers.add(controller); active++
      Promise.resolve().then(() => {
        if (job.generation !== generation) throw aborted()
        return read(job.queue, controller.signal)
      }).then(result => {
        if (job.generation !== generation) throw aborted()
        job.resolve(result)
      }).catch(job.reject).finally(() => { controllers.delete(controller); active--; pump() })
    }
  }
  return {
    load(queue) {
      if (!cache.has(queue)) cache.set(queue, new Promise((resolve, reject) => {
        pending.push({ queue, generation, resolve, reject }); pump()
      }))
      return cache.get(queue)
    },
    clear() {
      generation++; cache.clear()
      for (const controller of controllers) controller.abort()
      for (const job of pending) job.reject(aborted())
      pending = []
    },
  }
}
