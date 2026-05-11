import { Queen, Stream, tokenBucketGate } from 'queen-mq'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen({ url })

const queueChannel = 'channel-rl'

await q.queue(queueChannel).delete()

await q.queue(queueChannel).config({
  leaseTime: 1, // fast back-off after a DENY
  retryLimit: 10000, // denies are not failures; allow many redeliveries
  retentionEnabled: true,
  retentionSeconds: 3600,
  completedRetentionSeconds: 3600
}).create()

async function push() {
  let start = Date.now()
  while (true) {
    let partition = Math.floor(Math.random() * 10)
    const arrayOfMessages = Array.from({ length: 100 }, (_, i) => ({
      data: {
        tenantId: 'tenant-' + partition,
        partition,
        pushedAt: Date.now()
      }
    }))
    await q.queue(queueChannel).partition('tenant-' + partition).push(arrayOfMessages)
    if (Date.now() - start > 60000) {
      break
    }
  }
}

push()

const tenantReqS = {}

function recordAndRate(tenantId, windowMs = 1000) {
  const now = Date.now()
  let t = tenantReqS[tenantId]
  if (!t) {
    t = tenantReqS[tenantId] = { times: [] }
    tenantReqS[tenantId] = t
  }
  t.times.push(now)
  // Drop everything older than the window.
  const cutoff = now - windowMs
  while (t.times.length && t.times[0] < cutoff) t.times.shift()
  // Rate = events in window / window length, scaled to per-second.
  return (t.times.length * 1000) / windowMs
}

const stream = await Stream
  .from(q.queue(queueChannel))
  .gate(tokenBucketGate({ 
    capacity: 100,    // ≈ refillPerSec × leaseSec (1s of burst)
    refillPerSec: 100 // 100 tokens/sec
  }))
  .map(msg =>  {
    const { tenantId } = msg
    const rate = recordAndRate(tenantId, 10000)
    console.log(`msg tenant=${tenantId} rate=${rate.toFixed(1)} req/s`)
    return msg
  })
  .run({
    queryId:        `channel-rate-limiter-111`,
    url,
    batchSize:      50,
    maxPartitions:  10,         // one slot per tenant for parallel draining
    maxWaitMillis:  100,
    reset:          true
  })
