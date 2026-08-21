import { Queen, Stream } from 'queen-mq'
import { v4 as uuid } from 'uuid'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const RUN = Date.now().toString(36)
const ingressQueue = 'rrl.ingress.price-airbnb'
const outputQueue = 'rrl.egress.price-airbnb'
const QUERY_ID = `app-js-rate-limiter-${RUN}`

const airbnbPriceLimits = {
  msg: 100,
  timeSeconds: '1s'
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })
let stream = null

try {
  await queen.queue(ingressQueue).delete()
  await queen.queue(ingressQueue).config({ 
    leaseTime: 2, 
    retryLimit: 3 })
  .create()
  
  // Producer
  const produceForever = async () => {
    let i = 0
    while (true) {
      i += 1
      const key = '01' + (i % 2)
      await queen.queue(ingressQueue).partition(key).push({
        data: { 
          partition: key, 
          price: 100,
          priceId: uuid(),
          room: key,
          at: Date.now() 
        },
      })
    }
  }

  produceForever()

  // Counters
  stream = await Stream
    .from(queen.queue(outputQueue))
    .windowTumbling({ seconds: 1, idleFlushMs: 0 })
    .aggregate({
      // The extractors receive the payload itself, not the envelope.
      requests: () => 1,
      cost: (r) => r.cost ?? 1,
    })
    //.to(queen.queue(outputQueue))
    .foreach(w => console.log('window fired:', w))  
    .run({
      queryId: QUERY_ID,
      url: QUEEN_URL,
      batchSize: 200,
      maxPartitions: 8,
      maxWaitMillis: 200,
  })

  let sent = 0
  let startDate = new Date()
  // Consumers
  await queen.queue(ingressQueue) 
  .subscriptionMode('all')
  .batch(10)
  .renewLease(true, 500)
  .concurrency(10)
  .autoAck(false)
  .each()
  .consume( async (msg) => {
    try {
      const data = msg.data
      const partitionId = msg.partitionId

      while (true) {
        const res = await queen.kv.incr('rrl', 'budget:providerX', msg.data.cost ?? 1, { 
          max: airbnbPriceLimits.msg, 
          ttl: airbnbPriceLimits.timeSeconds 
        })
        if (res.applied) {
          sent += 1
          const time = ((new Date()) - startDate)/1000      

          //console.log(new Date(), partitionId, data, sent / time)  
          await queen
          .transaction()
          .queue(outputQueue)
          .push([{ transactionId: msg.data.priceId , data: { orderId: data } }])
          .ack(msg, 'completed')
          .commit()
          break
        } else {
          
          const row = await queen.kv.get('rrl', 'budget:providerX')
          const waitMs = row.found ? new Date(row.expiresAt) - Date.now() : 0
          if (waitMs <= 1000) {
            await new Promise(r => setTimeout(r, waitMs + 1))   
            // loop and re-incr
          } else {
            return                                     // release: let the lease expire,
          }            
        }
      }
    } catch(err) {
      console.log(err)
    }    
  })
  
} catch (err) {
  console.log(err)
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  if (stream) await stream.stop()
  await queen.close()
}
// docs:end
