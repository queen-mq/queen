import { Queen, Stream } from 'queen-mq'
const url = process.env.QUEEN_URL || 'http://localhost:6632'

const q = new Queen({ url, handleSignals: false })

const QUERY_ID = 'patient-heart-rate-aggregator'
const Queue = 'patient-heart-rate' 

await q.queue(Queue).delete()
await q.queue(Queue).create()

async function patientHeartRateProducer() {
    console.log('Starting patient heart rate producer')
    while (true) {
        for (let i = 0; i < 10; i++) {
            await q.queue(Queue).partition(`patient-${i}`).push({
                data: {
                    patientId: `patient-${i}`,
                    heartRate: Math.floor(Math.random() * 100)
                }
            })
        }
        await new Promise(resolve => setTimeout(resolve, 1))
    }
}

patientHeartRateProducer()


const handle = await Stream
  .from(q.queue(Queue))
  .windowTumbling({ seconds: 5 })
  .reduce(
    (acc, m) => {
      acc.count += 1
      acc.sum   += m.heartRate
      acc.min    = acc.min === null ? m.heartRate : Math.min(acc.min, m.heartRate)
      acc.max    = acc.max === null ? m.heartRate : Math.max(acc.max, m.heartRate)
      //acc.messages.push(m)         // collect every message that fell in this window
      return acc
    },
    { count: 0, sum: 0, min: null, max: null, messages: [] }
  )  
  //.aggregate({
  //  count: () => 1,
  //  sum:   m => m.heartRate,
  //  min:   m => m.heartRate,
  //  max:   m => m.heartRate,
  //  avg:   m => m.heartRate
  //})
  // The aggregate emit value is the {count, sum, min, max} object — we
  // attach a marker for visibility. .map runs as a post-reducer stage.
  //.map(agg => ({ ...agg, kind: 'window-emit' }))
  //.to(q.queue(SINK_QUEUE))
  .foreach(async (window, ctx) => {
    // window === { count, sum, min, max, avg, emittedAt }
    console.log('closed window:', window, ctx)
  })  
  .run({
    queryId: QUERY_ID,
    url,
    batchSize: 50,
    maxPartitions: 10,
    reset: true
  })
