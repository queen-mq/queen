/**
 * .gate() against a live broker: the partial ack counts SOURCE MESSAGES.
 *
 * The gate's partial ack is an offset commit -- the broker advances
 * `ack.count` messages from the head of the leased batch and keeps the lease
 * -- and the runner used to count the envelopes the gate saw instead. With a
 * .flatMap() in front, a denied message was acked together with its
 * predecessor and never came back (2026-10-02, 2.0.0-beta.6: 4 of 6 sink
 * items, message 2 lost). The unit tests pin the cycle body
 * (streams-unit/gate.test.js); this one pins the broker's side of it.
 */

import { Stream } from '../../client-v2/index.js'
import { STREAMS_URL, mkName, drainUntil, expect, summarise } from './_helpers.js'

export async function streamGateFlatMapPartialAck(client) {
  const src = mkName('streamGateFlatMapPartialAck', 'src')
  const sink = mkName('streamGateFlatMapPartialAck', 'sink')
  const queryId = mkName('streamGateFlatMapPartialAck', 'q')

  // A short lease: the denied tail comes back when it expires.
  await client.queue(src).config({ leaseTime: 2 }).create()
  await client.queue(sink).create()
  await client.queue(src).partition('tenant-1').push([{ data: { n: 1 } }, { data: { n: 2 } }, { data: { n: 3 } }])

  // Every message becomes two values; message 2 is denied the first time it
  // is seen and allowed after its redelivery.
  let deniedOnce = false
  const handle = await Stream
    .from(client.queue(src))
    .flatMap(m => [m.data, m.data])
    .gate((v) => {
      if (v.n === 2 && !deniedOnce) {
        deniedOnce = true
        return false
      }
      return true
    })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 10, maxPartitions: 1, reset: true, subscriptionMode: 'all' })

  const drained = await drainUntil(client, sink, { until: out => out.length >= 6, timeoutMs: 15000 })
  await handle.stop()

  const count = (n) => drained.filter(m => m.data.n === n).length
  const checks = [
    expect(deniedOnce, '===', true, 'the gate denied message 2 once'),
    expect(count(1), '===', 2, 'message 1 on the sink'),
    expect(count(2), '===', 2, 'message 2 on the sink after its redelivery'),
    expect(count(3), '===', 2, 'message 3 on the sink'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no errors')
  ]
  return summarise('streamGateFlatMapPartialAck', checks)
}
