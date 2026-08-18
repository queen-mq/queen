/**
 * Shared helpers for the kv and timers integration suites.
 *
 * Deliberately NOT registered in run.js: that runner treats every export of a
 * registered module as a test function, so a helper exported from kv.js would
 * be executed as a test and reported as one.
 *
 * WHY THERE IS NO AVAILABILITY PROBE HERE ANY MORE. There used to be one, plus
 * a `skipped()` verdict, because `QUEEN_KV_ENABLED` and `QUEEN_TIMERS_ENABLED`
 * were boot flags that defaulted to false: with them off the routes were not
 * registered and every call here answered 404, so a red suite was the DEFAULT
 * configuration's own fault and the skip existed to stop that training everyone
 * to ignore the colour.
 *
 * Those flags are gone. Kv and timers are not features, they are the broker:
 * there is no `QUEEN_PUSH_ENABLED` either. Every cell that runs this binary has
 * both surfaces, so these suites RUN, and a 404 from any of them is a bug in
 * the broker, not a configuration to detect. A test that skips is a test that
 * says nothing, and that was only tolerable while the 404 was legitimate.
 *
 * What still exists is the operator's RUNTIME kill switch (`kv_enabled`,
 * `timers_schedule_enabled`, `timers_fire_enabled` in `queen.system_state`) --
 * the maintenance-mode lever, pulled live during an incident. It answers 503
 * with `Retry-After` on the kv/timer routes and 403 on a `kv`/`timers` rider
 * inside a transaction, never 404. If a run here ever goes red against that,
 * the switch is down on the rig and somebody pulled it; it is not something
 * these tests should paper over.
 */

export const KV_NS = 'test-kv'
export const TIMER_QUEUE = 'test-timers'

export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Poll a queue until `match` accepts a message, or the deadline passes.
 *
 * WHY THIS IS A POLL AND NOT A LONG POLL, MEASURED ON A REAL RIG. A timer's
 * delivery is committed by the sweeper INSIDE PostgreSQL
 * (`log_timers_fire_v1` = DELETE + push in one transaction), so it does not
 * pass through the broker's push handler and the destination partition is not
 * marked ready in that broker's hot list. A consumer therefore sees the
 * message only at the next hot-list reseed: on a default broker
 * (QUEEN_HOTLIST_RESEED_MS = 30000) a timer scheduled for +300 ms was measured
 * arriving at +30.7 s, and a 20 s long poll timed out with the message already
 * committed in the log.
 *
 * That is a broker property, not a client one, and it is why these tests
 * assert ARRIVAL and never latency: `deliverAt` is "not before", never
 * "exactly at". The deadline here is generous on purpose -- it is sized
 * against the reseed period, not against the delay under test.
 */
export async function popUntil(client, queueName, match, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const messages = await client.queue(queueName).batch(10).wait(false).pop()
    const hit = messages.filter(match)
    if (hit.length > 0) return hit
    // Anything else in this queue is a stranger -- a delivery left behind by an
    // earlier run of this same test. It is ACKED rather than ignored, and that
    // is not tidiness: queue-mode delivery is ordered, so an unacked stranger
    // holds its lease and blocks the cursor, and the message under test would
    // never be reached inside any deadline. This is the shape of the failure
    // that a "wait longer" would never have fixed.
    for (const stranger of messages) {
      await client.ack(stranger)
    }
    if (Date.now() > deadline) return []
    await sleep(500)
  }
}
