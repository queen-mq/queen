
// The sweep is periodic AND backs off, so the wait below is a bound, not a guess.
//
// retention.rs gives a partition a strike for every visit that deletes nothing
// and skips it for BACKOFF_BASE_CYCLES (8) cycles after the first one. The
// sweep that runs while these 100 messages are still younger than
// `retentionSeconds` is exactly such a fruitless visit, so the partition is
// parked for 8 * RETENTION_INTERVAL before anything can be deleted:
//
//     worst case = retentionSeconds + BACKOFF_BASE_CYCLES * RETENTION_INTERVAL
//                = 10s             + 8                   * 5s (the default)
//                = 50s
//
// The old wait was 15s, taken from a comment that assumed the broker ran with
// RETENTION_INTERVAL=2000 -- which no stack in test/compose has ever set. It
// could not pass once the backoff landed (751435f5), and it is why `suites (js)`
// was 172/173 red on master for weeks. Measured on the single stack: still
// present at 15s/25s/35s, deleted by 50s.
//
// Do NOT turn this into a poll loop: pop() CONSUMES, so a poll that pops early
// empties the queue itself and the next read returns 0 whether retention ran or
// not -- a test that passes for the wrong reason.
const RETENTION_SWEEP_WAIT_MS = 65000

export async function retentionTest(client) {
    const queueName = 'test-queue-retention-01';
    const queue = await client
    .queue(queueName)
    .config({
        retentionEnabled: true,
        retentionSeconds: 10,
        completedRetentionSeconds: 5
    })
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    for (let i = 0; i < 100; i++) {
        const res = await client
        .queue(queueName)
        .push([{ data: { message: i } }])
    }

    await new Promise(resolve => setTimeout(resolve, RETENTION_SWEEP_WAIT_MS))

    const messages = await client
    .queue(queueName)
    .batch(100)
    .wait(false)
    .pop()

    if (messages.length === 0) {
        return { success: true, message: 'Messages cleaned up' }
    }

    return { success: false, message: `retention left ${messages.length}/100 messages after ${RETENTION_SWEEP_WAIT_MS / 1000}s` }
}


// maxWaitTimeSeconds eviction, which is a different retention phase with its own
// backoff map -- so it is not subject to the bound above and 15s has held.
export async function retentionTestMaxTime(client) {
    const queueName = 'test-queue-retention-02';
    const queue = await client
    .queue(queueName)
    .config({ retentionEnabled: true, maxWaitTimeSeconds: 10 })
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created with retention enabled' }
    }
    for (let i = 0; i < 100; i++) {
        const res = await client
        .queue(queueName)
        .push([{ data: { message: i } }])
    }

    await new Promise(resolve => setTimeout(resolve, 15000))

    const messages = await client
    .queue(queueName)
    .batch(100)
    .wait(false)
    .pop()

    if (messages.length === 0) {
        return { success: true, message: 'Messages cleaned up' }
    }

    return { success: false, message: `max-wait eviction left ${messages.length}/100 messages` }
}