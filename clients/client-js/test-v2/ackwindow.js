/**
 * Ack-window honesty test (2026-07-30).
 *
 * An ack whose transactionId the broker cannot resolve to a message of that
 * partition (here: one that was never pushed) is correctly NOT applied: the
 * cursor stays put and the real messages redeliver (redelivery over loss). The
 * BUG this test pins: the broker reported such items as success=true, so the
 * client believed the ack landed while the cursor never moved -- a silent
 * redelivery livelock, and a nack/dlq in that state could never dead-letter
 * its poison message.
 *
 * Expected contract (post-fix): unresolvable items come back success=false
 * with an explicit "unresolvable" error, and the lease survives for the real
 * batch.
 *
 * Run: node run.js ackUnknownTxnMustFail
 */

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

async function popRetry(client, queue, { batch = 3, group = null, partition = null, tries = 30, mode = null } = {}) {
    for (let i = 0; i < tries; i++) {
        let b = client.queue(queue).batch(batch).wait(false)
        if (group) b = b.group(group)
        if (partition) b = b.partition(partition)
        if (mode) b = b.subscriptionMode(mode)
        const msgs = await b.pop()
        if (msgs && msgs.length > 0) return msgs
        await sleep(150)
    }
    return []
}

/**
 * Wire-only: acking a transactionId that never existed must fail explicitly,
 * for both a completed ack and a failed nack.
 */
export async function ackUnknownTxnMustFail(client) {
    const queue = 'test-queue-v2-ackwindow-unknown'
    await client.queue(queue).create()
    await client.queue(queue).partition('Default').push([
        { data: { n: 1 }, transactionId: `${queue}-tx-1` },
        { data: { n: 2 }, transactionId: `${queue}-tx-2` },
        { data: { n: 3 }, transactionId: `${queue}-tx-3` },
    ])

    const msgs = await popRetry(client, queue, { batch: 3 })
    if (msgs.length !== 3) return { success: false, message: `expected 3 messages, got ${msgs.length}` }

    const ghost = {
        transactionId: `${queue}-ghost-never-pushed`,
        partitionId: msgs[0].partitionId,
        leaseId: msgs[0].leaseId,
    }

    // Completed ack of a nonexistent txn: must NOT be reported as success.
    const r1 = await client.ack([ghost], true)
    const item1 = (r1.results || [])[0] || {}
    if (r1.success !== false || item1.success !== false) {
        return { success: false, message: `BUG: completed ack of a never-pushed txn reported success (overall=${r1.success}, item=${item1.success})` }
    }
    if (!/unresolv/i.test(item1.error || '')) {
        return { success: false, message: `unknown-txn ack rejected with wrong error: '${item1.error}'` }
    }

    // Failed nack of a nonexistent txn: must NOT be swallowed as success
    // (pre-fix this was the worst variant: the nack vanished — no retry
    // charge, no DLQ — while the client was told ok).
    const r2 = await client.ack([{ ...ghost }], false)
    const item2 = (r2.results || [])[0] || {}
    if (r2.success !== false || item2.success !== false) {
        return { success: false, message: `BUG: failed nack of a never-pushed txn reported success (overall=${r2.success}, item=${item2.success})` }
    }
    if (!/unresolv/i.test(item2.error || '')) {
        return { success: false, message: `unknown-txn nack rejected with wrong error: '${item2.error}'` }
    }

    // The real batch must still be ackable (the lease survived the rejected calls).
    const r3 = await client.ack(msgs, true)
    if (r3.success !== true) {
        return { success: false, message: `real batch ack failed after ghost-ack rejections: ${r3.error}` }
    }

    return { success: true, message: 'unknown-txn ack and nack both rejected explicitly; real batch still ackable' }
}
