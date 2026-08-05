/**
 * Ack-window honesty tests (2026-07-30).
 *
 * queen.log_ack_by_hash_v1 resolves txn hashes through the queen.log_txns
 * sidecar, which is purged on its own clock (GREATEST(dedup_window,
 * completed_retention, 900s)). A hash that cannot be resolved — purged row,
 * or a transactionId that never existed — is correctly NOT acked (the cursor
 * stops, frames redeliver: redelivery over loss). The BUG these tests pin:
 * the broker reported those items as success=true (they appear in neither
 * noopHashes nor staleHashes), so the client believed the ack landed while
 * the cursor never moved — a silent redelivery livelock, and a nack/dlq in
 * that state could never dead-letter its poison message.
 *
 * Expected contract (post-fix): unresolvable items come back success=false
 * with an explicit "unresolvable" error, and the cursor is untouched.
 *
 * Companion test: emptyPartitionCursorSealsAfterRetention pins the empty-
 * partition cursor seal in log_pop_v1 (the Rust port of the C++
 * pop_unified_batch_v4 starvation fix): a partition whose segments were all
 * removed by retention must stop being "phantom pending" after one pop.
 *
 * Run: node run.js ackUnknownTxnMustFail
 *      node run.js ackAfterHashPurgeMustFailExplicitly
 *      node run.js emptyPartitionCursorSealsAfterRetention
 */

import { dbPool } from './run.js'

const TENANT = '00000000-0000-0000-0000-000000000001'

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

async function partitionRow(queue, partition = 'Default') {
    const r = await dbPool.query(
        `SELECT p.id::text AS id, p.last_offset::bigint AS last_offset, p.log_start::bigint AS log_start
         FROM queen.log_partitions p
         JOIN queen.queues lq ON lq.id = p.queue_id
         WHERE lq.name = $1 AND p.name = $2 AND lq.tenant_id = $3::uuid`,
        [queue, partition, TENANT])
    return r.rows[0] || null
}

async function committedOf(partitionId, group) {
    const r = await dbPool.query(
        `SELECT c.committed::bigint AS committed FROM queen.log_consumers c
         WHERE c.partition_id = $1::uuid AND c.consumer_group = $2`,
        [partitionId, group])
    return r.rows.length ? Number(r.rows[0].committed) : null
}

async function segmentCount(partitionId) {
    const r = await dbPool.query(
        `SELECT count(*)::int AS n FROM queen.log_segments WHERE partition_id = $1::uuid`,
        [partitionId])
    return r.rows[0].n
}

/**
 * Test A (wire-only): acking a transactionId that never existed must fail
 * explicitly, for both a completed ack and a failed nack. Today the broker
 * answers success=true for both.
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

/**
 * Test B (with SQL): the real-world shape — messages still delivered (segments
 * intact) but their log_txns hash rows purged. Per-message acks must fail
 * explicitly and the cursor must not move. Pre-fix: every ack reported
 * success=true while committed stayed put (silent livelock).
 */
export async function ackAfterHashPurgeMustFailExplicitly(client) {
    const queue = 'test-queue-v2-ackwindow-purged'
    await client.queue(queue).create()
    const txns = [1, 2, 3, 4, 5].map(n => `${queue}-tx-${n}`)
    await client.queue(queue).partition('Default').push(
        txns.map((t, i) => ({ data: { n: i }, transactionId: t })))

    // Wait until the segments are visible, then purge the hash sidecar —
    // exactly what log_txns_purge_step_v1 does after the window expires
    // (the 900s floor makes the real purge untestable in-suite).
    const part = await partitionRow(queue)
    if (!part) return { success: false, message: 'partition row not found' }
    for (let i = 0; i < 30 && (await segmentCount(part.id)) === 0; i++) await sleep(100)

    const del = await dbPool.query(
        `DELETE FROM queen.log_txns WHERE partition_id = $1::uuid`, [part.id])
    if (del.rowCount === 0) return { success: false, message: 'no log_txns rows to purge — test setup broken' }

    // Segments untouched: the messages still DELIVER.
    const msgs = await popRetry(client, queue, { batch: 5 })
    if (msgs.length !== 5) return { success: false, message: `expected 5 delivered messages after purge, got ${msgs.length}` }

    // Per-message completed ack (the JS per-message consumer shape): the hash
    // cannot resolve, so the honest answer is an explicit failure.
    const r1 = await client.ack([msgs[0]], true)
    const item1 = (r1.results || [])[0] || {}
    if (item1.success !== false || !/unresolv/i.test(item1.error || '')) {
        return { success: false, message: `BUG: ack with purged hash answered success=${item1.success}, error='${item1.error}'` }
    }

    // Nack of a purged-hash message: same — and pre-fix this also meant the
    // poison could never reach the DLQ.
    const r2 = await client.ack([msgs[1]], false)
    const item2 = (r2.results || [])[0] || {}
    if (item2.success !== false || !/unresolv/i.test(item2.error || '')) {
        return { success: false, message: `BUG: nack with purged hash answered success=${item2.success}, error='${item2.error}'` }
    }

    // The cursor must not have moved (redelivery over loss — that part of the
    // contract was always right; only the reporting lied).
    const committed = await committedOf(part.id, '__QUEUE_MODE__')
    if (committed !== -1) {
        return { success: false, message: `cursor moved on unresolvable acks: committed=${committed}, expected -1` }
    }

    return { success: true, message: 'purged-hash ack and nack rejected explicitly, cursor untouched, messages still deliverable' }
}

/**
 * Test C (bug 2, with SQL): empty-partition cursor seal. A group consumes
 * part of a partition, retention then deletes ALL segments (time rule deletes
 * unconsumed data too): last_offset > committed with zero segments = a
 * phantom-pending row that used to stay a wildcard candidate forever and
 * pinned the empty-scan watermark at epoch. After the fix, one empty pop
 * seals committed to the partition's last_offset. A delayed-processing queue
 * pins the guard: segments EXIST but are deferred → no seal.
 */
export async function emptyPartitionCursorSealsAfterRetention(client) {
    const queue = 'test-queue-v2-ackwindow-seal'
    const group = 'g-seal'
    await client.queue(queue).config({ retentionEnabled: true, retentionSeconds: 1, leaseTime: 30 }).create()
    await client.queue(queue).partition('Default').push(
        [0, 1, 2, 3, 4, 5].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    // Consume + ack only the first two: committed lands at 1, the rest stays.
    // First contact for g-seal, and the 6 messages are already pushed: the
    // group has to ask for the backlog it is about to half-consume.
    const first = await popRetry(client, queue, { batch: 2, group, mode: 'all' })
    if (first.length !== 2) return { success: false, message: `expected 2 messages, got ${first.length}` }
    const ackr = await client.ack(first, true, { group })
    if (ackr.success !== true) return { success: false, message: `ack failed: ${ackr.error}` }

    const part = await partitionRow(queue)
    if (!part) return { success: false, message: 'partition row not found' }

    // Retention (interval 2s in the harness, cutoff 1s) must delete ALL
    // segments — the time rule ignores cursors.
    let segs = -1
    for (let i = 0; i < 60; i++) {
        segs = await segmentCount(part.id)
        if (segs === 0) break
        await sleep(500)
    }
    if (segs !== 0) return { success: false, message: `retention never emptied the partition (segments=${segs})` }

    const before = await committedOf(part.id, group)
    const rowNow = await partitionRow(queue)
    if (before === null) return { success: false, message: 'consumer row missing before the empty pop' }
    if (before >= Number(rowNow.last_offset)) {
        return { success: false, message: `setup broken: no phantom span (committed=${before}, last_offset=${rowNow.last_offset})` }
    }

    // ONE empty pop on the emptied partition must seal the cursor.
    const empty = await client.queue(queue).partition('Default').group(group).batch(1).wait(false).pop()
    if (empty && empty.length > 0) return { success: false, message: `expected an empty pop, got ${empty.length} messages` }

    const after = await committedOf(part.id, group)
    if (after !== Number(rowNow.last_offset)) {
        return { success: false, message: `BUG: empty pop did not seal the cursor (committed=${after}, want last_offset=${rowNow.last_offset}) — partition stays phantom-pending forever` }
    }

    // Guard: delayed messages are NOT sealed over. Segments exist but are
    // deferred; the cursor must stay put so they deliver when eligible.
    const dqueue = 'test-queue-v2-ackwindow-seal-delayed'
    await client.queue(dqueue).config({ delayedProcessing: 30, leaseTime: 30 }).create()
    await client.queue(dqueue).partition('Default').push([
        { data: { n: 0 }, transactionId: `${dqueue}-tx-0` },
        { data: { n: 1 }, transactionId: `${dqueue}-tx-1` },
    ])
    // Give the fusion flush a moment so the segments exist before the pop.
    const dpart0 = await (async () => {
        for (let i = 0; i < 30; i++) {
            const p = await partitionRow(dqueue)
            if (p && (await segmentCount(p.id)) > 0) return p
            await sleep(100)
        }
        return null
    })()
    if (!dpart0) return { success: false, message: 'delayed queue segments never appeared' }

    // First contact for g-seal on THIS queue too (metadata is queue-scoped), and
    // the 2 delayed messages are already pushed: under the default 'new' the
    // cursor would seed at last_offset and the guard below could not tell a seed
    // from a seal. 'all' seeds at -1, so a non -1 cursor can only be the seal.
    const dempty = await client.queue(dqueue).partition('Default').group(group).subscriptionMode('all').batch(1).wait(false).pop()
    if (dempty && dempty.length > 0) return { success: false, message: 'delayed message delivered early?' }
    const dcommitted = await committedOf(dpart0.id, group)
    const dsegs = await segmentCount(dpart0.id)
    if (dsegs === 0) return { success: false, message: 'delayed queue segments vanished' }
    if (dcommitted !== -1 && dcommitted !== null) {
        return { success: false, message: `BUG: seal fired over deferred-but-present segments (committed=${dcommitted}) — delayed messages would be skipped` }
    }

    return { success: true, message: 'empty partition sealed after one pop; deferred segments not sealed over' }
}
