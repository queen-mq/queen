/**
 * Message-semantics parity tests (RUSTFIX follow-up).
 *
 * These target the semantic contracts that differed between the old row engine
 * and the segment engine during the Rust port — the exact places where subtle
 * regressions can silently reappear:
 *
 *  1. Implicit ack: acking message N completes everything before it (v0.16.0
 *     contract). A contiguous-prefix regression would redeliver the gap.
 *  2. `retry` ack status is budget-free (never consumes retry budget).
 *  3. `dlq` ack status force-dead-letters immediately, bypassing retries.
 *  4. Unconfigured queues dead-letter by default (deadLetterQueue defaults true).
 *  5. Lease expiry does NOT consume retry budget.
 *  6. Lease-less acks succeed even after lease expiry; stale leaseIds still fail.
 *  7. subscriptionMode='new' delivers messages pushed to partitions created
 *     AFTER the group registered (durable subscription record).
 *  8. Queue-mode pops never skip backlog, even when carrying subscriptionMode.
 *  9. Empty polls while another worker holds the lease never strand backlog
 *     (wildcard watermark guard).
 * 10. Push-only (never-configured) queues get a queen.queues row: visible in
 *     resources, discoverable by namespace pop.
 * 11. GET /api/v1/messages/:pid/:txn keeps the full v0.16.0 shape (including
 *     consumerGroups[].name — not .group).
 * 12. Transactions honor `retry` and `dlq` ack statuses (not collapsed to a
 *     boolean).
 * 13. Per-request ?leaseSeconds= override wins over the queue default.
 *
 * Ack-as-commit contract (explicit signals are never skipped):
 *
 * 14. A `failed` nack in the same ack call as later completed acks clamps the
 *     cursor: the nacked message and everything after it redelivers.
 * 15. Same for `retry`: a later completed ack in the same call cannot skip it.
 * 16. A nack that resolves BELOW the committed cursor is rejected
 *     (success=false, 'already committed') instead of a silent no-op.
 * 17. A completed ack below the cursor succeeds but is flagged noop:true.
 * 18. `.each()` consumers stop the current batch after a nack: messages after
 *     the failed one are not processed on a dead lease (no double delivery).
 *
 * Run: node run.js <testName>   e.g. node run.js implicitAckCompletesBatch
 */

import { dbPool, TEST_CONFIG } from './run.js'

// Lazy: run.js imports this module before TEST_CONFIG is initialized (TDZ),
// so the base URL must be resolved at call time, not module-eval time.
const baseUrl = () => TEST_CONFIG.baseUrls[0]
const sleep = ms => new Promise(r => setTimeout(r, ms))
const uniq = prefix => `test-sem-${prefix}-${Date.now()}`

/** Raw pop via fetch — used where the client builder has no knob (leaseSeconds). */
async function rawPop(queue, params = {}) {
    const qs = new URLSearchParams({ batch: '1', wait: 'false', ...params })
    const res = await fetch(`${baseUrl()}/api/v1/pop/queue/${queue}?${qs}`)
    if (res.status === 204) return []
    if (!res.ok) throw new Error(`pop http ${res.status}`)
    const body = await res.json()
    return body.messages || []
}

/** Pop retrying briefly — rides out push→visibility latency (fusion hold). */
async function popRetry(client, queue, { group = null, batch = 1, tries = 20 } = {}) {
    for (let i = 0; i < tries; i++) {
        let b = client.queue(queue).batch(batch).wait(false)
        if (group) b = b.group(group)
        const msgs = await b.pop()
        if (msgs.length > 0) return msgs
        await sleep(150)
    }
    return []
}

async function dlqCount(client, queue) {
    const res = await client.queue(queue).dlq().limit(50).get()
    return (res && res.messages) ? res.messages.length : 0
}

// ============================================================================
// 1. Implicit ack — acking the LAST message completes the whole batch
// ============================================================================
export async function implicitAckCompletesBatch(client) {
    const queue = uniq('implicit-ack')
    await client.queue(queue).config({ leaseTime: 1 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3, 4, 5].map(n => ({ data: { n } })))

    const msgs = await popRetry(client, queue, { batch: 5 })
    if (msgs.length !== 5) {
        return { success: false, message: `Expected 5 messages, got ${msgs.length}` }
    }

    // Ack ONLY the last message of the batch.
    const ackRes = await client.ack(msgs[4])
    if (!ackRes.success) {
        return { success: false, message: `Ack of last message failed: ${ackRes.error}` }
    }

    // Wait past lease expiry: a contiguous-prefix engine would redeliver 1-4
    // here; the v0.16.0 contract implicitly completed them.
    await sleep(2500)

    const again = await client.queue(queue).batch(10).wait(false).pop()
    if (again.length !== 0) {
        return {
            success: false,
            message: `Implicit-ack regression: ${again.length} messages redelivered after acking only the last one`
        }
    }
    return { success: true, message: 'Acking the last message implicitly completed the whole batch' }
}

// ============================================================================
// 1b. Out-of-order ack — earlier unacked skipped, later unacked redelivered
// ============================================================================
export async function outOfOrderAckSkipsEarlierOnly(client) {
    const queue = uniq('ooo-ack')
    await client.queue(queue).config({ leaseTime: 1 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const msgs = await popRetry(client, queue, { batch: 3 })
    if (msgs.length !== 3) {
        return { success: false, message: `Expected 3 messages, got ${msgs.length}` }
    }

    // Ack only the MIDDLE message: #1 is implicitly completed (cursor moves
    // past it), #3 stays unacked and must redeliver after lease expiry.
    const ackRes = await client.ack(msgs[1])
    if (!ackRes.success) {
        return { success: false, message: `Ack of middle message failed: ${ackRes.error}` }
    }

    await sleep(2500)

    const again = await client.queue(queue).batch(10).wait(false).pop()
    const txs = again.map(m => m.transactionId)
    if (txs.length !== 1 || txs[0] !== `${queue}-tx-3`) {
        return {
            success: false,
            message: `Expected only tx-3 redelivered, got [${txs.join(', ')}]`
        }
    }
    return { success: true, message: 'Out-of-order ack: #1 implicitly completed, #3 redelivered' }
}

// ============================================================================
// 2+4. `retry` status never consumes budget; unconfigured queue DLQs by default
// ============================================================================
export async function retryStatusIsBudgetFree(client) {
    // Deliberately NOT configured: push-only queue, server defaults
    // (retryLimit=3, deadLetterQueue=true).
    const queue = uniq('retry-free')
    await client.queue(queue).partition('Default')
        .push([{ data: { poison: true }, transactionId: `${queue}-tx` }])

    // 6 retry cycles — twice the default retry budget. Every cycle must
    // redeliver; a budget-charging regression would DLQ/drop around cycle 4.
    for (let i = 0; i < 6; i++) {
        const msgs = await popRetry(client, queue)
        if (msgs.length !== 1) {
            return { success: false, message: `Cycle ${i}: message not redelivered after 'retry' ack` }
        }
        const res = await client.ack(msgs[0], 'retry')
        if (!res.success) {
            return { success: false, message: `Cycle ${i}: retry ack failed: ${res.error}` }
        }
    }

    if (await dlqCount(client, queue) !== 0) {
        return { success: false, message: `'retry' acks leaked into the DLQ` }
    }

    // Now exhaust the real budget with 'failed' acks: with retryLimit=3 the
    // message must dead-letter by the 4th failed ack — proving both that
    // retries above didn't charge, and that DLQ is on by default (item 2).
    let dlqSeen = false
    for (let i = 0; i < 5; i++) {
        const msgs = await popRetry(client, queue)
        if (msgs.length === 0) break // gone from the queue — check DLQ below
        await client.ack(msgs[0], false, { error: 'sem-test failure' })
        await sleep(100)
    }
    dlqSeen = (await dlqCount(client, queue)) === 1

    if (!dlqSeen) {
        return { success: false, message: 'Message never reached the DLQ after exhausting failed acks (DLQ-by-default broken?)' }
    }
    return { success: true, message: `6 budget-free retries, then DLQ'd on failed-ack exhaustion with default config` }
}

// ============================================================================
// 3. `dlq` status force-dead-letters immediately
// ============================================================================
export async function dlqStatusBypassesRetries(client) {
    const queue = uniq('force-dlq')
    await client.queue(queue).partition('Default')
        .push([{ data: { poison: true } }])

    const msgs = await popRetry(client, queue)
    if (msgs.length !== 1) return { success: false, message: 'Message not delivered' }

    const res = await client.ack(msgs[0], 'dlq', { error: 'forced by test' })
    if (!res.success) return { success: false, message: `dlq ack failed: ${res.error}` }

    await sleep(300)
    if (await dlqCount(client, queue) !== 1) {
        return { success: false, message: `Forced 'dlq' ack did not dead-letter immediately` }
    }
    const again = await client.queue(queue).batch(5).wait(false).pop()
    if (again.length !== 0) {
        return { success: false, message: 'Message still deliverable after forced DLQ' }
    }
    return { success: true, message: `'dlq' status dead-lettered on first delivery, bypassing retries` }
}

// ============================================================================
// 5. Lease expiry does NOT consume retry budget
// ============================================================================
export async function leaseExpiryDoesNotChargeRetryBudget(client) {
    const queue = uniq('expiry-budget')
    await client.queue(queue).config({ leaseTime: 1, retryLimit: 2 }).create()
    await client.queue(queue).partition('Default')
        .push([{ data: { poison: true } }])

    // 4 expiry cycles (double the budget): pop without acking, let the lease
    // lapse. A delivery-counting engine would DLQ/drop around cycle 3.
    for (let i = 0; i < 4; i++) {
        const msgs = await popRetry(client, queue)
        if (msgs.length !== 1) {
            return { success: false, message: `Cycle ${i}: message not redelivered after lease expiry (budget charged?)` }
        }
        await sleep(1600) // > leaseTime
    }
    if (await dlqCount(client, queue) !== 0) {
        return { success: false, message: 'Lease expiries dead-lettered the message' }
    }

    // The full explicit-fail budget must still be available now.
    let dlqSeen = false
    for (let i = 0; i < 4; i++) {
        const msgs = await popRetry(client, queue)
        if (msgs.length === 0) break
        await client.ack(msgs[0], false, { error: 'sem-test failure' })
        await sleep(100)
    }
    dlqSeen = (await dlqCount(client, queue)) === 1
    if (!dlqSeen) {
        return { success: false, message: 'Message never reached DLQ on explicit failures after the expiry cycles' }
    }
    return { success: true, message: '4 lease expiries charged nothing; explicit failures still had the full budget' }
}

// ============================================================================
// 6. Lease-less ack succeeds after expiry; stale leaseId still fails
// ============================================================================
export async function leaselessAckAfterExpirySucceeds(client) {
    const queue = uniq('leaseless-ack')
    await client.queue(queue).config({ leaseTime: 1 }).create()
    await client.queue(queue).partition('Default')
        .push([{ data: { n: 1 }, transactionId: `${queue}-tx-1` }])

    const msgs = await popRetry(client, queue)
    if (msgs.length !== 1) return { success: false, message: 'Message not delivered' }
    const staleLease = msgs[0].leaseId

    await sleep(2000) // lease is now expired

    // (a) Ack WITHOUT a leaseId: v0.16.0 skipped lease validation entirely —
    // this must succeed and advance the cursor.
    const bare = { transactionId: msgs[0].transactionId, partitionId: msgs[0].partitionId }
    const okRes = await client.ack(bare)
    if (!okRes.success) {
        return { success: false, message: `Lease-less ack after expiry failed: ${okRes.error}` }
    }
    const again = await client.queue(queue).batch(5).wait(false).pop()
    if (again.length !== 0) {
        return { success: false, message: 'Lease-less ack did not advance the cursor (message redelivered)' }
    }

    // (b) A STALE leaseId must still be rejected: push a second message, pop
    // it, let the lease expire, then ack carrying the expired leaseId.
    await client.queue(queue).partition('Default')
        .push([{ data: { n: 2 }, transactionId: `${queue}-tx-2` }])
    const msgs2 = await popRetry(client, queue)
    if (msgs2.length !== 1) return { success: false, message: 'Second message not delivered' }
    await sleep(2000)
    const staleRes = await client.ack({
        transactionId: msgs2[0].transactionId,
        partitionId: msgs2[0].partitionId,
        leaseId: msgs2[0].leaseId || staleLease
    })
    if (staleRes.success) {
        return { success: false, message: 'Ack with an expired leaseId was accepted (must fail)' }
    }
    return { success: true, message: 'Lease-less ack succeeded post-expiry; stale leaseId rejected' }
}

// ============================================================================
// 7. subscriptionMode='new' + partition created AFTER registration
// ============================================================================
export async function subscriptionNewDeliversLatePartitions(client) {
    const queue = uniq('late-part')
    const group = `${queue}-cg`
    await client.queue(queue).create()

    // Backlog before subscription — must be skipped by mode 'new'.
    await client.queue(queue).partition('p-early')
        .push([{ data: { phase: 'backlog' }, transactionId: `${queue}-backlog` }])
    await sleep(300)

    // Register the group (first pop with mode 'new').
    const first = await client.queue(queue).group(group)
        .subscriptionMode('new').batch(5).wait(false).pop()
    if (first.length !== 0) {
        return { success: false, message: `mode 'new' delivered pre-subscription backlog (${first.length} msgs)` }
    }

    // A BRAND-NEW partition appears after registration and receives a message.
    // The v0.16.0 contract: everything pushed after the subscription timestamp
    // is delivered, even on partitions the group has never touched.
    await client.queue(queue).partition('p-late')
        .push([{ data: { phase: 'late' }, transactionId: `${queue}-late` }])
    await sleep(300)

    const got = await popRetry(client, queue, { group, batch: 5 })
    const txs = got.map(m => m.transactionId)
    if (!txs.includes(`${queue}-late`)) {
        return {
            success: false,
            message: `Late-created partition's message was skipped (got [${txs.join(', ')}]) — durable-subscription seeding broken`
        }
    }
    if (txs.includes(`${queue}-backlog`)) {
        return { success: false, message: 'Pre-subscription backlog was delivered to a mode-new group' }
    }
    return { success: true, message: `Late-created partition delivered its post-subscription message` }
}

// ============================================================================
// 8. Queue-mode pop carrying subscriptionMode must not skip backlog
// ============================================================================
export async function queueModePopNeverSkipsBacklog(client) {
    const queue = uniq('qmode-backlog')
    await client.queue(queue).partition('Default')
        .push([1, 2, 3].map(n => ({ data: { n } })))
    await sleep(300)

    // No consumer group + subscriptionMode='new': queue mode must ignore the
    // subscription hint and deliver the full backlog (the __QUEUE_MODE__
    // seeding-exclusion contract).
    let total = 0
    for (let i = 0; i < 20 && total < 3; i++) {
        const msgs = await client.queue(queue)
            .subscriptionMode('new').batch(10).wait(false).pop()
        for (const m of msgs) await client.ack(m)
        total += msgs.length
        if (msgs.length === 0) await sleep(150)
    }
    if (total !== 3) {
        return { success: false, message: `Queue-mode pop with subscriptionMode='new' skipped backlog: got ${total}/3` }
    }
    return { success: true, message: 'Queue-mode pop delivered full backlog despite subscriptionMode hint' }
}

// ============================================================================
// 9. Watermark: empty polls during a held lease must not strand backlog
// ============================================================================
export async function leasedBacklogNotStrandedByEmptyPolls(client) {
    const queue = uniq('watermark-lease')
    const group = `${queue}-cg`
    await client.queue(queue).config({ leaseTime: 3 }).create()

    await client.queue(queue).partition('Default')
        .push([{ data: { n: 1 }, transactionId: `${queue}-tx` }])

    // Worker A claims the only partition and never acks.
    const a = await popRetry(client, queue, { group })
    if (a.length !== 1) return { success: false, message: 'Worker A did not get the message' }

    // Worker B (same group) polls empty repeatedly while A holds the lease.
    // A buggy engine advances the empty-scan watermark here even though the
    // backlog is only invisible because of A's lease.
    for (let i = 0; i < 3; i++) {
        await client.queue(queue).group(group).batch(1).wait(false).pop()
        await sleep(100)
    }

    // Defeat the 2-minute candidate-filter grace so a wrongly-advanced
    // watermark actually hides the partition (same trick as watermark.js).
    // Queue identity is now the queen.queues id (log_queues merged away).
    await dbPool.query(`
        UPDATE queen.log_partitions p
        SET last_write_at = last_write_at - interval '10 minutes'
        FROM queen.queues q
        WHERE p.queue_id = q.id AND q.name = $1
    `, [queue])

    // Wait for A's lease to lapse. NO new push arrives.
    await sleep(3500)

    const b = await popRetry(client, queue, { group, tries: 10 })
    if (b.length !== 1) {
        return {
            success: false,
            message: 'Backlog stranded: empty polls during a held lease advanced the watermark past unconsumed data'
        }
    }
    return { success: true, message: 'Backlog survived lease churn + empty polls (watermark guard holds)' }
}

// ============================================================================
// 10. Push-only queue: resources row + namespace discovery
// ============================================================================
export async function pushOnlyQueueIsDiscoverable(client) {
    // Dotted name → namespace/task derived at push time (v0.16.0 contract).
    const ns = `test-sem-ns-${Date.now()}`
    const queue = `${ns}.taskA.q`
    await client.queue(queue).partition('Default')
        .push([{ data: { n: 1 }, transactionId: `${queue}-tx` }])
    await sleep(300)

    // (a) The queue must exist in resources with derived namespace/task.
    const res = await fetch(`${baseUrl()}/api/v1/resources/queues`)
    if (!res.ok) return { success: false, message: `resources/queues http ${res.status}` }
    const body = await res.json()
    const list = Array.isArray(body) ? body : (body.queues || [])
    const entry = list.find(q => (q.name || q.queue) === queue)
    if (!entry) {
        return { success: false, message: 'Push-only queue missing from /api/v1/resources/queues (no queen.queues row?)' }
    }
    const entryNs = entry.namespace ?? entry.ns
    if (entryNs !== ns) {
        return { success: false, message: `Namespace not derived from queue name: got '${entryNs}', want '${ns}'` }
    }

    // (b) Namespace-discovery pop must find it.
    let found = false
    for (let i = 0; i < 10 && !found; i++) {
        const qs = new URLSearchParams({ namespace: ns, batch: '1', wait: 'false', consumerGroup: `${ns}-cg` })
        const popRes = await fetch(`${baseUrl()}/api/v1/pop?${qs}`)
        if (popRes.status === 200) {
            const pb = await popRes.json()
            found = (pb.messages || []).length > 0
        }
        if (!found) await sleep(200)
    }

    // Cleanup: queue identity is now the queen.queues id, and deleting the
    // queues row cascades partitions/watermarks/metadata/lag-metrics. Only
    // log_txns/log_dlq are FK-less by design → explicit purge via partitions.
    await dbPool.query(`
        WITH parts AS (
          SELECT lp.id FROM queen.log_partitions lp
          JOIN queen.queues q ON q.id = lp.queue_id
          WHERE q.name = $1
        ),
        d1 AS (DELETE FROM queen.log_txns WHERE partition_id IN (SELECT id FROM parts)),
        d2 AS (DELETE FROM queen.log_dlq  WHERE partition_id IN (SELECT id FROM parts))
        SELECT 1`, [queue])
    await dbPool.query(`DELETE FROM queen.queues WHERE name = $1`, [queue])

    if (!found) {
        return { success: false, message: 'Namespace discovery pop never found the push-only queue' }
    }
    return { success: true, message: 'Push-only queue visible in resources and namespace-discoverable' }
}

// ============================================================================
// 11. Message detail endpoint contract (webapp-facing shape)
// ============================================================================
export async function messageDetailKeepsFullShape(client) {
    const queue = uniq('msg-detail')
    const group = `${queue}-cg`
    const tx = `${queue}-tx`
    await client.queue(queue).create()
    await client.queue(queue).partition('Default')
        .push([{ data: { hello: 'world' }, transactionId: tx }])

    // Pop (don't ack) so a consumer row exists for consumerGroups[].
    const msgs = await popRetry(client, queue, { group })
    if (msgs.length !== 1) return { success: false, message: 'Message not delivered' }
    const pid = msgs[0].partitionId

    const res = await fetch(`${baseUrl()}/api/v1/messages/${pid}/${tx}`)
    if (!res.ok) return { success: false, message: `message detail http ${res.status}` }
    const detail = await res.json()

    const requiredKeys = [
        'id', 'transactionId', 'partitionId', 'partition', 'queue', 'queuePath',
        'namespace', 'task', 'status', 'payload', 'createdAt',
        'retryCount', 'queueConfig', 'consumerGroups'
    ]
    const missing = requiredKeys.filter(k => !(k in detail))
    if (missing.length > 0) {
        return { success: false, message: `Message detail missing keys: ${missing.join(', ')}` }
    }
    if (!detail.queueConfig || !('leaseTime' in detail.queueConfig)) {
        return { success: false, message: 'queueConfig.leaseTime missing from message detail' }
    }
    if (!Array.isArray(detail.consumerGroups) || detail.consumerGroups.length === 0) {
        return { success: false, message: 'consumerGroups empty despite an active consumer' }
    }
    // The v0.16.0 contract keys each group entry by 'name' — a rename to
    // 'group' breaks the webapp silently.
    if (!('name' in detail.consumerGroups[0])) {
        return {
            success: false,
            message: `consumerGroups[0] has no 'name' key (keys: ${Object.keys(detail.consumerGroups[0]).join(', ')})`
        }
    }
    return { success: true, message: 'Message detail kept the full v0.16.0 shape' }
}

// ============================================================================
// 12. Transactions honor `retry` and `dlq` ack statuses
// ============================================================================
export async function transactionAckHonorsRetryAndDlq(client) {
    const queue = uniq('txn-status')
    await client.queue(queue).config({ retryLimit: 2 }).create()
    await client.queue(queue).partition('Default')
        .push([{ data: { poison: true }, transactionId: `${queue}-tx` }])

    // Phase A: 5 transactional 'retry' acks (budget is 2) — all must
    // redeliver, none may DLQ. A broker that collapses status to a boolean
    // charges the budget here and dead-letters around cycle 3.
    for (let i = 0; i < 5; i++) {
        const msgs = await popRetry(client, queue)
        if (msgs.length !== 1) {
            return { success: false, message: `Cycle ${i}: no redelivery after transactional 'retry' ack (status collapsed to failed?)` }
        }
        await client.transaction().ack(msgs[0], 'retry').commit()
    }
    if (await dlqCount(client, queue) !== 0) {
        return { success: false, message: `Transactional 'retry' acks dead-lettered the message` }
    }

    // Phase B: one transactional 'dlq' ack must dead-letter immediately.
    const msgs = await popRetry(client, queue)
    if (msgs.length !== 1) return { success: false, message: 'Message unavailable for phase B' }
    await client.transaction().ack(msgs[0], 'dlq').commit()
    await sleep(500)

    if (await dlqCount(client, queue) !== 1) {
        return { success: false, message: `Transactional 'dlq' ack did not dead-letter the message` }
    }
    const again = await client.queue(queue).batch(5).wait(false).pop()
    if (again.length !== 0) {
        return { success: false, message: 'Message still deliverable after transactional forced DLQ' }
    }
    return { success: true, message: 'Transactions honored retry (budget-free) and dlq (immediate) statuses' }
}

// ============================================================================
// 14. Ack-as-commit: a `failed` nack is never skipped by later completed acks
//     sent in the SAME ack call (cursor clamps at the nack).
// ============================================================================
export async function nackNotSkippedByLaterAckSameCall(client) {
    const queue = uniq('nack-clamp')
    await client.queue(queue).config({ leaseTime: 30 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3, 4, 5].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const msgs = await popRetry(client, queue, { batch: 5 })
    if (msgs.length !== 5) {
        return { success: false, message: `Expected 5 messages, got ${msgs.length}` }
    }

    // One batch ack call: #2 failed, everything else completed. The Promise.allSettled
    // pattern: parallel processing, per-message outcome, single commit.
    msgs[0]._status = 'completed'
    msgs[1]._status = 'failed'
    msgs[1]._error = 'sem-test poison'
    msgs[2]._status = 'completed'
    msgs[3]._status = 'completed'
    msgs[4]._status = 'completed'
    await client.ack(msgs)

    // Contract: the cursor must clamp just before #2. The nack releases the
    // lease, so #2..#5 redeliver immediately (later completed acks above a nack
    // are redelivered — at-least-once duplicates, never a lost nack).
    const again = await popRetry(client, queue, { batch: 10 })
    const txs = again.map(m => m.transactionId).sort()
    const want = [2, 3, 4, 5].map(n => `${queue}-tx-${n}`).sort()
    if (JSON.stringify(txs) !== JSON.stringify(want)) {
        return {
            success: false,
            message: `Nacked message skipped by later acks in the same call: redelivered [${txs.join(', ')}], want [${want.join(', ')}]`
        }
    }
    return { success: true, message: 'failed nack clamped the cursor: nacked message and later ones redelivered' }
}

// ============================================================================
// 15. Ack-as-commit: same clamp for `retry` (budget-free) in the same call
// ============================================================================
export async function retryNotSkippedByLaterAckSameCall(client) {
    const queue = uniq('retry-clamp')
    await client.queue(queue).config({ leaseTime: 30 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const msgs = await popRetry(client, queue, { batch: 3 })
    if (msgs.length !== 3) {
        return { success: false, message: `Expected 3 messages, got ${msgs.length}` }
    }

    msgs[0]._status = 'completed'
    msgs[1]._status = 'retry'
    msgs[2]._status = 'completed'
    await client.ack(msgs)

    const again = await popRetry(client, queue, { batch: 10 })
    const txs = again.map(m => m.transactionId).sort()
    const want = [2, 3].map(n => `${queue}-tx-${n}`).sort()
    if (JSON.stringify(txs) !== JSON.stringify(want)) {
        return {
            success: false,
            message: `'retry' skipped by a later ack in the same call: redelivered [${txs.join(', ')}], want [${want.join(', ')}]`
        }
    }
    if (await dlqCount(client, queue) !== 0) {
        return { success: false, message: `'retry' clamp leaked into the DLQ` }
    }
    return { success: true, message: `'retry' clamped the cursor without charging budget` }
}

// ============================================================================
// 16. Ack-as-commit honesty: a nack BELOW the committed cursor is rejected,
//     not silently swallowed as a no-op.
// ============================================================================
export async function nackBelowCursorIsRejected(client) {
    const queue = uniq('late-nack')
    await client.queue(queue).config({ leaseTime: 30 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const msgs = await popRetry(client, queue, { batch: 3 })
    if (msgs.length !== 3) {
        return { success: false, message: `Expected 3 messages, got ${msgs.length}` }
    }

    // Ack the MIDDLE message: cursor commits past #1 and #2, lease stays live
    // (batch end not reached).
    const mid = await client.ack(msgs[1])
    if (!mid.success) {
        return { success: false, message: `Ack of middle message failed: ${mid.error}` }
    }

    // Nack #1, which is now below the cursor. The server can no longer honor
    // it — it must SAY so instead of answering ok.
    const late = await client.ack(msgs[0], false, { error: 'too late' })
    if (late.success) {
        return {
            success: false,
            message: 'Nack below the committed cursor was silently accepted (must be rejected as already committed)'
        }
    }
    if (!/committed/i.test(late.error || '')) {
        return {
            success: false,
            message: `Nack below cursor rejected with wrong error: '${late.error}' (want mention of 'committed')`
        }
    }
    return { success: true, message: 'Late nack below the cursor rejected explicitly' }
}

// ============================================================================
// 17. Ack-as-commit honesty: a completed ack below the cursor succeeds but is
//     flagged noop:true (harmless duplicate commit).
// ============================================================================
export async function ackBelowCursorIsNoopFlagged(client) {
    const queue = uniq('late-ack')
    await client.queue(queue).config({ leaseTime: 30 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const msgs = await popRetry(client, queue, { batch: 3 })
    if (msgs.length !== 3) {
        return { success: false, message: `Expected 3 messages, got ${msgs.length}` }
    }

    const mid = await client.ack(msgs[1])
    if (!mid.success) {
        return { success: false, message: `Ack of middle message failed: ${mid.error}` }
    }

    // Completed ack of #1, already below the cursor: fine, but must carry noop.
    const late = await client.ack(msgs[0])
    if (!late.success) {
        return { success: false, message: `Completed ack below cursor failed: ${late.error}` }
    }
    if (late.noop !== true) {
        return {
            success: false,
            message: `Completed ack below cursor not flagged (noop=${JSON.stringify(late.noop)}, want true)`
        }
    }

    // A normal in-range ack must NOT carry the flag.
    const fresh = await client.ack(msgs[2])
    if (!fresh.success || fresh.noop === true) {
        return { success: false, message: `In-range ack wrongly flagged noop (success=${fresh.success}, noop=${fresh.noop})` }
    }
    return { success: true, message: 'Below-cursor completed ack flagged noop:true; in-range ack clean' }
}

// ============================================================================
// 18. `.each()` consumer stops the current batch after a nack — messages after
//     the failure are not processed on a dead lease (no systematic duplicates).
// ============================================================================
export async function eachStopsBatchAfterNack(client) {
    const queue = uniq('each-stop')
    await client.queue(queue).config({ leaseTime: 30, retryLimit: 1 }).create()

    await client.queue(queue).partition('Default')
        .push([1, 2, 3, 4, 5].map(n => ({ data: { n }, transactionId: `${queue}-tx-${n}` })))

    const seen = new Map() // txn -> handler invocations
    await client.queue(queue)
        .batch(5)
        .wait(false)
        .idleMillis(3000)
        .each()
        .consume(async msg => {
            seen.set(msg.transactionId, (seen.get(msg.transactionId) || 0) + 1)
            if (msg.data && msg.data.n === 2) {
                throw new Error('sem-test poison')
            }
        })

    // #2 is poison (retryLimit=1: delivered twice, then DLQ'd). Every other
    // message must be handled EXACTLY once: after the nack the client must
    // abandon the rest of the popped batch (dead lease) instead of processing
    // messages that are guaranteed to be redelivered.
    const dupes = [1, 3, 4, 5]
        .map(n => `${queue}-tx-${n}`)
        .filter(tx => (seen.get(tx) || 0) > 1)
    if (dupes.length > 0) {
        return {
            success: false,
            message: `Messages processed more than once after a mid-batch nack: ${dupes.map(tx => `${tx}x${seen.get(tx)}`).join(', ')}`
        }
    }
    const missing = [1, 3, 4, 5]
        .map(n => `${queue}-tx-${n}`)
        .filter(tx => !seen.has(tx))
    if (missing.length > 0) {
        return { success: false, message: `Messages never processed: ${missing.join(', ')}` }
    }
    if (await dlqCount(client, queue) !== 1) {
        return { success: false, message: 'Poison message did not end up in the DLQ' }
    }
    return { success: true, message: 'each() abandoned the dead-lease batch after the nack; no duplicates' }
}

// ============================================================================
// 13. Per-request ?leaseSeconds= override
// ============================================================================
export async function popLeaseSecondsOverride(client) {
    const queue = uniq('lease-override')
    const group = `${queue}-cg`
    // Configured lease is long (60s default via configure's 300? — set explicitly).
    await client.queue(queue).config({ leaseTime: 60 }).create()
    await client.queue(queue).partition('Default')
        .push([{ data: { n: 1 }, transactionId: `${queue}-tx` }])
    await sleep(300)

    // Claim with a 1-second per-request override.
    let got = []
    for (let i = 0; i < 20 && got.length === 0; i++) {
        got = await rawPop(queue, { consumerGroup: group, leaseSeconds: '1' })
        if (got.length === 0) await sleep(150)
    }
    if (got.length !== 1) return { success: false, message: 'Message not delivered on override pop' }

    // If the override took effect the lease lapses in ~1s; with the queue's
    // 60s lease this redelivery would not happen.
    await sleep(2500)
    const again = await rawPop(queue, { consumerGroup: group })
    if (again.length !== 1) {
        return {
            success: false,
            message: 'Message not redelivered after 2.5s — ?leaseSeconds=1 override ignored (queue lease used instead)'
        }
    }
    return { success: true, message: '?leaseSeconds=1 override won over the 60s queue lease' }
}
