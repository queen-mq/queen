/**
 * Seek / delete re-consumption tests (behaviour, not bookkeeping)
 *
 * These tests verify the BEHAVIOUR a user cares about:
 *   1. Seeking a consumer group backwards re-exposes the old messages, and the
 *      group re-consumes them from the start within a short bound.
 *   2. Deleting a consumer group (all queues, or for one queue) lets a group of
 *      the same name consume every message again from the start.
 *
 * WHY THIS WAS REWRITTEN (2026-07-24)
 * -----------------------------------
 * The consumer_watermarks table is a LEGACY-path optimisation: it records when a
 * consumer last found a queue empty so an indexed empty-scan can be skipped. The
 * original tests asserted the *row* was created after an empty poll and then aged
 * it to exercise the 2-minute buffer:
 *     pl.updated_at >= (v_watermark - interval '2 minutes')
 *
 * With the wildcard hot-list ON (QUEEN_HOTLIST=1, the default) empty polls are
 * answered from the in-memory candidate ring and that bookkeeping row is NEVER
 * written — by design, not a bug: the ring, not the watermark, is the discovery
 * source. So asserting the row exists fails on the hot-list path even though
 * re-consumption works perfectly.
 *
 * The behaviour is identical on both engines, so these tests now assert the
 * behaviour directly and only exercise the watermark-age scenario on the LEGACY
 * path — detected at runtime by whether the empty poll actually wrote the row
 * (no env flag needed). On the hot-list path the broker's seek/delete handlers
 * re-seed / invalidate the ring so the reconsume is immediate; the idle-bounded
 * consume below (idleMillis 3000) is what proves "within a short bound".
 *
 * Run: node run.js seekBackwardsAllowsReconsume
 *      node run.js deleteConsumerGroupAllowsReconsume
 *      node run.js deleteConsumerGroupForQueueAllowsReconsume
 */

import { dbPool } from './run.js'

const QUEUE_NAME_SEEK = 'test-queue-v2-watermark-seek'
const QUEUE_NAME_DELETE = 'test-queue-v2-watermark-delete'

/**
 * Helper: Move the watermark FORWARD in time to simulate that partitions
 * are "old" relative to the watermark.
 *
 * The filter is: pl.updated_at >= (v_watermark - interval '2 minutes')
 *
 * If watermark = NOW + 10 minutes, then:
 *   filter = pl.updated_at >= (NOW + 10 - 2) = pl.updated_at >= NOW + 8
 *
 * Partitions updated at NOW will fail this filter (NOW < NOW + 8)
 */
async function advanceWatermark(queueName, consumerGroup, minutesForward = 10) {
    const result = await dbPool.query(`
        UPDATE queen.consumer_watermarks w
        SET last_empty_scan_at = NOW() + interval '${minutesForward} minutes',
            updated_at = NOW() + interval '${minutesForward} minutes'
        FROM queen.queues q
        WHERE w.queue_id = q.id AND q.name = $1 AND w.consumer_group = $2
        RETURNING w.*
    `, [queueName, consumerGroup])
    return result.rows[0]
}

/**
 * Helper: Check if watermark exists for a queue/consumer_group
 */
async function getWatermark(queueName, consumerGroup) {
    const result = await dbPool.query(`
        SELECT w.* FROM queen.consumer_watermarks w
        JOIN queen.queues q ON q.id = w.queue_id
        WHERE q.name = $1 AND w.consumer_group = $2
    `, [queueName, consumerGroup])
    return result.rows[0]
}

/**
 * Test: Seeking backwards should allow re-consumption of messages
 *
 * Steps:
 * 1. Push messages to queue
 * 2. Consume all messages with a consumer group (watermark advances)
 * 3. Artificially age the watermark (simulate 10 minutes passing)
 * 4. Seek the consumer group backwards to before the messages
 * 5. Try to consume again - should get all messages
 *
 * Expected: All messages are re-consumed after seek
 * Actual (bug): Watermark blocks re-consumption, 0 messages returned
 */
export async function seekBackwardsAllowsReconsume(client) {
    const consumerGroup = 'watermark-seek-test-cg'

    // Cleanup
    try {
        await client.deleteConsumerGroup(consumerGroup, true)
        // Also clean up any orphaned watermark from previous runs
        await dbPool.query(
            'DELETE FROM queen.consumer_watermarks WHERE consumer_group = $1',
            [consumerGroup]
        )
    } catch (e) {
        // Ignore
    }

    // 1. Create queue
    const queue = await client.queue(QUEUE_NAME_SEEK).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // 2. Record timestamp BEFORE pushing messages
    const timestampBeforeMessages = new Date().toISOString()

    // Small delay to ensure timestamp ordering
    await new Promise(resolve => setTimeout(resolve, 100))

    // 3. Push messages
    const messageCount = 10
    for (let i = 0; i < messageCount; i++) {
        await client
            .queue(QUEUE_NAME_SEEK)
            .partition(`p-${i}`)
            .push([{ data: { id: i, batch: 'original' } }])
    }

    console.log(`  Pushed ${messageCount} messages`)

    // 4. Consume all messages with consumer group
    let firstConsumeCount = 0
    await client
        .queue(QUEUE_NAME_SEEK)
        .group(consumerGroup)
        .subscriptionMode('all')
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            firstConsumeCount += msgs.length
        })

    console.log(`  First consume: ${firstConsumeCount} messages`)

    if (firstConsumeCount !== messageCount) {
        return {
            success: false,
            message: `First consume should get ${messageCount} messages, got ${firstConsumeCount}`
        }
    }

    // 5. Verify queue is empty for this consumer (triggers watermark advancement)
    const emptyCheck = await client
        .queue(QUEUE_NAME_SEEK)
        .group(consumerGroup)
        .batch(10)
        .wait(false)
        .pop()

    if (emptyCheck.length !== 0) {
        return {
            success: false,
            message: `Queue should be empty after first consume, got ${emptyCheck.length} messages`
        }
    }

    console.log(`  Verified queue is empty (watermark should be set)`)

    // 6. Detect which discovery path we are on. On the LEGACY path the empty poll
    //    wrote a consumer_watermarks row; on the hot-list path it did not (empty
    //    polls are served from the ring — by design). We do NOT assert the row: it
    //    is a path-specific bookkeeping detail, not the behaviour under test.
    const watermarkBefore = await getWatermark(QUEUE_NAME_SEEK, consumerGroup)
    if (watermarkBefore) {
        // Legacy path: age the watermark 10 minutes into the future so the old
        // partitions fall outside the 2-minute buffer — the original repro that
        // proves seek must reset the watermark.
        const advancedWatermark = await advanceWatermark(QUEUE_NAME_SEEK, consumerGroup, 10)
        console.log(`  [legacy] Watermark row exists; advanced to ${advancedWatermark.last_empty_scan_at} (10m future)`)
    } else {
        console.log(`  [hotlist] No watermark row (empty polls served from the ring) — skipping the watermark-age step`)
    }

    // 8. Seek backwards to BEFORE the messages were pushed
    console.log(`  Seeking to timestamp: ${timestampBeforeMessages}`)

    const seekResult = await client.admin.seekConsumerGroup(
        consumerGroup,
        QUEUE_NAME_SEEK,
        { timestamp: timestampBeforeMessages }
    )

    console.log(`  Seek result: ${JSON.stringify(seekResult)}`)

    // 9. Check if watermark was reset (it should be, but currently isn't - that's the bug)
    const watermarkAfterSeek = await getWatermark(QUEUE_NAME_SEEK, consumerGroup)
    if (watermarkAfterSeek) {
        console.log(`  WARNING: Watermark still exists after seek: ${watermarkAfterSeek.last_empty_scan_at}`)
    } else {
        console.log(`  Watermark was properly deleted after seek`)
    }

    // Small delay after seek
    await new Promise(resolve => setTimeout(resolve, 500))

    // 10. Try to consume again - should get all messages back
    let secondConsumeCount = 0
    await client
        .queue(QUEUE_NAME_SEEK)
        .group(consumerGroup)
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            secondConsumeCount += msgs.length
        })

    console.log(`  Second consume (after seek): ${secondConsumeCount} messages`)

    // 11. Verify we got all messages back within the idle-bounded consume above.
    //     A shortfall means the backward seek did not re-expose the messages
    //     promptly: on the legacy path the consumer_watermarks row was not reset
    //     (still at ${watermarkAfterSeek?.last_empty_scan_at}); on the hot-list path
    //     the ring was not re-seeded, so reconsume would only resume at the ≤30s
    //     periodic floor instead of immediately.
    if (secondConsumeCount !== messageCount) {
        return {
            success: false,
            message: `After seek backwards, should re-consume ${messageCount} messages within the idle window, but got ${secondConsumeCount}. ` +
                     `(legacy watermark after seek: ${watermarkAfterSeek?.last_empty_scan_at})`
        }
    }

    return {
        success: true,
        message: `Seek backwards allowed re-consumption of ${secondConsumeCount} messages`
    }
}

/**
 * Test: Deleting consumer group should allow fresh consumption
 *
 * Steps:
 * 1. Push messages to queue
 * 2. Consume all messages with a consumer group (watermark advances)
 * 3. Artificially age the watermark (simulate 10 minutes passing)
 * 4. Delete the consumer group
 * 5. Create a new consumer group with the SAME name
 * 6. Try to consume - should get all messages
 *
 * Expected: All messages are consumed by the "new" consumer group
 * Actual (bug): Orphaned watermark blocks consumption, 0 messages returned
 */
export async function deleteConsumerGroupAllowsReconsume(client) {
    const consumerGroup = 'watermark-delete-test-cg'

    // Cleanup
    try {
        await client.deleteConsumerGroup(consumerGroup, true)
        // Also clean up any orphaned watermark from previous runs
        await dbPool.query(
            'DELETE FROM queen.consumer_watermarks WHERE consumer_group = $1',
            [consumerGroup]
        )
    } catch (e) {
        // Ignore
    }

    // 1. Create queue
    const queue = await client.queue(QUEUE_NAME_DELETE).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // 2. Push messages
    const messageCount = 10
    for (let i = 0; i < messageCount; i++) {
        await client
            .queue(QUEUE_NAME_DELETE)
            .partition(`p-${i}`)
            .push([{ data: { id: i, batch: 'original' } }])
    }

    console.log(`  Pushed ${messageCount} messages`)

    // 3. Consume all messages with consumer group
    let firstConsumeCount = 0
    await client
        .queue(QUEUE_NAME_DELETE)
        .group(consumerGroup)
        .subscriptionMode('all')
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            firstConsumeCount += msgs.length
        })

    console.log(`  First consume: ${firstConsumeCount} messages`)

    if (firstConsumeCount !== messageCount) {
        return {
            success: false,
            message: `First consume should get ${messageCount} messages, got ${firstConsumeCount}`
        }
    }

    // 4. Verify queue is empty for this consumer (triggers watermark advancement)
    const emptyCheck = await client
        .queue(QUEUE_NAME_DELETE)
        .group(consumerGroup)
        .batch(10)
        .wait(false)
        .pop()

    if (emptyCheck.length !== 0) {
        return {
            success: false,
            message: `Queue should be empty after first consume, got ${emptyCheck.length} messages`
        }
    }

    console.log(`  Verified queue is empty (watermark should be set)`)

    // 5. Detect the discovery path (see seekBackwardsAllowsReconsume). The
    //    watermark row is a legacy-path artefact, not the behaviour under test, so
    //    we do not assert it — we only exercise the watermark-age scenario when the
    //    row was actually written (legacy path).
    const watermarkBefore = await getWatermark(QUEUE_NAME_DELETE, consumerGroup)
    if (watermarkBefore) {
        const advancedWatermark = await advanceWatermark(QUEUE_NAME_DELETE, consumerGroup, 10)
        console.log(`  [legacy] Watermark row exists; advanced to ${advancedWatermark.last_empty_scan_at} (10m future)`)
    } else {
        console.log(`  [hotlist] No watermark row (empty polls served from the ring) — skipping the watermark-age step`)
    }

    // 7. Delete the consumer group
    console.log(`  Deleting consumer group: ${consumerGroup}`)

    const deleteResult = await client.deleteConsumerGroup(consumerGroup, true)
    console.log(`  Delete result: ${JSON.stringify(deleteResult)}`)

    // 8. Check if watermark was deleted (it should be, but currently isn't - that's the bug)
    const watermarkAfterDelete = await getWatermark(QUEUE_NAME_DELETE, consumerGroup)
    if (watermarkAfterDelete) {
        console.log(`  WARNING: Orphaned watermark still exists: ${watermarkAfterDelete.last_empty_scan_at}`)
    } else {
        console.log(`  Watermark was properly deleted`)
    }

    // Small delay after delete
    await new Promise(resolve => setTimeout(resolve, 500))

    // 9. Create a "new" consumer group with the same name and try to consume
    //    Since partition_consumers was deleted, this should start fresh
    //    But if consumer_watermarks wasn't deleted, it will skip all messages
    let secondConsumeCount = 0
    await client
        .queue(QUEUE_NAME_DELETE)
        .group(consumerGroup)
        .subscriptionMode('all')
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            secondConsumeCount += msgs.length
        })

    console.log(`  Second consume (after delete + recreate): ${secondConsumeCount} messages`)

    // 10. Verify we got all messages within the idle-bounded consume above. A
    //     shortfall means the delete left stale discovery state: a legacy orphaned
    //     watermark (at ${watermarkAfterDelete?.last_empty_scan_at}), or, on the
    //     hot-list path, a surviving group ring / stale seeded-groups cache that
    //     suppressed the from-the-start reconsume.
    if (secondConsumeCount !== messageCount) {
        return {
            success: false,
            message: `After deleting CG, new CG with same name should consume ${messageCount} messages within the idle window, ` +
                     `but got ${secondConsumeCount}. (legacy watermark after delete: ${watermarkAfterDelete?.last_empty_scan_at})`
        }
    }

    return {
        success: true,
        message: `Delete + recreate allowed fresh consumption of ${secondConsumeCount} messages`
    }
}

/**
 * Test: Deleting consumer group for specific queue should allow fresh consumption
 *
 * Similar to above but uses deleteConsumerGroupForQueue instead of deleteConsumerGroup
 */
export async function deleteConsumerGroupForQueueAllowsReconsume(client) {
    const consumerGroup = 'watermark-delete-queue-test-cg'
    const queueName = 'test-queue-v2-watermark-delete-queue'

    // Cleanup
    try {
        await client.admin.deleteConsumerGroupForQueue(consumerGroup, queueName, true)
        // Also clean up any orphaned watermark from previous runs
        await dbPool.query(
            'DELETE FROM queen.consumer_watermarks w USING queen.queues q WHERE w.queue_id = q.id AND q.name = $1 AND w.consumer_group = $2',
            [queueName, consumerGroup]
        )
    } catch (e) {
        // Ignore
    }

    // 1. Create queue
    const queue = await client.queue(queueName).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // 2. Push messages
    const messageCount = 10
    for (let i = 0; i < messageCount; i++) {
        await client
            .queue(queueName)
            .partition(`p-${i}`)
            .push([{ data: { id: i, batch: 'original' } }])
    }

    console.log(`  Pushed ${messageCount} messages`)

    // 3. Consume all messages with consumer group
    let firstConsumeCount = 0
    await client
        .queue(queueName)
        .group(consumerGroup)
        .subscriptionMode('all')
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            firstConsumeCount += msgs.length
        })

    console.log(`  First consume: ${firstConsumeCount} messages`)

    if (firstConsumeCount !== messageCount) {
        return {
            success: false,
            message: `First consume should get ${messageCount} messages, got ${firstConsumeCount}`
        }
    }

    // 4. Verify queue is empty (triggers watermark)
    const emptyCheck = await client
        .queue(queueName)
        .group(consumerGroup)
        .batch(10)
        .wait(false)
        .pop()

    if (emptyCheck.length !== 0) {
        return {
            success: false,
            message: `Queue should be empty after first consume, got ${emptyCheck.length} messages`
        }
    }

    console.log(`  Verified queue is empty (watermark should be set)`)

    // 5. Detect the discovery path (see seekBackwardsAllowsReconsume). The
    //    watermark row is a legacy-path artefact, not the behaviour under test.
    const watermarkBefore = await getWatermark(queueName, consumerGroup)
    if (watermarkBefore) {
        const advancedWatermark = await advanceWatermark(queueName, consumerGroup, 10)
        console.log(`  [legacy] Watermark row exists; advanced to ${advancedWatermark.last_empty_scan_at} (10m future)`)
    } else {
        console.log(`  [hotlist] No watermark row (empty polls served from the ring) — skipping the watermark-age step`)
    }

    // 7. Delete consumer group for this specific queue
    console.log(`  Deleting consumer group for queue: ${consumerGroup} / ${queueName}`)

    const deleteResult = await client.admin.deleteConsumerGroupForQueue(consumerGroup, queueName, true)
    console.log(`  Delete result: ${JSON.stringify(deleteResult)}`)

    // 8. Check if watermark was deleted
    const watermarkAfterDelete = await getWatermark(queueName, consumerGroup)
    if (watermarkAfterDelete) {
        console.log(`  WARNING: Orphaned watermark still exists: ${watermarkAfterDelete.last_empty_scan_at}`)
    } else {
        console.log(`  Watermark was properly deleted`)
    }

    await new Promise(resolve => setTimeout(resolve, 500))

    // 9. Try to consume again
    let secondConsumeCount = 0
    await client
        .queue(queueName)
        .group(consumerGroup)
        .subscriptionMode('all')
        .concurrency(5)
        .batch(1)
        .wait(false)
        .idleMillis(3000)
        .consume(async msgs => {
            secondConsumeCount += msgs.length
        })

    console.log(`  Second consume (after delete): ${secondConsumeCount} messages`)

    if (secondConsumeCount !== messageCount) {
        return {
            success: false,
            message: `After deleting CG for queue, should consume ${messageCount} messages within the idle window, ` +
                     `but got ${secondConsumeCount}. (legacy watermark after delete: ${watermarkAfterDelete?.last_empty_scan_at})`
        }
    }

    return {
        success: true,
        message: `Delete CG for queue allowed fresh consumption of ${secondConsumeCount} messages`
    }
}
