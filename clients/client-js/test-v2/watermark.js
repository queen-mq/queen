/**
 * Seek / delete re-consumption tests.
 *
 * These pin the BEHAVIOUR a user cares about, and nothing else:
 *   1. Seeking a consumer group backwards re-exposes the old messages, and the
 *      group re-consumes them from the start within a short bound.
 *   2. Deleting a consumer group (all queues, or for one queue) lets a group of
 *      the same name consume every message again from the start.
 *
 * Each test first drains the queue and polls it empty, so the group has seen
 * "nothing to deliver" before the seek or the delete. "Within a short bound"
 * is the idle-bounded consume (idleMillis 3000) that follows: a seek or a
 * delete that left stale per-group state behind makes that consume go idle
 * before every message came back, and the count comes up short.
 *
 * Run: node run.js seekBackwardsAllowsReconsume
 *      node run.js deleteConsumerGroupAllowsReconsume
 *      node run.js deleteConsumerGroupForQueueAllowsReconsume
 */

const QUEUE_NAME_SEEK = 'test-queue-v2-watermark-seek'
const QUEUE_NAME_DELETE = 'test-queue-v2-watermark-delete'

/**
 * Test: Seeking backwards should allow re-consumption of messages
 *
 * Steps:
 * 1. Push messages to queue
 * 2. Consume all messages with a consumer group, then poll it empty
 * 3. Seek the consumer group backwards to before the messages
 * 4. Try to consume again - should get all messages
 *
 * Expected: All messages are re-consumed after seek, within the idle window
 */
export async function seekBackwardsAllowsReconsume(client) {
    const consumerGroup = 'watermark-seek-test-cg'

    // Cleanup
    try {
        await client.deleteConsumerGroup(consumerGroup, true)
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

    // 5. Verify queue is empty for this consumer (the group has now polled empty)
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

    console.log(`  Verified queue is empty`)

    // 6. Seek backwards to BEFORE the messages were pushed
    console.log(`  Seeking to timestamp: ${timestampBeforeMessages}`)

    const seekResult = await client.admin.seekConsumerGroup(
        consumerGroup,
        QUEUE_NAME_SEEK,
        { timestamp: timestampBeforeMessages }
    )

    console.log(`  Seek result: ${JSON.stringify(seekResult)}`)

    // Small delay after seek
    await new Promise(resolve => setTimeout(resolve, 500))

    // 7. Try to consume again - should get all messages back
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

    // 8. Verify we got all messages back within the idle-bounded consume above.
    //    A shortfall means the backward seek did not re-expose the messages
    //    promptly.
    if (secondConsumeCount !== messageCount) {
        return {
            success: false,
            message: `After seek backwards, should re-consume ${messageCount} messages within the idle window, but got ${secondConsumeCount}`
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
 * 2. Consume all messages with a consumer group, then poll it empty
 * 3. Delete the consumer group
 * 4. Create a new consumer group with the SAME name
 * 5. Try to consume - should get all messages
 *
 * Expected: All messages are consumed by the "new" consumer group
 */
export async function deleteConsumerGroupAllowsReconsume(client) {
    const consumerGroup = 'watermark-delete-test-cg'

    // Cleanup
    try {
        await client.deleteConsumerGroup(consumerGroup, true)
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

    // 4. Verify queue is empty for this consumer (the group has now polled empty)
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

    console.log(`  Verified queue is empty`)

    // 5. Delete the consumer group
    console.log(`  Deleting consumer group: ${consumerGroup}`)

    const deleteResult = await client.deleteConsumerGroup(consumerGroup, true)
    console.log(`  Delete result: ${JSON.stringify(deleteResult)}`)

    // Small delay after delete
    await new Promise(resolve => setTimeout(resolve, 500))

    // 6. Create a "new" consumer group with the same name and try to consume.
    //    The delete removed the group's cursors and metadata, so it must start
    //    fresh; any per-group state that survived would skip the messages.
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

    // 7. Verify we got all messages within the idle-bounded consume above. A
    //    shortfall means the delete left stale per-group state behind that
    //    suppressed the from-the-start reconsume.
    if (secondConsumeCount !== messageCount) {
        return {
            success: false,
            message: `After deleting CG, new CG with same name should consume ${messageCount} messages within the idle window, ` +
                     `but got ${secondConsumeCount}`
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

    // 4. Verify queue is empty (the group has now polled empty)
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

    console.log(`  Verified queue is empty`)

    // 5. Delete consumer group for this specific queue
    console.log(`  Deleting consumer group for queue: ${consumerGroup} / ${queueName}`)

    const deleteResult = await client.admin.deleteConsumerGroupForQueue(consumerGroup, queueName, true)
    console.log(`  Delete result: ${JSON.stringify(deleteResult)}`)

    await new Promise(resolve => setTimeout(resolve, 500))

    // 6. Try to consume again
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
                     `but got ${secondConsumeCount}`
        }
    }

    return {
        success: true,
        message: `Delete CG for queue allowed fresh consumption of ${secondConsumeCount} messages`
    }
}
