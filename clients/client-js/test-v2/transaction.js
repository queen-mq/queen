export async function transactionBasicPushAck(client) {
    const queueA = await client.queue('test-queue-v2-txn-basic-a').create()
    const queueB = await client.queue('test-queue-v2-txn-basic-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push a message to queue A
    await client.queue('test-queue-v2-txn-basic-a').push([{ data: { value: 1 } }])

    // Pop from A and push to B in a transaction
    const messages = await client.queue('test-queue-v2-txn-basic-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    // docs:start(js-transaction)
    await client
        .transaction()
        .queue('test-queue-v2-txn-basic-b')
        .push([{ data: { value: messages[0].data.value + 1 } }])
        .ack(messages[0])
        .commit()
    // docs:end

    // Verify message moved to B
    const resultB = await client.queue('test-queue-v2-txn-basic-b').batch(1).wait(false).pop()
    const resultA = await client.queue('test-queue-v2-txn-basic-a').batch(1).wait(false).pop()

    return { 
        success: resultB.length === 1 && resultA.length === 0 && resultB[0].data.value === 2,
        message: 'Basic transaction push+ack completed'
    }
}

export async function transactionMultiplePushes(client) {
    const queueA = await client.queue('test-queue-v2-txn-multi-a').create()
    const queueB = await client.queue('test-queue-v2-txn-multi-b').create()
    const queueC = await client.queue('test-queue-v2-txn-multi-c').create()
    if (!queueA.configured || !queueB.configured || !queueC.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push to queue A
    await client.queue('test-queue-v2-txn-multi-a').push([{ data: { id: 'source' } }])

    // Pop from A
    const messages = await client.queue('test-queue-v2-txn-multi-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    // Transaction: Push to B and C, ack from A
    await client
        .transaction()
        .queue('test-queue-v2-txn-multi-b')
        .push([{ data: { id: 'b', source: messages[0].data.id } }])
        .queue('test-queue-v2-txn-multi-c')
        .push([{ data: { id: 'c', source: messages[0].data.id } }])
        .ack(messages[0])
        .commit()

    // Verify messages in B and C
    const resultB = await client.queue('test-queue-v2-txn-multi-b').batch(1).wait(false).pop()
    const resultC = await client.queue('test-queue-v2-txn-multi-c').batch(1).wait(false).pop()
    const resultA = await client.queue('test-queue-v2-txn-multi-a').batch(1).wait(false).pop()

    return { 
        success: resultB.length === 1 && resultC.length === 1 && resultA.length === 0,
        message: 'Multiple pushes in transaction completed'
    }
}

export async function transactionMultipleAcks(client) {
    const queueA = await client.queue('test-queue-v2-txn-multi-ack-a').config().create()
    const queueB = await client.queue('test-queue-v2-txn-multi-ack-b').config().create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push 3 messages to queue A
    await client.queue('test-queue-v2-txn-multi-ack-a').push([
        { data: { value: 1 } },
        { data: { value: 2 } },
        { data: { value: 3 } }
    ])

    // Pop 3 messages from A
    const messages = await client.queue('test-queue-v2-txn-multi-ack-a').batch(3).wait(false).pop()
    if (messages.length !== 3) {
        return { success: false, message: `Expected 3 messages, got ${messages.length}` }
    }

    // Transaction: Ack all 3, push summary to B
    const sum = messages.reduce((acc, msg) => acc + msg.data.value, 0)
    await client
        .transaction()
        .ack(messages[0])
        .ack(messages[1])
        .ack(messages[2])
        .queue('test-queue-v2-txn-multi-ack-b')
        .push([{ data: { sum } }])
        .commit()

    // Verify
    const resultA = await client.queue('test-queue-v2-txn-multi-ack-a').batch(3).wait(false).pop()
    const resultB = await client.queue('test-queue-v2-txn-multi-ack-b').batch(1).wait(false).pop()

    return { 
        success: resultA.length === 0 && resultB.length === 1 && resultB[0].data.sum === 6,
        message: 'Multiple acks in transaction completed'
    }
}

export async function transactionAckWithStatus(client) {
    const queueA = await client.queue('test-queue-v2-txn-ack-status-a').config({
        retryLimit: 0
    }).create()
    const queueB = await client.queue('test-queue-v2-txn-ack-status-b').config({
        retryLimit: 0
    }).create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push a message
    await client.queue('test-queue-v2-txn-ack-status-a').push([{ data: { value: 1 } }])

    // Pop the message
    const messages = await client.queue('test-queue-v2-txn-ack-status-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    // ACK with custom status in a transaction
    await client
        .transaction()
        .ack(messages[0], 'failed')  // Ack with 'failed' status
        .queue('test-queue-v2-txn-ack-status-b')
        .push([{ data: { processed: true } }])
        .commit()

    // Verify message was acked and new message was pushed
    const resultA = await client.queue('test-queue-v2-txn-ack-status-a').batch(1).wait(false).pop()
    const resultB = await client.queue('test-queue-v2-txn-ack-status-b').batch(1).wait(false).pop()

    return { 
        success: resultA.length === 0 && resultB.length === 1,
        message: 'Transaction with custom ack status completed'
    }
}

export async function transactionAtomicity(client) {
    const queueA = await client.queue('test-queue-v2-txn-atomic-a').create()
    const queueB = await client.queue('test-queue-v2-txn-atomic-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push a message to A
    await client.queue('test-queue-v2-txn-atomic-a').push([{ data: { value: 100 } }])

    // Pop from A
    const messages = await client.queue('test-queue-v2-txn-atomic-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    // Successful transaction
    await client
        .transaction()
        .ack(messages[0])
        .queue('test-queue-v2-txn-atomic-b')
        .push([{ data: { value: 200 } }])
        .commit()

    // Both operations should have succeeded atomically
    const resultA = await client.queue('test-queue-v2-txn-atomic-a').batch(1).wait(false).pop()
    const resultB = await client.queue('test-queue-v2-txn-atomic-b').batch(1).wait(false).pop()

    return { 
        success: resultA.length === 0 && resultB.length === 1 && resultB[0].data.value === 200,
        message: 'Transaction atomicity verified'
    }
}

export async function transactionChainedProcessing(client) {
    const queue1 = await client.queue('test-queue-v2-txn-chain-1').create()
    const queue2 = await client.queue('test-queue-v2-txn-chain-2').create()
    const queue3 = await client.queue('test-queue-v2-txn-chain-3').create()
    if (!queue1.configured || !queue2.configured || !queue3.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Initial message
    await client.queue('test-queue-v2-txn-chain-1').push([{ data: { step: 1, value: 10 } }])

    // Transaction 1: Process from queue1 to queue2
    const msg1 = await client.queue('test-queue-v2-txn-chain-1').batch(1).wait(false).pop()
    if (msg1.length === 0) {
        return { success: false, message: 'No message in queue 1' }
    }

    await client
        .transaction()
        .queue('test-queue-v2-txn-chain-2')
        .push([{ data: { step: 2, value: msg1[0].data.value * 2 } }])
        .ack(msg1[0])
        .commit()

    // Transaction 2: Process from queue2 to queue3
    const msg2 = await client.queue('test-queue-v2-txn-chain-2').batch(1).wait(false).pop()
    if (msg2.length === 0) {
        return { success: false, message: 'No message in queue 2' }
    }

    await client
        .transaction()
        .queue('test-queue-v2-txn-chain-3')
        .push([{ data: { step: 3, value: msg2[0].data.value + 5 } }])
        .ack(msg2[0])
        .commit()

    // Verify final result
    const result = await client.queue('test-queue-v2-txn-chain-3').batch(1).wait(false).pop()
    const empty1 = await client.queue('test-queue-v2-txn-chain-1').batch(1).wait(false).pop()
    const empty2 = await client.queue('test-queue-v2-txn-chain-2').batch(1).wait(false).pop()

    return { 
        success: result.length === 1 && result[0].data.value === 25 && empty1.length === 0 && empty2.length === 0,
        message: 'Chained transaction processing completed'
    }
}

export async function transactionBatchPush(client) {
    const queueA = await client.queue('test-queue-v2-txn-batch-a').create()
    const queueB = await client.queue('test-queue-v2-txn-batch-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push source message
    await client.queue('test-queue-v2-txn-batch-a').push([{ data: { id: 'batch-source' } }])

    // Pop from A
    const messages = await client.queue('test-queue-v2-txn-batch-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    // Transaction: Push multiple messages to B in a single batch, ack from A
    await client
        .transaction()
        .queue('test-queue-v2-txn-batch-b')
        .push([
            { data: { index: 1 } },
            { data: { index: 2 } },
            { data: { index: 3 } },
            { data: { index: 4 } },
            { data: { index: 5 } }
        ])
        .ack(messages[0])
        .commit()

    // Verify all 5 messages in B
    const resultB = await client.queue('test-queue-v2-txn-batch-b').batch(10).wait(false).pop()
    const resultA = await client.queue('test-queue-v2-txn-batch-a').batch(1).wait(false).pop()

    return { 
        success: resultB.length === 5 && resultA.length === 0,
        message: 'Batch push in transaction completed'
    }
}

export async function transactionWithPartitions(client) {
    const queue = await client.queue('test-queue-v2-txn-partitions').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Push messages to different partitions
    await client.queue('test-queue-v2-txn-partitions').partition('p1').push([{ data: { partition: 'p1', value: 1 } }])
    await client.queue('test-queue-v2-txn-partitions').partition('p2').push([{ data: { partition: 'p2', value: 2 } }])

    // Pop from both partitions
    const msg1 = await client.queue('test-queue-v2-txn-partitions').partition('p1').batch(1).wait(false).pop()
    const msg2 = await client.queue('test-queue-v2-txn-partitions').partition('p2').batch(1).wait(false).pop()

    if (msg1.length === 0 || msg2.length === 0) {
        return { success: false, message: 'Messages not found in partitions' }
    }

    // Transaction: Ack both messages from different partitions
    await client
        .transaction()
        .ack(msg1[0])
        .ack(msg2[0])
        .commit()

    // Verify both partitions are empty
    const result1 = await client.queue('test-queue-v2-txn-partitions').partition('p1').batch(1).wait(false).pop()
    const result2 = await client.queue('test-queue-v2-txn-partitions').partition('p2').batch(1).wait(false).pop()

    return { 
        success: result1.length === 0 && result2.length === 0,
        message: 'Transaction with multiple partitions completed'
    }
}

export async function transactionWithConsumer(client) {
    const queueA = await client.queue('test-queue-v2-txn-consumer-a').create()
    const queueB = await client.queue('test-queue-v2-txn-consumer-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push messages
    await client.queue('test-queue-v2-txn-consumer-a').push([
        { data: { value: 1 } },
        { data: { value: 2 } },
        { data: { value: 3 } }
    ])

    let processedCount = 0
    
    // Consume with transaction
    await client
        .queue('test-queue-v2-txn-consumer-a')
        .batch(1)
        .wait(false)
        .limit(3)
        .each()
        .autoAck(false)  // Manual ack in transaction
        .consume(async (msg) => {
            await client
                .transaction()
                .queue('test-queue-v2-txn-consumer-b')
                .push([{ data: { value: msg.data.value * 10 } }])
                .ack(msg)
                .commit()
            
            processedCount++
        })

    // Verify results
    const resultA = await client.queue('test-queue-v2-txn-consumer-a').batch(10).wait(false).pop()
    const resultB = await client.queue('test-queue-v2-txn-consumer-b').batch(10).wait(false).pop()

    return { 
        success: processedCount === 3 && resultA.length === 0 && resultB.length === 3,
        message: 'Transaction with consumer completed'
    }
}

export async function transactionEmptyCommit(client) {
    const queue = await client.queue('test-queue-v2-txn-empty').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Empty transaction - should throw error
    let errorThrown = false
    try {
        await client.transaction().commit()
    } catch (error) {
        errorThrown = error.message === 'Transaction has no operations to commit'
    }

    return { 
        success: errorThrown,
        message: 'Empty transaction correctly throws error'
    }
}

export async function transactionLargePayload(client) {
    const queueA = await client.queue('test-queue-v2-txn-large-a').create()
    const queueB = await client.queue('test-queue-v2-txn-large-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push a large message
    const largeData = { 
        items: Array.from({ length: 1000 }, (_, i) => ({ id: i, data: `item-${i}` }))
    }
    await client.queue('test-queue-v2-txn-large-a').push([{ data: largeData }])

    // Pop and process in transaction
    const messages = await client.queue('test-queue-v2-txn-large-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to consume' }
    }

    await client
        .transaction()
        .queue('test-queue-v2-txn-large-b')
        .push([{ data: { count: messages[0].data.items.length } }])
        .ack(messages[0])
        .commit()

    // Verify
    const resultB = await client.queue('test-queue-v2-txn-large-b').batch(1).wait(false).pop()

    return { 
        success: resultB.length === 1 && resultB[0].data.count === 1000,
        message: 'Large payload transaction completed'
    }
}

export async function transactionMultipleQueues(client) {
    const queueA = await client.queue('test-queue-v2-txn-multi-q-a').create()
    const queueB = await client.queue('test-queue-v2-txn-multi-q-b').create()
    const queueC = await client.queue('test-queue-v2-txn-multi-q-c').create()
    if (!queueA.configured || !queueB.configured || !queueC.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push messages to A and B
    await client.queue('test-queue-v2-txn-multi-q-a').push([{ data: { source: 'A' } }])
    await client.queue('test-queue-v2-txn-multi-q-b').push([{ data: { source: 'B' } }])

    // Pop from both
    const msgA = await client.queue('test-queue-v2-txn-multi-q-a').batch(1).wait(false).pop()
    const msgB = await client.queue('test-queue-v2-txn-multi-q-b').batch(1).wait(false).pop()
    
    if (msgA.length === 0 || msgB.length === 0) {
        return { success: false, message: 'Messages not found' }
    }

    // Transaction: Ack from A and B, push to C
    await client
        .transaction()
        .ack(msgA[0])
        .ack(msgB[0])
        .queue('test-queue-v2-txn-multi-q-c')
        .push([{ data: { merged: true } }])
        .commit()

    // Verify all operations succeeded
    const resultA = await client.queue('test-queue-v2-txn-multi-q-a').batch(1).wait(false).pop()
    const resultB = await client.queue('test-queue-v2-txn-multi-q-b').batch(1).wait(false).pop()
    const resultC = await client.queue('test-queue-v2-txn-multi-q-c').batch(1).wait(false).pop()

    return { 
        success: resultA.length === 0 && resultB.length === 0 && resultC.length === 1,
        message: 'Transaction across multiple queues completed'
    }
}

export async function transactionRollback(client) {
    const queueA = await client.queue('test-queue-v2-txn-rollback-a')
    .config({
        leaseTime: 1
    }).create()
    const queueB = await client.queue('test-queue-v2-txn-rollback-b').config({
        leaseTime: 1
    }).create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    // Push a test message to queue A
    await client.queue('test-queue-v2-txn-rollback-a').push([{ data: { test: 'rollback' } }])

    // Pop the message
    const messages = await client.queue('test-queue-v2-txn-rollback-a').batch(1).wait(false).pop()
    if (messages.length === 0) {
        return { success: false, message: 'No message to pop' }
    }

    // Attempt a transaction that should fail and rollback
    let transactionFailed = false
    try {
        await client
            .transaction()
            .queue('test-queue-v2-txn-rollback-b')
            .push([{ data: { value: 1 } }])  // This PUSH should succeed...
            .ack(messages[0])  // This ACK should succeed...
            .ack({ transactionId: 'non-existent-id', partitionId: messages[0].partitionId })  // But this should FAIL
            .commit()
    } catch (error) {
        transactionFailed = true
    }

    // Verify transaction failed
    if (!transactionFailed) {
        return { success: false, message: 'Transaction should have failed but did not' }
    }

    await new Promise(resolve => setTimeout(resolve, 2000))

    // Verify rollback: queue B should be EMPTY (PUSH was rolled back)
    const resultB = await client.queue('test-queue-v2-txn-rollback-b').batch(10).wait(false).pop()
    
    // Verify rollback: queue A should still have the message (ACK was rolled back)
    const resultA = await client.queue('test-queue-v2-txn-rollback-a').batch(10).wait(false).pop()

    const rollbackWorked = resultB.length === 0 && resultA.length === 1

    return { 
        success: rollbackWorked,
        message: rollbackWorked 
            ? 'Transaction rollback verified: PUSH and ACK were both rolled back' 
            : `Rollback failed: Queue A has ${resultA.length} messages (expected 1), Queue B has ${resultB.length} messages (expected 0)`
    }
}

export async function transactionAckWithConsumerGroup(client) {
    const queueA = await client.queue('test-queue-v2-txn-cg-a').create()
    const queueB = await client.queue('test-queue-v2-txn-cg-b').create()
    if (!queueA.configured || !queueB.configured) {
        return { success: false, message: 'Queues not created' }
    }

    const consumerGroup = 'test-consumer-group-txn'

    // Push messages to queue A
    await client.queue('test-queue-v2-txn-cg-a').push([
        { data: { value: 1 } },
        { data: { value: 2 } }
    ])

    // Pop with consumer group. Both groups here are created after the push,
    // so they must ask for 'all' to see the backlog under test.
    const messages = await client.queue('test-queue-v2-txn-cg-a')
        .group(consumerGroup)
        .subscriptionMode('all')
        .batch(2)
        .wait(false)
        .pop()

    if (messages.length !== 2) {
        return { success: false, message: `Expected 2 messages, got ${messages.length}` }
    }

    // Transaction: ACK with consumer group context, push to B
    await client
        .transaction()
        .ack(messages[0], 'completed', { consumerGroup })
        .ack(messages[1], 'completed', { consumerGroup })
        .queue('test-queue-v2-txn-cg-b')
        .push([{ data: { sum: messages[0].data.value + messages[1].data.value } }])
        .commit()

    // Verify: Pop again with same consumer group should get no messages
    // (consumer group cursor should have advanced)
    const messagesAfterAck = await client.queue('test-queue-v2-txn-cg-a')
        .group(consumerGroup)
        .batch(2)
        .wait(false)
        .pop()

    // Verify: Queue B has the result
    const resultB = await client.queue('test-queue-v2-txn-cg-b').batch(1).wait(false).pop()

    // Verify: Using a different consumer group should still see the messages
    const messagesOtherGroup = await client.queue('test-queue-v2-txn-cg-a')
        .group('other-consumer-group')
        .subscriptionMode('all')
        .batch(2)
        .wait(false)
        .pop()

    const success = messagesAfterAck.length === 0 && 
                    resultB.length === 1 && 
                    resultB[0].data.sum === 3 &&
                    messagesOtherGroup.length === 2

    return { 
        success,
        message: success 
            ? 'Transaction ack with consumer group completed - cursor advanced correctly'
            : `Failed: afterAck=${messagesAfterAck.length} (expected 0), resultB=${resultB.length} (expected 1), otherGroup=${messagesOtherGroup.length} (expected 2)`
    }
}

