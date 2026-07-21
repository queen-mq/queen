// Ordering assertion that tolerates REDELIVERY (at-least-once is the broker's
// contract: a lease expiry / ack timeout on a slow machine legally replays a
// batch). What it still catches is the real bug class — SKIPPED messages: the
// stream must never jump PAST an id it hasn't delivered yet. Throwing on a
// redelivered (already-seen) id instead would nack the batch, and four nacks
// dead-letter the head (v0.16.0 retry semantics) — turning one benign
// redelivery into message loss and a failed test.
function makeOrderingTracker() {
    let maxId = -1
    return {
        observe(id) {
            if (id > maxId + 1) {
                throw new Error(`Message ordering violation: jumped from ${maxId} to ${id} (skipped ${id - maxId - 1})`)
            }
            if (id > maxId) maxId = id
        }
    }
}

export async function testLoad(client) { 
    const queue = await client
    .queue('test-queue-v2-load')
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    const messagesToPush = 100000;

    let messages = [];
    for (let i = 0; i < messagesToPush; i++) {
        messages.push({ data: { id: i } })
        if (messages.length >= 100) {
            await client.queue('test-queue-v2-load').push(messages)
            messages = [];
        }
    }
    await client.queue('test-queue-v2-load').push(messages)    

    let uniqueIds = new Set();
    const order = makeOrderingTracker();

    // idleMillis instead of limit(10000): a hard per-worker delivery budget
    // counts redeliveries too, so one replayed batch makes workers return
    // "complete" with messages still queued (or spin forever waiting to hit an
    // exact share). Draining until idle is redelivery-proof — same pattern as
    // the bootstrap tests.
    await client
    .queue('test-queue-v2-load')
    .concurrency(10)
    .batch(100)
    .wait(false)
    .idleMillis(5000)
    .consume(async msgs => {
        for (const msg of msgs) {
            order.observe(msg.data.id)
            uniqueIds.add(msg.data.id)
        }
    })

    const uniqueIdsCount = uniqueIds.size;
    console.log(`Unique IDs count: ${uniqueIdsCount}`)

    return { success: uniqueIdsCount === messagesToPush, message: 'Load test completed successfully' }
}

export async function testLoadPartition(client) { 
    const queue = await client
    .queue('test-queue-v2-load-partition')
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    const messagesToPush = 100000;

    let messages = [];
    let k = 0
    for (let k = 0; k < 10; k++) {
    for (let i = 0; i < messagesToPush / 10; i++) {
        messages.push({ data: { id: i } })
        if (messages.length >= 100) {
            await client
            .queue('test-queue-v2-load-partition')
            .partition(k.toString())
            .push(messages)
            messages = [];
        }
    }
    }  

    let uniqueIds = new Set();
    
    
    for (let i = 0; i < 10; i++) {
        const order = makeOrderingTracker();
    await client
    .queue('test-queue-v2-load-partition')
    .partition(i.toString())
    .batch(100)
    .wait(false)
    .idleMillis(5000)
    .consume(async msgs => {
        for (const msg of msgs) {
            order.observe(msg.data.id)
            uniqueIds.add(msg.data.id)
        }
    })
    }

    console.log(uniqueIds.size, messagesToPush)
    const uniqueIdsCount = uniqueIds.size;

    return { success: uniqueIdsCount === messagesToPush / 10, message: 'Load test completed successfully' }
}

export async function testLoadConsumerGroup(client) { 
    const queue = await client
    .queue('')
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    const messagesToPush = 100000;

    let messages = [];
    for (let i = 0; i < messagesToPush; i++) {
        messages.push({ data: { id: i } })
        if (messages.length >= 100) {
            await client.queue('test-queue-v2-load-consumer-group').push(messages)
            messages = [];
        }
    }
    await client.queue('test-queue-v2-load-consumer-group').push(messages)    

    let uniqueIdsA = new Set();
    const orderA = makeOrderingTracker();

    await client
    .queue('test-queue-v2-load-consumer-group')
    .group('test-consumer-group-a')
    .subscriptionMode('all')
    .concurrency(10)
    .batch(100)
    .wait(false)
    .idleMillis(5000)
    .consume(async msgs => {
        for (const msg of msgs) {
            orderA.observe(msg.data.id)
            uniqueIdsA.add(msg.data.id)
        }
    })
    
    let uniqueIdsB = new Set();
    const orderB = makeOrderingTracker();

    await client
    .queue('test-queue-v2-load-consumer-group')
    .group('test-consumer-group-b')
    .subscriptionMode('all')
    .concurrency(10)
    .batch(100)
    .wait(false)
    .idleMillis(5000)
    .consume(async msgs => {
        for (const msg of msgs) {
            orderB.observe(msg.data.id)
            uniqueIdsB.add(msg.data.id)
        }
    })    

    const uniqueIdsCountA = uniqueIdsA.size;
    const uniqueIdsCountB = uniqueIdsB.size;
    console.log(`Unique IDs: groupA=${uniqueIdsCountA} groupB=${uniqueIdsCountB}`)
    return { success: uniqueIdsCountA === messagesToPush && uniqueIdsCountB === messagesToPush, message: 'Load test completed successfully' }
}
