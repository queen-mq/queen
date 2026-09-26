/**
 * Consumer Group Bootstrap Tests
 * 
 * Tests the partition_consumers bootstrap optimization:
 * When a new consumer group registers (via subscription mode), all existing
 * partition_consumers rows should be pre-seeded in bulk, avoiding slow
 * one-at-a-time discovery.
 * 
 * Test scenarios:
 * 1. testCgBootstrapNew: CG with subscriptionMode('new')
 * 2. testCgBootstrapTimestamp: CG with subscriptionFrom(timestamp)
 * 
 * Both push 5000 partitions, register a CG (triggering bootstrap), then
 * consume new messages and verify all partitions were discovered fast.
 * 
 * Run: node run.js testCgBootstrapNew
 *      node run.js testCgBootstrapTimestamp
 */

const PARTITION_COUNT = 5000
const QUEUE_NAME_NEW = 'test-queue-v2-bootstrap-new'
const QUEUE_NAME_TS = 'test-queue-v2-bootstrap-ts'

async function pushToPartitions(client, queueName, partitionCount, prefix = '') {
    const PARALLEL_BATCH = 50
    let pushed = 0
    for (let i = 0; i < partitionCount; i += PARALLEL_BATCH) {
        const batch = []
        for (let j = i; j < Math.min(i + PARALLEL_BATCH, partitionCount); j++) {
            batch.push(
                client
                    .queue(queueName)
                    .partition(`p-${j}`)
                    .push([{ data: { id: j, prefix } }])
            )
        }
        await Promise.all(batch)
        pushed += batch.length
    }
    return pushed
}

/**
 * Test bootstrap with subscriptionMode('new')
 * - Push historical messages to 5000 partitions
 * - Register CG with mode 'new' (triggers bootstrap)
 * - Push new messages to all partitions
 * - Consume with CG and verify all new messages are received fast
 */
export async function testCgBootstrapNew(client) {
    // Cleanup
    try {
        await client.deleteConsumerGroup('bootstrap-cg-new', true)
    } catch (e) {
        // Ignore
    }

    // 1. Create queue
    const queue = await client.queue(QUEUE_NAME_NEW).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // 2. Push 1 message to each of 5000 partitions (historical messages)
    console.log(`  Pushing to ${PARTITION_COUNT} partitions...`)
    const pushStart = Date.now()
    await pushToPartitions(client, QUEUE_NAME_NEW, PARTITION_COUNT, 'historical')
    const pushTime = Date.now() - pushStart
    console.log(`  Push complete: ${PARTITION_COUNT} partitions in ${pushTime}ms`)

    // 3. Consume all messages in queue mode (no CG) - baseline
    console.log(`  Consuming in queue mode (no CG)...`)
    const queueModeStart = Date.now()
    let queueModeCount = 0

    await client
        .queue(QUEUE_NAME_NEW)
        .concurrency(10)
        .batch(1)
        .wait(false)
        .idleMillis(5000)
        .consume(async msgs => {
            queueModeCount += msgs.length
        })

    const queueModeTime = Date.now() - queueModeStart
    console.log(`  Queue mode: consumed ${queueModeCount} messages in ${queueModeTime}ms`)

    if (queueModeCount !== PARTITION_COUNT) {
        return {
            success: false,
            message: `Queue mode consumed ${queueModeCount}/${PARTITION_COUNT} messages in ${queueModeTime}ms`
        }
    }

    // 4. Register a new CG with 'new' mode
    //    The first pop triggers metadata registration + bootstrap (seeds all partition_consumers)
    console.log(`  Registering CG 'bootstrap-cg-new' with mode 'new'...`)
    const registerStart = Date.now()

    const firstPop = await client
        .queue(QUEUE_NAME_NEW)
        .group('bootstrap-cg-new')
        .subscriptionMode('new')
        .batch(1)
        .wait(false)
        .pop()

    const registerTime = Date.now() - registerStart
    console.log(`  CG registered + bootstrap in ${registerTime}ms (first pop returned ${firstPop.length} msgs)`)

    // First pop should return 0 messages (mode 'new' skips historical)
    if (firstPop.length !== 0) {
        return {
            success: false,
            message: `CG with mode 'new' should skip historical messages, got ${firstPop.length}`
        }
    }

    // 5. Push NEW messages to all 5000 partitions
    console.log(`  Pushing new messages to ${PARTITION_COUNT} partitions...`)
    const pushNewStart = Date.now()
    await pushToPartitions(client, QUEUE_NAME_NEW, PARTITION_COUNT, 'new')
    const pushNewTime = Date.now() - pushNewStart
    console.log(`  Push new complete: ${PARTITION_COUNT} partitions in ${pushNewTime}ms`)

    // 6. Consume all new messages with the CG
    //    If bootstrap worked, all partitions are already known -> fast consumption
    //    If bootstrap didn't work, it would discover one-at-a-time -> very slow
    console.log(`  Consuming with CG 'bootstrap-cg-new'...`)
    const cgConsumeStart = Date.now()
    let cgConsumeCount = 0

    await client
        .queue(QUEUE_NAME_NEW)
        .group('bootstrap-cg-new')
        .subscriptionMode('new')
        .concurrency(10)
        .batch(1)
        .wait(false)
        .idleMillis(5000)
        .consume(async msgs => {
            cgConsumeCount += msgs.length
        })

    const cgConsumeTime = Date.now() - cgConsumeStart
    console.log(`  CG consume: ${cgConsumeCount} messages in ${cgConsumeTime}ms`)

    // 7. Drain new messages in queue mode too (cleanup)
    console.log(`  Draining new messages in queue mode...`)
    let queueDrainCount = 0
    await client
        .queue(QUEUE_NAME_NEW)
        .concurrency(10)
        .batch(1)
        .wait(false)
        .idleMillis(5000)
        .consume(async msgs => {
            queueDrainCount += msgs.length
        })
    console.log(`  Queue mode drain: ${queueDrainCount} messages`)

    // 8. Results
    const allConsumed = cgConsumeCount === PARTITION_COUNT
    const bootstrapFast = registerTime < 10000

    console.log(`  ---- Results (subscriptionMode='new') ----`)
    console.log(`  Partitions:          ${PARTITION_COUNT}`)
    console.log(`  Push (historical):   ${pushTime}ms`)
    console.log(`  Queue mode consume:  ${queueModeTime}ms (${queueModeCount} msgs)`)
    console.log(`  CG registration:     ${registerTime}ms (includes bootstrap)`)
    console.log(`  Push (new):          ${pushNewTime}ms`)
    console.log(`  CG consume:          ${cgConsumeTime}ms (${cgConsumeCount} msgs)`)
    console.log(`  Bootstrap fast:      ${bootstrapFast ? 'YES' : 'NO'} (${registerTime}ms < 10000ms)`)

    if (!allConsumed) {
        return {
            success: false,
            message: `CG consumed ${cgConsumeCount}/${PARTITION_COUNT} in ${cgConsumeTime}ms (bootstrap: ${registerTime}ms)`
        }
    }

    return {
        success: true,
        message: `[new] ${PARTITION_COUNT} partitions: push=${pushTime}ms, queue_mode=${queueModeTime}ms, bootstrap=${registerTime}ms, cg_consume=${cgConsumeTime}ms`
    }
}

/**
 * Test bootstrap with subscriptionFrom(timestamp)
 * - Push historical messages to 5000 partitions
 * - Record a cutoff timestamp
 * - Register CG with subscriptionFrom(cutoff) (triggers bootstrap via 'timestamp' path)
 * - Push new messages to all partitions
 * - Consume with CG and verify all new messages are received fast
 */
export async function testCgBootstrapTimestamp(client) {
    // Cleanup
    try {
        await client.deleteConsumerGroup('bootstrap-cg-ts', true)
    } catch (e) {
        // Ignore
    }

    // 1. Create queue
    const queue = await client.queue(QUEUE_NAME_TS).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // 2. Push 1 message to each of 5000 partitions (historical messages)
    console.log(`  Pushing to ${PARTITION_COUNT} partitions...`)
    const pushStart = Date.now()
    await pushToPartitions(client, QUEUE_NAME_TS, PARTITION_COUNT, 'historical')
    const pushTime = Date.now() - pushStart
    console.log(`  Push complete: ${PARTITION_COUNT} partitions in ${pushTime}ms`)

    // 3. Record cutoff timestamp (after historical, before new messages)
    //    Wait before capturing the cutoff to handle clock skew between the
    //    client (Date.now) and the broker's clock. Without this, the last
    //    push batch may have a broker created_at AFTER the client-side cutoff.
    await new Promise(resolve => setTimeout(resolve, 2000))
    const cutoffTimestamp = new Date().toISOString()
    console.log(`  Cutoff timestamp: ${cutoffTimestamp}`)

    // 4. Register a new CG with subscriptionFrom(timestamp)
    //    The first pop triggers metadata registration + bootstrap (seeds all partition_consumers)
    console.log(`  Registering CG 'bootstrap-cg-ts' with subscriptionFrom('${cutoffTimestamp}')...`)
    const registerStart = Date.now()

    const firstPop = await client
        .queue(QUEUE_NAME_TS)
        .group('bootstrap-cg-ts')
        .subscriptionFrom(cutoffTimestamp)
        .batch(1)
        .wait(false)
        .pop()

    const registerTime = Date.now() - registerStart
    console.log(`  CG registered + bootstrap in ${registerTime}ms (first pop returned ${firstPop.length} msgs)`)

    // First pop should return 0 messages (all messages are before the cutoff)
    if (firstPop.length !== 0) {
        return {
            success: false,
            message: `CG with subscriptionFrom should skip historical messages, got ${firstPop.length}`
        }
    }

    // 5. Push NEW messages to all 5000 partitions (after cutoff)
    console.log(`  Pushing new messages to ${PARTITION_COUNT} partitions...`)
    const pushNewStart = Date.now()
    await pushToPartitions(client, QUEUE_NAME_TS, PARTITION_COUNT, 'new')
    const pushNewTime = Date.now() - pushNewStart
    console.log(`  Push new complete: ${PARTITION_COUNT} partitions in ${pushNewTime}ms`)

    // 6. Consume all new messages with the CG
    //    If bootstrap worked, all partitions are already known -> fast consumption
    //    If bootstrap didn't work, it would discover one-at-a-time -> very slow
    console.log(`  Consuming with CG 'bootstrap-cg-ts'...`)
    const cgConsumeStart = Date.now()
    let cgConsumeCount = 0

    await client
        .queue(QUEUE_NAME_TS)
        .group('bootstrap-cg-ts')
        .subscriptionFrom(cutoffTimestamp)
        .concurrency(10)
        .batch(1)
        .wait(false)
        .idleMillis(5000)
        .consume(async msgs => {
            cgConsumeCount += msgs.length
        })

    const cgConsumeTime = Date.now() - cgConsumeStart
    console.log(`  CG consume: ${cgConsumeCount} messages in ${cgConsumeTime}ms`)

    // 7. Drain new messages in queue mode too (cleanup)
    console.log(`  Draining new messages in queue mode...`)
    let queueDrainCount = 0
    await client
        .queue(QUEUE_NAME_TS)
        .concurrency(10)
        .batch(1)
        .wait(false)
        .idleMillis(5000)
        .consume(async msgs => {
            queueDrainCount += msgs.length
        })
    console.log(`  Queue mode drain: ${queueDrainCount} messages`)

    // 8. Results
    const allConsumed = cgConsumeCount === PARTITION_COUNT
    const bootstrapFast = registerTime < 10000

    console.log(`  ---- Results (subscriptionFrom=timestamp) ----`)
    console.log(`  Partitions:          ${PARTITION_COUNT}`)
    console.log(`  Push (historical):   ${pushTime}ms`)
    console.log(`  CG registration:     ${registerTime}ms (includes bootstrap)`)
    console.log(`  Push (new):          ${pushNewTime}ms`)
    console.log(`  CG consume:          ${cgConsumeTime}ms (${cgConsumeCount} msgs)`)
    console.log(`  Bootstrap fast:      ${bootstrapFast ? 'YES' : 'NO'} (${registerTime}ms < 10000ms)`)

    if (!allConsumed) {
        return {
            success: false,
            message: `CG consumed ${cgConsumeCount}/${PARTITION_COUNT} in ${cgConsumeTime}ms (bootstrap: ${registerTime}ms)`
        }
    }

    return {
        success: true,
        message: `[timestamp] ${PARTITION_COUNT} partitions: push=${pushTime}ms, bootstrap=${registerTime}ms, cg_consume=${cgConsumeTime}ms`
    }
}
