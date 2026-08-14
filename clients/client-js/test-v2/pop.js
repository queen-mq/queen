export async function popEmptyQueue(client) {
    const queue = await client.queue('test-queue-v2-pop-empty').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    const res = await client
    .queue('test-queue-v2-pop-empty')
    .batch(1)      // Fetch 1 message
    .wait(false)
    .pop()
    return { success: res.length === 0 }
}

export async function popNonEmptyQueue(client) {
    const queue = await client.queue('test-queue-v2-pop-non-empty').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    await client
    .queue('test-queue-v2-pop-non-empty')
    .push([{ data: { message: 'Hello, world!' } }])

    // wait(true) rides out the PUSHPOPLOOKUPSOL race: partition_lookup is
    // updated asynchronously after push commits, so an immediate
    // wait(false) wildcard pop can race past the not-yet-committed lookup
    // row. Long-poll re-runs the candidate scan and picks up the row
    // once it commits (typically within ms).
    const res = await client
    .queue('test-queue-v2-pop-non-empty')
    .batch(1)
    .wait(true)
    .pop()
    return { success: res.length === 1 }
}

export async function popWithWait(client) {
    const queue = await client.queue('test-queue-v2-pop-with-wait').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    setTimeout(async () => {
        await client
        .queue('test-queue-v2-pop-with-wait')
        .push([{ data: { message: 'Hello, world!' } }])
    }, 2000);

    const res = await client
    .queue('test-queue-v2-pop-with-wait')
    .batch(1)
    .wait(true)
    .pop()

    return { success: res.length === 1 }
}

export async function popWithAck(client) {
    const queue = await client.queue('test-queue-v2-pop-with-ack').create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    await client
    .queue('test-queue-v2-pop-with-ack')
    .push([{ data: { message: 'Hello, world!' } }]) 


    // wait(true): ride out the PUSHPOPLOOKUPSOL race (see popNonEmptyQueue).
    const res = await client
    .queue('test-queue-v2-pop-with-ack')
    .batch(1)
    .wait(true)
    .pop()

    const resAck = await client.ack(res[0])

    if (!resAck.success) {
        return { success: false, message: `Ack failed: ${resAck.error}` }
    }

    return { success: true, message: 'Message popped and acked successfully' }
}

export async function popWithAckReconsume(client) {
    const queue = await client
    .queue('test-queue-v2-pop-with-ack-reconsume')
    .config({
        leaseTime: 1,  // 1 second lease
    })
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }
    
    await client
    .queue('test-queue-v2-pop-with-ack-reconsume')
    .push([{ data: { message: 'Hello, world!' } }])

    // wait(true): ride out the PUSHPOPLOOKUPSOL race (see popNonEmptyQueue).
    const res = await client
    .queue('test-queue-v2-pop-with-ack-reconsume')
    .batch(1)
    .wait(true)
    .pop()

    if (res.length !== 1) {
        return { success: false, message: `Expected 1 message, got ${res.length}` }
    }

    await new Promise(r => setTimeout(r, 2000))

    const res2 = await client
    .queue('test-queue-v2-pop-with-ack-reconsume')
    .batch(1)
    .wait(true)
    .pop()

    if (res2.length === 1) {
        return { success: true, message: 'Message consumed and reconsumed after lease expiry' }
    }
    return { success: false, message: `Expected 1 message, got ${res2.length}` }
}

// ============================================================================
// v4 Multi-Partition Pop Tests
// ============================================================================
//
// Exercises the .partitions(N) builder method end-to-end against the
// pop_unified_batch_v4 procedure:
//   * Server drains up to N partitions per call
//   * Each message carries its own partitionId / leaseId / partition
//   * batch(B) is a global cap on TOTAL messages across all partitions
//   * Default (no .partitions() call) preserves single-partition behaviour

export async function popV4MultiPartitionBasic(client) {
    const queueName = 'test-queue-v2-v4-pop-basic'
    const queue = await client.queue(queueName).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Seed 3 partitions × 2 messages each = 6 total.
    for (let p = 0; p < 3; p++) {
        await client
        .queue(queueName)
        .partition(`p${p}`)
        .push([
            { data: { p, m: 0 } },
            { data: { p, m: 1 } },
        ])
    }

    // Allow the post-push partition_lookup follow-up (PUSHPOPLOOKUPSOL)
    // to settle before the wildcard scan.
    await new Promise(r => setTimeout(r, 500))

    const res = await client
    .queue(queueName)
    .batch(100)
    .partitions(3)
    .wait(false)
    .pop()

    if (res.length !== 6) {
        return { success: false, message: `Expected 6 messages across 3 partitions, got ${res.length}` }
    }

    // Each message must carry its own partitionId; partitionIds must
    // span all 3 partitions; leaseIds must collapse to a single value.
    const distinctPartitionIds = new Set(res.map(m => m.partitionId))
    const distinctLeaseIds = new Set(res.map(m => m.leaseId).filter(Boolean))

    if (distinctPartitionIds.size !== 3) {
        return { success: false, message: `Expected 3 distinct partitionIds, got ${distinctPartitionIds.size}` }
    }
    if (distinctLeaseIds.size !== 1) {
        return { success: false, message: `All messages must share a single leaseId, got ${distinctLeaseIds.size} distinct` }
    }

    return { success: true, message: 'Multi-partition pop returned messages spanning 3 partitions with shared leaseId' }
}

export async function popV4GlobalCap(client) {
    const queueName = 'test-queue-v2-v4-pop-cap'
    const queue = await client.queue(queueName).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Seed 5 partitions × 100 messages each = 500 total available.
    for (let p = 0; p < 5; p++) {
        const payloads = []
        for (let m = 0; m < 100; m++) payloads.push({ data: { p, m } })
        await client.queue(queueName).partition(`p${p}`).push(payloads)
    }

    await new Promise(r => setTimeout(r, 500))

    // batch=10 is the GLOBAL cap, partitions=5 lets us span partitions.
    // We must receive EXACTLY 10 messages, not 5*10 or 5*100.
    const res = await client
    .queue(queueName)
    .batch(10)
    .partitions(5)
    .wait(false)
    .pop()

    if (res.length !== 10) {
        return { success: false, message: `batch(10) must be a hard global cap, got ${res.length}` }
    }

    return { success: true, message: 'Global batch cap respected across multi-partition pop' }
}

export async function popV4DefaultOne(client) {
    const queueName = 'test-queue-v2-v4-pop-default'
    const queue = await client.queue(queueName).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Seed 4 partitions × 5 messages each.
    for (let p = 0; p < 4; p++) {
        const payloads = []
        for (let m = 0; m < 5; m++) payloads.push({ data: { p, m } })
        await client.queue(queueName).partition(`p${p}`).push(payloads)
    }

    await new Promise(r => setTimeout(r, 500))

    // No .partitions() → max_partitions defaults to 1 → single partition.
    const res = await client
    .queue(queueName)
    .batch(100)
    .wait(false)
    .pop()

    if (res.length !== 5) {
        return { success: false, message: `Default max_partitions=1 should drain only one partition (5 msgs), got ${res.length}` }
    }

    const distinctPartitionIds = new Set(res.map(m => m.partitionId))
    if (distinctPartitionIds.size !== 1) {
        return { success: false, message: `All messages should come from a single partition, got ${distinctPartitionIds.size}` }
    }

    return { success: true, message: 'Default pop preserves legacy single-partition behaviour' }
}

export async function popV4MultiPartitionAck(client) {
    const queueName = 'test-queue-v2-v4-pop-ack'
    const queue = await client.queue(queueName).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Seed 2 partitions × 3 messages each.
    for (let p = 0; p < 2; p++) {
        await client.queue(queueName).partition(`p${p}`).push([
            { data: { p, m: 0 } },
            { data: { p, m: 1 } },
            { data: { p, m: 2 } },
        ])
    }

    await new Promise(r => setTimeout(r, 500))

    const res = await client
    .queue(queueName)
    .batch(100)
    .partitions(2)
    .wait(false)
    .pop()

    if (res.length !== 6) {
        return { success: false, message: `Expected 6 messages across 2 partitions, got ${res.length}` }
    }

    // ACK each message individually using its own per-message partitionId
    // and the shared leaseId. The v4 procedure has set each partition's
    // batch_size to its actual contribution (3), so once 3 ACKs land for
    // a given partition_id, that partition's lease auto-releases.
    const ackResult = await client.ack(res, true)
    if (!ackResult.success) {
        return { success: false, message: `Batch ACK failed: ${ackResult.error || 'unknown'}` }
    }

    return { success: true, message: 'Per-message ACK across multi-partition pop succeeded' }
}

export async function popV4SharedLeaseRenew(client) {
    const queueName = 'test-queue-v2-v4-pop-renew'
    const queue = await client
    .queue(queueName)
    .config({ leaseTime: 60 })
    .create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Seed 3 partitions × 1 message each.
    for (let p = 0; p < 3; p++) {
        await client.queue(queueName).partition(`p${p}`).push([{ data: { p } }])
    }

    await new Promise(r => setTimeout(r, 500))

    const res = await client
    .queue(queueName)
    .batch(100)
    .partitions(3)
    .wait(false)
    .pop()

    if (res.length !== 3) {
        return { success: false, message: `Expected 3 messages from 3 partitions, got ${res.length}` }
    }

    // All 3 messages share one leaseId. Passing res to renew() exercises
    // both the dedup logic AND the server-side multi-row lease extension
    // (renew_lease_v2 filters on worker_id alone, which matches all 3
    // partition_consumers rows).
    const renewResult = await client.renew(res)
    // renew() returns array when input is array. After dedup, it's a
    // single result for the shared leaseId.
    if (!Array.isArray(renewResult) || renewResult.length !== 1) {
        return { success: false, message: `Expected dedup to collapse to 1 renew call, got ${Array.isArray(renewResult) ? renewResult.length : 'non-array'}` }
    }
    if (!renewResult[0].success) {
        return { success: false, message: `Renew failed: ${renewResult[0].error}` }
    }

    // Cleanup: release leases by acking everything.
    await client.ack(res, true)

    return { success: true, message: 'Shared leaseId renewed once and dedup confirmed' }
}

