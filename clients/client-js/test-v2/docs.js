/**
 * The tests behind the published examples.
 *
 * Every marked region in this file is rendered on queenmq.com: the homepage,
 * the quickstart, the concept pages and the HTTP reference pull these exact
 * lines through webdoc/scripts/gen-snippets.mjs. They are tests written to
 * read as documentation: real queue names, a partition key that means
 * something, and no test scaffolding inside a marked region. Assertions stay
 * outside the markers.
 *
 * After editing a marked region, regenerate the partials with
 * `pnpm --dir webdoc gen` or the docs CI check fails on drift. The queues used
 * here (orders, payments, invoices) are wiped by cleanupTestData in run.js
 * before every suite run, exactly like the test-% queues.
 */

export async function docsProduceAndConsume(client) {
    // docs:start(js-push)
    const res = await client
      .queue('orders')
      .partition('customer-42')
      .push([{ data: { orderId: 9137, amount: 99.5 } }])
    // docs:end
    if (res[0].status !== 'queued') {
        return { success: false, message: `Push not queued: ${JSON.stringify(res[0])}` }
    }

    // The consume loop acks each message when the callback resolves, so when
    // the awaited chain returns, the billing group's cursor has moved.
    // docs:start(js-consume)
    await client
      .queue('orders')
      .group('billing')
      .subscriptionMode('all')
      .limit(1)
      .each()
      .consume(async (message) => {
        console.log(message.data)
      })
    // docs:end

    const drained = await client
        .queue('orders')
        .group('billing')
        .batch(1)
        .wait(false)
        .pop()
    if (drained.length !== 0) {
        return { success: false, message: 'Billing group cursor did not advance after consume' }
    }

    // A raw pop on another cursor still sees the message: groups are fan-out.
    // docs:start(js-pop)
    const messages = await client
      .queue('orders')
      .batch(10)
      .wait(true)
      .pop()
    // docs:end
    if (messages.length !== 1 || messages[0].data.orderId !== 9137) {
        return { success: false, message: `Fan-out pop expected the order back, got ${JSON.stringify(messages)}` }
    }

    return { success: true, message: 'Push, consume and fan-out pop all verified' }
}

export async function docsDeduplication(client) {
    // The fixed transactionId below survives reruns because cleanupTestData
    // purges log_txns for these queues before the suite starts.
    // docs:start(js-push-dedup)
    const first = await client
      .queue('payments')
      .partition('customer-42')
      .push([{ transactionId: 'order-9137-paid', data: { orderId: 9137, amount: 99.5 } }])

    const retry = await client
      .queue('payments')
      .partition('customer-42')
      .push([{ transactionId: 'order-9137-paid', data: { orderId: 9137, amount: 99.5 } }])
    // retry[0].status is 'duplicate': the second push wrote nothing
    // and answers with the first message's id.
    // docs:end
    if (first[0].status !== 'queued') {
        return { success: false, message: `First push not queued: ${JSON.stringify(first[0])}` }
    }
    if (retry[0].status !== 'duplicate') {
        return { success: false, message: `Retry not deduplicated: ${JSON.stringify(retry[0])}` }
    }
    if (retry[0].message_id !== first[0].message_id) {
        return { success: false, message: 'Duplicate did not answer with the original message id' }
    }
    return { success: true, message: 'Second push with the same transactionId wrote nothing' }
}

export async function docsReplayAndSeek(client) {
    const seeded = await client.queue('orders').partition('customer-91').push([
        { data: { orderId: 5001, amount: 12 } },
        { data: { orderId: 5002, amount: 24 } },
        { data: { orderId: 5003, amount: 36 } },
    ])
    if (!seeded.every(r => r.status === 'queued')) {
        return { success: false, message: 'Seed pushes failed' }
    }

    // docs:start(js-replay)
    // A fresh group with subscriptionMode('all') reads the whole retained
    // lane from the beginning, without touching any other group's cursor.
    const replayed = await client
      .queue('orders')
      .partition('customer-91')
      .group('audit')
      .subscriptionMode('all')
      .batch(100)
      .wait(true)
      .pop()
    // docs:end
    const ids = replayed.map(m => m.data.orderId)
    if (ids.length !== 3 || ids[0] !== 5001 || ids[1] !== 5002 || ids[2] !== 5003) {
        return { success: false, message: `Replay expected [5001,5002,5003] in order, got ${JSON.stringify(ids)}` }
    }

    // docs:start(js-seek)
    // Move the audit group's cursor back one hour. The seek also releases
    // any live lease, so an in-flight batch is abandoned, not acked.
    await client.admin.seekConsumerGroup('audit', 'orders', {
      timestamp: new Date(Date.now() - 3600 * 1000).toISOString(),
    })
    // docs:end

    const again = await client
        .queue('orders')
        .partition('customer-91')
        .group('audit')
        .batch(100)
        .wait(true)
        .pop()
    const againIds = again.map(m => m.data.orderId)
    if (againIds.length !== 3 || againIds[0] !== 5001) {
        return { success: false, message: `Post-seek pop expected the lane again, got ${JSON.stringify(againIds)}` }
    }
    return { success: true, message: 'Replay from the beginning and seek-back both verified' }
}

export async function docsTransactionalHandoff(client) {
    const seeded = await client
        .queue('orders')
        .partition('customer-77')
        .push([{ data: { orderId: 4102, amount: 18 } }])
    if (seeded[0].status !== 'queued') {
        return { success: false, message: 'Seed push failed' }
    }

    // docs:start(js-transaction)
    await client
      .queue('orders')
      .group('invoicing')
      .subscriptionMode('all')
      .each()
      .autoAck(false) // the acknowledgement belongs to the transaction, not to the loop
      .limit(1)
      .idleMillis(5000)
      .consume(async (message) => {
        // commit() throws when the broker rejects the bundle, so reaching the
        // line after it means the ack and the push are both durable.
        await client
          .transaction()
          .queue('invoices')
          .push([{ data: { orderId: message.data.orderId, invoiced: true } }])
          .ack(message, 'completed', { consumerGroup: 'invoicing' })
          .commit()
      })
    // docs:end

    // The loop takes whichever order is oldest for this group, and other docs
    // tests push to the same queue, so identity is checked against the orders
    // that actually exist rather than against a hardcoded id. A throwaway group
    // reads them without disturbing the invoicing cursor.
    const everyOrder = await client
        .queue('orders')
        .group(`docs-audit-${Date.now()}`)
        .subscriptionMode('all')
        .batch(50)
        .partitions(20)
        .wait(false)
        .pop()
    const orderIds = everyOrder.map(m => m.data.orderId)

    const invoiced = await client
        .queue('invoices')
        .batch(1)
        .partitions(10)
        .wait(true)
        .pop()
    if (invoiced.length !== 1 || !orderIds.includes(invoiced[0].data.orderId)) {
        return {
            success: false,
            message: `Invoice not committed for a real order: ${JSON.stringify(invoiced)} against ${JSON.stringify(orderIds)}`,
        }
    }
    if (invoiced[0].data.invoiced !== true) {
        return { success: false, message: 'Invoice payload corrupted' }
    }
    return { success: true, message: 'Ack and next-stage push committed together' }
}
