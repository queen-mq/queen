/**
 * Shared utilities for streaming tests.
 *
 * Tests in test-v2/stream/* are run live against a Queen instance and a
 * Postgres database. Each test:
 *   - generates a unique queue/query name to avoid collisions across runs
 *   - sets up source + sink queues
 *   - runs a Stream pipeline for a bounded time
 *   - asserts on the resulting sink-queue contents and/or runner metrics
 *   - stops the stream and lets the global cleanup hook drop the rows
 *
 * Test-name uniqueness is achieved with the test function's name + a
 * monotonic counter so the same suite can be re-run without state bleed.
 */

// Avoid importing TEST_CONFIG from ../run.js (circular: run.js imports
// this module via stream/index.js). Read env directly with the same
// fallback the existing harness uses.
let _nameCounter = 0

/**
 * Build a unique queue/query name scoped to a test. The prefix is always
 * "test-stream-" so the global cleanup query in run.js deletes it.
 *
 * @param {string} testName - e.g. fn.name
 * @param {string} suffix   - 'src' | 'sink' | 'query' | etc.
 */
export function mkName(testName, suffix = '') {
  _nameCounter++
  const stamp = Date.now().toString(36)
  const slug = String(testName || 'unknown')
    .replace(/[^a-zA-Z0-9]/g, '-')
    .toLowerCase()
  const tail = suffix ? `-${suffix}` : ''
  return `test-stream-${slug}-${stamp}-${_nameCounter}${tail}`
}

export const STREAMS_URL = process.env.QUEEN_URL || 'http://localhost:6632'

/**
 * Sleep helper.
 */
export function sleep(ms) {
  return new Promise(r => setTimeout(r, ms))
}

/**
 * Poll until predicate returns truthy or timeout elapses. Returns the last
 * value the predicate returned. Useful for "wait until N items have arrived
 * on the sink queue."
 */
export async function waitFor(predicate, { timeoutMs = 10_000, intervalMs = 100 } = {}) {
  const start = Date.now()
  let last = await predicate()
  while (!last && Date.now() - start < timeoutMs) {
    await sleep(intervalMs)
    last = await predicate()
  }
  return last
}

/**
 * Push a flat list of {partition, data} items to a queue. Issues one HTTP
 * call per partition so messages land on the right partition (queen-mq's
 * QueueBuilder.push uses the builder's currently-configured partition for
 * all items in a single call).
 */
export async function pushSpread(client, queueName, items) {
  // Group by partition first.
  const byPart = new Map()
  for (const it of items) {
    const part = it.partition || 'Default'
    if (!byPart.has(part)) byPart.set(part, [])
    byPart.get(part).push({ data: it.data })
  }
  // Issue parallel pushes per partition.
  await Promise.all(Array.from(byPart.entries()).map(([part, batch]) =>
    client.queue(queueName).partition(part).push(batch)
  ))
}

/**
 * Drain a sink queue completely: pop one message at a time and ack
 * immediately, return the concatenated list of messages. Stops when a
 * pop returns 0 messages OR cumulative timeout is hit.
 *
 * NOTE on batch size: we use batch=1 so that each ack triggers
 * `acked_count >= batch_size` in queen.partition_consumers and the lease
 * is released between pops. With a larger batch the lease would stay
 * held until acked_count reached the full batch_size (often never on
 * intermittent emits), and subsequent pops would skip the partition as
 * "leased by another worker." Worth the round-trip overhead in tests.
 */
export async function drainSink(client, queueName, { timeoutMs = 5000, group } = {}) {
  const cg = group || `drain-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
  const out = []
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const popped = await client
      .queue(queueName)
      .group(cg)
      .batch(1)
      .wait(false)
      .pop()
    if (!popped || popped.length === 0) break
    for (const m of popped) out.push(m)
    // Pass the consumer group so the SP validates the lease against OUR
    // group, not the default __QUEUE_MODE__. queen-mq uses `context.group`
    // (NOT `consumerGroup`) for the ack-context shape.
    await client.ack(popped, true, { group: cg })
  }
  return out
}

/**
 * Cumulative drain: keep popping (with short long-poll) until either the
 * predicate returns true or the timeout elapses. Useful for "wait until at
 * least N closed-window emits land on the sink." Uses the same batch=1
 * pattern as drainSink so the lease is released between pops.
 */
export async function drainUntil(client, queueName, { until, timeoutMs = 15_000, group } = {}) {
  const cg = group || `drain-until-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
  const out = []
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (typeof until === 'function' && until(out)) break
    const popped = await client
      .queue(queueName)
      .group(cg)
      .batch(1)
      .wait(true)
      .timeoutMillis(500)
      .pop()
    if (popped && popped.length > 0) {
      for (const m of popped) out.push(m)
      await client.ack(popped, true, { group: cg })
    }
  }
  return out
}

/**
 * Tiny assertion helper that returns the test-result shape used by run.js
 * instead of throwing. Lets tests aggregate multiple checks before reporting.
 */
export function expect(actual, op, expected, what) {
  let ok = false
  let detail = ''
  switch (op) {
    case '===': ok = actual === expected; detail = `${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`; break
    case '>=':  ok = actual >= expected;  detail = `${what}: expected >= ${expected}, got ${actual}`; break
    case '<=':  ok = actual <= expected;  detail = `${what}: expected <= ${expected}, got ${actual}`; break
    case '>':   ok = actual >  expected;  detail = `${what}: expected >  ${expected}, got ${actual}`; break
    case '<':   ok = actual <  expected;  detail = `${what}: expected <  ${expected}, got ${actual}`; break
    case 'in':  ok = expected.includes(actual); detail = `${what}: expected one of ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`; break
    default: throw new Error('unknown op: ' + op)
  }
  return { ok, detail }
}

/**
 * Walk through a list of expectation tuples and produce a final test result.
 * Returns { success, message } as expected by run.js's harness.
 */
export function summarise(testName, checks) {
  const failed = checks.filter(c => !c.ok)
  if (failed.length === 0) {
    return { success: true, message: `${checks.length} checks passed` }
  }
  return { success: false, message: failed.map(f => f.detail).join('; ') }
}

/**
 * Run a Stream for `runMs`, then stop and return the metrics + handle.
 * Lets every test follow the same `setup → run → stop → assert` shape.
 */
export async function runStreamFor(streamFactory, runMs) {
  const handle = await streamFactory()
  await sleep(runMs)
  await handle.stop()
  return handle
}

/**
 * Helpful guard for tests that need a clean queen_streams.queries row for
 * a given query name. Drops it (state cascades) before the test runs.
 */
export async function dropStreamQuery(dbPool, queryName) {
  await dbPool.query(`DELETE FROM queen_streams.queries WHERE name = $1`, [queryName])
}
