/**
 * Drain sinks: WHERE a buffered batch goes, and in WHAT shape.
 *
 * The buffer machinery -- blocking backpressure at `maxSize`, one drain loop
 * per address, a failed batch put back at the FRONT and retried until it lands
 * or a flush deadline expires -- is about ordering, occupancy and loss. None of
 * that is durable-specific, and none of it is worth writing twice. So the drain
 * takes a SINK instead of a hardcoded POST:
 *
 *     { path, format(queue, partition, batch) -> body }
 *
 * `format` receives the queue and partition because the two storage classes
 * disagree about where that identity lives on the wire, and that disagreement
 * is the entire reason this parameter exists:
 *
 *   * the DURABLE push wire repeats `{queue, partition}` on EVERY item, so the
 *     envelope is just `{items}` and the sink ignores both arguments;
 *   * the EPHEMERAL push wire hoists them to the envelope --
 *     `{queue, partition?, messages:[{payload}...]}` -- so the batch elements
 *     carry nothing but their payload.
 *
 * DURABLE_SINK IS TODAY'S REQUEST, BYTE FOR BYTE. It is the default for a
 * buffer created without a destination, which is every caller that existed
 * before ephemeral queues did, and test-v2/ephemeral-unit/durableSinkPin.test.js
 * exists for no other reason than to fail if that ever stops being true.
 *
 * ADDRESSES ARE NAMESPACED. A buffer address is the key of the one-buffer-one-
 * drain map, so an ephemeral `orders` and a durable `orders` must not hash to
 * the same entry -- they are unrelated objects (EPHEMERAL_QUEUES.md §10 Q8) and
 * a shared buffer would post one family's messages to the other family's route.
 * The `eph:` prefix is the same namespacing the broker applies to its own queue
 * keys (§3.2), for the same reason.
 */

/** The durable push wire: identity per item, envelope carries only the batch. */
export const DURABLE_SINK = {
  name: 'durable',
  path: '/api/v1/push',
  format(_queue, _partition, batch) {
    return { items: batch }
  }
}

/** The ephemeral push wire (EPHEMERAL_QUEUES.md §3.1): identity on the envelope. */
export const EPHEMERAL_SINK = {
  name: 'ephemeral',
  path: '/api/v1/ephemeral/push',
  format(queue, partition, batch) {
    const body = { queue }
    // Omitted, never defaulted client-side: which partition an ephemeral push
    // without one lands on is the broker's rule, and inventing a 'Default' here
    // would take that decision away from it in a way the caller never asked for.
    if (partition !== null && partition !== undefined) body.partition = partition
    body.messages = batch
    return body
  }
}

/**
 * What a buffer drains into: the sink, plus the identity that sink formats for.
 * The default is the durable push, so a buffer created without one behaves
 * exactly as buffers did before sinks existed.
 */
export const DURABLE_DESTINATION = { sink: DURABLE_SINK, queue: null, partition: null }

/** The ephemeral counterpart, bound to one (queue, partition). */
export function ephemeralDestination(queue, partition = null) {
  return { sink: EPHEMERAL_SINK, queue, partition }
}

/**
 * The durable buffer address, unchanged: `queue/partition`. Kept here next to
 * its ephemeral sibling so the two can be compared at a glance.
 */
export function durableAddress(queue, partition) {
  return `${queue}/${partition}`
}

/**
 * The ephemeral buffer address: `eph:queue/partition`, or `eph:queue` when the
 * caller named no partition (which is a different destination from any named
 * one, because the broker picks, and a buffer must not merge the two).
 *
 * Same ambiguity as the durable address -- a queue named `a/b` collides with
 * (`a`, `b`) -- inherited deliberately rather than fixed on one side only.
 */
export function ephemeralAddress(queue, partition = null) {
  return partition === null || partition === undefined ? `eph:${queue}` : `eph:${queue}/${partition}`
}
