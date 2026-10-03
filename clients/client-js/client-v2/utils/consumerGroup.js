/**
 * The consumer group a popped message belongs to.
 *
 * Every pop answers each message with the group it was claimed under
 * (`consumerGroup`), and the lease an ack has to match belongs to that group.
 * An ack that names no group is judged in queue mode, so a message popped by a
 * group and acked without one is refused: `rejected_ack` from a transaction,
 * `invalid or expired lease` from /ack. Reading the group off the message is
 * what lets `ack(message)` work without the caller repeating `.group(...)`.
 */

/** The broker's name for "no consumer group" (queue mode). */
export const QUEUE_MODE_GROUP = '__QUEUE_MODE__'

/**
 * The group to send with an ack of `message`, or null for queue mode.
 *
 * Null for queue mode on purpose: an ack with no group IS a queue-mode ack, so
 * leaving the key out keeps those requests byte-identical to what this client
 * always sent. Also null for anything that is not a popped message (a bare
 * transactionId string, a hand-built `{transactionId, partitionId}`).
 */
export function consumerGroupOf(message) {
  if (message === null || typeof message !== 'object') return null
  const group = message.consumerGroup
  if (typeof group !== 'string' || group.length === 0 || group === QUEUE_MODE_GROUP) return null
  return group
}

/**
 * The one group to send with a batch ack of `messages`, or null for queue mode.
 *
 * /api/v1/ack/batch carries ONE consumer group for the whole request, so a
 * batch whose messages were popped under different groups cannot be expressed
 * as one call: it throws rather than ack some of them under the wrong group.
 * Messages that state no group (hand-built ones) take whatever the others say.
 */
export function sharedConsumerGroupOf(messages) {
  const named = new Set()
  for (const message of messages) {
    if (message !== null && typeof message === 'object' &&
        typeof message.consumerGroup === 'string' && message.consumerGroup.length > 0) {
      named.add(message.consumerGroup)
    }
  }
  if (named.size > 1) {
    throw new Error(
      `Cannot ack messages from different consumer groups in one call (${[...named].join(', ')}): ` +
      'a batch ack carries one consumer group. Ack each group separately, or pass { group }.'
    )
  }
  const [group] = named
  return group && group !== QUEUE_MODE_GROUP ? group : null
}
