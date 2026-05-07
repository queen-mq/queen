/**
 * Build and submit a /streams/v1/cycle commit.
 *
 * Inputs: the full cycle context for one (query_id, source partition_id):
 *   - state_ops: upserts/deletes computed by the reducer
 *   - push_items: sink emissions (queue/partition/payload triples)
 *   - ack: source ack (transactionId, leaseId, status, optional error)
 *
 * The server runs streams_cycle_v1 in one transaction; on success the
 * SDK considers the source messages safely processed. On failure (including
 * EXCEPTION inside the SP), the entire cycle is rolled back and the source
 * messages remain visible for redelivery via Queen's existing lease/retry.
 */

export async function commitCycle({ http, queryId, partitionId, consumerGroup, stateOps, pushItems, ack }) {
  const body = {
    query_id: queryId,
    partition_id: partitionId,
    consumer_group: consumerGroup,
    state_ops: stateOps || [],
    push_items: pushItems || [],
    ack: ack || null
  }
  const res = await http.post('/streams/v1/cycle', body)
  if (!res || res.success === false) {
    const e = new Error(res && res.error ? res.error : 'cycle commit failed')
    e.code = 'STREAMS_CYCLE_FAILED'
    e.body = res
    throw e
  }
  return res
}
