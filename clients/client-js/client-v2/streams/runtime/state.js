/**
 * State-read helper. Calls /streams/v1/state/get.
 *
 * The server endpoint is batchable in libqueen — multiple workers' reads
 * can be merged into one drain pass — but at the SDK call site we always
 * issue one request per partition cycle (since each cycle's state needs
 * are partition-local).
 */

export async function getState({ http, queryId, partitionId, keys }) {
  const body = {
    query_id: queryId,
    partition_id: partitionId,
    keys: Array.isArray(keys) ? keys : []
  }
  const res = await http.post('/streams/v1/state/get', body)
  if (!res || res.success === false) {
    const e = new Error(res && res.error ? res.error : 'state.get failed')
    e.code = 'STREAMS_STATE_GET_FAILED'
    throw e
  }
  // res.rows: [{ key, value, updated_at }]
  const map = new Map()
  for (const row of res.rows || []) {
    if (row && typeof row.key === 'string') {
      // The reducer stored its accumulator under {acc, windowStart, windowEnd}
      // — we restore that envelope. For raw user-managed state, the .value
      // is whatever the user wrote.
      map.set(row.key, row.value)
    }
  }
  return map
}
