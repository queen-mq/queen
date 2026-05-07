/**
 * Query registration helper.
 *
 * Called once at Runner.start(). Sends the query name + source/sink names +
 * config_hash to /streams/v1/queries. The server returns the canonical
 * query_id (UUID) we'll use for all subsequent /streams/v1/cycle calls.
 *
 * On 409 (config_hash mismatch without reset:true), throws a clear error
 * pointing the user at the `reset: true` escape hatch.
 */

export async function registerQuery({ http, name, sourceQueue, sinkQueue, configHash, reset = false }) {
  const body = {
    name,
    source_queue: sourceQueue,
    config_hash: configHash,
    reset: !!reset
  }
  if (sinkQueue) body.sink_queue = sinkQueue

  let res
  try {
    res = await http.post('/streams/v1/queries', body)
  } catch (err) {
    if (err && err.status === 409 && err.body) {
      const msg = err.body.error || 'config_hash mismatch'
      const hint =
        '\n\nHint: pass `reset: true` to Stream.run({...}) to wipe the existing ' +
        'queen_streams.state for this queryId, or pick a new queryId.'
      const e = new Error(msg + hint)
      e.code = 'STREAMS_CONFIG_HASH_MISMATCH'
      throw e
    }
    throw err
  }

  if (!res || res.success === false) {
    const e = new Error(res && res.error ? res.error : 'register failed')
    e.code = 'STREAMS_REGISTER_FAILED'
    throw e
  }
  return {
    queryId: res.query_id,
    name: res.name,
    fresh: !!res.fresh,
    didReset: !!res.reset
  }
}
