/**
 * MapOperator — transform each input record. Sync or async.
 *
 * Operates on the runtime "envelope" object:
 *   { msg, key, value, ctx? }
 *
 * User-fn signature: `(msg, ctx?) => newValue`
 *
 * Pre-reducer:
 *   msg = the original Queen message (with .data, .partitionId, ...)
 *   ctx = undefined (Queen msg already carries the metadata)
 *
 * Post-reducer (after .windowTumbling + .reduce/.aggregate):
 *   msg = the aggregated value (e.g. { count, sum, min, max })
 *   ctx = { partition, partitionId, key, windowKey, windowStart, windowEnd }
 */
export class MapOperator {
  constructor(fn) {
    this.kind = 'map'
    this.fn = fn
  }

  async apply(envelope) {
    const value = await this.fn(envelope.msg, envelope.ctx)
    return [{ ...envelope, value }]
  }
}
