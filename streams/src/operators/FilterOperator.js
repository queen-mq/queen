/**
 * FilterOperator — drop envelopes where predicate returns false.
 *
 * The predicate sees the original Queen `msg` (msg.data, msg.partitionId, ...)
 * to keep filtering decisions tied to the source record, not to a possibly
 * already-mapped value.
 */
export class FilterOperator {
  constructor(predicate) {
    this.kind = 'filter'
    this.fn = predicate
  }

  async apply(envelope) {
    const keep = await this.fn(envelope.msg, envelope.ctx)
    return keep ? [envelope] : []
  }
}
