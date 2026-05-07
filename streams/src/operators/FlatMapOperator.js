/**
 * FlatMapOperator — emit zero or more values per input record.
 *
 * The user fn returns an array of values; the operator wraps each in an
 * envelope sharing the source message's metadata (so subsequent operators
 * still see the original partition_id etc.).
 */
export class FlatMapOperator {
  constructor(fn) {
    this.kind = 'flatMap'
    this.fn = fn
  }

  async apply(envelope) {
    const out = await this.fn(envelope.msg, envelope.ctx)
    if (!Array.isArray(out)) {
      throw new Error('flatMap fn must return an array (got ' + typeof out + ')')
    }
    return out.map(value => ({ ...envelope, value }))
  }
}
