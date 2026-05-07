/**
 * KeyByOperator — override the implicit partition-key with a user-derived
 * key for downstream stateful operators (windowTumbling/reduce/aggregate).
 *
 * The default key is the source partition_id, which gives per-partition
 * state isolation with zero cross-worker contention. .keyBy(...) overrides
 * to a different value per record. The Runner will warn at start time if
 * the chain's logical key consistently differs from the partition key,
 * because that means multiple workers may write the same logical key from
 * different partition leases — i.e. cross-worker contention on
 * (query_id, partition_id, key) state rows.
 */
export class KeyByOperator {
  constructor(fn) {
    this.kind = 'keyBy'
    this.fn = fn
  }

  async apply(envelope) {
    const key = String(await this.fn(envelope.msg))
    return [{ ...envelope, key }]
  }
}
