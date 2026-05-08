/**
 * GateOperator — per-message ALLOW/DENY decision with persistent per-key state.
 *
 * Used to build rate limiters, throttlers, fairness gates, circuit breakers,
 * and any other "should this message proceed RIGHT NOW?" pattern.
 *
 * Semantics
 * ---------
 * The runtime processes a popped batch sequentially in source order. For each
 * message, it builds a `ctx` object with mutable `state` (loaded from
 * `queen_streams.state` for the message's key) and invokes the user fn.
 *
 *   .gate((msg, ctx) => boolean)
 *
 * Returning `true` (or `{allow:true}`):
 *   - The mutated `ctx.state` is persisted as part of the cycle.
 *   - Downstream operators (sink, foreach) receive this message.
 *   - The message counts toward the cycle's ack count.
 *
 * Returning `false` (or `{allow:false}`):
 *   - The mutated state is DISCARDED for this message (it didn't "happen").
 *   - The runner stops processing the rest of the batch.
 *   - The cycle commits with `release_lease=false` and `ack.count = K`
 *     (the count of allowed messages BEFORE this one).
 *   - The message and its successors stay leased until natural lease
 *     expiry; the broker then redelivers them to a runner in their
 *     original partition order.
 *
 * If K === 0 (the very first message is denied), the runner skips the
 * cycle entirely — no state, no push, no ack — and the lease times out
 * naturally.
 *
 * Ordering
 * --------
 * FIFO per-partition is preserved by construction: the same partition lease
 * is held continuously across the deny → expiry → redeliver cycle, so the
 * denied messages never overtake messages that arrived after them.
 *
 * State key
 * ---------
 * State is keyed by the envelope's `key` (typically the source partition_id
 * unless `.keyBy()` overrides it). For a rate limiter, the natural setup is:
 *   - source queue partitioned by the rate-limit key (e.g. tenantId)
 *   - no .keyBy() needed (key defaults to partitionId)
 *
 * If the key DIVERGES from the partition (cross-partition keying), the same
 * cross-worker contention warning that applies to .reduce() applies here too:
 * two workers may hold leases for different partitions but write the same
 * (query_id, partition_id, key) row. For rate limiting this is almost always
 * a configuration mistake — keep partition == limit key.
 *
 * Why a dedicated operator
 * ------------------------
 * The "stop the batch + don't release the lease" semantics has no analog in
 * the existing operators (.map / .filter / .reduce all process the full batch).
 * Modelling it as a return-value side effect of a regular .map would be
 * surprising; a dedicated `.gate()` makes the intent and the cycle behaviour
 * explicit at the chain site.
 */
export class GateOperator {
  constructor(fn) {
    if (typeof fn !== 'function') {
      throw new Error('.gate(fn) requires a function')
    }
    this.kind = 'gate'
    this.fn = fn
  }

  /**
   * Evaluate the gate for a single envelope. Called by the runtime, not by
   * other operators. Returns `{ allow: boolean }`.
   *
   * The user fn receives `(msg, ctx)` where ctx.state is mutable and ctx
   * also carries `streamTimeMs` (system clock) and `partitionId` for
   * observability / time-based bucket math.
   */
  async evaluate(envelope, ctx) {
    const out = await this.fn(envelope.value, ctx)
    if (out === true || out === false) return { allow: out }
    if (out && typeof out === 'object' && typeof out.allow === 'boolean') {
      return { allow: out.allow }
    }
    throw new Error('.gate(fn) must return boolean or { allow: boolean }')
  }
}
