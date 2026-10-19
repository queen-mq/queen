// Partial analytics answers — PURE functions, no Vue, so `node --test` reaches them.
//
// The broker's per-queue and per-node metrics are NOT replicated: each node
// keeps the rows of the traffic it served itself, and an analytics read
// gathers every peer's rows when it is asked. A peer that does not answer in
// time (restarting, partitioned, slow) is simply left out, and the numbers
// that come back are smaller — not wrong-looking, just smaller. The worst case
// is the common one: a Kafka produce is counted on the raft leader's process,
// so while the node that led during the window restarts, the Overview reads
// about one push a second for fifteen hundred (stage, a rolling restart).
//
// From 2.5.x every gathered answer (queue-ops, workload, status,
// system-metrics, worker-metrics, queue-parked-replicas) carries
// `nodesMissing`: the ids of the nodes that did not answer, `[]` when every
// node did. An older broker does not send it at all, which is "not said", not
// "complete" — and is rendered as nothing, because there is nothing to say.

/**
 * The node ids missing from any of the answers, ascending and once each.
 *
 * @param {...(object|null|undefined)} payloads analytics answers
 * @returns {Array<number|string>}
 */
export function missingNodes(...payloads) {
  const out = new Set()
  for (const p of payloads) {
    const m = p?.nodesMissing
    if (!Array.isArray(m)) continue
    for (const id of m) if (id !== null && id !== undefined) out.add(id)
  }
  return [...out].sort((a, b) => (Number(a) - Number(b)) || String(a).localeCompare(String(b)))
}

const listOf = (ids) => {
  const names = ids.map(String)
  if (names.length <= 1) return names.join('')
  return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`
}

/**
 * The chip's words: which nodes did not answer and what that does to the
 * numbers. Empty when nothing is missing.
 *
 * @param {Array<number|string>} ids from {@link missingNodes}
 * @returns {string}
 */
export function partialText(ids) {
  if (!ids?.length) return ''
  return ids.length === 1
    ? `Partial: node ${ids[0]} did not answer — its traffic is missing from these numbers`
    : `Partial: nodes ${listOf(ids)} did not answer — their traffic is missing from these numbers`
}

/** The chip's tooltip: why a node's absence is a hole and not a dip. */
export const PARTIAL_TITLE = 'Each broker node keeps the metrics of the traffic it served itself, and '
  + 'this read gathers them from every node. The nodes named did not answer in time (restarting, '
  + 'unreachable or slow), so what they counted is not in these numbers: a Kafka produce is counted '
  + 'on the node that led the cluster, so its absence can take most of the push rate with it. '
  + 'The next refresh asks them again.'
