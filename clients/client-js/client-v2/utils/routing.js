/**
 * Routing response contracts (PLAN_ROUTING.md §4, §5): the conflation
 * pattern (utils/conflation.js) for filtered groups and reading queues.
 *
 *   "filtered": true   on EVERY answer of a pop that sent a `where`, empty ones
 *                      included: a filtered pop answers 200 with a body, never
 *                      a bodiless 204, so the echo has somewhere to sit.
 *   "route": {...}     on the answer of a configure that sent `from`.
 *
 * DEGRADE LOUDLY. No SDK negotiates a version with the broker. A broker older
 * than these features ignores the unknown pop parameter and the unknown
 * options: the "filtered" group gets every message of the queue, and the
 * "reading" queue is an ordinary one that nothing fills -- the silent failures
 * the features must not have. So: asked for + no echo => raise, on the FIRST
 * answer, before a single message is handled. A 204 (null) is the case that
 * matters most: it is what an idle queue looks like on an old broker.
 */

/** `error.code` on a filtered pop the broker did not filter. */
export const FILTERED_GROUPS_UNSUPPORTED = 'filtered_groups_unsupported'

/** `error.code` on a configure with `from` the broker did not route. */
export const READING_QUEUES_UNSUPPORTED = 'reading_queues_unsupported'

/**
 * Inspect one pop answer on behalf of a consumer that sent a `where`. Call it
 * for every answer, empty ones included.
 *
 * @param {object|null} result - the parsed pop answer (null for a 204)
 * @throws {Error} with `.code === FILTERED_GROUPS_UNSUPPORTED` when it does
 *   not carry `"filtered": true`
 */
export function checkFilteredResponse(result) {
  if (result && result.filtered === true) return
  const error = new Error(
    'this broker does not filter consumer groups (a pop with a where came back without "filtered": true); ' +
    'upgrade it to a broker with filtered groups'
  )
  error.code = FILTERED_GROUPS_UNSUPPORTED
  throw error
}

/**
 * Inspect the answer of a configure that sent `from`.
 *
 * @param {object|null} result - the parsed configure answer
 * @throws {Error} with `.code === READING_QUEUES_UNSUPPORTED` when it has no `route`
 */
export function checkRouteAnswer(result) {
  if (result && typeof result === 'object' && result.route && typeof result.route === 'object') return
  const error = new Error(
    'this broker does not read other queues (a configure with from came back without a route); ' +
    'upgrade it to a broker with reading queues'
  )
  error.code = READING_QUEUES_UNSUPPORTED
  throw error
}
