/**
 * Conflation response contract (PLAN_CONFLATION.md §3.3, §4).
 *
 * Conflation — last-value delivery for a consumer group — is declared as a pop
 * query parameter and answered in the pop RESPONSE. Two response keys carry the
 * whole client-side contract, and both are handled here so that pop() and the
 * consume loop cannot drift apart:
 *
 *   "conflation": true          the broker actually applied last-value delivery.
 *                               Rides EVERY conflating response, empty ones
 *                               included — a conflating pop answers 200 with a
 *                               body instead of a bodiless 204 precisely so the
 *                               echo has somewhere to sit.
 *   "conflationConflict": true  this consumer declared a policy the group does
 *                               not have. The STORED group setting wins (§3.3);
 *                               the consumer keeps working and warns once.
 *
 * DEGRADE LOUDLY (§4). No SDK negotiates a version with the broker. A 1.1.0
 * client against an older broker sends `conflation=true`, the broker ignores the
 * unknown query parameter, and the consumer silently processes the entire
 * backlog message by message — the exact silent failure the feature must not
 * have. So: requested conflation + no echo => raise, on the FIRST response,
 * before a single message is handled. `null` (an old broker's 204 on an empty
 * pop) is the most important case to raise on, not the least: it is what an idle
 * queue looks like, and it is the first thing a fresh consumer sees.
 */

import * as logger from './logger.js'

/** `error.code` on the degrade-loudly error, so callers can branch without string-matching. */
export const CONFLATION_UNSUPPORTED = 'conflation_unsupported'

/** The message the plan fixes for every SDK (§4). Keep it identical across clients. */
export const CONFLATION_UNSUPPORTED_MESSAGE =
  'conflation was requested but this broker did not apply it — requires broker >= 1.1.0'

// Once per (queue, group) PER PROCESS, per §3.3: a mismatched fleet during a
// rolling deploy would otherwise emit one warning per pop, forever.
const warnedConflicts = new Set()

// A (queue, group) whose conflict the broker has ALREADY told us about. Proof
// that this broker understands the parameter, so a later response without the
// echo for that same pair is the same known conflict and not an old broker.
//
// It is belt-and-braces now rather than the load-bearing part it once was: a
// 1.1.0 broker keeps the 200 and the body whenever the answer has anything to
// say about conflation (`pop_status`), so an EMPTY conflicting pop carries
// `conflationConflict` too and the check above catches it on the first round
// trip. Keeping the memo costs nothing and still covers a first contact whose
// answer somehow arrives without either key.
const knownConflicts = new Set()

/**
 * Identity of the thing a policy hangs on: the queue (or namespace/task pop
 * target) and the consumer group. Deliberately NOT the affinity key, which
 * includes the partition — the policy is per (queue, group).
 */
export function conflationKey({ queue, namespace, task, group }) {
  const target = queue || `${namespace || '*'}/${task || '*'}`
  return `${target}|${group || '__QUEUE_MODE__'}`
}

/**
 * Inspect one pop response on behalf of a consumer that ASKED for conflation.
 * Call it for every response, including empty ones — that is where the old
 * broker shows itself first.
 *
 * @param {object|null} result - parsed pop response (null for a 204)
 * @param {object} ctx - { queue, namespace, task, group }
 * @throws {Error} with `.code === CONFLATION_UNSUPPORTED` when the broker did
 *   not apply conflation and did not explain why.
 */
export function checkConflationResponse(result, ctx) {
  const key = conflationKey(ctx)

  if (result && result.conflationConflict === true) {
    knownConflicts.add(key)
    if (!warnedConflicts.has(key)) {
      warnedConflicts.add(key)
      logger.warn('Conflation.conflict', {
        queue: ctx.queue || null,
        namespace: ctx.namespace || null,
        task: ctx.task || null,
        group: ctx.group || null,
        message:
          'this consumer declared conflation but the consumer group is registered without it — ' +
          'the STORED group setting wins and this consumer is receiving normal batches. ' +
          'Delete/recreate the group to change its policy.'
      })
    }
    return
  }

  if (result && result.conflation === true) return

  // Pop maintenance: the broker refused the pop before it reached the claim
  // path, so there is no policy to echo and nothing to conclude from the
  // absence of one. Reading it as "old broker" would stop every conflating
  // consumer in the fleet the moment an operator pauses pops.
  if (result && result.paused === true) return

  // The group is known to disagree with us; this response is that same
  // conflict, seen through a body that could not carry the flag.
  if (knownConflicts.has(key)) return

  const error = new Error(CONFLATION_UNSUPPORTED_MESSAGE)
  error.code = CONFLATION_UNSUPPORTED
  throw error
}

/**
 * Test hook: the warn-once and known-conflict registries are process-wide by
 * design, which makes them sticky across tests in one runner process.
 */
export function resetConflationWarnings() {
  warnedConflicts.clear()
  knownConflicts.clear()
}
