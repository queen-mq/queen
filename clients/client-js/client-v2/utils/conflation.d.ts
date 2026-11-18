/**
 * Conflation response contract: a consumer that declares `.conflation(true)`
 * against a broker that does not apply it gets an error with
 * `.code === CONFLATION_UNSUPPORTED` on the first response, before a single
 * message is handled.
 */

/** `error.code` on the degrade-loudly error, so callers can branch without string-matching. */
export declare const CONFLATION_UNSUPPORTED: 'conflation_unsupported'

/** The message the plan fixes for every SDK. */
export declare const CONFLATION_UNSUPPORTED_MESSAGE: string

/** The pop target a conflation policy hangs on. */
export interface ConflationContext {
  queue?: string | null | undefined
  namespace?: string | null | undefined
  task?: string | null | undefined
  group?: string | null | undefined
}

/** Identity of the (queue, group) a policy hangs on. */
export declare function conflationKey(ctx: ConflationContext): string

/**
 * Inspect one pop response on behalf of a consumer that ASKED for
 * conflation. Throws with `.code === CONFLATION_UNSUPPORTED` when the broker
 * did not apply conflation and did not explain why.
 */
export declare function checkConflationResponse(result: unknown, ctx: ConflationContext): void

/** Test hook: clears the process-wide warn-once and known-conflict registries. */
export declare function resetConflationWarnings(): void
