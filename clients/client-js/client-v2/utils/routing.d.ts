/**
 * Routing response contracts for filtered groups and reading queues. A
 * broker older than these features ignores them; the client raises on the
 * first answer that lacks the echo instead of degrading silently.
 */

/** `error.code` on a filtered pop the broker did not filter. */
export declare const FILTERED_GROUPS_UNSUPPORTED: 'filtered_groups_unsupported'

/** `error.code` on a configure with `from` the broker did not route. */
export declare const READING_QUEUES_UNSUPPORTED: 'reading_queues_unsupported'

/** Throws `.code === FILTERED_GROUPS_UNSUPPORTED` when a filtered pop's answer lacks `"filtered": true`. */
export declare function checkFilteredResponse(result: unknown): void

/** Throws `.code === READING_QUEUES_UNSUPPORTED` when a configure with `from` was answered without a `route`. */
export declare function checkRouteAnswer(result: unknown): void
