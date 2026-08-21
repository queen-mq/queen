/**
 * Queen Message Queue Client - Entry Point
 *
 * One npm package for everything:
 *   import { Queen, Stream, tokenBucketGate, slidingWindowGate } from 'queen-mq'
 *
 * The broker client (`Queen`, `Admin`) and the fluent streaming SDK
 * (`Stream`, `tokenBucketGate`, `slidingWindowGate`, plus all operator
 * classes) ship in the same package. ESM named exports + tree-shaking
 * mean bundlers (Vite, Rollup, webpack with `sideEffects:false`) can
 * drop the streaming code from your output if you only import broker
 * symbols.
 *
 * Streaming source lives under `./streams/` of this package.
 */

export { Queen } from './Queen.js'
export { Admin } from './admin/Admin.js'
// RAM-class queues (EPHEMERAL_QUEUES.md §4). Reached as `queen.ephemeral`; the
// class is exported for typing and for embedding it on a client of your own.
// The two `.code`s the family's 404s carry, which are NOT the same fact:
// `EPHEMERAL_UNSUPPORTED` is a broker or proxy older than 1.1 (the routes are
// not there), `EPHEMERAL_QUEUE_NOT_FOUND` is `depth` on a queue that does not
// exist. Branch on the code, never on the message.
export { Ephemeral, EPHEMERAL_UNSUPPORTED, EPHEMERAL_QUEUE_NOT_FOUND } from './ephemeral/Ephemeral.js'
export { CLIENT_DEFAULTS, QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS, BUFFER_DEFAULTS } from './utils/defaults.js'
// Conflation degrade-loudly (PLAN_CONFLATION §4): a consumer that declares
// `.conflation(true)` against a broker older than 1.1.0 gets an error with this
// `.code`, so callers can branch on it (alert, fall back, exit) without
// matching on the message text.
export { CONFLATION_UNSUPPORTED } from './utils/conflation.js'

// Streaming SDK — full source under ./streams/.
export { Stream } from './streams/Stream.js'
export { tokenBucketGate, slidingWindowGate } from './streams/helpers/rateLimiter.js'
