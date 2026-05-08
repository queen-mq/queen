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
export { CLIENT_DEFAULTS, QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS, BUFFER_DEFAULTS } from './utils/defaults.js'

// Streaming SDK — full source under ./streams/.
export { Stream } from './streams/Stream.js'
export { tokenBucketGate, slidingWindowGate } from './streams/helpers/rateLimiter.js'
