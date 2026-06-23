/**
 * Logger utility for Queen Client v2
 *
 * Supports pluggable logger backends (pino, winston, bunyan, etc.).
 * When no custom logger is configured, falls back to console-based logging
 * gated by the QUEEN_CLIENT_LOG environment variable (Node.js)
 * or window.QUEEN_CLIENT_LOG (Browser).
 */

const LOG_ENABLED = (() => {
  // Node.js environment
  if (typeof process !== 'undefined' && process.env) {
    return process.env.QUEEN_CLIENT_LOG === 'true'
  }
  // Browser environment
  if (typeof window !== 'undefined') {
    return window.QUEEN_CLIENT_LOG === true
  }
  return false
})()

let customLogger = null
// When true, details are passed to the custom logger as structured fields
// (pino/bunyan style: `info(fields, message)`). Opt-in for backward compatibility.
let structuredMode = false

function getTimestamp() {
  return new Date().toISOString()
}

/**
 * Format log message with timestamp and operation
 */
function formatLog(operation, details, level = 'INFO') {
  const timestamp = getTimestamp()
  const detailsStr = typeof details === 'object' ? JSON.stringify(details) : details
  return `[${timestamp}] [${level}] [${operation}] ${detailsStr}`
}

/**
 * Configure a custom logger backend.
 *
 * The logger must implement: info(msg), warn(msg), error(msg).
 * debug(msg) is optional and falls back to info(msg) if missing.
 * When a custom logger is set, it is always active (no env var gating).
 *
 * By default (backward compatible) the logger is called with a single formatted
 * string: `info('[operation] {json}')`.
 *
 * Pass `{ structured: true }` to instead call the logger pino/bunyan style —
 * `info(fields, message)` — so `details` become structured, queryable fields
 * (the `operation` is also added as a field and used as the message). Use this
 * only with loggers that accept a leading merge object (e.g. pino, bunyan).
 *
 * The built-in console fallback (no custom logger) is unaffected by this option.
 *
 * @param {object} logger - Logger instance (e.g. pino(), winston.createLogger())
 * @param {{ structured?: boolean }} [options]
 */
export function configure(logger, options = {}) {
  if (logger && typeof logger.info !== 'function') {
    throw new Error('Custom logger must implement info(), warn(), and error() methods')
  }
  customLogger = logger
  structuredMode = options != null && options.structured === true
}

/**
 * Emit a log through the configured custom logger.
 * - structuredMode: `details` keys become top-level fields, `operation` is added
 *   as a field, and the operation is the message (pino/bunyan style).
 * - default (legacy): a single `[operation] {json}` string is passed.
 * Missing level methods fall back to info().
 */
function emitToCustomLogger(method, operation, details) {
  const fn = customLogger[method] || customLogger.info

  if (structuredMode) {
    let fields
    if (details && typeof details === 'object' && !Array.isArray(details)) {
      fields = { operation, ...details }
    } else if (details !== undefined) {
      fields = { operation, details }
    } else {
      fields = { operation }
    }
    fn.call(customLogger, fields, operation)
    return
  }

  const detailsStr = typeof details === 'object' ? JSON.stringify(details) : details
  fn.call(customLogger, `[${operation}] ${detailsStr}`)
}

export function log(operation, details) {
  if (customLogger) {
    emitToCustomLogger('info', operation, details)
    return
  }
  if (!LOG_ENABLED) return
  console.log(formatLog(operation, details))
}

export function debug(operation, details) {
  if (customLogger) {
    emitToCustomLogger('debug', operation, details)
    return
  }
  if (!LOG_ENABLED) return
  console.log(formatLog(operation, details, 'DEBUG'))
}

export function warn(operation, details) {
  if (customLogger) {
    emitToCustomLogger('warn', operation, details)
    return
  }
  if (!LOG_ENABLED) return
  console.warn(formatLog(operation, details, 'WARN'))
}

export function error(operation, details) {
  if (customLogger) {
    emitToCustomLogger('error', operation, details)
    return
  }
  if (!LOG_ENABLED) return
  console.error(formatLog(operation, details, 'ERROR'))
}

/**
 * Check if logging is enabled
 */
export function isEnabled() {
  return customLogger != null || LOG_ENABLED
}
