/**
 * Validation utilities
 */

const UUID_V4_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

export function isValidUUID(str) {
  return typeof str === 'string' && UUID_V4_REGEX.test(str)
}

// A header name as the broker takes it (server/src/frames.rs,
// header_key_valid): 1 to 64 bytes of a-z, 0-9, '.', '_' or '-'.
export const HEADER_NAME = /^[a-z0-9._-]{1,64}$/

// The prefix of the headers the broker sets: a client may not set them.
const RESERVED_HEADER_PREFIX = 'queen-'

/**
 * A message's headers, checked: an object of string values whose names are 1
 * to 64 bytes of a-z, 0-9, '.', '_' or '-', none starting with `queen-`.
 * Answers a copy, or null for none (absent, null or `{}`), so a message
 * without headers puts nothing new on the wire. How many and how large (64
 * headers and 8 KiB a message by default) are the broker's settings: it
 * refuses beyond them with 400 `bad_body`.
 *
 * @param {object|null|undefined} headers
 * @param {string} [what='push'] - the call, for the error message
 * @returns {object|null}
 */
export function checkHeaders(headers, what = 'push') {
  if (headers === undefined || headers === null) return null
  if (typeof headers !== 'object' || Array.isArray(headers)) {
    throw new Error(`${what}: headers must be an object of strings, got ${JSON.stringify(headers)}`)
  }
  const names = Object.keys(headers)
  if (names.length === 0) return null
  const out = {}
  for (const name of names) {
    if (!HEADER_NAME.test(name)) {
      throw new Error(`${what}: header name ${JSON.stringify(name)}: 1 to 64 bytes of a-z, 0-9, '.', '_' or '-'`)
    }
    if (name.startsWith(RESERVED_HEADER_PREFIX)) {
      throw new Error(`${what}: header name ${JSON.stringify(name)}: names starting with "queen-" are the broker's`)
    }
    const value = headers[name]
    if (typeof value !== 'string') {
      throw new Error(`${what}: header ${JSON.stringify(name)}: the value must be a string, got ${JSON.stringify(value)}`)
    }
    out[name] = value
  }
  return out
}

export function validateQueueName(name) {
  if (typeof name !== 'string' || name.trim().length === 0) {
    throw new Error('Queue name must be a non-empty string')
  }
  return name.trim()
}

export function validateUrl(url) {
  if (typeof url !== 'string' || !url.startsWith('http')) {
    throw new Error(`Invalid URL: ${url}`)
  }
  return url
}

export function validateUrls(urls) {
  if (!Array.isArray(urls) || urls.length === 0) {
    throw new Error('URLs must be a non-empty array')
  }
  return urls.map(validateUrl)
}

