/**
 * A Kafka record as the Kafka facade stores it, decoded for reading.
 *
 * The facade writes each record as ONE Queen payload
 * (protocols/queen-kafka/src/records.rs):
 *
 *   {"h": [{"k": "<header>", "v": <base64 | null>}, …],   // omitted when empty
 *    "k": <base64 | null>,
 *    "t": <producer CreateTime, ms>,                        // omitted when unset
 *    "v": <base64 | null>}
 *
 * base64 because a Kafka key or value is bytes, not text: a protobuf or Avro
 * value is not valid UTF-8. Read raw, every record looks encoded; this shows
 * text as text, JSON as JSON and anything else as hex.
 *
 * A payload is recognised by the facade's own rule (`as_envelope`): an object
 * whose keys are a subset of k/v/h/t, carrying both k and v, every field of the
 * right type and every base64 field decodable. Anything else was written by a
 * native producer and is shown as itself.
 */

const RECORD_KEYS = new Set(['k', 'v', 'h', 't'])
const HEADER_KEYS = new Set(['k', 'v'])
// Padded standard base64: the only form the facade writes (base64 STANDARD).
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/
const utf8 = new TextDecoder('utf-8', { fatal: true })

/** How many bytes of a binary field the hex view shows. */
export const HEX_LIMIT = 1024

const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value)

function bytesOf(base64) {
  if (!BASE64.test(base64)) return null
  const binary = atob(base64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  return bytes
}

/**
 * One stored field: `{kind: 'null'}`, or its bytes read as `json` (an object or
 * an array), `text` (valid UTF-8) or `binary`. `undefined` when the field is
 * neither null nor decodable base64, which makes the payload not a record.
 */
function fieldOf(stored) {
  if (stored === null) return { kind: 'null', size: 0 }
  if (typeof stored !== 'string') return undefined
  const bytes = bytesOf(stored)
  if (!bytes) return undefined
  const size = bytes.length
  let text
  try {
    text = utf8.decode(bytes)
  } catch {
    return { kind: 'binary', size, base64: stored, bytes }
  }
  // Only a document is shown as JSON: a bare number or string reads better as
  // the text it is.
  const lead = text.trimStart()[0]
  if (lead === '{' || lead === '[') {
    try {
      return { kind: 'json', size, text, json: JSON.parse(text) }
    } catch {
      // Not JSON after all: text.
    }
  }
  return { kind: 'text', size, text }
}

/**
 * The record a payload carries, or `null` when the payload is not one.
 * `payload` is the message detail's payload: the parsed JSON, or its text.
 */
export function kafkaRecordOf(payload) {
  let stored = payload
  if (typeof stored === 'string') {
    try {
      stored = JSON.parse(stored)
    } catch {
      return null
    }
  }
  if (!isObject(stored)) return null
  if (!Object.keys(stored).every(k => RECORD_KEYS.has(k))) return null
  if (!('k' in stored) || !('v' in stored)) return null
  const key = fieldOf(stored.k)
  const value = fieldOf(stored.v)
  if (!key || !value) return null

  const headers = []
  if ('h' in stored) {
    if (!Array.isArray(stored.h)) return null
    for (const entry of stored.h) {
      if (!isObject(entry) || !Object.keys(entry).every(k => HEADER_KEYS.has(k))) return null
      if (typeof entry.k !== 'string' || !('v' in entry)) return null
      const headerValue = fieldOf(entry.v)
      if (!headerValue) return null
      headers.push({ name: entry.k, value: headerValue })
    }
  }

  let timestamp = null
  if ('t' in stored) {
    if (!Number.isInteger(stored.t)) return null
    timestamp = stored.t
  }
  return { key, value, headers, timestamp }
}

/** `timestamp` as an ISO string, or `null` when unset or out of range. */
export function recordTime(timestamp) {
  if (timestamp === null || timestamp < 0) return null
  const date = new Date(timestamp)
  return Number.isNaN(date.getTime()) ? null : date.toISOString()
}

/**
 * A field as one short line: the text itself, `null`, or a note for binary.
 * For keys and headers, which are almost always short text.
 */
export function fieldLine(field) {
  switch (field.kind) {
    case 'null': return 'null'
    case 'binary': return `binary, ${field.size} bytes: ${hexOf(field.bytes, 32)}`
    default: return field.text
  }
}

/** Space-separated hex of the first `limit` bytes, with an ellipsis past it. */
export function hexOf(bytes, limit = HEX_LIMIT) {
  const shown = Array.from(bytes.subarray(0, limit), b => b.toString(16).padStart(2, '0')).join(' ')
  return bytes.length > limit ? `${shown} …` : shown
}

/** A classic hex dump (offset, 16 bytes, ASCII) of the first `limit` bytes. */
export function hexDump(bytes, limit = HEX_LIMIT) {
  const lines = []
  const end = Math.min(bytes.length, limit)
  for (let at = 0; at < end; at += 16) {
    const row = bytes.subarray(at, Math.min(at + 16, end))
    const hex = Array.from(row, b => b.toString(16).padStart(2, '0')).join(' ')
    const ascii = Array.from(row, b => (b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.')).join('')
    lines.push(`${at.toString(16).padStart(8, '0')}  ${hex.padEnd(47)}  ${ascii}`)
  }
  if (bytes.length > end) lines.push(`… ${bytes.length - end} more bytes`)
  return lines.join('\n')
}

function plainOf(field) {
  switch (field.kind) {
    case 'null': return null
    case 'json': return field.json
    case 'text': return field.text
    default: return { base64: field.base64 }
  }
}

/**
 * The record as plain JSON, for the clipboard: text as text, JSON as JSON,
 * binary as `{"base64": …}`. Headers stay a list, because Kafka allows one name
 * more than once.
 */
export function kafkaRecordPlain(record) {
  return {
    key: plainOf(record.key),
    timestamp: record.timestamp,
    time: recordTime(record.timestamp),
    headers: record.headers.map(h => ({ name: h.name, value: plainOf(h.value) })),
    value: plainOf(record.value),
  }
}
