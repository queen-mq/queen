// What may leave this server. A tool's answer goes to the user's AI provider,
// so application data is hidden unless the operator turned it on, and listed
// fields stay hidden even then.

export const MASKED = '[masked]'
const MAX_STRING = 2_000

const EMAIL = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi
const UUID = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi
const LONG_HEX = /\b[0-9a-f]{16,}\b/gi
const SECRET = /\b(bearer\s+|(?:token|password|passwd|secret|api[-_]?key)["']?\s*[:=]\s*["']?)[^\s"',;]+/gi
const NUMBER = /\d{4,}/g

/** App error text with what usually identifies someone taken out. */
export function redactText(text) {
  if (typeof text !== 'string') return text
  return text
    .replace(SECRET, '$1<hidden>')
    .replace(EMAIL, '<email>')
    .replace(UUID, '<id>')
    .replace(LONG_HEX, '<id>')
    .replace(NUMBER, '<n>')
}

/** Long strings cut, so one huge payload cannot fill the answer. */
export function clip(value, depth = 0) {
  if (typeof value === 'string') {
    return value.length > MAX_STRING ? `${value.slice(0, MAX_STRING)}… [${value.length - MAX_STRING} more characters]` : value
  }
  if (depth > 32 || value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((v) => clip(v, depth + 1))
  return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, clip(v, depth + 1)]))
}

function maskAt(value, parts) {
  if (value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((v) => maskAt(v, parts))
  const [head, ...rest] = parts
  const out = { ...value }
  for (const key of Object.keys(out)) {
    // A header's name has no fixed case on the wire.
    const hit = head === '*' || key === head || key.toLowerCase() === head.toLowerCase()
    if (!hit) continue
    out[key] = rest.length ? maskAt(out[key], rest) : MASKED
  }
  return out
}

export function createGuard({ readMessages = false, mask = [] } = {}) {
  const dataPaths = mask.filter((p) => p.startsWith('data.')).map((p) => p.split('.').slice(1))
  const headerPaths = mask.filter((p) => p.startsWith('headers.')).map((p) => p.split('.').slice(1))

  const maskData = (v) => clip(dataPaths.reduce((out, parts) => maskAt(out, parts), v))
  const maskHeaders = (v) => clip(headerPaths.reduce((out, parts) => maskAt(out, parts), v))

  /** A row that carries a message: its body, its headers and its error text. */
  function message(row) {
    if (!row || typeof row !== 'object') return row
    const out = { ...row }
    for (const f of ['data', 'payload', 'message_payload']) {
      if (!(f in out)) continue
      if (readMessages && out[f] !== null) out[f] = maskData(out[f])
      else delete out[f]
    }
    if ('headers' in out) {
      if (readMessages && out.headers !== null) out.headers = maskHeaders(out.headers)
      else delete out.headers
    }
    for (const f of ['errorMessage', 'error_message', 'lastError', 'error']) {
      if (typeof out[f] === 'string') out[f] = clip(readMessages ? out[f] : redactText(out[f]))
    }
    // The broker sends the body twice on some routes.
    if ('data' in out && 'payload' in out) delete out.payload
    return out
  }

  /** An application value: a KV value, a timer's payload. */
  const value = (v) => (readMessages ? maskData(v) : undefined)

  /** Text a person wrote into a log or an error. */
  const text = (s) => clip(readMessages ? s : redactText(s))

  /** A `where` on a hidden field would answer yes or no about its value. */
  function refusesField(field) {
    const name = String(field ?? '').trim().toLowerCase()
    return mask.map((p) => p.toLowerCase()).some((p) => name === p || name.startsWith(`${p}.`) || p.startsWith(`${name}.`))
  }

  return Object.freeze({ readMessages, message, value, text, refusesField })
}
