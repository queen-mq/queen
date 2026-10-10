// The console's settings: one JSON document in the tenant's KV that moves the
// lines this console judges by.
//
//   KV  queen.console / settings
//   { "defaults": { "lagWarnSeconds": 120 },
//     "queues":   { "reports.nightly": { "lagWarnSeconds": 900, "lagBadSeconds": 3600 },
//                   "audit.archive":   { "noReaderOk": true } } }
//
// A line is looked up on the queue first, then in `defaults`, then in the
// built-in table (useSeverity.js THRESHOLDS), so a document that says nothing
// changes nothing. LINES is the list of what may be moved. Everything else in
// THRESHOLDS is a floor, a fallback or a limit of the broker itself, and stays
// where it is. A queue may also carry a flag (QUEUE_FLAGS): a yes or no about
// that one queue, with no tenant-wide form.
//
// The document is shared: the Telegram alerter reads the same key and carries
// this same file. So there are no imports and nothing of Vue here — the
// built-in table comes in as an argument — and test/settings-doc.test.js runs
// the file as it is.

export const SETTINGS_NS = 'queen.console'
export const SETTINGS_KEY = 'settings'

/**
 * The lines a tenant may move, in the order the Settings page lists them.
 *
 *   unit     's' a whole number of seconds; '%' a share, stored as 0–1
 *   perQueue the line may also be set for one queue
 *   below    the line this one has to stay under (attention before failing)
 */
export const LINES = Object.freeze([
  {
    key: 'lagWarnSeconds', group: 'Consumer lag', label: 'Behind after', unit: 's', perQueue: true, below: 'lagBadSeconds',
    help: 'A consumer group whose oldest unconsumed message is older than this needs attention.',
  },
  {
    key: 'lagBadSeconds', group: 'Consumer lag', label: 'Falling behind after', unit: 's', perQueue: true,
    help: 'Older than this, the group is failing to keep up.',
  },
  {
    key: 'backlogWarnSeconds', group: 'Backlog', label: 'Attention at', unit: 's', perQueue: true, below: 'backlogBadSeconds',
    help: 'Pending messages, counted as seconds of work at the rate they are being acked.',
  },
  {
    key: 'backlogBadSeconds', group: 'Backlog', label: 'Failing at', unit: 's', perQueue: true,
    help: 'The same measure, where it turns red.',
  },
  {
    key: 'ackWarnRate', group: 'Ack failures', label: 'Attention at', unit: '%', perQueue: true, below: 'ackBadRate',
    help: 'Failed acks as a share of the acks attempted in the window.',
  },
  {
    key: 'ackBadRate', group: 'Ack failures', label: 'Failing at', unit: '%', perQueue: true,
    help: 'Red also needs a real number of failures, so a quiet queue cannot trip it.',
  },
  {
    key: 'memWarnShare', group: 'Broker', label: 'Memory, attention at', unit: '%', below: 'memBadShare',
    help: 'A node’s resident memory as a share of the limit it runs under.',
  },
  {
    key: 'memBadShare', group: 'Broker', label: 'Memory, failing at', unit: '%',
    help: 'At the limit the process is killed, so red has to come before it.',
  },
  {
    key: 'cpuWarnShare', group: 'Broker', label: 'CPU, attention at', unit: '%',
    help: 'A node’s CPU as a share of the CPUs it may use. Saturation, so never red.',
  },
])

export const LINE_META = Object.freeze(Object.fromEntries(LINES.map((m) => [m.key, m])))

/** The lines that may be set for one queue. */
export const QUEUE_LINES = LINES.filter((m) => m.perQueue)

/** What may be said of one queue with a yes: stored as `true`, or not stored. */
export const QUEUE_FLAGS = Object.freeze([
  {
    key: 'noReaderOk', label: 'No reader is expected', said: 'no reader expected',
    help: 'A queue that holds messages while no consumer group reads it needs attention. ' +
      'Turn this on for a queue that is read later, or by something that is not a consumer group.',
  },
])

/** `[{ name, lines }]`: lines under the heading each one names, in the catalogue's order. */
export function lineGroups(lines) {
  const out = []
  for (const meta of lines) {
    const last = out[out.length - 1]
    if (last?.name === meta.group) last.lines.push(meta)
    else out.push({ name: meta.group, lines: [meta] })
  }
  return out
}

const SECONDS_MAX = 31_536_000 // a year

const plain = (v) => (v && typeof v === 'object' && !Array.isArray(v) ? v : {})

/** A stored value as a number this file will use, or null. A line is never guessed at. */
function lineValue(meta, v) {
  if (typeof v !== 'number' || !Number.isFinite(v)) return null
  if (meta.unit === 's') return Number.isInteger(v) && v >= 1 && v <= SECONDS_MAX ? v : null
  return v > 0 && v <= 1 ? v : null
}

function cleanLines(raw, perQueue) {
  const src = plain(raw)
  const out = {}
  for (const meta of perQueue ? QUEUE_LINES : LINES) {
    if (!Object.hasOwn(src, meta.key)) continue
    const v = lineValue(meta, src[meta.key])
    if (v !== null) out[meta.key] = v
  }
  if (perQueue) {
    for (const flag of QUEUE_FLAGS) {
      if (Object.hasOwn(src, flag.key) && src[flag.key] === true) out[flag.key] = true
    }
  }
  return out
}

/**
 * The stored value as `{ defaults, queues }`, `queues` a Map of name → that
 * queue's own lines and flags. Only what this file knows and can use is kept:
 * an unknown key, a string where a number belongs or a queue left with
 * nothing usable is skipped, so a hand-edited document can make a line fall
 * back to the built-in one and nothing worse.
 */
export function readSettings(raw) {
  const doc = plain(raw)
  const queues = new Map()
  for (const [name, entry] of Object.entries(plain(doc.queues))) {
    const lines = cleanLines(entry, true)
    if (Object.keys(lines).length) queues.set(name, lines)
  }
  return { defaults: cleanLines(doc.defaults, false), queues }
}

/** The lines in force for one queue, with its flags; with no queue, the tenant's. */
export function resolveLines(base, settings, queue) {
  const own = queue === undefined || queue === null ? null : settings?.queues?.get(queue)
  return { ...base, ...(settings?.defaults || {}), ...(own || {}) }
}

/** '90 s' or '5%': a line as a sentence names it. */
export function formatLine(meta, value) {
  if (meta.unit === 's') return `${value} s`
  return `${Number((value * 100).toFixed(4))}%`
}

/** Seconds as a person says them: '45s', '10m', '1h 30m', '2d'. */
export function formatSpan(seconds) {
  const s = Math.round(Number(seconds))
  if (!Number.isFinite(s) || s < 0) return '—'
  if (s < 60) return `${s}s`
  const [big, small, unit, sub] = s < 3600 ? [60, 1, 'm', 's'] : s < 86400 ? [3600, 60, 'h', 'm'] : [86400, 3600, 'd', 'h']
  const rest = Math.floor((s % big) / small)
  return rest ? `${Math.floor(s / big)}${unit} ${rest}${sub}` : `${Math.floor(s / big)}${unit}`
}

/**
 * One queue's own settings as short phrases, for a list:
 * ['Consumer lag 10m / 30m', 'Backlog 5m / —', 'no reader expected']. A pair
 * is named by its heading, attention first; '—' is the side left to the tenant.
 */
export function summarizeQueue(own) {
  const set = plain(own)
  const said = (meta) => (set[meta.key] === undefined ? '—'
    : meta.unit === 's' ? formatSpan(set[meta.key]) : formatLine(meta, set[meta.key]))
  const out = []
  for (const group of lineGroups(QUEUE_LINES)) {
    if (group.lines.some((m) => set[m.key] !== undefined)) out.push(`${group.name} ${group.lines.map(said).join(' / ')}`)
  }
  for (const flag of QUEUE_FLAGS) if (set[flag.key] === true) out.push(flag.said)
  return out
}

/** A line as the text of its input: seconds as they are, a share as a percentage. */
export function toInput(meta, value) {
  if (value === undefined || value === null) return ''
  return meta.unit === 's' ? String(value) : String(Number((value * 100).toFixed(4)))
}

/** The text of an input as a line: null for a blank field, NaN for text that is not a number. */
export function fromInput(meta, text) {
  const s = typeof text === 'string' ? text.trim() : text === null || text === undefined ? '' : String(text)
  if (s === '') return null
  if (meta.unit === 's') return /^\d+$/.test(s) ? Number(s) : NaN
  return /^\d+(\.\d+)?$/.test(s) ? Number(s) / 100 : NaN
}

/**
 * `{key: message}` for `lines` (`{key: number|null}`, null = not set) laid
 * over `base`, the table they will sit on: the built-in one for `defaults`,
 * built-in plus defaults for a queue.
 */
export function validateLines(base, lines, { perQueue = false } = {}) {
  const errors = {}
  const merged = { ...base }
  const set = plain(lines)
  for (const meta of perQueue ? QUEUE_LINES : LINES) {
    const v = set[meta.key]
    if (v === undefined || v === null) continue
    if (lineValue(meta, v) === null) {
      errors[meta.key] = meta.unit === 's'
        ? 'A whole number of seconds, 1 or more.'
        : 'A percentage above 0, up to 100.'
      continue
    }
    merged[meta.key] = v
  }
  for (const meta of perQueue ? QUEUE_LINES : LINES) {
    if (!meta.below || errors[meta.key] || errors[meta.below]) continue
    if (merged[meta.key] < merged[meta.below]) continue
    const upper = LINE_META[meta.below]
    // Said on the field that was typed in; on the lower one when both were.
    const at = set[meta.key] !== undefined && set[meta.key] !== null ? meta.key : meta.below
    errors[at] = `“${meta.label}” (${formatLine(meta, merged[meta.key])}) has to be lower than ` +
      `“${upper.label}” (${formatLine(upper, merged[meta.below])}).`
  }
  return errors
}

// Writing. Both take the value AS STORED and give back the value to store, and
// both replace only the lines this file knows: a field another writer added (a
// newer console, the alerter) is still there after a save from this one.

function setLines(entry, lines, perQueue) {
  const known = new Set((perQueue ? [...QUEUE_LINES, ...QUEUE_FLAGS] : LINES).map((m) => m.key))
  const kept = Object.entries(plain(entry)).filter(([key]) => !known.has(key))
  return Object.fromEntries([...kept, ...Object.entries(cleanLines(lines, perQueue))])
}

/** The stored value with the tenant's lines replaced by `lines`. */
export function withDefaults(raw, lines) {
  const doc = plain(raw)
  return { ...doc, defaults: setLines(doc.defaults, lines, false) }
}

/** The stored value with one queue's lines and flags replaced by `lines`; none left removes the queue. */
export function withQueue(raw, queue, lines) {
  const doc = plain(raw)
  const queues = plain(doc.queues)
  const entry = setLines(Object.hasOwn(queues, queue) ? queues[queue] : null, lines, true)
  const rest = Object.entries(queues).filter(([name]) => name !== queue)
  if (Object.keys(entry).length) rest.push([queue, entry])
  return { ...doc, queues: Object.fromEntries(rest) }
}
