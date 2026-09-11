// Creating and editing a queue's configuration from the console
// (PLAN_DASHBOARD_ACTIONS.md §2.2) — the option catalogue and the four rules a
// form around `POST /api/v1/configure` needs, as pure functions so
// `app/test/queue-config.test.js` can hold them against the wire instead of a
// browser holding them by eye.
//
// WHAT THE BROKER DOES, and therefore what this file has to get right:
//
// `/configure` MERGES since 1.6.0 (server/sql/procedures/012_configure.sql, the
// parse-section header). Per option, in the effective bag the handler builds:
//
//   key ABSENT         the queue keeps its stored value
//   key present, null  the option goes back to its DEFAULT (the right-hand side
//                      of that option's COALESCE, listed here in OPTIONS)
//   key present, value that value
//   mode: "replace"    every option is re-parsed from defaults, i.e. the
//                      pre-1.6.0 behaviour, which is what a manifest wants
//
// So an editor that posts the whole form would be correct but wasteful and
// racy — it would overwrite an option a colleague changed while the modal was
// open with the value the modal read minutes ago. It posts `configDiff` instead:
// the keys the operator actually touched, and nothing else.
//
// A CLEARED FIELD IS `null`, NOT AN OMISSION. "I want the default back" and "do
// not touch this" are different requests and the wire spells them differently;
// collapsing the first into the second would make a field that cannot be
// un-set from the console.
//
// The echo is the EFFECTIVE row — all 19 options, namespace and task at the top
// level — so the outcome of a save never needs a second read, and the toast can
// state what the queue now is rather than what was asked for.
//
// Pure, and no import that reaches Vue or the alias-resolved shell: the node
// test runner imports this file directly.

import { describeApiError, refusalCode } from '../api/errors.js'

/**
 * The queue's two identity labels. They travel through the same options bag as
 * everything below (the SP reads them with `p_options->>'namespace'`), but they
 * get their own inputs in the form — an Autocomplete over what the cluster
 * already uses — so they are kept out of `OPTIONS` and out of its groups.
 */
export const IDENTITY_OPTIONS = [
  {
    key: 'namespace',
    label: 'Namespace',
    type: 'text',
    default: '',
    help:
      'A grouping label. Its only functional role is the discovery pop ' +
      '(/pop?namespace=…), which matches it for exact equality, and the namespace listings. ' +
      'Changing it moves no messages: the queue simply starts matching a different discovery pop.',
  },
  {
    key: 'task',
    label: 'Task',
    type: 'text',
    default: '',
    help: 'The second discovery dimension, matched the same way and with the same consequences.',
  },
]

/**
 * The 19 options `/configure` echoes, with the default each one lands on when a
 * body carries it as `null` (or omits it on a create, or on `mode: "replace"`).
 * Every default here is the literal in 012_configure.sql's parse section — if
 * the two ever disagree, that file is right and this one is a bug.
 *
 * `group` decides where the field sits: `common` is open when the form opens,
 * `advanced` is behind a disclosure, because nine of these change how and when
 * data is DELETED and a form that presents them at the same weight as the lease
 * time invites an operator to skim past one.
 *
 * `inert: true` marks an option the broker STORES, ECHOES and never READS
 * (decision D2, re-confirmed against the procedures on 2026-09-11: `ttl`,
 * `max_queue_size` and `retry_delay` appear in 011_log_stats and 012_configure
 * and in no push, pop, ack or maintenance path). The editor hides them rather
 * than offering a back-pressure cap and a TTL that do nothing; Queue Detail
 * still shows the stored values, labelled as declared and not enforced, because
 * a value that IS on the row must not vanish from the page that documents it.
 */
export const OPTIONS = [
  // ------------------------------------------------------------------ common
  {
    key: 'leaseTime',
    label: 'Lease time',
    type: 'int',
    unit: 's',
    default: 300,
    group: 'common',
    help:
      'How long a popped batch stays invisible to the rest of its consumer group. A pop that ' +
      'sends its own leaseSeconds overrides it, and leases already issued keep the expiry they ' +
      'were given. Note a queue created by a push leases at the column default of 60 until a ' +
      'configure writes one.',
  },
  {
    key: 'retryLimit',
    label: 'Retry limit',
    type: 'int',
    default: 3,
    group: 'common',
    help:
      'Explicit failed acks a batch may accumulate before the message is dead-lettered or ' +
      'dropped. A retry ack and a lease expiry do not charge it, so this is a budget for ' +
      'reported failures, not for delivery attempts. Existing counters are not reset.',
  },
  {
    key: 'deadLetterQueue',
    label: 'Dead-letter queue',
    type: 'bool',
    default: true,
    group: 'common',
    help:
      'Where a message that exhausts the retry budget goes. The ack path ORs this with ' +
      'dlqAfterMaxRetries: both must be off before an exhausted message is DROPPED instead, ' +
      'which is silent data loss.',
  },
  {
    key: 'dlqAfterMaxRetries',
    label: 'DLQ after max retries',
    type: 'bool',
    default: true,
    group: 'common',
    help: 'The second, compatibility spelling of the flag above. The two are combined with OR.',
  },
  {
    key: 'dedupWindowSeconds',
    label: 'Dedup window',
    type: 'int',
    unit: 's',
    default: 3600,
    group: 'common',
    help:
      'How long a repeated transactionId is recognised as a duplicate on the same partition; ' +
      '0 turns deduplication off. It also bounds how long a late ack can still be resolved, and ' +
      'the broker’s cache costs roughly 16 bytes per in-window message (rate × window).',
  },
  {
    key: 'delayedProcessing',
    label: 'Delayed processing',
    type: 'int',
    unit: 's',
    default: 0,
    group: 'common',
    help:
      'A message becomes deliverable this many seconds after it was committed. Enforced in SQL, ' +
      'so it applies to messages already in the queue the moment it changes: raising it hides ' +
      'messages that were visible a second ago.',
  },
  {
    key: 'priority',
    label: 'Priority',
    type: 'int',
    default: 0,
    group: 'common',
    help:
      'A label, carried as queuePriority on this queue’s message rows and listings. The log ' +
      'engine serves one partition at a time and reads no priority, so it does not reorder ' +
      'delivery between queues.',
  },

  // ---------------------------------------------------------------- advanced
  {
    key: 'windowBuffer',
    label: 'Window buffer',
    type: 'int',
    unit: 's',
    default: 0,
    group: 'advanced',
    help:
      'Quiet-period debounce, per partition: while the partition has been written to inside this ' +
      'window it delivers nothing. With the hotlist on (the default) the broker promotes a held ' +
      'partition early at 100 accumulated messages; with QUEEN_HOTLIST=0 a continuously written ' +
      'partition is never delivered at all.',
  },
  {
    key: 'retentionEnabled',
    label: 'Retention enabled',
    type: 'bool',
    default: false,
    group: 'advanced',
    help:
      'The master switch for the two time-based retention rules below. Without it the ' +
      'maintenance service deletes nothing for this queue, and the queue grows until you delete it.',
  },
  {
    key: 'retentionSeconds',
    label: 'Retention window',
    type: 'int',
    unit: 's',
    default: 0,
    group: 'advanced',
    help:
      'Deletes whole segments older than this, consumed or not — it will delete a backlog nobody ' +
      'read. Needs retentionEnabled and a positive value. Consumers inside a deleted range ' +
      'resume at the next existing offset and silently skip what went.',
  },
  {
    key: 'completedRetentionSeconds',
    label: 'Completed retention',
    type: 'int',
    unit: 's',
    default: 0,
    group: 'advanced',
    help:
      'The same age cut, but never past the slowest consumer group’s cursor, so unconsumed ' +
      'backlog survives it. Needs retentionEnabled and a positive value. A partition with no ' +
      'consumer group has consumed nothing, so this rule frees nothing there.',
  },
  {
    key: 'maxWaitTimeSeconds',
    label: 'Message TTL (age eviction)',
    type: 'int',
    unit: 's',
    default: 0,
    group: 'advanced',
    help:
      'Deletes whole segments older than the cutoff for every consumer group, IN-FLIGHT LEASES ' +
      'INCLUDED, and it ignores retentionEnabled. No dead letter, no notice. Use it only where ' +
      'stale data is worse than lost data.',
  },
  {
    key: 'minPopWaitTime',
    label: 'Batch fill wait',
    type: 'int',
    unit: 'ms',
    default: 0,
    group: 'advanced',
    help:
      'How long a pop may hold an under-full batch so one commit carries more messages. It ' +
      'engages only on a non-empty queue, with wait=true and a batch larger than 1. The broker ' +
      'clamps it to 0–60000 ms rather than rejecting it.',
  },
  {
    key: 'encryptionEnabled',
    label: 'Encryption at rest',
    type: 'bool',
    default: false,
    group: 'advanced',
    help:
      'Stores new payloads as an AES-256-GCM envelope. It needs a valid QUEEN_ENCRYPTION_KEY on ' +
      'the broker: with no key, pushes to a queue flagged here still succeed and store ' +
      'PLAINTEXT, and nothing in the response says so.',
  },
  {
    key: 'retentionSinkHold',
    label: 'Retention sink hold',
    type: 'text',
    default: '',
    group: 'advanced',
    help:
      'Name of the data-lake sink retention waits for: segments are held until that sink reports ' +
      'them committed. Blank is off. Letters, digits, dot, underscore and hyphen only, up to 64 ' +
      'characters — the name is a segment of the KV key the cycle probes.',
  },
  {
    key: 'retentionSinkHoldMaxSeconds',
    label: 'Sink hold ceiling',
    type: 'int',
    unit: 's',
    default: 604800,
    group: 'advanced',
    help:
      'How long retention waits for that sink before deleting anyway. Between 60 seconds and ' +
      '31536000 (one year); outside that the broker refuses the whole call and the queue keeps ' +
      'the configuration it had.',
  },

  // ------------------------------------------------------- stored, never read
  {
    key: 'ttl',
    label: 'TTL',
    type: 'int',
    unit: 's',
    default: 3600,
    group: 'advanced',
    inert: true,
    help: 'Declared, not enforced by this broker: no push, pop, ack or maintenance path reads it.',
  },
  {
    key: 'maxSize',
    label: 'Max queue size',
    type: 'int',
    default: 0,
    group: 'advanced',
    inert: true,
    help:
      'Declared, not enforced by this broker: there is no push back-pressure cap. Storage is ' +
      'bounded by retention and, on the cloud, by the plan’s retained-bytes quota.',
  },
  {
    key: 'retryDelay',
    label: 'Retry delay',
    type: 'int',
    unit: 'ms',
    default: 1000,
    group: 'advanced',
    inert: true,
    help: 'Declared, not enforced by this broker: a retry ack releases the lease immediately.',
  },
]

/** Every option the form may put on the wire, identity labels included. */
export const ALL_OPTIONS = [...IDENTITY_OPTIONS, ...OPTIONS]

/** key -> metadata, for the call sites that have a key and want its rules. */
export const OPTION_META = Object.freeze(
  Object.fromEntries(ALL_OPTIONS.map(o => [o.key, o]))
)

/** key -> the value the broker lands on for `null` (and on create / replace). */
export const OPTION_DEFAULTS = Object.freeze(
  Object.fromEntries(ALL_OPTIONS.map(o => [o.key, o.default]))
)

/** The options the editor offers, in order: everything the broker actually reads. */
export const EDITABLE_OPTIONS = OPTIONS.filter(o => !o.inert)

/** The three the broker stores and never reads — hidden from the editor (D2). */
export const INERT_OPTIONS = OPTIONS.filter(o => o.inert)

/** The editable options of one group, for the two sections of the form. */
export const optionsInGroup = (group) => EDITABLE_OPTIONS.filter(o => o.group === group)

// ---------------------------------------------------------------------------
// Form values <-> wire values
// ---------------------------------------------------------------------------

/**
 * The form's value for one option: a string for `int` and `text` (an input is
 * text, and an empty one has to stay distinguishable from a zero), a boolean
 * for `bool`.
 *
 * An option the source does not carry becomes '' / the default boolean, which
 * the reader below turns back into `null` — "restore the default" — so a broker
 * that answered with fewer keys than this dashboard knows cannot make the form
 * silently claim values it never read.
 */
export function toFormValue(meta, value) {
  if (meta.type === 'bool') return value === undefined || value === null ? !!meta.default : !!value
  if (value === undefined || value === null) return ''
  return String(value)
}

/** Every option of `source` (an echo's `options` + its top-level identity), as form values. */
export function toFormValues(source = {}) {
  const form = {}
  for (const meta of ALL_OPTIONS) form[meta.key] = toFormValue(meta, source[meta.key])
  return form
}

/**
 * One form value as the wire value: `null` for a cleared field ("restore the
 * default"), a number for an int, the trimmed string for text, the boolean for
 * bool.
 *
 * An int that is not a whole number comes back as the raw STRING rather than
 * NaN, so `validate` can name what was typed instead of reporting a field as
 * "not a number" with no way to say which number.
 */
export function readFormValue(meta, raw) {
  if (meta.type === 'bool') return !!raw
  const text = typeof raw === 'string' ? raw.trim() : raw === null || raw === undefined ? '' : String(raw)
  if (text === '') return null
  if (meta.type === 'int') return /^-?\d+$/.test(text) ? Number(text) : text
  return text
}

/** A whole form as wire values, keyed the way `/configure` spells them. */
export function readFormValues(form = {}) {
  const out = {}
  for (const meta of ALL_OPTIONS) {
    if (!(meta.key in form)) continue
    out[meta.key] = readFormValue(meta, form[meta.key])
  }
  return out
}

/** Compare two wire values for one option, tolerating a broker that answers "300" for an int. */
const sameValue = (meta, a, b) => {
  if (meta?.type === 'int') return Number(a) === Number(b)
  if (meta?.type === 'bool') return !!a === !!b
  return String(a ?? '') === String(b ?? '')
}

// ---------------------------------------------------------------------------
// The diff
// ---------------------------------------------------------------------------

/**
 * The keys to send: only those whose EFFECTIVE value differs from what the
 * queue holds now.
 *
 * `current` is the queue as the broker last described it (`GET
 * /api/v1/resources/queues/:queue` -> `options`, plus its top-level namespace
 * and task); `edited` is `readFormValues(form)`, where `null` means the
 * operator cleared the field and wants the default back.
 *
 * Two subtleties this encodes, both of which a naive `!==` gets wrong:
 *
 *   * a cleared field on a queue that is ALREADY at the default is not a
 *     change, so it is not sent — posting `null` there would be a no-op that
 *     still took the queue's row lock;
 *   * a cleared field on a queue that is not at the default is sent as `null`,
 *     never as the default's literal value. `null` says "whatever the default
 *     is", which is the request the operator made, and it stays true the day a
 *     default moves.
 *
 * Keys `edited` does not carry are never in the result: a form that does not
 * offer an option cannot change it, which is exactly what merge semantics buy.
 */
export function configDiff(current = {}, edited = {}) {
  const out = {}
  for (const [key, value] of Object.entries(edited)) {
    const meta = OPTION_META[key]
    const fallback = OPTION_DEFAULTS[key]
    // What the queue holds now. A key the broker did not answer is at its
    // default by definition — it is what the broker itself would have parsed.
    const now = current[key] === undefined || current[key] === null ? fallback : current[key]
    // What the operator asked for, with `null` resolved to the default so the
    // comparison is between two real values.
    const wanted = value === null ? fallback : value
    if (sameValue(meta, wanted, now)) continue
    out[key] = value
  }
  return out
}

// ---------------------------------------------------------------------------
// Validation — the SP's rules, before the round trip
// ---------------------------------------------------------------------------

/** The charset 012_configure.sql enforces on the sink name, verbatim. */
const SINK_HOLD_RE = /^[A-Za-z0-9._-]{0,64}$/
const SINK_HOLD_MAX_MIN = 60
const SINK_HOLD_MAX_MAX = 31536000

/** Postgres `integer`, which is the column type behind every int option here. */
const INT_MAX = 2147483647

/**
 * `{key: message}` for everything the broker would refuse, checked here so the
 * operator is told which field is wrong while it is still on screen.
 *
 * This mirrors the SP; it does not invent rules of its own. The two options
 * 012_configure.sql REJECTS out of range (`retentionSinkHold`,
 * `retentionSinkHoldMaxSeconds` — the two that govern deletion) are checked
 * exactly as it checks them, and the ones it CLAMPS (`minPopWaitTime`,
 * `dedupWindowSeconds`) are deliberately not: a clamp is not a refusal, and
 * pretending otherwise would block a value the broker accepts. The non-negative
 * integer check is this form's own floor, because every integer option here is
 * a duration or a count and a negative one is a typo in every case.
 */
export function validate(edited = {}) {
  const errors = {}
  for (const [key, value] of Object.entries(edited)) {
    const meta = OPTION_META[key]
    // `null` is "restore the default", and a default is by definition valid.
    if (value === null || value === undefined || !meta) continue

    if (meta.type === 'int') {
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        errors[key] = `${meta.label} must be a whole number${meta.unit ? ` of ${meta.unit === 'ms' ? 'milliseconds' : 'seconds'}` : ''} — got “${value}”.`
        continue
      }
      if (value < 0) {
        errors[key] = `${meta.label} cannot be negative. 0 is the value that turns it off.`
        continue
      }
      // Every one of these columns is a Postgres `integer`, and the SP does not
      // bound them: a bigger number reaches the driver, which answers `value
      // "99999999999" is out of range for type integer`. That string would then
      // be rendered to the operator as if it were a verdict about the field, so
      // the form states the limit in its own words first.
      if (value > INT_MAX) {
        errors[key] =
          `${meta.label} cannot be larger than ${INT_MAX}: the broker stores it in a 32-bit ` +
          'integer column and refuses the whole call above that.'
        continue
      }
    }

    if (key === 'retentionSinkHold' && !SINK_HOLD_RE.test(String(value))) {
      errors[key] =
        'The sink name may only contain letters, digits, dot, underscore and hyphen, up to 64 ' +
        'characters — the broker rejects anything else, because the name is a segment of the KV ' +
        'key the retention cycle probes.'
      continue
    }

    if (
      key === 'retentionSinkHoldMaxSeconds' &&
      typeof value === 'number' &&
      (value < SINK_HOLD_MAX_MIN || value > SINK_HOLD_MAX_MAX)
    ) {
      errors[key] =
        `The sink hold ceiling must be between ${SINK_HOLD_MAX_MIN} and ${SINK_HOLD_MAX_MAX} ` +
        'seconds. The broker refuses the whole call outside that range and the queue keeps the ' +
        'configuration it had.'
    }
  }
  return errors
}

/**
 * Why this form refuses to create a queue with no name, when the broker
 * accepts one.
 *
 * `handle_configure` takes `""` on purpose — the JS client's `.queue('')`
 * load test relies on it — but a nameless queue cannot be opened, filtered or
 * deleted from this console (every route puts the name in the path), so
 * creating one here would only produce a row nobody can reach. A name is
 * therefore required on the way IN, and nothing on the way out hides a queue
 * that already exists with that name.
 */
export function queueNameError(name) {
  const text = typeof name === 'string' ? name.trim() : ''
  if (!text) return 'A queue needs a name.'
  if (text !== name) return 'A queue name cannot start or end with a space.'
  return null
}

/**
 * The EXISTING queue a typed create-form name resolves to, or null when the
 * name is new (or unusable, or the form is the edit entry point, where the
 * queue comes from the props and not from typing).
 *
 * A NAME, not a boolean, and that is the whole point. The create form paints
 * itself from whatever queue the typed name names — `/configure` merges, so
 * typing an existing name makes this form that queue's editor — and the repaint
 * has to be driven by THIS value changing. Driven by "does it exist?" instead,
 * the repaint happens only when existence TOGGLES: replacing a selected
 * `orders` with a pasted `payments`, or picking a second suggestion, goes from
 * one existing queue straight to another without ever passing through "no such
 * queue", so the form would keep `orders`' 21 options on screen — and, worse,
 * as the left-hand side of `configDiff` — under the name `payments`. Every
 * option of `orders` that `payments` does not share would then look like an
 * operator edit and be sent to `payments` on save.
 *
 * `known` is anything with `.has` (the queues store's `queueMeta` Map); an
 * absent or malformed one resolves to null, which paints the defaults and is
 * the same thing this form shows before the queue list has loaded.
 */
export function existingQueueName(name, known, { isEdit = false } = {}) {
  if (isEdit) return null
  if (queueNameError(name)) return null
  if (!known || typeof known.has !== 'function') return null
  const text = name.trim()
  return known.has(text) ? text : null
}

// ---------------------------------------------------------------------------
// The request
// ---------------------------------------------------------------------------

/**
 * The `/configure` body.
 *
 * `namespace` and `task` go INSIDE the options bag, not at the top level, and
 * that is load-bearing: `handle_configure` folds a top-level namespace/task
 * into the bag only when it is a NON-EMPTY string, so a top-level `""` would be
 * dropped and "clear the namespace" would silently do nothing. Inside the bag
 * every value reaches the SP, `''` and `null` included.
 *
 * `mode` is sent explicitly when given. Absent means merge to a 1.6.0 broker,
 * and an older one ignores the key entirely and replaces — which is why the
 * modal never relies on a default it cannot see.
 */
export function buildBody({ queue, namespace, task, options = {}, mode } = {}) {
  if (typeof queue !== 'string') throw new TypeError('configure needs a queue name')
  if (mode !== undefined && mode !== 'merge' && mode !== 'replace') {
    // The broker answers 400 for any other value and writes nothing; failing
    // here makes that a bug in this file rather than a round trip.
    throw new TypeError(`mode must be "merge" or "replace", got ${JSON.stringify(mode)}`)
  }

  const bag = { ...options }
  if (namespace !== undefined) bag.namespace = namespace
  if (task !== undefined) bag.task = task

  const body = { queue, options: bag }
  if (mode !== undefined) body.mode = mode
  return body
}

// ---------------------------------------------------------------------------
// Rendering the answer
// ---------------------------------------------------------------------------

/** One option value as a person reads it: '120 s', 'on', 'none'. */
export function formatOptionValue(meta, value) {
  if (!meta) return String(value)
  if (meta.type === 'bool') return value ? 'on' : 'off'
  if (value === null || value === undefined) return '—'
  if (meta.type === 'text') return value === '' ? 'none' : String(value)
  return meta.unit ? `${value} ${meta.unit}` : String(value)
}

/**
 * What the queue IS now, for the success toast, read from the echo rather than
 * from what was sent — the echo is the effective row, and the difference shows
 * up the moment the broker clamps something (minPopWaitTime past 60000).
 *
 * `keys` is what the save asked about, so the toast states those and not all
 * nineteen. namespace and task are read from the top level, where the SP puts
 * them; every other option from `options`.
 */
export function summariseEffective(echo, keys = []) {
  if (!echo) return ''
  const opts = echo.options || {}
  const parts = []
  for (const key of keys) {
    const meta = OPTION_META[key]
    if (!meta) continue
    const value = key in opts ? opts[key] : echo[key]
    if (value === undefined) continue
    parts.push(`${meta.label} ${formatOptionValue(meta, value)}`)
  }
  return parts.join(' · ')
}

/**
 * Why the save was refused, in one sentence.
 *
 * One thing `describeApiError` cannot do here, and this exists for it:
 *
 *   * It flattens every 5xx to "Server error (HTTP 500)". The SP's two
 *     out-of-range refusals (`retentionSinkHold`, `retentionSinkHoldMaxSeconds`)
 *     carry `{"error": "<which option and what is wrong>"}`, and that sentence
 *     is the ONLY place the broker says which option it refused. A 1.6.0 broker
 *     answers them 400 (the envelope names the option in `invalid`, which
 *     handle_configure maps to a bad request); an older one answers the same
 *     body on a 500, because sp_result_to_response mapped only "not found" to a
 *     404. Both are rendered, so the sentence survives the upgrade in either
 *     direction — as do the handler's own 400s (`mode`, a missing queue).
 *
 * The plan refusals are NOT that. `describeApiError` branches on the code
 * before it reaches its role sentence, so a retention window over the plan's
 * ceiling and a tenant at its queue/partition cap already read as plan
 * refusals naming the cap — here, and on the global toast this modal cannot
 * suppress. What is added below is only the tail a configure can promise: that
 * the queue was left exactly as it was.
 *
 * Everything else — a role refusal, a 429 with its Retry-After, an unreachable
 * proxy — falls through, so the wording of those stays identical to the rest of
 * the product.
 */
export function describeConfigureRefusal(err) {
  const envelope = err?.body && typeof err.body === 'object' ? err.body : null
  const said = typeof envelope?.error === 'string' && envelope.error ? envelope.error : null

  switch (refusalCode(err)) {
    case 'quota_exceeded':
      // The shared describer repeats the proxy's own sentence, which names the
      // cap that fired. With no sentence to repeat, name the two caps a
      // CONFIGURE can trip rather than leaving the operator to guess: the
      // monthly message quota and the storage block are push-path verdicts and
      // cannot reach this route.
      return said
        ? `${describeApiError(err)}. Nothing was configured.`
        : 'The plan on this cluster refused it (retention ceiling, or the queue / partition cap). Nothing was configured.'
    case 'cluster_suspended':
      return 'This cluster is suspended, so its queues cannot be configured.'
    default:
      break
  }

  // The broker's own sentence about THIS configuration. Only when it actually
  // carried one: a 500 with no envelope is an outage, not a verdict on a field,
  // and must not be dressed up as one.
  if (said && (err?.status === 400 || err?.status === 500)) {
    return `${said}. The queue keeps the configuration it had.`
  }
  return describeApiError(err)
}
