// Creating and editing a queue from the console (PLAN_DASHBOARD_ACTIONS.md §2.2).
//
// The rules worth a test here are all about not resetting a queue the operator
// only meant to nudge: `configDiff`, which decides what is allowed on the wire
// at all; `validate`, which mirrors the two refusals 012_configure.sql makes so
// a bad sink name is caught before the round trip; `buildBody`, which decides
// where namespace and task go and what `mode` says; and the option catalogue
// itself, whose defaults have to be the SP's defaults or every "restore the
// default" in the editor is a guess.
//
// The wire this file pins:
//   server/sql/procedures/012_configure.sql  parse section, echo, the two rejects
//   server/src/handlers/queues.rs            handle_configure: the options bag,
//                                            the non-empty fold of namespace/task,
//                                            `mode` -> 400
//   server/sql/procedures/011_log_stats.sql  get_queue_v2's 21-key `options`

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { ApiError } from '../src/api/errors.js'
import {
  ALL_OPTIONS,
  EDITABLE_OPTIONS,
  INERT_OPTIONS,
  OPTION_DEFAULTS,
  buildBody,
  configDiff,
  describeConfigureRefusal,
  existingQueueName,
  formatOptionValue,
  optionsInGroup,
  queueNameError,
  readFormValues,
  summariseEffective,
  toFormValues,
  validate,
} from '../src/composables/useQueueConfig.js'

/** A queue as `get_queue_v2` describes it: 19 options plus the two labels. */
const current = {
  namespace: 'orders',
  task: 'shipped',
  priority: 0,
  leaseTime: 300,
  retryLimit: 3,
  retryDelay: 1000,
  maxSize: 0,
  ttl: 3600,
  deadLetterQueue: true,
  dlqAfterMaxRetries: true,
  delayedProcessing: 0,
  windowBuffer: 0,
  retentionSeconds: 0,
  completedRetentionSeconds: 0,
  retentionEnabled: false,
  encryptionEnabled: false,
  maxWaitTimeSeconds: 0,
  minPopWaitTime: 0,
  dedupWindowSeconds: 3600,
  retentionSinkHold: '',
  retentionSinkHoldMaxSeconds: 604800,
}

// ---------------------------------------------------------------------------
// The catalogue
// ---------------------------------------------------------------------------

test('the catalogue carries every option /configure accepts, and its default', () => {
  // The 21 keys of the echo, from the contract: 19 in `options` plus the two
  // identity labels the SP puts at the top level.
  const expected = [
    'namespace', 'task', 'priority', 'leaseTime', 'retryLimit', 'retryDelay', 'maxSize', 'ttl',
    'deadLetterQueue', 'dlqAfterMaxRetries', 'delayedProcessing', 'windowBuffer',
    'retentionSeconds', 'completedRetentionSeconds', 'retentionEnabled', 'encryptionEnabled',
    'maxWaitTimeSeconds', 'minPopWaitTime', 'dedupWindowSeconds', 'retentionSinkHold',
    'retentionSinkHoldMaxSeconds',
  ]
  assert.deepEqual(ALL_OPTIONS.map(o => o.key).sort(), [...expected].sort())

  // The defaults, verbatim from the parse section of 012_configure.sql. A drift
  // here makes every cleared field in the editor promise the wrong value.
  assert.deepEqual({ ...OPTION_DEFAULTS }, {
    namespace: '', task: '', priority: 0, leaseTime: 300, retryLimit: 3, retryDelay: 1000,
    maxSize: 0, ttl: 3600, deadLetterQueue: true, dlqAfterMaxRetries: true, delayedProcessing: 0,
    windowBuffer: 0, retentionSeconds: 0, completedRetentionSeconds: 0, retentionEnabled: false,
    encryptionEnabled: false, maxWaitTimeSeconds: 0, minPopWaitTime: 0, dedupWindowSeconds: 3600,
    retentionSinkHold: '', retentionSinkHoldMaxSeconds: 604800,
  })
})

test('the three knobs the broker never reads are hidden from the editor (D2)', () => {
  assert.deepEqual(INERT_OPTIONS.map(o => o.key).sort(), ['maxSize', 'retryDelay', 'ttl'])
  for (const key of ['ttl', 'maxSize', 'retryDelay']) {
    assert.equal(EDITABLE_OPTIONS.some(o => o.key === key), false, `${key} must not be editable`)
    assert.equal(optionsInGroup('common').some(o => o.key === key), false)
    assert.equal(optionsInGroup('advanced').some(o => o.key === key), false)
  }
  // …and they are still described, because Queue Detail renders them read-only.
  for (const meta of INERT_OPTIONS) assert.match(meta.help, /not enforced by this broker/)
})

test('every editable option sits in exactly one rendered group', () => {
  const grouped = [...optionsInGroup('common'), ...optionsInGroup('advanced')]
  assert.deepEqual(grouped.map(o => o.key).sort(), EDITABLE_OPTIONS.map(o => o.key).sort())
})

test('the option list matches the keys 012_configure.sql parses', (t) => {
  // Held against the SQL itself: an option added to the SP that nobody adds
  // here is an option the editor silently cannot set. Skipped rather than
  // failed when the file is not reachable (app/ built outside the monorepo),
  // because this assertion is about the pair, not about this package.
  let sql
  try {
    sql = readFileSync(new URL('../../server/sql/procedures/012_configure.sql', import.meta.url), 'utf8')
  } catch {
    t.skip('server/sql/procedures/012_configure.sql not reachable from here')
    return
  }
  const parsed = new Set([...sql.matchAll(/p_options \? '([A-Za-z]+)'/g)].map(m => m[1]))
  // `replace` is a directive, not an option: the handler inserts it and the SP
  // reads it with `->>`, never with `?`, so it never shows up in this set.
  assert.equal(parsed.has('replace'), false)
  assert.deepEqual([...parsed].sort(), ALL_OPTIONS.map(o => o.key).sort())
})

// ---------------------------------------------------------------------------
// Form values
// ---------------------------------------------------------------------------

test('an echo round-trips through the form without proposing a single change', () => {
  const form = toFormValues(current)
  assert.equal(form.leaseTime, '300')
  assert.equal(form.deadLetterQueue, true)
  assert.equal(form.retentionSinkHold, '')
  assert.deepEqual(configDiff(current, readFormValues(form)), {})
})

test('a key the broker did not answer reads as blank, not as an invented value', () => {
  // An older broker, or one that gained an option this dashboard does not know:
  // the field shows empty ("the default"), and an untouched empty field is not
  // a change, so nothing is sent for it.
  const partial = { leaseTime: 60 }
  const form = toFormValues(partial)
  assert.equal(form.dedupWindowSeconds, '')
  assert.deepEqual(configDiff(partial, readFormValues(form)), {})
})

// ---------------------------------------------------------------------------
// The diff
// ---------------------------------------------------------------------------

test('only the keys that changed are sent', () => {
  const edited = readFormValues({ ...toFormValues(current), leaseTime: '120', retryLimit: '5' })
  assert.deepEqual(configDiff(current, edited), { leaseTime: 120, retryLimit: 5 })
})

test('a cleared field is sent as null — restore the default', () => {
  const queue = { ...current, leaseTime: 120, dedupWindowSeconds: 300 }
  const edited = readFormValues({ ...toFormValues(queue), leaseTime: '', dedupWindowSeconds: '' })
  assert.deepEqual(configDiff(queue, edited), { leaseTime: null, dedupWindowSeconds: null })
})

test('clearing a field that is already at its default sends nothing', () => {
  // `null` there would be a no-op that still took the queue's row lock.
  const edited = readFormValues({ ...toFormValues(current), leaseTime: '', priority: '' })
  assert.deepEqual(configDiff(current, edited), {})
})

test('booleans and text are diffed, both directions, and "" is a real value', () => {
  assert.deepEqual(
    configDiff(current, readFormValues({ ...toFormValues(current), deadLetterQueue: false })),
    { deadLetterQueue: false },
  )
  const held = { ...current, retentionSinkHold: 'lake' }
  assert.deepEqual(
    configDiff(held, readFormValues({ ...toFormValues(held), retentionSinkHold: '' })),
    // Cleared text on a queue that HAD a sink: null, i.e. back to '' = off.
    { retentionSinkHold: null },
  )
  // Clearing the namespace of a queue that has one is a change: null, i.e. back
  // to the default ''. On a queue whose namespace is already '' it is not.
  assert.deepEqual(
    configDiff(current, readFormValues({ ...toFormValues(current), namespace: '' })),
    { namespace: null },
  )
  assert.deepEqual(
    configDiff({ ...current, namespace: '' }, readFormValues({ namespace: '' })),
    {},
  )
})

test('a broker that answers an int as a string still diffs as a number', () => {
  assert.deepEqual(configDiff({ ...current, leaseTime: '300' }, { leaseTime: 300 }), {})
})

test('a key the form does not offer can never be changed', () => {
  // The whole point of merge: the editor shows sixteen options and the other
  // five stay exactly as they are, because they are not in `edited`.
  const edited = readFormValues({ leaseTime: '120' })
  assert.deepEqual(Object.keys(configDiff(current, edited)), ['leaseTime'])
})

// ---------------------------------------------------------------------------
// Validation — the SP's rules
// ---------------------------------------------------------------------------

test('the sink name is held to the SP\'s charset and length', () => {
  assert.deepEqual(validate({ retentionSinkHold: 'lake-01.eu_west' }), {})
  assert.deepEqual(validate({ retentionSinkHold: '' }), {})
  assert.deepEqual(validate({ retentionSinkHold: 'a'.repeat(64) }), {})
  // A ':' composes the same KV key as a different (sink, queue) pair — the one
  // character the SP's regex exists to keep out.
  assert.ok(validate({ retentionSinkHold: 'lake:prod' }).retentionSinkHold)
  assert.ok(validate({ retentionSinkHold: 'a'.repeat(65) }).retentionSinkHold)
})

test('the sink hold ceiling is held to 60..31536000', () => {
  assert.deepEqual(validate({ retentionSinkHoldMaxSeconds: 60 }), {})
  assert.deepEqual(validate({ retentionSinkHoldMaxSeconds: 31536000 }), {})
  assert.ok(validate({ retentionSinkHoldMaxSeconds: 59 }).retentionSinkHoldMaxSeconds)
  assert.ok(validate({ retentionSinkHoldMaxSeconds: 31536001 }).retentionSinkHoldMaxSeconds)
  // 0 is out of range, and the message has to say so rather than reading as
  // "off" — the SP refuses the whole call.
  assert.ok(validate({ retentionSinkHoldMaxSeconds: 0 }).retentionSinkHoldMaxSeconds)
})

test('integers must be whole and non-negative, and the message names what was typed', () => {
  assert.deepEqual(validate({ leaseTime: 0, retryLimit: 12 }), {})
  assert.match(validate({ leaseTime: '3 minutes' }).leaseTime, /3 minutes/)
  assert.match(validate({ leaseTime: 1.5 }).leaseTime, /whole number/)
  assert.match(validate({ retryLimit: -1 }).retryLimit, /cannot be negative/)
})

test('a cleared field is never invalid — the default is valid by definition', () => {
  assert.deepEqual(validate({ retentionSinkHold: null, retentionSinkHoldMaxSeconds: null }), {})
})

test('the two options the SP clamps are not refused here', () => {
  // minPopWaitTime past 60000 and any dedup window are CLAMPED by the SP, not
  // rejected; refusing them in the form would block a value the broker accepts.
  assert.deepEqual(validate({ minPopWaitTime: 120000, dedupWindowSeconds: 999999 }), {})
})

test('a queue needs a name here even though the broker accepts ""', () => {
  assert.equal(queueNameError('orders.created'), null)
  assert.ok(queueNameError(''))
  assert.ok(queueNameError('   '))
  assert.ok(queueNameError('orders '))
})

test('the create form resolves a typed name to a QUEUE, not to a yes/no', () => {
  // The queues store's `queueMeta`, which is what the modal passes.
  const known = new Map([['orders', {}], ['payments', {}]])

  assert.equal(existingQueueName('orders', known), 'orders')
  assert.equal(existingQueueName('payments', known), 'payments')
  assert.equal(existingQueueName('orders.created', known), null)

  // THE POINT. One existing queue replaced by another — pasted, or a second
  // suggestion picked — never passes through "no such queue": a boolean is
  // `true` on both sides and the repaint that reads it would never run, leaving
  // `orders`' 21 options on screen, and in `configDiff`'s left-hand side, under
  // the name `payments`. The value the form watches has to CHANGE here.
  const before = existingQueueName('orders', known)
  const after = existingQueueName('payments', known)
  assert.equal(before === null, after === null, 'both name existing queues, so existence does not toggle')
  assert.notEqual(before, after, 'and yet they are different queues, which is what the form must see')

  // An unusable name is not a queue, whatever the map holds: the form refuses
  // it before it can name anything.
  assert.equal(existingQueueName('orders ', new Map([['orders ', {}]])), null)
  assert.equal(existingQueueName('', known), null)
  assert.equal(existingQueueName('   ', known), null)
  assert.equal(existingQueueName(null, known), null)

  // The edit entry point takes its queue from the props; a typed name is not
  // even on screen, so it can never repaint the form from something else.
  assert.equal(existingQueueName('orders', known, { isEdit: true }), null)

  // Before the queue list has loaded there is nothing to resolve against, and
  // "no such queue" is exactly what the form shows then: the defaults.
  assert.equal(existingQueueName('orders', new Map()), null)
  assert.equal(existingQueueName('orders', undefined), null)
  assert.equal(existingQueueName('orders', { has: 'not a function' }), null)

  // A Set works too — the helper asks for `.has`, not for the store's shape.
  assert.equal(existingQueueName('orders', new Set(['orders'])), 'orders')
})

// ---------------------------------------------------------------------------
// The body
// ---------------------------------------------------------------------------

test('namespace and task travel INSIDE the options bag', () => {
  // handle_configure folds a top-level namespace/task into the bag only when it
  // is a non-empty string, so a top-level '' would be dropped and "clear the
  // namespace" would silently do nothing.
  assert.deepEqual(
    buildBody({ queue: 'orders', namespace: '', task: 'shipped', options: { leaseTime: 120 }, mode: 'merge' }),
    { queue: 'orders', options: { leaseTime: 120, namespace: '', task: 'shipped' }, mode: 'merge' },
  )
})

test('an absent namespace/task is not sent at all', () => {
  const body = buildBody({ queue: 'orders', options: { leaseTime: 120 }, mode: 'merge' })
  assert.deepEqual(body.options, { leaseTime: 120 })
})

test('mode is sent only when asked for, and only in the two spellings', () => {
  assert.equal('mode' in buildBody({ queue: 'q', options: {} }), false)
  assert.equal(buildBody({ queue: 'q', options: {}, mode: 'replace' }).mode, 'replace')
  // Anything else is a 400 at the broker that writes nothing; fail here instead.
  assert.throws(() => buildBody({ queue: 'q', options: {}, mode: 'patch' }), /merge.*replace/)
  assert.throws(() => buildBody({ options: {} }), /queue name/)
})

test('an empty diff still produces a legal body', () => {
  // The modal refuses to submit one, but nothing in the builder may invent a
  // key to fill the gap: an empty bag merges nothing.
  assert.deepEqual(buildBody({ queue: 'orders', options: {}, mode: 'merge' }),
    { queue: 'orders', options: {}, mode: 'merge' })
})

// ---------------------------------------------------------------------------
// Rendering the answer
// ---------------------------------------------------------------------------

test('the toast states the EFFECTIVE values from the echo, not what was sent', () => {
  const echo = {
    configured: true, queue: 'orders', namespace: 'orders', task: '',
    options: { ...current, leaseTime: 120, minPopWaitTime: 60000 },
  }
  // 120000 was asked for; the SP clamped it to 60000 and the echo says so.
  assert.equal(
    summariseEffective(echo, ['leaseTime', 'minPopWaitTime', 'namespace']),
    'Lease time 120 s · Batch fill wait 60000 ms · Namespace orders',
  )
  assert.equal(summariseEffective(echo, []), '')
  assert.equal(summariseEffective(null, ['leaseTime']), '')
})

test('values render the way they read', () => {
  const meta = (key) => ALL_OPTIONS.find(o => o.key === key)
  assert.equal(formatOptionValue(meta('leaseTime'), 300), '300 s')
  assert.equal(formatOptionValue(meta('minPopWaitTime'), 0), '0 ms')
  assert.equal(formatOptionValue(meta('deadLetterQueue'), false), 'off')
  assert.equal(formatOptionValue(meta('retentionSinkHold'), ''), 'none')
  assert.equal(formatOptionValue(meta('priority'), 7), '7')
})

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

const apiError = (message, opts) => new ApiError(message, opts)

test('the SP\'s sentence survives whichever status it arrives on', () => {
  // 1.6.0 answers an option refusal 400 (the envelope names the option in
  // `invalid`), an older broker answers the same body 500. The operator reads
  // the same sentence either way.
  const refused = apiError('retentionSinkHold must match [A-Za-z0-9._-]{0,64}, got \'a:b\'', {
    status: 400,
    body: { error: 'retentionSinkHold must match [A-Za-z0-9._-]{0,64}, got \'a:b\'', invalid: 'retentionSinkHold' },
  })
  assert.match(describeConfigureRefusal(refused), /retentionSinkHold must match/)
  assert.match(describeConfigureRefusal(refused), /keeps the configuration it had/)
})

test('the same refusal from a pre-1.6.0 broker, on its 500', () => {
  const err = apiError('retentionSinkHoldMaxSeconds must be between 60 and 31536000, got 10', {
    status: 500,
    body: { error: 'retentionSinkHoldMaxSeconds must be between 60 and 31536000, got 10' },
  })
  const said = describeConfigureRefusal(err)
  assert.match(said, /between 60 and 31536000/)
  assert.match(said, /keeps the configuration it had/)
})

test('the handler\'s 400 about mode is rendered verbatim', () => {
  const err = apiError('mode must be "merge" or "replace", got "patch"', {
    status: 400, body: { error: 'mode must be "merge" or "replace", got "patch"' },
  })
  assert.match(describeConfigureRefusal(err), /must be "merge" or "replace"/)
})

test('a 500 with no envelope is an outage, not a verdict on a field', () => {
  assert.equal(describeConfigureRefusal(apiError('Internal Server Error', { status: 500 })),
    'Server error (HTTP 500)')
})

test('the proxy\'s plan refusals name the cap instead of blaming the role', () => {
  const ceiling = apiError("retentionSeconds of 100000s exceeds the plan's max_retention_seconds (86400s)", {
    status: 403,
    code: 'quota_exceeded',
    body: { error: "retentionSeconds of 100000s exceeds the plan's max_retention_seconds (86400s)", code: 'quota_exceeded' },
  })
  assert.match(describeConfigureRefusal(ceiling), /max_retention_seconds/)
  assert.doesNotMatch(describeConfigureRefusal(ceiling), /role/)

  const cap = apiError('queue limit reached (50)', {
    status: 403, code: 'quota_exceeded', body: { error: 'queue limit reached (50)', code: 'quota_exceeded' },
  })
  assert.match(describeConfigureRefusal(cap), /queue limit reached \(50\)/)
})

test('a role refusal, a rate limit and an unreachable proxy keep the shared wording', () => {
  assert.equal(
    describeConfigureRefusal(apiError('forbidden', { status: 403, code: 'forbidden' })),
    'Not permitted for your role on this cluster',
  )
  assert.equal(
    describeConfigureRefusal(apiError('slow down', { status: 429, retryAfter: 30 })),
    'Rate limited — retry in 30s',
  )
  assert.equal(
    describeConfigureRefusal(apiError('', { status: 0 })),
    'Cannot reach the API — the proxy or the broker is unreachable',
  )
})

// ---------------------------------------------------------------------------
// The two rules the modal composes out of these functions
// ---------------------------------------------------------------------------

test('an int larger than a Postgres integer is refused here, not by the driver', () => {
  // Every one of these columns is `integer` and the SP bounds none of them, so
  // 99999999999 reaches the driver and comes back as `value "99999999999" is
  // out of range for type integer` — which the modal would then render to the
  // operator as if it were a verdict about the field.
  assert.deepEqual(validate({ leaseTime: 2147483647 }), {})
  assert.match(validate({ leaseTime: 2147483648 }).leaseTime, /2147483647/)
  assert.match(validate({ dedupWindowSeconds: 99999999999 }).dedupWindowSeconds, /32-bit/)
})

test('what is validated is the DIFF, so an option the form never shows cannot block a save', () => {
  // `maxSize` is inert (D2): the editor does not render it, and no message
  // about it has a DOM node to appear in. A queue can still hold a negative one
  // — the SP bounds nothing, and the JS SDK or raw HTTP can write it — and
  // validating the whole form would then disable "Save changes" for good, with
  // the reason invisible.
  const queue = { ...current, maxSize: -1 }
  const edited = readFormValues({ ...toFormValues(queue), leaseTime: '120' })

  assert.ok(validate(edited).maxSize, 'the whole form does carry the bad stored value')
  const diff = configDiff(queue, edited)
  assert.deepEqual(diff, { leaseTime: 120 })
  assert.deepEqual(validate(diff), {}, 'nothing the operator can fix, nothing blocking the save')

  // ...and a value the operator really did type badly is in the diff by
  // construction, so its message is still rendered.
  const bad = readFormValues({ ...toFormValues(queue), leaseTime: 'an hour' })
  assert.match(validate(configDiff(queue, bad)).leaseTime, /an hour/)
})

test('a create on a name that already exists must diff against THAT queue', () => {
  // The create form paints from the defaults, so `current` is `{}` until the
  // typed name is read back from the broker. Diffed against `{}`, a value the
  // operator explicitly typed that happens to equal the default is dropped —
  // and the queue keeps its own, which the form never showed.
  const existing = { ...current, leaseTime: 60, deadLetterQueue: false }
  const typed = readFormValues({ ...toFormValues({}), leaseTime: '300' })

  assert.deepEqual(configDiff({}, typed), {}, 'against the defaults: nothing is sent')
  assert.deepEqual(configDiff(existing, typed), {
    // Against the queue: the lease the operator typed IS a change...
    leaseTime: 300,
    // ...and so is every field the default-painted form displays but the queue
    // does not hold — the dead-letter checkbox it shows as on, and the two
    // discovery labels it shows as blank, which would be CLEARED on that queue.
    // A form painted from the defaults over an existing queue is not a safe
    // form, which is why the modal repaints from the queue instead.
    deadLetterQueue: true,
    namespace: null,
    task: null,
  })
})
