// Pushing a message from the console — the rules a form around
// `POST /api/v1/push` needs, as pure functions so `app/test/push.test.js` can
// hold them against the wire instead of a browser holding them by eye.
//
// THE HTTP STATUS IS NOT THE OUTCOME. The broker answers 201 with a bare JSON
// ARRAY of per-item results — `{index, message_id, transaction_id, queueName,
// status, offset?}`, mixed casing and all (server/src/rsm/facade/real.rs
// `render_push`) — and only one of its three statuses means the message is on
// the queue:
//
//   queued     stored, and `offset` is its absolute position in the partition
//   duplicate  the transaction id is already inside the queue's dedup window:
//              NOTHING was written and the ids are the pre-existing message's
//   error      the broker refused the write for this item: nothing was written
//
// `buffered` and `failed` were 1.x statuses (its disk spool). The 2.0 broker
// never answers them, so they are not in the table below and render like any
// status this file does not know: never as a success.
//
// A modal that closed on 201 would report a duplicate and an error as success,
// and neither reached the queue. Hence `pushVerdict`: one mapping, in one
// place, that every entry point renders.
//
// There is no per-item error TEXT to render: an item carries its status and
// nothing beside it, so the sentences below are the dashboard's own and the
// operator's next step after an `error` is the Messages list.
//
// Pure, and no import that reaches Vue or the alias-resolved shell: the node
// test runner imports this file directly.

import { describeApiError, refusalCode, serverSentence } from '../api/errors.js'

// Bytes, not characters: every cap downstream of this form — the proxy's
// per-item `max_payload_bytes`, its 16 MiB body cap, the broker's 64 MiB one —
// is counted in bytes, and a payload of emoji is four times its string length.
const encoder = typeof TextEncoder === 'function' ? new TextEncoder() : null
const byteLength = (text) => (encoder ? encoder.encode(text).length : text.length)

/**
 * The point past which this form refuses the payload.
 *
 * It mirrors no server limit and must not pretend to: the per-item cap is a
 * plan limit the dashboard cannot read (it arrives as a 413 naming the cap),
 * and the body caps are 16 and 64 MiB. This is the dashboard's own line, drawn
 * where a textarea stops being a usable way to enter a message: past a
 * megabyte, parsing on the main thread and re-rendering the field costs more
 * than the push it describes, and a payload that size belongs in an SDK call.
 */
export const MAX_PAYLOAD_TEXT_BYTES = 1024 * 1024

// error < warning < success. The verdict of a response is its WORST item, so a
// multi-item answer (which the console never asks for) can never render as a
// success because item 0 happened to be queued.
const KIND_RANK = { error: 0, warning: 1, success: 2 }

const VERDICTS = {
  queued: {
    kind: 'success',
    title: 'Queued',
    detail: 'The broker stored the message. It is on the queue now.',
  },
  duplicate: {
    kind: 'warning',
    title: 'Duplicate — nothing was written',
    detail:
      'A message with this transaction id is already inside the queue’s dedup window, ' +
      'so this push stored nothing. The ids below belong to that existing message, not to a new one.',
  },
  error: {
    kind: 'error',
    title: 'The broker did not store the message',
    detail:
      'The broker refused the write for this message. Nothing was written, ' +
      'so the same push can be sent again.',
  },
}

/**
 * The item a push response should be judged by: the worst one, earliest first.
 *
 * The console pushes a single item, so this is item 0 in every normal answer.
 * It is a function rather than `results[0]` because the day a response carries
 * more than one result is exactly the day reading only the first one lies.
 *
 * Returns `null` when there is nothing to judge — which `pushVerdict` renders
 * as a failure, never as a quiet success.
 */
export function worstResult(results) {
  if (!Array.isArray(results) || results.length === 0) return null
  let worst = null
  let worstRank = Infinity
  for (const item of results) {
    const verdict = VERDICTS[item?.status]
    // An unknown status is not a success, so it sorts with the errors.
    const rank = verdict ? KIND_RANK[verdict.kind] : KIND_RANK.error
    if (rank < worstRank) {
      worst = item
      worstRank = rank
    }
  }
  return worst
}

/**
 * What to render for a push response: `{kind, title, detail}`, where `kind` is
 * 'success' | 'warning' | 'error' and only 'success' means the message reached
 * the queue.
 *
 * Takes the whole array (the 2xx body) — never a single item — because "how
 * many results came back" is itself part of the verdict.
 */
export function pushVerdict(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return {
      kind: 'error',
      title: 'The broker answered with no result',
      detail:
        'A push answers one result per item, and this response carried none, so nothing can be ' +
        'said about the message. Check the Messages list before pushing again.',
    }
  }

  const item = worstResult(results)
  const known = VERDICTS[item?.status]
  const verdict = known
    ? { ...known }
    : {
        kind: 'error',
        title: `Unrecognised status “${item?.status ?? 'none'}”`,
        detail:
          'This broker reported a per-item status this dashboard does not know, so the push cannot ' +
          'be confirmed either way. Check the Messages list before pushing again.',
      }

  // The console sends one item. More results than that is a broker or a proxy
  // doing something the form did not ask for, and the verdict says which item
  // it is describing rather than quietly summarising the batch.
  if (results.length > 1) {
    const index = Number.isInteger(item?.index) ? item.index : results.indexOf(item)
    verdict.detail =
      `${verdict.detail} The dashboard pushes one message at a time, but this response carried ` +
      `${results.length} results; this verdict is item ${index}.`
  }

  return verdict
}

/**
 * The statuses where the broker took responsibility for the message, and so
 * the only ones whose ids name anything at all.
 *
 * Exported rather than repeated in the modal because the rule is the point: a
 * message id printed beside “the message is lost” names a message that does
 * not exist, which is the class of lie this form exists to remove.
 */
export const ACCEPTED_STATUSES = ['queued', 'duplicate']

/** Does this status carry ids worth rendering? */
export function showsIds(status) {
  return ACCEPTED_STATUSES.includes(status)
}

/**
 * The offset to render for a result, or `null` when the broker allocated none.
 *
 * `render_push` OMITS the key rather than sending null whenever no offset
 * exists (an `error` item, and every broker older than the field), so the
 * absent case must not fall through to `0` — position 0 is the head of the
 * partition, which is a claim about where the message landed.
 */
export function offsetLine(item) {
  const off = item?.offset
  return off === null || off === undefined ? null : off
}

/**
 * Is this stored payload an encryption envelope the broker could not open?
 *
 * `/messages/:pid/:txid` decrypts `{encrypted,iv,authTag}` before answering,
 * but only when a key is configured AND it is the right one
 * (server/src/encryption.rs `decrypt_payload_bytes` returns None otherwise);
 * with no key, the raw envelope is what the drawer receives as `payload`.
 * Copying that would push the envelope as a plaintext payload — on an
 * encrypted queue, an envelope that decrypts to an envelope.
 *
 * Exactly the three fields the broker writes (encryption.rs:84), all strings:
 * a payload of the operator's own that merely carries an `encrypted` key is
 * not one of these.
 */
export function isEncryptedEnvelope(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const keys = Object.keys(value)
  return (
    keys.length === 3 &&
    typeof value.encrypted === 'string' &&
    typeof value.iv === 'string' &&
    typeof value.authTag === 'string'
  )
}

/**
 * Read the payload textarea. `{ok: true, value}` — where `value` is any JSON
 * value, because that is what the broker's `payload` field is — or
 * `{ok: false, message}` with a sentence that says what to do about it.
 *
 * A bare string, number, boolean or `null` is a legal payload, so the field is
 * NOT "a JSON object": `"hello"`, `42`, `true` and `null` all parse here and
 * all push. What does not parse is unquoted text, which is the mistake this
 * message exists to name.
 */
export function parsePayload(text) {
  const raw = typeof text === 'string' ? text : ''
  const trimmed = raw.trim()
  if (!trimmed) {
    return {
      ok: false,
      message:
        'The payload is empty. The broker stores any JSON value, so enter one: an object, an array, ' +
        'a quoted "string", a number, true / false, or null.',
    }
  }

  const bytes = byteLength(raw)
  if (bytes > MAX_PAYLOAD_TEXT_BYTES) {
    return {
      ok: false,
      message:
        `This payload is ${Math.round(bytes / 1024)} KB and the form stops at ` +
        `${MAX_PAYLOAD_TEXT_BYTES / 1024} KB. That is the dashboard’s own limit, not the ` +
        'broker’s: past a megabyte the browser spends longer parsing and redrawing the field ' +
        'than the push itself takes. Send a payload this size with an SDK.',
    }
  }

  try {
    return { ok: true, value: JSON.parse(trimmed) }
  } catch (e) {
    return {
      ok: false,
      message:
        `Not valid JSON: ${e.message}. Text must be quoted ("order-17", not order-17), ` +
        'keys need double quotes, and trailing commas are not JSON.',
    }
  }
}

/**
 * A stored payload as the text to edit and push back.
 *
 * Deliberately NOT the drawer's display formatter (`formatPayload` in
 * Messages.vue), which parses a payload that is itself a JSON string so a
 * double-encoded body reads nicely. That is right for reading and wrong for
 * copying: re-parsing would turn a message whose payload is the STRING
 * `"{\"a\":1}"` into one whose payload is the OBJECT `{a:1}` — a copy that
 * carries something the original never did. Round-tripping through
 * `parsePayload` here returns exactly the JSON VALUE that came off the wire.
 *
 * The value, not the bytes: the transport already parsed the response
 * (httpClient `responseData`), so a number the IEEE-754 double cannot hold has
 * been rewritten before this function ever sees it — `9007199254740993` came
 * back as `…92`, `1e400` as `null`. Nothing here can undo that, and no control
 * added later (a re-key, a move) may assume otherwise.
 */
export function payloadToText(value) {
  if (value === undefined) return ''
  const text = JSON.stringify(value, null, 2)
  return text === undefined ? '' : text
}

/**
 * The Messages filters, corrected so the refreshed list CAN contain the message
 * that was just pushed.
 *
 * A refresh that answers a successful push with a table unable to show the row
 * reads as a push that did nothing, and four filters can do that:
 *
 *   to         pinned once at page load (`setTimeRange`), and the list SP
 *              bounds its scan at `date_trunc('minute', to) + 1 minute`
 *              (010_log_admin.sql) — a page open for two minutes, about how
 *              long it takes to type a payload, asks for a window that ENDS
 *              before the message exists. Moved forward; the operator's lower
 *              bound and their span's start stay as chosen.
 *   status     a fresh message is `pending`, so any other value excludes it.
 *   queue      the push may have gone somewhere else entirely.
 *   partition  likewise, and a blank partition field in the form means
 *              `Default`, which a partition filter rarely matches.
 *
 * `current` and the return value are the four filters as the page holds them;
 * `pushed` is the modal's `pushed` payload; `now` is the datetime-local string
 * for the moment of the push (the page's own `formatDateTimeLocal`). Only the
 * filters that would hide the row are changed, so a page that was already
 * showing everything is left alone.
 */
export function filtersForPushedMessage(current, pushed, now) {
  const next = { ...current }
  // An empty `to` is an open-ended window and already covers the push.
  if (current?.to) next.to = now
  if (current?.status && current.status !== 'pending') next.status = ''
  if (pushed?.queue && current?.queue && current.queue !== pushed.queue) {
    next.queue = pushed.queue
  }
  if (pushed?.partition && current?.partition && current.partition !== pushed.partition) {
    next.partition = pushed.partition
  }
  return next
}

/**
 * Why the call was refused, in the one sentence the PUSH PATH would add.
 *
 * `describeApiError` words the plan refusals itself — it branches on the code
 * before it reaches its role sentence, so `quota_exceeded` (which names the cap
 * that fired), `feature_gated` and every other 403 that is about the cluster
 * rather than the principal already read correctly everywhere in the product,
 * including on the global toast this modal cannot suppress. This function used
 * to carry that whole table; what is left is the three refusals where the push
 * path knows something the shared describer cannot say — that NOTHING WAS
 * WRITTEN, and what would have to change for the next attempt to land — plus
 * the 413, which is minted only for a body a push sent.
 *
 * Everything else falls through, so the wording of a role refusal, a 429 with
 * its Retry-After or an unreachable proxy stays identical to the rest of the
 * product.
 */
export function describePushRefusal(err) {
  switch (refusalCode(err)) {
    case 'storage_quota_exceeded':
      return 'Storage quota exceeded on this cluster — pushes stay blocked until retained bytes fall back under the plan’s cap.'
    case 'push_blocked':
      return 'Pushes are blocked on this cluster (billing hold). Nothing was written.'
    case 'cluster_suspended':
      return 'This cluster is suspended, so nothing can be pushed to it.'
    case 'payload_too_large':
      // The proxy names the cap and the overage ("item 0: payload N bytes
      // exceeds max_payload_bytes (M)"), which is more useful than anything
      // this function could say about a limit it cannot read.
      return serverSentence(err) || 'The payload exceeds the size this cluster accepts.'
    default:
      return describeApiError(err)
  }
}
