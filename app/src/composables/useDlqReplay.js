// Replaying ONE dead-letter row from the console (PLAN_DASHBOARD_ACTIONS.md §2.3).
//
// The dashboard dropped its replay button in 8d357fa4 because the old retry
// route could not verify its own outcome: it minted a fresh transaction id, it
// pushed and deleted in two statements outside any transaction, and it resolved
// an address that can name one row PER CONSUMER GROUP to a single snapshot. The
// button comes back on the move primitive — lock, push, delete, one transaction,
// transaction id `dlq:<row id>` — so every answer below is a fact about the
// dead-letter row rather than a guess.
//
// WHAT THE BROKER ANSWERS (server/src/handlers/messages.rs, `move_response`):
//
//   200 {success:true, result:'moved'|'duplicate', queue, partition,
//        consumerGroup, dlqId, originalTransactionId,
//        replayedAs:{index, message_id, transaction_id, queueName, status, offset},
//        dlqRowRemoved}                          true for `moved`, false for
//                                                `duplicate` — the SP deletes
//                                                the row only when it moved it
//   400 {success:false, error, message}          only the id-addressed route.
//                                                The broker raises it for a
//                                                malformed body or a blank
//                                                override name; the proxy in
//                                                front raises its own, with the
//                                                same wording, for a
//                                                destination named in HALF —
//                                                which `replayRequest` below
//                                                exists never to send
//   404 {success:false, result:'gone', dlqId, error:'Message not found', message}
//   500 {success:false, error, dlqRowRemoved:false|null, message}
//                                                false = the database refused
//                                                and rolled back; null = the
//                                                broker never learned the
//                                                outcome
//   503 {success:false, result:'maintenance', error, dlqRowRemoved:false, message}
//
// TWO OF THOSE ARE NOT FAILURES, and telling them apart is the whole reason
// this module exists:
//
//   `duplicate` is HTTP 200 and NOTHING happened: no frame was written, and the
//   dead-letter row was not removed either. The destination already holds a
//   message under this replay's transaction id — which is `dlq:<row id>`, a
//   string anyone reading the list can derive — so the broker cannot tell that
//   copy apart from a decoy somebody pushed, and a record it did not replay is
//   a record it must not destroy. The row stays on screen, and the operator is
//   pointed at the offset to look at.
//
//   `gone` is HTTP 404 and means the row was already replayed or purged — by a
//   second click, by another operator, by the sweeper, or by a concurrent
//   caller that won the row lock. It is a stale list, not an error: the right
//   rendering is a quiet warning and a refresh, never a red toast.
//
// And one 404 that is neither: a broker older than the route, or a proxy that
// does not classify it, answers 404 too (or `route_blocked`, or the SPA
// fallback that api/httpClient.js turns into `not_an_api_response`). That one
// says nothing about the row, so it must not remove it from the list. The
// discriminator is the BODY — `result:'gone'` or the route's not-found error —
// which is why nothing here branches on the status code alone.
//
// Pure, and no import that reaches Vue or the alias-resolved shell: the node
// test runner imports this file directly (test/dlq-replay.test.js).

import { describeApiError, isMissingRouteError } from '../api/errors.js'
import { describePushRefusal } from './usePushVerdict.js'

/** Trimmed text, or '' for anything that is not usable text. */
const text = (value) => (typeof value === 'string' ? value.trim() : '')

/** The response body, from a `{data}` envelope the API client returns. */
const bodyOf = (answer) => {
  const data = answer && typeof answer === 'object' ? answer.data : null
  return data && typeof data === 'object' && !Array.isArray(data) ? data : null
}

/**
 * One punctuated sentence. The two shared describers disagree about the
 * trailing stop — `describeApiError` leaves it off, `describePushRefusal`
 * carries it — and both get pasted in front of a second sentence below.
 */
const sentence = (value) => {
  const t = text(value)
  if (!t) return ''
  return /[.!?…]$/.test(t) ? t : `${t}.`
}

/** `offset` is absent or null wherever the broker allocated none; 0 is a real position. */
const offsetOf = (replayedAs) =>
  Number.isFinite(replayedAs?.offset) ? replayedAs.offset : null

const at = (offset) => (offset === null ? '' : ` at offset ${offset}`)

/**
 * The transaction id a replay of this row will carry, every time.
 *
 * Deterministic by construction (`dlq:<log_dlq row id>`, minted by the handler,
 * not by the client): a second replay of the same row is a `duplicate` inside
 * the destination's dedup window rather than a second copy of the message. It
 * is in the confirm copy because an operator who later greps the destination
 * queue for this message needs to know what to grep for.
 */
export const replayTransactionId = (dlqId) => `dlq:${dlqId}`

/**
 * The identity of ONE dead-letter row: what the list keys, what the drawer
 * selects, and what a replay addresses.
 *
 * NOT the message's address. queen.log_dlq holds one row PER CONSUMER GROUP for
 * the same frame, so `transactionId` names a SET of rows that routinely sit next
 * to each other on the page — the collision the old retry route replayed blind
 * (§1.3). A surface that picks a row by it opens on whichever sibling the list
 * happened to put first, and every button on that surface then acts on a
 * different consumer group's record than the one that was clicked.
 *
 * `id` is the address `POST /api/v1/dlq/:id/replay` takes, so it is the identity
 * everywhere a row is named. A listing that carries none (a broker older than
 * the `id` key in `get_dlq_messages_v1`) falls back to the transaction id PLUS
 * the consumer group — the same row by a longer name, and still exactly one row
 * per group — never to the transaction id alone, which is the collision itself.
 * Null for a row that carries neither: there is nothing there to address.
 */
export const dlqRowKey = (row) => {
  const id = row?.id == null ? '' : String(row.id).trim()
  if (id) return id
  const tx = text(row?.transactionId)
  const group = text(row?.consumerGroup)
  return tx || group ? `${tx}::${group}` : null
}

/**
 * What to send and what to say about it: the request body for `dlq.replay`,
 * plus the destination the confirm modal has to name.
 *
 * The two Advanced inputs are independent; THE BODY IS NOT. A replay body names
 * a whole (queue, partition) pair or it names no destination at all:
 *
 *   * the proxy admits a named destination against the tenant's plan caps the
 *     way it admits a first-contact push (gateway.rs `admit_replay_dest`),
 *     because the replay SP provisions what it does not find. The registry can
 *     only admit a PAIR, so a body carrying one half is refused with a 400 that
 *     names the missing half, and the operator's edit never reaches the broker;
 *   * the half left blank is not unknown here — the row carries the queue and
 *     the partition it failed on (`get_dlq_messages_v1`), which is exactly what
 *     the handler resolves an omitted override to
 *     (messages.rs: `queue_override.unwrap_or_else(|| row.queue.clone())`).
 *
 * So filling EITHER input sends BOTH halves, the untouched one filled in from
 * the row, and filling neither sends `{}` — the plain "replay where it failed",
 * which names no destination and is admitted without a pair. A blank is never
 * sent as `""`: an empty name is refused by the route and, past it, would
 * provision a queue nobody can name.
 *
 * The one body that can still go out half-named is a row whose own listing
 * carries no queue or no partition name. Sending the typed half alone is the
 * honest answer there — the refusal names the missing half, under the input
 * that has to supply it — where dropping the override would quietly replay the
 * message somewhere the operator did not ask for.
 *
 * `moved` is the difference between "replay this where it failed" and "move it
 * somewhere else", which is the sentence the confirm modal changes;
 * `namesDestination` is whether the body carries a destination at all, which is
 * the clause the modal adds to it.
 */
export function replayRequest(row, overrides = {}) {
  const sourceQueue = text(row?.queue)
  const sourcePartition = text(row?.partition)

  const queueOverride = text(overrides.queue)
  const partitionOverride = text(overrides.partition)

  // What the message will land on either way: the override if there is one, the
  // row's own value if there is not. This is the pair the modal names AND — as
  // soon as either half was typed — the pair the body carries.
  const queue = queueOverride || sourceQueue
  const partition = partitionOverride || sourcePartition

  const body = {}
  if (queueOverride || partitionOverride) {
    if (queue) body.queue = queue
    if (partition) body.partition = partition
  }

  const id = row?.id ?? null

  return {
    id,
    body,
    queue,
    partition,
    namesDestination: Boolean(body.queue || body.partition),
    transactionId: id ? replayTransactionId(id) : null,
    moved: Boolean((queue && queue !== sourceQueue) || (partition && partition !== sourcePartition)),
  }
}

/** The caveat that is true of every successful replay, wherever it landed. */
const APPEND_CAVEAT =
  'Replay appends: it does not restore the message’s position in the partition, ' +
  'and its age starts again at the destination.'

/**
 * What the dashboard renders for one replay answer.
 *
 * Takes the RESOLVED response from `dlq.replay` (its `data` is read) or the
 * `ApiError` it rejected with — the call site hands over whichever it got, and
 * the branch between them is here so no view has to know that two of the five
 * outcomes arrive as rejections.
 *
 * Returns:
 *   kind       'success' | 'info' | 'warning' | 'error' — maps 1:1 onto the
 *              four toast levels, and only 'success' means a copy was written
 *   title      one line, the outcome
 *   detail     one paragraph, what happened to the message AND to the row
 *   removeRow  the dead-letter row is verified gone from queen.log_dlq, so the
 *              list may drop it. False for everything unverified — a row that
 *              is still there must stay on screen
 *   refresh    what is on screen is known to be behind (another caller moved
 *              this row, or we never learned the outcome): reload the page
 *   field      'queue' | 'partition' | null — which Advanced input a 400 is
 *              about, so the refusal renders under the field that caused it
 *   unavailable  present, and true, ONLY when the answer proved the route is
 *              not served here (an older broker, or a proxy that does not
 *              classify it). The page remembers that one for the cluster epoch
 *              and stops offering the action; no other 404 may do that, and the
 *              `gone` above is a 404
 *   target     {queue, partition, offset, transactionId, messageId} for the two
 *              answers that name a destination, else null. `messageId` is null
 *              on a duplicate on purpose: the id the broker minted names a
 *              frame it did not write, and the copy already in the log carries
 *              its own id inside a segment blob that no route can read back
 */
export function replayVerdict(answer) {
  if (answer instanceof Error) return failureVerdict(answer)

  const body = bodyOf(answer)
  if (!body) {
    return {
      kind: 'error',
      title: 'The broker answered with no verdict',
      detail:
        'A replay answers with a result — moved, duplicate or gone — and this response carried none, ' +
        'so nothing can be said about the message or the dead-letter record. Reload the list before trying again.',
      removeRow: false,
      refresh: true,
      field: null,
      target: null,
    }
  }

  const replayedAs = body.replayedAs && typeof body.replayedAs === 'object' ? body.replayedAs : {}
  const offset = offsetOf(replayedAs)
  const queue = text(body.queue) || text(replayedAs.queueName)
  const partition = text(body.partition)
  const where = partition ? `${queue}/${partition}` : queue
  const transactionId = text(replayedAs.transaction_id) || null

  if (body.success === true && body.result === 'moved') {
    return {
      kind: 'success',
      title: `Replayed onto ${where}`,
      detail:
        `A copy of this message is at the tail of ${where}${at(offset)}, with transaction id ` +
        `${transactionId || 'the one the broker minted'}. The dead-letter record was removed in the ` +
        `same transaction, so this message is no longer dead-lettered for its consumer group. ${APPEND_CAVEAT}`,
      removeRow: true,
      refresh: false,
      field: null,
      target: {
        queue,
        partition,
        offset,
        transactionId,
        messageId: text(replayedAs.message_id) || null,
      },
    }
  }

  if (body.success === true && body.result === 'duplicate') {
    return {
      kind: 'warning',
      title: `${where} already holds this transaction id`,
      detail:
        `Nothing was written and nothing was removed: ${where} already carries a message${at(offset)} under ` +
        `${transactionId || 'this replay’s transaction id'}, which is the id every replay of this row uses. ` +
        `The broker cannot verify that copy is this dead letter, so the record was kept — open the ` +
        `destination at that offset, then replay elsewhere or purge the row.`,
      removeRow: false,
      refresh: false,
      field: null,
      target: {
        queue,
        partition,
        offset,
        transactionId,
        // Not `replayedAs.message_id`: the broker answers the zero uuid there
        // (fusion.rs' "original unknown" sentinel) because the copy already in
        // the log carries its own id inside a segment blob no route reads back.
        messageId: null,
      },
    }
  }

  // A 2xx this dashboard cannot read as one of the two verdicts. Nothing is
  // known about the row, so it stays on screen and the list is reloaded.
  return {
    kind: 'error',
    title: body.result ? `Unrecognised replay result “${body.result}”` : 'The replay was not confirmed',
    detail:
      'This broker answered the replay with a result this dashboard does not know, so it cannot say ' +
      'whether the message was written or whether the dead-letter record survived. Reload the list and ' +
      'check the destination queue before replaying again.',
    removeRow: false,
    refresh: true,
    field: null,
    target: null,
  }
}

/** The half of the mapping that arrives as a rejection. */
function failureVerdict(err) {
  const body = err.body && typeof err.body === 'object' && !Array.isArray(err.body) ? err.body : null

  // ---- the row is already gone -------------------------------------------
  // Both routes answer 404 for it, with two different bodies: the id-addressed
  // one carries `result:'gone'`, the address-addressed one keeps its historical
  // not-found shape. Either marker proves the broker looked and found no row,
  // which is exactly what distinguishes this 404 from the route-not-here one
  // below.
  const gone = err.status === 404 &&
    (body?.result === 'gone' || body?.error === 'Message not found')
  if (gone) {
    return {
      kind: 'warning',
      title: 'Already replayed or purged',
      detail:
        'The broker has no dead-letter row with this id any more: a second click, another operator, a ' +
        'purge or the retention sweeper got there first. Nothing was written by this attempt, and the ' +
        'list on screen was out of date — it has been refreshed.',
      removeRow: true,
      refresh: true,
      field: null,
      target: null,
    }
  }

  // ---- the route is not served here --------------------------------------
  // A bare 404, the proxy's route_blocked, and the SPA fallback the HTTP client
  // reports as not_an_api_response all mean the same thing and none of them
  // says anything about the row, so the row stays.
  if (isMissingRouteError(err)) {
    return {
      kind: 'warning',
      title: 'Replay is not available here',
      detail:
        'The replay route is not served on this cell — the broker predates it, or the proxy in front does ' +
        'not classify it. Asking again cannot change that; the dead-letter record is untouched and can ' +
        'still be purged or replayed with queenctl.',
      removeRow: false,
      refresh: false,
      field: null,
      target: null,
      // The ONE verdict a caller may remember for the cluster epoch
      // (stores/routeSupport.js). It cannot be derived from the status: `gone`
      // is a 404 too, and a page that remembered THAT would take the button
      // away for every row after the first already-purged one.
      unavailable: true,
    }
  }

  // ---- the destination override was refused ------------------------------
  // 400 exists only on the id-addressed route and only for the optional body,
  // so it is always about one of the two Advanced fields. `field` puts the
  // sentence under the input that caused it instead of in a toast.
  if (err.status === 400) {
    const detail = text(body?.error) || describeApiError(err)
    const field = /queue override/i.test(detail)
      ? 'queue'
      : (/partition override/i.test(detail) ? 'partition' : null)
    return {
      kind: 'error',
      title: 'The destination was refused',
      detail: `${sentence(detail)} Nothing was replayed and the dead-letter record is untouched.`,
      removeRow: false,
      refresh: false,
      field,
      target: null,
    }
  }

  // ---- no answer at all ---------------------------------------------------
  // The request may have been applied: a move that commits and then loses its
  // response is indistinguishable from one that never ran. Claiming either way
  // would be a guess, so the row stays and the list is reloaded.
  if (err.status === 0) {
    return {
      kind: 'error',
      title: 'No answer — the outcome is unknown',
      detail:
        `${sentence(describeApiError(err))} The replay may or may not have been applied, so nothing here can ` +
        'say whether the message was written. Reload the list: if the row is gone, the move happened.',
      removeRow: false,
      refresh: true,
      field: null,
      target: null,
    }
  }

  // ---- push maintenance is on ---------------------------------------------
  // Not a failure and not a refusal of this caller: the cell is not writing to
  // the log at all right now. A move cannot be spooled the way a push is (the
  // spool carries frames, not the removal of a dead-letter record), so the
  // broker refuses it and the row stays — which makes "try again later" an
  // honest instruction rather than a hope.
  if (err.status === 503 && body?.result === 'maintenance') {
    return {
      kind: 'warning',
      title: 'Push maintenance is on',
      detail:
        'This cell is not writing to the log while push maintenance is on, and a replay is a write. ' +
        'Nothing was replayed and the dead-letter record is untouched — replay it once the maintenance ' +
        'switch is off.',
      removeRow: false,
      refresh: false,
      field: null,
      target: null,
    }
  }

  // ---- the move itself failed --------------------------------------------
  // Two different 500s, and the broker says which by what it can prove:
  //
  //   dlqRowRemoved:false  the DATABASE refused the statement (it raised, and
  //                        every guard raises before the delete), so one
  //                        transaction rolled back whole and retrying
  //                        duplicates nothing
  //   dlqRowRemoved:null   the broker never learned the outcome — a connection
  //                        lost after a single-statement transaction may have
  //                        committed, and a committed move deleted the row
  //
  // Anything else on a 5xx (an older broker, a proxy's own error page) is read
  // as the second: an absent fact is not a fact.
  if (err.status >= 500) {
    if (body?.dlqRowRemoved === false) {
      return {
        kind: 'error',
        title: 'The replay failed',
        detail:
          `${sentence(text(body?.error) || describeApiError(err))} Nothing was replayed and the dead-letter ` +
          'record is untouched, so the same replay can safely be sent again.',
        removeRow: false,
        refresh: false,
        field: null,
        target: null,
      }
    }
    return {
      kind: 'error',
      title: 'The replay failed — the outcome is unknown',
      detail:
        `${sentence(text(body?.error) || describeApiError(err))} The broker could not say whether the move ` +
        'was applied, so nothing here can. Reload the list: if the row is gone, the move happened; if it ' +
        'is still there, the replay can be sent again.',
      removeRow: false,
      refresh: true,
      field: null,
      target: null,
    }
  }

  // ---- refused before the broker saw it ----------------------------------
  // The proxy classifies the replay routes QueueAdmin and applies the PUSH
  // blocks to them, because a replay grows retained bytes exactly like a push
  // (PLAN_DASHBOARD_ACTIONS.md §2.0). So the refusals are the push refusals,
  // word for word — a storage-blocked or quota-exhausted tenant is not lacking
  // a permission, and `describeApiError`'s "not permitted for your role" would
  // send them to the wrong person.
  return {
    kind: 'error',
    title: 'The replay was refused',
    detail: `${sentence(describePushRefusal(err))} Nothing was replayed and the dead-letter record is untouched.`,
    removeRow: false,
    refresh: false,
    field: null,
    target: null,
  }
}
