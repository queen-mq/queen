// Timers page rules — PURE functions, no Vue, no DOM (the useWorkload.js
// convention), so `npm test` can assert them without a browser.
//
// Three of them are the ones that can be wrong in a way nobody notices:
//
//  1. The broker stamps every timer instant as
//     `to_char(… 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` — SIX fractional digits
//     (025_log_timers.sql). ECMAScript's date-time format specifies three, and
//     an engine is free to reject the rest; V8 happens to truncate. Parsing it
//     here, explicitly, is what keeps "deliverAt" from becoming Invalid Date on
//     some future runtime and rendering as an em dash for everybody.
//
//  2. A timer's instant is in the FUTURE, which no formatter in this app knew
//     how to say: useApi.js formatRelativeTime only counts backwards ("3m
//     ago"), so a timer due in three minutes rendered as "just now". A timer
//     that is already PAST is also a real and important state — the sweeper
//     has not reached it, or it is in backoff — and "overdue" is the word for
//     it, not "ago".
//
//  3. The payload. `peek` hands back the bytes AS STORED (§13.4), and the two
//     flags that describe them are not interchangeable:
//
//       payloadZstd  BOOLEAN — the stored bytes are a zstd frame, compressed by
//                    the CLIENT before it scheduled. (It is NOT the payload:
//                    the payload is always `payload`, base64.)
//       encrypted    BOOLEAN — the broker encrypted at SCHEDULE, and encryption
//                    is OUTERMOST (handlers/timers.rs prepare_schedule), so an
//                    encrypted payload cannot be decompressed without the key
//                    the broker holds. Peek promises the envelope as stored and
//                    must not pretend otherwise: the console renders "encrypted
//                    envelope" and stops.
//
//     And the decode is BOUNDED. The broker caps the stored bytes; nothing caps
//     what they expand to, and a producer picks the ratio — so the preview
//     refuses past TIMER_PAYLOAD_BUDGET_BYTES instead of asking the operator's
//     tab for whatever a frame header claims. TWO header fields claim, not one:
//     the size the frame declares (zstdDeclaredSize) and the window it asks the
//     decoder to HOLD (zstdWindowSize), which fzstd allocates and memcpys per
//     block regardless of how little comes out. Both are read before decoding.
import { Decompress } from 'fzstd'

/**
 * `.123456Z` → `.123Z`, on a string that has a sub-millisecond fraction.
 *
 * Exported and tested SEPARATELY from parseBrokerInstant because V8 happens to
 * truncate the extra digits by itself: a test that asserts the parsed Date
 * measures the engine, passes with this line deleted, and would only fail on
 * the runtime the truncation exists for. Asserting the text keeps the rule
 * pinned where it is written.
 */
export function normalizeBrokerStamp(text) {
  if (typeof text !== 'string') return text
  return text.replace(/\.(\d{3})\d+(Z|[+-]\d{2}:?\d{2})?$/, '.$1$2')
}

/**
 * The broker's UTC stamp → Date, or null.
 *
 * Truncates the microsecond fraction to milliseconds rather than trusting the
 * engine to tolerate it; everything else is handed to Date unchanged, so an
 * epoch-millisecond number or a plain ISO string still work.
 *
 * EVERY rendering of a timer instant goes through this, the absolute half of a
 * cell as much as the relative one: two halves of one column parsed by two
 * different rules is how they come to disagree on the runtime that rejects the
 * six digits.
 */
export function parseBrokerInstant(value) {
  if (value === null || value === undefined || value === '') return null
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value
  const text = typeof value === 'number' ? value : normalizeBrokerStamp(String(value))
  const date = new Date(text)
  return Number.isNaN(date.getTime()) ? null : date
}

/** Coarse span, seconds up to a minute and never more precision than the unit. */
const span = (ms) => {
  const sec = Math.floor(ms / 1000)
  if (sec < 60) return `${sec}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m`
  if (sec < 86_400) return `${Math.floor(sec / 3600)}h`
  return `${Math.floor(sec / 86_400)}d`
}

/**
 * "in 4m" / "12s overdue" / "due now" — the relative half of the deliverAt
 * column. `now` is passed in rather than read from the clock so the whole
 * column is rendered against ONE instant (the load), which is what the card's
 * freshness stamp above it claims.
 *
 * Past is "overdue", never "ago": the timer has not been delivered yet, it is
 * late. The sweeper cycle, a claim in backoff and a paused cell all land here,
 * and all three are things an operator wants to see as lateness.
 */
export function formatDeliverIn(value, now = Date.now()) {
  const date = parseBrokerInstant(value)
  if (!date) return '—'
  const delta = date.getTime() - now
  if (Math.abs(delta) < 1000) return 'due now'
  return delta > 0 ? `in ${span(delta)}` : `${span(-delta)} overdue`
}

/** base64 → bytes, without assuming a DOM (atob exists in Node 24 too). */
function base64ToBytes(b64) {
  const binary = atob(b64)
  const out = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) out[i] = binary.charCodeAt(i)
  return out
}

/**
 * What this console will decompress before it stops and says so.
 *
 * The STORED bytes are bounded by the broker (QUEEN_TIMERS_MAX_PAYLOAD_BYTES,
 * 1 MiB by default); the decompressed bytes are not bounded by anything. zstd's
 * ratio on a compressible payload is four orders of magnitude — 2 067 stored
 * bytes expand to 64 MB, measured with this very decoder — so a payload a
 * producer chose can ask the operator's tab for tens of gigabytes on the main
 * thread, synchronously, just by being peeked. A budget is the difference
 * between a drawer that says "too large to preview" and a browser that dies.
 *
 * 8 MB is far above any payload a queue message plausibly carries and far below
 * what a tab can absorb.
 */
export const TIMER_PAYLOAD_BUDGET_BYTES = 8 * 1024 * 1024

/**
 * `Frame_Content_Size` out of a zstd frame header, or null when the frame does
 * not declare one (RFC 8878 §3.1.1.1).
 *
 * Read BEFORE decoding: a frame that declares 30 GB is refused without the
 * decoder ever allocating for it. A frame that declares nothing — what every
 * streaming compressor produces — is bounded by the running total instead.
 */
export function zstdDeclaredSize(bytes) {
  const b = bytes
  if (!b || b.length < 5) return null
  // Magic_Number 0xFD2FB528, little-endian on the wire.
  if (b[0] !== 0x28 || b[1] !== 0xb5 || b[2] !== 0x2f || b[3] !== 0xfd) return null
  const descriptor = b[4]
  const fcsFlag = descriptor >> 6
  const singleSegment = (descriptor >> 5) & 1
  const dictIdFlag = descriptor & 3
  // Dictionary_ID_Field_Size: 0, 1, 2, 4 — the flag is the size except for 3.
  const dictIdSize = dictIdFlag === 3 ? 4 : dictIdFlag
  // Frame_Content_Size_Field_Size: flag 0 is 1 byte with Single_Segment set and
  // ABSENT without it; 1, 2, 3 are 2, 4, 8.
  const fcsSize = fcsFlag === 0 ? (singleSegment ? 1 : 0) : 1 << fcsFlag
  if (fcsSize === 0) return null
  // Magic(4) · descriptor(1) · Window_Descriptor(1, unless Single_Segment) · dict id
  const offset = 5 + (singleSegment ? 0 : 1) + dictIdSize
  if (b.length < offset + fcsSize) return null
  let value = 0
  for (let i = fcsSize - 1; i >= 0; i -= 1) value = value * 256 + b[offset + i]
  // The 2-byte form stores the size minus 256.
  return fcsSize === 2 ? value + 256 : value
}

/**
 * The WINDOW a frame asks the decoder to hold (`Window_Descriptor`, RFC 8878
 * §3.1.1.1.2), in bytes, or null when the frame is not zstd.
 *
 * A DIFFERENT header field from `Frame_Content_Size`, and the one that decides
 * what this decode actually costs. fzstd's streaming path — the one
 * `new Decompress()` takes — allocates the WINDOW up front (`rzfh`:
 * `new u8(ws + 12)`) and then `copyWithin`s the whole of it once per block, so
 * a frame whose output is one megabyte still allocates and memcpys whatever its
 * sixth byte asks for. fzstd's own only ceiling is ~2 GiB, so 134 stored bytes
 * declaring windowLog 30 hold the main thread for 3.3 s and take 1.9 GB while
 * decoding 4 MB — under both the declared-size check and the running total,
 * which is why neither of them sees it.
 *
 * `2 **`, not `1 <<`: the exponent reaches 41 and a JS shift counts mod 32,
 * which turns the largest windows into small (or negative) numbers.
 */
export function zstdWindowSize(bytes) {
  const b = bytes
  if (!b || b.length < 6) return null
  if (b[0] !== 0x28 || b[1] !== 0xb5 || b[2] !== 0x2f || b[3] !== 0xfd) return null
  // Single_Segment: there is no Window_Descriptor and the window IS the content
  // size, which the declared-size check already bounds.
  if ((b[4] >> 5) & 1) return zstdDeclaredSize(b)
  const wd = b[5]
  const base = 2 ** (10 + (wd >> 3))
  return base + (base / 8) * (wd & 7)
}

/**
 * zstd → bytes, refusing anything past `budget`.
 *
 * Streamed rather than decompressed whole, so the refusal happens while the
 * output is still one block long instead of after the allocation it exists to
 * prevent. Throws `RangeError` past the budget; every other failure is the
 * decoder's own error, which the caller renders as "did not decompress".
 */
function decompressBounded(stored, budget) {
  const chunks = []
  let total = 0
  const sink = new Decompress((chunk) => {
    total += chunk.length
    if (total > budget) throw new RangeError('payload_over_budget')
    chunks.push(chunk)
  })
  sink.push(stored, true)
  const out = new Uint8Array(total)
  let at = 0
  for (const chunk of chunks) {
    out.set(chunk, at)
    at += chunk.length
  }
  return out
}

/**
 * One peeked timer's payload, decoded as far as it honestly can be.
 *
 * `{state, text, bytes, storedBytes, error}` where state is one of:
 *
 *   'absent'     no payload field — a `found:false` peek, or a list row (the
 *                list never carries payloads, by design: an unbounded read
 *                whose cost the caller does not fix)
 *   'encrypted'  encrypted:true — rendered as an envelope, never decoded
 *   'empty'      zero bytes, which is a legal payload and not a failure
 *   'text'       valid UTF-8, in `text`
 *   'binary'     decoded, but not UTF-8: only the size is shown
 *   'too_large'  a zstd frame that expands past TIMER_PAYLOAD_BUDGET_BYTES —
 *                NOT decompressed; `bytes` is the declared size where the frame
 *                declared one, null where the budget stopped it mid-stream
 *   'error'      the base64 or the zstd frame did not decode, `error` says so
 *
 * `bytes` is the size AFTER decompression (what the consumer will receive);
 * `storedBytes` is what the row occupies in Postgres. They differ exactly when
 * payloadZstd is true, and showing both is the only way the console can say
 * whether a client's compression is doing anything.
 */
export function decodeTimerPayload(peeked) {
  const raw = peeked && typeof peeked === 'object' ? peeked.payload : null
  if (typeof raw !== 'string') return { state: 'absent', text: '', bytes: null, storedBytes: null, error: null }
  if (peeked.encrypted === true) {
    // Encryption is outermost, so there is nothing further this browser could
    // do even if it wanted to: the key is the broker's (§13.4).
    return { state: 'encrypted', text: '', bytes: null, storedBytes: null, error: null }
  }

  let stored
  try {
    stored = base64ToBytes(raw)
  } catch {
    return { state: 'error', text: '', bytes: null, storedBytes: null, error: 'the payload is not valid base64' }
  }
  if (stored.length === 0) {
    return { state: 'empty', text: '', bytes: 0, storedBytes: 0, error: null }
  }

  let bytes = stored
  if (peeked.payloadZstd === true) {
    // The header's own claim first: a frame that says it expands to gigabytes
    // is refused before the decoder allocates for it.
    const declared = zstdDeclaredSize(stored)
    if (declared !== null && declared > TIMER_PAYLOAD_BUDGET_BYTES) {
      return { state: 'too_large', text: '', bytes: declared, storedBytes: stored.length, error: null }
    }
    // And the window the frame asks us to HOLD, which the declared size does not
    // bound and the running total cannot see (zstdWindowSize). The budget is
    // reused as the ceiling because a window wider than the output we are
    // willing to keep buys a legitimate frame nothing: node's default level
    // declares 2 MB and level 19 declares exactly 8 MB, both admitted; only a
    // client compressing at `--ultra -22` (128 MB) is refused, and it is
    // refused as 'too_large' rather than decoded, which is the right direction.
    const window = zstdWindowSize(stored)
    if (window !== null && window > TIMER_PAYLOAD_BUDGET_BYTES) {
      return { state: 'too_large', text: '', bytes: null, storedBytes: stored.length, error: null }
    }
    try {
      bytes = decompressBounded(stored, TIMER_PAYLOAD_BUDGET_BYTES)
    } catch (err) {
      // A frame that declared nothing and kept going: stopped by the running
      // total, which is not a decode failure and must not read as one.
      if (err instanceof RangeError) {
        return { state: 'too_large', text: '', bytes: null, storedBytes: stored.length, error: null }
      }
      return {
        state: 'error',
        text: '',
        bytes: null,
        storedBytes: stored.length,
        error: `the zstd frame did not decompress (${err?.message || 'unknown error'})`,
      }
    }
  }

  try {
    // fatal: a payload that is not UTF-8 must read as binary, not as a string
    // full of replacement characters that looks like corruption.
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    return { state: 'text', text, bytes: bytes.length, storedBytes: stored.length, error: null }
  } catch {
    return { state: 'binary', text: '', bytes: bytes.length, storedBytes: stored.length, error: null }
  }
}

/**
 * The cancel verdict, in the words of the stored procedure that issued it.
 *
 * Three outcomes, all HTTP 200 (handlers/timers.rs: the status describes the
 * CALL, the body describes the timer), and the two that are not "cancelled"
 * are the ones a button must not round off:
 *
 *   too_late  a broker holds the claim and is about to commit the delivery.
 *             Granting the cancel would leave "did it go out?" unanswerable,
 *             so the SP refuses; the window is bounded by the lease. IT IS NOT
 *             A FINAL ANSWER: log_timers_fail_v1 pushes `claimed_until` out by
 *             a backoff and NULLs the claim token, so a delivery that FAILS
 *             puts the row back in nobody's hands and the next cancel takes it
 *             (025_log_timers.sql: "a cancel during a backoff SUCCEEDS, or a
 *             poisoned timer would be uncancellable for minutes"). A sentence
 *             that says the delivery cannot be stopped sends the operator away
 *             from the one retry that would have worked.
 *   absent    THERE IS NO TOMBSTONE (§4.4). The row is gone, which may mean it
 *             already fired. The SP echoes the caller's txn precisely so that
 *             check needs no second call — so the sentence names it.
 *
 * `status` is returned untouched for the view to render verbatim, including a
 * status this build has never heard of.
 */
export function describeCancel(result, { queue = '', timerKey = '' } = {}) {
  const r = result && typeof result === 'object' ? result : null
  const status = typeof r?.status === 'string' ? r.status : null
  const ok = r?.ok === true
  const txn = typeof r?.txn === 'string' && r.txn ? r.txn : null
  const key = timerKey || r?.timerKey || ''
  const q = queue || r?.queue || ''

  if (status === 'cancelled') {
    return {
      status, ok, tone: 'ok',
      sentence: `Cancelled — "${key}" is gone from ${q} and will not fire.`,
    }
  }
  if (status === 'too_late') {
    return {
      status, ok, tone: 'warn',
      sentence:
        `Too late — a broker holds the claim on "${key}" and is committing this delivery, so it ` +
        `was NOT cancelled. The claim is bounded by the lease: if that attempt fails the timer ` +
        `goes back to pending in backoff (attempts +1) and a cancel works again — reopen the row ` +
        `to check.`,
    }
  }
  if (status === 'absent') {
    return {
      status, ok, tone: 'warn',
      sentence:
        `No longer pending — and a cancelled timer leaves no tombstone, so this MAY mean it has ` +
        `already been delivered. The log is the authority: look for ` +
        `${txn ? `txn "${txn}"` : 'the timer’s txn'} on ${q}.`,
    }
  }
  return {
    status, ok, tone: 'warn',
    sentence: status
      ? `The broker answered "${status}", which this console does not recognise.`
      : 'The broker answered nothing this control can verify — treat the timer as still pending.',
  }
}

/**
 * The list route's query, with the first page's cursor OMITTED rather than sent
 * as null.
 *
 * This is the reason it is a function and not two lines in the view. ky
 * serialises `{after: null}` as the four characters `after=null`, and
 * handlers/timers.rs keeps any non-empty string as a cursor, so the SP would
 * apply `timer_key > 'null'` under COLLATE "C" and the FIRST page would
 * silently lose every key that sorts at or below it — digits, `-`, `.`, `:`,
 * every uppercase letter, `_`, and `a` through `nul`. No error, no banner: a
 * short page. `undefined` would also be dropped by ky, but "the key is not
 * there" is the property worth asserting, so the object is built to lack it.
 */
export function timerListParams(after, limit) {
  return after === null || after === undefined ? { limit } : { after, limit }
}

/**
 * Whether a page move whose own answer never landed may walk the cursor stack
 * back — the pure half of Timers.vue's `movePage`.
 *
 * `next()` has to push the cursor BEFORE the request can be sent (it is what
 * produces the cursor to send), so a page that does not land leaves the stack
 * one deeper than the rows on screen and the footer counts a page nobody is
 * looking at. The undo is therefore necessary. It is also dangerous: the walk
 * is shared state, and a load that is STILL IN FLIGHT already owns it.
 *
 * Two different tests, because there are two different ways to lose the walk:
 *
 *   the marks     a load that has already LANDED, or a reset (a new queue, a
 *                 new page size, a cluster switch), REPLACED `cursors` and
 *                 `tail` rather than mutating them, so reference identity sees
 *                 it. This is the completed-work test.
 *   the sequence  a load that is still IN FLIGHT has touched nothing yet, so
 *                 the marks are identical and identity says "nothing happened"
 *                 — wrongly. Pressing the header's Refresh during a Next starts
 *                 a second load, which supersedes the move's own (useApi
 *                 RESOLVES the superseded call with the previous data, so the
 *                 move sees `landed:false` and reaches here) and asks for the
 *                 MOVED cursor, because that is what the pager held when it
 *                 built its params. Unwinding under it prints "Page 2" over
 *                 page 3's rows the moment it lands. This is the work-in-
 *                 progress test, and it is the one identity cannot make.
 *
 * `seqAtMove` is the load counter read before the move's own load was started,
 * `seqNow` the same counter after its answer came back. At most ONE load may
 * have started in between — the move's own — and the move only owns the walk
 * while that holds. (A move whose reload refused to send at all, on a stable
 * gated verdict, leaves the counter untouched: nothing is in flight and the
 * undo is safe, which is why the test is `<= 1` and not `=== 1`.)
 *
 * A counter that is not a number means the caller cannot say what has been
 * started, and the walk stays where it is: "Page 3" over page 2 costs one press
 * of Previous, while an unwind under a live load mislabels rows silently.
 */
export function moveUnwinds({ landed, seqAtMove, seqNow, movedMark, nowMark } = {}) {
  if (landed) return false
  if (!Number.isFinite(seqAtMove) || !Number.isFinite(seqNow)) return false
  if (seqNow - seqAtMove > 1) return false
  if (!movedMark || !nowMark) return false
  return nowMark.cursors === movedMark.cursors && nowMark.tail === movedMark.tail
}

/**
 * The rows that belong to the queue on screen, or none.
 *
 * A page of timers is only ever rendered under the name it was fetched for:
 * between picking a new queue and its answer landing, the previous queue's rows
 * under the new queue's title is the same class of lie the tenant-keyed stores
 * exist to prevent, one scope down.
 */
export function rowsForQueue(loadedQueue, queue, data) {
  if (loadedQueue !== queue) return []
  const rows = data?.rows
  return Array.isArray(rows) ? rows : []
}

/**
 * Whether the tenant-wide `timerRows` figure is a MEASUREMENT, given the page
 * on screen (the same rule useKvView.js applies to `kvRows`, with the page as
 * the witness instead of the namespace listing).
 *
 * `handlers/queues.rs` sends `timerRows: 0` for a tenant the sweeper has not
 * reached — `kv_usage_snapshot` returning no row is `unwrap_or((0,0,0,0))` —
 * and omits the four fields only when the query itself FAILED. So a freshly
 * booted cell answers a confident zero, and `≈ 0 timers pending` over a full
 * page of timers is a number that is certainly wrong. A zero the page
 * contradicts is not a measurement, and the honest rendering of "this cell has
 * not measured yet" is silence: `—` would be a claim of its own.
 */
export function timerUsageIsMeasured(timerRows, rowsOnPage) {
  if (!Number.isFinite(timerRows)) return false
  if (timerRows > 0) return true
  return !(Array.isArray(rowsOnPage) && rowsOnPage.length > 0)
}
