/**
 * The binary push body: an alternative to the JSON body of POST /api/v1/push.
 *
 *   "QPB1" | u8 flags | [u32 raw_len, when zstd] | body
 *   body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
 *          | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
 *
 * All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
 * bytes, 2 = answer with the counts only, 4 = every item carries its
 * transaction id (empty = the broker mints it). No trace id. Each payload is
 * one JSON document.
 *
 * With flag 8 (encodePushFrames) a group is the messages as the broker stores
 * them, compressed here once:
 *
 *   body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
 *   z:     one zstd frame of n frames: u32 body_len | u8 flags | u8[16] message id
 *          | u16 len, transactionId | [u16 len, headers, flag 8] | payload
 *
 * The message id is a version-7 UUID minted by the client; a message without
 * its own transaction id uses the id's text. The broker checks every frame and
 * keeps the compressed bytes as they are, so it does not compress them again.
 *
 * A frame's flags are 0, or 8 for a message with headers (catalogue version
 * 8, PLAN_ROUTING.md §6): its headers block, `u16 count | (u8 len, name |
 * u16 len, value) x count`, sorted by name, comes after the transaction id.
 * The plain body (no flag 8) has no room for headers: a push with headers
 * travels in it only framed, and is JSON otherwise.
 *
 * A broker that does not know this body answers 400; a client sends it only
 * when `binaryPush` is set, so by default a push is the JSON request every
 * broker knows.
 *
 * The binary transaction body (encodeTransactionFrames), an alternative to the
 * JSON body of POST /api/v1/transaction, carries the pushes the same way:
 *
 *   "QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above
 *
 * head is the JSON transaction body without its push operations: the acks in
 * `operations`, `requiredLeases`, `kv`, `timers`. flags: 2 = the answer leaves
 * out the pushes' and the acks' results and counts them (`pushed`,
 * `duplicate`, `acked`); the riders' results stay. In the answer the head's
 * operations come first and the groups' messages after them, in the order
 * written; the riders follow, as in the JSON body.
 */

import { v7 as uuidv7 } from 'uuid'
import { zstdCompress } from './zstd.js'
import { HEADER_NAME } from '../utils/validation.js'

export const PUSH_BINARY_CONTENT_TYPE = 'application/x-queen-push'

const MAGIC = Uint8Array.of(0x51, 0x50, 0x42, 0x31) // "QPB1"
const TXN_MAGIC = Uint8Array.of(0x51, 0x54, 0x42, 0x31) // "QTB1"
const FLAG_ZSTD = 1
const FLAG_COUNTS = 2
const FLAG_TXN = 4
const FLAG_FRAMES = 8
// A message frame's own flag (not the body's): its headers block follows the transaction id.
const FRAME_FLAG_HEADERS = 8

const utf8 = new TextEncoder()
const HEX = '0123456789abcdef'

/** A message does not fit the body: a name or transaction id over 65535 bytes. */
function tooLong(what) {
  const error = new Error(`${what} longer than 65535 bytes`)
  error.tooLong = true
  return error
}

/** A growing byte buffer; strings are encoded straight into it. */
class Out {
  constructor(size = 1024) {
    this.bytes = new Uint8Array(size)
    this.view = new DataView(this.bytes.buffer)
    this.length = 0
  }

  need(extra) {
    if (this.length + extra <= this.bytes.length) return
    const grown = new Uint8Array(Math.max(this.bytes.length * 2, this.length + extra))
    grown.set(this.bytes.subarray(0, this.length))
    this.bytes = grown
    this.view = new DataView(grown.buffer)
  }

  u8(value) {
    this.need(1)
    this.bytes[this.length++] = value
  }

  u16(value) {
    this.need(2)
    this.view.setUint16(this.length, value, true)
    this.length += 2
  }

  u32(value) {
    this.need(4)
    this.view.setUint32(this.length, value, true)
    this.length += 4
  }

  /** Four bytes to fill in later with `setU32`; returns where they are. */
  reserveU32() {
    const at = this.length
    this.u32(0)
    return at
  }

  setU32(at, value) {
    this.view.setUint32(at, value, true)
  }

  raw(bytes) {
    this.need(bytes.length)
    this.bytes.set(bytes, this.length)
    this.length += bytes.length
  }

  /** `text` as UTF-8, no length in front; returns its length in bytes. */
  text(text) {
    this.need(text.length * 3)
    const { written } = utf8.encodeInto(text, this.bytes.subarray(this.length))
    this.length += written
    return written
  }

  /** `text` with its u16 length in front. */
  text16(text, what) {
    this.need(2)
    const at = this.length
    this.length += 2
    const written = this.text(text)
    if (written > 0xffff) throw tooLong(what)
    this.view.setUint16(at, written, true)
  }

  /** `text` with its u32 length in front. */
  text32(text) {
    const at = this.reserveU32()
    this.setU32(at, this.text(text))
  }

  done() {
    return this.bytes.subarray(0, this.length)
  }
}

function checkBatch(batch) {
  if (batch.transactionIds && batch.transactionIds.length !== batch.payloads.length) {
    throw new Error(`${batch.transactionIds.length} transaction ids for ${batch.payloads.length} payloads`)
  }
  if (batch.headers && batch.headers.length !== batch.payloads.length) {
    throw new Error(`${batch.headers.length} headers for ${batch.payloads.length} payloads`)
  }
}

/**
 * A message's headers as the frame's block, the bytes the broker writes for
 * them (server/src/frames.rs encode_headers): `u16 count | (u8 len, name |
 * u16 len, value) x count`, sorted by name. Null for none: a frame with an
 * empty block is refused, so it carries no flag instead.
 */
export function headersBlock(headers) {
  if (!headers) return null
  const names = Object.keys(headers).sort()
  if (names.length === 0) return null
  const block = new Out(64)
  block.u16(names.length)
  for (const name of names) {
    // Names are a-z 0-9 . _ - (addItem checked them): one byte a character.
    block.u8(name.length)
    block.text(name)
    block.text16(headers[name], 'a header value')
  }
  if (block.length > 0xffff) throw tooLong("a message's headers")
  return block.done()
}

/**
 * Encode batches as a binary push body.
 *
 * @param {Array<{queue: string, partition: string, payloads: string[], transactionIds?: string[]}>} batches
 *   one partition's messages each; a payload is the text of one JSON document,
 *   and transactionIds, when given, holds one per payload ('' = the broker
 *   mints it)
 * @param {object} [options]
 * @param {boolean} [options.compress=false] the body is zstd-compressed
 * @param {boolean} [options.counts=false] the broker answers
 *   `{count, queued, duplicate, error}` instead of one object per message
 * @returns {Promise<Uint8Array>}
 */
export async function encodePushBinary(batches, { compress = false, counts = false } = {}) {
  const withTxn = batches.some(batch => batch.transactionIds)
  const flags = (counts ? FLAG_COUNTS : 0) | (withTxn ? FLAG_TXN : 0)
  const body = new Out()
  body.u32(batches.length)
  for (const batch of batches) {
    checkBatch(batch)
    if (batch.headers && batch.headers.some(Boolean)) {
      throw new Error('the plain binary push body carries no headers: frame the batch (encodePushFrames)')
    }
    body.text16(batch.queue, 'queue name')
    body.text16(batch.partition, 'partition name')
    body.u32(batch.payloads.length)
    for (let i = 0; i < batch.payloads.length; i++) {
      if (withTxn) body.text16(batch.transactionIds ? batch.transactionIds[i] : '', 'transaction id')
      body.text32(batch.payloads[i])
    }
  }

  const out = new Out(MAGIC.length + 5 + (compress ? 0 : body.length))
  out.raw(MAGIC)
  if (!compress) {
    out.u8(flags)
    out.raw(body.done())
    return out.done()
  }
  out.u8(flags | FLAG_ZSTD)
  out.u32(body.length)
  out.raw(await zstdCompress(body.done()))
  return out.done()
}

/**
 * Encode batches as a binary push body whose groups are the messages framed
 * as the broker stores them and compressed once, here. Same batches as
 * `encodePushBinary`, and a batch may hold `headers` too, one per payload (an
 * object of strings, or null for none); `counts` asks for the short answer.
 *
 * @returns {Promise<Uint8Array>}
 */
export async function encodePushFrames(batches, { counts = false } = {}) {
  const out = new Out()
  out.raw(MAGIC)
  out.u8(FLAG_FRAMES | (counts ? FLAG_COUNTS : 0))
  return appendFrameGroups(out, batches)
}

/**
 * Encode a binary transaction body: `head`, the JSON transaction body without
 * its push operations (its text; empty for none), then batches as groups of
 * messages framed as the broker stores them and compressed once, here.
 * `counts`: the broker counts the pushes and the acks in its answer instead of
 * rendering one result for each.
 *
 * @returns {Promise<Uint8Array>}
 */
export async function encodeTransactionFrames(head, batches, { counts = false } = {}) {
  const out = new Out()
  out.raw(TXN_MAGIC)
  out.u8(counts ? FLAG_COUNTS : 0)
  out.text32(head || '')
  return appendFrameGroups(out, batches)
}

/**
 * Appends `u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
 * | u32 z_len | z` to `out`: each batch's messages framed as the broker stores
 * them, with a version-7 id minted here, as one zstd frame. A message with
 * headers gets flag 8 and its block after the transaction id.
 */
async function appendFrameGroups(out, batches) {
  out.u32(batches.length)
  const frames = new Out()
  for (const batch of batches) {
    checkBatch(batch)
    frames.length = 0
    for (let i = 0; i < batch.payloads.length; i++) {
      const block = batch.headers ? headersBlock(batch.headers[i]) : null
      const bodyLength = frames.reserveU32()
      frames.u8(block ? FRAME_FLAG_HEADERS : 0)
      frames.need(16)
      const id = frames.length
      uuidv7(undefined, frames.bytes, id)
      frames.length += 16
      const txn = batch.transactionIds ? batch.transactionIds[i] : ''
      frames.text16(txn || uuidText(frames.bytes, id), 'transaction id')
      if (block) {
        frames.u16(block.length)
        frames.raw(block)
      }
      frames.text(batch.payloads[i])
      frames.setU32(bodyLength, frames.length - bodyLength - 4)
    }
    out.text16(batch.queue, 'queue name')
    out.text16(batch.partition, 'partition name')
    out.u32(batch.payloads.length)
    const compressed = await zstdCompress(frames.done())
    out.u32(compressed.length)
    out.raw(compressed)
  }
  return out.done()
}

/** The lower-case hyphenated text of the 16 id bytes at `at`. */
export function uuidText(bytes, at = 0) {
  let text = ''
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) text += '-'
    const byte = bytes[at + i]
    text += HEX[byte >> 4] + HEX[byte & 0x0f]
  }
  return text
}

/**
 * The items of a JSON push (`{queue, partition, payload, transactionId,
 * headers}`) as a binary push body, or null when one of them cannot travel in
 * it: a trace id, headers without `compress` (the plain body has no room for
 * them), a transaction id that is not a string, a payload JSON does not carry,
 * a name over 65535 bytes. The caller then sends the JSON body, and the broker
 * answers that as it always did. Consecutive items of one partition share a
 * group, so the order of the items is the order on the wire.
 *
 * `compress`: the messages framed and compressed here, kept by the broker as
 * they are.
 */
export async function binaryPushBody(items, compress) {
  if (items.length === 0) return null
  const batches = []
  for (const item of items) {
    if (!addItem(batches, item, compress)) return null
  }
  try {
    return compress ? await encodePushFrames(batches) : await encodePushBinary(batches)
  } catch (error) {
    // A name too long for the body: the JSON push says so in the broker's words.
    if (error.tooLong) return null
    throw error
  }
}

/**
 * Adds one pushed item to `batches`, in the last one when it is the same
 * partition's. False when the item cannot travel in a binary body; `framed`:
 * the body frames its messages, and so carries their headers.
 */
function addItem(batches, item, framed) {
  if (!item || typeof item !== 'object' || typeof item.queue !== 'string') return false
  if (item.traceId !== undefined && item.traceId !== null) return false
  const transactionId = item.transactionId ?? ''
  if (typeof transactionId !== 'string') return false
  const partition = item.partition ?? 'Default'
  if (typeof partition !== 'string') return false
  const headers = frameHeaders(item.headers)
  if (headers === false || (headers !== null && !framed)) return false
  const payload = JSON.stringify(item.payload)
  if (payload === undefined) return false
  let last = batches[batches.length - 1]
  if (!last || last.queue !== item.queue || last.partition !== partition) {
    last = { queue: item.queue, partition, payloads: [], transactionIds: [], headers: [] }
    batches.push(last)
  }
  last.payloads.push(payload)
  last.transactionIds.push(transactionId)
  last.headers.push(headers)
  return true
}

/**
 * An item's headers as a frame carries them: null for none, false for
 * headers a frame cannot hold as the broker reads them (a name outside a-z
 * 0-9 . _ -, a value that is not a string). Those stay JSON, where the broker
 * says what is wrong in its own words.
 */
function frameHeaders(headers) {
  if (headers === undefined || headers === null) return null
  if (typeof headers !== 'object' || Array.isArray(headers)) return false
  const names = Object.keys(headers)
  if (names.length === 0) return null
  for (const name of names) {
    if (!HEADER_NAME.test(name) || typeof headers[name] !== 'string') return false
  }
  return headers
}

/**
 * A JSON transaction body (`{operations, requiredLeases, kv, timers}`) as the
 * binary one, when its pushes can travel in it: there is at least one, and
 * none carries a trace id. Everything but the pushes stays in the JSON head;
 * consecutive pushed items of one partition share a group, so the pushes keep
 * their order. Null otherwise: the caller then sends the JSON body. Null too
 * for a `link` after a push: a link writes a message, and the broker writes
 * the head's operations before the groups, so only the JSON body keeps the
 * two in the order they were added.
 *
 * `order` is null when the broker's answer lists the operations as the caller
 * added them (every other operation before every push). Otherwise `order[k]`
 * is the place, among the caller's operations, of the k-th result the broker
 * sends: it lists the head's operations first, then the pushes.
 *
 * @returns {Promise<{body: Uint8Array, order: number[]|null}|null>}
 */
export async function transactionFrames(request) {
  if (!request || typeof request !== 'object' || !Array.isArray(request.operations)) return null
  const head = { ...request, operations: [] }
  const batches = []
  const headAt = []
  const pushAt = []
  let flat = 0
  for (const operation of request.operations) {
    if (!operation || operation.type !== 'push') {
      if (operation && operation.type === 'link' && pushAt.length > 0) return null
      head.operations.push(operation)
      headAt.push(flat++)
      continue
    }
    // A push names its items, or is one item itself.
    const items = Array.isArray(operation.items) ? operation.items : [operation]
    for (const item of items) {
      if (!addItem(batches, item, true)) return null
      pushAt.push(flat++)
    }
  }
  if (batches.length === 0) return null
  let body
  try {
    body = await encodeTransactionFrames(JSON.stringify(head), batches)
  } catch (error) {
    if (error.tooLong) return null
    throw error
  }
  const order = [...headAt, ...pushAt]
  return { body, order: order.every((at, k) => at === k) ? null : order }
}

/**
 * Puts the results of a binary transaction back in the order the caller added
 * its operations in, and gives each its own index again. The riders' results,
 * after the operations', are already in place.
 */
export function restoreResultOrder(answer, order) {
  if (!order || !answer || !Array.isArray(answer.results) || answer.results.length < order.length) return
  const results = answer.results.slice()
  order.forEach((at, k) => {
    const result = answer.results[k]
    results[at] = result && typeof result === 'object' ? { ...result, index: at } : result
  })
  answer.results = results
}
