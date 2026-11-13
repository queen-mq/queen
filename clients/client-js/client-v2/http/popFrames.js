/**
 * The framed pop answer: what a broker sends for a pop with `frames=true`.
 * The messages come as the stored records, each still the zstd frame the
 * producer (or the broker) made, and are unpacked here.
 *
 *   "QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
 *   u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
 *   per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
 *   per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes
 *
 * All integers little-endian. A record's bytes are `count` frames:
 *
 *   u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
 *   | u16 len, transactionId | [u16 len, producer, flag 2]
 *   | [u16 len, headers, flag 8] | payload
 *
 * The broker answers JSON instead whenever a record holds headers or links
 * (catalogue version 8), as it does for encrypted ones; the headers block
 * (`u16 count | (u8 len, name | u16 len, value) x count`) is read all the same.
 *
 * Message i of a record has offset base_offset+i and belongs to the delivery
 * when that lies in the claim's start..=end. The JSON object in front holds what
 * the JSON answer has beside its messages: the conflation echo and the
 * autopilot's choice.
 */

import { uuidText } from './pushBinary.js'
import { zstdDecompress } from './zstd.js'

export const POP_FRAMES_CONTENT_TYPE = 'application/x-queen-pop'

const MAGIC = 'QPF1'
const FLAG_TRACE = 1
const FLAG_PRODUCER = 2
const FLAG_ENCRYPTED = 4
const FLAG_HEADERS = 8
const FLAG_LINK = 16

const utf8 = new TextDecoder()

function damaged(what) {
  return new Error(`framed pop answer: ${what}`)
}

/** Reads little-endian fields off a byte array; a read past the end throws. */
class In {
  constructor(bytes, short = 'truncated') {
    this.bytes = bytes
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    this.at = 0
    this.short = short
  }

  take(length) {
    if (this.bytes.length - this.at < length) throw damaged(this.short)
    const slice = this.bytes.subarray(this.at, this.at + length)
    this.at += length
    return slice
  }

  u8() {
    return this.take(1)[0]
  }

  u16() {
    this.take(2)
    return this.view.getUint16(this.at - 2, true)
  }

  u32() {
    this.take(4)
    return this.view.getUint32(this.at - 4, true)
  }

  u64() {
    this.take(8)
    return this.view.getBigUint64(this.at - 8, true)
  }

  i64() {
    this.take(8)
    return this.view.getBigInt64(this.at - 8, true)
  }

  text() {
    return utf8.decode(this.take(this.u16()))
  }

  rest() {
    return this.bytes.subarray(this.at)
  }
}

/** A frame's headers block as the object the JSON answer has: `{ name: value }`. */
function decodeHeaders(block) {
  const input = new In(block, 'a headers block is malformed')
  const headers = {}
  for (let n = input.u16(); n > 0; n--) {
    const name = utf8.decode(input.take(input.u8()))
    headers[name] = input.text()
  }
  if (input.at !== block.length) throw damaged('a headers block is malformed')
  return headers
}

/** Whether a pop answer's bytes are the framed one. */
export function isPopFrames(bytes) {
  if (!bytes || bytes.length < MAGIC.length + 1) return false
  for (let i = 0; i < MAGIC.length; i++) {
    if (bytes[i] !== MAGIC.charCodeAt(i)) return false
  }
  return true
}

/**
 * Unpack a framed pop answer into the object the JSON answer parses to:
 *
 *   { success, queue, partition, partitionId, leaseId, consumerGroup,
 *     messages: [{ id, transactionId, traceId, data, producerSub, createdAt,
 *                  partitionId, partition, leaseId, consumerGroup,
 *                  deliveryAttempt, offset, headers? }],
 *     partitionsClaimed, ...the answer's own keys }
 *
 * The messages are in delivery order. Everything that reads a pop answer
 * (the conflation check, the autopilot's choice, the consumer loop) reads
 * this object as it reads the JSON one.
 *
 * @param {Uint8Array} bytes
 * @returns {Promise<object>}
 */
export async function decodePopFrames(bytes) {
  if (!isPopFrames(bytes)) throw damaged('not a framed pop answer')
  const input = new In(bytes)
  input.take(MAGIC.length)
  const version = input.u8()
  let own = null
  if (version === 1) {
    const text = utf8.decode(input.take(input.u32()))
    if (text.length > 0) {
      try {
        own = JSON.parse(text)
      } catch (error) {
        throw damaged(`its own keys: ${error.message}`)
      }
    }
  } else if (version !== 0) {
    throw damaged(`unknown version ${version}`)
  }

  const queue = input.text()
  const leaseId = input.text()
  const consumerGroup = input.text()
  const claims = input.u32()
  const messages = []
  let firstPartition = ''
  let firstPartitionId = ''

  for (let c = 0; c < claims; c++) {
    const partitionId = input.u64().toString()
    const partition = input.text()
    const start = input.u64()
    const end = input.u64()
    const deliveryAttempt = input.u32()
    const records = input.u32()
    if (c === 0) {
      firstPartition = partition
      firstPartitionId = partitionId
    }
    for (let r = 0; r < records; r++) {
      const base = input.u64()
      const createdAt = new Date(Number(input.i64() / 1000n)).toISOString()
      const count = input.u32()
      const codec = input.u8()
      let raw = input.take(input.u32())
      if (codec === 1) {
        try {
          raw = await zstdDecompress(raw)
        } catch (error) {
          throw damaged(`zstd: ${error.message}`)
        }
      } else if (codec !== 0) {
        throw damaged(`unknown codec ${codec}`)
      }
      const frames = new In(raw, `a record is shorter than its ${count} frames`)
      for (let i = 0; i < count; i++) {
        const frame = new In(frames.take(frames.u32()), 'a frame is malformed')
        const flags = frame.u8()
        const id = frame.take(16)
        const trace = flags & FLAG_TRACE ? frame.take(16) : null
        const transactionId = frame.text()
        const producerSub = flags & FLAG_PRODUCER ? frame.text() : null
        const headers = flags & FLAG_HEADERS ? decodeHeaders(frame.take(frame.u16())) : null
        if (flags & FLAG_ENCRYPTED) throw damaged('a frame is encrypted')
        if (flags & FLAG_LINK) throw damaged('a frame is a link')
        const offset = base + BigInt(i)
        if (offset < start || offset > end) continue
        const payload = frame.rest()
        const message = {
          id: uuidText(id),
          transactionId,
          traceId: trace ? uuidText(trace) : null,
          data: payload.length === 0 ? null : JSON.parse(utf8.decode(payload)),
          producerSub,
          createdAt,
          partitionId,
          partition,
          leaseId,
          consumerGroup,
          deliveryAttempt,
          offset: Number(offset)
        }
        // Only when there are some, as in the JSON answer.
        if (headers) message.headers = headers
        messages.push(message)
      }
    }
  }

  const answer = {
    success: true,
    queue,
    partition: firstPartition,
    partitionId: firstPartitionId,
    leaseId,
    consumerGroup,
    messages,
    partitionsClaimed: claims
  }
  if (own && typeof own === 'object' && !Array.isArray(own)) {
    // `frames` only says the answer is this one; the JSON answer has no such key.
    const { frames: _frames, ...keys } = own
    Object.assign(answer, keys)
  }
  return answer
}
