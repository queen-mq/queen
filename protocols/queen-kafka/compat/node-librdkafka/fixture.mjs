// The dataset and the scoreboard both Node suites in this directory share.
//
// There are two clients here (node-rdkafka and @confluentinc/kafka-javascript)
// and they are the same librdkafka underneath, so the only honest way to compare
// them is to make them carry byte-identical payloads through byte-identical
// assertions. That is all this file is: the records, the checks, and the
// `  ok  ` / `  FAIL ` line format the rest of compat/ uses.
//
// The payloads are deliberately NOT strings. Every value carries a NUL, a 0xff
// and a 0xfe, so a client (or a facade) that round-trips through UTF-8 anywhere
// corrupts them visibly instead of silently — a string comparison would have
// passed on a lossy path.

import fs from 'node:fs'

export const PARTITIONS = 8
export const COUNT = 512

/**
 * Record i of the fixture: a key, a binary value, and headers.
 *
 * `binaryHeaders` is a CLIENT capability, not a facade one, and the difference
 * between the two bindings here is exactly it: @confluentinc/kafka-javascript
 * passes a Buffer header value through as bytes, node-rdkafka coerces every
 * header value to a UTF-8 string and truncates it at the first NUL
 * (node_modules/node-rdkafka/src/producer.cc, the `Nan::To<v8::String>` on the
 * value). probe-headers.mjs proves which side loses the bytes; the suites just
 * ask for what their binding can carry, so a node-rdkafka run is not polluted
 * by a defect that is not the facade's.
 */
export function record(i, tag, { binaryHeaders = true } = {}) {
  return {
    i,
    partition: i % PARTITIONS,
    key: Buffer.from(`${tag}-key-${String(i).padStart(4, '0')}`),
    // `\0 \xff \xfe <i>` after the readable prefix: the bytes a UTF-8 round
    // trip cannot survive. Both bindings DO carry these in key and value —
    // only the header path differs.
    value: Buffer.concat([
      Buffer.from(`${tag}-val-${String(i).padStart(4, '0')}-`),
      Buffer.from([0x00, 0xff, 0xfe, i & 0xff]),
    ]),
    headers: [
      { name: 'trace', value: Buffer.from(`h${i}`) },
      ...(binaryHeaders ? [{ name: 'bin', value: Buffer.from([0x00, 0x01, 0xfe, 0xff]) }] : []),
      { name: 'empty', value: Buffer.alloc(0) },
    ],
  }
}

export function fixture(tag, count = COUNT, opts = {}) {
  return Array.from({ length: count }, (_, i) => record(i, tag, opts))
}

// ------------------------------------------------------------------ scoreboard
export const failures = []

export const ok = (msg) => console.log(`  ok   ${msg}`)
export const info = (msg) => console.log(`  ..   ${msg}`)
export const say = (msg) => console.log(`\n=== ${msg}`)

export function bad(msg) {
  console.log(`  FAIL ${msg}`)
  failures.push(msg)
}

export function check(cond, msg) {
  if (cond) ok(msg)
  else bad(msg)
  return !!cond
}

export function finish(what) {
  console.log(
    `\nRESULT: ${failures.length === 0 ? 'PASS' : `FAIL (${failures.length})`}` +
      `  [${what}]`,
  )
  if (failures.length) for (const f of failures) console.log(`  - ${f}`)
  return failures.length === 0 ? 0 : 1
}

// ------------------------------------------------------------------- deadlines
// A hang is a result. Every blocking call in both suites goes through one of
// these, so a client that never returns is a FAIL with a name rather than a
// wedged process someone has to kill.
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

export function deadline(promise, ms, what) {
  let timer
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`TIMEOUT after ${ms}ms: ${what}`)), ms)
    }),
  ]).finally(() => clearTimeout(timer))
}

export async function waitFor(pred, what, timeoutMs = 90000, everyMs = 200) {
  const until = Date.now() + timeoutMs
  while (Date.now() < until) {
    if (await pred()) return true
    await sleep(everyMs)
  }
  throw new Error(`TIMEOUT after ${timeoutMs}ms waiting for ${what}`)
}

// ------------------------------------------------- librdkafka's own debug trace
// Both clients are librdkafka, so both can be asked what they NEGOTIATED rather
// than told what to expect: `debug=protocol` prints one line per request and the
// version is in it. This collects them; nothing here assumes a version.
const SENT = /Sent (\w+)Request \(v(\d+)/
const RECV = /Received (\w+)Response \(v(\d+)/

// librdkafka narrates its own shutdown in the vocabulary of failure — "Handle
// is terminating", "no next broker, failing 0 message(s)" — and none of it means
// anything went wrong. Filtering it here is what keeps the interesting lines
// (a declined codec, a refused credential, an unsupported API) readable.
const BENIGN =
  /Handle is terminating|failing 0 message|changed state|Broker handle destroyed|terminating in state|Purging|Removing|no next broker, failing 0/i

export class Negotiated {
  constructor() {
    this.byApi = new Map()
    this.errors = []
    // Set NEGOTIATED_TRACE_FILE to keep the whole librdkafka debug stream; the
    // suites print a summary, and this is where you go when the summary is not
    // enough. (compat/librdkafka/confluent_group.py keeps the same habit.)
    this.rawPath = process.env.NEGOTIATED_TRACE_FILE || null
    this.raw = this.rawPath ? [] : null
  }

  /** Feed one librdkafka log line (the `message` field of an event.log). */
  line(text) {
    if (!text) return
    if (this.raw) this.raw.push(text)
    for (const re of [SENT, RECV]) {
      const m = re.exec(text)
      if (m) {
        const [, api, v] = m
        if (!this.byApi.has(api)) this.byApi.set(api, new Set())
        this.byApi.get(api).add(Number(v))
      }
    }
    // "does not support" is in here on purpose: it is how librdkafka announces
    // that it is DOWNGRADING itself against this broker — the zstd fallback the
    // Fetch v6 cap causes is exactly this line, and it must never be filtered
    // away as chatter.
    if (!BENIGN.test(text) && /\b(error|fail|refused|unsupported|does not support|unknown)/i.test(text)) {
      this.errors.push(text.trim().slice(0, 300))
    }
  }

  /**
   * Every raw line matching `re`. Needs NEGOTIATED_TRACE_FILE to be set (that is
   * what turns raw retention on); without it this is empty and the suites say
   * so rather than pretending librdkafka was silent.
   */
  linesMatching(re) {
    return (this.raw || []).filter((l) => re.test(l))
  }

  /** Every line that mentions compression — the codec conversation, verbatim. */
  compressionLines() {
    return this.linesMatching(/compress/i)
  }

  flush() {
    if (this.rawPath && this.raw) {
      fs.writeFileSync(this.rawPath, this.raw.join('\n'))
      info(`full librdkafka trace: ${this.rawPath} (${this.raw.length} lines)`)
    }
  }

  report() {
    say('API versions this client actually negotiated (from librdkafka debug=protocol)')
    if (this.byApi.size === 0) {
      info('no protocol lines captured — the debug stream did not reach this suite')
      return
    }
    for (const api of [...this.byApi.keys()].sort()) {
      const vs = [...this.byApi.get(api)].sort((a, b) => a - b)
      console.log(`  ${api.padEnd(18)} v${vs.join(', v')}`)
    }
  }

  reportErrors(limit = 12) {
    say(`what librdkafka called an error or unsupported (${this.errors.length} lines, first ${limit})`)
    if (!this.errors.length) {
      info('none')
      return
    }
    for (const e of this.errors.slice(0, limit)) console.log(`  ${e}`)
  }
}

// ------------------------------------------------------------------ assertions
/**
 * The shared verdict on a consumed set: count, per-partition order, and a
 * byte-exact comparison of key, value and headers against the fixture.
 *
 * `seen` is [{partition, offset, key: Buffer, value: Buffer, headers: [{name,value}]}].
 */
export function verifyRoundTrip(seen, expected, label) {
  const byKey = new Map(expected.map((r) => [r.key.toString('binary'), r]))
  check(seen.length === expected.length, `${label}: read exactly ${expected.length} records (got ${seen.length})`)

  // per-partition: offsets strictly increasing, and the fixture's own order kept
  let orderBroken = null
  let offsetsBroken = null
  const perPartition = new Map()
  for (const m of seen) {
    if (!perPartition.has(m.partition)) perPartition.set(m.partition, [])
    perPartition.get(m.partition).push(m)
  }
  for (const [p, ms] of perPartition) {
    for (let i = 1; i < ms.length; i++) {
      if (!(ms[i].offset > ms[i - 1].offset)) {
        offsetsBroken ??= `p${p}: offset ${ms[i].offset} follows ${ms[i - 1].offset}`
      }
    }
    const wantIdx = expected.filter((r) => r.partition === p).map((r) => r.i)
    const gotIdx = ms.map((m) => byKey.get(m.key.toString('binary'))?.i)
    if (JSON.stringify(wantIdx) !== JSON.stringify(gotIdx)) {
      orderBroken ??= `p${p}: wanted ${wantIdx.slice(0, 5)}… got ${gotIdx.slice(0, 5)}…`
    }
  }
  check(
    perPartition.size >= 4,
    `${label}: records spread over ${perPartition.size} partitions (>= 4 required)`,
  )
  check(offsetsBroken === null, `${label}: offsets strictly increase inside every partition (${offsetsBroken ?? 'all'})`)
  check(orderBroken === null, `${label}: produce order preserved inside every partition (${orderBroken ?? 'all'})`)

  // byte-exact key / value / headers
  let badValue = null
  let badHeaders = null
  let badPartition = null
  let matched = 0
  for (const m of seen) {
    const want = byKey.get(m.key.toString('binary'))
    if (!want) {
      badValue ??= `key ${JSON.stringify(m.key.toString())} is not in the fixture`
      continue
    }
    matched++
    if (!want.value.equals(m.value)) {
      badValue ??= `${want.key}: value ${m.value.toString('hex')} != ${want.value.toString('hex')}`
    }
    if (want.partition !== m.partition) {
      badPartition ??= `${want.key}: landed on p${m.partition}, was produced to p${want.partition}`
    }
    const got = (m.headers || []).map((h) => `${h.name}=${Buffer.from(h.value ?? []).toString('hex')}`)
    const wnt = want.headers.map((h) => `${h.name}=${h.value.toString('hex')}`)
    if (JSON.stringify(got) !== JSON.stringify(wnt)) {
      badHeaders ??= `${want.key}: headers ${JSON.stringify(got)} != ${JSON.stringify(wnt)}`
    }
  }
  check(matched === expected.length, `${label}: every record matched a fixture key (${matched}/${expected.length})`)
  check(badValue === null, `${label}: every value is byte-exact, NULs and 0xff included (${badValue ?? 'all'})`)
  check(badPartition === null, `${label}: every record came back from the partition it was produced to (${badPartition ?? 'all'})`)
  check(
    badHeaders === null,
    `${label}: headers round-trip in order with values byte-exact (${badHeaders ?? 'all'})`,
  )
}
