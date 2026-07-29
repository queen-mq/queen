#!/usr/bin/env node
/**
 * sdk-smoke (JavaScript) -- the REAL published client (clients/client-js,
 * package `queen-mq`, entry point client-v2/index.js) driven against a live
 * queen_proxy + broker. No canned HTTP server anywhere in this file.
 *
 * One phase per invocation (argv[1]); the driver (../run.sh) owns the cell and
 * the control plane, this program owns the SDK. Output mirrors
 * scripts/isolation-smoke.sh: "  ok  - desc" / "  FAIL- desc", exit 1 on any
 * failure so the driver can score a language per phase.
 */
import { Queen } from '../../../../clients/client-js/client-v2/index.js'

const PHASE = process.argv[2] || ''

// A queen_proxy deployment selects the cluster from the Host header, and there
// are two ways a client can produce one:
//   (a) point the base URL at the cluster's own hostname (https://two.<zone>) --
//       what production DNS gives every customer;
//   (b) keep one base URL and set `hostHeader: '<slug>'` -- the cell address is
//       dialled while the request advertises the cluster, which is what makes
//       this runnable against 127.0.0.1 with no wildcard DNS.
// This harness uses (b). `headers: { Host }` also works (all three clients map
// it onto the supported mechanism); the isolation phase asserts that parity,
// since Node's fetch would otherwise drop the header silently.

function env(name, dflt) {
  const v = process.env[name] ?? dflt
  if (v === undefined) {
    console.error(`sdk-smoke(js): missing env ${name}`)
    process.exit(2)
  }
  return v
}

const URL_ = env('SDK_SMOKE_URL')
const HOST_A = env('SDK_SMOKE_HOST_A')
const KEY_A = env('SDK_SMOKE_KEY_A')
const HOST_B = env('SDK_SMOKE_HOST_B')
const KEY_B = env('SDK_SMOKE_KEY_B')
const QUEUE = env('SDK_SMOKE_QUEUE')
const ISO_QUEUE = env('SDK_SMOKE_ISO_QUEUE')
const RUN_ID = env('SDK_SMOKE_RUN_ID')
const DEADLINE_MS = Number(env('SDK_SMOKE_DEADLINE_MS', '150000'))
const BURN_MAX = Number(env('SDK_SMOKE_BURN_MAX', '400'))
const RECOVER_N = Number(env('SDK_SMOKE_RECOVER_N', '20'))

let PASS = 0
let FAIL = 0
const ok = (d) => { PASS++; console.log(`  ok  - ${d}`) }
const bad = (d) => { FAIL++; console.log(`  FAIL- ${d}`) }
const check = (desc, cond, detail) => cond ? ok(desc) : bad(detail ? `${desc} (${detail})` : desc)
const sleep = (ms) => new Promise(r => setTimeout(r, ms))

/** A client wired the way a customer would wire one at a proxy: the cell's
 *  address plus the cluster's advertised hostname, which is what selects the
 *  tenant. `hostHeader` keeps the socket on the cell while the request
 *  advertises the cluster — the same shape as curl --resolve, and the reason
 *  this needs no wildcard DNS locally. */
function client(slug, key, extra = {}) {
  return new Queen({
    url: URL_,
    hostHeader: slug,
    bearerToken: key,
    handleSignals: false,
    ...extra,
  })
}

// --------------------------------------------------------------------------
// 1. push -> pop -> ack round trip through the proxy, api-key auth
// --------------------------------------------------------------------------
async function roundtrip() {
  const q = client(HOST_A, KEY_A)
  try {
    const res = await q.queue(QUEUE).push([
      { n: 1, run: RUN_ID }, { n: 2, run: RUN_ID }, { n: 3, run: RUN_ID },
    ])
    check('push 3 items accepted through the proxy',
      Array.isArray(res) && res.length === 3 && res.every(r => r.status === 'queued'),
      JSON.stringify(res))

    const msgs = await q.queue(QUEUE).batch(10).wait(false).pop()
    const ns = msgs.map(m => m.data && m.data.n).sort()
    check('pop returns the 3 pushed messages',
      msgs.length === 3 && JSON.stringify(ns) === '[1,2,3]',
      `got ${msgs.length}: ${JSON.stringify(ns)}`)
    check('messages carry partitionId + transactionId',
      msgs.length > 0 && msgs.every(m => m.partitionId && m.transactionId))

    await q.ack(msgs, true)
    const after = await q.queue(QUEUE).batch(10).wait(false).pop()
    check('queue drained after ack', after.length === 0, `got ${after.length}`)
  } finally {
    await q.close()
  }
}

// --------------------------------------------------------------------------
// 2. tenant isolation with the real client: same queue name, two clusters
// --------------------------------------------------------------------------
async function isolation() {
  const a = client(HOST_A, KEY_A)
  const b = client(HOST_B, KEY_B)
  const foreign = client(HOST_B, KEY_A) // A's key presented on B's Host
  try {
    const ra = await a.queue(ISO_QUEUE).push([{ who: 'A', run: RUN_ID }])
    const rb = await b.queue(ISO_QUEUE).push([{ who: 'B', run: RUN_ID }])
    check('both clusters accept a push to the same queue name',
      ra[0].status === 'queued' && rb[0].status === 'queued',
      `${ra[0].status}/${rb[0].status}`)

    const ma = await a.queue(ISO_QUEUE).batch(10).wait(false).pop()
    const mb = await b.queue(ISO_QUEUE).batch(10).wait(false).pop()
    check('cluster A sees only its own message',
      ma.length === 1 && ma[0].data.who === 'A',
      `${ma.length} msg(s): ${JSON.stringify(ma.map(m => m.data))}`)
    check('cluster B sees only its own message',
      mb.length === 1 && mb[0].data.who === 'B',
      `${mb.length} msg(s): ${JSON.stringify(mb.map(m => m.data))}`)

    let crossErr = null
    try {
      await foreign.queue(ISO_QUEUE).push([{ who: 'X', run: RUN_ID }])
    } catch (e) {
      crossErr = e
    }
    check('cluster A key on cluster B host -> 403 forbidden',
      crossErr !== null && crossErr.status === 403 && crossErr.code === 'forbidden',
      crossErr ? `${crossErr.status}/${crossErr.code}` : 'push SUCCEEDED')

    // Cross-language parity on the other documented way to select a cluster:
    // one base URL + a Host header. Go maps it onto req.Host, httpx sends it
    // verbatim, and the JS client maps it onto hostHeader (Node's fetch would
    // otherwise drop it silently and land the request on the wrong cluster).
    // Kept as an assertion because a silent drop here is a wrong-tenant bug.
    const byHeader = new Queen({
      url: URL_, bearerToken: KEY_B, headers: { Host: HOST_B }, handleSignals: false,
    })
    let hdrRes = null
    let hdrErr = null
    try {
      hdrRes = await byHeader.queue(ISO_QUEUE).push([{ who: 'B-hdr', run: RUN_ID }])
    } catch (e) {
      hdrErr = e
    } finally {
      await byHeader.close()
    }
    check("headers:{ Host } selects the cluster (parity with Go/Python)",
      hdrRes !== null && hdrRes[0] && hdrRes[0].status === 'queued',
      hdrErr ? `${hdrErr.status}/${hdrErr.code}: ${hdrErr.message}` : 'no response')

    await a.ack(ma, true)
    await b.ack(mb, true)
    // The parity push above landed a real message on B (it is only skippable
    // when the header mechanism is broken), so B has one more to drain than A.
    const extra = await b.queue(ISO_QUEUE).batch(10).wait(false).pop()
    check('the Host-header push landed on B, not A',
      extra.length === 1 && extra[0].data.who === 'B-hdr',
      `${extra.length} msg(s): ${JSON.stringify(extra.map(m => m.data))}`)
    await b.ack(extra, true)

    const ea = await a.queue(ISO_QUEUE).batch(10).wait(false).pop()
    const eb = await b.queue(ISO_QUEUE).batch(10).wait(false).pop()
    check('both clusters drained independently after ack',
      ea.length === 0 && eb.length === 0, `${ea.length}/${eb.length}`)
  } finally {
    await Promise.all([a.close(), b.close(), foreign.close()])
  }
}

// --------------------------------------------------------------------------
// 3. live 429 from the real limiter + transparent recovery
// --------------------------------------------------------------------------
async function ratelimit() {
  // (a) a client with 429 backoff DISABLED, so the live 429 surfaces as an
  //     error we can inspect (code + Retry-After) instead of being absorbed.
  const strict = client(HOST_A, KEY_A, { retry429: { maxAttempts: 1 } })
  let hit = null
  let sent = 0
  try {
    for (let i = 0; i < BURN_MAX && !hit; i++) {
      try {
        await strict.queue(QUEUE).push([{ burn: i, run: RUN_ID }])
        sent++
      } catch (e) {
        if (e.status === 429) { hit = e; break }
        throw e
      }
    }
  } finally {
    await strict.close()
  }
  check('the real limiter returned a live 429 to the SDK', hit !== null,
    `no 429 after ${sent} pushes`)
  check('429 body carries code=rate_limited', hit !== null && hit.code === 'rate_limited',
    hit ? String(hit.code) : 'n/a')
  check('429 carries a Retry-After the client parsed',
    hit !== null && typeof hit.retryAfterSeconds === 'number' && hit.retryAfterSeconds >= 1,
    hit ? String(hit.retryAfterSeconds) : 'n/a')

  // (b) the same traffic through a stock client (default 429 policy): the
  //     bucket is empty, so every one of these has to be paced by the
  //     client's own Retry-After backoff -- and all of them must still land.
  const normal = client(HOST_A, KEY_A)
  const t0 = Date.now()
  let done = 0
  try {
    for (let i = 0; i < RECOVER_N; i++) {
      const r = await normal.queue(QUEUE).push([{ recover: i, run: RUN_ID }])
      if (r[0] && r[0].status === 'queued') done++
    }
  } finally {
    await normal.close()
  }
  const elapsed = Date.now() - t0
  check(`stock client completed all ${RECOVER_N} pushes against an empty bucket`,
    done === RECOVER_N, `${done}/${RECOVER_N}`)
  check('the run was paced by backoff, not served instantly', elapsed >= 1500, `${elapsed}ms`)
}

// --------------------------------------------------------------------------
// 4a. terminal 403: storage quota tripped by the driver's limit override
// --------------------------------------------------------------------------
async function blocked() {
  const q = client(HOST_A, KEY_A)
  const deadline = Date.now() + DEADLINE_MS
  let terminal = null
  let callMs = 0
  try {
    while (Date.now() < deadline && terminal === null) {
      const t = Date.now()
      try {
        await q.queue(QUEUE).push([{ probe: Date.now(), run: RUN_ID }])
      } catch (e) {
        callMs = Date.now() - t
        if (e.status === 403) { terminal = e; break }
        throw e
      }
      await sleep(2000)
    }
    check('push eventually rejected with a terminal 403', terminal !== null,
      `still accepted after ${Math.round(DEADLINE_MS / 1000)}s`)
    check('terminal code is storage_quota_exceeded',
      terminal !== null && terminal.code === 'storage_quota_exceeded',
      terminal ? String(terminal.code) : 'n/a')
    check('terminal 403 surfaced immediately (not retried with backoff)',
      terminal !== null && callMs < 2000, `${callMs}ms`)

    // consume must stay open while pushes are blocked: the rate-limit phase
    // left plenty of un-popped messages on this queue, so an empty result
    // here would mean the read path was blocked too.
    const msgs = await q.queue(QUEUE).batch(1).wait(false).pop()
    check('consume still allowed while push-blocked', msgs.length >= 1, `${msgs.length} msg(s)`)
    if (msgs.length) await q.ack(msgs, true)
  } finally {
    await q.close()
  }
}

// --------------------------------------------------------------------------
// 4b. recovery once the driver clears the override
// --------------------------------------------------------------------------
async function unblocked() {
  const q = client(HOST_A, KEY_A)
  const deadline = Date.now() + DEADLINE_MS
  let released = false
  let lastCode = null
  try {
    while (Date.now() < deadline && !released) {
      try {
        const r = await q.queue(QUEUE).push([{ release: Date.now(), run: RUN_ID }])
        released = Array.isArray(r) && r[0] && r[0].status === 'queued'
      } catch (e) {
        if (e.status !== 403) throw e
        lastCode = e.code
      }
      if (!released) await sleep(2000)
    }
    check('push accepted again after the override is cleared', released,
      `still ${lastCode} after ${Math.round(DEADLINE_MS / 1000)}s`)
  } finally {
    await q.close()
  }
}

const PHASES = { roundtrip, isolation, ratelimit, blocked, unblocked }

const fn = PHASES[PHASE]
if (!fn) {
  console.error(`sdk-smoke(js): unknown phase '${PHASE}' (want: ${Object.keys(PHASES).join('|')})`)
  process.exit(2)
}

try {
  await fn()
} catch (e) {
  bad(`phase threw: ${e && e.message} (status=${e && e.status} code=${e && e.code})`)
}
console.log(`  -- js/${PHASE}: ${PASS} ok, ${FAIL} fail`)
process.exit(FAIL === 0 ? 0 : 1)
