// Tests for server-stamped producer identity (issue #23, feature A).
//
// The goal of feature A is: when JWT auth is enabled, the server attaches the
// authenticated client's `sub` claim to every pushed message, and a client
// cannot spoof another producer's identity by supplying `producerSub` in the
// request body.
//
// Three environments are exercised here:
//
//   1. No auth (default dev setup): `producer_sub` must be NULL; clients that
//      send `producerSub` in the body must still get NULL (the field is not
//      client-settable).
//   2. JWT auth enabled (set JWT_SECRET env to match the server's
//      JWT_SECRET): `producer_sub` must equal the `sub` claim.
//   3. JWT auth enabled + spoofing attempt: body-supplied `producerSub` must
//      be ignored; stored value is the JWT `sub`.
//
// Tests gate themselves on the `JWT_SECRET` env var so the suite can run
// unattended in either server configuration.

import crypto from 'crypto'

const SERVER_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const JWT_SECRET = process.env.JWT_SECRET || ''
const JWT_ENABLED = JWT_SECRET.length > 0

// ---------------------------------------------------------------------------
// Minimal HS256 JWT signer (avoids pulling a dependency just for tests)
// ---------------------------------------------------------------------------
function b64url(buf) {
    return Buffer.from(buf).toString('base64')
        .replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_')
}

function signHs256Jwt(payload, secret) {
    const header = { alg: 'HS256', typ: 'JWT' }
    const encHeader = b64url(JSON.stringify(header))
    const encPayload = b64url(JSON.stringify(payload))
    const signingInput = `${encHeader}.${encPayload}`
    const sig = crypto.createHmac('sha256', secret).update(signingInput).digest()
    return `${signingInput}.${b64url(sig)}`
}

function makeToken(sub, role = 'read-write') {
    const now = Math.floor(Date.now() / 1000)
    return signHs256Jwt(
        { sub, username: sub, role, iat: now, exp: now + 3600 },
        JWT_SECRET
    )
}

// Raw HTTP push so we can control the Authorization header and inject
// arbitrary extra fields (like a forged `producerSub`) per-test.
async function httpPush({ queue, partition = 'Default', transactionId, data, bearerToken, extraItemFields = {} }) {
    const headers = { 'Content-Type': 'application/json' }
    if (bearerToken) headers['Authorization'] = `Bearer ${bearerToken}`

    const body = {
        items: [{
            queue,
            partition,
            transactionId,
            payload: data,
            ...extraItemFields
        }]
    }

    const res = await fetch(`${SERVER_URL}/api/v1/push`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body)
    })
    const text = await res.text()
    if (!res.ok) {
        throw new Error(`push failed ${res.status}: ${text}`)
    }
    return text ? JSON.parse(text) : null
}

// Raw HTTP configure (queue creation) with optional bearer.
async function httpConfigure(queue, bearerToken) {
    const headers = { 'Content-Type': 'application/json' }
    if (bearerToken) headers['Authorization'] = `Bearer ${bearerToken}`
    const res = await fetch(`${SERVER_URL}/api/v1/configure`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ queue, options: {} })
    })
    if (!res.ok) {
        const text = await res.text()
        throw new Error(`configure failed ${res.status}: ${text}`)
    }
}

// Black-box observation for the HTTP push tests: pop the message back through
// the API and read its `producerSub`. The segments engine stores messages in
// queen.seg_segments (never queen.messages), so a seg-broker push is only
// observable via pop — the seg-native ground truth. Optional bearer so the
// JWT-enabled pops are authorized.
async function httpPop({ queue, bearerToken, batch = 20, waitMs = 5000 }) {
    const headers = {}
    if (bearerToken) headers['Authorization'] = `Bearer ${bearerToken}`
    const url = `${SERVER_URL}/api/v1/pop/queue/${encodeURIComponent(queue)}`
        + `?batch=${batch}&wait=true&timeout=${waitMs}&autoAck=true`
    const res = await fetch(url, { method: 'GET', headers })
    if (res.status === 204) return []
    const text = await res.text()
    if (!res.ok) {
        throw new Error(`pop failed ${res.status}: ${text}`)
    }
    const body = text ? JSON.parse(text) : {}
    return (body.messages || []).filter(m => m != null)
}

// Pop until we observe the message with the given transactionId; returns its
// producerSub (a seg push is immediately visible via fire-on-idle, but retry a
// few times to be robust to any push→visibility race).
async function poppedProducerSub(queue, transactionId, bearerToken) {
    const deadline = Date.now() + 6000
    while (Date.now() < deadline) {
        const msgs = await httpPop({ queue, bearerToken })
        const hit = msgs.find(m => m.transactionId === transactionId)
        if (hit) return { found: true, producerSub: hit.producerSub }
        await new Promise(r => setTimeout(r, 150))
    }
    return { found: false, producerSub: undefined }
}

// ===========================================================================
// (Retired) SP-level producer_sub round-trip / NULL / empty-string tests.
// These drove the ROWS engine directly (queen.push_messages_v3 +
// queen.pop_specific_batch + a queen.messages read) to verify the SP's
// producer_sub NULLIF handling. The rows engine is retired (segments-only), so
// they were removed — the segments engine stores payloads in queen.seg_segments
// and stamps producer_sub in the broker, never in queen.messages. The shipping
// producer_sub behavior (server-stamped, body-ignored without auth, spoof-proof
// under JWT) is covered black-box through the HTTP push -> pop path by the tests
// below.
// ===========================================================================

// ===========================================================================
// TEST 4: HTTP push without JWT => producer_sub is NULL
// ---------------------------------------------------------------------------
// Runs only when the server is NOT configured with JWT (typical dev setup).
// Also checks that a client sending `producerSub` in the push body does NOT
// end up with it persisted (the field is not client-settable under any mode).
// ===========================================================================
export async function producerSubIgnoredFromBodyWithoutAuth(client) {
    if (JWT_ENABLED) {
        return { success: true, message: 'Skipped (JWT_SECRET is set - run producerSubStampedFromJwt instead)' }
    }

    const q = 'test-auth-http-no-jwt'
    const tx = `tx-noauth-${Date.now()}`

    const queue = await client.queue(q).create()
    if (!queue.configured) {
        return { success: false, message: 'Queue not created' }
    }

    // Spoofing attempt against an unauthenticated server.
    await httpPush({
        queue: q,
        transactionId: tx,
        data: { hello: 'world' },
        extraItemFields: { producerSub: 'attacker-no-jwt' }
    })

    // Black-box: observe the pushed message via pop. producerSub must be null
    // (the field is not client-settable, and there is no JWT sub to stamp).
    const { found, producerSub } = await poppedProducerSub(q, tx)
    if (!found) {
        return { success: false, message: `Pushed message ${tx} not observed via pop` }
    }
    if (producerSub !== null) {
        return { success: false, message: `Expected producerSub null (auth disabled), got ${JSON.stringify(producerSub)} - client was able to set it!` }
    }

    return { success: true, message: 'Body-supplied producerSub ignored when auth disabled; pop shows null' }
}

// ===========================================================================
// TEST 5: HTTP push WITH JWT => producer_sub = sub claim
// ---------------------------------------------------------------------------
// Requires:
//   - Server started with JWT_ENABLED=true JWT_ALGORITHM=HS256 JWT_SECRET=<x>
//   - Test run with JWT_SECRET=<x>
// ===========================================================================
export async function producerSubStampedFromJwt(client) {
    if (!JWT_ENABLED) {
        return { success: true, message: 'Skipped (set JWT_SECRET env var matching server to run)' }
    }

    const q = 'test-auth-http-jwt-stamp'
    const tx = `tx-jwt-${Date.now()}`
    const token = makeToken('alice-producer')

    await httpConfigure(q, token)
    await httpPush({
        queue: q,
        transactionId: tx,
        data: { hello: 'world' },
        bearerToken: token
    })

    // Black-box: pop the message back and read producerSub (the seg-native
    // ground truth). It must equal the authenticated JWT sub.
    const { found, producerSub } = await poppedProducerSub(q, tx, token)
    if (!found) {
        return { success: false, message: `Pushed message ${tx} not observed via pop` }
    }
    if (producerSub !== 'alice-producer') {
        return { success: false, message: `Expected producerSub='alice-producer', got ${JSON.stringify(producerSub)}` }
    }

    return { success: true, message: 'producer_sub stamped from authenticated JWT sub claim (observed via pop)' }
}

// ===========================================================================
// TEST 6: Spoofing producerSub in body is ignored even with valid JWT
// ---------------------------------------------------------------------------
// This is the core anti-impersonation invariant of issue #23.
// ===========================================================================
export async function producerSubSpoofingIgnoredWithJwt(client) {
    if (!JWT_ENABLED) {
        return { success: true, message: 'Skipped (set JWT_SECRET env var matching server to run)' }
    }

    const q = 'test-auth-http-jwt-spoof'
    const tx = `tx-spoof-${Date.now()}`
    const token = makeToken('legit-producer')

    await httpConfigure(q, token)
    await httpPush({
        queue: q,
        transactionId: tx,
        data: { hello: 'world' },
        bearerToken: token,
        extraItemFields: { producerSub: 'attacker' }
    })

    // Black-box: pop the message back; the body-supplied producerSub must be
    // ignored and the observed value must be the validated JWT sub.
    const { found, producerSub } = await poppedProducerSub(q, tx, token)
    if (!found) {
        return { success: false, message: `Pushed message ${tx} not observed via pop` }
    }
    if (producerSub !== 'legit-producer') {
        return {
            success: false,
            message: `Impersonation not prevented: observed producerSub=${JSON.stringify(producerSub)}, expected 'legit-producer'`
        }
    }

    return { success: true, message: 'Body-supplied producerSub ignored; observed sub is from validated JWT' }
}
