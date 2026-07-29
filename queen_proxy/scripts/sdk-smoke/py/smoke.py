#!/usr/bin/env python3
"""sdk-smoke (Python) -- the REAL published client (clients/client-py, package
`queen-mq`) driven against a live queen_proxy + broker. No MockTransport, no
canned server: every request in this file leaves the process.

One phase per invocation (argv[1]); the driver (../run.sh) owns the cell and
the control plane, this program owns the SDK. Output mirrors
scripts/isolation-smoke.sh: "  ok  - desc" / "  FAIL- desc", exit 1 on any
failure so the driver can score a language per phase.
"""

import asyncio
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# ../../../../clients/client-py -- import the checked-out client by path, the
# same source a `pip install queen-mq` would ship. No publishing step.
CLIENT_PY = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", "clients", "client-py"))
sys.path.insert(0, CLIENT_PY)

import httpx  # noqa: E402  (the client's own HTTP dependency)
from queen import Queen  # noqa: E402


def env(name, default=None):
    v = os.environ.get(name, default)
    if v is None:
        print(f"sdk-smoke(py): missing env {name}", file=sys.stderr)
        sys.exit(2)
    return v


PHASE = sys.argv[1] if len(sys.argv) > 1 else ""
URL = env("SDK_SMOKE_URL")
HOST_A = env("SDK_SMOKE_HOST_A")
KEY_A = env("SDK_SMOKE_KEY_A")
HOST_B = env("SDK_SMOKE_HOST_B")
KEY_B = env("SDK_SMOKE_KEY_B")
QUEUE = env("SDK_SMOKE_QUEUE")
ISO_QUEUE = env("SDK_SMOKE_ISO_QUEUE")
RUN_ID = env("SDK_SMOKE_RUN_ID")
DEADLINE_S = float(env("SDK_SMOKE_DEADLINE_MS", "150000")) / 1000.0
BURN_MAX = int(env("SDK_SMOKE_BURN_MAX", "400"))
RECOVER_N = int(env("SDK_SMOKE_RECOVER_N", "20"))

STATE = {"pass": 0, "fail": 0}


def ok(desc):
    STATE["pass"] += 1
    print(f"  ok  - {desc}")


def bad(desc):
    STATE["fail"] += 1
    print(f"  FAIL- {desc}")


def check(desc, cond, detail=""):
    if cond:
        ok(desc)
    else:
        bad(f"{desc} ({detail})" if detail else desc)


def client(host, key, **extra):
    """A client wired exactly like a customer would wire one at a proxy: base
    URL, api key as bearer token, Host header selecting the cluster."""
    return Queen(url=URL, bearer_token=key, headers={"Host": host}, **extra)


def status_of(err):
    resp = getattr(err, "response", None)
    return getattr(resp, "status_code", None)


# ---------------------------------------------------------------------------
# 1. push -> pop -> ack round trip through the proxy, api-key auth
# ---------------------------------------------------------------------------
async def roundtrip():
    q = client(HOST_A, KEY_A)
    try:
        res = await q.queue(QUEUE).push(
            [{"n": 1, "run": RUN_ID}, {"n": 2, "run": RUN_ID}, {"n": 3, "run": RUN_ID}]
        )
        check(
            "push 3 items accepted through the proxy",
            isinstance(res, list) and len(res) == 3 and all(r.get("status") == "queued" for r in res),
            json.dumps(res)[:200],
        )

        msgs = await q.queue(QUEUE).batch(10).wait(False).pop()
        ns = sorted(m.get("data", {}).get("n") for m in msgs)
        check(
            "pop returns the 3 pushed messages",
            len(msgs) == 3 and ns == [1, 2, 3],
            f"got {len(msgs)}: {ns}",
        )
        check(
            "messages carry partitionId + transactionId",
            len(msgs) > 0 and all(m.get("partitionId") and m.get("transactionId") for m in msgs),
        )

        await q.ack(msgs, True)
        after = await q.queue(QUEUE).batch(10).wait(False).pop()
        check("queue drained after ack", len(after) == 0, f"got {len(after)}")
    finally:
        await q.close()


# ---------------------------------------------------------------------------
# 2. tenant isolation with the real client: same queue name, two clusters
# ---------------------------------------------------------------------------
async def isolation():
    a = client(HOST_A, KEY_A)
    b = client(HOST_B, KEY_B)
    foreign = client(HOST_B, KEY_A)  # A's key presented on B's Host
    try:
        ra = await a.queue(ISO_QUEUE).push([{"who": "A", "run": RUN_ID}])
        rb = await b.queue(ISO_QUEUE).push([{"who": "B", "run": RUN_ID}])
        check(
            "both clusters accept a push to the same queue name",
            ra[0].get("status") == "queued" and rb[0].get("status") == "queued",
            f"{ra[0].get('status')}/{rb[0].get('status')}",
        )

        ma = await a.queue(ISO_QUEUE).batch(10).wait(False).pop()
        mb = await b.queue(ISO_QUEUE).batch(10).wait(False).pop()
        check(
            "cluster A sees only its own message",
            len(ma) == 1 and ma[0]["data"]["who"] == "A",
            f"{len(ma)} msg(s): {[m.get('data') for m in ma]}",
        )
        check(
            "cluster B sees only its own message",
            len(mb) == 1 and mb[0]["data"]["who"] == "B",
            f"{len(mb)} msg(s): {[m.get('data') for m in mb]}",
        )

        cross = None
        try:
            await foreign.queue(ISO_QUEUE).push([{"who": "X", "run": RUN_ID}])
        except httpx.HTTPStatusError as e:
            cross = e
        check(
            "cluster A key on cluster B host -> 403 forbidden",
            cross is not None and status_of(cross) == 403 and getattr(cross, "code", None) == "forbidden",
            f"{status_of(cross)}/{getattr(cross, 'code', None)}" if cross else "push SUCCEEDED",
        )

        await a.ack(ma, True)
        await b.ack(mb, True)
        ea = await a.queue(ISO_QUEUE).batch(10).wait(False).pop()
        eb = await b.queue(ISO_QUEUE).batch(10).wait(False).pop()
        check(
            "both clusters drained independently after ack",
            len(ea) == 0 and len(eb) == 0,
            f"{len(ea)}/{len(eb)}",
        )
    finally:
        await a.close()
        await b.close()
        await foreign.close()


# ---------------------------------------------------------------------------
# 3. live 429 from the real limiter + transparent recovery
# ---------------------------------------------------------------------------
async def ratelimit():
    # (a) 429 backoff DISABLED, so the live 429 surfaces as an error we can
    #     inspect (code + Retry-After) instead of being absorbed.
    strict = client(HOST_A, KEY_A, retry_429={"max_attempts": 1})
    hit = None
    sent = 0
    try:
        for i in range(BURN_MAX):
            try:
                await strict.queue(QUEUE).push([{"burn": i, "run": RUN_ID}])
                sent += 1
            except httpx.HTTPStatusError as e:
                if status_of(e) == 429:
                    hit = e
                    break
                raise
    finally:
        await strict.close()

    check("the real limiter returned a live 429 to the SDK", hit is not None, f"no 429 after {sent} pushes")
    check(
        "429 body carries code=rate_limited",
        hit is not None and getattr(hit, "code", None) == "rate_limited",
        str(getattr(hit, "code", None)) if hit else "n/a",
    )
    ra = getattr(hit, "retry_after_seconds", None) if hit else None
    check(
        "429 carries a Retry-After the client parsed",
        isinstance(ra, float) and ra >= 1,
        str(ra),
    )

    # (b) the same traffic through a stock client (default 429 policy): the
    #     bucket is empty, so every one of these has to be paced by the
    #     client's own Retry-After backoff -- and all of them must still land.
    normal = client(HOST_A, KEY_A)
    t0 = time.monotonic()
    done = 0
    try:
        for i in range(RECOVER_N):
            r = await normal.queue(QUEUE).push([{"recover": i, "run": RUN_ID}])
            if r and r[0].get("status") == "queued":
                done += 1
    finally:
        await normal.close()
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    check(
        f"stock client completed all {RECOVER_N} pushes against an empty bucket",
        done == RECOVER_N,
        f"{done}/{RECOVER_N}",
    )
    check("the run was paced by backoff, not served instantly", elapsed_ms >= 1500, f"{elapsed_ms}ms")


# ---------------------------------------------------------------------------
# 4a. terminal 403: storage quota tripped by the driver's limit override
# ---------------------------------------------------------------------------
async def blocked():
    q = client(HOST_A, KEY_A)
    deadline = time.monotonic() + DEADLINE_S
    terminal = None
    call_ms = 0
    try:
        while time.monotonic() < deadline and terminal is None:
            t = time.monotonic()
            try:
                await q.queue(QUEUE).push([{"probe": time.time(), "run": RUN_ID}])
            except httpx.HTTPStatusError as e:
                call_ms = int((time.monotonic() - t) * 1000)
                if status_of(e) == 403:
                    terminal = e
                    break
                raise
            await asyncio.sleep(2)

        check(
            "push eventually rejected with a terminal 403",
            terminal is not None,
            f"still accepted after {int(DEADLINE_S)}s",
        )
        check(
            "terminal code is storage_quota_exceeded",
            terminal is not None and getattr(terminal, "code", None) == "storage_quota_exceeded",
            str(getattr(terminal, "code", None)) if terminal else "n/a",
        )
        check(
            "terminal 403 surfaced immediately (not retried with backoff)",
            terminal is not None and call_ms < 2000,
            f"{call_ms}ms",
        )

        # consume must stay open while pushes are blocked: the rate-limit phase
        # left plenty of un-popped messages on this queue, so an empty result
        # here would mean the read path was blocked too.
        msgs = await q.queue(QUEUE).batch(1).wait(False).pop()
        check("consume still allowed while push-blocked", len(msgs) >= 1, f"{len(msgs)} msg(s)")
        if msgs:
            await q.ack(msgs, True)
    finally:
        await q.close()


# ---------------------------------------------------------------------------
# 4b. recovery once the driver clears the override
# ---------------------------------------------------------------------------
async def unblocked():
    q = client(HOST_A, KEY_A)
    deadline = time.monotonic() + DEADLINE_S
    released = False
    last_code = None
    try:
        while time.monotonic() < deadline and not released:
            try:
                r = await q.queue(QUEUE).push([{"release": time.time(), "run": RUN_ID}])
                released = bool(r) and r[0].get("status") == "queued"
            except httpx.HTTPStatusError as e:
                if status_of(e) != 403:
                    raise
                last_code = getattr(e, "code", None)
            if not released:
                await asyncio.sleep(2)
        check(
            "push accepted again after the override is cleared",
            released,
            f"still {last_code} after {int(DEADLINE_S)}s",
        )
    finally:
        await q.close()


PHASES = {
    "roundtrip": roundtrip,
    "isolation": isolation,
    "ratelimit": ratelimit,
    "blocked": blocked,
    "unblocked": unblocked,
}


def main():
    fn = PHASES.get(PHASE)
    if fn is None:
        print(
            f"sdk-smoke(py): unknown phase '{PHASE}' (want: {'|'.join(PHASES)})",
            file=sys.stderr,
        )
        sys.exit(2)
    try:
        asyncio.run(fn())
    except Exception as e:  # noqa: BLE001 -- a phase blowing up is a failure, not a crash
        bad(f"phase threw: {type(e).__name__}: {e} (status={status_of(e)} code={getattr(e, 'code', None)})")
    print(f"  -- py/{PHASE}: {STATE['pass']} ok, {STATE['fail']} fail")
    sys.exit(0 if STATE["fail"] == 0 else 1)


main()
