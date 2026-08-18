"""
The KV wire contract, asserted on the EXACT JSON body -- no broker.

PLAN_KV_TIMERS.md §5 (semantics), §8.1 (routes and the one status-code rule),
§10.4 (the per-language traps).

Every op the SDK can build is pinned here, because the body is the only place a
wrong shape is visible before production: the broker answers a named 400 at
best, and at worst commits a bundle whose gate was silently never sent.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from queen import Queen
from queen.errors import KvError

from .plan_server import PlanServer, error_body, kv_results


def make(plan=None):
    server = PlanServer(*(plan or []))
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def ops_of(server: PlanServer):
    return server.last.body["operations"]


# ---------------------------------------------------------------------------
# The surface: every op goes to POST /api/v1/kv.
#
# The path routes of §8.1 are sugar for the three cases people write by hand
# with curl; the SDK uses the batch route because it is "la superficie
# completa" -- the only one that accepts getPrefix and incr, the only one with
# a uniform result element, and the only one on which a DELETE can carry an
# `expect` body at all.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_body_is_exactly_this():
    client, server = make()
    await client.kv.put("orders", "order:9f1", {"state": "held"}, ttl_seconds=60)
    assert server.only.route == "POST /api/v1/kv"
    assert server.only.body == {
        "operations": [
            {
                "op": "put",
                "ns": "orders",
                "key": "order:9f1",
                "value": {"state": "held"},
                "ttlSeconds": 60,
            }
        ]
    }
    await client.close()


@pytest.mark.asyncio
async def test_put_forever_sends_forever_and_never_a_ttl():
    client, server = make()
    await client.kv.put("orders", "k", 1, forever=True)
    assert ops_of(server) == [
        {"op": "put", "ns": "orders", "key": "k", "value": 1, "forever": True}
    ]
    await client.close()


@pytest.mark.asyncio
async def test_expiry_is_mandatory_and_exclusive():
    """§5.1: exactly one of ttlSeconds and forever. Zero or two is an error,
    and the client says so before spending a round trip."""
    client, _ = make()
    with pytest.raises(ValueError, match="kv_expiry_not_specified"):
        await client.kv.put("orders", "k", 1)
    with pytest.raises(ValueError, match="exactly one"):
        await client.kv.put("orders", "k", 1, ttl_seconds=60, forever=True)
    await client.close()


@pytest.mark.asyncio
async def test_until_and_ttl_timedelta_become_ttl_seconds_on_the_wire():
    """§5.1: `until: <instant>` is SDK sugar and is converted to a delta at
    send time -- there is no `expiresAt` input field, ever."""
    client, server = make()
    await client.kv.put("orders", "k", 1, ttl=timedelta(minutes=5))
    assert ops_of(server)[0]["ttlSeconds"] == 300
    assert "until" not in ops_of(server)[0] and "expiresAt" not in ops_of(server)[0]

    until = datetime.now(timezone.utc) + timedelta(seconds=90)
    await client.kv.put("orders", "k", 1, until=until)
    ttl = ops_of(server)[0]["ttlSeconds"]
    assert isinstance(ttl, int) and 80 <= ttl <= 90
    await client.close()


@pytest.mark.asyncio
async def test_put_if_absent_is_its_own_op_name_and_refuses_an_expect():
    """§5.3: putIfAbsent desugars to put+expect:0 INSIDE the procedure, so the
    wire keeps the name of the thing -- and a caller passing a different expect
    is a 22023 at the broker, which the SDK turns into a local error."""
    client, server = make()
    await client.kv.put_if_absent("saga", "order:9f1", {"by": "me"}, ttl_seconds=3600)
    assert ops_of(server) == [
        {
            "op": "putIfAbsent",
            "ns": "saga",
            "key": "order:9f1",
            "value": {"by": "me"},
            "ttlSeconds": 3600,
        }
    ]
    assert "expect" not in ops_of(server)[0]
    await client.close()


@pytest.mark.asyncio
async def test_expect_zero_is_sent_and_expect_none_is_a_client_side_error():
    """§5.3 / S14: `expect` present but empty is a BUG IN THE CALLER'S CODE,
    never a silent downgrade to an unconditional upsert. And expect=0 must
    survive, which a plain falsiness check would eat."""
    client, server = make()
    await client.kv.put("orders", "k", 1, ttl_seconds=60, expect=0)
    assert ops_of(server)[0]["expect"] == 0

    with pytest.raises(ValueError, match="expect"):
        await client.kv.put("orders", "k", 1, ttl_seconds=60, expect=None)
    with pytest.raises(ValueError, match="expect"):
        await client.kv.delete("orders", "k", expect=None)
    await client.close()


@pytest.mark.asyncio
async def test_delete_body():
    client, server = make()
    await client.kv.delete("orders", "order:9f1")
    assert ops_of(server) == [{"op": "delete", "ns": "orders", "key": "order:9f1"}]

    await client.kv.delete("orders", "order:9f1", expect=41)
    assert ops_of(server) == [
        {"op": "delete", "ns": "orders", "key": "order:9f1", "expect": 41}
    ]
    await client.close()


@pytest.mark.asyncio
async def test_incr_body_and_that_it_has_no_expect():
    """§5.4: incr is the way OUT of the CAS loop, so it has no precondition --
    the parameter does not exist on the method at all."""
    client, server = make()
    await client.kv.incr("quota", "acme:2026-08-17T10", delta=1, max=100, ttl_seconds=3600)
    assert ops_of(server) == [
        {
            "op": "incr",
            "ns": "quota",
            "key": "acme:2026-08-17T10",
            "delta": 1,
            "max": 100,
            "ttlSeconds": 3600,
        }
    ]
    with pytest.raises(TypeError):
        await client.kv.incr("quota", "k", delta=1, ttl_seconds=60, expect=1)
    await client.close()


@pytest.mark.asyncio
async def test_incr_refuses_a_bool_delta():
    """Python-only trap: bool IS an int, so `delta=True` would go out as 1 and
    a rate limiter would count someone's typo as a request."""
    client, _ = make()
    with pytest.raises(TypeError, match="bool"):
        await client.kv.incr("quota", "k", delta=True, ttl_seconds=60)
    await client.close()


@pytest.mark.asyncio
async def test_get_and_get_many_bodies():
    client, server = make()
    await client.kv.get("orders", "order:9f1")
    assert ops_of(server) == [{"op": "get", "ns": "orders", "key": "order:9f1"}]

    await client.kv.get_many("orders", ["a", "b", "a"])
    assert ops_of(server) == [{"op": "getMany", "ns": "orders", "keys": ["a", "b", "a"]}]
    await client.close()


@pytest.mark.asyncio
async def test_get_prefix_body_and_that_it_never_becomes_a_query_string():
    """§5.5: a prefix in a URL is recorded by every access log between the
    client and the database. The SDK must never put one there."""
    client, server = make()
    await client.kv.get_prefix("saga", "order:9f1:", after="order:9f1:b", limit=50, keys_only=True)
    assert server.only.route == "POST /api/v1/kv"
    assert server.only.query == {}
    assert ops_of(server) == [
        {
            "op": "getPrefix",
            "ns": "saga",
            "prefix": "order:9f1:",
            "after": "order:9f1:b",
            "limit": 50,
            "keysOnly": True,
        }
    ]
    await client.close()


@pytest.mark.asyncio
async def test_get_prefix_refuses_an_empty_prefix_locally():
    """§5.5: `400 kv_prefix_required` at the broker; a namespace is not a table
    to enumerate. Saying so locally saves the round trip and the log line."""
    client, _ = make()
    with pytest.raises(ValueError, match="kv_prefix_required"):
        await client.kv.get_prefix("saga", "")
    await client.close()


@pytest.mark.asyncio
async def test_batch_preserves_order_and_sends_one_request():
    client, server = make()
    results = await client.kv.batch(
        [
            client.kv.op.get("a", "k1"),
            client.kv.op.put("a", "k2", 2, ttl_seconds=30),
            client.kv.op.incr("a", "k3", delta=5, ttl_seconds=30),
        ]
    )
    assert len(server.requests) == 1
    assert [o["op"] for o in ops_of(server)] == ["get", "put", "incr"]
    assert [r["index"] for r in results] == [0, 1, 2]
    await client.close()


# ---------------------------------------------------------------------------
# Results.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_applied_false_is_a_200_and_not_an_exception():
    """§8.1 / §10.3: `applied:false` is the single most frequent outcome of the
    product. It must never reach a retry policy or an error metric."""
    client, server = make(
        [kv_results({"index": 0, "op": "put", "applied": False, "reason": "exists", "key": "k", "value": {"by": "other"}, "version": 90101})]
    )
    res = await client.kv.put_if_absent("saga", "k", {"by": "me"}, ttl_seconds=60)
    assert res["applied"] is False
    assert res["reason"] == "exists"
    assert res["value"] == {"by": "other"}
    await client.close()


@pytest.mark.asyncio
async def test_a_result_is_falsy_when_it_did_not_apply():
    """§10.4 lists "objects are always truthy" as the JavaScript trap with no
    structural defence. Python has one, so it is used: the result is a dict in
    every other respect, and `if await kv.delete(...)` reads the verdict."""
    client, server = make(
        [
            kv_results({"index": 0, "op": "delete", "applied": False, "reason": "absent", "key": "k"}),
            kv_results({"index": 0, "op": "delete", "applied": True, "key": "k", "version": 7}),
            kv_results({"index": 0, "op": "get", "found": False, "key": "k"}),
            kv_results({"index": 0, "op": "get", "found": True, "key": "k", "value": None, "version": 7}),
        ]
    )
    assert not await client.kv.delete("orders", "k")
    assert await client.kv.delete("orders", "k")
    assert not await client.kv.get("orders", "k")
    # §5.5: {found:true, value:null} and {found:false} are DIFFERENT things,
    # and the truthiness must follow `found`, never the value.
    got = await client.kv.get("orders", "k")
    assert got and got["value"] is None
    await client.close()


@pytest.mark.asyncio
async def test_once_returns_a_plain_bool():
    """§18.8: `once()` is the gate that makes the safe form shorter to write
    than the unsafe one. It answers the only question its caller has."""
    client, server = make(
        [
            kv_results({"index": 0, "op": "put", "applied": True, "key": "k", "version": 1}),
            kv_results({"index": 0, "op": "put", "applied": False, "reason": "exists", "key": "k", "version": 1}),
        ]
    )
    assert await client.kv.once("dedup", "evt:1", ttl_seconds=86400) is True
    assert await client.kv.once("dedup", "evt:1", ttl_seconds=86400) is False
    assert ops_of(server)[0]["op"] == "putIfAbsent"
    await client.close()


@pytest.mark.asyncio
async def test_list_all_follows_next_after_and_stops():
    """`listAll` is the paged form of getPrefix. `nextAfter` is an EXCLUSIVE
    keyset cursor, and every page is its own snapshot (§5.5) -- which is why
    this helper is documented as "good for compacting state, not for an exact
    count"."""
    client, server = make(
        [
            kv_results(
                {
                    "index": 0,
                    "op": "getPrefix",
                    "rows": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
                    "truncated": True,
                    "nextAfter": "b",
                }
            ),
            kv_results(
                {
                    "index": 0,
                    "op": "getPrefix",
                    "rows": [{"key": "c", "value": 3}],
                    "truncated": False,
                    "nextAfter": None,
                }
            ),
        ]
    )
    rows = await client.kv.list_all("saga", "order:")
    assert [r["key"] for r in rows] == ["a", "b", "c"]
    assert len(server.requests) == 2
    assert server.requests[0].body["operations"][0].get("after") is None
    assert server.requests[1].body["operations"][0]["after"] == "b"
    await client.close()


# ---------------------------------------------------------------------------
# Errors: the client branches on the CODE, never on the prose (§13.5).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_operator_kill_switch_is_a_503_the_caller_can_wait_out():
    """The KV surface exists on every cell that runs the broker binary, so there
    is no "not enabled here" answer to handle -- the 404 `kv_not_enabled` this
    test used to assert cannot be produced any more.

    What CAN still turn KV off is the operator's runtime switch
    (server/src/switches.rs), and it is a different thing in every respect that
    matters to a caller: 503 rather than 404, a `Retry-After` rather than a dead
    end, and an incident rather than a deployment. The SDK must surface it as
    exactly that -- a temporary refusal carrying its own code -- and never as a
    missing feature.
    """
    client, _ = make([{**error_body(503, "kv_disabled", "kv_disabled"), "retry_after": 1}])
    with pytest.raises(KvError) as ei:
        await client.kv.get("orders", "k")
    assert ei.value.status == 503
    assert ei.value.code == "kv_disabled"
    assert ei.value.reason == "kv_disabled"
    # The wait is honest and reaches the caller, which is what makes this
    # retryable rather than fatal.
    assert ei.value.retry_after_seconds == 1
    # Still an httpx.HTTPStatusError, so code that already catches that keeps
    # working.
    assert isinstance(ei.value, httpx.HTTPStatusError)
    await client.close()


@pytest.mark.asyncio
async def test_a_proxy_403_keeps_its_code_field():
    """The proxy puts the stable identifier in `code`; the broker's kv surface
    puts it in `error`. One attribute reads both, so nobody string-matches."""
    client, _ = make([{"status": 403, "json": {"error": "kv writes are blocked", "code": "storage_quota_exceeded"}}])
    with pytest.raises(KvError) as ei:
        await client.kv.put("orders", "k", 1, ttl_seconds=60)
    assert ei.value.code == "storage_quota_exceeded"
    await client.close()
