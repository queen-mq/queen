"""
Locks: the wire of ``POST /api/v1/locks``, the ``check`` KV op, and what the
lock handle does around them -- asserted on the EXACT JSON body, no broker.

Same method and same reason as test_kv_wire.py: the body is the contract, and
a wrong field name is a 400 nobody can diagnose from the client side.

What lives in the CLIENT and nowhere else, and is pinned here:
  * ``acquire()`` returns a bool, and every wire result's ``bool`` is its
    verdict (a plain dict would be truthy whatever it said);
  * the handle always sends its owner, the same one on every call;
  * a renew's NEW token replaces the old one for the guard and the release;
  * a guard that lost to the handle's own renewal is sent again with the new
    token, and one that lost to another holder is returned and marks the lock
    lost;
  * a transaction that asked for a guard never goes out without one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from queen import Lock, LockNotHeldError, Queen

from .plan_server import PlanServer, kv_results


def guard_of(name, slot, token):
    return {"op": "check", "ns": "queen-locks", "key": f"{name}#{slot}", "expect": token, "required": True}


def results(element):
    return {"status": 200, "json": {"results": [element]}}


def granted(name, token, slot=0):
    return results(
        {"index": 0, "op": "acquire", "name": name, "acquired": True, "slot": slot, "token": token,
         "owner": "o", "guard": guard_of(name, slot, token)}
    )


def refused(name, reason="held"):
    return results(
        {"index": 0, "op": "acquire", "name": name, "acquired": False, "reason": reason,
         "holders": [{"slot": 0, "owner": "other"}]}
    )


def renewed(name, token, slot=0):
    return results(
        {"index": 0, "op": "renew", "name": name, "renewed": True, "slot": slot, "token": token,
         "guard": guard_of(name, slot, token)}
    )


def lost_renew(name):
    return results(
        {"index": 0, "op": "renew", "name": name, "renewed": False, "reason": "lost", "slot": 0,
         "holders": [{"slot": 0, "owner": "other"}]}
    )


def released(name, slot=0):
    return results({"index": 0, "op": "release", "name": name, "released": True, "slot": slot})


def committed():
    return {"status": 200, "json": {"transactionId": "t", "success": True, "results": []}}


def lost_to(failed_index, kv_reason, value, version):
    return {
        "status": 200,
        "json": {"transactionId": "t", "success": False, "reason": "kv_precondition", "error": "QKV",
                 "results": [], "ok": False, "failedIndex": failed_index, "kvReason": kv_reason,
                 "value": value, "version": version},
    }


def make(*plan, default=None):
    server = PlanServer(*plan, default=default)
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def op_of(rec):
    return rec.body["operations"][0]


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_sends_its_expect_and_nothing_else():
    client, server = make(
        kv_results({"index": 0, "op": "check", "applied": True, "key": "k", "version": 7}),
        kv_results({"index": 0, "op": "check", "applied": False, "reason": "version", "key": "k",
                    "value": {"n": 2}, "version": 9}),
    )
    held = await client.kv.check("orders", "k", expect=7)
    assert server.requests[0].route == "POST /api/v1/kv"
    assert server.requests[0].body == {"operations": [{"op": "check", "ns": "orders", "key": "k", "expect": 7}]}
    assert held and "value" not in held, "a held check hands back no value"

    stale = await client.kv.check("orders", "k", expect=7, required=True)
    assert op_of(server.requests[1]) == {"op": "check", "ns": "orders", "key": "k", "expect": 7, "required": True}
    assert not stale and stale["reason"] == "version" and stale["version"] == 9


@pytest.mark.asyncio
async def test_a_check_with_no_expect_is_refused_before_the_request():
    client, server = make()
    with pytest.raises(ValueError, match="check needs expect"):
        await client.kv.check("orders", "k")
    with pytest.raises(ValueError, match="expect=None is not"):
        await client.kv.check("orders", "k", expect=None)
    await client.kv.check("orders", "k", expect=0)
    assert op_of(server.only) == {"op": "check", "ns": "orders", "key": "k", "expect": 0}


@pytest.mark.asyncio
async def test_check_rides_a_transaction_in_the_kv_array():
    client, server = make(committed())
    await (
        client.transaction()
        .kv.check("queen-locks", "job#0", expect=41, required=True)
        .kv.put("work", "state", {"n": 1}, forever=True)
        .commit()
    )
    assert server.only.route == "POST /api/v1/transaction"
    assert server.only.body["kv"] == [
        {"op": "check", "ns": "queen-locks", "key": "job#0", "expect": 41, "required": True},
        {"op": "put", "ns": "work", "key": "state", "value": {"n": 1}, "forever": True},
    ]


# ---------------------------------------------------------------------------
# The four operations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_four_operations_send_exactly_their_fields():
    client, server = make(
        granted("daily-report", 100),
        renewed("gpu", 101, slot=2),
        released("daily-report"),
        results({"index": 0, "op": "get", "name": "daily-report", "held": False, "holders": []}),
    )
    a = await client.locks.acquire("daily-report", ttl_seconds=30, owner="o")
    assert server.requests[0].route == "POST /api/v1/locks"
    assert server.requests[0].body == {
        "operations": [{"op": "acquire", "name": "daily-report", "ttlSeconds": 30, "owner": "o"}]
    }
    assert a and a["token"] == 100 and a["guard"] == guard_of("daily-report", 0, 100)

    r = await client.locks.renew("gpu", token=100, slot=2, ttl=timedelta(seconds=45), owner="o")
    assert op_of(server.requests[1]) == {
        "op": "renew", "name": "gpu", "token": 100, "ttlSeconds": 45, "slot": 2, "owner": "o"
    }
    assert r and r["token"] == 101, "a renew answers a NEW token"

    d = await client.locks.release("daily-report", token=101)
    assert op_of(server.requests[2]) == {"op": "release", "name": "daily-report", "token": 101}
    assert d

    g = await client.locks.get("daily-report")
    assert op_of(server.requests[3]) == {"op": "get", "name": "daily-report"}
    assert not g, "nobody holds it: the result is falsy"


@pytest.mark.asyncio
async def test_a_held_lock_is_a_falsy_result_never_an_error():
    client, _ = make(refused("job"))
    r = await client.locks.acquire("job", ttl_seconds=30)
    assert not r and r["reason"] == "held" and r["holders"][0]["owner"] == "other"


@pytest.mark.asyncio
async def test_what_the_broker_would_refuse_is_refused_before_the_request():
    client, server = make()
    locks = client.locks
    for call, match in [
        (lambda: locks.acquire("a"), "needs a lifetime"),
        (lambda: locks.acquire("a", ttl_seconds=30, ttl=timedelta(seconds=30)), "declared twice"),
        (lambda: locks.acquire("a", ttl_seconds=0), "needs a lifetime"),
        (lambda: locks.acquire("a", ttl_seconds=True), "needs a lifetime"),
        (lambda: locks.acquire("a#b", ttl_seconds=1), "without '#'"),
        (lambda: locks.acquire("", ttl_seconds=1), "non-empty string"),
        (lambda: locks.acquire("a", ttl_seconds=1, limit=0), "limit is a whole number"),
        (lambda: locks.acquire("a", ttl_seconds=1, owner=""), "owner is a non-empty string"),
        (lambda: locks.renew("a", token=0, ttl_seconds=1), "renew needs the token"),
        (lambda: locks.release("a", token=None), "release needs the token"),
    ]:
        with pytest.raises(ValueError, match=match):
            await call()
    with pytest.raises(ValueError, match="client.semaphore"):
        client.lock("a", ttl_seconds=1, limit=3)
    with pytest.raises(ValueError, match="needs a lifetime"):
        client.lock("a")
    with pytest.raises(ValueError, match="renew_every must be shorter"):
        client.lock("a", ttl_seconds=3, renew_every=3)
    assert server.requests == []


@pytest.mark.asyncio
async def test_a_ttl_is_rounded_up_to_the_second():
    client, server = make(granted("a", 1))
    await client.locks.acquire("a", ttl=timedelta(milliseconds=1500))
    assert op_of(server.only)["ttlSeconds"] == 2


# ---------------------------------------------------------------------------
# The handle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_returns_a_bool_and_the_handle_names_its_owner():
    client, server = make(refused("job"), granted("job", 100), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    assert isinstance(lock, Lock) and not lock.held and lock.token is None

    assert await lock.acquire() is False
    sent = op_of(server.requests[0])
    assert sent == {"op": "acquire", "name": "job", "ttlSeconds": 30, "owner": lock.owner}
    assert lock.owner.count(":") >= 2, "host:pid:random, unique per handle"

    assert await lock.acquire() is True
    assert op_of(server.requests[1])["owner"] == lock.owner, "the same owner on the retry"
    assert lock.held and lock.token == 100 and lock.slot == 0
    assert lock.guard() == guard_of("job", 0, 100)
    assert await lock.acquire() is True, "already held: no call"
    assert len(server.requests) == 2

    assert await lock.release() is True
    assert op_of(server.requests[2]) == {"op": "release", "name": "job", "token": 100, "slot": 0}
    assert not lock.held and not lock.lost.is_set(), "a release is not a loss"
    with pytest.raises(LockNotHeldError):
        lock.guard()
    assert client.lock("job", ttl_seconds=30).owner != lock.owner
    assert client.lock("job", ttl_seconds=30, owner="cron-7").owner == "cron-7"


@pytest.mark.asyncio
async def test_acquire_with_a_wait_comes_back_until_the_permit_is_free():
    client, server = make(refused("job"), refused("job", "contended"), granted("job", 5), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False, retry_min=0.005, retry_max=0.01)
    assert await lock.acquire(wait=5) is True
    assert len(server.requests) == 3
    await lock.release()

    client, server = make(default=refused("job"))
    lock = client.lock("job", ttl_seconds=30, wait=0.06, retry_min=0.005, retry_max=0.01)
    assert await lock.acquire() is False, "the handle's own wait ran out"
    assert len(server.requests) >= 2


@pytest.mark.asyncio
async def test_a_renew_puts_the_new_token_in_the_guard_and_in_the_release():
    client, server = make(granted("job", 100), renewed("job", 101), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    await lock.acquire()
    assert await lock.renew() is True
    assert op_of(server.requests[1]) == {
        "op": "renew", "name": "job", "token": 100, "ttlSeconds": 30, "slot": 0, "owner": lock.owner
    }
    assert lock.token == 101 and lock.guard() == guard_of("job", 0, 101)
    await lock.release()
    assert op_of(server.requests[2])["token"] == 101


@pytest.mark.asyncio
async def test_it_renews_in_the_background_and_stops_when_released():
    token = {"n": 1}

    def answer(rec):
        op = op_of(rec)
        if op["op"] == "acquire":
            return granted("job", token["n"])
        if op["op"] == "renew":
            token["n"] += 1
            return renewed("job", token["n"])
        return released("job")

    client, server = make(default=answer)
    lock = client.lock("job", ttl_seconds=2, renew_every=0.04)
    await lock.acquire()
    await asyncio.sleep(0.15)
    renews = [op_of(r) for r in server.requests if op_of(r)["op"] == "renew"]
    assert len(renews) >= 2
    assert [r["token"] for r in renews[:2]] == [1, 2], "each renew carries the token of the one before"
    assert lock.held and lock.token == token["n"]
    await lock.release()
    assert op_of(server.last) == {"op": "release", "name": "job", "token": token["n"], "slot": 0}
    calls = len(server.requests)
    await asyncio.sleep(0.1)
    assert len(server.requests) == calls, "a released lock renews no more"


@pytest.mark.asyncio
async def test_a_refused_renew_is_a_loss():
    client, server = make(granted("job", 100), lost_renew("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    seen = []
    lock.on_lost(seen.append)
    await lock.acquire()
    lost = lock.lost
    assert await lock.renew() is False
    assert not lock.held and lock.token is None
    assert lost.is_set() and seen == ["renew"]
    assert await lock.release() is False, "nothing to give back, and no call"
    assert len(server.requests) == 2


@pytest.mark.asyncio
async def test_a_lifetime_that_runs_out_here_is_a_loss():
    client, server = make(granted("job", 100))
    lock = client.lock("job", ttl_seconds=1, auto_renew=False)
    seen = []
    lock.on_lost(seen.append)
    await lock.acquire()
    lost = lock.lost
    assert lock.held
    await asyncio.wait_for(lost.wait(), timeout=3)
    assert not lock.held and seen == ["expired"]
    assert len(server.requests) == 1


@pytest.mark.asyncio
async def test_async_with_holds_for_the_block_and_refuses_to_run_without_the_lock():
    client, server = make(granted("job", 100), released("job"), refused("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    async with lock as held:
        assert held is lock and lock.held
    assert op_of(server.requests[1])["op"] == "release"
    ran = False
    with pytest.raises(LockNotHeldError):
        async with lock:
            ran = True
    assert not ran, "a block must not run without its lock"


@pytest.mark.asyncio
async def test_run_releases_whatever_fn_does_and_close_gives_back_what_is_held():
    client, server = make(granted("job", 100), released("job"), refused("job"), granted("job", 200), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)

    async def work(held):
        assert held.held
        return 42

    assert await lock.run(work) == {"acquired": True, "value": 42}
    assert await lock.run(work) == {"acquired": False}

    async def boom(_):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await lock.run(boom)
    assert op_of(server.requests[4])["op"] == "release", "released although fn raised"

    client, server = make(granted("job", 7), released("job"))
    lock = client.lock("job", ttl_seconds=30)
    await lock.acquire()
    await client.close()
    assert op_of(server.requests[1]) == {"op": "release", "name": "job", "token": 7, "slot": 0}
    assert not lock.held


@pytest.mark.asyncio
async def test_a_semaphore_handle_is_one_permit_of_several():
    client, server = make(granted("gpu", 9, slot=3), released("gpu", slot=3))
    permit = client.semaphore("gpu", 4, ttl_seconds=60, auto_renew=False)
    assert permit.limit == 4
    assert await permit.acquire()
    assert op_of(server.requests[0])["limit"] == 4
    assert permit.slot == 3 and permit.guard() == guard_of("gpu", 3, 9)
    await permit.release()
    assert op_of(server.requests[1]) == {"op": "release", "name": "gpu", "token": 9, "slot": 3}


# ---------------------------------------------------------------------------
# guard(lock) on a transaction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_guard_is_the_first_kv_op_at_the_token_held_when_commit_sends():
    client, server = make(granted("job", 100), renewed("job", 101), committed(), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    await lock.acquire()
    txn = (
        client.transaction()
        .guard(lock)
        .queue("reports").push([{"data": {"n": 1}, "transactionId": "t1"}])
        .kv.put("work", "state", {"n": 1}, forever=True)
    )
    await lock.renew()  # after the guard was asked for, before commit
    assert await txn.commit()
    assert server.requests[2].route == "POST /api/v1/transaction"
    assert server.requests[2].body["kv"] == [
        guard_of("job", 0, 101),
        {"op": "put", "ns": "work", "key": "state", "value": {"n": 1}, "forever": True},
    ]
    await lock.release()


@pytest.mark.asyncio
async def test_a_guard_that_lost_to_its_own_renewal_is_sent_again_with_the_new_token():
    lock_box = {}

    def first_commit(rec):
        # The lock's renewal landed while this commit was on its way: by the
        # time its answer is read, the handle holds token 101. Two pushed items
        # come first in the flat index space, so the guard is index 2.
        lock = lock_box["lock"]
        lock._token = 101
        lock._guard = guard_of("job", 0, 101)
        return lost_to(2, "version", {"owner": lock.owner}, 101)

    client, server = make(granted("job", 100), first_commit, committed(), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    lock_box["lock"] = lock
    await lock.acquire()
    res = await (
        client.transaction()
        .guard(lock)
        .queue("reports").push([{"data": 1, "transactionId": "a"}, {"data": 2, "transactionId": "b"}])
        .commit()
    )
    assert res, "the step committed on the second send"
    sends = [r.body for r in server.requests if r.path == "/api/v1/transaction"]
    assert len(sends) == 2
    assert sends[0]["kv"][0]["expect"] == 100 and sends[1]["kv"][0]["expect"] == 101
    assert sends[1]["operations"] == sends[0]["operations"], "the same step, not a second one"
    assert lock.held
    await lock.release()


@pytest.mark.asyncio
async def test_a_guard_that_lost_to_another_holder_is_the_verdict_and_the_lock_is_lost():
    client, server = make(granted("job", 100), lost_to(0, "version", {"owner": "somebody-else"}, 250))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    seen = []
    lock.on_lost(seen.append)
    await lock.acquire()
    res = await client.transaction().guard(lock).kv.put("w", "k", 1, forever=True).commit()
    assert not res and res["reason"] == "kv_precondition", "returned, not raised -- like once()"
    assert len([r for r in server.requests if r.path == "/api/v1/transaction"]) == 1, "not sent again"
    assert not lock.held and seen == ["guard"]

    # An expired permit is a lost guard too.
    client, _ = make(granted("job", 100), lost_to(0, "absent", None, 0))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    await lock.acquire()
    assert not await client.transaction().guard(lock).kv.put("w", "k", 1, forever=True).commit()
    assert not lock.held


@pytest.mark.asyncio
async def test_a_precondition_that_is_not_the_guards_leaves_the_lock_alone():
    # kv = [guard, once]: flat index 1 is the once marker.
    client, _ = make(granted("job", 100), lost_to(1, "exists", True, 77), released("job"))
    lock = client.lock("job", ttl_seconds=30, auto_renew=False)
    await lock.acquire()
    res = await client.transaction().guard(lock).once("idem", "order-1", ttl_seconds=3600).commit()
    assert not res
    assert lock.held, "the marker lost, not the lock"
    await lock.release()


@pytest.mark.asyncio
async def test_a_step_that_asked_for_a_guard_never_goes_out_without_one():
    client, server = make()
    lock = client.lock("job", ttl_seconds=30)
    txn = client.transaction().guard(lock).kv.put("w", "k", 1, forever=True)
    with pytest.raises(LockNotHeldError):
        await txn.commit()
    assert server.requests == []
    with pytest.raises(TypeError, match="takes a lock"):
        client.transaction().guard(object())
