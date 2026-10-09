"""
Locks, semaphores, ``check`` and guarded transactions against a live broker.

Every lock here is named under ``test-`` plus a per-run suffix and carries a
lifetime, so a run that goes wrong leaves nothing that does not expire by
itself, and a rerun against the same broker does not meet the first run's
permits.

What only a real broker can show: one holder; a call sent again by its owner is
the same permit; an expired lock goes to the next handle with a higher token and
the old handle's guarded step pushes nothing; a guarded step survives the
lock's own renewal; a semaphore never grants more than its limit.
"""

import asyncio
import time

import pytest

from queen.errors import QueenHttpError


def unique(base: str) -> str:
    return f"test-{base}-{time.time_ns()}"


async def checks_are_served(client) -> None:
    """A broker alone raises its cluster version to 5 on its first tick;
    ``check`` needs it."""
    for _ in range(100):
        try:
            await client.kv.check("test-locks-probe", "k", expect=0)
            return
        except QueenHttpError as error:
            if error.response.status_code != 503:
                raise
            await asyncio.sleep(0.1)
    raise AssertionError("the broker never served a check (cluster version below 5?)")


async def drain(client, queue: str):
    seen = []
    while True:
        messages = await client.queue(queue).batch(50).wait(False).pop()
        if not messages:
            return seen
        seen.extend(m["data"] for m in messages)
        await client.ack(messages, True)


@pytest.mark.asyncio
async def test_a_lock_has_one_holder_and_its_guarded_step_commits(client):
    await checks_are_served(client)
    name = unique("one-holder")
    a = client.lock(name, ttl_seconds=30)
    b = client.lock(name, ttl_seconds=30)
    try:
        assert await a.acquire() is True
        assert await b.acquire() is False, "a second handle acquired a held lock"
        token = a.token

        who = await client.locks.get(name)
        assert who and who["holders"][0]["owner"] == a.owner and who["holders"][0]["token"] == token

        # The permit is a KV row and nothing else.
        row = await client.kv.get("queen-locks", f"{name}#0")
        assert row and row["version"] == token and row["value"]["owner"] == a.owner

        queue = unique("one-holder-q")
        res = await client.transaction().guard(a).queue(queue).push([{"data": {"step": 1}}]).commit()
        assert res, res
        assert await drain(client, queue) == [{"step": 1}]

        assert await a.release() is True
        assert await b.acquire() is True, "free after its release"
        assert b.token > token, "a later holder, a higher token"
    finally:
        await a.release()
        await b.release()


@pytest.mark.asyncio
async def test_a_call_sent_again_by_its_owner_is_the_same_permit(client):
    name = unique("retry-owner")
    first = await client.locks.acquire(name, ttl_seconds=30, owner="me")
    again = await client.locks.acquire(name, ttl_seconds=30, owner="me")
    assert first and again
    assert again.get("already") is True and again["token"] == first["token"]

    renewed = await client.locks.renew(name, token=first["token"], ttl_seconds=30, owner="me")
    # The renew's answer is lost; the old token is sent again.
    resent = await client.locks.renew(name, token=first["token"], ttl_seconds=30, owner="me")
    assert renewed and resent and resent["token"] > renewed["token"]
    stranger = await client.locks.renew(name, token=first["token"], ttl_seconds=30, owner="somebody-else")
    assert not stranger and stranger["reason"] == "lost" and stranger["holders"][0]["owner"] == "me"
    assert await client.locks.release(name, token=resent["token"])


@pytest.mark.asyncio
async def test_an_expired_lock_is_taken_over_and_the_old_holder_commits_nothing(client):
    await checks_are_served(client)
    name = unique("expiry")
    queue = unique("expiry-q")
    # The old holder does not renew: it is "paused" for longer than its lease.
    old = client.lock(name, ttl_seconds=1, auto_renew=False)
    nxt = client.lock(name, ttl_seconds=30)
    try:
        assert await old.acquire()
        old_token = old.token
        stale_guard = old.guard()
        await asyncio.sleep(1.3)
        assert not old.held, "past its lifetime a handle does not claim to hold"

        assert await nxt.acquire(), "an expired lock is free for the next holder"
        assert nxt.token > old_token

        # The old holder wakes up and sends the step it was about to send.
        stale = await (
            client.transaction()
            .kv.check(stale_guard["ns"], stale_guard["key"], expect=stale_guard["expect"], required=True)
            .queue(queue).push([{"data": {"from": "old"}}])
            .commit()
        )
        assert not stale and stale["reason"] == "kv_precondition"

        ok = await client.transaction().guard(nxt).queue(queue).push([{"data": {"from": "next"}}]).commit()
        assert ok
        assert await drain(client, queue) == [{"from": "next"}], "only the new holder's message exists"
    finally:
        await nxt.release()


@pytest.mark.asyncio
async def test_a_guarded_step_survives_the_locks_own_renewal(client):
    await checks_are_served(client)
    name = unique("renewal")
    queue = unique("renewal-q")
    lock = client.lock(name, ttl_seconds=2, renew_every=0.1)
    try:
        assert await lock.acquire()
        first = lock.token
        committed = 0
        end = time.monotonic() + 1.5
        while time.monotonic() < end:
            res = await client.transaction().guard(lock).queue(queue).push([{"data": {"n": committed}}]).commit()
            assert res, f"step {committed} did not commit while the lock was held: {res}"
            committed += 1
        assert lock.token > first, "the lock renewed during the run"
        assert lock.held and not lock.lost.is_set()
        assert len(await drain(client, queue)) == committed
    finally:
        await lock.release()


@pytest.mark.asyncio
async def test_a_semaphore_never_grants_more_than_its_limit(client):
    name = unique("crowd")
    limit = 3
    permits = [client.semaphore(name, limit, ttl_seconds=30) for _ in range(12)]
    try:
        got = await asyncio.gather(*(p.acquire() for p in permits))
        holders = [p for p, ok in zip(permits, got) if ok]
        assert 1 <= len(holders) <= limit, f"{len(holders)} permits granted of {limit}"
        # A crowd can leave a permit free; one at a time, the semaphore fills.
        for p in permits:
            if len(holders) == limit:
                break
            if not p.held and await p.acquire():
                holders.append(p)
        assert len(holders) == limit
        assert sorted(p.slot for p in holders) == [0, 1, 2], "one holder per slot"

        extra = client.semaphore(name, limit, ttl_seconds=30)
        assert not await extra.acquire(), "a permit past the limit"
        assert len((await client.locks.get(name))["holders"]) == limit

        # One leaves; the waiter gets exactly that slot.
        freed = holders[0].slot
        waiting = asyncio.ensure_future(extra.acquire(wait=5))
        await asyncio.sleep(0.2)
        await holders[0].release()
        assert await waiting
        assert extra.slot == freed
        await extra.release()
    finally:
        for p in permits:
            await p.release()
