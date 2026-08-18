"""
KV integration tests -- against a real broker (PLAN_KV_TIMERS.md §5).

Every namespace here starts with ``test-`` so ``cleanup_test_data`` purges it.
That purge is MANDATORY and not cosmetic (§10.4): without it the putIfAbsent
tests below are green on their first run and red forever after, and the incr
tests accumulate across runs until a rate-limit assertion fails with a number
nobody can explain from the test source.

``forever`` appears nowhere in this file, deliberately: a test that goes wrong
must not be able to leave immortal state in a shared test database.
"""

import asyncio

import pytest

from queen.errors import KvError

NS = "test-kv-py"


@pytest.mark.asyncio
async def test_put_then_get_round_trips_the_value_and_a_version(client):
    put = await client.kv.put(NS, "round:trip", {"state": "held", "n": 1}, ttl_seconds=60)
    assert put["applied"] is True
    assert put["version"] > 0

    got = await client.kv.get(NS, "round:trip")
    assert got["found"] is True
    assert got["value"] == {"state": "held", "n": 1}
    assert got["version"] == put["version"]
    assert got["expiresAt"] is not None


@pytest.mark.asyncio
async def test_a_missing_key_is_a_200_with_found_false(client):
    got = await client.kv.get(NS, "never:written")
    assert got["found"] is False
    assert not got


@pytest.mark.asyncio
async def test_a_null_value_is_a_value(client):
    """§5.5: `'null'::jsonb` is legal, and {found:true, value:null} is not the
    same thing as {found:false}. No SDK may collapse them."""
    await client.kv.put(NS, "explicit:null", None, ttl_seconds=60)
    got = await client.kv.get(NS, "explicit:null")
    assert got["found"] is True
    assert got["value"] is None
    assert got  # truthiness follows `found`, never the value


@pytest.mark.asyncio
async def test_put_if_absent_has_exactly_one_winner(client):
    """§5.3: Postgres takes the row lock BEFORE evaluating the condition, so N
    concurrent callers serialise and the losers re-evaluate against the new row."""
    key = "claim:one-winner"
    results = await asyncio.gather(
        *[client.kv.put_if_absent(NS, key, {"by": i}, ttl_seconds=60) for i in range(8)]
    )
    winners = [r for r in results if r["applied"]]
    assert len(winners) == 1

    # And every loser got the WINNER's value back, without a second round trip.
    losers = [r for r in results if not r["applied"]]
    assert all(loser["reason"] == "exists" for loser in losers)
    assert all(loser["value"] == winners[0]["value"] for loser in losers)


@pytest.mark.asyncio
async def test_once_is_the_gate(client):
    key = "once:evt-1"
    assert await client.kv.once(NS, key, ttl_seconds=60) is True
    assert await client.kv.once(NS, key, ttl_seconds=60) is False


@pytest.mark.asyncio
async def test_expect_on_an_absent_key_creates_nothing(client):
    """THE REPAIR THAT MATTERS MOST (§5.3). In the naive ON CONFLICT form an
    `expect:N>0` on an absent key falls into the INSERT branch and CREATES the
    row -- which in a saga fires the compensating command the expect existed to
    prevent. An expect that matches zero rows must create NOTHING."""
    key = "fence:absent"
    res = await client.kv.put(NS, key, {"compensate": True}, ttl_seconds=60, expect=42)
    assert res["applied"] is False
    assert res["reason"] == "absent"
    assert res["version"] == 0

    got = await client.kv.get(NS, key)
    assert got["found"] is False, "an expect that matched nothing created a row"


@pytest.mark.asyncio
async def test_expect_fences_a_stale_writer(client):
    key = "fence:version"
    first = await client.kv.put(NS, key, {"v": 1}, ttl_seconds=60)
    second = await client.kv.put(NS, key, {"v": 2}, ttl_seconds=60, expect=first["version"])
    assert second["applied"] is True

    stale = await client.kv.put(NS, key, {"v": 3}, ttl_seconds=60, expect=first["version"])
    assert stale["applied"] is False
    assert stale["reason"] == "version"
    # The version handed to a loser is ADVISORY -- but it is at least the
    # current one, so a caller can re-read without a second call.
    assert stale["version"] == second["version"]
    assert stale["value"] == {"v": 2}


@pytest.mark.asyncio
async def test_delete_is_idempotent_and_says_so(client):
    key = "delete:twice"
    await client.kv.put(NS, key, 1, ttl_seconds=60)
    first = await client.kv.delete(NS, key)
    assert first["applied"] is True

    second = await client.kv.delete(NS, key)
    assert second["applied"] is False
    assert second["reason"] == "absent"
    assert not second


@pytest.mark.asyncio
async def test_an_expired_key_is_gone_before_the_sweeper_prunes_it(client):
    """§5.7: a key past its expiry is NEVER returned and never counts as
    existing, even though the sweeper deletes it later. The truth is the
    predicate, not the presence of the row -- which is also what lets a
    putIfAbsent RESURRECT an expired lineage."""
    key = "expiry:short"
    await client.kv.put(NS, key, {"gone": "soon"}, ttl_seconds=1)
    await asyncio.sleep(1.6)

    assert (await client.kv.get(NS, key))["found"] is False
    # And the resurrection: "must not exist" wins against the unpruned row.
    again = await client.kv.put_if_absent(NS, key, {"new": "lineage"}, ttl_seconds=60)
    assert again["applied"] is True


@pytest.mark.asyncio
async def test_incr_with_max_is_the_admission_decision(client):
    """§5.4: `max` does not saturate and does not truncate -- it REFUSES, so the
    request that would have blown the ceiling has spent no budget. `applied` IS
    the admission decision."""
    key = "limit:acme"
    for expected in (1, 2, 3):
        res = await client.kv.incr(NS, key, delta=1, max=3, ttl_seconds=60)
        assert res["applied"] is True
        assert int(res["value"]) == expected

    refused = await client.kv.incr(NS, key, delta=1, max=3, ttl_seconds=60)
    assert refused["applied"] is False
    assert refused["reason"] == "limit"
    assert int(refused["value"]) == 3, "a refused increment must not have spent budget"


@pytest.mark.asyncio
async def test_the_first_incr_of_a_window_is_guarded_too(client):
    """§5.4 repair 2: the naive form applies no guard on the INSERT branch, so
    with max=5 and delta=10 the FIRST call returns applied:true and the counter
    is 10 -- the quota blown on the first shot, and again at every window
    rotation, i.e. exactly when a limiter is under attack."""
    res = await client.kv.incr(NS, "limit:first-shot", delta=10, max=5, ttl_seconds=60)
    assert res["applied"] is False
    assert res["reason"] == "limit"
    assert (await client.kv.get(NS, "limit:first-shot"))["found"] is False


@pytest.mark.asyncio
async def test_incr_on_an_expired_non_numeric_key_does_not_wedge(client):
    """§5.4 repair 3: the type guard is evaluated on the OLD row, so somebody
    "initialising" a counter with a JSON object used to wedge every later incr
    with reason:'type' until the sweeper pruned it -- i.e. the customer's entire
    traffic refused, with a reason no client handles as "retry"."""
    key = "limit:initialised-wrong"
    await client.kv.put(NS, key, {"count": 0}, ttl_seconds=1)
    await asyncio.sleep(1.6)
    res = await client.kv.incr(NS, key, delta=1, ttl_seconds=60)
    assert res["applied"] is True
    assert int(res["value"]) == 1


@pytest.mark.asyncio
async def test_incr_does_not_extend_a_live_ttl(client):
    """§5.4: the TTL of incr is CREATE-ONLY. If it renewed, a fixed-window
    limiter on an always-busy client would never close its window, i.e. would
    stop limiting exactly under load."""
    key = "limit:window-closes"
    await client.kv.incr(NS, key, delta=1, ttl_seconds=2)
    first = await client.kv.get(NS, key)
    await asyncio.sleep(0.5)
    await client.kv.incr(NS, key, delta=1, ttl_seconds=3600)
    second = await client.kv.get(NS, key)
    assert second["expiresAt"] == first["expiresAt"]


@pytest.mark.asyncio
async def test_get_many_reports_absence_as_a_datum(client):
    """§5.5: `missing` is explicit; absence must not be something the caller
    computes by difference."""
    await client.kv.put(NS, "many:a", 1, ttl_seconds=60)
    await client.kv.put(NS, "many:b", 2, ttl_seconds=60)
    res = await client.kv.get_many(NS, ["many:a", "many:b", "many:nope"])
    assert sorted(r["key"] for r in res["rows"]) == ["many:a", "many:b"]
    assert res["missing"] == ["many:nope"]


@pytest.mark.asyncio
async def test_get_prefix_pages_and_list_all_walks_it(client):
    prefix = "walk:"
    for i in range(5):
        await client.kv.put(NS, f"{prefix}{i}", {"i": i}, ttl_seconds=60)

    page = await client.kv.get_prefix(NS, prefix, limit=2)
    assert len(page["rows"]) == 2
    assert page["truncated"] is True
    assert page["nextAfter"] == page["rows"][-1]["key"]

    rows = await client.kv.list_all(NS, prefix, limit=2)
    assert [r["key"] for r in rows] == [f"{prefix}{i}" for i in range(5)]

    # starts_with(), not LIKE: a prefix full of metacharacters is data, and the
    # user does not have to remember an escape (§5.5).
    weird = "walk%_:"
    await client.kv.put(NS, f"{weird}x", 1, ttl_seconds=60)
    assert [r["key"] for r in (await client.kv.get_prefix(NS, weird))["rows"]] == [f"{weird}x"]


@pytest.mark.asyncio
async def test_get_prefix_needs_a_prefix(client):
    with pytest.raises(ValueError):
        await client.kv.get_prefix(NS, "")


@pytest.mark.asyncio
async def test_a_batch_is_index_aligned(client):
    """§6.4: results[i] belongs to operations[i], always."""
    results = await client.kv.batch(
        [
            client.kv.op.put(NS, "batch:1", "one", ttl_seconds=60),
            client.kv.op.get(NS, "batch:absent"),
            client.kv.op.incr(NS, "batch:counter", delta=2, ttl_seconds=60),
        ]
    )
    assert [r["index"] for r in results] == [0, 1, 2]
    assert results[0]["applied"] is True
    assert results[1]["found"] is False
    assert int(results[2]["value"]) == 2


@pytest.mark.asyncio
async def test_a_malformed_namespace_is_a_named_400(client):
    """§13.5: the client branches on the CODE. It never string-matches the
    prose, which is why the code has to be there at all."""
    with pytest.raises(KvError) as ei:
        await client.kv.put("NOT A NAMESPACE", "k", 1, ttl_seconds=60)
    assert ei.value.status == 400
    assert ei.value.code == "kv_bad_request"
