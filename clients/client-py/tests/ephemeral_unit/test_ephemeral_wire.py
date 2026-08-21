"""
The ephemeral wire contract, asserted on the EXACT request -- no broker.

EPHEMERAL_QUEUES.md §3.1 is the authority for every method, path, query string
and body below, and this file is what keeps this SDK on it. The reason to assert
the request rather than the return value is the same one the kv suite gives: a
wrong shape does not raise anywhere useful. A push whose messages carried the
durable ``{queue, partition, payload}`` per item is a 400 nobody sees until a
live broker is involved; a pop that forgot to send ``timeout`` alongside
``wait=true`` is a long poll that returns on the BROKER's default instead of the
caller's, which nothing observes at all.

One more thing is pinned here that no end-to-end run against a 1.1 broker could
ever produce: the 404 mapping (§4). A broker or proxy older than 1.1 answers 404
on the whole family, and the SDK has to turn that into one clear
"upgrade" verdict rather than let it read as "your queue is missing" -- while a
1.1 broker's OWN 404, the one ``depth`` answers for a queue that is not there,
has to stay the second thing and never become the first.
"""

from __future__ import annotations

import pytest

from queen.ephemeral import (
    EPHEMERAL_QUEUE_NOT_FOUND,
    EPHEMERAL_UNSUPPORTED,
    EPHEMERAL_UNSUPPORTED_MESSAGE,
)
from queen.errors import EphemeralError, EphemeralQueueNotFoundError

from .envelopes import (
    OLD_BROKER,
    OLD_PROXY,
    QUEUE_NOT_FOUND,
    acked,
    frame,
    make,
    popped,
    pushed,
)

QUEUE = "inbox"


# ---------------------------------------------------------------------------
# Declaration: configure / reset / delete.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_configure_sends_the_queue_and_its_options_under_options():
    client, server = make()
    await client.ephemeral.configure(
        QUEUE,
        max_bytes=1048576,
        max_length=500,
        policy="dropOldest",
        ttl_seconds=30,
        lease_seconds=15,
        retry_limit=3,
        window_buffer={"ms": 20, "count": 50},
    )

    assert server.only.route == "POST /api/v1/ephemeral/configure"
    assert server.only.body == {
        "queue": QUEUE,
        "options": {
            "maxBytes": 1048576,
            "maxLength": 500,
            "policy": "dropOldest",
            "ttlSeconds": 30,
            "leaseSeconds": 15,
            "retryLimit": 3,
            "windowBuffer": {"ms": 20, "count": 50},
        },
    }
    await client.close()


@pytest.mark.asyncio
async def test_configure_sends_only_the_options_it_was_given():
    client, server = make()
    await client.ephemeral.configure(QUEUE, ttl_seconds=30)
    assert server.requests[0].body == {"queue": QUEUE, "options": {"ttlSeconds": 30}}

    await client.ephemeral.configure(QUEUE)
    assert server.requests[1].body == {"queue": QUEUE, "options": {}}
    await client.close()


@pytest.mark.asyncio
async def test_configure_accepts_the_wire_spelling_in_a_mapping():
    """A config file carries ``ttlSeconds``, a call site writes ``ttl_seconds``,
    and both mean the same knob."""
    client, server = make()
    await client.ephemeral.configure(QUEUE, {"ttlSeconds": 30, "maxLength": 10})
    assert server.only.body["options"] == {"maxLength": 10, "ttlSeconds": 30}
    await client.close()


@pytest.mark.asyncio
async def test_configure_refuses_an_option_this_client_does_not_know():
    """Refused, not dropped: every one of the seven knobs bounds something, and
    a silently ignored `ttlSecond` is a ring that grows until a global budget
    answers 503."""
    client, server = make()
    with pytest.raises(ValueError, match="ttlSecond"):
        await client.ephemeral.configure(QUEUE, {"ttlSecond": 30})
    with pytest.raises(ValueError, match="maxbytes"):
        await client.ephemeral.configure(QUEUE, maxbytes=10)
    assert server.requests == []
    await client.close()


@pytest.mark.asyncio
async def test_configure_refuses_the_same_knob_spelled_twice_with_two_values():
    client, server = make()
    with pytest.raises(ValueError, match="two values for ttlSeconds"):
        await client.ephemeral.configure(QUEUE, {"ttlSeconds": 30}, ttl_seconds=60)
    assert server.requests == []
    await client.close()


@pytest.mark.asyncio
async def test_reset_and_delete_name_the_queue_where_each_route_expects_it():
    client, server = make()
    await client.ephemeral.reset(QUEUE)
    assert server.requests[0].route == "POST /api/v1/ephemeral/reset"
    assert server.requests[0].body == {"queue": QUEUE}

    await client.ephemeral.delete(QUEUE)
    assert server.requests[1].route == f"DELETE /api/v1/ephemeral/queue/{QUEUE}"
    assert server.requests[1].body is None
    await client.close()


@pytest.mark.asyncio
async def test_percent_encodes_a_queue_name_that_would_change_the_path():
    # `raw_path`, not `path`: a slash left unencoded turns one queue name into
    # two path segments, and httpx's decoded view cannot tell those apart.
    client, server = make()
    await client.ephemeral.delete("rooms/7")
    assert server.requests[0].raw_path == "/api/v1/ephemeral/queue/rooms%2F7"

    await client.ephemeral.depth("rooms/7")
    assert server.requests[1].raw_path == "/api/v1/ephemeral/queues/rooms%2F7/depth"
    await client.close()


@pytest.mark.asyncio
async def test_refuses_a_missing_queue_name_before_spending_a_request():
    client, server = make()
    for call in (
        client.ephemeral.configure(""),
        client.ephemeral.reset(""),
        client.ephemeral.delete(""),
        client.ephemeral.push("", [{"a": 1}]),
        client.ephemeral.pop(""),
        client.ephemeral.ack("", ["e:1"]),
        client.ephemeral.depth(""),
    ):
        with pytest.raises(ValueError, match="non-empty string"):
            await call
    assert server.requests == []
    await client.close()


# ---------------------------------------------------------------------------
# Push.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_push_sends_the_flat_envelope_with_payload_only_messages():
    client, server = make([pushed(2)])
    result = await client.ephemeral.push(QUEUE, [{"a": 1}, {"a": 2}])

    assert server.only.route == "POST /api/v1/ephemeral/push"
    assert server.only.body == {
        "queue": QUEUE,
        "messages": [{"payload": {"a": 1}}, {"payload": {"a": 2}}],
    }
    assert result == {"pushed": 2}
    await client.close()


@pytest.mark.asyncio
async def test_push_omits_partition_unless_the_caller_named_one():
    client, server = make([pushed(1), pushed(1)])
    await client.ephemeral.push(QUEUE, [{"a": 1}])
    assert "partition" not in server.requests[0].body

    await client.ephemeral.push(QUEUE, [{"a": 1}], partition="room-7")
    assert server.requests[1].body == {
        "queue": QUEUE,
        "partition": "room-7",
        "messages": [{"payload": {"a": 1}}],
    }
    await client.close()


@pytest.mark.asyncio
async def test_push_accepts_the_durable_push_sugar():
    """A bare value, ``{"data": …}`` or ``{"payload": …}`` -- one mental model
    across both families, including the trap: a dict with a ``data`` key is read
    as the sugar and its other keys do not travel."""
    client, server = make([pushed(4)])
    await client.ephemeral.push(
        QUEUE, ["plain", 7, {"data": {"n": 1}}, {"payload": {"n": 2}}]
    )
    assert server.only.body["messages"] == [
        {"payload": "plain"},
        {"payload": 7},
        {"payload": {"n": 1}},
        {"payload": {"n": 2}},
    ]
    await client.close()


@pytest.mark.asyncio
async def test_push_carries_no_transaction_id():
    """There is no dedup index on this engine to hold one (§9)."""
    client, server = make([pushed(1)])
    await client.ephemeral.push(QUEUE, [{"payload": {"n": 1}, "transactionId": "t-1"}])
    assert list(server.only.body["messages"][0].keys()) == ["payload"]
    await client.close()


@pytest.mark.asyncio
async def test_push_of_nothing_answers_pushed_zero_without_spending_a_request():
    client, server = make()
    assert await client.ephemeral.push(QUEUE, []) == {"pushed": 0}
    assert server.requests == []
    await client.close()


@pytest.mark.asyncio
async def test_push_refuses_none_rather_than_inventing_a_null_payload():
    client, server = make()
    with pytest.raises(ValueError, match='"payload": None'):
        await client.ephemeral.push(QUEUE, [None])
    assert server.requests == []

    # ... and the explicit form does travel, because null IS a legal payload.
    server.plan.append(pushed(1))
    await client.ephemeral.push(QUEUE, [{"payload": None}])
    assert server.only.body["messages"] == [{"payload": None}]
    await client.close()


# ---------------------------------------------------------------------------
# Pop.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pop_sends_the_queue_and_nothing_else_by_default():
    client, server = make([popped(QUEUE)])
    await client.ephemeral.pop(QUEUE)
    assert server.only.path == "/api/v1/ephemeral/pop"
    assert server.only.query == {"queue": [QUEUE]}
    await client.close()


@pytest.mark.asyncio
async def test_pop_puts_every_declared_parameter_on_the_query_string():
    client, server = make([popped(QUEUE)])
    await client.ephemeral.pop(
        QUEUE,
        partition="room-7",
        batch=10,
        wait=True,
        timeout=1500,
        group="workers",
        auto_ack=True,
    )
    # The order is §3.1's, so a query read out of an access log is the query the
    # plan documents.
    assert server.only.query_string == (
        "queue=inbox&partition=room-7&batch=10&wait=true&timeout=1500"
        "&group=workers&autoAck=true"
    )
    await client.close()


@pytest.mark.asyncio
async def test_pop_sends_an_explicit_timeout_whenever_it_waits():
    """And none when it does not: a plain pop leaves every default to the
    broker, while a long poll states the deadline it is holding the client's
    socket open for."""
    client, server = make([popped(QUEUE), popped(QUEUE)])
    await client.ephemeral.pop(QUEUE, wait=True)
    assert server.requests[0].query["wait"] == ["true"]
    assert server.requests[0].query["timeout"] == ["30000"]

    await client.ephemeral.pop(QUEUE, batch=5)
    assert "wait" not in server.requests[1].query
    assert "timeout" not in server.requests[1].query
    await client.close()


@pytest.mark.asyncio
async def test_pop_accepts_timeout_millis_and_refuses_both_spellings():
    client, server = make([popped(QUEUE)])
    await client.ephemeral.pop(QUEUE, wait=True, timeout_millis=2500)
    assert server.only.query["timeout"] == ["2500"]

    with pytest.raises(ValueError, match="not both"):
        await client.ephemeral.pop(QUEUE, wait=True, timeout=1, timeout_millis=2)
    assert len(server.requests) == 1
    await client.close()


@pytest.mark.asyncio
async def test_pop_returns_an_empty_list_on_a_timeout_and_on_a_bodiless_204():
    client, server = make(
        [popped(QUEUE), {"status": 204, "json": None}, popped(QUEUE, [frame(1)])]
    )

    assert await client.ephemeral.pop(QUEUE, wait=True) == {"queue": QUEUE, "messages": []}
    # A 204 parses to None in this client; it must not reach the caller as one,
    # or every `for m in result["messages"]` is a TypeError on an idle queue.
    assert await client.ephemeral.pop(QUEUE) == {"queue": QUEUE, "messages": []}

    delivered = await client.ephemeral.pop(QUEUE)
    assert [m["id"] for m in delivered["messages"]] == ["e:beef:Default:1"]
    await client.close()


# ---------------------------------------------------------------------------
# Ack.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_sends_the_ids_under_acks_with_the_group_beside_them():
    client, server = make([acked({"id": "e:beef:Default:1", "outcome": "acked"})])
    await client.ephemeral.ack(QUEUE, ["e:beef:Default:1"], group="workers")

    assert server.only.route == "POST /api/v1/ephemeral/ack"
    assert server.only.body == {
        "queue": QUEUE,
        "group": "workers",
        "acks": [{"id": "e:beef:Default:1"}],
    }
    await client.close()


@pytest.mark.asyncio
async def test_ack_takes_popped_messages_bare_ids_or_the_wire_objects():
    client, server = make()
    await client.ephemeral.ack(QUEUE, frame(9))
    assert server.requests[0].body["acks"] == [{"id": "e:beef:Default:9"}]

    await client.ephemeral.ack(
        QUEUE, ["e:beef:Default:1", {"id": "e:beef:Default:2", "status": "retry"}]
    )
    assert server.requests[1].body["acks"] == [
        {"id": "e:beef:Default:1"},
        {"id": "e:beef:Default:2", "status": "retry"},
    ]
    await client.close()


@pytest.mark.asyncio
async def test_ack_maps_the_boolean_sugar_and_lets_a_per_message_status_win():
    client, server = make()
    await client.ephemeral.ack(QUEUE, ["e:1"], status=False)
    assert server.requests[0].body["acks"] == [{"id": "e:1", "status": "failed"}]

    await client.ephemeral.ack(
        QUEUE,
        ["e:1", {"id": "e:2", "status": "retry", "error": "downstream 503"}],
        status=True,
    )
    assert server.requests[1].body["acks"] == [
        {"id": "e:1", "status": "completed"},
        {"id": "e:2", "status": "retry", "error": "downstream 503"},
    ]
    await client.close()


@pytest.mark.asyncio
async def test_ack_omits_group_in_queue_mode_and_refuses_an_ack_with_no_id():
    client, server = make()
    await client.ephemeral.ack(QUEUE, ["e:1"])
    assert "group" not in server.only.body

    with pytest.raises(ValueError, match="carries no message id"):
        await client.ephemeral.ack(QUEUE, [{"payload": {"n": 1}}])
    assert len(server.requests) == 1
    await client.close()


@pytest.mark.asyncio
async def test_ack_of_nothing_answers_empty_results_without_spending_a_request():
    client, server = make()
    assert await client.ephemeral.ack(QUEUE, []) == {"results": []}
    assert server.requests == []
    await client.close()


# ---------------------------------------------------------------------------
# Status.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queues_and_depth_are_plain_gets_on_the_status_routes():
    client, server = make(
        [
            {"status": 200, "json": {"queues": []}},
            {"status": 200, "json": {"queue": QUEUE, "depth": 0}},
        ]
    )
    await client.ephemeral.queues()
    assert server.requests[0].route == "GET /api/v1/ephemeral/queues"
    assert server.requests[0].query == {}

    await client.ephemeral.depth(QUEUE)
    assert server.requests[1].route == f"GET /api/v1/ephemeral/queues/{QUEUE}/depth"
    await client.close()


# ---------------------------------------------------------------------------
# The two kinds of 404 (§4, §8).
#
# The status alone cannot tell them apart, which is exactly why the mapping
# reads the body's CODE:
#
#   * no SDK negotiates a version, so a pre-1.1 broker (routes never registered)
#     and a pre-1.1 proxy (`route_blocked`, fails closed on unknown API paths)
#     both answer 404, and to a caller those are one fact: upgrade;
#   * a broker that DOES support the family answers 404 with
#     `ephemeral_queue_not_found` when `depth` names a queue that is not there.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_maps_a_missing_broker_route_to_the_one_clear_error():
    client, server = make([OLD_BROKER])
    with pytest.raises(EphemeralError) as caught:
        await client.ephemeral.push(QUEUE, [{"a": 1}])

    assert caught.value.code == EPHEMERAL_UNSUPPORTED
    assert caught.value.status == 404
    assert str(caught.value).endswith(EPHEMERAL_UNSUPPORTED_MESSAGE)
    await client.close()


@pytest.mark.asyncio
async def test_maps_the_old_proxy_route_blocked_to_the_same_error():
    """And keeps the original as ``__cause__``: "the proxy answered
    route_blocked" is the evidence for "upgrade", and an SDK that threw the
    evidence away would leave the operator with a claim and no proof."""
    client, server = make([OLD_PROXY])
    with pytest.raises(EphemeralError) as caught:
        await client.ephemeral.pop(QUEUE)

    assert caught.value.code == EPHEMERAL_UNSUPPORTED
    assert caught.value.__cause__ is not None
    assert caught.value.__cause__.response.json()["code"] == "route_blocked"
    await client.close()


@pytest.mark.asyncio
async def test_every_verb_of_the_family_maps_the_404():
    """Eight verbs, one verdict. A family where six routes say "upgrade" and two
    say "HTTP 404" is a family somebody will branch on by accident."""
    client, server = make([OLD_BROKER] * 8)
    calls = [
        client.ephemeral.configure(QUEUE),
        client.ephemeral.reset(QUEUE),
        client.ephemeral.delete(QUEUE),
        client.ephemeral.push(QUEUE, [{"a": 1}]),
        client.ephemeral.pop(QUEUE),
        client.ephemeral.ack(QUEUE, ["e:1"]),
        client.ephemeral.queues(),
        client.ephemeral.depth(QUEUE),
    ]
    for call in calls:
        with pytest.raises(EphemeralError) as caught:
            await call
        assert caught.value.code == EPHEMERAL_UNSUPPORTED
    assert len(server.requests) == 8
    await client.close()


@pytest.mark.asyncio
async def test_a_404_for_a_missing_queue_is_its_own_error():
    """Not "your broker is too old".

    ``depth`` is the only verb that can answer a real 404 -- push and pop create
    implicitly, ``reset`` answers ``dropped:0``, ``delete`` answers
    ``deleted:false`` -- and collapsing it into the version verdict would send
    somebody chasing a broker version over a queue name typo.
    """
    client, server = make([QUEUE_NOT_FOUND])
    with pytest.raises(EphemeralQueueNotFoundError) as caught:
        await client.ephemeral.depth(QUEUE)

    assert caught.value.code == EPHEMERAL_QUEUE_NOT_FOUND
    assert caught.value.status == 404
    assert caught.value.queue == QUEUE, "the error names the queue that was not found"
    assert "does not exist" in str(caught.value)
    assert "1.1" not in str(caught.value), "a missing queue must not read as a version problem"
    # Nothing the HTTP layer surfaced is lost by the mapping.
    assert caught.value.__cause__ is not None
    assert caught.value.__cause__.response.json()["code"] == EPHEMERAL_QUEUE_NOT_FOUND
    assert server.only.route == f"GET /api/v1/ephemeral/queues/{QUEUE}/depth"
    await client.close()


@pytest.mark.asyncio
async def test_tells_the_two_404s_apart_on_the_same_verb():
    """By the BODY and not the status, which is all they have in common.

    The regression this pins: ``depth`` answering a real 404 while the routes are
    demonstrably present, because the very next call proves an old broker reads
    differently on the same verb.
    """
    client, server = make([QUEUE_NOT_FOUND, OLD_BROKER])

    with pytest.raises(EphemeralError) as missing:
        await client.ephemeral.depth(QUEUE)
    assert missing.value.code == EPHEMERAL_QUEUE_NOT_FOUND

    with pytest.raises(EphemeralError) as old:
        await client.ephemeral.depth(QUEUE)
    assert old.value.code == EPHEMERAL_UNSUPPORTED
    assert not isinstance(old.value, EphemeralQueueNotFoundError)

    # And the narrower type is still catchable as the family's error, so an
    # existing `except EphemeralError` keeps working.
    assert isinstance(missing.value, EphemeralQueueNotFoundError)
    assert len(server.requests) == 2
    await client.close()


@pytest.mark.asyncio
async def test_leaves_every_other_refusal_alone_code_and_status_intact():
    """429 `queue_full` is the per-queue bound doing its job (§1.6) and 403 is
    the grant; neither is a version verdict and neither may be dressed up as
    one."""
    client, server = make(
        [
            {"status": 429, "json": {"error": "queue full", "code": "queue_full"}},
            {"status": 403, "json": {"error": "not granted", "code": "feature_gated"}},
        ],
        retry_429={"max_attempts": 1},
    )

    with pytest.raises(EphemeralError) as full:
        await client.ephemeral.push(QUEUE, [{"a": 1}])
    assert (full.value.status, full.value.code) == (429, "queue_full")

    with pytest.raises(EphemeralError) as gated:
        await client.ephemeral.push(QUEUE, [{"a": 1}])
    assert (gated.value.status, gated.value.code) == (403, "feature_gated")
    await client.close()
