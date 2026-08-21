"""
The conflation wire contract, asserted with no broker (PLAN_CONFLATION §4, §7.2).

Three things are pinned here, and they are the three §7.2 asks of every SDK:

1. `conflation` reaches the query string from BOTH builders. The plan opens §4
   with the reason: "The pop() and consume() param builders are separate code in
   every SDK except Rust", and this SDK already carries the scar -- `partitions`
   / maxPartitions is missing from Python's CONSUME_DEFAULTS entirely, which is
   exactly the drift a two-sided test would have caught.
2. Degrade-loudly. No SDK does capability negotiation, so a new SDK against an
   old broker would send `conflation=true`, get it ignored, and silently drain
   the whole backlog. §4's blockquote makes that an ERROR on the FIRST response,
   detectable because the broker echoes `"conflation":true` on empty pops too.
3. Exactly one conflict warning per (queue, group) per process (§3.3 item 3).
   The stored group policy wins; a mismatched fleet must not flood.
"""

from __future__ import annotations

import asyncio

import pytest

from queen import Queen
from queen.errors import ConflationUnsupportedError
from queen.utils import conflation as conflation_warnings
from queen.utils.defaults import CONSUME_DEFAULTS

from .pop_transport import PopTransport, message, pop_body

# The consume loop must be bounded in every test: a degrade-loudly regression
# that swallows the error shows up as a HANG, and an unbounded await would turn
# a red test into a stuck suite.
CONSUME_TIMEOUT_S = 5.0


@pytest.fixture(autouse=True)
def reset_conflict_registry():
    """The 'warn once' registry is process-wide by design (§3.3), so tests must
    not inherit each other's state."""
    conflation_warnings.reset_conflict_warnings()
    yield
    conflation_warnings.reset_conflict_warnings()


def make(*pop_plan, default_pop=None):
    transport = PopTransport(*pop_plan, default_pop=default_pop)
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)
    return client, transport


async def consume_once(builder, handler, timeout: float = CONSUME_TIMEOUT_S):
    """Await a consume that is expected to stop on its own."""
    return await asyncio.wait_for(builder.consume(handler), timeout)


# ---------------------------------------------------------------------------
# 1a. The option exists at all, and it is in the defaults table.
# ---------------------------------------------------------------------------


def test_conflation_is_in_consume_defaults():
    """The §4 drift note, turned into an assertion: maxPartitions never made it
    into this table, so the consume path had no default to read and the option
    lived only on the builder. conflation does not repeat that."""
    assert "conflation" in CONSUME_DEFAULTS
    assert CONSUME_DEFAULTS["conflation"] is False, "default OFF (§1.1)"


# ---------------------------------------------------------------------------
# 1b. pop() puts it on the wire.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pop_sends_conflation_on_the_query_string():
    client, transport = make(pop_body(conflation=True))
    try:
        await client.queue("orders").group("workers").conflation().pop()
    finally:
        await client.close()

    assert len(transport.pops) == 1
    popped = transport.pops[0]
    assert popped.route == "GET /api/v1/pop/queue/orders"
    assert popped.param("conflation") == "true"
    assert popped.param("consumerGroup") == "workers"


@pytest.mark.asyncio
async def test_pop_omits_conflation_when_not_requested():
    """Byte-identical wire for every consumer that does not opt in (§8)."""
    client, transport = make(pop_body())
    try:
        await client.queue("orders").group("workers").pop()
    finally:
        await client.close()

    assert transport.pops[0].param("conflation") is None


@pytest.mark.asyncio
async def test_pop_conflation_false_is_not_sent():
    """Only Some(true) is emitted, mirroring the autoAck precedent (§3.2)."""
    client, transport = make(pop_body())
    try:
        await client.queue("orders").group("workers").conflation(False).pop()
    finally:
        await client.close()

    assert transport.pops[0].param("conflation") is None


# ---------------------------------------------------------------------------
# 1c. consume() puts it on the wire -- the SEPARATE param builder.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consume_sends_conflation_on_the_query_string():
    client, transport = make(pop_body([message()], conflation=True))
    seen = []

    async def handler(msg):
        seen.append(msg)

    try:
        await consume_once(
            client.queue("orders").group("workers").conflation().wait(False).limit(1),
            handler,
        )
    finally:
        await client.close()

    assert len(seen) == 1, "the conflated frame was handed to the handler"
    assert transport.pops[0].param("conflation") == "true"
    assert transport.pops[0].param("consumerGroup") == "workers"


@pytest.mark.asyncio
async def test_consume_omits_conflation_when_not_requested():
    client, transport = make(pop_body([message()]))

    async def handler(msg):
        pass

    try:
        await consume_once(
            client.queue("orders").group("workers").wait(False).limit(1),
            handler,
        )
    finally:
        await client.close()

    assert transport.pops[0].param("conflation") is None


# ---------------------------------------------------------------------------
# 2. Degrade loudly: the broker did not apply what we asked for.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pop_raises_when_the_broker_does_not_echo_conflation():
    """An old broker ignores the unknown query param and answers a normal pop.

    pop() swallows every other failure into [] on purpose; this one must NOT be
    swallowed, or the silent-backlog-drain is back with an empty list in front
    of it.
    """
    client, transport = make(pop_body([message()]))  # no "conflation" key
    try:
        with pytest.raises(ConflationUnsupportedError) as raised:
            await client.queue("orders").group("workers").conflation().pop()
    finally:
        await client.close()

    assert "requires broker >= 1.1.0" in str(raised.value)
    assert len(transport.pops) == 1, "it fires on the FIRST response"


@pytest.mark.asyncio
async def test_pop_raises_on_an_empty_response_too():
    """The broker echoes the key on empty pops as well, so the check fires
    before a single message has been processed (§4)."""
    client, _ = make(pop_body())  # empty AND unconflated
    try:
        with pytest.raises(ConflationUnsupportedError):
            await client.queue("orders").group("workers").conflation().pop()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_consume_stops_the_loop_when_the_broker_does_not_echo():
    client, transport = make(default_pop=pop_body([message()]))
    seen = []

    async def handler(msg):
        seen.append(msg)

    try:
        with pytest.raises(ConflationUnsupportedError):
            await consume_once(
                client.queue("orders").group("workers").conflation().wait(False),
                handler,
            )
    finally:
        await client.close()

    assert len(transport.pops) == 1, "the loop stopped on the first response"
    assert seen == [], "no message was processed before the error"


@pytest.mark.asyncio
async def test_no_error_when_conflation_was_never_requested():
    """Old broker + old-shaped call = unchanged. The check keys off what THIS
    consumer asked for, never off the response alone."""
    client, transport = make(pop_body([message()]))
    try:
        messages = await client.queue("orders").group("workers").pop()
    finally:
        await client.close()

    assert len(messages) == 1
    assert len(transport.pops) == 1


# ---------------------------------------------------------------------------
# 3. Declaration conflict: the stored group policy wins, loudly, ONCE.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_conflict_warns_exactly_once_per_queue_group(capsys):
    """§3.3: rejecting would break rolling deploys, so the SDK warns. Warning on
    EVERY response would flood a mismatched fleet at pop rate, so it warns once
    per (queue, group) per process."""
    conflicted = pop_body(conflation=True, conflict=True)
    client, transport = make(default_pop=conflicted)
    try:
        for _ in range(3):
            await client.queue("orders").group("workers").conflation().pop()
    finally:
        await client.close()

    assert len(transport.pops) == 3
    err = capsys.readouterr().err
    assert err.count("conflation conflict") == 1, err


@pytest.mark.asyncio
async def test_conflict_warns_again_for_a_different_group(capsys):
    """The key is (queue, group): a second disagreeing group is a second fact."""
    conflicted = pop_body(conflation=True, conflict=True)
    client, _ = make(default_pop=conflicted)
    try:
        await client.queue("orders").group("workers").conflation().pop()
        await client.queue("orders").group("auditors").conflation().pop()
        await client.queue("orders").group("auditors").conflation().pop()
    finally:
        await client.close()

    err = capsys.readouterr().err
    assert err.count("conflation conflict") == 2, err


@pytest.mark.asyncio
async def test_conflict_does_not_stop_the_consumer(capsys):
    """A conflict is a warning, never an error: the stored policy is applied and
    both consumers keep working (§3.3 / E2E-4)."""
    client, _ = make(pop_body([message()], conflation=True, conflict=True))
    seen = []

    async def handler(msg):
        seen.append(msg)

    try:
        await consume_once(
            client.queue("orders").group("workers").conflation().wait(False).limit(1),
            handler,
        )
    finally:
        await client.close()

    assert len(seen) == 1
    assert capsys.readouterr().err.count("conflation conflict") == 1


@pytest.mark.asyncio
async def test_no_conflict_warning_when_the_response_agrees(capsys):
    client, _ = make(default_pop=pop_body(conflation=True))
    try:
        await client.queue("orders").group("workers").conflation().pop()
        await client.queue("orders").group("workers").conflation().pop()
    finally:
        await client.close()

    assert "conflation conflict" not in capsys.readouterr().err
