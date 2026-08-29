"""
Pop autopilot, client side, asserted with no broker.

The four things a client can be wrong about here, and why each is asserted
against the WHOLE query string rather than against one parameter:

1. BOTH BUILDERS MUST AGREE. pop() and consume() assemble their query strings
   separately (QueueBuilder.pop's inline params vs
   ConsumerManager._build_params) -- the hazard PLAN_CONFLATION §4 opens on by
   name, and the one this SDK already carries a scar from. Every case below is
   run through both, and both are compared to the same expected string, so a
   rule implemented in one and not the other cannot pass.
2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
   hatch is only worth having if it is exact, and "exact" is not something a
   test of one parameter can show: a stray autopilot=true, or a batch that
   stopped being emitted, is a different request. Hence full-string equality
   including the parameters this feature never touches, and including their
   ORDER -- this SDK builds a dict and urlencodes it, so order is part of the
   bytes.
3. AN EXPLICIT VALUE IS SACRED, PER DIMENSION. partitions(1) and "never called
   partitions" both used to reach the wire as nothing at all; they are now
   different requests, and the pinned one must survive autopilot.
4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does not
   send it, sends it half-filled, or sends it with fields this SDK has never
   heard of, all have to work.

The transport comes from the conflation suite next door: it is the one scripted
pop seam this SDK has, and duplicating it would leave two harnesses to keep in
step over the same route.
"""

from __future__ import annotations

import asyncio

import pytest

from queen import Queen
from queen.utils.autopilot import (
    EMPTY_POLL_BACKOFF_SECONDS,
    ENV_POP_AUTOPILOT,
    AutopilotDecision,
    empty_poll_delay_seconds,
    parse_autopilot_decision,
    pop_autopilot_disabled_by_env,
    pop_sizing,
)

from ..conflation_unit.pop_transport import PopTransport, message, pop_body

CONSUME_TIMEOUT_S = 5.0

# The shared spine of every case: a named queue and group, no long poll, default
# timeout. Everything that varies below is sizing.
TAIL = "wait=false&timeout=30000&consumerGroup=workers"


def make(*pop_plan, default_pop=None):
    transport = PopTransport(*pop_plan, default_pop=default_pop)
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)
    return client, transport


def base(client):
    return client.queue("orders").group("workers").wait(False)


# (name, build, expected query string)
CASES = [
    # (a) nothing set: both knobs go to the broker, neither travels.
    ("nothing set", lambda qb: qb, f"autopilot=true&{TAIL}"),
    # (b) partitions pinned, batch left to the broker.
    ("partitions only", lambda qb: qb.partitions(4), f"autopilot=true&{TAIL}&partitions=4"),
    # (b') the pin that used to be indistinguishable from unset. partitions(1)
    # is a decision -- hold this consumer to one partition -- and the broker has
    # to be told, or autopilot would widen it.
    ("partitions pinned to one", lambda qb: qb.partitions(1), f"autopilot=true&{TAIL}&partitions=1"),
    # (c) batch pinned, sweep width left to the broker.
    ("batch only", lambda qb: qb.batch(50), f"autopilot=true&batch=50&{TAIL}"),
    # (d) both set: nothing left to decide, so no autopilot parameter and the
    # exact request the pre-autopilot SDK sent.
    ("both set", lambda qb: qb.batch(50).partitions(4), f"batch=50&{TAIL}&partitions=4"),
    # (d') both set with partitions at 1: still byte-identical to the old SDK,
    # which never emitted partitions=1.
    ("both set, partitions one", lambda qb: qb.batch(50).partitions(1), f"batch=50&{TAIL}"),
    # (e) escape hatch, nothing set: the client-side defaults are back.
    ("autopilot off, nothing set", lambda qb: qb.autopilot(False), f"batch=1&{TAIL}"),
    # (e') escape hatch with a pin: partitions=1 stays off the wire, exactly as
    # before autopilot existed.
    ("autopilot off, partitions one", lambda qb: qb.autopilot(False).partitions(1), f"batch=1&{TAIL}"),
    (
        "autopilot off, both set",
        lambda qb: qb.autopilot(False).batch(50).partitions(4),
        f"batch=50&{TAIL}&partitions=4",
    ),
    # autopilot(True) is the default, spelled out. It must not change anything,
    # including for a caller who set both knobs.
    (
        "autopilot explicitly on, both set",
        lambda qb: qb.autopilot(True).batch(50).partitions(4),
        f"batch=50&{TAIL}&partitions=4",
    ),
    # batch(0) is not "a batch of zero" and never was: it is the absence of an
    # opinion, which now means the broker decides.
    ("batch zero is unset", lambda qb: qb.batch(0), f"autopilot=true&{TAIL}"),
]


# ---------------------------------------------------------------------------
# 1. Param assembly, from both builders.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,build,want", CASES, ids=[c[0] for c in CASES])
@pytest.mark.asyncio
async def test_pop_param_assembly(name, build, want):
    client, transport = make(pop_body([message()]))
    try:
        await build(base(client)).pop()
    finally:
        await client.close()

    assert len(transport.pops) == 1
    assert transport.pops[0].raw_query == want


@pytest.mark.parametrize("name,build,want", CASES, ids=[c[0] for c in CASES])
@pytest.mark.asyncio
async def test_consume_param_assembly(name, build, want):
    client, transport = make(pop_body([message()]))
    seen = []

    async def handler(msg):
        seen.append(msg)

    try:
        await asyncio.wait_for(
            build(base(client)).limit(1).consume(handler), CONSUME_TIMEOUT_S
        )
    finally:
        await client.close()

    assert len(seen) == 1
    assert transport.pops[0].raw_query == want


# ---------------------------------------------------------------------------
# 2. The process-wide rollback.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_env_var_disables_autopilot(monkeypatch):
    monkeypatch.setenv(ENV_POP_AUTOPILOT, "off")
    client, transport = make(pop_body([message()]))
    try:
        await base(client).pop()
    finally:
        await client.close()

    assert transport.pops[0].raw_query == f"batch=1&{TAIL}"


@pytest.mark.asyncio
async def test_env_var_is_read_once_at_construction(monkeypatch):
    """A deployment-level rollback, not a per-request switch."""
    client, transport = make(pop_body([message()]))
    monkeypatch.setenv(ENV_POP_AUTOPILOT, "off")
    try:
        await base(client).pop()
    finally:
        await client.close()

    assert transport.pops[0].raw_query == f"autopilot=true&{TAIL}"


@pytest.mark.asyncio
async def test_explicit_autopilot_outranks_the_environment(monkeypatch):
    monkeypatch.setenv(ENV_POP_AUTOPILOT, "off")
    client, transport = make(pop_body([message()]))
    try:
        await base(client).autopilot(True).pop()
    finally:
        await client.close()

    assert transport.pops[0].raw_query == f"autopilot=true&{TAIL}"


def test_env_var_vocabulary(monkeypatch):
    for value in ("off", "OFF", " off ", "false", "0", "no", "disabled"):
        monkeypatch.setenv(ENV_POP_AUTOPILOT, value)
        assert pop_autopilot_disabled_by_env(), f"{value!r} should disable autopilot"
    for value in ("", "on", "true", "1", "yes", "nonsense"):
        monkeypatch.setenv(ENV_POP_AUTOPILOT, value)
        assert not pop_autopilot_disabled_by_env(), f"{value!r} should leave autopilot on"
    monkeypatch.delenv(ENV_POP_AUTOPILOT, raising=False)
    assert not pop_autopilot_disabled_by_env()


# ---------------------------------------------------------------------------
# 3. The additive response field.
# ---------------------------------------------------------------------------


def test_parse_autopilot_decision():
    assert parse_autopilot_decision(None) is None
    assert parse_autopilot_decision({"messages": []}) is None, "absent"
    assert parse_autopilot_decision({"autopilot": None}) is None, "null"
    assert parse_autopilot_decision({"autopilot": True}) is None, "not an object"
    assert parse_autopilot_decision({"autopilot": []}) is None, "a list is not an object"

    assert parse_autopilot_decision(
        {"autopilot": {"partitions": 8, "batch": 200, "waitMs": 25}}
    ) == AutopilotDecision(partitions=8, batch=200, wait_millis=25)

    # waitMs is optional: the broker sends it only when it has an opinion.
    assert parse_autopilot_decision(
        {"autopilot": {"partitions": 4, "batch": 64}}
    ) == AutopilotDecision(partitions=4, batch=64, wait_millis=0)

    # Forward compatibility: a newer broker growing a field must not cost this
    # client the fields it does understand.
    assert parse_autopilot_decision(
        {"autopilot": {"partitions": 2, "batch": 10, "waitMs": 5, "reason": "ready_age"}}
    ) == AutopilotDecision(partitions=2, batch=10, wait_millis=5)

    # A field of the wrong type is dropped, not fatal.
    assert parse_autopilot_decision(
        {"autopilot": {"partitions": "eight", "batch": 10}}
    ) == AutopilotDecision(partitions=0, batch=10, wait_millis=0)


@pytest.mark.asyncio
async def test_pop_result_reports_what_the_broker_chose():
    body = pop_body([message()])
    body["autopilot"] = {"partitions": 8, "batch": 200, "waitMs": 25}
    client, _ = make(body)
    try:
        res = await base(client).pop_result()
    finally:
        await client.close()

    assert len(res.messages) == 1
    assert res.autopilot == AutopilotDecision(partitions=8, batch=200, wait_millis=25)


@pytest.mark.asyncio
async def test_pop_result_is_none_when_the_broker_said_nothing():
    """A 1.1 broker, or a pop that never asked."""
    client, _ = make(pop_body([message()]))
    try:
        res = await base(client).pop_result()
    finally:
        await client.close()

    assert len(res.messages) == 1
    assert res.autopilot is None


@pytest.mark.asyncio
async def test_pop_still_returns_a_bare_list():
    body = pop_body([message()])
    body["autopilot"] = {"partitions": 8, "batch": 200}
    client, _ = make(body)
    try:
        messages = await base(client).pop()
    finally:
        await client.close()

    assert isinstance(messages, list)
    assert len(messages) == 1


# ---------------------------------------------------------------------------
# 4. Empty-poll pacing.
# ---------------------------------------------------------------------------


def test_empty_poll_delay():
    assert empty_poll_delay_seconds(None) == EMPTY_POLL_BACKOFF_SECONDS
    assert (
        empty_poll_delay_seconds(AutopilotDecision(1, 1, 0)) == EMPTY_POLL_BACKOFF_SECONDS
    )
    assert empty_poll_delay_seconds(AutopilotDecision(1, 1, 250)) == 0.25


# ---------------------------------------------------------------------------
# 5. The rule itself, in isolation.
# ---------------------------------------------------------------------------


def test_pop_sizing_leaves_a_pinned_dimension_alone():
    assert pop_sizing(None, None, 1, True) == (True, None, None)
    assert pop_sizing(50, None, 1, True) == (True, "50", None)
    assert pop_sizing(None, 1, 1, True) == (True, None, "1")
    # Both set: nothing to decide, so the flag does not travel either.
    assert pop_sizing(50, 4, 1, True) == (False, "50", "4")
    # Off: the client-side default comes back and partitions keeps its >1 gate.
    assert pop_sizing(None, None, 1, False) == (False, "1", None)
    assert pop_sizing(None, 1, 1, False) == (False, "1", None)
    assert pop_sizing(None, 4, 1, False) == (False, "1", "4")


# ---------------------------------------------------------------------------
# 6. The streams runtime pins its width.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_streams_runner_pins_max_partitions_even_at_one():
    """
    Skipping the call for max_partitions == 1 used to be a harmless optimisation
    -- 1 was what an omitted `partitions` meant on the wire -- but under
    autopilot an omitted `partitions` means "broker, you choose", which would
    widen a query that explicitly asked for one partition per cycle.
    """
    from queen.streams.runtime.runner import Runner

    calls = []

    class RecordingSource:
        def batch(self, v):
            calls.append(("batch", v))
            return self

        def wait(self, v):
            calls.append(("wait", v))
            return self

        def timeout_millis(self, v):
            calls.append(("timeout_millis", v))
            return self

        def group(self, v):
            calls.append(("group", v))
            return self

        def partitions(self, v):
            calls.append(("partitions", v))
            return self

        async def pop(self):
            return []

    class FakeStream:
        source = RecordingSource()

    runner = Runner.__new__(Runner)
    runner.stream = FakeStream()
    runner.batch_size = 100
    runner.max_wait_millis = 1000
    runner.consumer_group = "stream-g"
    runner.max_partitions = 1
    runner.subscription_mode = None
    runner.subscription_from = None
    runner.conflation = False

    await runner._pop_messages()

    assert [c for c in calls if c[0] == "partitions"] == [("partitions", 1)]
