"""
Pop autopilot, client side.

The broker owns a controller that sizes a pop from state this client cannot
see: how many partitions of the (queue, group) are ready, how old their oldest
ready message is, at what rate messages are arriving. Two knobs are under its
control -- ``partitions`` (the sweep width) and ``batch`` (the message budget
for the sweep).

THE RULE, and it is the only one: an explicit user value is sacred. Autopilot
applies ONLY to the knobs the user left unset, and it applies to them one by
one. A consumer that pins ``partitions(1)`` and says nothing about batch keeps
its single-partition claim forever and lets the broker size the batch; the
pinned dimension is never "adjusted", not even towards a value the controller
would consider better.

The wire shape follows the conflation precedent (see utils/conflation.py): a
client that is not engaging autopilot sends the byte-identical request it sent
before this feature existed.

    autopilot=true      emitted ONLY when at least one of the two knobs is
                        being left to the broker. Never as autopilot=false.
    partitions / batch  OMITTED for the dimensions the broker is choosing,
                        sent exactly as before for the ones the user set.

WHAT AN OLD BROKER DOES, and why there is no capability check here. A broker
older than 1.2 ignores unknown query params: the request succeeds, and the two
omitted knobs fall back to the SERVER-side defaults (batch 200, partitions 1)
instead of the old client-side ones. That is a sizing difference, not a
correctness one -- nothing is lost, misordered or delivered twice -- so unlike
conflation (which silently hands a last-value consumer a whole backlog, hence
ConflationUnsupportedError) this degrades quietly and on purpose. Callers who
need the old numbers against an old broker set them explicitly, or turn
autopilot off.
"""

import os
from typing import Any, Dict, NamedTuple, Optional

#: The environment variable that disables pop autopilot for a whole process:
#: ``QUEEN_SDK_POP_AUTOPILOT=off`` restores the client-side defaults this SDK
#: applied before autopilot existed, byte for byte. It is read once, in the
#: Queen constructor, so a single deployment can be rolled back without touching
#: code.
#:
#: "off", "false", "0", "no" and "disabled" all disable it (case-insensitive,
#: surrounding space ignored). Every other value, including the empty one,
#: leaves autopilot on.
ENV_POP_AUTOPILOT = "QUEEN_SDK_POP_AUTOPILOT"

_DISABLING_VALUES = frozenset({"off", "false", "0", "no", "disabled"})


def pop_autopilot_disabled_by_env() -> bool:
    """Whether ENV_POP_AUTOPILOT asks for the pre-autopilot behavior."""
    return os.environ.get(ENV_POP_AUTOPILOT, "").strip().lower() in _DISABLING_VALUES


class PopSizing(NamedTuple):
    """
    Which of the three sizing keys travel on one pop, and with what values.

    ``batch`` and ``partitions`` are the rendered strings, or None for "this key
    does not travel".
    """

    autopilot: bool
    batch: Optional[str]
    partitions: Optional[str]


def pop_sizing(
    batch: Optional[int],
    max_partitions: Optional[int],
    fallback_batch: int,
    autopilot: bool,
) -> PopSizing:
    """
    The batch/partitions/autopilot decision for one pop.

    IT EXISTS SO THERE IS EXACTLY ONE COPY OF THE EMISSION RULE. pop() and
    consume() build their query strings separately (QueueBuilder.pop's inline
    params vs ConsumerManager._build_params) and the two have drifted before --
    PLAN_CONFLATION §4 opens on a bug of exactly that shape. A rule with three
    branches and a per-dimension carve-out is precisely the kind that gets
    copied wrong, so both builders call this and then only PLACE what it
    returns; where each key sits in the query string stays with the builder,
    because the pre-autopilot key order is part of what "byte-identical" means.

    Args:
        batch: the USER's batch. None or 0 means unset -- the dimension the
            broker gets to choose. Neither builder may substitute a default
            before calling this.
        max_partitions: the USER's sweep width, same convention.
        fallback_batch: client-side default applied to an unset batch when
            autopilot is NOT engaged.
        autopilot: the resolved decision for this call.
    """
    batch_set = batch is not None and batch > 0
    partitions_set = max_partitions is not None and max_partitions > 0

    # Note the case that looks like an omission and is not: when the user set
    # BOTH knobs there is nothing left for the controller to decide, so
    # autopilot=true is NOT emitted and the request is byte-identical to the one
    # this SDK sent before autopilot existed. Sending the flag anyway would be
    # harmless on the broker and dishonest in a packet capture.
    if autopilot and not (batch_set and partitions_set):
        return PopSizing(
            autopilot=True,
            batch=str(batch) if batch_set else None,
            partitions=str(max_partitions) if partitions_set else None,
        )

    return PopSizing(
        autopilot=False,
        batch=str(batch if batch_set else fallback_batch),
        # The legacy gate: partitions travels only above 1, because 1 IS the
        # server-side default and a v4-era client never sent it.
        partitions=str(max_partitions) if partitions_set and max_partitions > 1 else None,
    )


class AutopilotDecision(NamedTuple):
    """
    What the broker chose for one pop, echoed back in the response under
    "autopilot" when the request engaged autopilot.

    Reading it is optional -- the messages are already sized by it -- but it is
    the only way to see the controller working from the client side, and the
    only input to the empty-poll pacing below.
    """

    #: Sweep width the broker used for this pop.
    partitions: int
    #: Message budget the broker used for this pop.
    batch: int
    #: The broker's advice on how long to wait before polling again (wire name:
    #: waitMs). Present only when the broker has an opinion, and it is advice,
    #: not a lease: the consume loop honors it for the sleep it was already
    #: taking between empty non-waiting pops, nothing more. 0 = no advice.
    wait_millis: int


def parse_autopilot_decision(result: Optional[Dict[str, Any]]) -> Optional[AutopilotDecision]:
    """
    Pull the additive "autopilot" object out of a decoded pop response,
    returning None when it is absent or not an object.

    Unknown keys inside it are ignored, and an unknown-shaped value is treated
    as absent rather than as an error: this field is the broker telling the
    client what it did, and a client that refuses to run because a newer broker
    grew a fourth number would be a self-inflicted outage.
    """
    if not isinstance(result, dict):
        return None
    raw = result.get("autopilot")
    if not isinstance(raw, dict):
        return None

    def num(value: Any) -> int:
        # bool is an int in Python and "partitions": true is not a number.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return 0
        return int(value)

    return AutopilotDecision(
        partitions=num(raw.get("partitions")),
        batch=num(raw.get("batch")),
        wait_millis=num(raw.get("waitMs")),
    )


#: The sleep the consume loop has always taken between two empty pops that are
#: NOT long-polling, in seconds. A waiting pop already blocks on the broker, so
#: it never reaches here.
EMPTY_POLL_BACKOFF_SECONDS = 0.1


def empty_poll_delay_seconds(decision: Optional[AutopilotDecision]) -> float:
    """
    How long to wait after an empty pop: the broker's advice when it gave one,
    the historical constant otherwise.

    The advice is honored as given, without a ceiling of this client's
    invention. The sleep it feeds is an ``asyncio.sleep`` inside a cancellable
    task, so even an absurd value cannot outlive a cancellation -- which is the
    only property that has to hold locally.
    """
    if decision is not None and decision.wait_millis > 0:
        return decision.wait_millis / 1000.0
    return EMPTY_POLL_BACKOFF_SECONDS
