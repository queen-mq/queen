"""
Drain sinks: WHERE a buffered batch goes, and in WHAT shape.

EPHEMERAL_QUEUES.md §4.1. The buffer machinery -- the blocking bound at
``max_size``, one drain per address, a failed batch back at the FRONT and
retried until it lands -- is about ordering, occupancy and loss. None of that is
durable-specific and none of it is worth writing twice, so the drain takes a
SINK instead of a hardcoded POST:

    Sink(name, path, format(queue, partition, batch) -> body)

``format`` receives the queue and the partition because the two storage classes
disagree about where that identity lives on the wire, and that disagreement is
the entire reason this parameter exists:

  * the DURABLE push wire repeats ``{queue, partition}`` on EVERY item, so the
    envelope is just ``{items}`` and the sink ignores both arguments;
  * the EPHEMERAL push wire hoists them to the envelope --
    ``{queue, partition?, messages:[{payload}...]}`` -- so the batch elements
    carry nothing but their payload.

DURABLE_SINK IS TODAY'S REQUEST, BYTE FOR BYTE. It is what a buffer created
without a destination drains into, which is every caller that existed before
ephemeral queues did, and tests/ephemeral_unit/test_durable_sink_pin.py exists
for no other reason than to fail if that ever stops being true.

ADDRESSES ARE NAMESPACED. A buffer address is the key of the one-buffer-one-
drain registry, so an ephemeral ``orders`` and a durable ``orders`` must not
land on the same entry -- they are unrelated objects (EPHEMERAL_QUEUES.md §10
Q8) and a shared buffer would post one family's messages to the other family's
route. The ``eph:`` prefix is the same namespacing the broker applies to its own
queue keys (§3.2), for the same reason.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, NamedTuple, Optional


class Sink(NamedTuple):
    """A route plus the envelope its batches travel in."""

    name: str
    path: str
    format: Callable[[Optional[str], Optional[str], List[Dict[str, Any]]], Dict[str, Any]]


class Destination(NamedTuple):
    """What a buffer drains into: the sink, plus the identity that sink formats for."""

    sink: Sink
    queue: Optional[str]
    partition: Optional[str]


def _format_durable(
    _queue: Optional[str], _partition: Optional[str], batch: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """The durable push wire: identity per item, envelope carries only the batch."""
    return {"items": batch}


def _format_ephemeral(
    queue: Optional[str], partition: Optional[str], batch: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """The ephemeral push wire (§3.1): identity on the envelope."""
    body: Dict[str, Any] = {"queue": queue}
    # Omitted, never defaulted client-side: which partition an ephemeral push
    # without one lands on is the broker's rule, and inventing a "Default" here
    # would take that decision away from it in a way the caller never asked for.
    if partition is not None:
        body["partition"] = partition
    body["messages"] = batch
    return body


DURABLE_SINK = Sink("durable", "/api/v1/push", _format_durable)
EPHEMERAL_SINK = Sink("ephemeral", "/api/v1/ephemeral/push", _format_ephemeral)

#: The default for a buffer created without one, so a buffer that predates sinks
#: behaves exactly as it did before they existed.
DURABLE_DESTINATION = Destination(DURABLE_SINK, None, None)


def ephemeral_destination(queue: str, partition: Optional[str] = None) -> Destination:
    """The ephemeral counterpart, bound to one (queue, partition)."""
    return Destination(EPHEMERAL_SINK, queue, partition)


def durable_address(queue: Optional[str], partition: Optional[str]) -> str:
    """The durable buffer address, unchanged: ``queue/partition``.

    Kept here next to its ephemeral sibling so the two can be compared at a
    glance; the durable push path builds the same string inline.
    """
    return f"{queue}/{partition}"


def ephemeral_address(queue: str, partition: Optional[str] = None) -> str:
    """The ephemeral buffer address: ``eph:queue/partition``.

    Or ``eph:queue`` when the caller named no partition, which is a DIFFERENT
    destination from any named one -- the broker picks, and a buffer must not
    merge the two.

    Same ambiguity as the durable address (a queue named ``a/b`` collides with
    (``a``, ``b``)), inherited deliberately rather than fixed on one side only.
    """
    return f"eph:{queue}" if partition is None else f"eph:{queue}/{partition}"
