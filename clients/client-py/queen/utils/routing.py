"""
Client-side half of the routing contract (PLAN_ROUTING.md §4, §5).

As for conflation (utils/conflation.py), the broker's answer is the only
proof: there is no capability negotiation in this SDK, and a broker older than
routing ignores what it does not know and answers success.

* A pop that sent a ``where`` must be answered ``"filtered": true`` -- every
  answer, empty ones included (the broker sends a 200 with a body then, never
  a bodiless 204). Without it the broker gave the group the whole queue.
* A configure that named sources (``from``) must be answered with the queue's
  ``route``. Without it the broker made an ordinary queue that nothing fills.
"""

from __future__ import annotations

from typing import Any

from ..errors import FilterUnsupportedError, ReadingQueueUnsupportedError

FILTER_UNSUPPORTED_MESSAGE = (
    "where was sent but this broker does not filter consumer groups; upgrade it "
    "(filtered groups need every node at catalogue version 8)"
)
READING_UNSUPPORTED_MESSAGE = (
    "from was sent but this broker does not read other queues; upgrade it "
    "(reading queues need every node at catalogue version 8)"
)


def check_filtered_response(response: Any, *, requested: bool) -> None:
    """Raise FilterUnsupportedError when a pop sent a where and its answer
    does not say ``"filtered": true``. A 204 (no body) is no proof either. A
    no-op for a pop without a where."""
    if not requested:
        return
    if isinstance(response, dict) and response.get("filtered") is True:
        return
    raise FilterUnsupportedError(FILTER_UNSUPPORTED_MESSAGE)


def check_route_response(response: Any, *, requested: bool) -> None:
    """Raise ReadingQueueUnsupportedError when a configure named sources and
    its answer has no ``route``. A no-op for any other configure."""
    if not requested:
        return
    if isinstance(response, dict) and isinstance(response.get("route"), dict):
        return
    raise ReadingQueueUnsupportedError(READING_UNSUPPORTED_MESSAGE)
