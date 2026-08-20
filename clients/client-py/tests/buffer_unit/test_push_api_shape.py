"""
The public buffered-push API is unchanged by the backpressure rewrite.

`add_message` became a coroutine, so every caller on the way down from
``await queen.queue(...).buffer({...}).push([...])`` had to start awaiting it.
A missed await there would not raise -- it would just never push -- so the
whole chain is exercised here through the real Queen object against
httpx.MockTransport (no broker, no database, no sockets).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import httpx
import pytest

from queen import Queen
from queen.errors import QueenError

QUEUE = "test-buffer-api"


def recording_transport(posted: List[List[Dict[str, Any]]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        items = body.get("items", [])
        posted.append(items)
        return httpx.Response(200, json=[{"status": "queued"} for _ in items])

    return httpx.MockTransport(handler)


async def test_buffered_push_still_returns_the_buffered_result():
    posted: List[List[Dict[str, Any]]] = []
    queen = Queen(url="http://plan.local", transport=recording_transport(posted), retry_attempts=1)
    try:
        result = await queen.queue(QUEUE).buffer({"message_count": 10, "time_millis": 60000}).push(
            [{"data": {"n": 1}}, {"data": {"n": 2}}]
        )
        assert result == {"buffered": True, "count": 2}
        assert posted == [], "buffered push must not hit the broker yet"

        stats = queen.get_buffer_stats()
        assert stats["totalBufferedMessages"] == 2

        await queen.queue(QUEUE).flush_buffer()
        assert [item["payload"] for batch in posted for item in batch] == [{"n": 1}, {"n": 2}]
    finally:
        await queen.close()


async def test_a_push_after_close_is_refused_rather_than_buffered_forever():
    """A closed client has no drain left, so accepting a message would be a lie.

    Before the stopped flag, add_message simply created a fresh buffer on the
    emptied registry: the push returned {"buffered": True}, and the message sat
    in a buffer nothing would ever flush.
    """
    posted: List[List[Dict[str, Any]]] = []
    queen = Queen(url="http://plan.local", transport=recording_transport(posted), retry_attempts=1)
    await queen.close()

    with pytest.raises(QueenError):
        await queen.queue(QUEUE).buffer({"message_count": 10, "time_millis": 60000}).push(
            [{"data": {"n": 1}}]
        )

    assert queen.get_buffer_stats()["totalBufferedMessages"] == 0


async def test_close_flushes_what_is_still_buffered():
    posted: List[List[Dict[str, Any]]] = []
    queen = Queen(url="http://plan.local", transport=recording_transport(posted), retry_attempts=1)

    await queen.queue(QUEUE).buffer({"message_count": 100, "time_millis": 60000}).push(
        [{"data": {"n": i}} for i in range(5)]
    )
    assert posted == []

    # close() flushes, then cleans up -- and the cleanup is awaited, which is
    # what wakes anything parked on a backpressure bound at shutdown.
    await queen.close()

    assert [item["payload"] for batch in posted for item in batch] == [{"n": i} for i in range(5)]
    assert queen.get_buffer_stats()["totalBufferedMessages"] == 0
