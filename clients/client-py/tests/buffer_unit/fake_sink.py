"""
A fake HttpClient for the buffer tests.

BufferManager only ever asks its http client for ``await post(path, body)``, so
the whole flush pipeline can be exercised with no broker, no database and no
sockets: this object records every batch it is offered, can be told which
attempts must fail, and can be gated shut so a flush hangs on demand (which is
how a producer is driven onto the max_size bound deterministically).
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Iterable, List, Optional, Set


class FakeSink:
    """Stands in for queen.http.HttpClient on the flush path"""

    def __init__(
        self,
        fail_attempts: Optional[Iterable[int]] = None,
        fail_predicate: Optional[Callable[[int], bool]] = None,
    ):
        """
        Args:
            fail_attempts: 1-based attempt numbers that must raise
            fail_predicate: called with the 1-based attempt number; return True
                to make that attempt raise (used for "every Nth attempt fails")
        """
        self.attempts: List[List[Dict[str, Any]]] = []  # every batch offered, failures included
        self.posts: List[List[Dict[str, Any]]] = []  # batches that were accepted
        self.paths: List[str] = []
        self.in_flight = 0
        self._fail_attempts: Set[int] = set(fail_attempts or ())
        self._fail_predicate = fail_predicate
        self._gate: Optional[asyncio.Event] = None

    # ----- control -----

    def close_gate(self) -> None:
        """Make every post hang until open_gate() is called"""
        self._gate = asyncio.Event()

    def open_gate(self) -> None:
        """Release any hanging post, and let later posts through"""
        if self._gate is not None:
            self._gate.set()

    # ----- the one method BufferManager uses -----

    async def post(self, path: str, body: Dict[str, Any]) -> Any:
        items = list(body["items"])
        self.attempts.append(items)
        self.paths.append(path)
        attempt = len(self.attempts)

        self.in_flight += 1
        try:
            # Always yield at least once, so a test never depends on post()
            # completing without giving the event loop a turn.
            await asyncio.sleep(0)
            if self._gate is not None:
                await self._gate.wait()

            if attempt in self._fail_attempts or (
                self._fail_predicate is not None and self._fail_predicate(attempt)
            ):
                raise RuntimeError(f"fake sink refused attempt {attempt}")
        finally:
            self.in_flight -= 1

        self.posts.append(items)
        return [{"status": "queued"} for _ in items]

    # ----- assertions helpers -----

    @property
    def delivered(self) -> List[Dict[str, Any]]:
        """Every item the sink accepted, in the order it accepted them"""
        return [item for batch in self.posts for item in batch]

    @property
    def delivered_payloads(self) -> List[Any]:
        return [item["payload"] for item in self.delivered]

    def attempt_payloads(self, index: int) -> List[Any]:
        """Payloads of the index-th attempt (0-based), failed attempts included"""
        return [item["payload"] for item in self.attempts[index]]


def item(payload: Any, queue: str = "test-buffer", partition: str = "Default") -> Dict[str, Any]:
    """Build a formatted push item, the shape QueueBuilder.push() produces"""
    return {
        "queue": queue,
        "partition": partition,
        "payload": payload,
        "transactionId": f"txn-{payload}",
    }


async def settle(turns: int = 40) -> None:
    """
    Let every runnable task reach its next suspension point.

    Plain ``sleep(0)`` in a loop rather than one long sleep: the paths under
    test hand off between producer, drain task and condition wake several times
    per message, and a wall-clock sleep would make the tests slower and no more
    deterministic.
    """
    for _ in range(turns):
        await asyncio.sleep(0)
