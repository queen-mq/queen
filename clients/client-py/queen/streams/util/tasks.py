"""Background tasks that are not garbage collected while they run.

asyncio keeps only a weak reference to a task: one nobody else holds can be
collected (and destroyed) while it is still pending. ``spawn`` holds it until
it is done.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Set

_RUNNING: Set["asyncio.Future[Any]"] = set()


def _done(task: "asyncio.Future[Any]") -> None:
    _RUNNING.discard(task)
    if not task.cancelled():
        task.exception()  # retrieved: a failed background task is not "never retrieved"


def spawn(aw: Awaitable[Any]) -> "asyncio.Future[Any]":
    task = asyncio.ensure_future(aw)
    _RUNNING.add(task)
    task.add_done_callback(_done)
    return task
