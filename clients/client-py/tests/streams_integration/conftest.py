"""
Pytest configuration for streams integration tests.

These run live against a Queen broker (default http://localhost:6632 or
QUEEN_URL env var). They auto-skip when the broker is unreachable.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import pytest_asyncio

from queen import Queen

from ._helpers import STREAMS_URL


@pytest.fixture(scope="session")
def queen_url() -> str:
    return STREAMS_URL


@pytest_asyncio.fixture
async def client():
    """Per-test Queen client; auto-skip if broker not reachable."""
    if STREAMS_URL == "skip":
        pytest.skip("QUEEN_URL=skip")
    try:
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(STREAMS_URL + "/health")
            if not r.is_success:
                pytest.skip(f"Queen at {STREAMS_URL} returned {r.status_code}")
    except Exception as ex:
        pytest.skip(f"Queen at {STREAMS_URL} not reachable: {ex}")

    q = Queen(url=STREAMS_URL)
    try:
        yield q
    finally:
        await q.close()
