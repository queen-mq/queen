"""
Pytest configuration and fixtures for Queen client tests

There is no cleanup fixture and no database to reach. The harness
(test/run.sh) brings up a fresh broker for every lane and throws its data
volume away afterwards, so the integration tests start from an empty broker.
Most of them use fixed queue names, so a rerun against the same broker needs
a fresh one too (see tests/README.md).
"""

import os
import pytest_asyncio

from queen import Queen


# Test configuration
TEST_CONFIG = {
    "base_urls": [os.environ.get("QUEEN_SERVER_URL", "http://localhost:6632")],
}


# Explicit loop scope (see pytest.ini): `client` is per-test, so it must share
# the FUNCTION-scoped loop the test itself runs on — otherwise httpx's aclose()
# at teardown does loop.call_soon() on a loop that is already closed and floods
# the run with "Event loop is closed" RuntimeErrors.
@pytest_asyncio.fixture(loop_scope="function")
async def client():
    """Create Queen client for tests"""
    queen = Queen(TEST_CONFIG["base_urls"][0])
    yield queen
    await queen.close()


# There is no `kv_client` / `timers_client` fixture, and that absence is the
# point. Those two probed the broker and SKIPPED the whole kv/timers tree when
# it answered 404, because a cell built with QUEEN_KV_ENABLED false did not
# register the routes at all. Those boot flags are gone: every broker that runs
# this binary carries both surfaces, so there is nothing left to detect and the
# tests take the plain `client` above and run.
#
# What survives is the OPERATOR's runtime kill switch (server/src/switches.rs),
# which answers 503 `kv_disabled` / `timers_disabled` on the route and 403
# inside a transaction. That is an incident lever, not a configuration to probe
# before use: the SDK's handling of it is asserted in tests/kv_unit, against a
# scripted response, where it belongs.
