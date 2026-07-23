"""
Pytest configuration and fixtures for Queen client tests
"""

import os
import pytest
import pytest_asyncio
import asyncpg

from queen import Queen


# Test configuration
TEST_CONFIG = {
    "base_urls": [os.environ.get("QUEEN_SERVER_URL", "http://localhost:6632")],
    "db_config": {
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", 5432)),
        "database": os.environ.get("PG_DB", "postgres"),
        "user": os.environ.get("PG_USER", "postgres"),
        "password": os.environ.get("PG_PASSWORD", "postgres"),
    },
}


# Explicit loop scopes (see pytest.ini): db_pool and cleanup_test_data are
# session-scoped, so they must run on a session-scoped event loop. `client` is
# per-test, so it must share the FUNCTION-scoped loop the test itself runs on —
# otherwise httpx's aclose() at teardown does loop.call_soon() on a loop that is
# already closed and floods the run with "Event loop is closed" RuntimeErrors.
@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool():
    """Create database pool for tests"""
    pool = await asyncpg.create_pool(**TEST_CONFIG["db_config"])
    yield pool
    await pool.close()


@pytest_asyncio.fixture(loop_scope="function")
async def client():
    """Create Queen client for tests"""
    queen = Queen(TEST_CONFIG["base_urls"][0])
    yield queen
    await queen.close()


@pytest_asyncio.fixture(loop_scope="session", scope="session", autouse=True)
async def cleanup_test_data(db_pool):
    """Cleanup test data before and after test run"""
    
    async def cleanup():
        patterns = ["test-%", "edge-%", "pattern-%", "workflow-%"]
        try:
            # LOG ENGINE cleanup (mirrors the JS suite's cleanupTestData):
            # deleting queen.queues only cascades the retired rows-engine tables;
            # the log engine keeps its own queue/partition rows plus tables with
            # no FK by design (log_txns, log_dlq, watermarks, subscriptions).
            # log_partitions/log_segments/log_consumers cascade from log_queues.
            try:
                await db_pool.execute(
                    """WITH parts AS (
                           SELECT lp.id FROM queen.log_partitions lp
                           JOIN queen.log_queues lq ON lq.id = lp.queue_id
                           WHERE lq.name LIKE ANY($1::text[])
                       ),
                       d1 AS (DELETE FROM queen.log_txns WHERE partition_id IN (SELECT id FROM parts)),
                       d2 AS (DELETE FROM queen.log_dlq WHERE partition_id IN (SELECT id FROM parts))
                       SELECT 1""",
                    patterns,
                )
                await db_pool.execute(
                    "DELETE FROM queen.consumer_watermarks WHERE queue_name LIKE ANY($1::text[])", patterns
                )
                await db_pool.execute(
                    "DELETE FROM queen.consumer_groups_metadata WHERE queue_name LIKE ANY($1::text[])", patterns
                )
                await db_pool.execute(
                    "DELETE FROM queen.log_queues WHERE name LIKE ANY($1::text[])", patterns
                )
            except Exception:
                pass  # log-engine schema not installed (rows-only server)

            await db_pool.execute(
                "DELETE FROM queen.queues WHERE name LIKE ANY($1::text[])", patterns
            )
            print("Test data cleaned up (rows + segments)")
        except Exception as error:
            print(f"Cleanup error: {error}")
    
    # Cleanup before tests
    await cleanup()
    
    yield
    
    # Cleanup after tests (commented out for debugging)
    # await cleanup()

