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
            # Queue identity is now the queen.queues id (log_queues merged
            # away, mirrors the JS suite's cleanupTestData): log_partitions,
            # consumer_watermarks, consumer_groups_metadata and
            # queue_lag_metrics all cascade from the queues row. Only
            # log_txns/log_dlq have no FK by design → explicit purge keyed
            # via log_partitions before the queues delete.
            try:
                await db_pool.execute(
                    """WITH parts AS (
                           SELECT lp.id FROM queen.log_partitions lp
                           JOIN queen.queues q ON q.id = lp.queue_id
                           WHERE q.name LIKE ANY($1::text[])
                       ),
                       d1 AS (DELETE FROM queen.log_txns WHERE partition_id IN (SELECT id FROM parts)),
                       d2 AS (DELETE FROM queen.log_dlq WHERE partition_id IN (SELECT id FROM parts))
                       SELECT 1""",
                    patterns,
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

