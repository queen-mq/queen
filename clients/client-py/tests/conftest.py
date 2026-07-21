"""
Pytest configuration and fixtures for Queen client tests
"""

import os
import pytest
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


@pytest.fixture(scope="session")
async def db_pool():
    """Create database pool for tests"""
    pool = await asyncpg.create_pool(**TEST_CONFIG["db_config"])
    yield pool
    await pool.close()


@pytest.fixture
async def client():
    """Create Queen client for tests"""
    queen = Queen(TEST_CONFIG["base_urls"][0])
    yield queen
    await queen.close()


@pytest.fixture(scope="session", autouse=True)
async def cleanup_test_data(db_pool):
    """Cleanup test data before and after test run"""
    
    async def cleanup():
        patterns = ["test-%", "edge-%", "pattern-%", "workflow-%"]
        try:
            # SEGMENTS ENGINE cleanup (mirrors the JS suite's cleanupTestData):
            # deleting queen.queues only cascades the retired rows-engine tables;
            # the segments engine keeps its own queue/partition rows plus tables
            # with no FK (seg_dedup, seg_dlq, watermarks, subscriptions).
            # partition_consumers cascades from seg_partitions (099 FK).
            try:
                await db_pool.execute(
                    """WITH parts AS (
                           SELECT sp.id FROM queen.seg_partitions sp
                           JOIN queen.seg_queues sq ON sq.id = sp.queue_id
                           WHERE sq.name LIKE ANY($1::text[])
                       ),
                       d1 AS (DELETE FROM queen.seg_dedup WHERE partition_id IN (SELECT id FROM parts)),
                       d2 AS (DELETE FROM queen.partition_consumers WHERE partition_id IN (SELECT id FROM parts)),
                       d3 AS (DELETE FROM queen.seg_dlq WHERE partition_id IN (SELECT id FROM parts))
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
                    "DELETE FROM queen.seg_queues WHERE name LIKE ANY($1::text[])", patterns
                )
            except Exception:
                pass  # segments schema not installed (rows-only server)

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

