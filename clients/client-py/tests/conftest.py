"""
Pytest configuration and fixtures for Queen client tests
"""

import os
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
    """Create database pool for tests.

    Yields None when there is no database to reach. This fixture is pulled in
    by the autouse `cleanup_test_data` below, so a hard failure here would make
    EVERY test in the tree require Postgres -- including the ones that were
    written to need neither a broker nor a database (tests/http_unit,
    tests/kv_unit, tests/streams_unit, all of which run against
    httpx.MockTransport). Degrading to None keeps those runnable on a laptop
    and leaves the DB-backed tests to fail on their own terms.
    """
    try:
        pool = await asyncpg.create_pool(**TEST_CONFIG["db_config"])
    except Exception as error:
        print(f"No database at {TEST_CONFIG['db_config']['host']}:{TEST_CONFIG['db_config']['port']} ({error}); DB-backed fixtures are inert")
        yield None
        return
    yield pool
    await pool.close()


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


@pytest_asyncio.fixture(loop_scope="session", scope="session", autouse=True)
async def cleanup_test_data(db_pool):
    """Cleanup test data before and after test run"""
    
    async def cleanup():
        if db_pool is None:
            return
        # The three exact names are the documentation queues (test_docs.py):
        # purging them lets the published dedup snippet keep a fixed
        # transactionId across runs.
        patterns = ["test-%", "edge-%", "pattern-%", "workflow-%", "orders", "payments", "invoices"]

        # PLAN_KV_TIMERS.md §10.4 -- MANDATORY, not cosmetic. Without this
        # purge a putIfAbsent test is green on its first run and red forever
        # after (the marker survives), and an incr test accumulates across
        # runs, so a rate-limit assertion passes once and then fails with a
        # number nobody can explain from the test source.
        #
        # Neither table hangs off queen.queues, so neither is reached by the
        # cascade below: queen.kv has no queue at all, and queen.log_timers
        # keys the queue by NAME with no foreign key. They are purged
        # explicitly, each in its own try/except like the rest, because the
        # schema may not exist (a broker built before these surfaces, or one
        # started with QUEEN_APPLY_SCHEMA=0 against an older database).
        try:
            await db_pool.execute(
                "DELETE FROM queen.kv WHERE namespace LIKE ANY($1::text[])",
                ["test-%", "edge-%", "pattern-%", "workflow-%"],
            )
        except Exception:
            pass  # kv schema not installed
        try:
            await db_pool.execute(
                "DELETE FROM queen.log_timers WHERE queue LIKE ANY($1::text[])", patterns
            )
        except Exception:
            pass  # timers schema not installed

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

