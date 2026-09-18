import pg from 'pg';
import { Queen } from '../client-v2/index.js'
import * as queueTests from './queue.js'
import * as pushTests from './push.js'
import * as popTests from './pop.js'
import * as consumerTests from './consume.js'
import * as loadTests from './load.js'
import * as dlqTests from './dlq.js'
import * as completeTests from './complete.js'
import * as transactionTests from './transaction.js'
import * as subscriptionTests from './subscription.js'
import * as maintenanceTests from './maintenance.js'
import * as retentionTests from './retention.js'
import * as bootstrapTests from './bootstrap.js'
import * as loggerTests from './logger.js'
import * as watermarkTests from './watermark.js'
import * as authTests from './auth.js'
import * as semanticsTests from './semantics.js'
import * as ackWindowTests from './ackwindow.js'
import * as kvTests from './kv.js'
import * as timerTests from './timers.js'
import * as streamTests from './stream/index.js'
import * as docsTests from './docs.js'
import { LoadBalancer } from '../client-v2/http/LoadBalancer.js';


// Test configuration

export const TEST_CONFIG_SINGLE = {
    baseUrls: [process.env.QUEEN_SERVER_URL || 'http://localhost:6632'],
    loadBalancingStrategy: 'affinity',
    dbConfig: {
      host: process.env.PG_HOST || 'localhost',
      port: process.env.PG_PORT || 5432,
      database: process.env.PG_DB || 'postgres',
      user: process.env.PG_USER || 'postgres',
      password: process.env.PG_PASSWORD || 'postgres'
    }
  };

export const TEST_CONFIG_MULTIPLE = {
  baseUrls: ['http://localhost:6632','http://localhost:6633'],
  loadBalancingStrategy: 'round-robin',
  dbConfig: {
    host: process.env.PG_HOST || 'localhost',
    port: process.env.PG_PORT || 5432,
    database: process.env.PG_DB || 'postgres',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || 'postgres'
  }
};

export const TEST_CONFIG = process.env.TEST_CONFIG === 'multiple' ? TEST_CONFIG_MULTIPLE : TEST_CONFIG_SINGLE;
console.log('TEST_CONFIG:', TEST_CONFIG);

// PLAN_RAFT.md §13.3 / WP-1.9 — the raft1 lane (QUEEN_TEST_STORAGE=raft).
//
// A phase-1 raft broker serves ONLY the message path — push, the three pops,
// ack/ack-batch, lease renew — with IMPLICIT queue/partition creation, plus
// /health, /metrics and the ephemeral RAM verbs. Every other /api or /streams
// route answers 503 raft_phase1_unsupported (server handlers/raft.rs
// raft_fallback): queue configure/create/delete, transactions, kv, timers,
// streams, consumer-group admin and seek, and every dashboard/analytics/
// resources read. And there is NO Postgres, so the direct-DB setup and cleanup
// do not run.
//
// So this lane runs only the tests that exercise the served surface (or touch no
// broker at all) and SKIPS the rest LOUDLY, each printed with its reason, so the
// RAFT PARITY gate in test/run.sh compares single<->raft1 on the tests both
// lanes actually run. The kept set GROWS as phase 2 ports routes — a WP that
// ports queue admin removes `queue` from RAFT_SKIP_REASON and its push/pop/…
// tests start running here.
const RAFT_LANE = process.env.QUEEN_TEST_STORAGE === 'raft';

// The name→module map, so a skipped test can be attributed to the route class it
// needs. Order is cosmetic; keys are the reason-table keys below.
const RAFT_MODULES = {
  queue: queueTests, push: pushTests, pop: popTests, consume: consumerTests,
  load: loadTests, dlq: dlqTests, complete: completeTests, transaction: transactionTests,
  subscription: subscriptionTests, maintenance: maintenanceTests, retention: retentionTests,
  bootstrap: bootstrapTests, logger: loggerTests, watermark: watermarkTests, auth: authTests,
  semantics: semanticsTests, ackwindow: ackWindowTests, kv: kvTests, timers: timerTests,
  docs: docsTests, stream: streamTests,
};

// Tests a phase-1 raft broker runs GREEN. Kept as an EXPLICIT allow-list, not a
// heuristic, because the served surface is narrow: the only integration test
// that pushes and pops without first calling the un-ported queue configure/
// create route is testPushAutoCreatesQueueAndPartition (implicit creation on the
// push path). The whole `logger` module is broker-free (it exercises the SDK's
// own logger, never the wire) and so is storage-agnostic — it is kept via its
// module below, not named here.
const RAFT_KEEP = new Set([
  'testPushAutoCreatesQueueAndPartition',
]);
const RAFT_KEEP_MODULES = new Set(['logger']);

// Why each class of test is skipped on raft1 — printed once per skipped test.
const RAFT_SKIP_REASON = {
  queue:        'queue configure/create/delete not served in raft phase 1 (503; WP-2.5 admin)',
  transaction:  'transaction wire not served in raft phase 1 (503; WP-2.1)',
  subscription: 'consumer-group subscription/seek admin not served in raft phase 1 (503; WP-2.5)',
  maintenance:  'maintenance-mode admin / dashboard SQL not served in raft phase 1 (503; WP-2.5/2.8)',
  retention:    'retention loop not started in raft phase 1 (§10.3; WP-2.7)',
  bootstrap:    'consumer-group bootstrap admin not served in raft phase 1 (503; WP-2.5)',
  watermark:    'reads Postgres directly + consumer seek; unavailable in raft phase 1 (no DB; WP-2.5/2.6)',
  kv:           'kv routes not served in raft phase 1 (503; WP-2.2)',
  timers:       'timer routes not served in raft phase 1 (503; WP-2.3)',
  docs:         'reads Postgres directly for the published dedup snippet; unavailable in raft phase 1 (no DB)',
  stream:       'streams routes not served in raft phase 1 (503; WP-2.4)',
};
const RAFT_SKIP_DEFAULT =
  'needs queue configure/create or another admin/read route not served in raft phase 1 (503; WP-2.5/2.6)';

// Global test state
export let dbPool;  
let activeClient;

// Initialize database pool
export async function initDb() {
  if (RAFT_LANE) {
    // No Postgres in raft mode (G-2). The suite's direct-DB assertions and the
    // SQL cleanup do not apply; dbPool stays undefined and the raft lane runs
    // only the tests that never touch it.
    log(true, 'raft1 lane: QUEEN_TEST_STORAGE=raft — no Postgres, skipping the direct-DB setup');
    return undefined;
  }
  dbPool = new pg.Pool(TEST_CONFIG.dbConfig);
  await dbPool.query('SELECT 1');
  return dbPool;
}

// Close database pool
export async function closeDb() {
  const pool = dbPool;
  dbPool = undefined;
  if (pool) await pool.end();
}

function log (success, ...args) {
    console.log(new Date().toISOString(), success ? '✅' : '❌', ...args)
}

const testResults = []
function addRestResult (success, testName, message) {
    testResults.push({ success, testName, message })
}

function printResults() {
    console.log('='.repeat(80))
    console.log('Results:')
    console.log(testResults.map(x => `${x.success ? '✅' : '❌'} ${x.testName}: ${x.message}`).join('\n'))

    const passed = testResults.filter(x => x.success).length
    const failed = testResults.filter(x => !x.success).length
    const total = testResults.length
    console.log('='.repeat(80))
    console.log(`Overall Results: ${passed}/${total} tests passed, ${failed}/${total} tests failed`)
    console.log('='.repeat(80))
}

export const cleanupTestData = async () => {
    if (RAFT_LANE) {
      // A raft1 stack has no Postgres and boots on a fresh data volume every
      // run (test/run.sh tears it down with `down -v`), so there is nothing to
      // purge and no SQL to run it with — the store is already empty.
      log(true, 'raft1 lane: fresh data volume, no Postgres — skipping SQL cleanup (store is empty at boot)');
      return;
    }
    // All the LIKE patterns test queues use. The three exact names are the
    // documentation queues (test-v2/docs.js): purging them here is what lets
    // the published dedup snippet keep a fixed transactionId across runs.
    const patterns = ['test-%', 'edge-%', 'pattern-%', 'workflow-%', 'orders', 'payments', 'invoices'];
    try {
      // Drop streaming queries first (CASCADE removes their state rows).
      // Safe even when queen_streams isn't installed yet — we swallow the
      // error if the schema doesn't exist.
      try {
        await dbPool.query(`DELETE FROM queen_streams.queries WHERE name LIKE 'test-%'`);
      } catch (e) {
        // queen_streams schema not installed — ignore.
      }

      // Queue identity is now the queen.queues id (log_queues was merged
      // away): log_partitions, consumer_watermarks, consumer_groups_metadata
      // and queue_lag_metrics all cascade from the queues row. Only log_txns
      // and log_dlq have NO foreign key by design, so they get an explicit
      // purge keyed via log_partitions first. Without it, every suite run
      // inherits the previous run's messages and dedup window entries —
      // fixed-transactionId tests report 'duplicate' on their FIRST push.
      try {
        await dbPool.query(`
          WITH parts AS (
            SELECT lp.id FROM queen.log_partitions lp
            JOIN queen.queues q ON q.id = lp.queue_id
            WHERE q.name LIKE ANY($1::text[])
          ),
          d1 AS (DELETE FROM queen.log_txns WHERE partition_id IN (SELECT id FROM parts)),
          d2 AS (DELETE FROM queen.log_dlq  WHERE partition_id IN (SELECT id FROM parts))
          SELECT 1`, [patterns]);
      } catch (e) {
        // Log-engine schema not installed (rows-only server) — ignore.
      }

      await dbPool.query(`DELETE FROM queen.queues WHERE name LIKE ANY($1::text[])`, [patterns]);

      // KV keys and pending timers (PLAN_KV_TIMERS.md §10.4). NOT cosmetic:
      // without this purge a putIfAbsent test is green on its first run and red
      // forever after, an incr test accumulates between runs, and a timer left
      // pending by an earlier run fires into a later one and shows up as a
      // phantom message in an unrelated test. Neither table has a foreign key
      // to queen.queues -- log_timers is keyed by NAMES on purpose -- so the
      // queue delete above does not reach them.
      //
      // Both are deleted across every tenant: a test rig may run with
      // QUEEN_TENANCY_HEADER on, and the rows to purge are identified by the
      // test naming convention, never by tenant.
      //
      // These two used to be wrapped in a swallowing try/catch, on the grounds
      // that a broker booted with the kv/timer flags off had never applied
      // 024_kv.sql / 025_timers.sql. There are no such flags: schema.rs applies
      // both on every boot, so a missing `queen.kv` or `queen.log_timers` is a
      // broken rig and must be loud. Swallowing it would leave the purge silently
      // undone, which is exactly the failure the purge exists to prevent -- a
      // putIfAbsent test green on its first run and red forever after.
      await dbPool.query(`DELETE FROM queen.kv WHERE namespace LIKE ANY($1::text[])`, [patterns]);
      await dbPool.query(`DELETE FROM queen.log_timers WHERE queue LIKE ANY($1::text[])`, [patterns]);

      log(true, 'Test data cleaned up (rows + segments + kv + timers)');
    } catch (error) {
      log(false, `Cleanup error: ${error.message}`);
    }
  };

async function main() {
    const client = new Queen({
        urls: TEST_CONFIG.baseUrls,
        loadBalancingStrategy: TEST_CONFIG.loadBalancingStrategy
    })
    activeClient = client
    await initDb()

    // Separate human and AI tests
    const humanTests = [
        queueTests,
        pushTests,
        popTests,
        consumerTests,
        loadTests,
        dlqTests,
        completeTests,
        transactionTests,
        subscriptionTests,
        retentionTests,
        maintenanceTests,
        bootstrapTests,
        loggerTests,
        watermarkTests,
        authTests,
        semanticsTests,
        ackWindowTests,
        kvTests,
        timerTests,
        docsTests
    ]
    
    const aiTests = [

    ]

    // Streaming tests (queen-streams). Run via `node run.js stream`.
    // Require Queen v0.2+ with the queen_streams schema applied.
    const streamGroupTests = [streamTests]

    const allTests = [...humanTests, ...aiTests, ...streamGroupTests]
    const allTestFunctions = allTests.map(x => Object.values(x)).flat()
    const humanTestFunctions = humanTests.map(x => Object.values(x)).flat()
    const aiTestFunctions = aiTests.map(x => Object.values(x)).flat()
    const streamTestFunctions = streamGroupTests.map(x => Object.values(x)).flat()

    // Check command line arguments
    const firstArg = process.argv[2]
    
    let testsToRun = allTestFunctions
    let mode = 'all'

    // Check if filtering by test origin
    if (firstArg === 'ai') {
        testsToRun = aiTestFunctions
        mode = 'ai'
        log(true, `Running AI-generated tests only (${aiTestFunctions.length} tests)...`)
    } else if (firstArg === 'human') {
        testsToRun = humanTestFunctions
        mode = 'human'
        log(true, `Running human-written tests only (${humanTestFunctions.length} tests)...`)
    } else if (firstArg === 'stream' || firstArg === 'streams') {
        testsToRun = streamTestFunctions
        mode = 'stream'
        log(true, `Running queen-streams tests only (${streamTestFunctions.length} tests)...`)
    } else if (firstArg && firstArg !== 'all') {
        // Check if it's a specific test name
        const testFunc = allTestFunctions.find(t => t.name === firstArg)
        if (!testFunc) {
            console.log(`❌ Test '${firstArg}' not found`)
            console.log('\nUsage:')
            console.log('  node run.js              # Run all tests')
            console.log('  node run.js ai           # Run only AI-generated tests')
            console.log('  node run.js human        # Run only human-written tests')
            console.log('  node run.js stream       # Run only queen-streams tests')
            console.log('  node run.js <testName>   # Run specific test')
            console.log('\nAvailable tests:')
            console.log('\n🤖 AI-generated tests:')
            aiTestFunctions.forEach(t => console.log(`  - ${t.name}`))
            console.log('\n👤 Human-written tests:')
            humanTestFunctions.forEach(t => console.log(`  - ${t.name}`))
            console.log('\n🌊 queen-streams tests:')
            streamTestFunctions.forEach(t => console.log(`  - ${t.name}`))
            return 1
        }
        testsToRun = [testFunc]
        mode = 'single'
        log(true, `Running single test: ${firstArg}`)
    } else {
        log(true, `Running all tests (${allTestFunctions.length} tests)...`)
    }

    // PLAN_RAFT.md §13.3 / WP-1.9 — on the raft1 lane, keep only the tests the
    // phase-1 broker can serve and skip the rest LOUDLY, each with its reason.
    if (RAFT_LANE) {
        const fnModule = new Map()
        for (const [mn, mod] of Object.entries(RAFT_MODULES)) {
            for (const f of Object.values(mod)) {
                if (typeof f === 'function') fnModule.set(f.name, mn)
            }
        }
        const kept = []
        const skipped = []
        for (const t of testsToRun) {
            const mn = fnModule.get(t.name) || '?'
            if (RAFT_KEEP.has(t.name) || RAFT_KEEP_MODULES.has(mn)) kept.push(t)
            else skipped.push([mn, t.name])
        }
        log(true, `raft1 lane: keeping ${kept.length} served/broker-free test(s), skipping ${skipped.length} that need a route not in raft phase 1 (listed below)`)
        for (const [mn, name] of skipped) {
            const reason = RAFT_SKIP_REASON[mn] || RAFT_SKIP_DEFAULT
            console.log(`⏭️  SKIP (raft1) ${mn}.${name} — ${reason}`)
        }
        testsToRun = kept
    }

    // Cleanup test data
    await cleanupTestData()

    for (const test of testsToRun) {
        try {
            console.log('Running test:', test.name)
            const result = await test(client)
            const message = result.message || 'Test completed successfully'
            addRestResult(result.success, test.name, message)
            log(result.success, test.name, message)
        } catch (error) {
            addRestResult(false, test.name, `Test threw error: ${error.message}`)
            log(false, test.name, 'Test failed:', error.message)
        }
    }
    
    printResults()
    
    // Show summary based on mode
    if (mode === 'ai') {
        console.log('\n💡 Tip: Run "node run.js human" to test human-written tests')
        console.log('💡 Tip: Run "node run.js" to test all tests')
    } else if (mode === 'human') {
        console.log('\n💡 Tip: Run "node run.js ai" to test AI-generated tests')
        console.log('💡 Tip: Run "node run.js" to test all tests')
    } else if (mode === 'stream') {
        console.log('\n💡 Tip: Run "node run.js" to test all tests')
    }
    
    const failedCount = testResults.filter(x => !x.success).length
    return failedCount > 0 ? 1 : 0
}

let exitCode = 1
try {
    exitCode = await main()
} catch (error) {
    log(false, 'Main error:', error.message)
} finally {
    // Always release both resource owners. Previously, an init/query failure
    // skipped this teardown and the outer catch returned a successful process
    // status, allowing a broken integration lane to appear green.
    const cleanupResults = await Promise.allSettled([
        closeDb(),
        activeClient ? activeClient.close() : Promise.resolve()
    ])
    for (const result of cleanupResults) {
        if (result.status === 'rejected') {
            exitCode = 1
            log(false, 'Cleanup error:', result.reason?.message || String(result.reason))
        }
    }
}

process.exit(exitCode)
