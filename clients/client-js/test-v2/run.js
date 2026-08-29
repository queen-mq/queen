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

// Global test state
export let dbPool;  
let activeClient;

// Initialize database pool
export async function initDb() {
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
