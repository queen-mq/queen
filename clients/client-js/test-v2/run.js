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
    loadBalancingStrategy: 'affinity'
  };

export const TEST_CONFIG_MULTIPLE = {
  baseUrls: ['http://localhost:6632','http://localhost:6633'],
  loadBalancingStrategy: 'round-robin'
};

export const TEST_CONFIG = process.env.TEST_CONFIG === 'multiple' ? TEST_CONFIG_MULTIPLE : TEST_CONFIG_SINGLE;
console.log('TEST_CONFIG:', TEST_CONFIG);

// Global test state
let activeClient;

// The suite talks to the broker through its public HTTP API only: there is no
// database and no cleanup step. test/run.sh gives every lane a FRESH broker
// (its data volume is created empty and destroyed with `down -v`), so the
// suite starts on an empty store. A second run against the same broker is not
// expected to be green: fixed-name fixtures (docs.js's fixed transactionId,
// the kv and timer suites) would find the first run's state.
//
// The preflight makes a missing or unhealthy broker fail the run up front,
// with the reason, instead of as a wall of per-test connection errors.
async function preflightBroker(baseUrl) {
    const url = `${baseUrl}/health`
    let res
    try {
        res = await fetch(url, { signal: AbortSignal.timeout(5000) })
    } catch (e) {
        throw new Error(`broker preflight ${url}: ${e.cause?.message || e.message}`)
    }
    // Drain the body so the connection is released either way.
    await res.arrayBuffer().catch(() => {})
    if (!res.ok) {
        throw new Error(`broker preflight ${url}: HTTP ${res.status}`)
    }
    log(true, `Broker preflight ${url}: HTTP ${res.status}`)
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

async function main() {
    const client = new Queen({
        urls: TEST_CONFIG.baseUrls,
        loadBalancingStrategy: TEST_CONFIG.loadBalancingStrategy
    })
    activeClient = client
    await preflightBroker(TEST_CONFIG.baseUrls[0])

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
    // Require a broker that serves the /streams/v1 routes.
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
    // Always release the client. Previously, an init failure skipped this
    // teardown and the outer catch returned a successful process status,
    // allowing a broken integration lane to appear green.
    const cleanupResults = await Promise.allSettled([
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
