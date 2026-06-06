import autocannon from 'autocannon';
import axios from 'axios';
import cluster from 'cluster';
import os from 'os';

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:6632';
const QUEUE_NAME = 'queen-long-running';
const NUM_WORKERS = Number(process.env.NUM_WORKERS || 4);
const CONNECTIONS_PER_WORKER = Number(process.env.CONNECTIONS_PER_WORKER || 200);
const DURATION = Number(process.env.DURATION || 60);
const MAX_PARTITION = Number(process.env.MAX_PARTITION || 1000);
const NUMBER_OF_MESSAGES_PER_PER_PUSH = Number(process.env.NUMBER_OF_MESSAGES_PER_PER_PUSH || 10);

// Pre-generate requests array
function generateRequests() {
  const requests = [];
  for (let i = 0; i <= MAX_PARTITION; i++) {
    let items = []
    for (let j = 0; j < NUMBER_OF_MESSAGES_PER_PER_PUSH; j++) {
      items.push({
        queue: QUEUE_NAME,
        partition: `${i}`,
        payload: { 
          message: "Hello World",
          partition_id: i
        }
      })
    }    
    requests.push({
      method: 'POST',
      path: '/api/v1/push',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        items: items
      })
    });
  }
  return requests;
}

async function resetQueue() {
  /*try {
    console.log('Deleting queue...');
    await axios.delete(`${SERVER_URL}/api/v1/resources/queues/${QUEUE_NAME}`);
    console.log('✅ Queue deleted');
  } catch (error) {
    // Queue might not exist, that's fine
  }*/

  try {
    console.log('Creating queue...');
    await axios.post(`${SERVER_URL}/api/v1/configure`, {
      queue: QUEUE_NAME,
      options: { leaseTime: 60, retryLimit: 3, retentionEnabled: true, retentionSeconds: 1800, completedRetentionSeconds: 1800 }
    });
    console.log('✅ Queue created');
  } catch (error) {
    console.error('❌ Error creating queue:', error.response?.data || error.message);
  }
}

if (cluster.isPrimary) {
  // ========== PRIMARY PROCESS ==========
  console.log(`\n🚀 Multi-process load test starting`);
  console.log(`   Workers: ${NUM_WORKERS}`);
  console.log(`   Connections per worker: ${CONNECTIONS_PER_WORKER}`);
  console.log(`   Total connections: ${NUM_WORKERS * CONNECTIONS_PER_WORKER}`);
  console.log(`   Duration: ${DURATION}s\n`);

  // Reset queue before starting
  await resetQueue()
  const workerResults = [];
  let workersReady = 0;
  let workersFinished = 0;

  // Fork workers
  for (let i = 0; i < NUM_WORKERS; i++) {
    const worker = cluster.fork();
    
    worker.on('message', (msg) => {
      if (msg.type === 'ready') {
        workersReady++;
        console.log(`Worker ${msg.workerId} ready (${workersReady}/${NUM_WORKERS})`);
        
        // All workers ready, signal them to start simultaneously
        if (workersReady === NUM_WORKERS) {
          console.log('\n⏱️  All workers ready, starting benchmark...\n');
          for (const id in cluster.workers) {
            cluster.workers[id].send({ type: 'start' });
          }
        }
      } else if (msg.type === 'done') {
        workerResults.push(msg.results);
        workersFinished++;
        console.log(`Worker ${msg.workerId} finished (${workersFinished}/${NUM_WORKERS})`);
        
        // All workers done, aggregate results
        if (workersFinished === NUM_WORKERS) {
          aggregateResults(workerResults);
        }
      }
    });
  }

  async function aggregateResults(results) {
    // Aggregate metrics
    const aggregated = {
      totalRequests: 0,
      totalBytes: 0,
      errors: 0,
      non2xx: 0,
      timeouts: 0,
      duration: DURATION,
      latencies: [],
      throughputs: []
    };

    for (const r of results) {
      aggregated.totalRequests += r.requests.total;
      aggregated.totalBytes += r.throughput.total;
      aggregated.errors += r.errors;
      aggregated.non2xx += r.non2xx;
      aggregated.timeouts += r.timeouts;
      aggregated.latencies.push(r.latency);
      aggregated.throughputs.push(r.requests.average);
    }

    // Calculate combined latency percentiles (weighted average approximation)
    const avgLatency = {
      p50: aggregated.latencies.reduce((sum, l) => sum + l.p50, 0) / results.length,
      p90: aggregated.latencies.reduce((sum, l) => sum + (l.p90 || l.p99), 0) / results.length,
      p99: aggregated.latencies.reduce((sum, l) => sum + l.p99, 0) / results.length,
      avg: aggregated.latencies.reduce((sum, l) => sum + l.average, 0) / results.length,
      max: Math.max(...aggregated.latencies.map(l => l.max))
    };

    const totalThroughput = aggregated.throughputs.reduce((sum, t) => sum + t, 0);

    console.log('\n' + '='.repeat(50));
    console.log('       AGGREGATED BENCHMARK RESULTS');
    console.log('='.repeat(50));
    console.log(`Workers:           ${NUM_WORKERS}`);
    console.log(`Connections:       ${NUM_WORKERS * CONNECTIONS_PER_WORKER}`);
    console.log(`Duration:          ${aggregated.duration}s`);
    console.log('─'.repeat(50));
    console.log(`Total Requests:    ${aggregated.totalRequests.toLocaleString()}`);
    console.log(`Throughput:        ${Math.round(totalThroughput).toLocaleString()} req/s`);
    console.log(`Data transferred:  ${(aggregated.totalBytes / 1024 / 1024).toFixed(2)} MB`);
    console.log('─'.repeat(50));
    console.log(`Latency avg:       ${avgLatency.avg.toFixed(2)}ms`);
    console.log(`Latency p50:       ${avgLatency.p50.toFixed(2)}ms`);
    console.log(`Latency p90:       ${avgLatency.p90.toFixed(2)}ms`);
    console.log(`Latency p99:       ${avgLatency.p99.toFixed(2)}ms`);
    console.log(`Latency max:       ${avgLatency.max.toFixed(2)}ms`);
    console.log('─'.repeat(50));
    console.log(`Errors:            ${aggregated.errors}`);
    console.log(`Non-2xx:           ${aggregated.non2xx}`);
    console.log(`Timeouts:          ${aggregated.timeouts}`);
    console.log('='.repeat(50));

    // Verify messages in queue
    try {
      const queue = await axios.get(`${SERVER_URL}/api/v1/resources/queues/${QUEUE_NAME}`);
      console.log(`\n✅ Queue total messages: ${queue.data.totals.total.toLocaleString()}`);
    } catch (e) {
      console.log(`\n⚠️  Could not verify queue: ${e.message}`);
    }

    process.exit(0);
  }

} else {
  // ========== WORKER PROCESS ==========
  const workerId = cluster.worker.id;
  const requests = generateRequests();

  // Signal ready
  process.send({ type: 'ready', workerId });

  // Wait for start signal
  process.on('message', (msg) => {
    if (msg.type === 'start') {
      runBenchmark();
    }
  });

  function runBenchmark() {
    const instance = autocannon({
      url: SERVER_URL,
      connections: CONNECTIONS_PER_WORKER,
      duration: DURATION,
      pipelining: 1,
      requests: requests,
    });

    instance.on('done', (results) => {
      process.send({ type: 'done', workerId, results });
    });
  }
}
