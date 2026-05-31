import autocannon from 'autocannon';
import axios from 'axios';
import cluster from 'cluster';

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:6632';
const QUEUE_NAMES = (process.env.QUEUE_NAMES || process.env.QUEUE_NAME || 'bench-queue').split(',').map(s => s.trim()).filter(Boolean);
const NUM_WORKERS = parseInt(process.env.NUM_WORKERS || '1', 10);
const CONNECTIONS_PER_WORKER = parseInt(process.env.CONNECTIONS_PER_WORKER || '50', 10);
const DURATION = parseInt(process.env.DURATION || '900', 10);
const MAX_PARTITION = parseInt(process.env.MAX_PARTITION || '1000', 10);
const MSGS_PER_PUSH = parseInt(process.env.MSGS_PER_PUSH || '10', 10);

function generateRequests() {
  const requests = [];
  for (const q of QUEUE_NAMES) {
    for (let i = 0; i <= MAX_PARTITION; i++) {
      const items = [];
      for (let j = 0; j < MSGS_PER_PUSH; j++) {
        items.push({
          queue: q,
          partition: `${i}`,
          payload: { message: 'Hello World', partition_id: i }
        });
      }
      requests.push({
        method: 'POST',
        path: '/api/v1/push',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items })
      });
    }
  }
  return requests;
}

async function configureQueues() {
  for (const q of QUEUE_NAMES) {
    await axios.post(`${SERVER_URL}/api/v1/configure`, {
      queue: q,
      options: {
        leaseTime: 60,
        retryLimit: 3,
        retentionEnabled: true,
        retentionSeconds: 7200,
        completedRetentionSeconds: 1800
      }
    });
  }
}

if (cluster.isPrimary) {
  console.log(JSON.stringify({
    role: 'producer', event: 'start',
    queues: QUEUE_NAMES, queueCount: QUEUE_NAMES.length,
    numWorkers: NUM_WORKERS, conns: CONNECTIONS_PER_WORKER,
    duration: DURATION, maxPartition: MAX_PARTITION, msgsPerPush: MSGS_PER_PUSH,
    timestamp: new Date().toISOString()
  }));

  await configureQueues();
  const results = [];
  let ready = 0;
  let done = 0;

  for (let i = 0; i < NUM_WORKERS; i++) {
    const w = cluster.fork();
    w.on('message', (m) => {
      if (m.type === 'ready') {
        ready++;
        if (ready === NUM_WORKERS) {
          for (const id in cluster.workers) cluster.workers[id].send({ type: 'start' });
        }
      } else if (m.type === 'done') {
        results.push(m.results);
        done++;
        if (done === NUM_WORKERS) aggregate(results);
      }
    });
  }

  function aggregate(rs) {
    const agg = { totalRequests: 0, totalBytes: 0, errors: 0, non2xx: 0, timeouts: 0,
                  latencies: [], throughputs: [] };
    for (const r of rs) {
      agg.totalRequests += r.requests.total;
      agg.totalBytes += r.throughput.total;
      agg.errors += r.errors;
      agg.non2xx += r.non2xx;
      agg.timeouts += r.timeouts;
      agg.latencies.push(r.latency);
      agg.throughputs.push(r.requests.average);
    }
    const lat = {
      p50: agg.latencies.reduce((s, l) => s + l.p50, 0) / rs.length,
      p90: agg.latencies.reduce((s, l) => s + (l.p90 || l.p99), 0) / rs.length,
      p99: agg.latencies.reduce((s, l) => s + l.p99, 0) / rs.length,
      avg: agg.latencies.reduce((s, l) => s + l.average, 0) / rs.length,
      max: Math.max(...agg.latencies.map(l => l.max))
    };
    const reqPerSec = Math.round(agg.throughputs.reduce((s, t) => s + t, 0));
    console.log(JSON.stringify({
      role: 'producer', event: 'result',
      totalRequests: agg.totalRequests,
      totalMessages: agg.totalRequests * MSGS_PER_PUSH,
      reqPerSec,
      msgPerSec: reqPerSec * MSGS_PER_PUSH,
      latency: lat,
      errors: agg.errors,
      non2xx: agg.non2xx,
      timeouts: agg.timeouts,
      durationSec: DURATION
    }, null, 2));
    process.exit(0);
  }
} else {
  const requests = generateRequests();
  process.send({ type: 'ready' });
  process.on('message', (m) => {
    if (m.type !== 'start') return;
    const inst = autocannon({
      url: SERVER_URL,
      connections: CONNECTIONS_PER_WORKER,
      duration: DURATION,
      pipelining: 1,
      requests
    });
    inst.on('done', (r) => process.send({ type: 'done', results: r }));
  });
}
