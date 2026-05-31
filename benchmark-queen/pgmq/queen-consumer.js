import autocannon from 'autocannon';
import cluster from 'cluster';

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:6632';
const QUEUE_NAMES = (process.env.QUEUE_NAMES || process.env.QUEUE_NAME || 'bench-queue').split(',').map(s => s.trim()).filter(Boolean);
const NUM_WORKERS = parseInt(process.env.NUM_WORKERS || '1', 10);
const CONNECTIONS_PER_WORKER = parseInt(process.env.CONNECTIONS_PER_WORKER || '50', 10);
const DURATION = parseInt(process.env.DURATION || '900', 10);
const BATCH_SIZE = parseInt(process.env.CONSUMER_BATCH || '100', 10);
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || '';

function generateRequests() {
  return QUEUE_NAMES.map(q => {
    const cgParam = CONSUMER_GROUP ? `&consumerGroup=${encodeURIComponent(CONSUMER_GROUP)}` : '';
    return {
      method: 'GET',
      path: `/api/v1/pop/queue/${q}?batch=${BATCH_SIZE}&wait=true&autoAck=true${cgParam}`,
      headers: { 'Content-Type': 'application/json' }
    };
  });
}

if (cluster.isPrimary) {
  console.log(JSON.stringify({
    role: 'consumer', event: 'start',
    queues: QUEUE_NAMES, queueCount: QUEUE_NAMES.length,
    consumerGroup: CONSUMER_GROUP || null,
    numWorkers: NUM_WORKERS, conns: CONNECTIONS_PER_WORKER,
    duration: DURATION, batchSize: BATCH_SIZE,
    timestamp: new Date().toISOString()
  }));

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
      role: 'consumer', event: 'result',
      consumerGroup: CONSUMER_GROUP || null,
      totalRequests: agg.totalRequests,
      reqPerSec,
      maxMsgPerSecCeiling: reqPerSec * BATCH_SIZE,
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
