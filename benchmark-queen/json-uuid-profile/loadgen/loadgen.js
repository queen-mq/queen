// Push load generator for the JSON/UUID profiling harness.
//
// Hammers POST /api/v1/push with batched items so the broker's push hot path
// (JSON parse -> validate -> build array -> dump x2 -> parse result, plus one
// UUIDv7 per message) runs as hard as possible. Adapted from
// benchmark-queen/test-perf/workload/producer.js.
import autocannon from 'autocannon';
import axios from 'axios';

const SERVER_URL        = process.env.SERVER_URL        || 'http://localhost:6632';
const QUEUE_NAME        = process.env.QUEUE_NAME        || 'perf-json-uuid';
const WORKERS           = parseInt(process.env.WORKERS           || '4', 10);
const CONNECTIONS       = parseInt(process.env.CONNECTIONS       || '200', 10);
const DURATION          = parseInt(process.env.DURATION          || '60', 10);
const PUSH_BATCH        = parseInt(process.env.PUSH_BATCH        || '10', 10);
const MAX_PARTITIONS    = parseInt(process.env.MAX_PARTITIONS    || '200', 10);
const PAYLOAD_SIZE_BYTES = parseInt(process.env.PAYLOAD_SIZE_BYTES || '256', 10);
const WARMUP            = process.env.WARMUP === '1';

async function ensureQueue() {
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      await axios.post(`${SERVER_URL}/api/v1/configure`, {
        queue: QUEUE_NAME,
        options: { leaseTime: 60, retryLimit: 3, retentionEnabled: true, retentionSeconds: 1800, completedRetentionSeconds: 1800 }
      }, { timeout: 5000 });
      return;
    } catch (err) {
      // Broker may still be booting / applying schema on the first runs.
      if (attempt === 30) {
        console.error(`[loadgen] queue configure failed after retries: ${err?.message}`);
        return;
      }
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

function makePayload(p, j) {
  const base = { message: 'Hello World', partition_id: p, seq: j };
  if (PAYLOAD_SIZE_BYTES > 0) {
    const baseStr = JSON.stringify(base);
    const padLen = Math.max(0, PAYLOAD_SIZE_BYTES - baseStr.length - 15);
    base.pad = 'x'.repeat(padLen);
  }
  return base;
}

await ensureQueue();

// Pre-generate one request per partition, each carrying PUSH_BATCH items.
const requests = [];
for (let p = 0; p <= MAX_PARTITIONS; p++) {
  const items = [];
  for (let j = 0; j < PUSH_BATCH; j++) {
    items.push({ queue: QUEUE_NAME, partition: `${p}`, payload: makePayload(p, j) });
  }
  requests.push({
    method: 'POST',
    path: '/api/v1/push',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items })
  });
}

console.log(`[loadgen] ${WARMUP ? 'WARMUP' : 'MEASURE'}: url=${SERVER_URL} conns=${CONNECTIONS} workers=${WORKERS} dur=${DURATION}s batch=${PUSH_BATCH} parts=${MAX_PARTITIONS} payload~${PAYLOAD_SIZE_BYTES}B`);

const instance = autocannon({ url: SERVER_URL, connections: CONNECTIONS, duration: DURATION, workers: WORKERS, requests });

instance.on('done', (results) => {
  const reqs = results.requests || {};
  const lat = results.latency || {};
  const msgPerSec = (reqs.average || 0) * PUSH_BATCH;
  console.log(`[loadgen] req/s avg=${reqs.average} total=${reqs.total} | ~msg/s=${Math.round(msgPerSec)} | p50=${lat.p50}ms p99=${lat.p99}ms | errors=${results.errors} non2xx=${results.non2xx} timeouts=${results.timeouts}`);
});

if (!WARMUP) autocannon.track(instance, { renderProgressBar: false });
