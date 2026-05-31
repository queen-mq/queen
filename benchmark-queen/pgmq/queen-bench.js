// Closed-loop HTTP client for Queen — the symmetric counterpart of pgmq-bench.js
// (same model, same latency histogram, same JSON output), so latency (#5) and
// worker-bound (#6) numbers are apples-to-apples vs pgmq's SQL client instead of
// autocannon-vs-pg-pool.
//
// PROCESS_MS simulates real per-message work (e.g. an LLM/API call) so we can
// show the worker-bound regime where the broker stops being the bottleneck.

import axios from 'axios';
import http from 'http';

const ROLE            = process.env.ROLE || 'producer';
const SERVER_URL      = process.env.SERVER_URL || 'http://localhost:6633';
const QUEUE           = process.env.QUEUE || 'bench';
const CONNECTIONS     = parseInt(process.env.CONNECTIONS || '1', 10);
const DURATION        = parseInt(process.env.DURATION || '60', 10);
const MSGS_PER_PUSH   = parseInt(process.env.MSGS_PER_PUSH || '1', 10);
const READ_QTY        = parseInt(process.env.READ_QTY || '100', 10);
const NUM_PARTITIONS  = parseInt(process.env.NUM_PARTITIONS || '1000', 10);
const CONSUMER_GROUP  = process.env.CONSUMER_GROUP || '';
const PROCESS_MS      = parseInt(process.env.PROCESS_MS || '0', 10);   // simulated work per message
const WAIT            = (process.env.WAIT || 'false') === 'true';
const PAYLOAD_BYTES   = parseInt(process.env.PAYLOAD_BYTES || '0', 10);
const EMPTY_BACKOFF_MS = parseInt(process.env.EMPTY_BACKOFF_MS || '5', 10);
const RECORD_EMPTY    = (process.env.RECORD_EMPTY || 'true') === 'true';
const RETENTION_SECONDS = parseInt(process.env.RETENTION_SECONDS || '7200', 10);
const COMPLETED_RETENTION_SECONDS = parseInt(process.env.COMPLETED_RETENTION_SECONDS || '1800', 10);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const agent = new http.Agent({ keepAlive: true, maxSockets: CONNECTIONS + 2, maxFreeSockets: CONNECTIONS + 2 });
const client = axios.create({ baseURL: SERVER_URL, httpAgent: agent, timeout: 60000,
  headers: { 'Content-Type': 'application/json' }, validateStatus: () => true });

// ---- latency histogram: 0.1 ms buckets up to 30 s ----
const BUCKETS = 300000;
const hist = new Int32Array(BUCKETS);
let nOps = 0, nErr = 0, nMsg = 0, nLat = 0, latSumUs = 0, latMaxUs = 0, lastErr = null;
function record(us) { latSumUs += us; if (us > latMaxUs) latMaxUs = us; let b = Math.round(us / 100); if (b >= BUCKETS) b = BUCKETS - 1; if (b < 0) b = 0; hist[b]++; nLat++; }
function pct(p) { if (!nLat) return 0; const t = Math.ceil((p / 100) * nLat); let c = 0; for (let i = 0; i < BUCKETS; i++) { c += hist[i]; if (c >= t) return +(i * 0.1).toFixed(1); } return +((BUCKETS - 1) * 0.1).toFixed(1); }

const padStr = PAYLOAD_BYTES > 0 ? 'x'.repeat(Math.max(0, PAYLOAD_BYTES - 40)) : '';
function payload(gid) { return padStr ? { message: 'Hello World', g: gid, pad: padStr } : { message: 'Hello World', g: gid }; }

async function configureQueue() {
  await client.post('/api/v1/configure', {
    queue: QUEUE,
    options: { leaseTime: 60, retryLimit: 3, retentionEnabled: true, retentionSeconds: RETENTION_SECONDS, completedRetentionSeconds: COMPLETED_RETENTION_SECONDS },
  }).catch(() => {});
}

function makeOp() {
  if (ROLE === 'producer') {
    return async () => {
      const part = NUM_PARTITIONS > 0 ? String(Math.floor(Math.random() * NUM_PARTITIONS)) : 'Default';
      const items = [];
      for (let i = 0; i < MSGS_PER_PUSH; i++) items.push({ queue: QUEUE, partition: part, payload: payload(part) });
      const r = await client.post('/api/v1/push', { items });
      if (r.status >= 300) throw new Error('push HTTP ' + r.status);
      return MSGS_PER_PUSH;
    };
  }
  const cg = CONSUMER_GROUP ? `&consumerGroup=${encodeURIComponent(CONSUMER_GROUP)}` : '';
  const url = `/api/v1/pop/queue/${QUEUE}?batch=${READ_QTY}&wait=${WAIT}&autoAck=true${cg}`;
  return async () => {
    const r = await client.get(url);
    if (r.status >= 300) throw new Error('pop HTTP ' + r.status);
    const msgs = (r.data && r.data.messages) || [];
    const n = msgs.length;
    if (n > 0 && PROCESS_MS > 0) await sleep(PROCESS_MS * n);   // simulate per-message work
    return n;
  };
}

async function clientLoop(op, deadline) {
  while (Date.now() < deadline) {
    const t0 = process.hrtime.bigint();
    try {
      const n = await op();
      const us = Number(process.hrtime.bigint() - t0) / 1000;
      // op latency excludes the work-sleep (subtract it back out for consumers);
      // skip empty consumer reads when RECORD_EMPTY=false.
      const opUs = (ROLE === 'consumer' && PROCESS_MS > 0) ? Math.max(us - PROCESS_MS * Math.max(n, 0) * 1000, 0) : us;
      if (RECORD_EMPTY || ROLE !== 'consumer' || n > 0) record(opUs);
      nOps++; nMsg += n;
      if (n === 0 && ROLE === 'consumer' && EMPTY_BACKOFF_MS > 0) await sleep(EMPTY_BACKOFF_MS);
    } catch (e) { nErr++; lastErr = e.message; await sleep(5); }
  }
}

async function main() {
  if (ROLE === 'producer') await configureQueue();
  // fail fast
  const ping = await client.get('/api/v1/status'); if (ping.status >= 300) throw new Error('broker not ready: ' + ping.status);
  console.error(JSON.stringify({ role: ROLE, event: 'start', connections: CONNECTIONS, duration: DURATION, msgsPerPush: MSGS_PER_PUSH, readQty: READ_QTY, processMs: PROCESS_MS, target: SERVER_URL, timestamp: new Date().toISOString() }));
  const deadline = Date.now() + DURATION * 1000;
  const op = makeOp();
  const loops = []; for (let i = 0; i < CONNECTIONS; i++) loops.push(clientLoop(op, deadline));
  await Promise.all(loops);
  const result = {
    role: ROLE, event: 'result', connections: CONNECTIONS, processMs: PROCESS_MS,
    totalOps: nOps, totalMessages: nMsg, msgPerSec: Math.round(nMsg / DURATION), opsPerSec: Math.round(nOps / DURATION),
    latency: { p50: pct(50), p90: pct(90), p99: pct(99), avg: nLat ? +(latSumUs / nLat / 1000).toFixed(2) : 0, max: +(latMaxUs / 1000).toFixed(2) },
    errors: nErr, lastError: lastErr, durationSec: DURATION,
  };
  console.log(JSON.stringify(result, null, 2));
  process.exit(0);
}
main().catch((e) => { console.error(JSON.stringify({ role: ROLE, event: 'fatal', error: e.message })); process.exit(1); });
