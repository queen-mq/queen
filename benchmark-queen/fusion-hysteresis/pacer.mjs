// Open-loop, rate-paced push generator for the fusion-hysteresis experiment.
//
// Why this exists: every load tool already in the repo (autocannon, goload,
// queenctl bench) is CLOSED-loop -- a fixed pool of clients each blocks on the
// response before issuing the next request. In closed-loop you CANNOT offer more
// than the server can serve: when PG slows, the clients slow with it, so you
// never reach (or even observe) the saturation cliff. This pacer dispatches
// requests on a wall-clock schedule -- exactly RATE req/s -- independently of how
// fast responses come back. When PG can't keep up, in-flight grows to a cap and
// the offered/achieved gap becomes the saturation signal.
//
// Push-only, one message per request (push_batch=1), so msgs/commit measured on
// the PG side == requests/commit == the pure SERVER-side fusion ratio.
//
// No external deps -- uses node:http with a keep-alive agent.
//
// Env:
//   SERVER_URL   default http://localhost:6632
//   QUEUE_NAME   default fusion-test
//   RATE         target requests/sec (required, >0)
//   DURATION     seconds to run at RATE (required)
//   PARTITIONS   distinct partitions to round-robin over (default 8 -- keep small
//                so background stats maintenance, which is O(partitions), stays
//                negligible and does not mask the per-commit signal)
//   PAYLOAD_SIZE_BYTES  approx bytes per message payload (default 0 = tiny)
//   MAX_INFLIGHT byte-the-bullet cap on outstanding requests (default 4000).
//                Reaching it == PG is behind; the remainder is counted as deficit.
//   OUTPUT_FILE  where to write the JSON summary (default ./pacer.json)

import http from 'node:http';
import { writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:6632';
const QUEUE_NAME = process.env.QUEUE_NAME || 'fusion-test';
const RATE = parseFloat(process.env.RATE || '0');
const DURATION = parseFloat(process.env.DURATION || '20');
const PARTITIONS = parseInt(process.env.PARTITIONS || '8', 10);
const PAYLOAD_SIZE_BYTES = parseInt(process.env.PAYLOAD_SIZE_BYTES || '0', 10);
const MAX_INFLIGHT = parseInt(process.env.MAX_INFLIGHT || '4000', 10);
const OUTPUT_FILE = process.env.OUTPUT_FILE || './pacer.json';

if (!(RATE > 0)) { console.error('RATE must be > 0'); process.exit(2); }

const u = new URL(SERVER_URL);
const agent = new http.Agent({
  keepAlive: true,
  maxSockets: Math.max(256, MAX_INFLIGHT + 64),
  maxFreeSockets: 512,
});

// ---- precompute one request body per partition (push_batch = 1) -------------
function makeBody(p) {
  const base = { message: 'x', partition_id: p };
  if (PAYLOAD_SIZE_BYTES > 0) {
    const pad = Math.max(0, PAYLOAD_SIZE_BYTES - JSON.stringify(base).length - 15);
    base.pad = 'x'.repeat(pad);
  }
  return Buffer.from(JSON.stringify({
    items: [{ queue: QUEUE_NAME, partition: `${p}`, payload: base }],
  }));
}
const bodies = Array.from({ length: PARTITIONS }, (_, p) => makeBody(p));

const reqOpts = (p) => ({
  protocol: u.protocol,
  hostname: u.hostname,
  port: u.port || 80,
  path: '/api/v1/push',
  method: 'POST',
  agent,
  headers: { 'Content-Type': 'application/json', 'Content-Length': bodies[p].length },
});

// ---- counters ---------------------------------------------------------------
let dispatched = 0, completed = 0, non2xx = 0, failed = 0;
let inflight = 0, maxInflight = 0;
// latency histogram: 1ms buckets up to 30s, plus an overflow bucket
const HBUCKETS = 30000;
const hist = new Int32Array(HBUCKETS + 1);
let latMax = 0;
// per-second timeline of {sec, intended, dispatched, completed}
const timeline = [];

function recordLatency(ms) {
  const b = ms >= HBUCKETS ? HBUCKETS : Math.max(0, Math.floor(ms));
  hist[b]++;
  if (ms > latMax) latMax = ms;
}
function percentile(p) {
  const target = Math.ceil((completed) * p);
  let acc = 0;
  for (let i = 0; i <= HBUCKETS; i++) { acc += hist[i]; if (acc >= target) return i; }
  return latMax;
}

function fire(p) {
  inflight++; if (inflight > maxInflight) maxInflight = inflight;
  const t0 = performance.now();
  const req = http.request(reqOpts(p), (res) => {
    res.on('data', () => {});
    res.on('end', () => {
      inflight--;
      recordLatency(performance.now() - t0);
      if (res.statusCode >= 200 && res.statusCode < 300) completed++; else non2xx++;
    });
  });
  req.on('error', () => { inflight--; failed++; });
  req.end(bodies[p]);
}

// ---- the open-loop scheduler ------------------------------------------------
// Drift-corrected: each tick computes how many requests SHOULD have been sent by
// now (elapsed * RATE) and fires the difference, bounded by MAX_INFLIGHT. If PG
// is keeping up, dispatched tracks intended exactly. If not, dispatched lags and
// the end-of-run deficit (intended - dispatched) is the saturation signal.
let start = 0, rr = 0, lastSec = -1, lastDispatchedAtSec = 0, lastCompletedAtSec = 0;
let timer = null;

function tick() {
  const elapsed = (performance.now() - start) / 1000;
  const intended = Math.floor(elapsed * RATE);
  while (dispatched < intended && inflight < MAX_INFLIGHT) {
    fire(rr % PARTITIONS); rr++; dispatched++;
  }
  const sec = Math.floor(elapsed);
  if (sec !== lastSec && sec >= 0) {
    timeline.push({
      sec,
      intended,
      dispatched_in_sec: dispatched - lastDispatchedAtSec,
      completed_in_sec: completed - lastCompletedAtSec,
      inflight,
    });
    lastSec = sec; lastDispatchedAtSec = dispatched; lastCompletedAtSec = completed;
  }
  if (elapsed >= DURATION) finish();
}

function finish() {
  if (timer) { clearInterval(timer); timer = null; }
  const wall = (performance.now() - start) / 1000;
  const intendedTotal = Math.floor(DURATION * RATE);
  // let a brief grace period drain in-flight so latency percentiles are honest
  setTimeout(() => {
    const summary = {
      config: { rate: RATE, duration: DURATION, partitions: PARTITIONS, max_inflight: MAX_INFLIGHT },
      wall_seconds: wall,
      offered_rps: RATE,
      intended_total: intendedTotal,
      dispatched_total: dispatched,
      completed_total: completed,
      non2xx, failed,
      deficit: Math.max(0, intendedTotal - dispatched), // dispatch-side back-pressure
      achieved_rps_client: completed / wall,            // client-side served rate
      max_inflight_seen: maxInflight,
      saturated_dispatch: dispatched < intendedTotal * 0.99,
      latency_ms: { p50: percentile(0.50), p90: percentile(0.90), p99: percentile(0.99), max: Math.round(latMax) },
      timeline,
    };
    writeFileSync(OUTPUT_FILE, JSON.stringify(summary, null, 2));
    console.log(`[pacer] rate=${RATE} offered=${intendedTotal} dispatched=${dispatched} completed=${completed} non2xx=${non2xx} failed=${failed} deficit=${summary.deficit} p50=${summary.latency_ms.p50}ms p99=${summary.latency_ms.p99}ms maxInflight=${maxInflight}`);
    process.exit(0);
  }, 1500);
}

// ---- ensure the queue exists, then run --------------------------------------
function ensureQueue() {
  return new Promise((resolve) => {
    const body = Buffer.from(JSON.stringify({
      queue: QUEUE_NAME,
      options: { leaseTime: 60, retryLimit: 3, retentionEnabled: true, retentionSeconds: 1800, completedRetentionSeconds: 1800 },
    }));
    const req = http.request({
      protocol: u.protocol, hostname: u.hostname, port: u.port || 80,
      path: '/api/v1/configure', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': body.length },
    }, (res) => { res.on('data', () => {}); res.on('end', resolve); });
    req.on('error', (e) => { console.error(`[pacer] configure warning: ${e.message}`); resolve(); });
    req.end(body);
  });
}

await ensureQueue();
start = performance.now();
// 1ms tick: at high RATE we fire many per tick (catch-up loop); at low RATE the
// granularity is far finer than the inter-arrival gap.
timer = setInterval(tick, 1);
