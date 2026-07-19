// Closed-loop SQL load harness for pgmq, designed to mirror the Queen
// autocannon harness (bench-producer.js / bench-consumer.js) so results are
// directly comparable.
//
// Model: ROLE=producer|consumer. Spawn CONNECTIONS concurrent "virtual clients",
// each in a tight loop issuing one operation, recording latency, repeating until
// the deadline. Connections go through PgBouncer (transaction pooling), which is
// the honest way to drive pgmq at high client concurrency.
//
// MODE=fifo  -> ordered, high-cardinality: producer sends to one x-pgmq-group
//               per push; consumer uses read_grouped_head (one msg per group,
//               up to N groups) — the direct analog of Queen partitions + the
//               multi-partition pop. This is the apples-to-apples comparison.
// MODE=plain -> SQS-style competing consumers (pgmq's best case, no ordering):
//               send_batch + read(qty) + delete.
//
// Output: a single JSON result line matching the Queen harness shape
// (role, event:'result', totalMessages, msgPerSec, latency{p50,p90,p99,avg,max},
//  errors, durationSec, ...).

import pg from 'pg';
import { randomUUID } from 'crypto';
const { Pool } = pg;

// ENGINE=queen calls Queen's stored procedures DIRECTLY over libpq (no broker,
// no HTTP hop) — the direct-to-Postgres deployment. This isolates Queen's SQL engine so
// we can compare it apples-to-apples with pgmq's functions, separating the
// engine from the broker/HTTP cost measured in the latency test.
const ENGINE          = process.env.ENGINE || 'pgmq';
const QUEEN_PARTITION = process.env.QUEEN_PARTITION || 'p0';
const QUEEN_WORKER_ID = 'w-' + Math.random().toString(36).slice(2, 8);

const ROLE            = process.env.ROLE || 'producer';            // producer | consumer
const MODE            = process.env.MODE || 'fifo';                // fifo | plain
const PGHOST          = process.env.PGHOST || 'localhost';
const PGPORT          = parseInt(process.env.PGPORT || '6432', 10); // pgbouncer
const PGDATABASE      = process.env.PGDATABASE || 'postgres';
const PGUSER          = process.env.PGUSER || 'postgres';
const PGPASSWORD      = process.env.PGPASSWORD || 'postgres';
const QUEUE           = process.env.QUEUE || 'bench';
const CONNECTIONS     = parseInt(process.env.CONNECTIONS || '50', 10);
const DURATION        = parseInt(process.env.DURATION || '60', 10);  // seconds
const MSGS_PER_PUSH   = parseInt(process.env.MSGS_PER_PUSH || '10', 10);
const READ_QTY        = parseInt(process.env.READ_QTY || '100', 10); // plain: msgs/read; fifo: groups/read
const NUM_PARTITIONS  = parseInt(process.env.NUM_PARTITIONS || '1000', 10); // fifo only
const NUM_QUEUES      = parseInt(process.env.NUM_QUEUES || '1', 10);        // plain: shard clients across N queues (QUEUE_0..QUEUE_{N-1})
const GROUPED_FN      = process.env.GROUPED_FN || 'read_grouped_head';      // fifo consume fn: read_grouped_head | read_grouped_rr
const ROUTING_KEY     = process.env.ROUTING_KEY || 'evt';            // fanout: topic routing key
const VT              = parseInt(process.env.VT || '30', 10);        // visibility timeout (s)
const PAYLOAD_BYTES   = parseInt(process.env.PAYLOAD_BYTES || '0', 10); // pad payload to ~N bytes
const EMPTY_BACKOFF_MS = parseInt(process.env.EMPTY_BACKOFF_MS || '5', 10);
const WARMUP_MS       = parseInt(process.env.WARMUP_MS || '0', 10);
const PROCESS_MS      = parseInt(process.env.PROCESS_MS || '0', 10);  // simulated work per message (worker-bound test)
const RECORD_EMPTY    = (process.env.RECORD_EMPTY || 'true') === 'true'; // record latency for empty consumer reads?

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- latency histogram: 0.1 ms buckets up to 30 s ----
const BUCKETS = 300000;
const hist = new Int32Array(BUCKETS);
let nOps = 0, nErr = 0, nMsg = 0, nLat = 0, latSumUs = 0, latMaxUs = 0;
let lastErr = null;

function record(us) {
  latSumUs += us;
  if (us > latMaxUs) latMaxUs = us;
  let b = Math.round(us / 100);
  if (b >= BUCKETS) b = BUCKETS - 1;
  if (b < 0) b = 0;
  hist[b]++;
  nLat++;
}
function pct(p) {
  if (nLat === 0) return 0;
  const target = Math.ceil((p / 100) * nLat);
  let c = 0;
  for (let i = 0; i < BUCKETS; i++) {
    c += hist[i];
    if (c >= target) return +(i * 0.1).toFixed(1);
  }
  return +((BUCKETS - 1) * 0.1).toFixed(1);
}

// ---- payload builder ----
const padStr = PAYLOAD_BYTES > 0 ? 'x'.repeat(Math.max(0, PAYLOAD_BYTES - 40)) : '';
function buildPayloads(n, gid) {
  const out = new Array(n);
  for (let i = 0; i < n; i++) {
    out[i] = padStr
      ? { message: 'Hello World', g: gid, pad: padStr }
      : { message: 'Hello World', g: gid };
  }
  return out;
}

// ---- operations ----
// Producer FIFO: K messages to one group in a single batched insert
// (send_batch with a per-message headers array, all the same group). This is
// pgmq's best-case batched path, so the comparison can't be called a strawman.
const SQL_SEND_FIFO =
  'SELECT pgmq.send_batch($1::text, ' +
  'ARRAY(SELECT jsonb_array_elements($2::jsonb)), ' +
  'ARRAY(SELECT $3::jsonb FROM generate_series(1, jsonb_array_length($2::jsonb))))';
// Producer plain: K messages, one batched insert (pgmq best case).
const SQL_SEND_PLAIN =
  'SELECT pgmq.send_batch($1::text, ARRAY(SELECT jsonb_array_elements($2::jsonb)))';
// Producer fanout: one topic publish fans K messages into EVERY bound queue
// (one per consumer group). $1 is the routing key. This is pgmq's idiomatic
// pub/sub — and it inserts a physical copy per queue (the M× write amplification).
const SQL_SEND_FANOUT =
  'SELECT pgmq.send_batch_topic($1::text, ARRAY(SELECT jsonb_array_elements($2::jsonb)))';
// Producer fanout+fifo: fan K messages of ONE partition (x-pgmq-group=$3) into
// EVERY bound queue, carrying the group header so each consumer-group queue can
// be drained per-partition FIFO via read_grouped_head. This is the faithful
// "1000 ordered partitions x N consumer groups" Smartchat shape on pgmq —
// it stacks BOTH pgmq taxes: Nx write amplification AND per-group head scans.
const SQL_SEND_FANOUT_FIFO =
  'SELECT pgmq.send_batch_topic($1::text, ' +
  'ARRAY(SELECT jsonb_array_elements($2::jsonb)), ' +
  'ARRAY(SELECT $3::jsonb FROM generate_series(1, jsonb_array_length($2::jsonb))))';
// Consumer: read + delete in one round-trip (gives pgmq its best latency).
const SQL_CONSUME_FIFO =
  `WITH r AS (SELECT msg_id FROM pgmq.${GROUPED_FN}($1::text,$2::int,$3::int)), ` +
  "ids AS (SELECT COALESCE(array_agg(msg_id),'{}'::bigint[]) AS a, count(*)::int AS n FROM r), " +
  "d AS (SELECT count(*)::int AS dn FROM pgmq.delete($1::text,(SELECT a FROM ids))) " +
  "SELECT (SELECT n FROM ids) AS n, (SELECT dn FROM d) AS deleted";
const SQL_CONSUME_PLAIN =
  "WITH r AS (SELECT msg_id FROM pgmq.read($1::text,$2::int,$3::int)), " +
  "ids AS (SELECT COALESCE(array_agg(msg_id),'{}'::bigint[]) AS a, count(*)::int AS n FROM r), " +
  "d AS (SELECT count(*)::int AS dn FROM pgmq.delete($1::text,(SELECT a FROM ids))) " +
  "SELECT (SELECT n FROM ids) AS n, (SELECT dn FROM d) AS deleted";

function makeOp(pool, q = QUEUE) {
  // ENGINE=queen: call Queen's stored procedures directly.
  if (ENGINE === 'queen') {
    if (ROLE === 'producer') {
      return async () => {
        const items = [];
        for (let i = 0; i < MSGS_PER_PUSH; i++) {
          items.push({ queue: QUEUE, partition: QUEEN_PARTITION, payload: padStr ? { message: 'Hello World', pad: padStr } : { message: 'Hello World' }, transactionId: randomUUID() });
        }
        await pool.query('SELECT queen.push_messages_v3($1::jsonb, true, false)', [JSON.stringify(items)]);
        return MSGS_PER_PUSH;
      };
    }
    const req = JSON.stringify([{ idx: 0, queue_name: QUEUE, partition_name: QUEEN_PARTITION,
      consumer_group: '__QUEUE_MODE__', batch_size: READ_QTY, lease_seconds: 60,
      worker_id: QUEEN_WORKER_ID, auto_ack: true, max_partitions: 1 }]);
    return async () => {
      const res = await pool.query(
        "SELECT COALESCE(jsonb_array_length(queen.pop_unified_batch_v4($1::jsonb)->0->'result'->'messages'),0) AS n", [req]);
      return res.rows[0].n;
    };
  }
  if (ROLE === 'producer') {
    if (MODE === 'fanoutfifo') {
      // K messages of one partition fanned to every queue, with the group header.
      return async () => {
        const gid = Math.floor(Math.random() * NUM_PARTITIONS);
        const payloads = buildPayloads(MSGS_PER_PUSH, gid);
        await pool.query(SQL_SEND_FANOUT_FIFO, [
          ROUTING_KEY,
          JSON.stringify(payloads),
          JSON.stringify({ 'x-pgmq-group': String(gid) }),
        ]);
        return MSGS_PER_PUSH;
      };
    }
    if (MODE === 'fanout') {
      // Returns LOGICAL messages (the physical ×N copies are the cost we measure
      // separately as write amplification, not as throughput credit).
      return async () => {
        const payloads = buildPayloads(MSGS_PER_PUSH, 0);
        await pool.query(SQL_SEND_FANOUT, [ROUTING_KEY, JSON.stringify(payloads)]);
        return MSGS_PER_PUSH;
      };
    }
    if (MODE === 'fifo') {
      return async () => {
        const gid = Math.floor(Math.random() * NUM_PARTITIONS);
        const payloads = buildPayloads(MSGS_PER_PUSH, gid);
        await pool.query(SQL_SEND_FIFO, [
          QUEUE,
          JSON.stringify(payloads),
          JSON.stringify({ 'x-pgmq-group': String(gid) }),
        ]);
        return MSGS_PER_PUSH;
      };
    }
    return async () => {
      const payloads = buildPayloads(MSGS_PER_PUSH, 0);
      await pool.query(SQL_SEND_PLAIN, [q, JSON.stringify(payloads)]);
      return MSGS_PER_PUSH;
    };
  }
  // consumer
  const sql = MODE === 'fifo' ? SQL_CONSUME_FIFO : SQL_CONSUME_PLAIN;
  return async () => {
    const res = await pool.query(sql, [q, VT, READ_QTY]);
    return res.rows[0] ? res.rows[0].n : 0;
  };
}

async function clientLoop(op, deadline) {
  while (Date.now() < deadline) {
    const t0 = process.hrtime.bigint();
    try {
      const n = await op();
      const us = Number(process.hrtime.bigint() - t0) / 1000;
      // timed op = read+delete only (broker op latency, excludes work sleep).
      // Skip empty consumer reads when RECORD_EMPTY=false so pop-latency isn't skewed.
      if (RECORD_EMPTY || ROLE !== 'consumer' || n > 0) record(us);
      nOps++;
      nMsg += n;
      // Simulated per-message work (worker-bound test) — OUTSIDE the timed op.
      if (n > 0 && ROLE === 'consumer' && PROCESS_MS > 0) {
        await sleep(PROCESS_MS * n);
      }
      if (n === 0 && ROLE === 'consumer' && EMPTY_BACKOFF_MS > 0) {
        await sleep(EMPTY_BACKOFF_MS);
      }
    } catch (e) {
      nErr++;
      lastErr = e.message;
      await sleep(5);
    }
  }
}

async function main() {
  const pool = new Pool({
    host: PGHOST, port: PGPORT, database: PGDATABASE,
    user: PGUSER, password: PGPASSWORD,
    max: CONNECTIONS,
    idleTimeoutMillis: 0,
    connectionTimeoutMillis: 10000,
    application_name: `pgmq-bench-${ROLE}`,
  });

  // fail fast if the stack isn't reachable
  await pool.query('SELECT 1');

  console.error(JSON.stringify({
    role: ROLE, event: 'start', mode: MODE,
    connections: CONNECTIONS, duration: DURATION,
    msgsPerPush: MSGS_PER_PUSH, readQty: READ_QTY,
    numPartitions: MODE === 'fifo' ? NUM_PARTITIONS : null,
    target: `${PGHOST}:${PGPORT}`, timestamp: new Date().toISOString(),
  }));

  if (WARMUP_MS > 0) await sleep(WARMUP_MS);

  const deadline = Date.now() + DURATION * 1000;
  const loops = [];
  for (let i = 0; i < CONNECTIONS; i++) {
    const q = NUM_QUEUES > 1 ? `${QUEUE}_${i % NUM_QUEUES}` : QUEUE;
    loops.push(clientLoop(makeOp(pool, q), deadline));
  }
  await Promise.all(loops);

  const elapsed = DURATION;
  const result = {
    role: ROLE, event: 'result', mode: MODE,
    connections: CONNECTIONS,
    totalOps: nOps,
    totalMessages: nMsg,
    msgPerSec: Math.round(nMsg / elapsed),
    opsPerSec: Math.round(nOps / elapsed),
    latency: {
      p50: pct(50), p90: pct(90), p99: pct(99),
      avg: nLat ? +(latSumUs / nLat / 1000).toFixed(2) : 0,
      max: +(latMaxUs / 1000).toFixed(2),
    },
    errors: nErr,
    lastError: lastErr,
    durationSec: elapsed,
  };
  console.log(JSON.stringify(result, null, 2));
  await pool.end();
  process.exit(0);
}

main().catch((e) => {
  console.error(JSON.stringify({ role: ROLE, event: 'fatal', error: e.message }));
  process.exit(1);
});
