#!/usr/bin/env node
// End-to-end verification of the storage v2 (segments) engine THROUGH the
// real server binary: fresh DB, full schema + procedures, ./server/bin/queen-server
// on PORT 6642, wire-level assertions against the HTTP API plus psql-level
// checks of q2.* state. Self-contained; exits non-zero on any FAIL.
//
//  [1]  push/pop/ack round-trip: 100 msgs over 4 partitions, order + exact ids
//  [2]  wildcard pop drains all partitions, shared lease, per-message partitionId
//  [3]  wait=true long-poll gets a message pushed 200ms later
//  [4]  duplicates: intra-batch, cross-batch, intra-fusion-window -> 'duplicate'
//       with the ORIGINAL message_id
//  [5]  nack -> full redelivery; partial ack -> resume at exact frame
//  [6]  retry_limit poison: configure retryLimit=2 + DLQ via /api/v1/configure,
//       nack 3x -> head lands in q2.dlq, queue unblocks
//  [7]  lease renew extends q2.consumers.lease_expires_at
//  [8]  atomic transaction: ack batch + push -> cursor advanced, lease released,
//       pushed msg poppable; mixed rows+segments -> 400
//  [9]  ack fallback cross-process: pop, RESTART server (registry gone), ack by
//       txn over HTTP -> q2.ack_by_txn_v1 advances the cursor
//  [10] retention: backdate segments+dedup, q2.retention_sweep_v1() -> segments
//       gone, stale cursor offset NOT applied after the gap
//  [11] eviction: q2.evict_v1() enforces max_queue_size
//  [12] encryption: QUEEN_ENCRYPTION_KEY + encryption_enabled -> blob stores an
//       encryption envelope (no plaintext marker), pop returns plaintext
//  [13] /metrics/prometheus exposes queen_queue_depth_* for both engines
//  [14] v1 regression: rows queue passes [1], [4], [5]
//  [15] fusion: 20 parallel single pushes -> <=NUM_WORKERS segments with
//       HOLD_MS=15; exactly 20 segments with HOLD_MS=0
//  [16] subscriptionMode=new / subscriptionFrom=now seed a fresh group PAST
//       the backlog (specific + wildcard pops); default group still sees all
//  [17] delayedProcessing=2 withholds delivery ~2s; windowBuffer=2 withholds
//       a partition whose newest segment is younger than the window
//  [18] GET /api/v1/dlq serves the q2 poison row with the v1 keys
//  [19] one push mixing a committed-dup txn with a fresh one: dup row
//       'duplicate' with the ORIGINAL mid AND the fresh row commits (repack)
//  [20] raw socket: empty pop answers 204 with NO body and NO Content-Length
//       (segments specific + wildcard + rows)
//  [21] ack rows carry index/success/error/queueName/partitionName/
//       leaseReleased/dlq; ack after lease expiry -> HTTP 200, per-item
//       success:false
//  [22] renew never SHORTENS: 60s renew on a 300s lease keeps the expiry
//  [23] retryLimit=2 -> exactly 3 deliveries before DLQ on BOTH engines
//       (segments/rows parity, same batch=1 nack protocol)
//  [24] /api/v1/transaction acks AFTER a restart (registry wiped) commit via
//       the SQL txns-form fallback, atomically with the transaction's push
//  [25] explicit ack status='dlq' dead-letters immediately (single ack and
//       batch-closing ack), no redelivery
//
// Usage:  node e2e-server-test.mjs
// Env:    PG on localhost:5440 (docker container queen-spike-pg), password
//         'postgres'. Creates/drops database queen_verify.
import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

const require = createRequire(import.meta.url);
const { Pool, Client } = require('../../proxy/node_modules/pg');

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '../..');
const SERVER_BIN = path.join(ROOT, 'server/bin/queen-server');
const SCHEMA_DIR = path.join(ROOT, 'lib/schema');

const PG = { host: 'localhost', port: 5440, user: 'postgres', password: 'postgres' };
const DB = 'queen_verify';
const PORT = 6642;
const NUM_WORKERS = 2;
const ENC_KEY = 'a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'; // 64 hex
const MARKER = 'PLAINTEXT_MARKER_1234567890_QUEEN';

// ---------------------------------------------------------------- reporting
let failures = 0;
function ok(cond, name, detail = '') {
  if (cond) console.log(`  PASS ${name}`);
  else { failures++; console.log(`  FAIL ${name}${detail ? ' :: ' + detail : ''}`); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- HTTP
// Minimal raw-socket HTTP/1.1 client, one connection per request.
// Deliberate: fresh sockets keep server restarts and the 20-parallel-push
// fusion test deterministic (no keep-alive pooling in the way). Note: empty
// pops answer 204 with NO body and NO Content-Length (RFC 9110) — the
// historical 204-with-JSON-body shape that llhttp rejected is gone; this
// client treats a header-only close as an empty body either way.
function req(method, urlPath, body, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? null : Buffer.from(JSON.stringify(body));
    const head =
      `${method} ${urlPath} HTTP/1.1\r\n` +
      `Host: localhost:${PORT}\r\n` +
      `Connection: close\r\n` +
      (data ? `Content-Type: application/json\r\nContent-Length: ${data.length}\r\n` : '') +
      '\r\n';
    const s = net.connect(PORT, 'localhost');
    const chunks = [];
    let settled = false;
    const finish = (fn, v) => {
      if (settled) return;
      settled = true;
      clearTimeout(tm);
      s.destroy();
      fn(v);
    };
    const tm = setTimeout(
      () => finish(reject, new Error(`HTTP timeout ${method} ${urlPath}`)), timeoutMs);
    const tryParse = (eof) => {
      const buf = Buffer.concat(chunks);
      const he = buf.indexOf('\r\n\r\n');
      if (he < 0) return;
      const headText = buf.subarray(0, he).toString('latin1');
      const st = headText.match(/^HTTP\/1\.[01] (\d{3})/);
      if (!st) return finish(reject, new Error(`bad status line: ${headText.slice(0, 60)}`));
      const cl = headText.match(/\r\ncontent-length:\s*(\d+)/i);
      const bodyStart = he + 4;
      let text = null;
      if (cl) {
        const need = bodyStart + Number(cl[1]);
        if (buf.length >= need) text = buf.subarray(bodyStart, need).toString('utf8');
      } else if (eof) {
        text = buf.subarray(bodyStart).toString('utf8');
      }
      if (text === null) return; // need more bytes
      let json = null;
      if (text.length) { try { json = JSON.parse(text); } catch { /* raw */ } }
      finish(resolve, { status: Number(st[1]), json, text });
    };
    s.on('error', (e) => finish(reject, e));
    s.on('connect', () => { s.write(head); if (data) s.write(data); });
    s.on('data', (c) => { chunks.push(c); tryParse(false); });
    s.on('end', () => tryParse(true));
  });
}
// Raw-wire variant for [20]: returns status line + verbatim header text and
// whatever bytes followed the header terminator (must be NONE for a 204).
function reqRaw(method, urlPath, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const head =
      `${method} ${urlPath} HTTP/1.1\r\n` +
      `Host: localhost:${PORT}\r\nConnection: close\r\n\r\n`;
    const s = net.connect(PORT, 'localhost');
    const chunks = [];
    const tm = setTimeout(() => { s.destroy(); reject(new Error(`raw HTTP timeout ${urlPath}`)); }, timeoutMs);
    s.on('error', (e) => { clearTimeout(tm); reject(e); });
    s.on('connect', () => s.write(head));
    s.on('data', (c) => chunks.push(c));
    s.on('close', () => {
      clearTimeout(tm);
      const buf = Buffer.concat(chunks);
      const he = buf.indexOf('\r\n\r\n');
      if (he < 0) return reject(new Error('no header terminator in raw response'));
      const headText = buf.subarray(0, he).toString('latin1');
      const st = headText.match(/^HTTP\/1\.[01] (\d{3})/);
      resolve({ status: st ? Number(st[1]) : 0, headText, body: buf.subarray(he + 4) });
    });
  });
}
const push = (items) => req('POST', '/api/v1/push', { items });
function popUrl(queue, partition, q = {}) {
  const qp = new URLSearchParams();
  for (const [k, v] of Object.entries(q)) qp.set(k, String(v));
  const qs = qp.toString() ? `?${qp.toString()}` : '';
  return partition
    ? `/api/v1/pop/queue/${queue}/partition/${partition}${qs}`
    : `/api/v1/pop/queue/${queue}${qs}`;
}
const pop = (queue, partition, q = {}, timeoutMs = 30000) =>
  req('GET', popUrl(queue, partition, q), undefined, timeoutMs);
const ackBatch = (acks, consumerGroup) =>
  req('POST', '/api/v1/ack/batch', { acknowledgments: acks, ...(consumerGroup ? { consumerGroup } : {}) });
const msgsOf = (r) => (r.json && Array.isArray(r.json.messages)) ? r.json.messages : [];

// ---------------------------------------------------------------- frames
// Mirror of server/include/queen/storage_v2.hpp unpack_frames (flags-aware).
function unpackFrames(raw) {
  const out = [];
  let o = 0;
  while (o + 4 <= raw.length) {
    const len = raw.readUInt32LE(o);
    const end = o + 4 + len;
    if (end > raw.length || len < 19) throw new Error('bad frame');
    let p = o + 4;
    const flags = raw.readUInt8(p); p += 1;
    const mid = raw.subarray(p, p + 16).toString('hex'); p += 16;
    if (flags & 1) p += 16; // trace id
    const txnLen = raw.readUInt16LE(p); p += 2;
    const txn = raw.subarray(p, p + txnLen).toString('utf8'); p += txnLen;
    if (flags & 2) { const n = raw.readUInt16LE(p); p += 2 + n; } // producer sub
    const payload = raw.subarray(p, end).toString('utf8');
    out.push({ flags, mid, txn, payload, encrypted: (flags & 4) !== 0 });
    o = end;
  }
  return out;
}

// ---------------------------------------------------------------- database
let pool = null;
const sql = (text, params = []) => pool.query(text, params);

async function freshDatabase() {
  const c = new Client({ ...PG, database: 'postgres' });
  await c.connect();
  await c.query(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
  await c.query(`CREATE DATABASE ${DB}`);
  await c.end();
}
async function applySchema() {
  const c = new Client({ ...PG, database: DB });
  await c.connect();
  await c.query(fs.readFileSync(path.join(SCHEMA_DIR, 'schema.sql'), 'utf8'));
  const procDir = path.join(SCHEMA_DIR, 'procedures');
  const files = fs.readdirSync(procDir).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) {
    try {
      await c.query(fs.readFileSync(path.join(procDir, f), 'utf8'));
    } catch (e) {
      await c.end();
      throw new Error(`applying ${f}: ${e.message}`);
    }
  }
  await c.end();
  return files.length;
}

// Test queues. e2edlq is configured through /api/v1/configure (exercises the
// configure path); e2eenc stays SQL-seeded (arbitrary — [12] also verifies
// the configure path accepts encryptionEnabled=true with storage='segments').
async function seedQueues() {
  const rows = [
    `INSERT INTO queen.queues (name, storage) VALUES ('e2e', 'segments')`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2ewild', 'segments')`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2ewait', 'segments')`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2edup', 'segments')`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2etxn', 'segments')`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2efuse', 'segments')`,
    `INSERT INTO queen.queues (name, storage, encryption_enabled) VALUES ('e2eenc', 'segments', true)`,
    `INSERT INTO queen.queues (name, storage, retention_enabled, retention_seconds) VALUES ('e2eret', 'segments', true, 3600)`,
    `INSERT INTO queen.queues (name, storage, max_queue_size) VALUES ('e2eevict', 'segments', 50)`,
    `INSERT INTO queen.queues (name, storage) VALUES ('e2esub', 'segments')`,
    `INSERT INTO queen.queues (name, retry_delay) VALUES ('e2erows', 0)`,
  ];
  for (const r of rows) await sql(r);
}

const q2PartitionId = async (queue, partition) =>
  (await sql(
    `SELECT p.id FROM q2.partitions p JOIN q2.queues q ON q.id = p.queue_id
     WHERE q.name = $1 AND p.name = $2`, [queue, partition])).rows[0]?.id;
const q2Consumer = async (partitionId, group = '__QUEUE_MODE__') =>
  (await sql(
    `SELECT worker_id, lease_expires_at, next_seq::int AS next_seq, next_off,
            batch_end_seq::int AS batch_end_seq, total_consumed::int AS total_consumed
     FROM q2.consumers WHERE partition_id = $1 AND consumer_group = $2`,
    [partitionId, group])).rows[0];
const q2SegStats = async (queue, partition) =>
  (await sql(
    `SELECT COUNT(*)::int AS segs, COALESCE(SUM(s.msg_count), 0)::int AS frames
     FROM q2.segments s JOIN q2.partitions p ON p.id = s.partition_id
     JOIN q2.queues q ON q.id = p.queue_id
     WHERE q.name = $1 ${partition ? 'AND p.name = $2' : ''}`,
    partition ? [queue, partition] : [queue])).rows[0];

// ---------------------------------------------------------------- server
let server = null;
let serverLog = null;
function startServer(extraEnv = {}, tag = 'run') {
  const logPath = path.join(os.tmpdir(), `queen-e2e-${PORT}-${tag}.log`);
  const out = fs.openSync(logPath, 'w');
  serverLog = logPath;
  server = spawn(SERVER_BIN, [], {
    cwd: ROOT,
    stdio: ['ignore', out, out],
    env: {
      ...process.env,
      PG_HOST: PG.host, PG_PORT: String(PG.port), PG_USER: PG.user,
      PG_PASSWORD: PG.password, PG_DB: DB,
      PORT: String(PORT), NUM_WORKERS: String(NUM_WORKERS),
      DB_POOL_SIZE: '40', SIDECAR_POOL_SIZE: '30',
      QUEEN_ENCRYPTION_KEY: ENC_KEY,
      // Sweeps are driven EXPLICITLY by this suite (tests [10]/[11]); park the
      // background services so they cannot race the assertions.
      RETENTION_INTERVAL: '600000', EVICTION_INTERVAL: '600000',
      ...extraEnv,
    },
  });
  server.on('exit', (code, sig) => {
    if (server && !server._expectedExit) {
      console.error(`server exited unexpectedly (code=${code} sig=${sig}); log: ${logPath}`);
    }
  });
  console.log(`server started (pid ${server.pid}, log ${logPath})`);
  return waitHealthy();
}
async function waitHealthy() {
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    try {
      const r = await req('GET', '/health', undefined, 2000);
      if (r.status === 200) return;
    } catch { /* not up yet */ }
    await sleep(250);
  }
  throw new Error(`server did not become healthy; log: ${serverLog}`);
}
function stopServer() {
  if (!server) return Promise.resolve();
  const p = server;
  server._expectedExit = true;
  return new Promise((resolve) => {
    const t = setTimeout(() => { try { p.kill('SIGKILL'); } catch {} }, 5000);
    p.once('exit', () => { clearTimeout(t); resolve(); });
    p.kill('SIGTERM');
  }).then(() => { server = null; });
}

// ==========================================================================
// Tests
// ==========================================================================

async function t1_roundTrip() {
  console.log('[1] push/pop/ack round-trip (100 msgs, 4 partitions)');
  const parts = ['p0', 'p1', 'p2', 'p3'];
  const pushedIds = new Map(); // txn -> message_id
  for (const p of parts) {
    const items = Array.from({ length: 25 }, (_, i) => ({
      queue: 'e2e', partition: p, transactionId: `rt-${p}-${i}`,
      payload: { p, i, filler: 'x'.repeat(120) },
    }));
    const r = await push(items);
    ok(r.status === 201 && Array.isArray(r.json) && r.json.length === 25 &&
       r.json.every((x) => x.status === 'queued' && x.message_id),
       `[1] push ${p} all queued`, JSON.stringify(r.json?.[0] ?? r.text).slice(0, 200));
    for (const x of r.json ?? []) pushedIds.set(x.transaction_id, x.message_id);
  }
  ok(pushedIds.size === 100, '[1] 100 unique txns pushed', `got ${pushedIds.size}`);

  let allGood = true, orderGood = true, idGood = true, dataGood = true;
  for (const p of parts) {
    const r = await pop('e2e', p, { batch: 200 });
    const msgs = msgsOf(r);
    allGood &&= (r.status === 200 && msgs.length === 25);
    msgs.forEach((m, i) => {
      orderGood &&= m.transactionId === `rt-${p}-${i}`;
      idGood &&= pushedIds.get(m.transactionId) === m.id;
      dataGood &&= (m.data && m.data.p === p && m.data.i === i);
    });
    const acks = msgs.map((m) => ({
      transactionId: m.transactionId, partitionId: m.partitionId,
      leaseId: m.leaseId, status: 'completed',
    }));
    const a = await ackBatch(acks);
    // v1 wire rows: {index, transactionId, success, error, queueName,
    // partitionName, leaseReleased, dlq} (003_ack.sql shape).
    allGood &&= (a.status === 200 && Array.isArray(a.json) &&
                 a.json.every((x) => x.success === true && x.queueName === 'e2e' &&
                                     x.partitionName === p && x.dlq === false));
  }
  ok(allGood, '[1] pop returned 25/partition and acks completed');
  ok(orderGood, '[1] per-partition order exact');
  ok(idGood, '[1] message ids match push responses exactly');
  ok(dataGood, '[1] payloads round-tripped');
  const again = await pop('e2e', 'p0', { batch: 10 });
  ok(again.status === 204 && msgsOf(again).length === 0,
     '[1] re-pop after ack is empty', `status ${again.status}`);
}

async function t2_wildcard() {
  console.log('[2] wildcard pop across partitions');
  const parts = ['wa', 'wb', 'wc', 'wd'];
  for (const p of parts) {
    const items = Array.from({ length: 10 }, (_, i) => ({
      queue: 'e2ewild', partition: p, transactionId: `w-${p}-${i}`,
      payload: { p, i },
    }));
    const r = await push(items);
    ok(r.status === 201 && r.json.every((x) => x.status === 'queued'),
       `[2] seeded ${p}`);
  }
  const r = await pop('e2ewild', null, { batch: 100, partitions: 10 });
  const msgs = msgsOf(r);
  ok(r.status === 200 && msgs.length === 40, '[2] wildcard drained 40 msgs',
     `status ${r.status} got ${msgs.length}`);
  const leaseIds = new Set(msgs.map((m) => m.leaseId));
  ok(leaseIds.size === 1 && msgs[0].leaseId === r.json.leaseId,
     '[2] single shared leaseId', JSON.stringify([...leaseIds]));
  const pids = new Set(msgs.map((m) => m.partitionId));
  ok(pids.size === 4, '[2] 4 distinct per-message partitionIds', `got ${pids.size}`);
  let pidGood = true;
  for (const p of parts) {
    const dbPid = await q2PartitionId('e2ewild', p);
    const claimed = msgs.filter((m) => m.partition === p);
    pidGood &&= claimed.length === 10 && claimed.every((m) => m.partitionId === dbPid);
  }
  ok(pidGood, '[2] per-message partitionId matches q2.partitions uuid');
  ok(r.json.partitionsClaimed === 4, '[2] partitionsClaimed=4',
     `got ${r.json.partitionsClaimed}`);

  const a = await ackBatch(msgs.map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'completed',
  })));
  ok(a.status === 200 && a.json.every((x) => x.success === true),
     '[2] batch ack over shared lease completed (v1 success rows)');
  let cursors = true;
  for (const p of parts) {
    const c = await q2Consumer(await q2PartitionId('e2ewild', p));
    cursors &&= c && c.worker_id === null && c.next_seq === 2 && c.next_off === 0;
  }
  ok(cursors, '[2] all 4 cursors advanced, leases released (psql)');
  const empty = await pop('e2ewild', null, { batch: 100, partitions: 10 });
  ok(empty.status === 204, '[2] wildcard re-pop empty', `status ${empty.status}`);
}

async function t3_waitPop() {
  console.log('[3] wait=true long-poll');
  const t0 = Date.now();
  const popP = pop('e2ewait', 'Default', { wait: true, timeout: 5000, batch: 5 }, 10000);
  await sleep(200);
  await push([{ queue: 'e2ewait', partition: 'Default', transactionId: 'wait-0', payload: { hello: 1 } }]);
  const r = await popP;
  const dt = Date.now() - t0;
  const msgs = msgsOf(r);
  ok(r.status === 200 && msgs.length === 1 && msgs[0].transactionId === 'wait-0',
     '[3] parked pop delivered the late message', `status ${r.status} n=${msgs.length}`);
  ok(dt >= 190 && dt < 4500, '[3] answered after the push, before the deadline', `${dt}ms`);
  await ackBatch([{ transactionId: 'wait-0', partitionId: msgs[0].partitionId, leaseId: msgs[0].leaseId, status: 'completed' }]);
}

async function t4_duplicates() {
  console.log('[4] duplicate handling (intra-batch, cross-batch, fusion-window)');
  // intra-batch
  const r1 = await push([
    { queue: 'e2edup', partition: 'Default', transactionId: 'dup-A', payload: { v: 1 } },
    { queue: 'e2edup', partition: 'Default', transactionId: 'dup-A', payload: { v: 2 } },
  ]);
  const first = r1.json?.[0], second = r1.json?.[1];
  ok(r1.status === 201 && first?.status === 'queued' && second?.status === 'duplicate' &&
     second?.message_id === first?.message_id,
     '[4] intra-batch dup -> duplicate with original message_id',
     JSON.stringify(r1.json).slice(0, 300));

  // cross-batch (second request, distinct fusion window because we await)
  const rb1 = await push([{ queue: 'e2edup', partition: 'Default', transactionId: 'dup-B', payload: { v: 1 } }]);
  const rb2 = await push([{ queue: 'e2edup', partition: 'Default', transactionId: 'dup-B', payload: { v: 2 } }]);
  ok(rb1.json?.[0]?.status === 'queued' && rb2.json?.[0]?.status === 'duplicate' &&
     rb2.json?.[0]?.message_id === rb1.json?.[0]?.message_id,
     '[4] cross-batch dup -> duplicate with original message_id',
     JSON.stringify([rb1.json?.[0], rb2.json?.[0]]));

  // intra-fusion-window: two CONCURRENT single pushes with the same txn.
  const [rc1, rc2] = await Promise.all([
    push([{ queue: 'e2edup', partition: 'Default', transactionId: 'dup-C', payload: { v: 1 } }]),
    push([{ queue: 'e2edup', partition: 'Default', transactionId: 'dup-C', payload: { v: 2 } }]),
  ]);
  const both = [rc1.json?.[0], rc2.json?.[0]];
  const queued = both.find((x) => x?.status === 'queued');
  const dup = both.find((x) => x?.status === 'duplicate');
  ok(!!queued && !!dup && dup.message_id === queued.message_id,
     '[4] fusion-window dup -> one queued, one duplicate with same message_id',
     JSON.stringify(both));

  // nothing double-delivered
  const seen = await pop('e2edup', 'Default', { batch: 100 });
  const msgs = msgsOf(seen);
  ok(msgs.length === 3 &&
     new Set(msgs.map((m) => m.transactionId)).size === 3,
     '[4] exactly one copy of each txn delivered', `got ${msgs.map((m) => m.transactionId)}`);
  await ackBatch(msgs.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t5_nackPartial() {
  console.log('[5] nack full redelivery + partial ack resume');
  const items = Array.from({ length: 10 }, (_, i) => ({
    queue: 'e2e', partition: 'pnack', transactionId: `n-${i}`, payload: { i },
  }));
  await push(items);
  const p1 = await pop('e2e', 'pnack', { batch: 10 });
  const m1 = msgsOf(p1);
  ok(m1.length === 10, '[5] delivered 10');
  // nack: single failed ack closes the batch with an empty acked prefix
  const nack = await req('POST', '/api/v1/ack', {
    transactionId: m1[0].transactionId, partitionId: m1[0].partitionId,
    leaseId: m1[0].leaseId, status: 'failed',
  });
  ok(nack.status === 200, '[5] nack accepted', `status ${nack.status}`);
  const p2 = await pop('e2e', 'pnack', { batch: 10 });
  const m2 = msgsOf(p2);
  ok(m2.length === 10 && m2[0].transactionId === 'n-0',
     '[5] nack -> full redelivery from n-0', `n=${m2.length} first=${m2[0]?.transactionId}`);

  // partial: ack n-0..n-5 completed, n-6 failed -> cursor stops after n-5
  const acks = m2.slice(0, 6).map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'completed',
  }));
  acks.push({ transactionId: m2[6].transactionId, partitionId: m2[6].partitionId,
              leaseId: m2[6].leaseId, status: 'failed' });
  const pa = await ackBatch(acks);
  ok(pa.status === 200, '[5] partial ack accepted', `status ${pa.status}`);
  const p3 = await pop('e2e', 'pnack', { batch: 10 });
  const m3 = msgsOf(p3);
  ok(m3.length === 4 && m3[0].transactionId === 'n-6' && m3[3].transactionId === 'n-9',
     '[5] redelivery resumes at exact frame n-6', `n=${m3.length} first=${m3[0]?.transactionId}`);
  await ackBatch(m3.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t6_dlq() {
  console.log('[6] retry_limit poison -> q2.dlq');
  // Exercise the configure path (persists storage flag + refreshes the config
  // cache in-process; retryLimit/deadLetterQueue drive the pop-side DLQ).
  const cfg = await req('POST', '/api/v1/configure', {
    queue: 'e2edlq',
    options: { storage: 'segments', retryLimit: 2, deadLetterQueue: true, leaseTime: 300 },
  });
  ok(cfg.status === 200 || cfg.status === 201,
     '[6] configure storage=segments retryLimit=2 dlq=true', `status ${cfg.status} ${cfg.text?.slice(0, 200)}`);

  const items = Array.from({ length: 5 }, (_, i) => ({
    queue: 'e2edlq', partition: 'Default', transactionId: `dlq-${i}`, payload: { i, poison: i === 0 },
  }));
  await push(items);

  // v1 grants retryLimit retries AFTER the first delivery: at retryLimit=2
  // the head is served 3 times (verified against the rows engine) before the
  // 4th pop dead-letters it.
  for (let round = 1; round <= 3; round++) {
    const p = await pop('e2edlq', 'Default', { batch: 5 });
    const ms = msgsOf(p);
    ok(ms.length === 5 && ms[0].transactionId === 'dlq-0', `[6] attempt ${round} delivered from head`);
    const nk = await req('POST', '/api/v1/ack', {
      transactionId: ms[0].transactionId, partitionId: ms[0].partitionId,
      leaseId: ms[0].leaseId, status: 'failed',
    });
    ok(nk.status === 200, `[6] attempt ${round} nacked`);
  }

  // 4th pop: attempt 4 > retryLimit 2 + first delivery -> head dead-lettered,
  // re-pop serves the rest
  const p3 = await pop('e2edlq', 'Default', { batch: 5 });
  const m3 = msgsOf(p3);
  ok(m3.length === 4 && m3[0].transactionId === 'dlq-1',
     '[6] queue unblocked: pop after eviction starts at dlq-1', `n=${m3.length} first=${m3[0]?.transactionId}`);
  const dlqRows = (await sql(
    `SELECT d.transaction_id, d.consumer_group, d.error, d.payload
     FROM q2.dlq d JOIN q2.partitions p ON p.id = d.partition_id
     JOIN q2.queues q ON q.id = p.queue_id WHERE q.name = 'e2edlq'`)).rows;
  ok(dlqRows.length === 1 && dlqRows[0].transaction_id === 'dlq-0' &&
     dlqRows[0].consumer_group === '__QUEUE_MODE__' &&
     String(dlqRows[0].error).includes('Retries exhausted') &&
     dlqRows[0].payload && dlqRows[0].payload.poison === true,
     '[6] q2.dlq holds the poison head with payload snapshot (psql)',
     JSON.stringify(dlqRows).slice(0, 300));
  await ackBatch(m3.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
  const after = await pop('e2edlq', 'Default', { batch: 5 });
  ok(after.status === 204, '[6] queue drained after ack', `status ${after.status}`);
}

async function t7_renew() {
  console.log('[7] lease renew');
  await push(Array.from({ length: 3 }, (_, i) => ({
    queue: 'e2e', partition: 'prenew', transactionId: `rn-${i}`, payload: { i },
  })));
  const p = await pop('e2e', 'prenew', { batch: 3 });
  const ms = msgsOf(p);
  const pid = await q2PartitionId('e2e', 'prenew');
  const before = await q2Consumer(pid);
  ok(!!before?.lease_expires_at && before.worker_id === ms[0].leaseId,
     '[7] lease registered in q2.consumers', JSON.stringify(before));
  const ext = await req('POST', `/api/v1/lease/${ms[0].leaseId}/extend`, { seconds: 900 });
  ok(ext.status === 200 && Array.isArray(ext.json) && ext.json[0]?.success === true,
     '[7] extend endpoint success', `status ${ext.status} ${ext.text?.slice(0, 200)}`);
  const after = await q2Consumer(pid);
  ok(new Date(after.lease_expires_at).getTime() >
     new Date(before.lease_expires_at).getTime() + 400000,
     '[7] lease_expires_at extended (psql)',
     `${before.lease_expires_at} -> ${after.lease_expires_at}`);
  await ackBatch(ms.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t8_transaction() {
  console.log('[8] atomic transaction (ack batch + push)');
  await push(Array.from({ length: 5 }, (_, i) => ({
    queue: 'e2etxn', partition: 'Default', transactionId: `t-${i}`, payload: { i },
  })));
  const p = await pop('e2etxn', 'Default', { batch: 5 });
  const ms = msgsOf(p);
  ok(ms.length === 5, '[8] popped leased batch of 5');
  const pid = ms[0].partitionId;

  const operations = ms.map((m) => ({
    type: 'ack', transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'completed',
  }));
  operations.push({ type: 'push', items: [{ queue: 'e2etxn', partition: 'out', transactionId: 'txn-out-1', payload: { fromTxn: true } }] });
  const tr = await req('POST', '/api/v1/transaction', { operations });
  ok(tr.status === 200 && tr.json?.success === true &&
     Array.isArray(tr.json?.results) && tr.json.results.length === 6 &&
     tr.json.results.every((x) => x?.success === true),
     '[8] transaction committed', `status ${tr.status} ${tr.text?.slice(0, 300)}`);

  const c = await q2Consumer(pid);
  ok(c && c.worker_id === null && c.next_seq === 2 && c.next_off === 0 &&
     c.batch_end_seq === null && c.total_consumed === 5,
     '[8] cursor advanced + lease released atomically (psql)', JSON.stringify(c));
  const outPop = await pop('e2etxn', 'out', { batch: 5 });
  const outMs = msgsOf(outPop);
  ok(outMs.length === 1 && outMs[0].transactionId === 'txn-out-1' &&
     outMs[0].data?.fromTxn === true,
     '[8] transaction-pushed message poppable', `n=${outMs.length}`);
  await ackBatch(outMs.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));

  const mixed = await req('POST', '/api/v1/transaction', { operations: [
    { type: 'push', items: [{ queue: 'e2e', partition: 'Default', payload: { a: 1 } }] },
    { type: 'push', items: [{ queue: 'e2erows', partition: 'Default', payload: { b: 2 } }] },
  ] });
  ok(mixed.status === 400, '[8] mixed rows+segments transaction rejected 400',
     `status ${mixed.status} ${mixed.text?.slice(0, 200)}`);
}

async function t10_retention() {
  console.log('[10] retention sweep + cursor guard');
  for (let b = 0; b < 3; b++) {
    await push(Array.from({ length: 10 }, (_, i) => ({
      queue: 'e2eret', partition: 'Default', transactionId: `r-${b * 10 + i}`, payload: { i: b * 10 + i },
    })));
  }
  const pre = await q2SegStats('e2eret', 'Default');
  ok(pre.segs === 3 && pre.frames === 30, '[10] 3 segments of 10 seeded (psql)', JSON.stringify(pre));
  const p1 = await pop('e2eret', 'Default', { batch: 15 });
  const m1 = msgsOf(p1);
  await ackBatch(m1.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
  const pid = await q2PartitionId('e2eret', 'Default');
  const mid = await q2Consumer(pid);
  ok(m1.length === 15 && mid.next_seq === 2 && mid.next_off === 5,
     '[10] cursor mid-segment (2,5) before sweep', JSON.stringify(mid));

  await sql(`UPDATE q2.segments SET created_at = created_at - interval '2 hours'
             WHERE partition_id = $1`, [pid]);
  await sql(`UPDATE q2.dedup SET created_at = created_at - interval '2 hours'
             WHERE partition_id = $1`, [pid]);
  const sweep = (await sql(`SELECT q2.retention_sweep_v1() AS r`)).rows[0].r;
  ok(sweep.segments_deleted === 3 && sweep.dedup_purged === 30,
     '[10] sweep deleted 3 segments + purged 30 dedup entries', JSON.stringify(sweep));
  const post = await q2SegStats('e2eret', 'Default');
  const wm = (await sql(`SELECT retention_seq::int AS ws FROM q2.partitions WHERE id = $1`, [pid])).rows[0];
  ok(post.segs === 0 && wm.ws === 4, '[10] segments gone, watermark at 4 (psql)',
     JSON.stringify({ post, wm }));

  await push(Array.from({ length: 10 }, (_, i) => ({
    queue: 'e2eret', partition: 'Default', transactionId: `rnew-${i}`, payload: { i },
  })));
  const p2 = await pop('e2eret', 'Default', { batch: 100 });
  const m2 = msgsOf(p2);
  ok(m2.length === 10 && m2[0].transactionId === 'rnew-0',
     '[10] stale offset NOT applied to post-gap segment (10 msgs from frame 0)',
     `n=${m2.length} first=${m2[0]?.transactionId}`);
  await ackBatch(m2.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t11_eviction() {
  console.log('[11] eviction enforces max_queue_size');
  for (let b = 0; b < 10; b++) {
    await push(Array.from({ length: 10 }, (_, i) => ({
      queue: 'e2eevict', partition: 'Default', transactionId: `e-${b * 10 + i}`, payload: { i: b * 10 + i },
    })));
  }
  const pre = await q2SegStats('e2eevict', 'Default');
  ok(pre.segs === 10 && pre.frames === 100, '[11] 100 frames in 10 segments (psql)', JSON.stringify(pre));
  const ev = (await sql(`SELECT q2.evict_v1() AS r`)).rows[0].r;
  ok(ev.queues === 1 && ev.segments_deleted === 5 && ev.messages_evicted === 50,
     '[11] evict_v1 dropped oldest 5 segments / 50 msgs', JSON.stringify(ev));
  const post = await q2SegStats('e2eevict', 'Default');
  ok(post.frames === 50, '[11] cap enforced: 50 frames remain (psql)', JSON.stringify(post));
  const p = await pop('e2eevict', 'Default', { batch: 100 });
  const ms = msgsOf(p);
  ok(ms.length === 50 && ms[0].transactionId === 'e-50',
     '[11] survivors start at e-50 (gap tolerated by pop)',
     `n=${ms.length} first=${ms[0]?.transactionId}`);
  await ackBatch(ms.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t12_encryption() {
  console.log('[12] encryption at rest');
  const r = await push([{ queue: 'e2eenc', partition: 'Default', transactionId: 'enc-0', payload: { secret: MARKER, n: 7 } }]);
  ok(r.status === 201 && r.json?.[0]?.status === 'queued', '[12] push to encrypted queue queued');
  const blobRow = (await sql(
    `SELECT s.blob FROM q2.segments s JOIN q2.partitions p ON p.id = s.partition_id
     JOIN q2.queues q ON q.id = p.queue_id WHERE q.name = 'e2eenc' ORDER BY s.seq DESC LIMIT 1`)).rows[0];
  const raw = zlib.zstdDecompressSync(blobRow.blob);
  const frames = unpackFrames(raw);
  const f = frames.find((x) => x.txn === 'enc-0');
  let env = null; try { env = JSON.parse(f.payload); } catch {}
  ok(!raw.toString('latin1').includes(MARKER),
     '[12] stored segment contains NO plaintext marker (psql blob)');
  ok(!!f && f.encrypted && env && env.encrypted && env.iv && env.authTag,
     '[12] frame flagged encrypted, payload is {encrypted,iv,authTag} envelope',
     f ? `flags=${f.flags}` : 'frame missing');
  const p = await pop('e2eenc', 'Default', { batch: 5 });
  const ms = msgsOf(p);
  ok(ms.length === 1 && ms[0].data?.secret === MARKER && ms[0].data?.n === 7,
     '[12] pop returns decrypted plaintext', JSON.stringify(ms[0]?.data));
  await ackBatch(ms.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));

  // control: same marker on a non-encrypted segments queue IS visible
  await push([{ queue: 'e2e', partition: 'penc', transactionId: 'ctl-0', payload: { secret: MARKER } }]);
  const ctlRow = (await sql(
    `SELECT s.blob FROM q2.segments s JOIN q2.partitions p ON p.id = s.partition_id
     JOIN q2.queues q ON q.id = p.queue_id WHERE q.name = 'e2e' AND p.name = 'penc'
     ORDER BY s.seq DESC LIMIT 1`)).rows[0];
  ok(zlib.zstdDecompressSync(ctlRow.blob).toString('latin1').includes(MARKER),
     '[12] control: plaintext queue stores the marker verbatim');
  const cp = await pop('e2e', 'penc', { batch: 5 });
  await ackBatch(msgsOf(cp).map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));

  // the stale slice-1 guard is gone: configure accepts encryption+segments
  // (engine support verified by the asserts above)
  const cfg = await req('POST', '/api/v1/configure', {
    queue: 'e2eenc2', options: { storage: 'segments', encryptionEnabled: true },
  });
  ok(cfg.status === 200 || cfg.status === 201,
     '[12] configure accepts encryption+segments',
     `status ${cfg.status}`);
}

async function t14_v1Regression() {
  console.log('[14] v1 rows-engine regression ([1],[4],[5] shapes)');
  // [1] round-trip on 2 partitions. NOTE: sequential single-item pushes.
  // v1's FIFO guarantee is cross-push (PUSHSER commit-ordered created_at);
  // WITHIN one multi-item push the rows engine stamps created_at in its
  // internal processing order (observed: lexicographic by transactionId),
  // so within-batch item order is unspecified on master. The segments
  // engine is strictly item-ordered either way ([1] asserts it).
  const parts = ['q0', 'q1'];
  for (const p of parts) {
    let allQueued = true;
    for (let i = 0; i < 20; i++) {
      const r = await push([{ queue: 'e2erows', partition: p, transactionId: `v1-${p}-${i}`, payload: { p, i } }]);
      allQueued &&= (r.status === 201 && r.json?.[0]?.status === 'queued');
    }
    ok(allQueued, `[14] v1 push ${p} queued (20 sequential pushes)`);
  }
  let rt = true, order = true;
  for (const p of parts) {
    const r = await pop('e2erows', p, { batch: 50 });
    const ms = msgsOf(r);
    rt &&= (r.status === 200 && ms.length === 20);
    ms.forEach((m, i) => { order &&= m.transactionId === `v1-${p}-${i}`; });
    const a = await ackBatch(ms.map((m) => ({
      transactionId: m.transactionId, partitionId: m.partitionId,
      leaseId: m.leaseId, status: 'completed',
    })));
    rt &&= a.status === 200;
  }
  ok(rt, '[14] v1 round-trip 20/partition popped + acked');
  ok(order, '[14] v1 per-partition order exact');
  const empty = await pop('e2erows', 'q0', { batch: 10 });
  ok(empty.status === 204, '[14] v1 re-pop empty', `status ${empty.status}`);

  // [4] duplicates
  const d1 = await push([
    { queue: 'e2erows', partition: 'q0', transactionId: 'v1dup-A', payload: { v: 1 } },
    { queue: 'e2erows', partition: 'q0', transactionId: 'v1dup-A', payload: { v: 2 } },
  ]);
  const statuses = (d1.json ?? []).map((x) => x.status).sort();
  ok(d1.status === 201 && statuses.join(',') === 'duplicate,queued',
     '[14] v1 intra-batch dup -> one queued one duplicate', JSON.stringify(d1.json));
  const d2 = await push([{ queue: 'e2erows', partition: 'q0', transactionId: 'v1dup-A', payload: { v: 3 } }]);
  ok(d2.status === 201 && d2.json?.[0]?.status === 'duplicate',
     '[14] v1 cross-batch dup -> duplicate', JSON.stringify(d2.json));
  const dd = await pop('e2erows', 'q0', { batch: 10 });
  const dms = msgsOf(dd);
  ok(dms.length === 1 && dms[0].transactionId === 'v1dup-A',
     '[14] v1 exactly one copy delivered', `n=${dms.length}`);
  await ackBatch(dms.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));

  // [5] nack full redelivery + partial ack resume
  await push(Array.from({ length: 10 }, (_, i) => ({
    queue: 'e2erows', partition: 'q2', transactionId: `v1n-${i}`, payload: { i },
  })));
  const p1 = await pop('e2erows', 'q2', { batch: 10 });
  const m1 = msgsOf(p1);
  ok(m1.length === 10, '[14] v1 delivered 10');
  const nk = await ackBatch(m1.map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'failed',
  })));
  ok(nk.status === 200, '[14] v1 nack accepted', `status ${nk.status}`);
  await sleep(300); // retry_delay=0 on e2erows; small grace for visibility
  const p2 = await pop('e2erows', 'q2', { batch: 10 });
  const m2 = msgsOf(p2);
  ok(m2.length === 10 && m2[0].transactionId === 'v1n-0',
     '[14] v1 nack -> full redelivery', `n=${m2.length} first=${m2[0]?.transactionId}`);
  const acks = m2.slice(0, 6).map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'completed',
  })).concat(m2.slice(6).map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: 'failed',
  })));
  const pa = await ackBatch(acks);
  ok(pa.status === 200, '[14] v1 partial ack accepted');
  await sleep(300);
  const p3 = await pop('e2erows', 'q2', { batch: 10 });
  const m3 = msgsOf(p3);
  ok(m3.length === 4 && m3[0].transactionId === 'v1n-6',
     '[14] v1 redelivery resumes at exact message v1n-6',
     `n=${m3.length} first=${m3[0]?.transactionId}`);
  await ackBatch(m3.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })));
}

async function t13_prometheus() {
  console.log('[13] prometheus queue depth for both engines');
  await req('POST', '/api/v1/stats/refresh', {});
  let text = '';
  for (let i = 0; i < 10; i++) {
    const r = await req('GET', '/metrics/prometheus');
    text = r.text || '';
    if (/queen_queue_depth_total_messages\{[^}]*queue="e2erows"/.test(text) &&
        /queen_queue_depth_total_messages\{[^}]*queue="e2e"/.test(text)) break;
    await sleep(1000);
    if (i === 4) await req('POST', '/api/v1/stats/refresh', {});
  }
  const segLine = text.split('\n').find((l) =>
    l.startsWith('queen_queue_depth_total_messages') && l.includes('queue="e2e"') && l.includes('storage="segments"'));
  const rowLine = text.split('\n').find((l) =>
    l.startsWith('queen_queue_depth_total_messages') && l.includes('queue="e2erows"') && l.includes('storage="rows"'));
  ok(!!segLine, '[13] queen_queue_depth_total_messages{queue="e2e",storage="segments"} present', segLine || 'missing');
  ok(!!rowLine, '[13] queen_queue_depth_total_messages{queue="e2erows",storage="rows"} present', rowLine || 'missing');
  ok(text.includes('queen_queue_depth_pending_messages'), '[13] pending_messages family present');
}

async function t15a_fusionOn() {
  console.log('[15a] cross-request fusion (HOLD_MS=15): 20 parallel single pushes');
  const rs = await Promise.all(Array.from({ length: 20 }, (_, i) =>
    push([{ queue: 'e2efuse', partition: 'hold15', transactionId: `fu-${i}`, payload: { i } }])));
  ok(rs.every((r) => r.status === 201 && r.json?.[0]?.status === 'queued'),
     '[15a] all 20 pushes queued');
  const st = await q2SegStats('e2efuse', 'hold15');
  ok(st.frames === 20 && st.segs >= 1 && st.segs <= NUM_WORKERS,
     `[15a] fused into <=${NUM_WORKERS} segments (one accumulator per uWS worker)`,
     JSON.stringify(st));
  console.log(`      (fusion produced ${st.segs} segment(s) for 20 requests)`);
}

async function t15b_fusionOff() {
  console.log('[15b] fusion bypass (HOLD_MS=0): 20 parallel single pushes');
  const rs = await Promise.all(Array.from({ length: 20 }, (_, i) =>
    push([{ queue: 'e2efuse', partition: 'hold0', transactionId: `fz-${i}`, payload: { i } }])));
  ok(rs.every((r) => r.status === 201 && r.json?.[0]?.status === 'queued'),
     '[15b] all 20 pushes queued');
  const st = await q2SegStats('e2efuse', 'hold0');
  ok(st.segs === 20 && st.frames === 20,
     '[15b] exactly 20 one-frame segments (no fusion)', JSON.stringify(st));
}

const ackAll = (msgs, group) => ackBatch(msgs.map((m) => ({
  transactionId: m.transactionId, partitionId: m.partitionId,
  leaseId: m.leaseId, status: 'completed',
})), group);

async function t16_subscription() {
  console.log('[16] subscription seeding: mode=new / from=now skip the backlog');
  // Backlog: two pushes -> two segments (seq 1, 2), 10 msgs.
  for (let b = 0; b < 2; b++) {
    await push(Array.from({ length: 5 }, (_, i) => ({
      queue: 'e2esub', partition: 'Default', transactionId: `sub-${b * 5 + i}`,
      payload: { i: b * 5 + i },
    })));
  }
  // First contact of fresh groups with a subscription window: seeded PAST the
  // backlog, nothing delivered.
  const pNew = await pop('e2esub', 'Default',
    { batch: 100, consumerGroup: 'g-new', subscriptionMode: 'new' });
  ok(pNew.status === 204, '[16] subscriptionMode=new first pop skips backlog (204)',
     `status ${pNew.status}`);
  const pNow = await pop('e2esub', 'Default',
    { batch: 100, consumerGroup: 'g-now', subscriptionFrom: 'now' });
  ok(pNow.status === 204, '[16] subscriptionFrom=now first pop skips backlog (204)',
     `status ${pNow.status}`);
  const pid = await q2PartitionId('e2esub', 'Default');
  const cNew = await q2Consumer(pid, 'g-new');
  const cNow = await q2Consumer(pid, 'g-now');
  ok(cNew?.next_seq === 3 && cNew?.next_off === 0 && cNow?.next_seq === 3,
     '[16] both cursors seeded at last_seq+1 = 3 (psql)',
     JSON.stringify({ cNew, cNow }));

  // Post-subscription messages ARE delivered, and only those.
  await push(Array.from({ length: 3 }, (_, i) => ({
    queue: 'e2esub', partition: 'Default', transactionId: `subnew-${i}`, payload: { i },
  })));
  const dNew = await pop('e2esub', 'Default',
    { batch: 100, consumerGroup: 'g-new', subscriptionMode: 'new' });
  const mNew = msgsOf(dNew);
  ok(mNew.length === 3 && mNew[0].transactionId === 'subnew-0' &&
     mNew.every((m) => m.transactionId.startsWith('subnew-')),
     '[16] g-new sees ONLY the 3 post-subscription msgs',
     `n=${mNew.length} first=${mNew[0]?.transactionId}`);
  await ackAll(mNew, 'g-new');
  const dNow = await pop('e2esub', 'Default',
    { batch: 100, consumerGroup: 'g-now', subscriptionFrom: 'now' });
  const mNow = msgsOf(dNow);
  ok(mNow.length === 3 && mNow[0].transactionId === 'subnew-0',
     '[16] g-now sees ONLY the 3 post-subscription msgs',
     `n=${mNow.length} first=${mNow[0]?.transactionId}`);
  await ackAll(mNow, 'g-now');

  // Wildcard pop forwards the same subscription args.
  const pWild = await pop('e2esub', null,
    { batch: 100, partitions: 10, consumerGroup: 'g-wild', subscriptionMode: 'new' });
  ok(pWild.status === 204, '[16] wildcard pop with mode=new seeds past backlog (204)',
     `status ${pWild.status}`);
  const cWild = await q2Consumer(pid, 'g-wild');
  ok(cWild?.next_seq === 4, '[16] wildcard-seeded cursor at last_seq+1 = 4 (psql)',
     JSON.stringify(cWild));

  // Control: a default-subscription fresh group still gets the whole backlog.
  const dAll = await pop('e2esub', 'Default', { batch: 100, consumerGroup: 'g-all' });
  const mAll = msgsOf(dAll);
  ok(mAll.length === 13 && mAll[0].transactionId === 'sub-0',
     '[16] control group with default subscription sees all 13',
     `n=${mAll.length} first=${mAll[0]?.transactionId}`);
  await ackAll(mAll, 'g-all');
}

async function t17_delayWindow() {
  console.log('[17] delayedProcessing=2 delays delivery; windowBuffer=2 withholds fresh partition');
  const c1 = await req('POST', '/api/v1/configure', {
    queue: 'e2edelay', options: { storage: 'segments', delayedProcessing: 2 } });
  const c2 = await req('POST', '/api/v1/configure', {
    queue: 'e2ewin', options: { storage: 'segments', windowBuffer: 2 } });
  ok((c1.status === 200 || c1.status === 201) && (c2.status === 200 || c2.status === 201),
     '[17] configured delayedProcessing=2 and windowBuffer=2 segment queues',
     `${c1.status}/${c2.status}`);

  const t0 = Date.now();
  await push([{ queue: 'e2edelay', partition: 'Default', transactionId: 'dly-0', payload: { d: 1 } }]);
  await push([{ queue: 'e2ewin', partition: 'Default', transactionId: 'win-0', payload: { w: 1 } }]);

  const d1 = await pop('e2edelay', 'Default', { batch: 5 });
  const w1 = await pop('e2ewin', 'Default', { batch: 5 });
  ok(d1.status === 204, '[17] delayed: immediate pop withheld (204)', `status ${d1.status}`);
  ok(w1.status === 204, '[17] window: immediate pop of fresh partition withheld (204)',
     `status ${w1.status}`);

  await sleep(Math.max(0, t0 + 900 - Date.now()));
  const mid = Date.now() - t0;
  if (mid < 1500) {  // guard: only meaningful while still inside the 2s window
    const d2 = await pop('e2edelay', 'Default', { batch: 5 });
    const w2 = await pop('e2ewin', 'Default', { batch: 5 });
    ok(d2.status === 204 && w2.status === 204,
       `[17] still withheld at +${mid}ms (204/204)`, `${d2.status}/${w2.status}`);
  } else {
    ok(true, '[17] mid-window check skipped (slow host)');
  }

  await sleep(Math.max(0, t0 + 2400 - Date.now()));
  const d3 = await pop('e2edelay', 'Default', { batch: 5 });
  const m3 = msgsOf(d3);
  ok(m3.length === 1 && m3[0].transactionId === 'dly-0',
     `[17] delayed message delivered after ~2s (at +${Date.now() - t0}ms)`,
     `status ${d3.status} n=${m3.length}`);
  const w3 = await pop('e2ewin', 'Default', { batch: 5 });
  const wm3 = msgsOf(w3);
  ok(wm3.length === 1 && wm3[0].transactionId === 'win-0',
     '[17] window opened: partition idle >2s delivers',
     `status ${w3.status} n=${wm3.length}`);
  await ackAll(m3);
  await ackAll(wm3);
}

async function t18_dlqEndpoint() {
  console.log('[18] GET /api/v1/dlq serves the q2 poison row with v1 keys');
  const r = await req('GET', '/api/v1/dlq?queue=e2edlq');
  const msgs = r.json?.messages;
  ok(r.status === 200 && Array.isArray(msgs) && msgs.length === 1,
     '[18] exactly the [6] poison row returned', `status ${r.status} ${r.text?.slice(0, 200)}`);
  const m = (msgs && msgs[0]) || {};
  const pid = await q2PartitionId('e2edlq', 'Default');
  ok(m.transactionId === 'dlq-0' && m.queue === 'e2edlq' && m.partition === 'Default' &&
     m.consumerGroup === '__QUEUE_MODE__' && m.partitionId === pid,
     '[18] identity keys (queue/partition/partitionId/consumerGroup) match q2',
     JSON.stringify(m).slice(0, 300));
  ok(typeof m.errorMessage === 'string' && m.errorMessage.includes('Retries exhausted') &&
     m.retryCount === 2 && m.data && m.data.poison === true && m.data.i === 0,
     '[18] errorMessage/retryCount/data carry the poison snapshot',
     JSON.stringify({ errorMessage: m.errorMessage, retryCount: m.retryCount, data: m.data }));
  ok('id' in m && 'producerSub' in m && m.producerSub === null &&
     typeof m.createdAt === 'string' && typeof m.failedAt === 'string',
     '[18] id/producerSub/createdAt/failedAt present (v1 shape)',
     JSON.stringify(Object.keys(m)));
  ok(r.json?.pagination?.limit === 100 && r.json?.pagination?.offset === 0,
     '[18] pagination envelope present', JSON.stringify(r.json?.pagination));
}

async function t19_mixedDupFresh() {
  console.log('[19] mixed dup+fresh push: dup -> duplicate w/ original mid, fresh commits');
  const r1 = await push([{ queue: 'e2edup', partition: 'mix', transactionId: 'mix-A', payload: { v: 1 } }]);
  const origMid = r1.json?.[0]?.message_id;
  ok(r1.status === 201 && r1.json?.[0]?.status === 'queued' && !!origMid,
     '[19] seeded mix-A (committed)', JSON.stringify(r1.json).slice(0, 200));

  // ONE request mixing the committed dup with a fresh item: unique_violation
  // rolls the segment back, the wrapper lists the dup, the fresh frame is
  // repacked and retried (attempt 1).
  const r2 = await push([
    { queue: 'e2edup', partition: 'mix', transactionId: 'mix-A', payload: { v: 2 } },
    { queue: 'e2edup', partition: 'mix', transactionId: 'mix-B', payload: { v: 3 } },
  ]);
  const rows = r2.json ?? [];
  const dupRow = rows.find((x) => x.transaction_id === 'mix-A');
  const freshRow = rows.find((x) => x.transaction_id === 'mix-B');
  ok(r2.status === 201 && dupRow?.status === 'duplicate' && dupRow?.message_id === origMid,
     '[19] dup row: duplicate with the ORIGINAL message_id', JSON.stringify(rows).slice(0, 300));
  ok(freshRow?.status === 'queued' && !!freshRow?.message_id &&
     freshRow.message_id !== origMid,
     '[19] fresh row queued with its own message_id', JSON.stringify(freshRow));

  const p = await pop('e2edup', 'mix', { batch: 10 });
  const ms = msgsOf(p);
  ok(ms.length === 2 && ms[0].transactionId === 'mix-A' && ms[0].id === origMid &&
     ms[1].transactionId === 'mix-B' && ms[1].id === freshRow?.message_id &&
     ms[1].data?.v === 3,
     '[19] delivered exactly mix-A (original) then mix-B (fresh)',
     `got ${ms.map((m) => m.transactionId)}`);
  await ackAll(ms);
}

async function t20_raw204() {
  console.log('[20] raw socket: empty pop 204 has NO body and NO Content-Length');
  const check = (r, name) => {
    ok(r.status === 204 && r.body.length === 0 &&
       !/content-length/i.test(r.headText) && !/transfer-encoding/i.test(r.headText),
       name, `status ${r.status} bodyLen=${r.body.length} :: ${r.headText.replace(/\r\n/g, ' | ')}`);
  };
  check(await reqRaw('GET', popUrl('e2e', 'pvoid', { batch: 5 })),
        '[20] segments specific empty pop: header-only 204');
  check(await reqRaw('GET', popUrl('e2ewild', null, { batch: 5, partitions: 4 })),
        '[20] segments wildcard empty pop: header-only 204');
  check(await reqRaw('GET', popUrl('e2erows', 'q0', { batch: 5 })),
        '[20] rows empty pop: header-only 204');
}

async function t21_ackShape() {
  console.log('[21] ack row shape + expired lease -> HTTP 200 per-item success:false');
  const c1 = await req('POST', '/api/v1/configure', {
    queue: 'e2elease1', options: { storage: 'segments', leaseTime: 300 } });
  const c2 = await req('POST', '/api/v1/configure', {
    queue: 'e2elease2', options: { storage: 'segments', leaseTime: 2 } });
  ok((c1.status === 200 || c1.status === 201) && (c2.status === 200 || c2.status === 201),
     '[21] configured leaseTime=300 and leaseTime=2 segment queues',
     `${c1.status}/${c2.status}`);

  await push(Array.from({ length: 3 }, (_, i) => ({
    queue: 'e2elease1', partition: 'Default', transactionId: `ls-${i}`, payload: { i },
  })));
  const p1 = await pop('e2elease1', 'Default', { batch: 3 });
  const m1 = msgsOf(p1);
  const a1 = await ackAll(m1);
  const rows = a1.json ?? [];
  ok(a1.status === 200 && rows.length === 3 &&
     rows.every((x, i) => x.index === i && x.transactionId === `ls-${i}` &&
                          x.success === true && x.error === null &&
                          x.queueName === 'e2elease1' && x.partitionName === 'Default' &&
                          x.dlq === false),
     '[21] rows carry index/transactionId/success/error/queueName/partitionName/dlq',
     JSON.stringify(rows).slice(0, 400));
  ok(rows[0]?.leaseReleased === false && rows[1]?.leaseReleased === false &&
     rows[2]?.leaseReleased === true,
     '[21] leaseReleased true exactly on the batch-closing row',
     JSON.stringify(rows.map((x) => x.leaseReleased)));

  await push(Array.from({ length: 2 }, (_, i) => ({
    queue: 'e2elease2', partition: 'Default', transactionId: `le-${i}`, payload: { i },
  })));
  const p2 = await pop('e2elease2', 'Default', { batch: 2 });
  const m2 = msgsOf(p2);
  ok(m2.length === 2, '[21] popped 2 under a 2s lease', `n=${m2.length}`);
  await sleep(3100);  // lease long dead, registry entry still alive (TTL+grace)
  const a2 = await ackAll(m2);
  const rows2 = a2.json ?? [];
  ok(a2.status === 200 && rows2.length === 2 &&
     rows2.every((x) => x.success === false && typeof x.error === 'string' &&
                        x.error.includes('expired') && x.leaseReleased === false &&
                        x.dlq === false),
     '[21] expired lease: HTTP 200 with per-item success:false',
     `status ${a2.status} ${JSON.stringify(rows2).slice(0, 300)}`);
  const p3 = await pop('e2elease2', 'Default', { batch: 5 });
  const m3 = msgsOf(p3);
  ok(m3.length === 2 && m3[0].transactionId === 'le-0',
     '[21] expired batch redelivered in full', `n=${m3.length} first=${m3[0]?.transactionId}`);
  await ackAll(m3);
}

async function t22_renewNoShorten() {
  console.log('[22] renew never shortens: 60s renew on a 300s lease keeps the expiry');
  await push([{ queue: 'e2e', partition: 'prenew2', transactionId: 'rns-0', payload: { i: 0 } }]);
  const p = await pop('e2e', 'prenew2', { batch: 1 });
  const ms = msgsOf(p);
  const pid = await q2PartitionId('e2e', 'prenew2');
  const before = await q2Consumer(pid);
  ok(ms.length === 1 && !!before?.lease_expires_at && before.worker_id === ms[0].leaseId,
     '[22] leased for the queue default 300s', JSON.stringify(before));
  const ext = await req('POST', `/api/v1/lease/${ms[0].leaseId}/extend`, { seconds: 60 });
  ok(ext.status === 200 && Array.isArray(ext.json) && ext.json[0]?.success === true,
     '[22] 60s renew answered success', `status ${ext.status} ${ext.text?.slice(0, 200)}`);
  const after = await q2Consumer(pid);
  const b = new Date(before.lease_expires_at).getTime();
  const a = new Date(after.lease_expires_at).getTime();
  ok(a >= b, '[22] expiry NOT shortened (>= original)',
     `${before.lease_expires_at} -> ${after.lease_expires_at}`);
  ok(a - b < 1000, '[22] shorter renew leaves the expiry effectively unchanged',
     `drift ${a - b}ms`);
  const ext2 = await req('POST', `/api/v1/lease/${ms[0].leaseId}/extend`, { seconds: 900 });
  const after2 = await q2Consumer(pid);
  ok(ext2.status === 200 && new Date(after2.lease_expires_at).getTime() > b + 400000,
     '[22] longer renew still extends (control)',
     `${before.lease_expires_at} -> ${after2.lease_expires_at}`);
  await ackAll(ms);
}

async function t23_retryParity() {
  console.log('[23] retryLimit=2 -> exactly 3 deliveries before DLQ on BOTH engines');
  const cs = await req('POST', '/api/v1/configure', {
    queue: 'e2edlq2',
    options: { storage: 'segments', retryLimit: 2, deadLetterQueue: true, leaseTime: 300 } });
  const cr = await req('POST', '/api/v1/configure', {
    queue: 'e2erowsdlq',
    options: { retryLimit: 2, deadLetterQueue: true, retryDelay: 0, leaseTime: 300 } });
  ok((cs.status === 200 || cs.status === 201) && (cr.status === 200 || cr.status === 201),
     '[23] configured segments + rows retryLimit=2 DLQ queues', `${cs.status}/${cr.status}`);

  // Same protocol on both engines: batch=1 pops, nack the head until it is
  // dead-lettered, count its deliveries; empty pops (rows DLQ-eviction pop /
  // retryDelay grace) retry after a short sleep.
  const run = async (queue, prefix, isRows) => {
    await push(Array.from({ length: 3 }, (_, i) => ({
      queue, partition: 'Default', transactionId: `${prefix}-${i}`,
      payload: { i, poison: i === 0 },
    })));
    let headDeliveries = 0;
    let firstAfter = null;
    for (let round = 0; round < 10 && headDeliveries <= 5; round++) {
      const p = await pop(queue, 'Default', { batch: 1 });
      const ms = msgsOf(p);
      if (ms.length === 0) { await sleep(300); continue; }
      const m = ms[0];
      if (m.transactionId === `${prefix}-0`) {
        headDeliveries++;
        await req('POST', '/api/v1/ack', {
          transactionId: m.transactionId, partitionId: m.partitionId,
          leaseId: m.leaseId, status: 'failed',
        });
        if (isRows) await sleep(350);
      } else {
        firstAfter = m;
        break;
      }
    }
    // Drain: complete the popped survivor and everything behind it.
    if (firstAfter) await ackAll([firstAfter]);
    for (let i = 0; i < 4; i++) {
      const p = await pop(queue, 'Default', { batch: 10 });
      const ms = msgsOf(p);
      if (ms.length === 0) break;
      await ackAll(ms);
    }
    return { headDeliveries, next: firstAfter?.transactionId };
  };

  const seg = await run('e2edlq2', 'pp', false);
  ok(seg.headDeliveries === 3 && seg.next === 'pp-1',
     '[23] segments: head delivered exactly 3 times, then pp-1', JSON.stringify(seg));
  const segDlq = (await sql(
    `SELECT d.transaction_id FROM q2.dlq d
     JOIN q2.partitions p ON p.id = d.partition_id
     JOIN q2.queues q ON q.id = p.queue_id WHERE q.name = 'e2edlq2'`)).rows;
  ok(segDlq.length === 1 && segDlq[0].transaction_id === 'pp-0',
     '[23] segments poison landed in q2.dlq (psql)', JSON.stringify(segDlq));

  const rows = await run('e2erowsdlq', 'pr', true);
  ok(rows.headDeliveries === 3 && rows.next === 'pr-1',
     '[23] rows: head delivered exactly 3 times, then pr-1', JSON.stringify(rows));
  const rowsDlq = (await sql(
    `SELECT m.transaction_id FROM queen.dead_letter_queue dlq
     JOIN queen.messages m ON m.id = dlq.message_id
     JOIN queen.partitions p ON p.id = dlq.partition_id
     JOIN queen.queues q ON q.id = p.queue_id WHERE q.name = 'e2erowsdlq'`)).rows;
  ok(rowsDlq.length === 1 && rowsDlq[0].transaction_id === 'pr-0',
     '[23] rows poison landed in queen.dead_letter_queue (psql)', JSON.stringify(rowsDlq));
  ok(seg.headDeliveries === rows.headDeliveries,
     '[23] delivery-count parity: segments == rows',
     `${seg.headDeliveries} vs ${rows.headDeliveries}`);
}

async function t25_explicitDlq() {
  console.log("[25] explicit ack status='dlq' dead-letters immediately");
  // Single-message case: pop 1, ack it status=dlq with a consumer error.
  await push([{ queue: 'e2edlq', partition: 'expl2', transactionId: 'y-0', payload: { bad: true } }]);
  const p1 = await pop('e2edlq', 'expl2', { batch: 1 });
  const m1 = msgsOf(p1)[0];
  const a1 = await req('POST', '/api/v1/ack', {
    transactionId: m1.transactionId, partitionId: m1.partitionId,
    leaseId: m1.leaseId, status: 'dlq', error: 'poison detected by consumer',
  });
  ok(a1.status === 200 && a1.json?.[0]?.success === true && a1.json?.[0]?.dlq === true &&
     a1.json?.[0]?.leaseReleased === true,
     '[25] single dlq ack: success + dlq:true + lease released',
     `status ${a1.status} ${a1.text?.slice(0, 200)}`);
  const row1 = (await sql(
    `SELECT d.transaction_id, d.error, d.message_id::text AS mid, d.consumer_group
     FROM q2.dlq d JOIN q2.partitions p ON p.id = d.partition_id
     JOIN q2.queues q ON q.id = p.queue_id
     WHERE q.name = 'e2edlq' AND p.name = 'expl2'`)).rows;
  ok(row1.length === 1 && row1[0].transaction_id === 'y-0' &&
     row1[0].error === 'poison detected by consumer' && row1[0].mid === m1.id &&
     row1[0].consumer_group === '__QUEUE_MODE__',
     '[25] q2.dlq row filed with the consumer error + message id (psql)',
     JSON.stringify(row1));
  const rp1 = await pop('e2edlq', 'expl2', { batch: 5 });
  ok(rp1.status === 204, '[25] dead-lettered message NOT redelivered', `status ${rp1.status}`);

  // Batch case: dlq as the FINAL disposed message -> cursor lands after it.
  await push(Array.from({ length: 3 }, (_, i) => ({
    queue: 'e2edlq', partition: 'expl', transactionId: `x-${i}`, payload: { i },
  })));
  const p2 = await pop('e2edlq', 'expl', { batch: 3 });
  const m2 = msgsOf(p2);
  const a2 = await ackBatch(m2.map((m, i) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: m.leaseId, status: i === 2 ? 'dlq' : 'completed',
  })));
  const rows2 = a2.json ?? [];
  ok(a2.status === 200 && rows2.length === 3 &&
     rows2.every((x) => x.success === true) &&
     rows2[0]?.dlq === false && rows2[1]?.dlq === false && rows2[2]?.dlq === true,
     '[25] batch ack: all success, dlq:true only on the dlq row',
     JSON.stringify(rows2).slice(0, 300));
  const row2 = (await sql(
    `SELECT d.transaction_id, d.error FROM q2.dlq d
     JOIN q2.partitions p ON p.id = d.partition_id
     JOIN q2.queues q ON q.id = p.queue_id
     WHERE q.name = 'e2edlq' AND p.name = 'expl'`)).rows;
  ok(row2.length === 1 && row2[0].transaction_id === 'x-2' &&
     row2[0].error === 'Dead-lettered by consumer ack',
     '[25] q2.dlq row filed with the default reason (psql)', JSON.stringify(row2));
  const c = await q2Consumer(await q2PartitionId('e2edlq', 'expl'));
  ok(c && c.worker_id === null && c.next_seq === 2 && c.next_off === 0,
     '[25] cursor advanced past the dlq frame, lease released (psql)', JSON.stringify(c));
  const rp2 = await pop('e2edlq', 'expl', { batch: 5 });
  ok(rp2.status === 204, '[25] nothing redelivered after the batch dlq', `status ${rp2.status}`);
}

// [24] spans the restart like [9]: part A pops under run-1, part B commits a
// TRANSACTION whose acks resolve via the SQL txns-form fallback under run-2.
let t24state = null;
async function t24a_popBeforeRestart() {
  console.log('[24a] pop a batch for the post-restart transaction ack');
  await push(Array.from({ length: 5 }, (_, i) => ({
    queue: 'e2etxn', partition: 'pfall2', transactionId: `tf-${i}`, payload: { i },
  })));
  const p = await pop('e2etxn', 'pfall2', { batch: 5 });
  const ms = msgsOf(p);
  ok(ms.length === 5, '[24a] popped 5 under lease', `n=${ms.length}`);
  t24state = { msgs: ms, pid: ms[0].partitionId, lease: ms[0].leaseId };
}
async function t24b_transactionAckAfterRestart() {
  console.log('[24b] transaction acks AFTER restart -> SQL txns-form fallback');
  const operations = t24state.msgs.map((m) => ({
    type: 'ack', transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: t24state.lease, status: 'completed',
  }));
  operations.push({ type: 'push', items: [{
    queue: 'e2etxn', partition: 'out2', transactionId: 'txn-out-2',
    payload: { fromTxn2: true } }] });
  const tr = await req('POST', '/api/v1/transaction', { operations });
  ok(tr.status === 200 && tr.json?.success === true &&
     Array.isArray(tr.json?.results) && tr.json.results.length === 6 &&
     tr.json.results.every((x) => x?.success === true),
     '[24b] transaction committed via SQL ack fallback',
     `status ${tr.status} ${tr.text?.slice(0, 300)}`);
  const c = await q2Consumer(t24state.pid);
  ok(c && c.worker_id === null && c.next_seq === 2 && c.next_off === 0 &&
     c.batch_end_seq === null && c.total_consumed === 5,
     '[24b] cursor advanced + lease released atomically (psql)', JSON.stringify(c));
  const p = await pop('e2etxn', 'pfall2', { batch: 10 });
  ok(p.status === 204, '[24b] nothing redelivered', `status ${p.status}`);
  const outPop = await pop('e2etxn', 'out2', { batch: 5 });
  const outMs = msgsOf(outPop);
  ok(outMs.length === 1 && outMs[0].transactionId === 'txn-out-2' &&
     outMs[0].data?.fromTxn2 === true,
     '[24b] transaction-pushed message poppable', `n=${outMs.length}`);
  await ackAll(outMs);
}

// [9] spans the restart: part A pops under run-1, part B acks under run-2.
let t9state = null;
async function t9a_popBeforeRestart() {
  console.log('[9a] pop a batch (lease will survive only in SQL)');
  await push(Array.from({ length: 6 }, (_, i) => ({
    queue: 'e2e', partition: 'pfall', transactionId: `f-${i}`, payload: { i },
  })));
  const p = await pop('e2e', 'pfall', { batch: 6 });
  const ms = msgsOf(p);
  ok(ms.length === 6, '[9a] popped 6 under lease', `n=${ms.length}`);
  t9state = { msgs: ms, pid: ms[0].partitionId, lease: ms[0].leaseId };
  const c = await q2Consumer(t9state.pid);
  ok(c.worker_id === t9state.lease && c.batch_end_seq !== null,
     '[9a] lease persisted in q2.consumers (psql)', JSON.stringify(c));
}
async function t9b_ackAfterRestart() {
  console.log('[9b] ack by txn AFTER restart (registry wiped) -> q2.ack_by_txn_v1');
  const a = await ackBatch(t9state.msgs.map((m) => ({
    transactionId: m.transactionId, partitionId: m.partitionId,
    leaseId: t9state.lease, status: 'completed',
  })));
  ok(a.status === 200 && Array.isArray(a.json) && a.json.length === 6 &&
     a.json.every((x) => x.success === true && x.queueName === 'e2e' &&
                         x.partitionName === 'pfall' && x.leaseReleased === true),
     '[9b] fallback ack answered v1 success rows', `status ${a.status} ${a.text?.slice(0, 200)}`);
  const c = await q2Consumer(t9state.pid);
  ok(c && c.worker_id === null && c.next_seq === 2 && c.next_off === 0 &&
     c.total_consumed === 6,
     '[9b] cursor advanced + lease released via SQL resolver (psql)', JSON.stringify(c));
  const p = await pop('e2e', 'pfall', { batch: 10 });
  ok(p.status === 204, '[9b] nothing redelivered', `status ${p.status}`);
}

// ==========================================================================
async function main() {
  console.log(`== fresh database ${DB} + schema (${SCHEMA_DIR}) ==`);
  await freshDatabase();
  const n = await applySchema();
  console.log(`applied schema.sql + ${n} procedure files`);
  pool = new Pool({ ...PG, database: DB, max: 3 });
  await seedQueues();

  console.log('== server run 1 (QUEEN_V2_FUSION_HOLD_MS=15) ==');
  await startServer({ QUEEN_V2_FUSION_HOLD_MS: '15' }, 'run1');

  await t1_roundTrip();
  await t2_wildcard();
  await t3_waitPop();
  await t4_duplicates();
  await t5_nackPartial();
  await t6_dlq();
  await t7_renew();
  await t8_transaction();
  await t10_retention();
  await t11_eviction();
  await t12_encryption();
  await t14_v1Regression();
  await t13_prometheus();
  await t15a_fusionOn();
  await t16_subscription();
  await t17_delayWindow();
  await t18_dlqEndpoint();   // MUST run before [25] adds more e2edlq rows
  await t19_mixedDupFresh();
  await t20_raw204();
  await t21_ackShape();
  await t22_renewNoShorten();
  await t23_retryParity();
  await t25_explicitDlq();
  await t9a_popBeforeRestart();
  await t24a_popBeforeRestart();

  console.log('== restart: server run 2 (QUEEN_V2_FUSION_HOLD_MS=0) ==');
  await stopServer();
  await startServer({ QUEEN_V2_FUSION_HOLD_MS: '0' }, 'run2');

  await t9b_ackAfterRestart();
  await t24b_transactionAckAfterRestart();
  await t15b_fusionOff();

  await stopServer();
  await pool.end();
  console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
  process.exit(failures === 0 ? 0 : 1);
}

process.on('SIGINT', async () => { await stopServer(); process.exit(130); });
main().catch(async (e) => {
  console.error('FATAL:', e);
  if (serverLog) console.error(`server log: ${serverLog}`);
  await stopServer();
  process.exit(1);
});
