#!/usr/bin/env node
// Head-to-head storage bench: v1 (queen.messages, per-message rows) vs
// v2 (q2.segments, K frames per row, zstd group compression).
//
// The driver does the broker's job for v2 (frame packing + zstd) and calls
// the exact v1 procedures libqueen dispatches (see lib/queen/pending_job.hpp):
//   PUSH: SELECT queen.push_messages_v3($1::jsonb)
//   POP:  SELECT queen.pop_unified_batch_v4($1::jsonb)
//   ACK:  SELECT queen.ack_messages_v2($1::jsonb)
//
// Usage: node bench-driver.mjs <phase>
//   phases: ingest-v1 | consume-v1 | retention-v1 | sizes-v1
//           ingest-v2 | consume-v2 | retention-v2 | sizes-v2
// Env: PGURL (required), MSGS, PARTITIONS, BATCH (v1 items/call = v2 K),
//      POP_BATCH, CONCURRENCY, QUEUE, SEED
// Output: one JSON object on stdout (phase metrics).

import { createRequire } from 'node:module';
import zlib from 'node:zlib';
import crypto from 'node:crypto';

const require = createRequire(import.meta.url);
const { Pool } = require('../../proxy/node_modules/pg');

const PGURL = process.env.PGURL;
if (!PGURL) { console.error('PGURL required'); process.exit(2); }

const MSGS = parseInt(process.env.MSGS || '600000', 10);
const PARTITIONS = parseInt(process.env.PARTITIONS || '8', 10);
const BATCH = parseInt(process.env.BATCH || '50', 10);           // v1 items/call == v2 frames/segment (K)
const POP_BATCH = parseInt(process.env.POP_BATCH || '100', 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '4', 10);
const QUEUE = process.env.QUEUE || 'bench';
const SEED = parseInt(process.env.SEED || '42', 10);
const phase = process.argv[2];

const pool = new Pool({ connectionString: PGURL, max: CONCURRENCY + 2 });

// ---------------------------------------------------------------- payloads
// Deterministic LCG so both engines see byte-identical message streams.
function lcg(seed) {
  let s = seed >>> 0;
  return () => (s = (s * 1664525 + 1013904223) >>> 0) / 2 ** 32;
}
// Deterministic UUIDv4-shaped ids from the LCG: v1 and v2 must see
// byte-identical streams (ids live inside v1 rows AND v2 frames — unseeded
// ids would confound the size/compression comparison).
function uuidFrom(rnd) {
  const b = Buffer.alloc(16);
  for (let i = 0; i < 16; i++) b[i] = Math.floor(rnd() * 256);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
const STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'returned'];
function makePayload(rnd, i) {
  // Realistic-ish order event, ~250-280 bytes of JSON, same keys every
  // message (typical MQ traffic shape), varying values.
  return JSON.stringify({
    orderId: 1000000 + i,
    customerId: uuidFrom(rnd),
    status: STATUSES[Math.floor(rnd() * STATUSES.length)],
    items: [
      { sku: `SKU-${Math.floor(rnd() * 90000 + 10000)}`, qty: 1 + Math.floor(rnd() * 5), price: +(rnd() * 200).toFixed(2) },
      { sku: `SKU-${Math.floor(rnd() * 90000 + 10000)}`, qty: 1 + Math.floor(rnd() * 3), price: +(rnd() * 80).toFixed(2) },
    ],
    channel: rnd() < 0.7 ? 'web' : 'mobile',
    ts: new Date(1750000000000 + i * 137).toISOString(),
    note: 'auto-generated benchmark event payload',
  });
}
// Message stream: (partition, messageId, transactionId, payload)
function* messageStream() {
  const rnd = lcg(SEED);
  for (let i = 0; i < MSGS; i++) {
    yield {
      i,
      partition: `p${i % PARTITIONS}`,
      mid: uuidFrom(rnd),
      txn: uuidFrom(rnd),
      payload: makePayload(rnd, i),
    };
  }
}

// ---------------------------------------------------------------- frames
// [u32 len][u8 flags][16B message_id][u16 txnLen][txn][payload]
function packFrames(msgs) {
  const parts = [];
  for (const m of msgs) {
    const mid = Buffer.from(m.mid.replace(/-/g, ''), 'hex');
    const txn = Buffer.from(m.txn, 'utf8');
    const pl = Buffer.from(m.payload, 'utf8');
    const body = Buffer.alloc(1 + 16 + 2);
    body.writeUInt8(0, 0);
    mid.copy(body, 1);
    body.writeUInt16LE(txn.length, 17);
    const frame = Buffer.concat([Buffer.alloc(4), body, txn, pl]);
    frame.writeUInt32LE(frame.length - 4, 0);
    parts.push(frame);
  }
  return Buffer.concat(parts);
}
function unpackFrames(buf) {
  const out = [];
  let o = 0;
  while (o < buf.length) {
    const len = buf.readUInt32LE(o);
    const flags = buf.readUInt8(o + 4);
    const midHex = buf.subarray(o + 5, o + 21).toString('hex');
    const txnLen = buf.readUInt16LE(o + 21);
    const txn = buf.subarray(o + 23, o + 23 + txnLen).toString('utf8');
    const payload = buf.subarray(o + 23 + txnLen, o + 4 + len).toString('utf8');
    out.push({ flags, midHex, txn, payload });
    o += 4 + len;
  }
  return out;
}
const zstdC = (buf) => zlib.zstdCompressSync(buf);
const zstdD = (buf) => zlib.zstdDecompressSync(buf);

// ---------------------------------------------------------------- metrics
async function walLsn(c) {
  const r = await c.query('SELECT pg_current_wal_lsn() AS l');
  return r.rows[0].l;
}
async function walDiff(c, a, b) {
  const r = await c.query('SELECT pg_wal_lsn_diff($1, $2) AS d', [b, a]);
  return Number(r.rows[0].d);
}
async function withMetrics(fn) {
  const c = await pool.connect();
  await c.query('CHECKPOINT');
  const lsn0 = await walLsn(c);
  c.release();
  const cpu0 = process.cpuUsage();
  const t0 = Date.now();
  const res = await fn();
  const t1 = Date.now();
  const cpu1 = process.cpuUsage(cpu0);
  const c2 = await pool.connect();
  const lsn1 = await walLsn(c2);
  const wal = await walDiff(c2, lsn0, lsn1);
  c2.release();
  return {
    ...res, seconds: (t1 - t0) / 1000, wal_bytes: wal,
    driver_cpu_seconds: +((cpu1.user + cpu1.system) / 1e6).toFixed(1),
  };
}

// ---------------------------------------------------------------- v1 flows
async function ingestV1() {
  // Group the stream per partition into batches of BATCH (mirrors the fusion
  // engine's per-partition batch assembly), CONCURRENCY workers.
  const buckets = new Map();
  const batches = [];
  for (const m of messageStream()) {
    let b = buckets.get(m.partition);
    if (!b) { b = []; buckets.set(m.partition, b); }
    b.push(m);
    if (b.length >= BATCH) { batches.push(b); buckets.set(m.partition, []); }
  }
  for (const b of buckets.values()) if (b.length) batches.push(b);

  let sent = 0, dup = 0, cursor = 0;
  async function worker() {
    const c = await pool.connect();
    try {
      while (true) {
        const idx = cursor++;
        if (idx >= batches.length) break;
        const b = batches[idx];
        const items = b.map((m) => ({
          queue: QUEUE, partition: m.partition,
          messageId: m.mid, transactionId: m.txn,
          payload: JSON.parse(m.payload),
        }));
        const r = await c.query('SELECT queen.push_messages_v3($1::jsonb) AS r', [JSON.stringify(items)]);
        const res = r.rows[0].r;
        // v3 response shape: { items: [...], partition_updates: [...] }
        const arr = res.items || res.results || (Array.isArray(res) ? res : []);
        for (const it of arr) if (it.status === 'duplicate') dup++;
        sent += b.length;
      }
    } finally { c.release(); }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  return { msgs: sent, duplicates: dup, calls: batches.length };
}

async function consumeV1() {
  let consumed = 0;
  const parts = Array.from({ length: PARTITIONS }, (_, i) => `p${i}`);
  let next = 0;
  async function worker(w) {
    const c = await pool.connect();
    const workerId = `bench-w${w}-${process.pid}`;
    const empty = new Set();
    try {
      while (empty.size < parts.length) {
        const p = parts[next++ % parts.length];
        if (empty.has(p)) continue;
        const req = [{
          idx: 0, queue_name: QUEUE, partition_name: p,
          consumer_group: '__QUEUE_MODE__', batch_size: POP_BATCH,
          lease_seconds: 300, worker_id: workerId, auto_ack: false, max_partitions: 1,
        }];
        const r = await c.query('SELECT queen.pop_unified_batch_v4($1::jsonb) AS r', [JSON.stringify(req)]);
        const results = r.rows[0].r;
        // v4 response shape: [ {idx, result: {success, messages, ...}} ]
        const el = Array.isArray(results) ? results[0] : results;
        const res = (el && el.result) || el;
        const msgs = (res && res.messages) || [];
        if (!msgs.length) {
          // lease_held = another worker is on it, not exhausted
          if (!res || res.error !== 'lease_held') empty.add(p);
          continue;
        }
        const acks = msgs.map((m) => ({
          transactionId: m.transactionId, partitionId: m.partitionId,
          consumerGroup: m.consumerGroup || '__QUEUE_MODE__',
          leaseId: m.leaseId, status: 'completed',
        }));
        await c.query('SELECT queen.ack_messages_v2($1::jsonb) AS r', [JSON.stringify(acks)]);
        consumed += msgs.length;
      }
    } finally { c.release(); }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, (_, w) => worker(w)));
  return { msgs: consumed };
}

async function retentionV1() {
  // Batched age-based delete via idx_messages_partition_created, the same
  // victim-selection shape retention_service.cpp uses.
  const c = await pool.connect();
  let deleted = 0;
  try {
    const parts = await c.query(`SELECT id FROM queen.partitions`);
    for (const row of parts.rows) {
      while (true) {
        const r = await c.query(
          `DELETE FROM queen.messages WHERE id IN (
             SELECT id FROM queen.messages
             WHERE partition_id = $1 AND created_at < now()
             ORDER BY created_at, id LIMIT 10000)`,
          [row.id]);
        deleted += r.rowCount;
        if (r.rowCount === 0) break;
      }
    }
  } finally { c.release(); }
  return { deleted };
}

// ---------------------------------------------------------------- v2 flows
async function ingestV2() {
  const buckets = new Map();
  const batches = [];
  for (const m of messageStream()) {
    let b = buckets.get(m.partition);
    if (!b) { b = []; buckets.set(m.partition, b); }
    b.push(m);
    if (b.length >= BATCH) { batches.push(b); buckets.set(m.partition, []); }
  }
  for (const b of buckets.values()) if (b.length) batches.push(b);

  let sent = 0, dup = 0, cursor = 0, blobBytes = 0, rawBytes = 0;
  if (process.env.DEDUP === '0') {
    const c = await pool.connect();
    await c.query(`INSERT INTO q2.queues (name, dedup_window_seconds) VALUES ($1, 0)
                   ON CONFLICT (name) DO UPDATE SET dedup_window_seconds = 0`, [QUEUE]);
    c.release();
  }
  async function pushSegment(c, part, frames) {
    const raw = packFrames(frames);
    const blob = zstdC(raw);
    blobBytes += blob.length; rawBytes += raw.length;
    const metas = frames.map((m, i) => ({ i, mid: m.mid, txn: m.txn }));
    try {
      await c.query('SELECT q2.push_segment_v1($1, $2, $3::jsonb, $4, $5) AS r',
        [QUEUE, part, JSON.stringify(metas), blob, frames.length]);
      sent += frames.length;
    } catch (e) {
      if (!String(e.message).includes('QDUP')) throw e;
      // Rare path: resolve dup indices, repack survivors, retry once.
      const d = await c.query('SELECT q2.find_dups_v1($1,$2,$3::jsonb) AS r',
        [QUEUE, part, JSON.stringify(metas)]);
      const dups = new Set(d.rows[0].r);
      dup += dups.size;
      const rest = frames.filter((_, i) => !dups.has(i));
      if (rest.length) await pushSegment(c, part, rest);
    }
  }
  async function worker() {
    const c = await pool.connect();
    try {
      while (true) {
        const idx = cursor++;
        if (idx >= batches.length) break;
        await pushSegment(c, batches[idx][0].partition, batches[idx]);
      }
    } finally { c.release(); }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  return { msgs: sent, duplicates: dup, calls: batches.length,
           zstd_ratio: +(rawBytes / blobBytes).toFixed(2) };
}

async function consumeV2() {
  let consumed = 0;
  const parts = Array.from({ length: PARTITIONS }, (_, i) => `p${i}`);
  let next = 0;
  async function worker(w) {
    const c = await pool.connect();
    const workerId = `bench-w${w}-${process.pid}`;
    const empty = new Set();
    try {
      while (empty.size < parts.length) {
        const p = parts[next++ % parts.length];
        if (empty.has(p)) continue;
        const r = await c.query(
          'SELECT * FROM q2.pop_segments_v1($1,$2,$3,$4,$5,$6,false)',
          [QUEUE, p, '__QUEUE_MODE__', POP_BATCH, 300, workerId]);
        if (!r.rows.length) { empty.add(p); continue; }
        let batchMsgs = 0;
        let last = null;
        for (const row of r.rows) {
          const frames = unpackFrames(zstdD(row.r_blob));
          if (frames.length !== row.r_msg_count) throw new Error('frame count mismatch');
          const take = frames.slice(row.r_start_off, row.r_start_off + row.r_take);
          batchMsgs += take.length;
          last = row;
        }
        await c.query(
          'SELECT q2.ack_segments_v1($1,$2,$3,$4,$5,$6,true,$7) AS r',
          [QUEUE, p, '__QUEUE_MODE__', workerId,
           last.r_seq, last.r_start_off + last.r_take, batchMsgs]);
        consumed += batchMsgs;
      }
    } finally { c.release(); }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, (_, w) => worker(w)));
  return { msgs: consumed };
}

async function retentionV2() {
  const c = await pool.connect();
  try {
    const r = await c.query('SELECT q2.retention_v1(now()) AS n');
    const d = await c.query('SELECT q2.purge_dedup_v1(now(), 100000) AS n');
    return { deleted_segments: Number(r.rows[0].n), purged_dedup: Number(d.rows[0].n) };
  } finally { c.release(); }
}

// ---------------------------------------------------------------- sizes
async function sizes(tables) {
  const c = await pool.connect();
  const out = {};
  try {
    for (const t of tables) {
      const r = await c.query(
        `SELECT pg_table_size($1::regclass) AS heap,
                pg_indexes_size($1::regclass) AS idx,
                pg_total_relation_size($1::regclass) AS total`, [t]);
      const cnt = await c.query(`SELECT count(*) AS n FROM ${t}`);
      out[t] = {
        heap: Number(r.rows[0].heap), idx: Number(r.rows[0].idx),
        total: Number(r.rows[0].total), rows: Number(cnt.rows[0].n),
      };
      const ix = await c.query(
        `SELECT indexrelid::regclass::text AS name, pg_relation_size(indexrelid) AS bytes
         FROM pg_index WHERE indrelid = $1::regclass`, [t]);
      out[t].indexes = Object.fromEntries(ix.rows.map((x) => [x.name, Number(x.bytes)]));
    }
  } finally { c.release(); }
  return out;
}

// ---------------------------------------------------------------- main
const phases = {
  'ingest-v1': () => withMetrics(ingestV1),
  'consume-v1': () => withMetrics(consumeV1),
  'retention-v1': () => withMetrics(retentionV1),
  'ingest-v2': () => withMetrics(ingestV2),
  'consume-v2': () => withMetrics(consumeV2),
  'retention-v2': () => withMetrics(retentionV2),
  'sizes-v1': () => sizes(['queen.messages', 'queen.partition_consumers', 'queen.messages_consumed']),
  'sizes-v2': () => sizes(['q2.segments', 'q2.dedup', 'q2.consumers']),
};
if (!phases[phase]) { console.error(`unknown phase ${phase}`); process.exit(2); }

phases[phase]()
  .then((r) => { console.log(JSON.stringify({ phase, ...r })); return pool.end(); })
  .catch((e) => { console.error(e); process.exit(1); });
