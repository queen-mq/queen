#!/usr/bin/env node
// Functional correctness of the v2 segments engine. Small volumes, hostile
// sequences. Exits non-zero on any failure.
//
//  T1  round-trip: push N, pop all, exact message_id set + per-partition order
//  T2  dedup: re-push same txns -> duplicate, nothing double-delivered
//  T3  partial ack mid-segment -> redelivery resumes at exact frame
//  T4  nack -> full batch redelivered
//  T5  lease: second worker can't pop a leased partition; expiry frees it
//  T6  ack with wrong/expired lease rejected
//  T7  retention deletes consumed segments; cursor-on-deleted-segment guard
//      (offset must NOT apply to a later segment)
//  T8  two consumer groups get independent full streams
import { createRequire } from 'node:module';
import zlib from 'node:zlib';
import crypto from 'node:crypto';
const require = createRequire(import.meta.url);
const { Pool } = require('../../proxy/node_modules/pg');

const PGURL = process.env.PGURL;
const pool = new Pool({ connectionString: PGURL, max: 4 });
let failures = 0;
function ok(cond, name, detail = '') {
  if (cond) console.log(`  PASS ${name}`);
  else { failures++; console.log(`  FAIL ${name} ${detail}`); }
}

function packFrames(msgs) {
  const parts = [];
  for (const m of msgs) {
    const mid = Buffer.from(m.mid.replace(/-/g, ''), 'hex');
    const txn = Buffer.from(m.txn, 'utf8');
    const pl = Buffer.from(m.payload, 'utf8');
    const body = Buffer.alloc(1 + 16 + 2);
    body.writeUInt8(0, 0); mid.copy(body, 1); body.writeUInt16LE(txn.length, 17);
    const frame = Buffer.concat([Buffer.alloc(4), body, txn, pl]);
    frame.writeUInt32LE(frame.length - 4, 0);
    parts.push(frame);
  }
  return Buffer.concat(parts);
}
function unpackFrames(buf) {
  const out = []; let o = 0;
  while (o < buf.length) {
    const len = buf.readUInt32LE(o);
    const midHex = buf.subarray(o + 5, o + 21).toString('hex');
    const txnLen = buf.readUInt16LE(o + 21);
    out.push({
      mid: midHex, txn: buf.subarray(o + 23, o + 23 + txnLen).toString('utf8'),
      payload: buf.subarray(o + 23 + txnLen, o + 4 + len).toString('utf8'),
    });
    o += 4 + len;
  }
  return out;
}

function mkMsgs(n, tag) {
  return Array.from({ length: n }, (_, i) => ({
    mid: crypto.randomUUID(), txn: `${tag}-${i}`,
    payload: JSON.stringify({ tag, i, filler: 'x'.repeat(180) }),
  }));
}
async function push(c, queue, part, msgs) {
  const metas = msgs.map((m, i) => ({ i, mid: m.mid, txn: m.txn }));
  const blob = zlib.zstdCompressSync(packFrames(msgs));
  try {
    const r = await c.query('SELECT q2.push_segment_v1($1,$2,$3::jsonb,$4,$5) AS r',
      [queue, part, JSON.stringify(metas), blob, msgs.length]);
    return r.rows[0].r;
  } catch (e) {
    if (!String(e.message).includes('QDUP')) throw e;
    const d = await c.query('SELECT q2.find_dups_v1($1,$2,$3::jsonb) AS r',
      [queue, part, JSON.stringify(metas)]);
    return { status: 'duplicate', dups: d.rows[0].r };
  }
}
async function pop(c, queue, part, group, budget, worker, { lease = 300, autoAck = false } = {}) {
  const r = await c.query('SELECT * FROM q2.pop_segments_v1($1,$2,$3,$4,$5,$6,$7)',
    [queue, part, group, budget, lease, worker, autoAck]);
  const msgs = [];
  let last = null;
  for (const row of r.rows) {
    const frames = unpackFrames(zlib.zstdDecompressSync(row.r_blob));
    msgs.push(...frames.slice(row.r_start_off, row.r_start_off + row.r_take));
    last = { seq: row.r_seq, off: row.r_start_off + row.r_take };
  }
  return { msgs, last };
}
async function ack(c, queue, part, group, worker, pos, okFlag = true) {
  const r = await c.query('SELECT q2.ack_segments_v1($1,$2,$3,$4,$5,$6,$7) AS r',
    [queue, part, group, worker, pos ? pos.seq : null, pos ? pos.off : null, okFlag]);
  return r.rows[0].r;
}

const Q = 'ct';
async function main() {
  const c = await pool.connect();

  // T1 round-trip, order, exact set
  console.log('T1 round-trip');
  const m1 = mkMsgs(120, 't1');
  for (let i = 0; i < 120; i += 40) await push(c, Q, 'a', m1.slice(i, i + 40));
  let got = [];
  while (true) {
    const { msgs, last } = await pop(c, Q, 'a', 'g', 50, 'w1');
    if (!msgs.length) break;
    got.push(...msgs);
    await ack(c, Q, 'a', 'g', 'w1', last);
  }
  ok(got.length === 120, 'T1 count', `got ${got.length}`);
  ok(got.every((m, i) => m.txn === `t1-${i}`), 'T1 order');
  ok(new Set(got.map((m) => m.mid)).size === 120, 'T1 unique mids');

  // T2 dedup
  console.log('T2 dedup');
  const r2 = await push(c, Q, 'a', m1.slice(0, 40));
  ok(r2.status === 'duplicate' && r2.dups.length === 40, 'T2 full-dup rejected', JSON.stringify(r2));
  const { msgs: after } = await pop(c, Q, 'a', 'g', 500, 'w1');
  ok(after.length === 0, 'T2 nothing redelivered', `got ${after.length}`);

  // T3 partial ack mid-segment
  console.log('T3 partial ack');
  const m3 = mkMsgs(30, 't3');
  await push(c, Q, 'b', m3);          // one segment of 30
  const p3 = await pop(c, Q, 'b', 'g', 30, 'w1');
  ok(p3.msgs.length === 30, 'T3 delivered 30');
  // ack only first 12: position = (seq of first row, off 12)
  await ack(c, Q, 'b', 'g', 'w1', { seq: p3.last.seq, off: 12 });
  const p3b = await pop(c, Q, 'b', 'g', 100, 'w1');
  ok(p3b.msgs.length === 18, 'T3 redelivery count', `got ${p3b.msgs.length}`);
  ok(p3b.msgs[0].txn === 't3-12', 'T3 resumes at frame 12', p3b.msgs[0].txn);
  await ack(c, Q, 'b', 'g', 'w1', p3b.last);

  // T4 nack
  console.log('T4 nack');
  const m4 = mkMsgs(10, 't4');
  await push(c, Q, 'b', m4);
  const p4 = await pop(c, Q, 'b', 'g', 10, 'w1');
  await ack(c, Q, 'b', 'g', 'w1', null, false);   // nack, no advance
  const p4b = await pop(c, Q, 'b', 'g', 10, 'w1');
  ok(p4b.msgs.length === 10 && p4b.msgs[0].txn === 't4-0', 'T4 full redelivery');
  await ack(c, Q, 'b', 'g', 'w1', p4b.last);

  // T5 lease exclusivity + expiry
  console.log('T5 lease');
  const m5 = mkMsgs(10, 't5');
  await push(c, Q, 'c', m5);
  const p5 = await pop(c, Q, 'c', 'g', 5, 'w1', { lease: 1 });
  ok(p5.msgs.length === 5, 'T5 w1 leased 5');
  const p5b = await pop(c, Q, 'c', 'g', 5, 'w2');
  ok(p5b.msgs.length === 0, 'T5 w2 blocked by lease');
  await new Promise((r) => setTimeout(r, 1200));
  const p5c = await pop(c, Q, 'c', 'g', 100, 'w2');
  ok(p5c.msgs.length === 10 && p5c.msgs[0].txn === 't5-0', 'T5 expiry -> w2 gets all from start', `got ${p5c.msgs.length}`);
  await ack(c, Q, 'c', 'g', 'w2', p5c.last);

  // T6 ack with wrong lease
  console.log('T6 bad ack');
  const m6 = mkMsgs(5, 't6');
  await push(c, Q, 'c', m6);
  const p6 = await pop(c, Q, 'c', 'g', 5, 'w1');
  const bad = await ack(c, Q, 'c', 'g', 'w-imposter', p6.last);
  ok(bad.ok === false, 'T6 imposter rejected', JSON.stringify(bad));
  const good = await ack(c, Q, 'c', 'g', 'w1', p6.last);
  ok(good.ok === true, 'T6 owner accepted');

  // T7 retention + cursor-on-deleted-segment guard
  console.log('T7 retention guard');
  const m7a = mkMsgs(20, 't7a');
  await push(c, Q, 'd', m7a);
  const p7 = await pop(c, Q, 'd', 'g', 8, 'w1');   // cursor mid-segment (off 8)
  await ack(c, Q, 'd', 'g', 'w1', p7.last);
  // delete everything consumed AND the cursor segment itself
  await c.query('SELECT q2.retention_v1(now())');
  // segment gone entirely (retention is segment-granular and our segment had
  // created_at < now); push new data, cursor.off=8 must NOT apply to it
  const m7b = mkMsgs(20, 't7b');
  await push(c, Q, 'd', m7b);
  const p7b = await pop(c, Q, 'd', 'g', 100, 'w1');
  ok(p7b.msgs.length === 20 && p7b.msgs[0].txn === 't7b-0',
     'T7 offset not applied to later segment', `got ${p7b.msgs.length} first=${p7b.msgs[0]?.txn}`);
  await ack(c, Q, 'd', 'g', 'w1', p7b.last);

  // T9 concurrent pushers + concurrent popper, same partition: exactly-once,
  // nothing stranded. (Also empirically refutes the hypothesized "gap race":
  // the row-lock seq allocator makes seq N+1 unallocatable until N commits,
  // so a pop can never observe N+1 without N.)
  console.log('T9 concurrent push+pop');
  const PUSHERS = 3, BATCHES = 15, BSIZE = 20;
  const expected = new Set();
  const pushed = [];
  for (let w = 0; w < PUSHERS; w++) {
    const msgs = mkMsgs(BATCHES * BSIZE, `t9w${w}`);
    msgs.forEach((m) => expected.add(m.mid));
    pushed.push(msgs);
  }
  const collected = [];
  let pushersDone = false;
  async function pusher(w) {
    const pc = await pool.connect();
    try {
      for (let b = 0; b < BATCHES; b++) {
        await push(pc, Q, 'conc', pushed[w].slice(b * BSIZE, (b + 1) * BSIZE));
      }
    } finally { pc.release(); }
  }
  async function popper() {
    const pc = await pool.connect();
    try {
      while (true) {
        const { msgs, last } = await pop(pc, Q, 'conc', 'g', 37, 'w-pop');
        if (msgs.length) {
          collected.push(...msgs);
          await ack(pc, Q, 'conc', 'g', 'w-pop', last);
        } else if (pushersDone) break;
        else await new Promise((r) => setTimeout(r, 5));
      }
    } finally { pc.release(); }
  }
  const popPromise = popper();
  await Promise.all(Array.from({ length: PUSHERS }, (_, w) => pusher(w)));
  pushersDone = true;
  await popPromise;
  const gotMids = new Set(collected.map((m) => m.mid));
  ok(collected.length === PUSHERS * BATCHES * BSIZE, 'T9 exactly-once count',
     `got ${collected.length}/${PUSHERS * BATCHES * BSIZE}`);
  ok(gotMids.size === collected.length, 'T9 no duplicates');
  const expectedHex = new Set([...expected].map((m) => m.replace(/-/g, '')));
  ok([...expectedHex].every((m) => gotMids.has(m)), 'T9 nothing stranded');

  // T8 independent consumer groups
  console.log('T8 consumer groups');
  const m8 = mkMsgs(15, 't8');
  await push(c, Q, 'e', m8);
  const g1 = await pop(c, Q, 'e', 'group1', 100, 'w1');
  await ack(c, Q, 'e', 'group1', 'w1', g1.last);
  const g2 = await pop(c, Q, 'e', 'group2', 100, 'w2');
  await ack(c, Q, 'e', 'group2', 'w2', g2.last);
  ok(g1.msgs.length === 15 && g2.msgs.length === 15, 'T8 both groups full stream',
     `g1=${g1.msgs.length} g2=${g2.msgs.length}`);

  c.release();
  await pool.end();
  console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
  process.exit(failures === 0 ? 0 : 1);
}
main().catch((e) => { console.error(e); process.exit(1); });
