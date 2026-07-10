#!/usr/bin/env node
// Render results.jsonl into a comparison table.
import fs from 'node:fs';

const lines = fs.readFileSync(process.argv[2], 'utf8').trim().split('\n').map(JSON.parse);
const get = (p) => lines.find((l) => l.phase === p) || {};
const fmt = (n) => n >= 1e9 ? (n / 1e9).toFixed(2) + ' GB'
  : n >= 1e6 ? (n / 1e6).toFixed(1) + ' MB'
  : n >= 1e3 ? (n / 1e3).toFixed(1) + ' KB' : n + ' B';

function engine(v) {
  const ing = get(`ingest-${v}`), con = get(`consume-${v}`), ret = get(`retention-${v}`);
  const ingCpu = get(`ingest-${v}-cpu`), conCpu = get(`consume-${v}-cpu`);
  const sz = lines.find((l) => !l.phase && (v === 'v1' ? l['queen.messages'] : l['q2.segments']))
          || lines.find((l) => l.phase === undefined);
  // sizes rows were appended raw; find by table key
  const sizes = lines.find((l) => l[v === 'v1' ? 'queen.messages' : 'q2.segments']);
  const msgs = ing.msgs || 1;
  const main = sizes ? sizes[v === 'v1' ? 'queen.messages' : 'q2.segments'] : {};
  const dedup = sizes && v === 'v2' ? sizes['q2.dedup'] : null;
  const totalStored = (main.total || 0) + (dedup ? dedup.total : 0);
  return {
    v, msgs,
    ingest_rate: Math.round(msgs / (ing.seconds || 1)),
    ingest_wal_per_msg: Math.round((ing.wal_bytes || 0) / msgs),
    ingest_cpu: ingCpu.cpu_avg_pct,
    zstd_ratio: ing.zstd_ratio,
    heap_per_msg: +((main.heap || 0) / msgs).toFixed(1),
    idx_per_msg: +((main.idx || 0) / msgs).toFixed(1),
    dedup_per_msg: dedup ? +(dedup.total / msgs).toFixed(1) : 0,
    stored_per_msg: +(totalStored / msgs).toFixed(1),
    table_total: totalStored,
    consume_rate: Math.round((con.msgs || 0) / (con.seconds || 1)),
    consume_wal_per_msg: Math.round((con.wal_bytes || 0) / (con.msgs || 1)),
    consume_cpu: conCpu.cpu_avg_pct,
    retention_seconds: ret.seconds,
    retention_wal_per_msg: Math.round((ret.wal_bytes || 0) / msgs),
  };
}

const a = engine('v1'), b = engine('v2');
const rows = [
  ['messages', a.msgs, b.msgs, ''],
  ['ingest msgs/s', a.ingest_rate, b.ingest_rate, x(b.ingest_rate / a.ingest_rate)],
  ['ingest WAL/msg (B)', a.ingest_wal_per_msg, b.ingest_wal_per_msg, x(a.ingest_wal_per_msg / b.ingest_wal_per_msg)],
  ['ingest PG CPU %', a.ingest_cpu, b.ingest_cpu, ''],
  ['zstd group ratio', '-', b.zstd_ratio, ''],
  ['heap B/msg', a.heap_per_msg, b.heap_per_msg, x(a.heap_per_msg / b.heap_per_msg)],
  ['index B/msg', a.idx_per_msg, b.idx_per_msg, x(a.idx_per_msg / b.idx_per_msg)],
  ['dedup B/msg (v2 window)', '-', b.dedup_per_msg, ''],
  ['stored B/msg TOTAL', a.stored_per_msg, b.stored_per_msg, x(a.stored_per_msg / b.stored_per_msg)],
  ['table total', fmt(a.table_total), fmt(b.table_total), ''],
  ['consume msgs/s', a.consume_rate, b.consume_rate, x(b.consume_rate / a.consume_rate)],
  ['consume WAL/msg (B)', a.consume_wal_per_msg, b.consume_wal_per_msg, ''],
  ['consume PG CPU %', a.consume_cpu, b.consume_cpu, ''],
  ['retention sweep (s)', a.retention_seconds, b.retention_seconds, ''],
  ['retention WAL/msg (B)', a.retention_wal_per_msg, b.retention_wal_per_msg, x(a.retention_wal_per_msg / b.retention_wal_per_msg)],
];
function x(r) { return Number.isFinite(r) ? r.toFixed(1) + 'x' : ''; }

const w = [26, 14, 14, 8];
console.log(pad(['metric', 'v1 (rows)', 'v2 (segments)', 'gain']));
console.log('-'.repeat(w.reduce((s, n) => s + n + 2, 0)));
for (const r of rows) console.log(pad(r));
function pad(cols) {
  return cols.map((c, i) => String(c ?? '').padEnd(w[i])).join('  ');
}
