// Combine a pgmq result dir and a Queen result dir into one side-by-side table.
// Usage: node combine.mjs <pgmqDir> <queenDir> <durationSec>
import fs from 'fs';

const [, , pgmqDir, queenDir, durStr] = process.argv;
const D = Number(durStr) || 120;

const readJSON = (p) => { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return null; } };
// Extract the number immediately preceding `unit` on the first line containing `label`.
// (Anchoring to the unit avoids matching digits inside labels like "p99".)
const grepVal = (p, label, unit) => {
  try {
    const line = fs.readFileSync(p, 'utf8').split('\n').find((l) => l.includes(label));
    if (!line) return null;
    const re = new RegExp('([0-9][0-9,]*\\.?[0-9]*)\\s*' + unit.replace('/', '\\/'));
    const m = line.match(re);
    return m ? Number(m[1].replace(/,/g, '')) : null;
  } catch { return null; }
};
function metrics(dir) {
  try {
    const rows = fs.readFileSync(`${dir}/metrics.csv`, 'utf8').trim().split('\n').slice(1);
    let na = 0, nc = 0, maxAct = 0, maxTot = 0, maxDead = 0, upd = 0, del = 0, av = 0;
    for (const r of rows) {
      const c = r.split(','); if (c.length < 10) continue;
      const a = +c[1]; na += a; nc++;
      if (a > maxAct) maxAct = a;
      if (+c[2] > maxTot) maxTot = +c[2];
      if (+c[4] > maxDead) maxDead = +c[4];
      upd = +c[6]; del = +c[7]; av = +c[8];
    }
    return { avgActive: nc ? na / nc : 0, peakActive: maxAct, peakTotal: maxTot, peakDead: maxDead, upd, del, av };
  } catch { return null; }
}

// pgmq (JSON results from the SQL harness)
const pp = readJSON(`${pgmqDir}/producer.json`) || {};
const pc = readJSON(`${pgmqDir}/consumer.json`) || {};
const pm = metrics(pgmqDir) || {};

// Queen: authoritative lifetime counters from /api/v1/status; latency from the
// example client logs (retention sweeps the queue, so we can't use queue totals).
const qs = readJSON(`${queenDir}/status.json`);
const qmsg = qs ? (qs.messages || qs) : {};
const qm = metrics(queenDir) || {};
const qPushMs = qmsg.total != null ? Math.round(qmsg.total / D) : null;
const qPopMs = qmsg.completed != null ? Math.round(qmsg.completed / D) : null;
const qProdP50 = grepVal(`${queenDir}/producer.log`, 'Latency p50:', 'ms');
const qProdP99 = grepVal(`${queenDir}/producer.log`, 'Latency p99:', 'ms');
const qConsP50 = grepVal(`${queenDir}/consumer.log`, 'Latency p50:', 'ms');
const qConsP99 = grepVal(`${queenDir}/consumer.log`, 'Latency p99:', 'ms');

const f = (v) => (v == null || Number.isNaN(v) ? 'n/a' : `${v}`);
const pad = (s, n) => String(s).padEnd(n);
const row = (label, q, p) => console.log(`${pad(label, 26)}${pad(q, 20)}${p}`);

console.log('\n' + '='.repeat(70));
console.log(`  QUEEN vs pgmq — Mac, ${D}s, equal Docker budget (6 vCPU / 5 GiB per stack)`);
console.log('='.repeat(70));
row('metric', 'QUEEN', 'pgmq');
console.log('-'.repeat(70));
row('push msg/s', f(qPushMs), f(pp.msgPerSec));
row('pop msg/s', f(qPopMs), f(pc.msgPerSec));
row('push p50 / p99 (ms)', `${f(qProdP50)} / ${f(qProdP99)}`, `${f(pp.latency?.p50)} / ${f(pp.latency?.p99)}`);
row('pop p50 / p99 (ms)', `${f(qConsP50)} / ${f(qConsP99)}`, `${f(pc.latency?.p50)} / ${f(pc.latency?.p99)}`);
console.log('-'.repeat(70));
row('PG active backends (avg)', f(qm.avgActive != null ? qm.avgActive.toFixed(1) : null), f(pm.avgActive != null ? pm.avgActive.toFixed(1) : null));
row('PG backends (peak total)', f(qm.peakTotal), f(pm.peakTotal));
console.log('-'.repeat(70));
row('hot-table UPDATEs', f(qm.upd), f(pm.upd));
row('hot-table DELETEs', f(qm.del), f(pm.del));
row('peak dead tuples', f(qm.peakDead), f(pm.peakDead));
row('autovacuum runs', f(qm.av), f(pm.av));
console.log('='.repeat(70));
console.log('push/pop msg/s (Queen) = server lifetime total/completed ÷ duration.');
console.log('Both PG: identical config, 3 CPU / 3 GiB. NOTE: Queen broker image is');
console.log('amd64 running EMULATED on arm64 Mac — its throughput is understated here.');
console.log('"hot table": pgmq.q_bench (UPDATE vt + DELETE per msg) vs queen.messages (append-only).\n');
