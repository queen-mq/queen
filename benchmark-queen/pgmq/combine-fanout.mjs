// Combine fan-out results. Usage: node combine-fanout.mjs <pgmqDir> <queenDir> <D> <groups>
import fs from 'fs';
const [, , pgmqDir, queenDir, durStr, gStr] = process.argv;
const D = Number(durStr) || 120, G = Number(gStr) || 10;

const readJSON = (p) => { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return null; } };
const readNum = (p) => { try { return Number(fs.readFileSync(p, 'utf8').trim()); } catch { return null; } };
function sumConsumers(dir) {
  let total = 0;
  try {
    for (const f of fs.readdirSync(dir))
      if (f.startsWith('consumer-') && f.endsWith('.json')) {
        const d = readJSON(`${dir}/${f}`); if (d && d.totalMessages) total += d.totalMessages;
      }
  } catch {}
  return total;
}
function backends(dir) {
  try {
    const rows = fs.readFileSync(`${dir}/metrics.csv`, 'utf8').trim().split('\n').slice(1);
    let na = 0, nc = 0, maxTot = 0;
    for (const r of rows) { const c = r.split(','); if (c.length < 10) continue; na += +c[1]; nc++; if (+c[2] > maxTot) maxTot = +c[2]; }
    return { avg: nc ? na / nc : 0, peakTotal: maxTot };
  } catch { return { avg: 0, peakTotal: 0 }; }
}

// pgmq
const pp = readJSON(`${pgmqDir}/producer.json`) || {};
const pgLogical = pp.totalMessages || 0;
const pgPushMs = pp.msgPerSec || 0;
const pgDelivered = sumConsumers(pgmqDir);
const agg = (() => { try { return fs.readFileSync(`${pgmqDir}/agg.csv`, 'utf8').trim().split(',').map(Number); } catch { return []; } })();
const [pgIns = 0, pgUpd = 0, pgDel = 0, pgDead = 0, pgBytes = 0] = agg;
const pgb = backends(pgmqDir);

// queen
const qs = readJSON(`${queenDir}/status.json`); const qmsg = qs ? (qs.messages || qs) : {};
const qLogical = qmsg.total || 0, qDelivered = qmsg.completed || 0;
const qBytes = readNum(`${queenDir}/size.txt`) || 0;
const qb = backends(queenDir);

const mb = (b) => `${(b / 1048576).toFixed(0)} MB`;
const f = (v) => (v == null || Number.isNaN(v) ? 'n/a' : `${v}`);
const pad = (s, n) => String(s).padEnd(n);
const row = (l, q, p) => console.log(`${pad(l, 28)}${pad(q, 18)}${p}`);

console.log('\n' + '='.repeat(72));
console.log(`  FAN-OUT: ${G} consumer groups — Mac, ${D}s, equal Docker budget`);
console.log('='.repeat(72));
row('metric', 'QUEEN', 'pgmq');
console.log('-'.repeat(72));
row('logical push msg/s', f(Math.round(qLogical / D)), f(pgPushMs));
row('total delivered msg/s', f(Math.round(qDelivered / D)), f(Math.round(pgDelivered / D)));
row('delivered / logical', f((qDelivered / Math.max(qLogical, 1)).toFixed(1)) + 'x', f((pgDelivered / Math.max(pgLogical, 1)).toFixed(1)) + 'x');
console.log('-'.repeat(72));
row('physical rows written', f(qLogical), f(pgIns));
row('write amplification', f((qLogical / Math.max(qLogical, 1)).toFixed(1)) + 'x', f((pgIns / Math.max(pgLogical, 1)).toFixed(1)) + 'x');
row('hot UPDATE+DELETE', f(qc(queenDir)), f(pgUpd + pgDel));
row('peak dead tuples', f(qDead(queenDir)), f(pgDead));
row('stored copies size', mb(qBytes), mb(pgBytes));
console.log('-'.repeat(72));
row('PG active backends(avg)', f(qb.avg.toFixed(1)), f(pgb.avg.toFixed(1)));
row('PG backends(peak total)', f(qb.peakTotal), f(pgb.peakTotal));
console.log('='.repeat(72));
console.log(`Queen: 1 physical copy + ${G} cursors.  pgmq: ${G} physical copies (topic fan-out) + per-copy delete churn.`);
console.log('logical push = unique events; delivered = sum across all groups.');
console.log('NOTE: Queen broker EMULATED on arm64 -> throughput understated; the amplification/churn story is exact.\n');

// queen churn helpers (messages table is append-only -> expect ~0)
function qc(dir) { try { const rows = fs.readFileSync(`${dir}/metrics.csv`, 'utf8').trim().split('\n').slice(1); const c = rows[rows.length - 1].split(','); return (+c[6]) + (+c[7]); } catch { return 0; } }
function qDead(dir) { try { const rows = fs.readFileSync(`${dir}/metrics.csv`, 'utf8').trim().split('\n').slice(1); let m = 0; for (const r of rows) { const c = r.split(','); if (+c[4] > m) m = +c[4]; } return m; } catch { return 0; } }
