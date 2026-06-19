// Turn the raw per-step snapshots into metrics (results.csv / results.json) and a
// self-contained report.html with the curves that matter. Up-sweep and down-sweep
// are drawn as two overlaid series so a hysteresis loop is visible at a glance.
//
//   usage: node analyze.mjs <OUT_DIR>
//
// The headline metric is msgs/commit = Δ(queen.messages.n_tup_ins) / Δ(xact_commit):
// the pure server-side fusion ratio (push_batch is 1 on the client). PG cost per
// message = PG cores / (msgs/s) is the curve that should fall as fusion amortizes
// the fixed per-commit fsync; watch the absolute PG-cores curve for a possible dip
// at the knee, and watch achieved-vs-offered for the saturation cliff.

import { readFileSync, writeFileSync, existsSync } from 'node:fs';

const OUT_DIR = process.argv[2];
if (!OUT_DIR) { console.error('usage: node analyze.mjs <OUT_DIR>'); process.exit(2); }

const manifest = readFileSync(`${OUT_DIR}/manifest.jsonl`, 'utf8')
  .trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));

const num = (x) => (typeof x === 'number' ? x : parseFloat(x));
function meanCpuPct(file) {
  if (!existsSync(file)) return null;
  const vals = readFileSync(file, 'utf8').trim().split('\n')
    .map((s) => parseFloat(s)).filter((v) => Number.isFinite(v));
  if (!vals.length) return null;
  return vals.reduce((a, b) => a + b, 0) / vals.length;
}

const rows = [];
for (const m of manifest) {
  const dir = m.dir;
  if (!existsSync(`${dir}/pre.json`) || !existsSync(`${dir}/post.json`)) continue;
  const pre = JSON.parse(readFileSync(`${dir}/pre.json`, 'utf8'));
  const post = JSON.parse(readFileSync(`${dir}/post.json`, 'utf8'));
  const secs = num(post.database.ts) - num(pre.database.ts);
  if (!(secs > 0)) continue;

  const dCommit = num(post.database.xact_commit) - num(pre.database.xact_commit);
  const dInsMsg = num(post.messages.n_tup_ins ?? 0) - num(pre.messages.n_tup_ins ?? 0);
  const dWalBytes = num(post.wal.wal_bytes) - num(pre.wal.wal_bytes);
  const dWalSync = num(post.wal.wal_sync) - num(pre.wal.wal_sync);
  const dWalRecords = num(post.wal.wal_records) - num(pre.wal.wal_records);

  const commitsPerS = dCommit / secs;
  const msgsPerS = dInsMsg / secs;
  const msgsPerCommit = dCommit > 0 ? dInsMsg / dCommit : 0;
  const walSyncPerS = dWalSync / secs;
  const walSyncPerMsg = dInsMsg > 0 ? dWalSync / dInsMsg : 0;
  const walBytesPerMsg = dInsMsg > 0 ? dWalBytes / dInsMsg : 0;
  const walRecordsPerCommit = dCommit > 0 ? dWalRecords / dCommit : 0;

  const cpuPct = meanCpuPct(`${dir}/pg-cpu.txt`);
  const pgCores = cpuPct == null ? null : cpuPct / 100;
  const coresPerMmsg = pgCores != null && msgsPerS > 0 ? (pgCores * 1e6) / msgsPerS : null;

  let pacer = {};
  if (existsSync(`${dir}/pacer.json`)) pacer = JSON.parse(readFileSync(`${dir}/pacer.json`, 'utf8'));
  const offered = m.rate;
  const achievedRatio = offered > 0 ? msgsPerS / offered : 0;

  rows.push({
    idx: m.idx, phase: m.phase, offered,
    achieved_msgs_per_s: round(msgsPerS),
    achieved_ratio: round(achievedRatio, 3),
    saturated: achievedRatio < 0.9,
    msgs_per_commit: round(msgsPerCommit, 2),
    commits_per_s: round(commitsPerS, 1),
    wal_sync_per_s: round(walSyncPerS, 1),
    wal_sync_per_msg: round(walSyncPerMsg, 4),
    wal_bytes_per_msg: round(walBytesPerMsg, 1),
    wal_records_per_commit: round(walRecordsPerCommit, 1),
    pg_cores: pgCores == null ? null : round(pgCores, 3),
    pg_cores_per_Mmsg: coresPerMmsg == null ? null : round(coresPerMmsg, 3),
    p50_ms: pacer?.latency_ms?.p50 ?? null,
    p99_ms: pacer?.latency_ms?.p99 ?? null,
    deficit: pacer?.deficit ?? null,
    measure_secs: round(secs, 1),
  });
}

function round(x, d = 1) { const f = 10 ** d; return Math.round(x * f) / f; }

// ---- CSV --------------------------------------------------------------------
const cols = Object.keys(rows[0] || { note: 'no-data' });
const csv = [cols.join(',')]
  .concat(rows.map((r) => cols.map((c) => (r[c] == null ? '' : r[c])).join(',')))
  .join('\n');
writeFileSync(`${OUT_DIR}/results.csv`, csv + '\n');
writeFileSync(`${OUT_DIR}/results.json`, JSON.stringify(rows, null, 2));

// ---- HTML report ------------------------------------------------------------
const up = rows.filter((r) => r.phase === 'up').sort((a, b) => a.offered - b.offered);
const down = rows.filter((r) => r.phase === 'down').sort((a, b) => a.offered - b.offered);
const allRates = [...new Set(rows.map((r) => r.offered))].sort((a, b) => a - b);

function chart({ title, sub, yacc, fmtY = (v) => `${v}`, yFromZero = true, diag = false }) {
  const W = 560, H = 320, ml = 64, mr = 16, mt = 16, mb = 52;
  const iw = W - ml - mr, ih = H - mt - mb;
  const lx = (r) => Math.log10(Math.max(1, r));
  const xmin = lx(Math.min(...allRates)), xmax = lx(Math.max(...allRates));
  const X = (r) => ml + (iw * (lx(r) - xmin)) / (xmax - xmin || 1);
  const ys = rows.map(yacc).filter((v) => v != null && Number.isFinite(v));
  let ymin = yFromZero ? 0 : Math.min(...ys), ymax = Math.max(...ys, 0.0001);
  if (ymax === ymin) ymax = ymin + 1;
  ymax *= 1.08;
  const Y = (v) => mt + ih - (ih * (v - ymin)) / (ymax - ymin);

  const grid = [];
  for (let i = 0; i <= 4; i++) {
    const v = ymin + ((ymax - ymin) * i) / 4, y = Y(v);
    grid.push(`<line x1="${ml}" y1="${y.toFixed(1)}" x2="${ml + iw}" y2="${y.toFixed(1)}" stroke="#e5e7eb"/>`);
    grid.push(`<text x="${ml - 8}" y="${(y + 4).toFixed(1)}" text-anchor="end" font-size="11" fill="#6b7280">${fmtY(round(v, 2))}</text>`);
  }
  const xticks = allRates.map((r) => {
    const x = X(r);
    return `<text x="${x.toFixed(1)}" y="${H - mb + 16}" text-anchor="middle" font-size="10" fill="#6b7280">${r >= 1000 ? r / 1000 + 'k' : r}</text>`
      + `<line x1="${x.toFixed(1)}" y1="${mt}" x2="${x.toFixed(1)}" y2="${mt + ih}" stroke="#f3f4f6"/>`;
  }).join('');

  function path(series, color) {
    const pts = series.map((r) => ({ x: X(r.offered), y: yacc(r) })).filter((p) => p.y != null && Number.isFinite(p.y)).map((p) => ({ x: p.x, y: Y(p.y) }));
    if (!pts.length) return '';
    const d = pts.map((p, i) => `${i ? 'L' : 'M'}${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
    const dots = pts.map((p) => `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="3" fill="${color}"/>`).join('');
    return `<path d="${d}" fill="none" stroke="${color}" stroke-width="2"/>${dots}`;
  }

  // optional y=x diagonal (for achieved-vs-offered), in offered-rate units
  let diagLine = '';
  if (diag) {
    const pts = allRates.map((r) => `${X(r).toFixed(1)} ${Y(r).toFixed(1)}`);
    diagLine = `<polyline points="${pts.join(' ')}" fill="none" stroke="#9ca3af" stroke-width="1" stroke-dasharray="4 4"/>`;
  }

  return `<div class="card">
    <h3>${title}</h3><div class="sub">${sub}</div>
    <svg viewBox="0 0 ${W} ${H}" width="100%">
      ${grid.join('')}${xticks}
      <line x1="${ml}" y1="${mt}" x2="${ml}" y2="${mt + ih}" stroke="#9ca3af"/>
      <line x1="${ml}" y1="${mt + ih}" x2="${ml + iw}" y2="${mt + ih}" stroke="#9ca3af"/>
      ${diagLine}
      ${path(up, '#2563eb')}
      ${path(down, '#ea580c')}
      <text x="${ml + iw / 2}" y="${H - 6}" text-anchor="middle" font-size="11" fill="#374151">offered push/s (log)</text>
    </svg>
  </div>`;
}

const charts = [
  chart({ title: 'Fusion ratio — msgs / commit', sub: 'THE curve: Δn_tup_ins / Δxact_commit. Should climb from ~1 toward preferred/max_batch as load rises.', yacc: (r) => r.msgs_per_commit }),
  chart({ title: 'Commits / s', sub: 'Rises with load then plateaus once batching kicks in. The plateau = fusion engaged.', yacc: (r) => r.commits_per_s }),
  chart({ title: 'WAL fsync / s', sub: 'pg_stat_wal.wal_sync rate. Flattening while msgs/s keeps rising = the fsync cost is being amortized.', yacc: (r) => r.wal_sync_per_s }),
  chart({ title: 'PG cores (absolute)', sub: 'Mean PG container CPU. Usually monotone up — but watch for a DIP near the knee (the non-monotonic effect).', yacc: (r) => r.pg_cores, yFromZero: true }),
  chart({ title: 'PG cost per message — cores per Mmsg/s', sub: 'PG cores ÷ (msgs/s). The amortization curve: should fall steeply then bottom out.', yacc: (r) => r.pg_cores_per_Mmsg }),
  chart({ title: 'Achieved vs offered (the cliff)', sub: 'Ground-truth msgs/s vs offered. Dashed line = ideal y=x. Where the curve peels off below it, PG is saturated.', yacc: (r) => r.achieved_msgs_per_s, diag: true }),
];

const tableRows = rows.map((r) => `<tr class="${r.phase}">
  <td>${r.phase}</td><td>${r.offered}</td><td>${r.achieved_msgs_per_s}</td><td>${r.msgs_per_commit}</td>
  <td>${r.commits_per_s}</td><td>${r.wal_sync_per_s}</td><td>${r.wal_bytes_per_msg}</td>
  <td>${r.pg_cores ?? ''}</td><td>${r.pg_cores_per_Mmsg ?? ''}</td><td>${r.p50_ms ?? ''}</td><td>${r.p99_ms ?? ''}</td>
  <td>${r.saturated ? '⚠︎' : ''}</td></tr>`).join('');

const html = `<!doctype html><meta charset="utf-8"><title>queen fusion hysteresis</title>
<style>
  body{font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;color:#111827;max-width:1200px;margin:24px auto;padding:0 16px}
  h1{font-size:22px;margin:0 0 4px} h3{font-size:14px;margin:0 0 2px} .sub{font-size:11px;color:#6b7280;margin-bottom:6px;min-height:28px}
  .legend{margin:8px 0 20px;font-size:13px} .legend b{font-weight:600}
  .sw{display:inline-block;width:12px;height:12px;border-radius:2px;vertical-align:middle;margin:0 4px 0 12px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:18px}
  .card{border:1px solid #e5e7eb;border-radius:10px;padding:12px}
  table{border-collapse:collapse;width:100%;margin-top:24px;font-size:12px}
  th,td{border-bottom:1px solid #eee;padding:4px 8px;text-align:right} th:first-child,td:first-child{text-align:left}
  tr.down td{color:#9a3412} thead th{position:sticky;top:0;background:#fff}
  .note{background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;padding:12px 16px;margin:12px 0 20px;font-size:13px}
</style>
<h1>Queen fusion engine — hysteresis sweep</h1>
<div class="legend">
  <span class="sw" style="background:#2563eb"></span><b>up-sweep</b>
  <span class="sw" style="background:#ea580c"></span><b>down-sweep</b>
  — if the two trace different paths, that's real hysteresis from the self-clocked group commit.
</div>
<div class="note">
  <b>How to read it.</b> PG cost ≈ <i>commits/s × fixed&nbsp;fsync&nbsp;cost + rows/s × per-row cost + maintenance</i>.
  At low offered rate each push is its own commit (msgs/commit ≈ 1) so the fixed fsync dominates and
  <b>PG cost per message is high</b>. As load rises the fusion engine batches (msgs/commit climbs, commits/s & fsync/s plateau)
  and <b>cost per message falls</b>. Absolute PG cores usually keep rising; a genuine <i>dip</i> there near the knee would be the
  non-monotonic effect worth a screenshot. The cliff shows up in the last chart, where achieved peels away from offered.
</div>
<div class="grid">${charts.join('')}</div>
<table>
  <thead><tr><th>phase</th><th>offered</th><th>achieved/s</th><th>msgs/commit</th><th>commits/s</th>
  <th>fsync/s</th><th>wal B/msg</th><th>PG cores</th><th>cores/Mmsg</th><th>p50 ms</th><th>p99 ms</th><th>sat</th></tr></thead>
  <tbody>${tableRows}</tbody>
</table>`;

writeFileSync(`${OUT_DIR}/report.html`, html);
console.log(`[analyze] ${rows.length} steps -> results.csv, results.json, report.html`);
for (const r of rows) {
  console.log(`  ${r.phase.padEnd(4)} offered=${String(r.offered).padStart(6)} achieved=${String(r.achieved_msgs_per_s).padStart(7)} msgs/commit=${String(r.msgs_per_commit).padStart(6)} fsync/s=${String(r.wal_sync_per_s).padStart(6)} cores=${r.pg_cores ?? '—'} cores/Mmsg=${r.pg_cores_per_Mmsg ?? '—'}${r.saturated ? '  ⚠ saturated' : ''}`);
}
