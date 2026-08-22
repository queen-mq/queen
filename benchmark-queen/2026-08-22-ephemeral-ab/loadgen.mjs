// Durable vs ephemeral A/B on one broker, one Postgres, one machine.
//
// Raw HTTP on purpose: an SDK would add its own batching and retries, and the thing under
// measurement is the storage class, not a client. Both lanes send the SAME payload bytes, the
// same batch sizes, the same partition count and the same worker count; the only difference is
// the route family and therefore whether PostgreSQL is in the path.
//
// Both lanes run at their DEFAULTS, because that is the choice a user actually faces: durable
// keeps its dedup index and its retention, ephemeral has neither, and pretending otherwise by
// tuning the durable lane down would measure a queue nobody runs.
//
// Modes:
//   throughput  fixed work, closed loop, two phases (push everything, then drain everything).
//               Phases are separate so a slow drain cannot be charged to the push.
//   latency     signal delivery: park a long-poll pop, push one message, measure until the pop
//               answers. One in flight, so the number is a floor and not a queueing artifact.
//
//   node loadgen.mjs --class durable --mode throughput --messages 20000

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].replace(/^--/, ''), process.argv[i + 1]);
const opt = (k, d) => (args.has(k) ? args.get(k) : d);
const num = (k, d) => Number(opt(k, d));

const URL_BASE   = opt('url', 'http://localhost:6632');
const CLASS      = opt('class', 'durable');          // durable | ephemeral
const MODE       = opt('mode', 'throughput');        // throughput | latency
const QUEUE      = opt('queue', `ab-${CLASS}`);
const GROUP      = opt('group', 'ab-group');
const PARTITIONS = num('partitions', 8);
const MESSAGES   = num('messages', 20000);
const BATCH      = num('batch', 100);                // messages per push call
const POP_BATCH  = num('popBatch', 100);
const WORKERS    = num('workers', 8);
const ITERS      = num('iters', 200);                // latency mode
const EPHEMERAL  = CLASS === 'ephemeral';

// 256-byte payload: big enough that the codec and the WAL see a realistic message, small enough
// that the run measures per-message work rather than memcpy.
const PAYLOAD = { ab: 'eph-vs-durable', v: 1, filler: 'q'.repeat(200) };

let httpErrors = [];
async function req(method, path, body) {
  const r = await fetch(`${URL_BASE}${path}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (r.status >= 400) {
    const t = await r.text();
    if (httpErrors.length < 5) httpErrors.push(`${method} ${path} -> ${r.status} ${t.slice(0, 200)}`);
    throw new Error(`${r.status}`);
  }
  if (r.status === 204) return null;
  const t = await r.text();
  return t ? JSON.parse(t) : null;
}

const pct = (a, p) => (a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(a.length * p))] : 0);

// ---------- wire, per class ----------
function pushBody(part, n, seqBase) {
  if (EPHEMERAL) return [`/api/v1/ephemeral/push`, { queue: QUEUE, partition: part, messages: Array.from({ length: n }, () => ({ payload: PAYLOAD })) }];
  return [`/api/v1/push`, { items: Array.from({ length: n }, (_, i) => ({ queue: QUEUE, partition: part, transactionId: `${part}-${seqBase + i}`, payload: PAYLOAD })) }];
}
function popPath({ batch, wait, timeout }) {
  if (EPHEMERAL) return `/api/v1/ephemeral/pop?queue=${QUEUE}&group=${GROUP}&batch=${batch}&wait=${wait}&timeout=${timeout}`;
  return `/api/v1/pop/queue/${QUEUE}?consumerGroup=${GROUP}&batch=${batch}&partitions=${PARTITIONS}&wait=${wait}&timeout=${timeout}`;
}
async function ack(res) {
  if (EPHEMERAL) {
    return req('POST', '/api/v1/ephemeral/ack', { queue: QUEUE, group: GROUP, acks: res.messages.map((m) => ({ id: m.id, status: 'completed' })) });
  }
  return req('POST', '/api/v1/ack/batch', {
    consumerGroup: GROUP,
    acknowledgments: res.messages.map((m) => ({ transactionId: m.transactionId, partitionId: m.partitionId, leaseId: m.leaseId, status: 'completed' })),
  });
}

// ---------- throughput ----------
// Seed every partition and establish the group's cursor BEFORE the measured phases.
// Without this the durable lane measures nothing: `subscriptionMode` defaults to `new`, so a
// group first created at drain time starts from now and never sees the pushed backlog. The
// steady state a real consumer runs in is a group that already exists, and that is what both
// lanes are measured in.
async function seedAndPrime() {
  for (let i = 0; i < PARTITIONS; i++) {
    const [path, body] = pushBody(`p-${i}`, 1, 900_000 + i);
    await req('POST', path, body);
  }
  let empties = 0;
  while (empties < 3) {
    const res = await req('GET', popPath({ batch: 1000, wait: false, timeout: 1000 }));
    const msgs = res && res.messages ? res.messages : [];
    if (!msgs.length) { empties++; continue; }
    empties = 0;
    await ack(res);
  }
}


async function throughput() {
  await seedAndPrime();

  const perWorker = Math.floor(MESSAGES / WORKERS);
  const pushLat = [];

  const t0 = process.hrtime.bigint();
  await Promise.all(Array.from({ length: WORKERS }, async (_, w) => {
    let sent = 0;
    while (sent < perWorker) {
      const n = Math.min(BATCH, perWorker - sent);
      const part = `p-${(w + sent) % PARTITIONS}`;
      const [path, body] = pushBody(part, n, w * 1_000_000 + sent);
      const a = process.hrtime.bigint();
      await req('POST', path, body);
      pushLat.push(Number(process.hrtime.bigint() - a) / 1e6);
      sent += n;
    }
  }));
  const pushMs = Number(process.hrtime.bigint() - t0) / 1e6;
  const pushed = perWorker * WORKERS;

  // Phase 2: drain. Stop when every worker has seen a run of empties.
  const popLat = [];
  let drained = 0;
  const t1 = process.hrtime.bigint();
  await Promise.all(Array.from({ length: WORKERS }, async () => {
    let empties = 0;
    while (empties < 3 && drained < pushed) {
      const a = process.hrtime.bigint();
      const res = await req('GET', popPath({ batch: POP_BATCH, wait: false, timeout: 1000 }));
      const msgs = res && res.messages ? res.messages : [];
      if (!msgs.length) { empties++; continue; }
      empties = 0;
      await ack(res);
      popLat.push(Number(process.hrtime.bigint() - a) / 1e6);
      drained += msgs.length;
    }
  }));
  const popMs = Number(process.hrtime.bigint() - t1) / 1e6;

  return {
    class: CLASS, mode: MODE, messages: pushed, partitions: PARTITIONS, workers: WORKERS, batch: BATCH,
    push: { ms: +pushMs.toFixed(1), msgPerSec: Math.round((pushed / pushMs) * 1000), callP50ms: +pct(pushLat, 0.5).toFixed(2), callP99ms: +pct(pushLat, 0.99).toFixed(2) },
    drain: { ms: +popMs.toFixed(1), drained, msgPerSec: Math.round((drained / popMs) * 1000), callP50ms: +pct(popLat, 0.5).toFixed(2), callP99ms: +pct(popLat, 0.99).toFixed(2) },
    httpErrors,
  };
}

// ---------- latency ----------
async function latency() {
  const lat = [];
  // Warm the queue and the group cursor so iteration 1 is not measuring creation.
  await seedAndPrime();

  for (let i = 0; i < ITERS; i++) {
    const parked = req('GET', popPath({ batch: 1, wait: true, timeout: 5000 }) + '&autoAck=true');
    await new Promise((r) => setTimeout(r, 5)); // let the pop actually park on the gate
    const a = process.hrtime.bigint();
    const [path, body] = pushBody('p-0', 1, 1_000_000 + i);
    await req('POST', path, body);
    const res = await parked;
    const dt = Number(process.hrtime.bigint() - a) / 1e6;
    if (res && res.messages && res.messages.length) lat.push(dt);
  }
  return {
    class: CLASS, mode: MODE, iterations: ITERS, delivered: lat.length,
    p50ms: +pct(lat, 0.5).toFixed(2), p95ms: +pct(lat, 0.95).toFixed(2), p99ms: +pct(lat, 0.99).toFixed(2),
    minms: +Math.min(...lat).toFixed(2), httpErrors,
  };
}

const out = MODE === 'latency' ? await latency() : await throughput();
console.log(JSON.stringify(out, null, 2));
