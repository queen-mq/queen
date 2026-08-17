// Deterministic load for the PLAN_KV_TIMERS performance gates. Raw HTTP on purpose: an SDK
// would add its own batching, retries and version drift between the two captures, and the thing
// under measurement is the broker, not a client.
//
// THREE PROPERTIES, and each one is there because dropping it breaks the 1% gate:
//
//  1. FIXED WORK, not fixed duration. The gate compares CPU per message, and CPU per message is
//     a function of the load level — a run that happens to go 8% faster is measured at a
//     different point on the curve. Every run sends exactly the same number of bundles, so the
//     denominator is identical by construction and only the numerator can move.
//  2. BYTE-IDENTICAL REQUEST BODIES. The payload is a constant, the transaction ids are derived
//     from (tag, worker, sequence), the queue and partition names are fixed. Two runs of
//     `bundle` mode against a fresh database send the same bytes in the same order, so a
//     difference in bytes hashed, parsed, compressed or written is a difference in the broker.
//     The gate prints the body checksum; if it differs between captures, the comparison is void.
//  3. CLOSED LOOP. Each worker sends the next bundle only after the previous one answered, so
//     the broker is never asked to queue work it has not been paid for and the run cannot
//     silently turn into a backlog measurement.
//
// `bundle` mode sends the wire bundle WITHOUT `kv` and WITHOUT `timers` — the exact payload
// shape §15 names, and the one that must stay byte-identical across the F4 patch.
//
//   node loadgen.mjs --mode bundle --url http://localhost:16644 --bundles 20000 --items 20
//
// Prints one JSON object on stdout. Everything else goes to stderr.

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) {
  args.set(process.argv[i].replace(/^--/, ''), process.argv[i + 1]);
}
const opt = (k, d) => (args.has(k) ? args.get(k) : d);
const num = (k, d) => Number(opt(k, d));

const URL_BASE = opt('url', 'http://localhost:16644');
const MODE = opt('mode', 'bundle');
const TAG = opt('tag', 'gate');
const SRC = opt('src', 'gate-src');
const DST = opt('dst', 'gate-dst');
const GROUP = opt('group', 'gate-group');
const PARTITIONS = num('partitions', 16);
const BUNDLES = num('bundles', 20000);
const ITEMS = num('items', 20);
const WORKERS = num('workers', 8);

// A fixed 256-byte payload: big enough that the frame codec, zstd and the WAL see a realistic
// message, small enough that the run is dominated by per-message work rather than by memcpy.
const FILLER = 'q'.repeat(200);
const PAYLOAD = { gate: 'kv-timers', v: 1, filler: FILLER };

let sentBytes = 0;
let bodySum = 0; // cheap order-sensitive checksum over every byte we send
let errors = 0;
let httpErrors = [];

function checksum(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h;
}

async function post(path, bodyObj) {
  const body = JSON.stringify(bodyObj);
  sentBytes += Buffer.byteLength(body);
  bodySum = (bodySum ^ checksum(body)) >>> 0;
  const res = await fetch(URL_BASE + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body,
  });
  const text = await res.text();
  if (!res.ok) {
    errors++;
    if (httpErrors.length < 5) httpErrors.push(`${path} ${res.status} ${text.slice(0, 200)}`);
  }
  return { status: res.status, text };
}

function pushItems(worker, seq) {
  const items = [];
  for (let i = 0; i < ITEMS; i++) {
    const n = seq * ITEMS + i;
    items.push({
      queue: DST,
      partition: `p-${n % PARTITIONS}`,
      payload: PAYLOAD,
      // Unique (dedup must not swallow the message) and deterministic (the body must be the
      // same bytes on the next capture).
      transactionId: `${TAG}-w${worker}-${seq}-${i}`,
    });
  }
  return items;
}

// --------------------------------------------------------------------------- modes

// The gate workload: one transaction wire bundle per round trip, pushes only. No pop, no lease,
// no consumer-group state — nothing whose timing depends on what a previous run left behind.
async function bundleWorker(worker, rounds) {
  for (let seq = 0; seq < rounds; seq++) {
    await post('/api/v1/transaction', {
      operations: [{ type: 'push', items: pushItems(worker, seq) }],
    });
  }
}

// Seed for `mixed`: plain pushes into the source queue.
async function seedWorker(worker, rounds) {
  for (let seq = 0; seq < rounds; seq++) {
    await post('/api/v1/push', { items: pushItems(worker, seq).map((i) => ({ ...i, queue: SRC })) });
  }
}

// The production-shaped variant: pop a batch, then one bundle that acks it and pushes the
// results. Closer to a real consumer, and noisier — the pop's batch size depends on what the
// partitions happen to hold, so the denominator is no longer fixed. Use it as a sanity pass,
// never as the 1% gate.
async function mixedWorker(worker, rounds) {
  for (let seq = 0; seq < rounds; seq++) {
    const url =
      `${URL_BASE}/api/v1/pop/queue/${SRC}?batch=${ITEMS}&consumerGroup=${GROUP}` +
      `&subscriptionMode=all&wait=false`;
    const res = await fetch(url);
    if (res.status === 204) continue;
    if (!res.ok) {
      errors++;
      continue;
    }
    const body = await res.json();
    const msgs = body.messages || [];
    if (msgs.length === 0) continue;
    const lease = body.leaseId;
    const acks = msgs.map((m) => ({
      type: 'ack',
      transactionId: m.transactionId,
      partitionId: m.partitionId || body.partitionId,
      consumerGroup: GROUP,
      status: 'completed',
      leaseId: m.leaseId || lease,
    }));
    await post('/api/v1/transaction', {
      requiredLeases: lease ? [lease] : [],
      operations: [{ type: 'push', items: pushItems(worker, seq) }, ...acks],
    });
  }
}

// --------------------------------------------------------------------------- run

const worker =
  MODE === 'bundle' ? bundleWorker : MODE === 'seed' ? seedWorker : MODE === 'mixed' ? mixedWorker : null;
if (!worker) {
  console.error(`unknown --mode ${MODE} (bundle|seed|mixed)`);
  process.exit(2);
}

const perWorker = Math.floor(BUNDLES / WORKERS);
const rounds = perWorker * WORKERS;
if (rounds !== BUNDLES) {
  console.error(`note: bundles rounded ${BUNDLES} -> ${rounds} so every worker does ${perWorker}`);
}

const t0 = process.hrtime.bigint();
await Promise.all(Array.from({ length: WORKERS }, (_, w) => worker(w, perWorker)));
const elapsedMs = Number(process.hrtime.bigint() - t0) / 1e6;

const messages = rounds * ITEMS;
console.log(
  JSON.stringify({
    mode: MODE,
    bundles: rounds,
    items: ITEMS,
    messages,
    workers: WORKERS,
    elapsed_ms: Math.round(elapsedMs),
    msg_per_s: Math.round((messages * 1000) / elapsedMs),
    bytes_sent: sentBytes,
    // Two captures whose body_checksum differs did NOT send the same bytes; the CPU-per-message
    // comparison between them means nothing and the gate says so.
    body_checksum: bodySum,
    errors,
    first_errors: httpErrors,
  }),
);
process.exit(errors > 0 ? 3 : 0);
