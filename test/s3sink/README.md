# test/s3sink — the S3 sink end to end, in-process

The S3 / data-lake sink (`connectors/queen-s3`) runs **inside the broker**
(`QUEEN_S3_EMBEDDED=true`, `server/src/s3_inproc.rs`), one sink per broker
tenant: the default tenant's from `QUEEN_S3_*`, every other tenant's from the
embedded proxy's control plane (`PUT /api/cp/clusters/:slug/s3`). The crate's
tests and the broker's in-process tests drive it against an in-memory bucket and
the real state machine. This suite drives what they can't: the real `queen`
binary as processes (one node, or a three-node openraft cluster on the
loopback), the real control plane over HTTP, a real HTTP S3 endpoint, real
`abort()`s, `SIGKILL`s and `SIGTERM`s, and restarts on the same data directory.

The bucket is the one thing that is simulated. `fake_s3.py` is a small
S3-compatible server that stores every object as a plain file. The suite
verifies the lake **by reading those files only**, the way a lake reader would,
and never trusts what the sink says it did.

```
cargo build --manifest-path server/Cargo.toml --bin queen   # target/debug/queen

python3 test/s3sink/run.py                        # every scenario, ~7 minutes
python3 test/s3sink/run.py --scenario a,d         # some of them
python3 test/s3sink/run.py --keep --work /tmp/s3e2e   # keep data dirs, bucket and logs
```

Python 3 standard library only. Nothing to install, no Docker, no network
beyond 127.0.0.1. Ports are picked free at run time. A full run needs about
100 MB of disk, and each scenario deletes its data dirs and lake prefix once it
passes; the rest is deleted at the end unless `--keep` is given or a scenario
failed (a failed scenario's data stays as evidence). Logs stay under `<work>/logs`:
one file per broker node (every restart appended, with a `===== harness:
start` line naming the `QUEEN_S3_*` it ran with), plus `fake_s3.log`, one JSON
line per S3 request.

| flag | default | |
|---|---|---|
| `--bin` | `target/debug/queen` (or `$QUEEN_BIN`) | the broker binary |
| `--work` | a fresh temp dir | where data, bucket and logs go |
| `--keep` | off | keep data dirs and the bucket after a passing run |
| `--scenario` | all | comma list of `s3,self,a,b,b2,c,d,e,g,f,h,i,j,k` |
| `--crash-points` | all five | for scenario b |
| `--a-partitions` | `1100,200,50` | partitions of scenario a's three queues |
| `--a-records` | `20` | records per partition in scenario a |
| `--a-pause` | `0.3` | seconds between scenario a's push rounds |
| `--f-seconds` | `40` | how long scenario f drives the race |
| `--rust-log` | `warn,queen-s3=info,boot=info,shutdown=info` | the brokers' `RUST_LOG` |

Exit code: 0 when every scenario passed, 1 when one failed, 2 when the suite
could not run (no binary, bad flag).

## What the broker runs with

Every node gets a fresh `QUEEN_RAFT_DIR`, `QUEEN_BIND_ADDR=127.0.0.1`, and
`QUEEN_RAFT_DISK_HIGH_PCT=99.9`/`LOW=99.8` (the broker refuses writes above its
disk mark). Only `PATH`, `HOME`, `TMPDIR`, `LANG`, `LC_ALL`, `USER` and `LOGNAME`
are inherited, so a `QUEEN_*` variable in your shell can't leak in. The sink:
`format=jsonl`, `compression=gzip` (so the standard library can read the lake),
`align=none`, `max_window_ms=1000`, `discovery_interval_ms=100`,
`safe_guard_ms=0` (windows close right at `safeTime`, so the claim that the raft
broker's `safeTime` is exact is what gets tested, not a margin),
`lease_ttl_ms=3000`, `shutdown_grace_ms=10000`, `checkpoint_every=2`,
`start=earliest`. A cluster adds `QUEEN_RAFT_REPLICATOR=openraft`,
`QUEEN_RAFT_NODE_ID`, `QUEEN_RAFT_PEERS`, `QUEEN_RAFT_LISTEN` and
`QUEEN_RAFT_TOKEN`.

A **cell** (scenarios h and i) adds the embedded proxy on a port of its own
(`QUEEN_PROXY_EMBEDDED=true`, `QUEEN_PROXY_PORT`, so `PORT` keeps serving the
broker router itself), one `QUEEN_PROXY_CP_TOKEN`, `QUEEN_TENANCY_HEADER=true`
with `QUEEN_KV_TRUSTED_PROXY=1` (the broker refuses the tenant header without
that affirmation), `QUEEN_PROXY_SPOOL_DIR` inside the data dir, and the same
`QUEEN_ENCRYPTION_KEY` (64 hex) on every node. Pushes into a broker tenant go
to `PORT` with `x-queen-tenant: <its uuid>`. The node-wide sink knobs (lease
TTL, discovery cadence, guard, checkpoint cadence) stay environment-only; a
tenant document carries the rest (`queues`, `endpoint`, `bucket`, `prefix`,
`accessKey`, `pathStyle`, `format`, `compression`, `align`, `start`,
`maxWindowMs`) plus `secretKey` beside it. The proxy boots without the console
assets (the SPA answers 404 "console not built").

Most scenarios create their queues **before** the sink starts, in a boot of
their own with the sink off (`provision`), so nothing waits on discovery. A
queue that doesn't exist yet is looked for again every 5 s (`MISSING_RETRY`,
`connectors/queen-s3/src/sink.rs`); scenario h creates its default-tenant queue
after the sink starts on purpose.

Payloads are pushed as **raw JSON text** and never round-tripped through a JSON
library on this side: integers far beyond 64 bits, `1E+400`, `-0`, escapes, raw
UTF-8 (including U+2028, U+0085 and DEL, which JSON allows raw inside a string),
odd whitespace, bare scalars, nested arrays, and 100 B to 4 KB strings. One item
in five in scenario a has no `transactionId`, so the broker mints one.

## What "the lake is right" means

`verify_queue` reads only the bucket directory. For one (tenant, queue):

- the manifests under `<prefix>/_queen/tenant=<t>/queue=<q>/windows/` number
  1..n with no gap, name the tenant and the queue, and each window starts where
  the previous one ended (`tStart(k) == tEnd(k-1)`);
- every manifest names exactly one object (merged layout) at the key the layout
  derives from the tenant, `k`, `tStart` and `tEnd`
  (`<prefix>/tenant=<t>/queue=<q>/dt=/hour=/w-…`), and its `bytes`, `sha256`,
  `records`, `partitions`, `minTs` and `maxTs` match the object; `lost` is
  empty;
- every object is gzip, ends with a newline, has one line per record, and each
  line is the documented envelope **byte for byte**
  (`{"partition":…,"offset":…,"transactionId":…,"ts":…,"payload":…}`, strings
  escaped as serde_json escapes them, `ts` with six fractional digits);
- lines in an object are sorted by `(partition, offset)`, and every `ts` is
  inside its window `[tStart, tEnd)`;
- every `(partition, offset)` the push answers acknowledged for THIS tenant is
  in the lake **exactly once**, nothing else is there (a record of another
  tenant is "never acknowledged"), the `transactionId` is the push answer's,
  and the payload text is **byte-identical** to what was pushed — with a raw
  `\n` or `\r` written as a space, the one change the JSONL writer makes so a
  record stays one line;
- offsets per partition are contiguous from 0, both in the lake and in the push
  answers, and `ts` never goes backwards with the offset;
- no data object exists that no manifest names (an orphan).

Scenario `self` shows the checker isn't vacuous. It writes a lake by hand, has
it accepted, then damages it nine ways (payload text, transactionId, a
duplicate, a missing record, order, a `ts` outside its window, tiling, an
orphan object, bytes that no longer match the manifest) and requires each one
to be refused for the right reason.

Waiting is always on the lake itself: the manifests' record counts equal what
was acknowledged, and nothing is orphaned. Nothing waits on a sleep and then
asserts. Every wait has a deadline and prints what it saw when it times out
(lake counts, each node's `/status` row, log tails).

## Scenarios

| | what happens | what it shows |
|---|---|---|
| `s3` | the fake S3 alone | it answers like S3: ETag = MD5, `Content-MD5` → `BadDigest`, payload hash → `XAmzContentSHA256Mismatch`, HEAD/GET 404 shapes (the sink's `HEAD prefix/` probe), LIST paged by `start-after` (what the sink sends), by continuation token and with a delimiter, multipart with S3's ETag rule (`md5(md5s)-N`, quotes escaped in the XML), `EntityTooSmall`, `InvalidPart`, abort, `NoSuchUpload`, injected faults |
| `self` | the lake checker alone | see above |
| `a` | one node, three queues of 1100/200/50 partitions × 20 records (27 000), pushed in 20 rounds while the sink runs | scale: discovery past one page (1000) and fetch past one call (1024 entries), ~30 windows, exactly once, byte-exact payloads, broker-minted transactionIds; `/status`, `queen_s3_records_written_total` and the KV commit pointers agree with the lake |
| `b` | one node, for each `QUEEN_S3_CRASH_AT` point (`after_intent`, `mid_upload`, `after_upload`, `before_commit`, `after_commit`): three committed windows, a restart with the point armed, push until the broker aborts (SIGABRT), read KV with the sink off, restart unarmed, push more | the crash left exactly the state the point names (intent ahead of commit; object without manifest; object and manifest; commit moved). The restart redoes the window (or commits a finished upload from its manifest), every object present before the crash is byte-identical after (the redo's re-PUT is reported), and everything is exactly once |
| `b2` | one node, `mid_upload` on a ~21 MB window (`compression=none`, 5 MB multipart threshold), so the abort lands **between multipart parts** | a stranded upload holding one 16 MiB part is left behind. The restart redoes the window as a 2-part upload whose first part is byte-identical to the stranded one, and exactly once holds |
| `c` | one node, SIGTERM in three positions: while windows fill; while a data-object PUT is held 2.5 s by the fake (an upload in flight); while it is held 12 s against a 4 s grace | the process exits by itself within the grace (rc 0). The drain commits the in-flight window and gives the leases back: lease rows are **absent** after restart with a 60 s TTL, so they were released, not expired, and intent == commit. Cut by the grace, the window stays an intent; the PUT lands late, and the restart redoes it with identical bytes. Exactly once each time |
| `d` | three nodes, six queues, sink on every node, pushing continuously through every live node | every node's `/status` names the same owner per queue. A follower owns a queue and commits windows (reads from its own state, KV writes forwarded to the leader). `kill -9` the owner: the survivors take every queue over (time to claim and to first commit reported). The killed node rejoins. SIGTERM the new owner: its leases are seen gone **before their own `expiresAt`** (released) and handed over. That node rejoins too. No window is committed by two nodes (from the logs), and all six queues are exactly once |
| `e` | one node, a queue named `e.ord/eu été:1%`, a 1.5 MB payload (above the 1 MiB fetch budget), and payloads whose JSON whitespace holds `\n` / `\r` (pretty-printed JSON) | escaping into `queue=…` and into the KV key, the over-budget record, and **whether a JSONL line stays one record** |
| `g` | one node; the bucket **down** when the broker boots (records pushed meanwhile), then 4 × `503 SlowDown` (Retry-After) on data PUTs and 2 × `500` on sidecar PUTs mid-traffic | `/status` says `reachable=false` and the sink waits rather than dropping. The faults are retried and counted in `queen_s3_s3_requests_total`. Exactly once |
| `f` | one node, the lease refresh made frequent (TTL 1 s) against 100 ms windows | no self-fence: the lease refresh and the window batches never fence the node out of its own queue (the round-1 bug, fixed by the per-handle write lock in `lease.rs`) |
| `h` | one node, a cell, four tenants on one queue NAME: the default tenant's environment sink (bucket `env-lake`) and three made by the control plane (`acme`, `globex`, `initech`; initech SHARES acme's bucket and prefix) | `/status` lists the sinks (`env`, `cp`); a refused document stores nothing; PUT/GET answer the redacted view only, no secret in `/status`, the broker's log or the S3 log, and the proxy's rows hold it sealed. Each tenant's records exactly once in its own `tenant=` root, nothing crossing, manifests naming their own tenant. DELETE stops a sink within a reload while the others (initech in the same bucket) keep shipping; suspending a cluster or its owner tenant stops it and reactivating resumes it; `push_blocked` keeps it; a secret rotation rebuilds it mid-traffic; `enabled=false` stops it and keeps the row; a re-PUT resumes from the commit pointer with no duplicate; per-tenant metrics (no label = the default tenant). A second cell without `QUEEN_ENCRYPTION_KEY` answers 409 `encryption_required` and stores nothing |
| `i` | three nodes, a cell, two tenants made by the control plane only (no environment sink), 4 queues each (one name shared) | a PUT on one node reaches every node's manager (time per node); tenant queues spread over the nodes and ship exactly once; `kill -9` of the node owning the most tenant queues — they are taken over (claim and first commit timed); the node rejoins; a node restarted with a DIFFERENT `QUEEN_ENCRYPTION_KEY` reports its tenant sinks in `phase=error`, owns nothing, and the others keep shipping exactly once; a DELETE made on one node stops the sink on every node |
| `j` | three nodes, six queues, nodes 1 and 2 boot, node 3 boots 1.5 s later; then node 3 stops and rejoins under traffic | the placement converges to the fair share (2/2/2) and nothing moves for three TTLs; with node 3 down the two others settle at 3/3, and once it rejoins the placement is 2/2/2 again. Every record exactly once |
| `k` | three nodes, six queues; six rounds that SIGTERM the leader (when it owns queues) or a follower, a LeaseWatch telling for each lease whether its row went before its own `expiresAt` (released) or at it (expired) | every lease of a SIGTERMed node, the leader's as well as a follower's, is released before its own `expiresAt` |

## Current state

Every scenario passes: 14 of 14 in the last full run (2026-10-02). The two
that were written to reproduce product bugs now check the fixes:

- **`j`: queues did not spread at a staggered cold start.** The node that
  could claim first took most of the queues, and nothing moved them
  afterwards. Fixed by rebalancing (`placement.rs`): every node counts the
  live sink nodes through presence rows, and a node over its fair share
  `ceil(queues / nodes)` gives one queue back at a time. Last run: a late
  joiner went from 3/3/0 to 2/2/2 in 5.2 s, 5,160 records exactly once.
- **`k`: a SIGTERMed leader did not reliably give its leases back.** The
  release (a KV get, then a fenced delete) raced the leader's own hand-off and
  gave up silently on a failed call, so the leases expired instead. Fixed by
  retrying a failed release for up to ten seconds (`lease.rs`). Last run:
  leader 8 of 8 released, follower 9 of 9.

The round-1 bugs no longer reproduce: `f` (the self-fence: none in 40 s of
100 ms windows) and `e` (a raw newline in a payload breaking JSONL) pass, and
a missing queue is looked for every 5 s (h). The multipart ETag is now
unescaped by the client (`client.rs` `xml_tags`); `b2` passes but does not
assert on that check's debug line.

## Limits

- **The bucket is a fake.** It's single-process, consistent and strictly
  read-after-write. It doesn't verify SigV4, so a signing bug would pass here
  (`infra_s3_versitygw.rs` covers a real gateway). It has no lifecycle rules,
  versioning or SSE. It answers errors with S3's XML shapes, and the escaped
  `&quot;` ETag of AWS's `CompleteMultipartUpload`, but it is not S3's
  behaviour under load. Faults are what `/_fake/delay` and `/_fake/fail` inject;
  there are no torn connections or partial bodies.
- **Debug binary, one machine.** Timings (push rates, takeover and hand-over
  seconds) describe a debug build on a laptop's loopback and are reported, not
  asserted, beyond generous bounds that follow from the configuration (for
  example takeover ≤ 2 × TTL + 10 s). The cluster runs on 127.0.0.1: no network
  partitions, no clock skew, no slow disks.
- **What isn't covered:** Parquet, `per-partition` layout, `align=hour/day`,
  `start=latest`, retention overrunning the sink (lost ranges), the
  `retentionSinkHold` interplay, SSE, `QUEEN_S3_QUEUES=*` and `queues: "*"`, a
  queue deleted while the sink runs, a tenant's purge and delete
  (`/api/cp/tenants/:slug/purge`), the proxy's data plane (pushes go straight to
  the broker port with the tenant header; plan limits are not exercised),
  `/api/cp/bootstrap` and `/api/cp/provision`, many tenants per node, and the
  1M-msg/s regime. The 1.5.0 suite's scenarios 3, 5, 6 and 7
  (`git show edc699b6:test/runners/s3sink/scenarios.py`) are the natural next
  ports.
- Scenario d reports the placement it sees at a cold start. If the leader
  holds every queue, it moves them off it with a rolling restart of the owner
  (bounded, and reported) before checking that a follower commits.
- The sink's log lines name the queue, not the tenant: with two tenants'
  queues of one name on a node, `window committed queue=h.orders k=1` does not
  say whose. Scenario i keeps its log-based "no window committed twice" check
  to the queue names only one tenant has.
