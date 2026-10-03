# test/pgconn — the Postgres source and sink end to end

The PostgreSQL connectors (`connectors/queen-pg`, PLAN_PG_CONNECTORS.md) run
**inside the broker** (`server/src/pg_inproc.rs`, feature `pg`), configured
through `PUT /api/v1/connectors/:name`. The crate's tests drive the engines
against an in-memory Queen; this suite drives what they can't: the real `queen`
binary as processes (one node, or a three-node openraft cluster on the
loopback), a real PostgreSQL 17+, real `kill -9`s and `SIGTERM`s, and restarts
on the same data directory.

Nothing is simulated. Every check reads the data back — the queue message by
message (pops of a consumer group of the suite's own), the tables row by row
(`psql`) — and never trusts what a connector says it did.

```
cd server && cargo build --bin queen          # server/target/debug/queen, feature pg

python3 test/pgconn/run.py                    # every scenario, ~5 minutes
python3 test/pgconn/run.py a d h              # some of them (or "a,d,h")
python3 test/pgconn/run.py --seed 42          # the same random choices again
python3 test/pgconn/run.py --scale 10 g       # the 2 M-row transaction of PLAN §7
python3 test/pgconn/run.py --keep             # keep data dirs, logs and the tables
```

Python 3 standard library only, plus `psql` (any version that can talk to the
server). Ports are picked free at run time. Exit code: 0 every scenario passed
(or was skipped), 1 one failed, 2 the suite could not run (no binary, no usable
PostgreSQL, or a binary built without the connectors).

| flag | default | |
|---|---|---|
| `scenario …` | all | `self a b c d e f g h i`, space or comma separated |
| `--seed N` | random, printed | every random choice (data, writer, kill moments) derives from it |
| `--scale X` | 1 | multiplies row and message counts |
| `--bin` | `$QUEEN_BIN`, else `server/target/debug/queen` | the broker |
| `--work DIR` | a new temp dir | data dirs, `logs/<node>.log` (every restart appended), `harness.log` |
| `--keep` | off | keep data dirs and the scenarios' tables (slots and publications are ALWAYS dropped) |
| `--rust-log` | `warn,queen-pg=info,…` | the brokers' `RUST_LOG` |

## The database

| variable | default |
|---|---|
| `QUEEN_PGCONN_E2E_HOST` / `_PORT` | `127.0.0.1` / `55432` |
| `QUEEN_PGCONN_E2E_SUPERUSER` (`_SUPERPASSWORD`) | `postgres` (trust) |
| `QUEEN_PGCONN_E2E_USER` / `_PASSWORD` | `queen_test` / `queen_test_pw` (LOGIN REPLICATION) |
| `QUEEN_PGCONN_E2E_DB` | `queen_test` (owned by the user) |
| `PSQL` | `psql` on PATH, else `/opt/homebrew/opt/postgresql@18/bin/psql` |

The server must run PostgreSQL ≥ 17 with `wal_level = logical` (the suite
refuses anything else). The role and the database are created when missing
(the superuser does it), so a fresh container is enough. CI:

```
docker run -d --name pg -p 55432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres:17 -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20
QUEEN_BIN=server/target/debug/queen python3 test/pgconn/run.py
```

Everything a run creates is named after a run id: schema `pgc_<rid>` (every
table, and the sinks' progress tables except in `d` and `e`, which use the
default `queen.sink_progress` — in `e` inside the second database), connectors
`pgc-<rid>-<scenario>-…`, slots and publications `queen_pgc_<rid>_…`, the
second database `pgc_<rid>_b`. Each scenario drops its slots, publications,
tables, database and progress rows when it ends, pass or fail (a leaked slot
pins WAL on a shared server); a run also drops what an earlier crashed run
left (inactive `queen_pgc_%` slots, `pgc_%` schemas and databases). Nothing
of another name is touched. Scenario `d` uses the default progress table
`queen.sink_progress`: it deletes its own rows, and drops the table and the
schema only when it created them and they are empty.

## What the broker runs with

Every node: a fresh `QUEEN_RAFT_DIR`, `QUEEN_BIND_ADDR=127.0.0.1`,
`QUEEN_SERVER_ID=<node name>` (the lease names it), one
`QUEEN_ENCRYPTION_KEY` (64 hex, the same on every node: the connector API
seals the password with it), `QUEEN_PG_LEASE_TTL_MS=3000` (the minimum),
`QUEEN_PG_RELOAD_MS=500` (the minimum), `QUEEN_PG_SHUTDOWN_GRACE_MS=10000`,
`QUEEN_PG_THREADS=2`, `QUEEN_PG_ALLOW_PRIVATE_NETWORKS=true`, the disk marks
at 99.9/99.8 %. Only `PATH HOME TMPDIR LANG LC_ALL USER LOGNAME` are
inherited, so a `QUEEN_*` variable in your shell cannot leak in. A cluster
adds `QUEEN_RAFT_REPLICATOR=openraft`, `QUEEN_RAFT_NODE_ID`, `_PEERS`,
`_LISTEN` and `_TOKEN`. Queues are created with `retentionEnabled: false`
before a connector starts.

## What "exact" means

**Source.** `checks.analyze()` takes every message of the source's queues,
read with wildcard pops (`autoAck`, a group of the suite's own, 64 partitions
a call — the pop answer splices the stored payload bytes, so a 30-digit
`numeric` or a `bigint` past 2^53 arrives exactly; `/api/v1/fetch` re-renders
payloads through f64 and is not used for values) and checks:

- every message is a §4.6 event (`op`, `table`, `key`, `after`, `lsn`, `seq`, `xid`);
- every `transactionId` is unique across the queues, and spelled
  `pg:<epoch>:<16 hex>:<seq>[.k]` (stream; `.k` is the k-th event of one
  change, the `c` of a key move), `pg:<epoch>:s:<16 hex>:<seq>` (snapshot row)
  or `pg:<epoch>:s:<16 hex>:f<seq>` (a FILL at a high watermark: a `u` carrying
  the unchanged-TOAST columns of a chunk row a window change dropped, no
  `xid`) — `events.rs`; another spelling is a note;
- per partition, the `lsn` field never goes back (commit order);
- every key lives in ONE partition (per-key order depends on it), and the
  partition is named as §4.6 renders the key (`|`/`\` escaping, `\N`, the
  `~sha256` form) — a naming deviation is a *note*, not a failure;
- replaying each partition in offset order — `c`/`r` put `after`, `u` (fills
  included) merges `after` keeping the `unchanged` columns, `d` deletes —
  equals `SELECT` of the table, column by column (numbers as exact decimals,
  timestamps as instants, jsonb as JSON);
- every offset `logStart..lastOffset` of every partition (from
  `POST /api/v1/partitions/changed`) was read exactly once;
- once it matches, 3 s later nothing new has arrived (a late duplicate would);
- one epoch only, unless the scenario resynced;
- sampled every 0.5 s during the run: the slot's `confirmed_flush_lsn` never
  passes the pointer's `lsn` (§4.4; the slot is read first, the pointer second,
  both only grow, so a sample can only err on the safe side).

Waiting is always on the data: the suite reads the queue again until the replay
matches or a deadline passes, then prints the differences (keys, columns, the
key's event history). Scenario `self` shows the checker is not vacuous: a hand-made
correct queue passes, and nine kinds of damage (duplicate id, lsn going back, a
lost insert, a lost delete, a wrong value, a key in two partitions, unchanged
TOAST without an earlier value, a delete before its snapshot row, an offset
twice) are each refused for the right reason; a dropped chunk row repaired by a
fill passes, and the same queue without its fill is refused.

**Sink.** The target table read back: the sum of every account in `d`
(Decimal arithmetic over what was pushed), `md5(row::text)` of every row against
the source table in `c`, `e`, `i`; then once more after `leaseSeconds` + a
margin, so a redelivered batch applied twice would show.

## Scenarios

| | what happens | what it shows |
|---|---|---|
| `self` | the checker on hand-made queues | see above |
| `a` | one node; `single` (bigint key; int, text, numeric, bool, jsonb with ints past 2^53, text[], timestamptz) and `comp` (composite key, region values with `\|` and `\`) with 20 000 rows; a writer runs multi-statement transactions (inserts, updates, deletes, key changes, the same row twice, insert+update+delete, multi-row updates) from before the connector is created, through the snapshot (250-row chunks) and 8 s after | snapshot + stream exactly once per key under concurrent writes (§4.5 watermarks), commit order per partition, key changes as `d` + `c`, values exact; the connector API never returns the password or its sealed form, and the broker log never holds it |
| `b` | as `a` with 100-row chunks, and `kill -9` of the broker 5 times, each restart on the same data dir: two kills inside the snapshot (when the pointer's `snapshot.rows` passes a seeded 15–35 % and 55–80 % of the rows), three in the stream (1–5 s apart) | a crash repeats at most the chunk/bundle in flight: same equality, no duplicate id, still one epoch, slot invariant held |
| `c` | three nodes; 10 000 rows (100-row chunks) and a writer; the source on whichever node takes the lease (read from `state.lease` and the KV row); `kill -9` the owner mid-snapshot (when `snapshot.rows` passes 20–60 %), restart it; `SIGTERM` the new owner, restart it; then a `cdc` sink on all three into a copy table | takeover after a crash within 2 × TTL + 10 s (time reported), pointer moving again; on SIGTERM the lease is **released** (seen gone before its own `expiresAt`) and taken at once; the queue is exact across both owners; the sink's copy == the source table and the work is spread over ≥ 2 nodes (`applied` per node) |
| `d` | one node; 20 000 messages over 200 partitions pushed first (`{"account_id","amount"}`, 1 in 200 with a 30-digit amount); a `sql` sink `UPDATE accounts SET balance = balance + $1::numeric WHERE id = $2::bigint` (batch 10, 2 workers, leaseSeconds 5); `kill -9` ×5 at progress 5–88 % (read from the progress table) | every balance equals its exact Decimal sum, still after leaseSeconds + 6 s; `queen.sink_progress` has one row per partition at its highest offset; the queue holds each push once |
| `e` | one node; table A in `queen_test` (jsonb, 30-digit numeric, bigint > 2^53 and negative, text[] with NULL/commas/quotes/braces, a TOASTed text column: 20–40 kB incompressible values stored out of line, `STORAGE EXTERNAL`, in one row of three); 500-row chunks (wide rows make each chunk read take tens of ms); two writers from before the source starts — multi-statement transactions updating mostly OTHER columns, and single-row updates of the TOASTed rows' other columns; a `cdc` sink into table B in a second database | B == A, every column (md5 of every row). On a difference it prints, per key, the columns and the key's queue history — a key with no `r` event whose `u` events left the TOAST column unchanged is the §4.5-step-4 hole (a chunk dropped the key for a stream change that never carried the value) |
| `f` | one node; snapshot done, some writes; the slot dropped as superuser (walsender terminated in the same breath); writes while it is gone (inserts, updates, deletes); `POST …/resync` | status error code `slot_lost`; the slot is NOT recreated while that stands; the resync makes a new epoch whose events alone replay to the table (gap writes and deletes included); ids unique across epochs |
| `g` | one node; `partitionBy: "single"`, `maxBundleMessages` 500; ONE `INSERT … generate_series` of 200 000 rows (`--scale 10`: the 2 M of PLAN §7; skipped when it would leave under 6 GB free); `kill -9` when `inTxn.done` passes 30–60 % | pushed in pieces (`inTxn.done` values seen), every id once, `seq` 0..n-1 once, one commit lsn, offsets in order; read with a streaming reader (no message kept) |
| `h` | one node; 950 messages over 20 partitions + 3 poison ones (`qty` < 0 against `CHECK (qty >= 0)`: first of p11, middle of p3, last of p7) into an `upsert` sink (maxAttempts 2) | the 3 poison messages are in `GET /api/v1/dlq?queue=…&consumerGroup=pg-<name>` once each, every other row exact (last write per key in partition order), progress at each partition's end (p7: its poison offset not written, §5.4 — a note if it is) |
| `i` | one node; a row with a 100 kB incompressible value stored out of line (snapshot path) and one inserted after (stream path); 100 updates of another column each; a `cdc` sink | the copy keeps both values byte for byte (length + md5), counter 100; reports how many update events carried the value as `unchanged`. A probe beyond §7 changes the KEY of a TOASTed row (`d` to the old partition, `c` to the new one): the copy's new row must keep the value; a loss is reported under *findings*, not as a failure |

Measured on the laptop with a debug build (2026-10-03, seed 4242): `self` 0 s,
`a` 24 s, `b` 35 s, `c` 38 s, `d` 36 s, `e` 37 s (about 150 s when it fails: it
waits 120 s for B to converge), `f` 20 s, `g` 16 s (200 000 rows), `h` 14 s,
`i` 3 s — under 4 minutes for the suite. `a` also passes against PostgreSQL
17.11 with SCRAM-SHA-256 password authentication (set
`QUEEN_PGCONN_E2E_PORT` and `QUEEN_PGCONN_E2E_SUPERPASSWORD`).

## Bugs this suite found (fixed, and what now guards them)

- **`e`: a TOAST value that a snapshot chunk dropped was never sent** (found
  2026-10-03, fixed the same day). §4.5 step 4 drops a chunk's key when a
  stream change of that key lands between the chunk's watermarks; when that
  change left an out-of-line TOAST column unchanged, no event ever carried the
  value and the `cdc` copy got NULL (5 and 17 rows in two runs: no `r` event,
  only `u` events with `"unchanged":["toast"]`). The source now emits a FILL
  at the high watermark (`u`, id `…:s:<hw>:f<seq>`) with the columns the
  dropped chunk row held and no event carried. `e` provokes it (wide values,
  500-row chunks, a writer aimed at the TOASTed rows) and prints how many of
  the `u` events are fills: 1-22 per run, B == A every time.
- **`i` probe: a key change of a TOASTed row lost the value** (same day). With
  `partitionBy: "key"` the move is a `d` to the old partition and a `c` to the
  new one, and pgoutput sends the new tuple's out-of-line column as
  unchanged. The source now reads the unchanged columns back for the new key,
  so the `c` carries them (a row already gone keeps them unchanged and a
  delete follows).

## Limits

- **One machine, a debug build.** Times (takeover, hand-over, snapshot) are
  reported, and asserted only against generous bounds that follow from the
  configuration. No network partitions, no clock skew, no slow disks; the
  database is never failed over (PG 17 failover slots are not exercised).
- **What isn't covered:** TLS to PostgreSQL (`sslMode` is `disable`),
  `onTruncate: emit`, `partitionBy` by explicit columns, unmanaged
  publications, `snapshot: never`, `append` mode, `metadata` columns,
  `subscriptionMode: new`, tenancy (every connector is the default tenant's),
  the egress policy with the embedded proxy, a database restored from a backup
  (`system_changed`), `slot_ahead`, `max_slot_wal_keep_size` invalidation,
  retention overrunning a sink.
- The reader's pops create one consumer group per read (`pgc-…`); they live in
  the scenario's broker, which is deleted with it.
