# HANDOFF queen-kafka: brief for the next agent

You are picking up the Kafka wire-protocol front for Queen. This file is
self-contained: read it, then PLAN_QUEEN_KAFKA.md (goal, architecture, STATUS
with the full inventory), then queen-kafka/README.md. The webdoc pages
`/reference/kafka` (support matrix, generated from versions.rs) and
`/deploy/kafka` (operator/config reference) and queen-kafka/compat/README.md
and compat/ERRORS.md complete the picture. Everything below was true on
2026-08-28, branch `queen-kafka`.

## What is done

M0-M6 and the two server core changes C1/C2 are implemented and verified:

- C1: `POST /api/v1/push` responses carry the assigned absolute offset per
  result. C2: `POST /api/v1/fetch`, a batched, non-destructive, tenant-scoped,
  long-polling read-from-offset. Both additive, both in server/ + the protocol
  crate.
- The facade crate `queen-kafka/`: framing + serial muted dispatch, ApiVersions
  (with the v0 fallback quirk), Metadata + auto-create, Produce (all four
  codecs, budgeted decompression), Fetch capped at v6 + ListOffsets, the classic
  consumer-group coordinator (actor per group, deterministic paused-time test
  suite), offsets durable in Queen KV, TLS + SNI, SASL/PLAIN (password = Queen
  bearer token), 429 -> throttle_time_ms.
- Verified: 454 crate tests + clippy `-D warnings` + fmt clean; server 1122;
  protocol 173; client-rust 543. Live rig 26 tests green including a SIGKILL
  facade restart with offset resume. Client matrix all PASS: franz-go, kafkajs,
  librdkafka (kcat + confluent-python), Java kafka-clients 3.9.1. Differential
  vs apache/kafka:3.9.1: 47 divergences, all classified (28 deliberate, 19
  accepted), zero unexplained.

How to re-verify (there is NO cargo workspace; every crate builds on its own
manifest):

```
cd crates/queen-protocol && cargo test --locked
cd server && cargo build && cargo test --locked        # SQL is include_str!-embedded: rebuild after ANY .sql edit
cd clients/client-rust && cargo test --locked
cd queen-kafka && cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
queen-kafka/compat/rig.sh --m5 -count=1 -v             # live e2e; needs Docker + Go
```

The differential runner lives in queen-kafka/compat/differential (own ports
25543/26699/29192/29092; it exits 1 on any divergence not in its accepted
table, so it is a future gate).

## What needs doing, in priority order

1. **Unblock Cloud consume (the one real blocker).** `classify` in
   proxy/src/routes.rs has no arm for `POST /api/v1/fetch`, so the proxy 404s
   the entire consume path; offset commits go to `/api/v1/kv/*`, which is
   `Gated(Feature::Kv)`. Add the fetch arm (read-only, tenant-scoped) and
   decide the offset-path gating/metering with Alice. CAUTION: proxy/ carries
   her unrelated uncommitted work; touch only what you must.
2. **sourceoftruth debt.** `pnpm --dir webdoc check:sourceoftruth` is RED: 126
   stale pairs over 69 pages, 120 caused by the spike commit 33fbc8d6 touching
   server files. Each page needs re-reading (or a recorded exemption; Alice
   considers exemptions debt). Everything else in webdoc is green.
3. **One stale paragraph on `/reference/kafka`**: it still says groups are
   keyed by (credential, group id); identity.rs now keys by (tenant, group id)
   with credential fallback. `/deploy/kafka` already says it right.
4. **Make tenant identity real in Cloud.** identity.rs resolves via
   `GET /auth/me`, but no deployed surface answers a bearer there (broker only
   with JWT off, proxy is cookie-only), so it falls back to per-credential keys.
   Needs a bearer-authenticated identity endpoint on the proxy.
5. **C3, the native cursor**: mirror or move Kafka group offsets into Queen's
   consumer-group cursor so the console lag views show Kafka groups.
6. **Packaging**: no Dockerfile, no build.sh entry, no Helm chart (stated on
   /deploy/kafka). Also .github/workflows/docs.yml does not trigger on
   queen-kafka/src/versions.rs, which the new webdoc generator reads.
7. **Deviations awaiting Alice's ratification** (documented in code, decide,
   do not silently change): produce log_start_offset = -1 (Kafka says 0);
   heartbeat during CompletingRebalance answers NONE; offsets never expire and
   there is no DeleteGroups.
8. **M7 backlog** (see the plan): `queen.dedup=key`, DLQ pseudo-topics,
   CreateTopics/DescribeConfigs, the idempotent-producer sequence window,
   raw-bytes payload mode (today a native consumer of a Kafka-written queue
   sees the base64 envelope), fetch partition rotation, the cycle re-key recipe
   (docs only). Eventually: promote the compat lane and the differential runner
   into CI once boring, per the release-CI rule below.

## Traps that cost hours, learned the hard way

- **The working tree carries UNRELATED uncommitted work.** Never run
  git checkout/reset/stash/clean. Commit NOTHING; Alice commits.
- **localhost:5432 is a live stack.** Never touch it. Throwaway postgres:16 on
  55432 (rig) or 2xxxx ports (differential); always tear down.
- **Go compat suite**: GOWORK=off is mandatory (root go.work excludes the
  module) and `go test` MUST get `-count=1` or it silently replays cached
  results.
- **versions.rs is the compat contract.** Do not widen a version range without
  re-running the client matrix. The Fetch v6 cap is deliberate (no fetch
  sessions) and has a known consequence: librdkafka silently does not compress
  zstd. Kafka 4.0 (KIP-896) drops several versions the facade caps at; the
  differential oracle is pinned to 3.9.1 on purpose.
- **kafka-protocol 0.18 loses duplicate header names** (IndexMap). wire.rs owns
  the record-section encode/decode for exactly this reason; keep it in the loop.
- **Clients need idempotence off**: Java `enable.idempotence=false`, franz-go
  `kgo.DisableIdempotentWrite()` (InitProducerId is unimplemented until M7).
- **Server crate has PRE-EXISTING clippy/fmt debt** (~55 errors, ~2100 fmt
  diffs) in files this work never touched. Do not fix globally; keep your own
  files clean.
- **A cold apache/kafka container answers NOT_COORDINATOR for its first
  minute**; the differential group scenario needs the warmup the runner does.
- **Webdoc editorial rules**: no em dashes in webdoc prose; generators derive
  reference pages from Rust source and are fingerprint-pinned; the compat lane
  stays OUT of release-day CI until it has been boring for a while.
- The deliberate-deviations list in the plan's STATUS is authoritative. A
  divergence from Apache Kafka that is on it is not a bug; do not "fix" it
  without Alice's sign-off.

## Verification culture to keep

Every change here was landed with: unit tests in the crate, the live rig run
from a clean database, adversarial review of fresh code, and real clients as
the final judge. Keep that bar: a milestone is done when a real client proves
it against a running broker, not when the unit tests pass.
