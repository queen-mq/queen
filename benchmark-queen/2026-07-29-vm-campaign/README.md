# 2026-07-29 VM campaign — load generator (Task L)

`goload/` is the load generator for the queen_proxy cloud campaign. It is the
**2026-06-04 goload with two modes added**, not a rewrite:

* `app.go`, `cm.go`, `tenants.go` are **byte-identical** to `2026-06-04/goload/`
  (`cmp` clean). `main.go` differs only by the two new dispatch cases and the
  header comment. So `-mode max` / `-mode openloop` / `-mode cm` still produce
  numbers directly comparable with the July baselines — same pacer, same
  log-linear latency histogram, same code paths.
* `cloud.go` + `verify.go` + `errkinds.go` — the new `-mode cloud` multi-tenant
  workload, delivery verifier and error classifier.
* `tenantfile.go` — `-mode provision`, real tenants via
  `queen_proxy.bootstrap_tenant`.

Rewriting from scratch would have thrown away the coordinated-omission-correct
pacer, the histogram and (most importantly) the July A/B, to re-earn the same
functionality. Extending `-mode tenants` in place was rejected for the opposite
reason: that mode's "tenant" is a queue-name prefix on one shared client, which
is not a tenant at all, and changing it would have broken the July comparison
for anyone re-running it.

## Build

```sh
./goload/build.sh            # host binary  -> goload/goload
./goload/build.sh linux      # static amd64 -> goload/goload-linux-amd64
```

`GOWORK=off` is **required** (build.sh sets it). This module sits inside the
repo tree, so the repo's `go.work` — which lists only `clients/client-cli` and
`clients/client-go` — claims the directory and plain `go build` fails with
*"current directory is contained in a module that is not one of the workspace
modules listed in go.work"*. With the workspace off, the module's own `replace`
directive resolves `../../../clients/client-go`, i.e. the loader is built
against exactly the client the campaign is measuring.

The VM has Go 1.23.4 while the module declares `go 1.24.0`; the toolchain
downloads `go1.24.0` on first build (cached afterwards, ~1s builds).

## `-mode order` (added for TASK M — the minimum pop wait)

`-mode cloud` proves loss / duplication / tenant isolation from per-tenant
bitmaps, but it deliberately does **not** check delivery ORDER: with several
producers pushing concurrently into one partition, producer-sequence order is
not the partition's storage order, so "delivered sequence must increase" would
fail on correct behaviour.

`-mode order` builds the one shape where storage order is known — a **single**
producer pushing **sequentially** (each push awaited before the next is issued),
so partition p's commit order is exactly s=1,2,3,… Consumers then run in the
shape under test (N of them, batch B, window W) and the run asserts, together:

* **order** — per partition, no delivered sequence ever goes backwards
* **loss** — every pushed sequence is delivered
* **duplication** — no sequence delivered twice
* **lease/ack** — every delivered message is acked and accepted
* **batching** — messages-per-pop histogram and pop-latency percentiles, i.e.
  what the window actually bought and what it cost

Exit 0 = all checks passed, 3 = a check failed, 1 = setup failure.

`-min-pop-wait N` (also added to `-mode cloud`) sets `minPopWaitTime` on the
queue in the raw configure options bag: client-go's `QueueConfig` struct has no
field for it, and the campaign configures the queue exactly the way a tenant
would rather than reaching into the database behind the broker's back.

## Exit codes

| code | meaning |
|------|---------|
| 0 | run completed, delivery verdict PASS |
| 1 | setup failure (bad credentials, configure refused, unwritable `-out`) — no numbers produced |
| 2 | bad flags |
| 3 | run completed, delivery verdict **FAIL** (loss / duplication / cross-tenant) |

`-fail-on-verify=false` turns 3 back into 0. Note that piping through `tail`
masks the code — use `${PIPESTATUS[0]}` or redirect to a file.

## Provisioning real tenants

```sh
./goload -mode provision -tenants 50 -file /root/campaign/tenants.json
./goload -mode provision -tenants 50 -prefix free -plan free -file /root/campaign/tenants-free.json
```

One psql batch per run (`docker exec -i cell-pxdb psql …`, override with
`-psql-cmd`). Tenant slug = cluster slug = `<prefix>-NNNN`; the **cluster slug is
the routing Host label**. The plaintext API key is returned exactly once, so it
is cached in the file and a re-run provisions only the missing indices.

The key name defaults to `bench-<unix>`, unique per provisioning run: a cluster
that already exists therefore still yields a usable key instead of the NULL
`bootstrap_tenant` returns for a repeated key name.

⚠️ The broker tenant UUID is fetched in a **separate statement**. A single
statement that joined `queen_proxy.clusters` would read a snapshot taken before
`bootstrap_tenant` inserted those rows — the join silently returns zero rows
while the bootstrap itself commits. (Observed, then fixed.)

## The invocation lines the later phases should use

Cell shape must be recorded with every number:
`curl -s localhost:6711/healthz` and `systemctl show queen-broker -p CPUQuotaPerSecUSec`.

**Through the proxy, shared-cell shape** (every tenant a real tenant, all on the
same queue name and consumer group — the shape that stresses the tenant-keyed
hot-list ring and the wake gates, and the one where cross-tenant delivery is
falsifiable):

```sh
./goload -mode cloud -target proxy -url http://127.0.0.1:6711 \
  -tenants-file /root/campaign/tenants.json -tenants 20 \
  -shared-queue -queue orders -group workers \
  -rate 2000 -push-batch 10 -consumers-per-tenant 2 -pop-batch 100 \
  -duration 300 -drain 30 -report 10 \
  -out /root/campaign/<experiment> -run-id <experiment>-proxy \
  -note "cell: --cell-cpus N --cell-mem G enforce=1"
```

**Direct to the broker (July-comparable A/B)** — same tenants, same
`x-queen-tenant` UUID the proxy would have injected, so both targets hit the
same broker-side tenant rows and the delta is the proxy hop alone:

```sh
./goload -mode cloud -target broker -url http://127.0.0.1:6632 \
  -tenants-file /root/campaign/tenants.json -tenants 20 \
  -shared-queue -queue orders -group workers \
  -rate 2000 -push-batch 10 -consumers-per-tenant 2 -pop-batch 100 \
  -duration 300 -drain 30 -report 10 \
  -out /root/campaign/<experiment> -run-id <experiment>-broker \
  -note "…"
```

Useful variants:

* `-shared-queue=false` — per-tenant queue names `<queue>-tNNNN` (the isolated
  shape; contrast with shared to price the shared-cell hot path).
* `-broker-tenant-header=false` — direct-to-broker with **no** tenancy header at
  all: one shared client, everything in the default tenant. The pre-tenancy
  control. Combined with `-shared-queue` it is the "no isolation" case and
  should report cross-tenant deliveries — that is the checker working, not a bug.
* `-per-tenant-rate X` instead of `-rate` when scaling tenant count with the
  per-tenant rate held constant.
* `-producers-per-tenant N` when one tenant exceeds ~2k req/s (a single pacer
  goroutine cannot time finer than the runtime timer).
* `-retry429-attempts 0` — let client-go do its built-in 429 backoff (bounded
  push / unbounded long-poll) instead of surfacing every 429. Default is `1`:
  the campaign **measures** 429s rather than hiding them in silent retries.
* `-verify=false` — drop the per-message bitmaps for an extreme ceiling probe.
  The run then prints `UNVERIFIED` and proves nothing about delivery.

## TASK M scripts (minimum pop wait)

| script | what it does |
|--------|--------------|
| `m-usebin.sh base\|M` | installs one of the two archived builds as the broker binary and restarts it. `base` = pre-feature (what B1/B2/B3 measured), `M` = adds `min_pop_wait_time`. Both are kept under `/root/campaign/M/bin` so any number can be re-attributed to a build by hash. |
| `phaseM.sh` | the measured sweep: M0 parity (base vs M with the option off), M1 window sweep at a fixed near-ceiling load, M2 ceiling with the window on, M3 low load, M4 fault-injection proof. Every point resets the DB and restarts the broker first. |
| `phaseM-order.sh` | the correctness gate — `-mode order` across window / consumer count / batch / deadline. |
| `msum.py` | the TASK M table: window, throughput, latency, CPU, **commits per delivered message**, pop/ack **calls** per message from pg_stat_statements, and the broker's own fill-wait counters. Reuses `ptsum.summarise`, so CPU and commit accounting are defined in exactly one place. |

The broker exports two counters for this feature, both 0 on every queue with
`minPopWaitTime = 0`: `queen_pop_fill_wait_total` (pops that held an under-full
batch back) and `queen_pop_fill_wait_microseconds_total` (time they spent doing
it). Their ratio is the average window actually paid.

## Artifacts per run

* `<out>/<run-id>.json` — config, offered/achieved rates, latency percentiles
  (e2e from schedule = coordinated-omission-correct, e2e from send, push RTT,
  ack RTT: p50/p95/p99/p999/max), errors by kind **and** by proxy code with
  Retry-After stats, nominal bytes, per-tenant delivery verdicts.
* `<out>/<run-id>-interval.csv` — per-report-interval time series with a
  `phase` column (`load` / `drain`). Error columns are cumulative counts.

## Correctness (the point of the whole thing)

Every message carries `(tenant idx, monotonic seq, scheduled µs, sent µs)`. The
producer records the sequences the server accepted; consumers record what they
received. After the producers stop, consumers keep draining for `-drain`, then
the two bitmaps are diffed per tenant:

* **missing** = sent, never delivered → loss
* **duplicate** = a sequence delivered more than once
* **extra** = delivered but never recorded as sent (e.g. a push that timed out
  client-side but landed)
* **cross** = a message whose payload tenant ≠ the consumer's tenant → **tenant
  isolation breach**

In-flight pushes are deliberately *not* cancelled when producers stop (they run
under the run context, not the producer context): killing a request the broker
may already have committed would manufacture a spurious "extra".

`missing > 0` is only loss if the consumers had actually finished; the summary
says so explicitly, and warns when deliveries were still arriving at the cutoff
or the consume side was being refused (a rate-limited tenant needs a `-drain`
sized to the rate its plan permits).

### Proving the checker can fail

`-fault dup-push=N,lose-msg=N,drop-ack=N` injects a deliberate discrepancy:

* `lose-msg` — mark a message sent, never push it → must surface as **missing**
* `dup-push` — push the same `(tenant, seq)` twice → must surface as **duplicate**
* `drop-ack` — receive a batch and never ack it → redelivery after `leaseTime`

It is a flag rather than a temporary code edit, so the checker can be re-proved
at any point in the campaign without touching the binary under test.
