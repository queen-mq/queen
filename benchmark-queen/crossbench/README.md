# CM-BENCH — cross-broker channel-manager benchmark

Runs the **same channel-manager workload** — high-cardinality per-key ordering,
three hops, fan-out to five consumer groups, consumers doing real work — against
QueenMQ, Apache Kafka, RabbitMQ and pgmq, and answers one question:

> At a fixed workload and a fixed correctness bar, on identical hardware, what
> does each system **cost to serve**?

Read [SPEC.md](SPEC.md) first. It is the contract: topology, invariants,
correctness criteria and the fairness rules a reviewer should check before
believing any number here.

---

## Why it is built this way

**One workload, four systems, identical code.** The pacer, the stage logic, the
work sleeps, the recorder and the verifier live in `internal/workload`,
`internal/runner` and `internal/verify` and know nothing about any broker.
Each system is a `broker.Broker` adapter. So a difference between two runs is
the system's, not the harness's.

**The interface is the application's need, not a broker's mechanism.** Adapters
implement ordered batch delivery per key, explicit batch ack, and keyed ordered
publish. How a system gets there is its own business, the best-known idiom must
be used, and the machinery it takes is a *result*: partitions, queues, members,
connections, publishes-per-event and application code it forces you to write all
land in the cost table.

**There is a control.** `-system mem` is an in-memory reference broker with
perfect semantics. A run against it must verify PASS with zero defects. Run it
first on any new machine: without it, a FAIL on Kafka could always be the rig.

---

## Quick start

```bash
make build          # -> ./cmbench
make control        # the experiment's control; must PASS
```

Bring up one system and run against it:

```bash
docker compose -f deploy/docker-compose.pgmq.yml -p cmbench-pgmq up -d
./cmbench -system pgmq -pgmq-dsn 'postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
          -rate 5000 -properties 1000 -duration 1200
```

Every run writes to `-logdir`: twelve stage logs, `produced.meta`, `run.log` and
`result.json`. Re-verify any of them offline, on another machine, by someone who
did not run it:

```bash
./cmbench -verify-only ./cmlogs -properties 1000
```

Exit codes: `0` PASS, `3` correctness FAIL (a legitimate, publishable result),
anything else an error.

---

## The full campaign

Start the sampler on the broker VM and on the loader VM, then drive from the
loader:

```bash
./scripts/sampler.sh cmbench-pgmq > broker-samples.csv     # on the broker VM
./scripts/run-campaign.sh pgmq --pgmq-dsn 'postgres://…'   # on the loader VM
```

`run-campaign.sh` runs the three sweeps of SPEC.md §6: cost-to-serve at fixed
rate, the correctness-bounded ceiling, and the cardinality sweep that separates
cost-per-message from cost-per-lane.

---

## Systems

| flag | brings up | notes |
|---|---|---|
| `-system mem` | nothing | the control. Perfect semantics, must PASS. |
| `-system queen` | `deploy/docker-compose.queen.yml` | Uses the **published** `ghcr.io/queen-mq/queen:latest`, not a local build: the home team running a bespoke binary against everyone else's release artefact is not a comparison. **Run it twice**, `-queen-pop-mode wildcard` and `targeted`, and publish the pair (SPEC.md §5.5). Broker-side dedup is set through `/api/v1/configure`, so `-dedup-window` works here. |
| `-system pgmq` | `deploy/docker-compose.pgmq.yml` | **Pin `ghcr.io/pgmq/pg17-pgmq`.** Measured 2026-08-02: `quay.io/tembo/pg17-pgmq` ships pgmq 1.5.1 with no grouped read at all, so it cannot express per-key ordering. The adapter's preflight refuses to start on such a build rather than producing an unordered benchmark. |
| `-system kafka` | `deploy/docker-compose.kafka.yml` | Set `KAFKA_ADVERTISED_HOST` to the broker VM's IP, and keep `KAFKA_PORT` equal to the published port — the advertised listener is what clients reconnect to. |
| `-system rabbit` | `deploy/docker-compose.rabbit.yml` | Needs `12 × lanes` queues. At one property per lane that is 12 000 queues; use `-lanes 100` and report the head-of-line blocking as its cost. |

`deploy/postgres.conf` is shared **byte-identical** by the Queen and pgmq runs.
That identity is what makes those two comparable; change it for one side only
and the comparison is void.

---

## Things that will bite you

These all cost real debugging time on 2026-08-02 and are now guarded in code,
but the guards only fire if you let them:

- **A run that delivers nothing used to PASS.** An empty log has no gaps and no
  order violations. The verifier now fails any stream that recorded zero
  messages. If you see `FAIL(empty)`, the pipeline is broken, not clean.
- **Postgres prunes unreferenced CTEs.** A `WITH … SELECT 1` wrapping
  `pgmq.send_batch` succeeds at full speed and inserts nothing. Every CTE in
  the pgmq publish path is referenced by the final SELECT on purpose.
- **pgx defaults to TLS.** Add `?sslmode=disable` to a plain DSN, or the
  preflight dies with a confusing EOF.
- **A replaced `postgresql.conf` drops `listen_addresses`.** Postgres then binds
  loopback only and the published port resets connections. It is set explicitly
  in `deploy/postgres.conf`.
- **Durability tiers are not equal.** Kafka's defaults do not fsync per write;
  Postgres `synchronous_commit=on` does. Run Kafka in both tiers and never put
  them in one table without saying which is which (SPEC.md §5.3).

---

## Layout

```
SPEC.md                     the contract — read first
Makefile                    build / test / control  (GOWORK=off: standalone module)
cmd/cmbench/                CLI + result.json writer
internal/workload/          topology, invariants, stamp, pacer, producer, recorder
internal/runner/            stage logic, run orchestration, 1 Hz report, summary
internal/verify/            the verifier — a faithful port of the July cm.go judge
internal/broker/            the adapter contract
internal/broker/mem/        control: perfect in-memory broker
internal/broker/kafka/      franz-go + the parallel-consumer pattern
internal/broker/rabbit/     amqp091, direct exchange per group, one consumer per lane
internal/broker/pgmq/       pgx, read_grouped_rr, fan-out materialised per group
deploy/                     compose files, shared postgres.conf, rabbitmq.conf
scripts/sampler.sh          1 Hz cgroup/disk/net sampler
scripts/run-campaign.sh     the three sweeps
```

## Tests

```bash
make test          # verifier semantics, workload invariants, harness control
make test-short    # skip the timed end-to-end runs
```

The verifier tests are the important ones: they pin gap detection, the
redelivery-vs-reorder distinction, the per-property clamp that stops a negative
term cancelling a real gap, and the empty-stream guard. If the judge is wrong,
every number in the campaign is wrong.
