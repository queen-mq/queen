# compat/embedded — queen-kafka EMBEDDED MODE, measured

**Embedded mode** is a switch on the **broker**, not on the facade:
`QUEEN_KAFKA_EMBEDDED=true` makes the Queen broker run `queen-kafka`
**in-process** — the library linked into the broker binary (the `kafka` cargo
feature, on by default), on its own tokio runtime whose threads are named
`queen-kafka`, calling the broker through its router instead of over a socket.
One deployment, one process, one binary.

The glue is `server/src/kafka_inproc.rs`. This directory is its live acceptance
suite.

## What it is, and what it is not

The facade reads the same `QUEEN_KAFKA_*` environment it reads when it is
deployed on its own, through the same `queen_kafka::boot::Config`, so every knob
keeps its meaning. What changes is the transport (the broker's router, called as
a service), the threads, and the blast radius: a panic on a facade thread kills
that task only and the broker keeps serving; a serve loop that ends on its own
is restarted with a 1 s doubling to 30 s ladder.

It is not a child process. The child-supervising mode this suite used to
measure (`server/src/kafka_facade.rs`) belonged to the Postgres-backed broker
and went with it. An explicit `QUEEN_URL` still keeps the in-process facade on
HTTP to that URL — the Cloud hairpin through a proxy — and `/status` then says
`"transport": "http (explicit QUEEN_URL)"`.

## Run it

The rig stands the whole shape up, runs the suite and tears it down:

```sh
protocols/queen-kafka/compat/embedded/rig-embedded.sh
protocols/queen-kafka/compat/embedded/rig-embedded.sh --keep      # leave the stack up
```

It builds the broker only (the facade is linked in) and starts **three
brokers**, each one raft node on its own throwaway data directory: one with the
facade embedded, one with no `QUEEN_KAFKA_*` at all (the default-off
regression), and one whose facade configuration is invalid (the boot refusal).
Ports are `32601`/`32602` the embedded broker and its Kafka listener, `32603`
the default-off broker, `32604`/`32605` the refused one — never `6632`, which is
a live stack on a developer machine. Override with `BROKER_PORT`, `KAFKA_PORT`,
`OFF_PORT`, `REFUSED_PORT`, `REFUSED_KAFKA_PORT`. The brokers' disk gate runs at
`QUEEN_RAFT_DISK_HIGH_PCT=99.5` unless that variable is set: a throwaway rig on
a developer disk is not what the gate (85 by default) protects.

To run the suite against a stack that is **already up**, use `run.sh` and give it
addresses:

```sh
QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:32602 \
QUEEN_BROKER_URL=http://127.0.0.1:32601 ./run.sh
```

| variable | meaning | default |
| --- | --- | --- |
| `QUEEN_KAFKA_BOOTSTRAP` | the embedded facade's `host:port` | **required** |
| `QUEEN_BROKER_URL` | the broker it runs in | **required** |
| `QUEEN_BROKER_PIDFILE` | a file holding the broker's current pid | unset ⇒ the one-process check and shutdown **skip** |
| `QUEEN_BROKER_RESTART_CMD` | SIGKILLs the broker and starts it again on the same data directory, rewriting the pidfile | unset ⇒ the crash-and-resume scenario **skips** |
| `QUEEN_BROKER_LOG` | the broker's log file | unset ⇒ the log assertions **skip** |
| `QUEEN_EMBEDDED_SHUTDOWN` | `1` opts in to the DESTRUCTIVE shutdown scenario | `0` |
| `QUEEN_KAFKA_PARTITIONS` | the facade's `QUEEN_KAFKA_DEFAULT_PARTITIONS` | `8` |
| `QUEEN_KAFKA_GRACE_MS` | the broker's `QUEEN_KAFKA_SHUTDOWN_GRACE_MS` | `5000` |

`kcat` is the client, on purpose: it is librdkafka, i.e. one of the real clients
the compat matrix already pins, and it needs no Go module to run one produce and
one grouped consume.

## What each scenario proves

| # | scenario | the claim it earns |
| --- | --- | --- |
| 1 | `GET /status` carries a `kafka` block with `"mode":"in-process"`, `phase` `running` and `transport` `in-process` | an operator can see the facade from the endpoint they already use, with no new API surface and no auth token |
| 2 | the broker has no child process, and (with `lsof`) the Kafka port is held by the broker's own pid | there is no second process to deploy, supervise or orphan |
| 3 | produce and grouped consume through the Kafka port | the in-process transport is real end to end |
| 4 | **SIGKILL the broker** and restart it on its data directory → new pid, the facade back, and the same group resumes at m9 with no replay and no gap | what a crash leaves: the records and the committed offsets are in the broker's replicated log, not in the facade |
| 5 | the broker's log carries `starting the Kafka facade in-process` and no unplanned facade restart | the facade is visible in the broker's own log stream |
| 6 | **SIGTERM the broker** → it exits within the grace window and nothing listens on the Kafka port | no port left held against the next start |

And in `rig-embedded.sh`, two more that need a broker of their own:

| scenario | the claim it earns |
| --- | --- |
| **default-off** — a broker with no `QUEEN_KAFKA_*` | `/status` is byte-identical to the string it always answered, there is no `kafka` line anywhere in the boot log, and there is no child. A feature nobody opted into changed nothing |
| **boot refusal** — `QUEEN_KAFKA_DEFAULT_PARTITIONS=0`, which the facade refuses | the broker exits non-zero before its listener binds, naming the variable, and holds neither port. In-process there is no child to crash-loop: a configuration the facade cannot run on is a broker that does not start |

## Status

Rewritten on 2026-09-26, when the Postgres-backed broker and its child mode
were removed: nothing in this directory has been run against the in-process
facade yet, so read it as unratified until somebody runs the rig and says so
here.
