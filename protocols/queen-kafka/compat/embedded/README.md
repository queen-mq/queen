# compat/embedded — queen-kafka EMBEDDED MODE, measured

**Embedded mode** is a switch on the **broker**, not on the facade:
`QUEEN_KAFKA_EMBEDDED=true` makes the Queen broker process spawn `queen-kafka`
as a supervised **child process**, wired to its own HTTP listener over loopback.
One deployment, two processes, isolation preserved — the facade keeps its own
accept loop, its own connection budget and its own crash modes, and when it dies
the broker keeps serving and starts it again.

The supervisor is `server/src/kafka_facade.rs`. This directory is its live
acceptance suite.

## What it is not

It is not in-process embedding. Nothing links the facade into the broker, and
nothing about the facade changes: the child reads the same `QUEEN_KAFKA_*`
environment it reads when it is deployed on its own, because it inherits the
broker's environment verbatim. The only variable the supervisor sets is
`QUEEN_URL`, pointed at the address the broker's listener actually bound.

## Run it

The rig stands the whole shape up, runs the suite and tears it down:

```sh
protocols/queen-kafka/compat/embedded/rig-embedded.sh
protocols/queen-kafka/compat/embedded/rig-embedded.sh --keep      # leave the stack up
```

It starts **one throwaway Postgres** and **three brokers**: one with the facade
embedded, one with no `QUEEN_KAFKA_*` at all (the default-off regression), and
one whose child cannot boot (the crash-loop). Ports are `32600` Postgres,
`32601`/`32602` the embedded broker and its Kafka listener, `32603` the
default-off broker, `32604`/`32605` the crash-loop one — never `5432`, which is a
live stack on a developer machine. Override with `PG_HOST_PORT`, `BROKER_PORT`,
`KAFKA_PORT`, `OFF_PORT`, `LOOP_PORT`, `LOOP_KAFKA_PORT`.

To run the suite against a stack that is **already up**, use `run.sh` and give it
addresses:

```sh
QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:32602 \
QUEEN_BROKER_URL=http://127.0.0.1:32601 ./run.sh
```

| variable | meaning | default |
| --- | --- | --- |
| `QUEEN_KAFKA_BOOTSTRAP` | the embedded facade's `host:port` | **required** |
| `QUEEN_BROKER_URL` | the broker that spawned it | **required** |
| `QUEEN_BROKER_PID` | the broker's pid | unset ⇒ parentage and shutdown **skip** |
| `QUEEN_BROKER_LOG` | the broker's log file | unset ⇒ the log assertions **skip** |
| `QUEEN_EMBEDDED_SHUTDOWN` | `1` opts in to the DESTRUCTIVE shutdown scenario | `0` |
| `QUEEN_KAFKA_PARTITIONS` | the facade's `QUEEN_KAFKA_DEFAULT_PARTITIONS` | `8` |
| `QUEEN_KAFKA_GRACE_MS` | the broker's `QUEEN_KAFKA_SHUTDOWN_GRACE_MS` | `5000` |

`kcat` is the client, on purpose: it is librdkafka, i.e. one of the five real
clients the compat matrix already pins, and it needs no Go module to run one
produce and one grouped consume.

## What each scenario proves

| # | scenario | the claim it earns |
| --- | --- | --- |
| 1 | `GET /status` carries a `kafka` block with `phase`, `pid`, `restarts`, `lastExit` | an operator can see the child from the endpoint they already use, with no new API surface and no auth token |
| 2 | `pgrep -P <broker>` lists the facade's pid | it really is a child process, not a sidecar someone remembered to deploy |
| 3 | produce and grouped consume through the Kafka port | the loopback wiring is real: the child reached the broker at the address the supervisor handed it |
| 4 | **SIGKILL the child** → new pid, `restarts` +1, `lastExit=signal 9`, and the same group resumes at m9 with no replay and no gap | the supervision claim. A facade asked politely to stop proves nothing about a crash; what survives is not in the facade at all, because the offsets live in Queen |
| 5 | the broker's log carries the spawn, the exit at ERROR with its backoff, and the child's own output tagged `stream="stdout"` | the child is visible in the broker's log stream, attributed, and rate-guarded |
| 6 | **SIGTERM the broker** → the child is gone within the grace window and nothing listens on the Kafka port | no orphan, and no port left held against the next start |

And in `rig-embedded.sh`, two more that need a broker of their own:

| scenario | the claim it earns |
| --- | --- |
| **default-off** — a broker with no `QUEEN_KAFKA_*` | `/status` is byte-identical to the string it always answered, there is no `kafka` line anywhere in the boot log, and there is no child. A feature nobody opted into changed nothing |
| **crash-loop** — `QUEEN_KAFKA_DEFAULT_PARTITIONS=0`, which the facade refuses to boot on | the backoff ladder `1000 2000 4000 8000 16000` in the log, five exits in twenty seconds instead of hundreds, `/status` reporting `restarting`, and the broker serving its own HTTP throughout. It also proves the passthrough: the broker never reads that variable, it only forwards it |

## What is guaranteed when the broker dies, per platform

Measured, not assumed. The child is put in its own process group, so a stop
signals the whole group.

| the broker gets | the child | why |
| --- | --- | --- |
| SIGTERM / Ctrl-C | **gone, guaranteed** | `main` awaits the supervisor's shutdown after the serve loop drains: SIGTERM to the group, SIGKILL after `QUEEN_KAFKA_SHUTDOWN_GRACE_MS` |
| a panic or an unexpected drop | gone | tokio's `kill_on_drop` |
| **SIGKILL, on Linux** | **gone, guaranteed** | the child sets `PR_SET_PDEATHSIG(SIGKILL)`; verified in the container — SIGKILL the broker and no queen process remains, the Kafka port stops answering |
| **SIGKILL, on macOS/BSD** | **survives, orphaned** | there is no `PR_SET_PDEATHSIG` equivalent. Verified on the host: the facade keeps running with PPID 1 and keeps the Kafka port bound. A dev-machine caveat, not a production one |

## In the Docker image

The broker image ships both binaries in `/app/bin`, and the supervisor resolves
the child from the directory of its own executable, so embedded mode needs no
`QUEEN_KAFKA_BIN`:

```sh
docker run -p 9092:9092 -p 6632:6632 \
  -e PG_HOST=... \
  -e QUEEN_KAFKA_EMBEDDED=true \
  -e QUEEN_KAFKA_ADVERTISED_ADDR=kafka.example.com:9092 \
  queen-mq
```

`QUEEN_KAFKA_ADVERTISED_ADDR` has no default and the broker refuses to boot
without it, because a container that advertises its own internal address is a
bootstrap that succeeds and a produce that hangs. To run the facade alone from
the same image, override the command: `docker run ... queen-mq ./bin/queen-kafka`.

## The rig.sh wiring this would take

`compat/rig.sh` is not edited by this work, and here is exactly what would be
added to it if it were.

**One flag, `--embedded`**, alongside `--m5`. With it set, the broker section
gains four variables and the facade section is not run at all:

```sh
# ... the existing broker launch, plus:
QUEEN_KAFKA_EMBEDDED=true \
QUEEN_KAFKA_BIN="$REPO_ROOT/protocols/queen-kafka/target/debug/queen-kafka" \
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
  "$REPO_ROOT/server/target/debug/queen" > "$BROKER_LOG" 2>&1 &
BROKER_PID=$!
```

`QUEEN_KAFKA_BIN` is explicit only because the two debug binaries live in two
target directories; the image's zero-configuration path is the default one.

Three consequences, each of which is the whole reason this is a note rather than
a patch:

1. **The facade has no pid of its own to keep.** `$FACADE_PIDFILE` and
   `$FACADE_START` exist because the suite RESTARTS the facade itself. Under
   `--embedded` the supervisor owns that, so `start-facade.sh` becomes
   `restart-facade.sh`: read the current child from
   `curl -s $BROKER_URL/status`, `kill -9` it, then poll `/status` until the pid
   CHANGES. Same test, same guarantee, one less thing for the rig to own — and
   it is a stronger assertion, because the restart is now the product's job
   rather than the harness's.
2. **Teardown gets shorter, not longer.** `kill $BROKER_PID` takes the facade
   with it (scenario 6 above). The `kill -9` backstop stays, for a broker that
   never reaches its own shutdown.
3. **`--m5` and `--embedded` are compatible but not the same lane.** The M5
   listener is a SECOND facade with TLS and SASL; embedded mode supervises ONE
   child. Running both means the rig keeps its own TLS facade exactly as it does
   today, and only the plaintext one moves under the broker.

The whole of `compat/go` then runs unchanged against `127.0.0.1:$KAFKA_PORT`,
which is the point: embedded mode changes how the facade is deployed and nothing
about what it speaks.
