# compat/transactions

The acceptance suite for **M9, Kafka transactions by buffer-and-commit**. Every
check is driven by a real client library, and every one of them names something
observed rather than something believed.

The design it settles is `scratchpad/design-transactions/DESIGN.md`; the check
letters below are its section 8.2.

```
queen-kafka/compat/transactions/run.sh            # the whole suite
queen-kafka/compat/transactions/run.sh s1 s3      # only those scenarios
queen-kafka/compat/transactions/run.sh --keep     # leave the stack up
```

It brings up its **own** stack and tears it down on every exit path, including a
failure and a Ctrl-C: a throwaway Postgres on 32910, a debug broker on 32911 and
three debug facades. Three, because three checks need a facade configured
differently from the others, and reconfiguring one mid-suite would make every
earlier result unreproducible.

| port | facade | why it is its own process |
| --- | --- | --- |
| 32912 | single node, 4 partitions | the facade under test, and the one that gets SIGKILLed |
| 32913 | `QUEEN_KAFKA_NODE_ID=1` | the cluster gate is read from CONFIGURATION, so it cannot be turned on in place |
| 32914 | caps at their floor, 70 partitions | `QUEEN_KAFKA_TXN_MAX_BYTES=65536` makes the byte cap reachable in a second instead of in eight megabytes |

Ports 32910 to 32914 and the container name `qkt-acc-pg` are its own and are
overridable (`PG_HOST_PORT`, `BROKER_PORT`, `KAFKA_PORT`, `KAFKA_CLUSTER_PORT`,
`KAFKA_TIGHT_PORT`, `CONTAINER`), so it runs beside `compat/rig.sh` without
touching it.

Requires docker, cargo, go and a JDK 17 or newer. The `kafka-clients` jars are
fetched from Maven Central on first use and cached outside the repository, the
same way `compat/java-matrix` does it; point `JARS_CACHE` at a populated
directory to run with no network at all.

## The scenarios

| id | client | what it settles | design |
| --- | --- | --- | --- |
| `s2` | kafka-clients | `initTransactions()` returns in well under a second. The campaign measured a **20 s** hang here, which was `max.block.ms` expiring while the client retried a retriable `FindCoordinator` refusal. | A1 |
| `s1` | kafka-clients | commit visibility at both isolation levels, abort invisibility, and the `read_uncommitted` divergence: a consumer sees **less** than Kafka would show it, never more. The log advances by N and not N+1, because there is no commit marker. | A2, A3 |
| `s3` | kafka-clients | fencing. Two producers, one `transactional.id`; the loser's commit raises `ProducerFencedException` **and the partitions are read** to prove it wrote nothing. | A4 |
| `s8` | kafka-clients | the idempotent, non-transactional producer is untouched: real contiguous offsets, not the staged -1. | A11 |
| `s6` | kafka-clients | the stage caps, none of which has a Kafka analogue at all. Byte cap, `transaction.timeout.ms` above the cap, the timeout sweep, and the 62-offset cap. | A9 |
| `s7` | kafka-clients | the cluster gate: fatal and fast, not a hang. | A1 |
| `s4` | kafka-clients | crash mid-transaction. SIGKILL after 500 sends, restart, and nothing partial is in the log. | A5 |
| `eos` | franz-go | exactly-once consume-transform-produce with an induced kill **between the last produce and the commit**. The decisive check of the whole design. | A6 |
| `go` | franz-go | a quick `compat/go` run, for the regression half of A11. | A11 |

`s4` and `eos` restart the facade through the same generated script the rig
uses, so "the same facade, restarted" is the same command line by construction.
The pid comes from a file that script writes, never from a port lookup.

## What a red run means

The suite exits non-zero when any check fails, when a facade log contains a
panic, or when one contains an `ERROR`. The last is deliberate and it is not
strictness for its own sake: every `tracing::error!` on the transaction path is
on a branch whose own comment calls it unreachable, so one appearing means an
assumption is wrong. `WARN` lines are summarised and are not a failure, because
a fenced producer and a swept transaction both warn by design.

## Not applied: the wiring block for `compat/rig.sh`

This suite runs its own stack, so it does not need `rig.sh` and `rig.sh` does not
know about it. If the fleet later wants one command, this is the block, and it is
written here rather than applied because `rig.sh` is shared with every other
compat directory and this suite has never needed to move it:

```bash
# --------------------------------------------------------------- transactions
# compat/transactions brings up its OWN stack (three facades, one of them
# clustered), so it is invoked rather than wired into this one: nothing here is
# reusable by it except the build.
if [ "$TRANSACTIONS" = 1 ]; then
  say "compat/transactions (its own stack on 32910-32914)"
  "$SCRIPT_DIR/transactions/run.sh" || RESULT=1
fi
```

with `TRANSACTIONS=0` beside `M5=0` in the argument loop and `--transactions)
TRANSACTIONS=1;;` in the `case`. Note that it must run **after** the franz-go
suite and not beside it: both bind a Postgres container and a facade, and the
machine that runs CI is not guaranteed to have the cores for two stacks at once.

## The one core change this suite caused

Scenario `s6`'s `transaction.timeout.ms` check failed on the first run against
the landed implementation, and it failed in the worst possible direction: the
producer's `commitTransaction()` **succeeded** for a transaction whose staged
records had already been dropped by the timeout sweep. The application would
have believed a commit that never happened, which is precisely the failure
`src/txn.rs`'s module header says is unreachable.

The cause was one line. `Txns::sweep` freed the stage with `Txn::clear`, which
leaves the binding in `Empty` -- the state a **decided** transaction ends in, and
the state in which `EndTxn(commit)` is a legitimate commit of nothing. The fix
is `Txn::expire`, which clears the stage and leaves `Abortable` instead, so the
commit is answered `INVALID_TXN_STATE` (48) exactly as `sweep`'s own doc comment
already promised; `handlers::end_txn::abort` then falls through to `discard` on
an expired binding, so a producer that aborts a timed-out transaction can open
the next one without re-initialising.
