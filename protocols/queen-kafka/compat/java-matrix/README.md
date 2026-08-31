# compat/java-matrix — kafka-clients across versions

`compat/java` proves the official Java client works against the facade at one
version. This directory asks the question that version cannot answer on its own:
**does the facade's advertised window keep satisfying kafka-clients as
kafka-clients moves?**

That question got sharp with Kafka 4.0. KIP-896 raised the minimum protocol
versions a client is willing to speak — 4.x clients refuse brokers older than
2.1 — while `protocols/queen-kafka/src/versions.rs` deliberately caps several APIs well
below their newest schema version (Fetch at v6 because v7 is fetch sessions,
Metadata at v9 because v10 is topic ids, the group APIs one below
`group_instance_id`). Those two facts point at each other, and only a real client
against a running facade settles it.

**It is settled, and the answer is yes.** kafka-clients 4.3.1 negotiates every
API to the top of the facade's window and passes the whole suite. The KIP-896
floors land below the caps, not above them.

## What is here

| file | what it is |
| --- | --- |
| `QueenKafkaMatrix.java` | the scored suite: negotiation, bulk produce, compression, group consume, commit-and-resume, auto-create, offsets and seek |
| `QueenKafkaEdges4x.java` | the unsupported shapes, unscored — what a user actually sees when they hit one |
| `run.sh` | acquires the jars and drives every version; starts and stops nothing |

`run.sh` assumes a stack is **already running**. Everything it needs comes from
the environment:

```
KAFKA_BOOTSTRAP=127.0.0.1:19092    RUN_ID=<suffix on every topic and group>
KAFKA_VERSIONS="4.3.1 3.6.2"       SUITES="matrix edges"
JARS_CACHE=$TMPDIR/queen-kafka-clients
```

and, for the SASL_SSL lane:

```
QK_SECURITY_PROTOCOL=SASL_SSL   QK_SASL_MECHANISM=PLAIN
QK_SASL_USERNAME=<free label>   QK_SASL_PASSWORD=<the Queen bearer token>
QK_TRUSTSTORE=<path.p12>        QK_TRUSTSTORE_PASSWORD=changeit
QK_DISABLE_HOSTNAME_VERIFICATION=1   # only when the advertised host is not a cert SAN
```

There is no pom and no gradle, matching `compat/java`: the programs run in
java's single-file source mode against a directory of jars, which `run.sh`
fetches from Maven Central on first use and caches **outside the repo**. Point
`JARS_CACHE` at a populated directory to run with no network.

## The negotiation section is the point

Section 1 of `QueenKafkaMatrix.java` reads the client's own `NetworkClient`
debug stream — never an assumption — and asserts, per API, both the window the
facade advertised and the version the client actually settled on:

```
ok   Fetch advertised 4..6 as versions.rs says (4..6)
ok   Fetch settled on v6, the top of the advertised window
```

If a future kafka-clients raises a floor past one of the caps, this is the check
that goes red first, and it names the API and both numbers.

## Things that are the client's fault, not the facade's

- **`enable.idempotence` defaults to true** since 3.0. InitProducerId is
  unimplemented (M7), so every producer here sets it false. Left alone, the
  client dies on its *first* send — `UnsupportedVersionException: The node does
  not support INIT_PRODUCER_ID`, then `KafkaException: Cannot execute
  transactional method because we are in an error state` — in about 400ms, and
  never recovers.
- **KIP-714 client telemetry** is on by default in 4.x. `GetTelemetrySubscriptions`
  is not advertised, so the client logs an `UnsupportedVersionException` and
  carries on. Noise, not a failure.
- **`group.protocol=consumer`** (KIP-848) fails in ~70ms on 4.x with an error
  that names the fix: *"Set group.protocol=classic on the consumer configs."*
  The default in 4.3.1 is still `classic`, so an app that does not ask for the
  new protocol never meets this.
- **`initTransactions()` does not fail fast.** It blocks for the whole of
  `max.block.ms` and then reports a `TimeoutException` about InitProducerId
  rather than the `UnsupportedVersionException` the plain idempotent producer
  gets in 400ms. Nothing to fix facade-side — FindCoordinator succeeds, so the
  client keeps waiting on a response it will never negotiate — but it is worth
  knowing that this one shape hangs for `max.block.ms`.
- **kafka-clients before 3.9 cannot do SASL on JDK 24+.** Their
  `SaslClientCallbackHandler` calls `Subject.getSubject`, which JEP 486 removed;
  `-Djava.security.manager=allow` is itself rejected by JDK 24. `run.sh` warns
  when it sees that combination. The plaintext lane is unaffected, and the same
  client passes the SASL lane cleanly on a JDK 21.
- **A Java client sends no SNI for a single-label host.** Bootstrapping at
  `localhost:9093` logs `sni=""` facade-side even though the name is not an IP
  literal — the JDK only sends SNI for names containing a dot. Anything that
  routes on `QUEEN_KAFKA_FORWARD_SNI_HOST` should not expect a value from such a
  client.
