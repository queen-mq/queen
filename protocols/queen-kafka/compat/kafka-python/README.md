# kafka-python against queen-kafka

The pure-Python client. No librdkafka underneath: its own protocol encoders,
its own group coordinator, its own partitioner, its own version negotiation.
Nothing `compat/librdkafka` proves carries over to it, and it is what
`pip install kafka-python` has meant since 2016.

Nothing here starts or stops a stack. Point it at one:

```sh
KAFKA_BOOTSTRAP=127.0.0.1:19092 RUN_ID=$(date +%s) ./run.sh all
```

| file | what it proves |
| --- | --- |
| `probe_api_version.py` | how this release negotiates versions, and what the explicit `api_version=` hint does to that |
| `compat.py` | the bar: 512 records over 8 partitions with keys and headers, four codecs, group consume with order and byte-exactness, commit and resume, auto-create, seek and offsets |
| `sasl_tls.py` | the M5 lane, SASL/PLAIN over TLS. **Fails today** — see below |
| `raw_sasl_probe.py` | a stdlib-only Kafka client that reads the SASL refusal off the wire, so the SASL finding does not rest on any client's logs |
| `run.sh` | the entry point; builds a venv for one `KAFKA_PYTHON_SPEC` and runs the scenarios |

## Four clients wearing one name

`kafka-python` is not one client, and the differences land exactly where this
facade is picky. Run `run.sh` once per `KAFKA_PYTHON_SPEC`:

| spec | negotiation | Fetch sent | SASL framing | notes |
| --- | --- | --- | --- | --- |
| `kafka-python==2.0.2` | infers a Kafka RELEASE from ApiVersions | v4 | SaslHandshake v0, raw | dead on Python 3.12: `No module named 'kafka.vendor.six.moves'`. Needs a ≤3.11 interpreter |
| `kafka-python==2.3.2` | per-API clamp | v6 | SaslAuthenticate v0 | |
| `kafka-python==3.0.11` | per-API clamp | v6 | SaslAuthenticate v1 | dropped `api_version_auto_timeout_ms` |
| `kafka-python-ng==2.2.3` | as 2.0.2 (a fork of that line) | v4 | SaslHandshake v0, raw | |

## The plaintext lane passes, on all four

Produce, group consume, commit and resume, auto-create, seek, gzip / snappy /
lz4 / zstd — all green, with the DEFAULT config and no `api_version` hint.

Two things worth knowing anyway:

* **2.0.x is right by accident.** It reads ApiVersions, sees Produce
  advertised to v8, concludes "this is Kafka 2.4" and picks every other
  request version from that release — which would mean Fetch v11 at a facade
  that caps Fetch at v6. It survives because its Fetcher never emits above
  Fetch v4. 2.1+ and 3.x clamp per API and ask for v6 because v6 is what is
  advertised.
* **An explicit `api_version=` hint is a footgun on 2.1+/3.x**, the opposite of
  the usual advice. The hint means "assume the broker is this release" and
  switches the per-API clamp OFF: `(1, 1, 0)` sends Fetch v7, `(2, 0, 0)` sends
  Fetch v8, the facade closes the connection on both, and the consumer spins in
  a reconnect loop (~95 reconnects per 10s, from the facade's own `suppressed=`
  counter). If you must pin one, `(0, 11, 0)` is the ceiling: Fetch v5,
  Produce v3, Metadata v4, all inside the advertised windows.

## The SASL lane does not pass, and it is the facade's side

kafka-python builds its PLAIN initial response as

```python
'\0'.join([username, username, password])   # kafka/sasl/plain.py:26
                                            # kafka/conn.py:628 on 2.0.x
```

— the username goes in the authzid field as well as the authcid field. RFC 4616
allows that, and Apache Kafka's own `PlainSaslServer` allows it: the only
authzid it refuses is one that *differs* from the username ("Authentication
failed: Client requested an authorization id that is different from username",
in `kafka-clients-3.9.1.jar`).

`protocols/queen-kafka/src/sasl.rs:218` refuses **any** non-empty authzid, so every
kafka-python release is locked out of a SASL listener, and there is no
client-side workaround: the username *is* the authzid, so emptying one empties
the other and the facade then answers `NoUsername`. `sasl_tls.py` case 2
demonstrates both dead ends rather than asserting them.

Case 3 monkeypatches the client to send the empty authzid every other client
sends and drives the full produce + group-consume path over TLS through it.
That is not a deployable workaround; it is the proof that this one check is the
only thing in the way, and that TLS, the credential forwarding and the authgate
refusal all work behind it.

The one-line change that would fix it, matching Apache Kafka:

```rust
if !authzid.is_empty() && authzid != username {
    return Err(PlainError::Impersonation);
}
```

## A second, smaller SASL finding

The facade answers a refusal correctly at SaslAuthenticate v0 *and* v1 —
`error_code=58` with a legible message — and then closes the socket with no gap
(`src/handlers/sasl_authenticate.rs`, "no artificial delay", deliberate).
kafka-python 3.x's async transport loses the response to that close: it reports
`KafkaConnectionError: socket disconnected`, which is *retriable*, and retries a
wrong password forever. Relaying the same listener through a proxy that holds
the FIN for 300 ms turns that into the parsed
`SaslAuthenticateResponse(error_code=58, error_message='Queen refused this
credential (HTTP 401)…')`. Apache Kafka spends
`connection.failed.authentication.delay.ms` (default 100 ms) at exactly this
point. `raw_sasl_probe.py` establishes the wire behaviour independently of any
client.

## Conventions

Same as the rest of `compat/`: `[bootstrap] [runId]` positionally, every topic
and group carries the runId, one `  ok  ` / `  FAIL` line per assertion, `=== `
section headers, a final `RESULT: PASS` / `RESULT: FAIL (n)` and a non-zero
exit. Every blocking call has a deadline and every script has a watchdog,
because this client's characteristic failure IS a hang. The API versions
printed at the end are read out of kafka-python's own DEBUG stream, never
assumed.
