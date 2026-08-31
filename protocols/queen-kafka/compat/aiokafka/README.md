# aiokafka against queen-kafka

The asyncio client, and the default in async Python services. It shares its
protocol lineage with kafka-python — the same record codecs, the same
`SaslPlainAuthenticator` shape — but it has its own connection layer, its own
group coordinator and, since 0.13, its own version negotiation. Nothing
`compat/kafka-python` proves carries over automatically; the two agree on one
finding and disagree sharply on another.

Nothing here starts or stops a stack. Point it at one:

```sh
KAFKA_BOOTSTRAP=127.0.0.1:19092 RUN_ID=$(date +%s) ./run.sh all
```

| file | what it proves |
| --- | --- |
| `compat.py` | the bar: 512 records over 8 partitions with keys and headers, four codecs, group consume with order and byte-exactness, commit and resume, auto-create, seek and offsets, and the idempotence failure mode |
| `sasl_tls.py` | the M5 lane, SASL/PLAIN over TLS. **Fails today** — see below |
| `run.sh` | the entry point; builds a venv for one `AIOKAFKA_SPEC` and runs the scenarios |

## The plaintext lane passes on 0.14.0, with the default config

`RESULT: PASS`, 30 assertions, no `api_version` hint and no other knob. Produce
(acks=all), gzip / snappy / lz4 / zstd, group consume with byte-exact keys,
values and headers, per-partition order, commit-and-resume with zero loss and
zero duplicates, auto-create, `beginning_offsets` / `end_offsets` / `seek` /
`position`. What 0.14.0 negotiates, read out of its own debug stream:

```
ApiVersion v0, Fetch v6, FindCoordinator v1, JoinGroup v2, LeaveGroup v1,
Metadata v5, Offset v3, OffsetCommit v3, OffsetFetch v3, Produce v7,
SyncGroup v1
```

Every one inside the advertised window, and all 41 connection closes in the
trace are `SHUTDOWN` or explicit with `exc=None` — no reconnect loop, and the
facade logged no error at all.

## 0.13.0 is a hard compatibility boundary

**aiokafka ≤ 0.12 cannot use consumer groups against this facade.** Not slowly,
not with a warning: it hangs forever.

| spec | negotiation | JoinGroup sent | groups? |
| --- | --- | --- | --- |
| `aiokafka==0.14.0` | per-API clamp from ApiVersions (`conn.py:414`, `request.prepare(self._versions)`) | v2 | **yes** |
| `aiokafka==0.12.0` | infers a Kafka *release*; against this facade it concludes `(2, 4, 0)` | **v5** | **no — infinite reconnect** |
| `aiokafka==0.12.0` + `api_version="2.2.0"` | pin forces the `< (2,3,0)` branch | v2 | yes |

The chain, all four links verified:

1. 0.12's `check_version()` infers a Kafka **release** from ApiVersions rather
   than clamping per API. The facade advertises Produce to v9 and Metadata to
   v9, so it concludes `(2, 4, 0)` — printed straight off `client.api_version`.
2. `consumer/group_coordinator.py:1313` branches on that: `elif self._api_version
   < (2, 3, 0)` picks `JoinGroupRequest[2]`, and the `else` picks
   `JoinGroupRequest[3]`, which in 0.12's table **is `JoinGroupRequest_v5`**.
3. The facade advertises JoinGroup **0–4**, and an out-of-window version on an
   advertised key is answered by closing the connection (deliberate; see
   `compat/ERRORS.md`).
4. aiokafka marks the coordinator dead and retries forever. In one 15-minute
   run: **17,769 JoinGroup v5 requests, 5,952 connection closes**, no exception
   ever raised to the caller.

Produce is unaffected throughout — `ProduceRequest_v7` is in window, and
sections 1 and 2 of `compat.py` pass on 0.12.0. It is only the group that dies,
which makes it look like a broker hang rather than a negotiation failure.

Note the contrast with `compat/kafka-python`, where an explicit `api_version=`
hint is the footgun that *breaks* a working client. On aiokafka ≤ 0.12 the pin
is the **fix**: anything below `(2, 3, 0)` works, and `"2.2.0"` is the best of
them (JoinGroup v2, Produce v7, Fetch v4, OffsetCommit v2 — all in window).
The real advice is simply **require aiokafka >= 0.13**.

## The SASL lane does not pass, and it is the facade's side

Independently reproduced, by a client with its own connection layer: this is
the same defect `compat/kafka-python` found.

aiokafka builds its PLAIN initial response at `aiokafka/conn.py:616-622` as

```python
"\0".join([username, username, password])
```

so the username lands in the **authzid** field as well as the authcid field.
On the wire:

```
SaslAuthenticateRequest_v1(sasl_auth_bytes=b'aiok\x00aiok\x00<token>')
```

RFC 4616 allows it, and Apache Kafka's own `PlainSaslServer` allows it — the
only authzid it refuses is one that *differs* from the username.
`protocols/queen-kafka/src/sasl.rs:217` refuses **any** non-empty authzid, so the facade
answers:

```
SaslAuthenticateResponse_v1(error_code=58, error_message='an authorization
identity was requested. This facade authorizes the token in the password field
and nothing else, so it cannot honour one')
```

There is no client-side workaround, because in aiokafka the username *is* the
authzid: emptying one empties the other and the facade then answers
`no username`. `sasl_tls.py` case 2 demonstrates that dead end rather than
asserting it.

One aiokafka-specific aggravation worth knowing: the error reaches the caller
as a generic `KafkaConnectionError: Unable to bootstrap from [...]`. The real
reason — error 58 and its full message — appears only at `DEBUG` level. An
operator hitting this without debug logging on has nothing to go on.

Case 3 monkeypatches the authenticator to send the empty authzid every other
client sends, and drives a full produce + group consume + commit over TLS
through it. That is **not** a deployable workaround; it is the proof that this
one check is the only thing in the way. Behind it, everything works: TLS with
**real hostname verification** (the rig cert carries an IP SAN for 127.0.0.1,
so a host client needs no `check_hostname=False`), credential forwarding, and
case 4's wrong password correctly refused with the authgate logging
`refused GET /auth/me`.

## Two client-side gotchas, neither the facade's fault

* **`enable_idempotence=True` cannot work.** InitProducerId is M7 and is not
  advertised. aiokafka fails fast and legibly at `start()` with
  `IncompatibleBrokerVersion: InitProducerIdRequest cannot be used if the API
  version is unknown` — much better than the Java client, which dies mid-send
  and never recovers. `compat.py` section 8 records it; it is expected, not a
  defect.
* **`partitions_for_topic()` is synchronous and reads a cache that
  `await topics()` does not fill.** Verified: `topics()` returned all 11 topics
  while `partitions_for_topic` on the very next line still returned `None`,
  because `topics()` answers from a throwaway `fetch_all_metadata()`.
  `client.add_topic()` is the call that primes the cache. `compat.py`'s
  `partitions_of()` helper exists only for this. Pure aiokafka API shape, and
  the first thing that bites when porting a script from kafka-python.

## Wiring this into rig.sh

`rig.sh` was not edited. To add this row, mirror the kafka-python block:

```sh
if [ -x "$COMPAT/aiokafka/run.sh" ]; then
  say "aiokafka"
  KAFKA_BOOTSTRAP="$KAFKA_HOST:$KAFKA_PORT" \
  RUN_ID="$RUN_ID" \
  QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
  KAFKA_TLS_BOOTSTRAP="${KAFKA_TLS_HOST:-}:${KAFKA_TLS_PORT:-}" \
  QUEEN_KAFKA_SASL_TOKEN="$TOKEN" \
  QUEEN_KAFKA_TLS_CA="$LOGDIR/tls.crt" \
  "$COMPAT/aiokafka/run.sh" all || RC=1
fi
```

Two cautions. **The `sasl` scenario fails by design** until `sasl.rs:217`
changes, so gate it out of a green run or expect the row to be red for the
reason documented above. And a group formation costs 3s
(`QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`); `compat.py` forms four groups and assigns
partitions directly everywhere else to keep the bill down. The whole
`compat` scenario runs in about 90s.
