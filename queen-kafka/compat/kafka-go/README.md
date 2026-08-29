# compat/kafka-go — segmentio/kafka-go against queen-kafka

`github.com/segmentio/kafka-go v0.4.51`, the other big pure-Go Kafka client, and
the only one in the matrix that carries **two protocol stacks at once**. The
suite exists because that split is where a facade gets caught.

Full prose — what the suite proves, and which behaviours are the client's fault
rather than the facade's — is at the top of `helpers_test.go`. This file is the
operating manual.

## Run it

Nothing here starts a stack. Bring one up (`compat/rig.sh --keep -run TestNothing`,
or your own), then:

```sh
QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:19092 ./run.sh
```

| variable | meaning | default |
| --- | --- | --- |
| `QUEEN_KAFKA_BOOTSTRAP` | plaintext facade | `127.0.0.1:19092` |
| `QUEEN_KAFKA_PARTITIONS` | the facade's `QUEEN_KAFKA_DEFAULT_PARTITIONS` | `8` |
| `RUN_ID` | suffix on every topic and group | epoch seconds |
| `QUEEN_KAFKA_TLS_BOOTSTRAP` | TLS+SASL facade; **unset skips the M5 lane** | — |
| `QUEEN_KAFKA_SASL_TOKEN` | the bearer token that is also the SASL password | — |
| `QUEEN_KAFKA_TLS_CA` | PEM of the listener's cert; set it and the chain is verified for real, unset falls back to `InsecureSkipVerify` | — |

Extra arguments pass through to `go test`, so `./run.sh -run TestGroupConsumeAll`
works.

Point `QUEEN_KAFKA_TLS_BOOTSTRAP` at a **name**, not an IP — `localhost:19093`,
which the rig's certificate covers. Go sends no SNI for an IP-literal
`ServerName`, so the `127.0.0.1` form authenticates fine but the facade logs
`sni=""` and the SNI-forwarding path is never exercised.

`GOWORK=off` and `-count=1` are both mandatory and `run.sh` sets both: the root
`go.work` does not list this module, and without `-count=1` Go replays a cached
PASS that proves nothing about the stack now running.

## What it covers

| test | bar |
| --- | --- |
| `TestApiVersionsAndNegotiation` | the advertised table vs the versions kafka-go **hardcodes** |
| `TestMetadataShape` | one broker, leader of every partition, controller resolves |
| `TestAutoCreateIsGatedByTheWireFlag` | auto-create follows the Metadata flag from v4 up |
| `TestUnknownTopicIsNamed` | a `__`-prefixed name answers UNKNOWN_TOPIC_OR_PARTITION |
| `TestProduceUncompressed` | 512 records, 8 partitions, keys + headers, byte-exact |
| `TestProduceEveryCodec` | gzip, snappy, lz4, zstd — all four kafka-go ships |
| `TestProduceAcksZero` | the `&kafka.Writer{}` acks=0 trap, documented not asserted |
| `TestAutoCreateOnProduce` | `Writer.AllowAutoTopicCreation` on a fresh topic |
| `TestAutoCreateRefusedIsNotRefused` | the same with the flag false |
| `TestOffsetBounds` | ListOffsets first/last, through both kafka-go APIs |
| `TestSeek` | `SetOffset` absolute / LastOffset / FirstOffset, and `ReadLag` |
| `TestOffsetOutOfRange` | the code surfaces with `OffsetOutOfRangeError`, and recovers without it |
| `TestGroupConsumeAll` | group consume, commit, member restart, exact resume |
| `TestOffsetFetchAllTopics` | the NULL-topics OffsetFetch shape |
| `TestGroupTwoMembers` | two members, disjoint and complete assignment |
| `TestSaslTlsRoundTrip` | produce + group consume over SASL/PLAIN on TLS |
| `TestSaslWrongPasswordIsRefused` | a wrong bearer is refused legibly (needs `compat/authgate`) |

## The negotiated-versions table

kafka-go has no protocol debug stream — no `debug=protocol`, no
`NetworkClient=debug`. So instead of assuming, every connection the suite opens
is wrapped in a `net.Conn` that parses the request header of each frame the
client writes. `TestMain` prints the table at the end of every run. Measured
against a facade at `QUEEN_KAFKA_DEFAULT_PARTITIONS=8`:

```
Produce          v8            (Transport)     ListOffsets  v1, v5
Fetch            v5            (Conn)          Metadata     v6 (Conn), v8 (Transport)
OffsetCommit     v2            (Conn, fixed)   OffsetFetch  v1 (Conn, fixed), v5 (Transport)
FindCoordinator  v0, v2        JoinGroup v2    SyncGroup v0  Heartbeat v0  LeaveGroup v0
ApiVersions      v0 (Conn), v2 (Transport)
```

The two to watch are **OffsetCommit v2** and **OffsetFetch v1**: kafka-go writes
them with no negotiation at all, and they sit on the facade's exact advertised
floors (`2-6` and `1-7`). `apiVersionMap.negotiate` (kafka-go `conn.go:72`) never
reads MinVersion, so raising either floor by one turns every kafka-go consumer
group into an unexplained mid-join EOF. `TestApiVersionsAndNegotiation` is the
tripwire.

## Wiring it into rig.sh

Not done here — `rig.sh` is deliberately untouched. The block would go after the
franz-go suite and needs no new stack, only the addresses rig.sh already holds:

```sh
say "kafka-go suite"
QUEEN_KAFKA_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
RUN_ID="$$" \
QUEEN_KAFKA_TLS_BOOTSTRAP="$([ "$M5" = 1 ] && echo "$KAFKA_TLS_HOST:$KAFKA_TLS_PORT")" \
QUEEN_KAFKA_SASL_TOKEN="$([ "$M5" = 1 ] && echo "$SASL_TOKEN")" \
QUEEN_KAFKA_TLS_CA="$([ "$M5" = 1 ] && echo "$LOGDIR/tls.crt")" \
  "$SCRIPT_DIR/kafka-go/run.sh" || RESULT=1
```

`KAFKA_TLS_HOST` already defaults to `localhost` in rig.sh, which is the name the
rig certificate covers, so the SNI path works with no further change. There is no
`QUEEN_URL` and no `QUEEN_KAFKA_RESTART_CMD` here: this suite talks only Kafka
wire, and does not drive a facade restart.
