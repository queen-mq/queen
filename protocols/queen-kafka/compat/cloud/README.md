# queen-kafka compat: QUEEN CLOUD

A whole Cloud cell in one script, and sixteen scenarios run against it by real
Kafka clients (franz-go). Nothing here asserts Kafka semantics — `compat/go`
does that over two hundred cases. What this asserts is the set of things that
only become true once a facade is put behind a control plane.

```
   Kafka client ──TCP──► queen-kafka ──QUEEN_URL──► queen-proxy ──► queen
                          (facade)                   auth, tenant      (broker)
                                                     scoping, quotas,
                                                     metering
```

The one line that makes it a cell rather than a demo is `QUEEN_URL`. In OSS
embedded mode the facade is handed a **loopback** address and talks to the
broker directly; in Cloud that is exactly wrong, because a Kafka client would
then be a tenant with no quota, no metering and no isolation. Every request
here crosses the proxy.

## Run it

```bash
protocols/queen-kafka/compat/cloud/rig-cloud.sh                    # the whole suite
protocols/queen-kafka/compat/cloud/rig-cloud.sh -run TestMetering -v
protocols/queen-kafka/compat/cloud/rig-cloud.sh --keep             # leave the stack up
```

`--keep` prints a `rig.env`; source it and run `./run.sh` for as many iterations
as you like against the same cell.

`run.sh` on its own runs the suite against a cell that is **already up** — this
one, or a real staging cell. Every address and credential comes from the
environment, so nothing in the Go needs editing to point it somewhere else.
Each variable it does not get skips exactly one scenario, loudly.

## Ports and containers

`33040-33059`, container prefix `qkc-t2-`. Deliberately not the defaults and
not any port another rig in this repository uses. `5432` is a live stack on a
developer machine and is never touched.

| what | where |
|---|---|
| pxdb (proxy control plane) | docker `qkc-t2-pxdb`, host `:33040` |
| cell PG (the broker's database) | docker `qkc-t2-cellpg`, host `:33041` |
| broker | host process `:33042` |
| proxy | host process `:33043` |
| facade (Kafka wire) | host process `:33044` |

Teardown is `docker rm -f` on **those two names only**, and host processes by
**the pids recorded at spawn**. Never a pid resolved from a port; never a
`pkill -f` on a shared binary path. Both of those reach into somebody else's
stack on a machine running more than one.

## Routing: a shared host, and why not per-cluster SNI

The proxy resolves a cluster either from the `Host` header (a per-cluster
hostname) or, on a host listed in `QUEEN_PROXY_SHARED_HOSTS`, **from the
credential**. This rig uses the second, and that is a design call rather than a
shortcut.

`protocols/queen-kafka/src/lib.rs` keeps `advertised_host` per **process**, not per SNI
lane. One facade therefore hands every client the same bootstrap address
whatever name they dialled, so a second tenant's connections would come back
carrying the first tenant's SNI and route to the first tenant's cluster.
Per-cluster SNI needs one facade process per cluster; **one facade fronting many
tenants needs the credential to be the authority**, and that is the shape a
Kafka listener actually has in Cloud.

The consequence is the point: the tenant of a Kafka connection is the tenant of
its SASL password, and nothing else.

## Scopes: `read` is not optional

The facade checks a credential by calling `GET /api/v1/resources/queues`, which
is a `read` route. Every Kafka client issues Metadata before anything else, and
Metadata **is** that queue listing. So a key without `read` is refused at SASL
and the connection never opens at all.

| client | scopes |
|---|---|
| consumer | `consume`, `read` |
| producer | `produce`, `read` |
| transactional producer | `produce`, `consume`, `read` |
| admin | `admin`, `read` (plus the verbs it also uses) |

The transactional producer needs `consume` because its `qk:txn:` marker is a
`POST /api/v1/kv` batch, which the proxy's `qk:` prefix rule classifies as
Consume.

## What each scenario proves

| # | Test | Asserts |
|---|---|---|
| 1 | `TwoTenantsDoNotSeeEachOthersTopics` | Same topic name both sides; Metadata for A lists A's only. |
| 2 | `TwoTenantsDoNotReadEachOthersRecords` | Distinct payloads, same topic name; each reads only its own. |
| 3 | `TwoTenantsDoNotShareAConsumerGroup` | Same group id both sides; neither is carried along by the other's commits. |
| 4 | `TwoTenantsDoNotShareCommittedOffsets` | `OffsetFetch` per tenant returns only its own cursor. |
| 5 | `AConsumeScopedKeyCanReadButNotCreateTopics` | Fetch OK; CreateTopics `TOPIC_AUTHORIZATION_FAILED` **with the proxy's reason in `error_message`**. |
| 6 | `AProduceScopedKeyCannotConsume` | Produce OK; Fetch `TOPIC_AUTHORIZATION_FAILED`. |
| 7 | `AKeyWithoutReadCannotEvenAuthenticate` | `SASL_AUTHENTICATION_FAILED` whose message names **scopes**, not a bad password. |
| 8 | `ARateCappedTenantIsThrottledAndNotFailed` | A 1 req/s cap produces `throttle_time_ms > 0` and zero transport failures; the work completes. |
| 9 | `MeteringRowsAppearInPxdbForKafkaTraffic` | `queen_proxy.usage_minutes` has rows for both clusters; Produce is message-metered, Fetch is not. |
| 10 | `TheFacadeResolvesItsTenantFromAuthMe` | Two keys of one cluster resolve to one `acting_cluster.id` and form ONE group. |
| 11 | `OffsetsCommitForATenantWhosePlanHasNoKv` | Both clusters are on `free`, whose `features` is `{}` — offsets still commit and fetch. |
| 12 | `ABlockedTenantCanStillCommitAndReadOffsets` | Under a 64-byte storage cap: produce is refused, the cursor still moves. |
| 13 | `APlainKvBatchIsStillGated` | A non-`qk:` KV batch is still 403 `feature_gated`; a `qk:`-only one is 200. |
| 14 | `ALongPollFetchIsNotCutByTheProxyTimeout` | A `fetch.max.wait.ms = 30000` poll returns cleanly, and the margin is reported. |
| 15 | `TheTransactionRouteIsStillProduceClassified` | A `{produce, consume, read}` txn producer commits; the asymmetry is documented. |
| 16 | `TheSmartMirrorShowsAKafkaGroupThroughTheProxy` | The console listing carries the Kafka group with `kind: "kafka"`. |

Scenario 12 runs **last** and its cleanup **waits** for the block to lift:
removing a storage cap is not the write to pxdb, it is the registry reconciler
noticing it on top of the broker's retained-bytes lane, and returning early
would fail the next scenario for this one's reason.

## Three things it MEASURES and does not fix

Printed as `MEASURED` lines, and each of them a decision somebody made:

* **Fetch is billed as requests.** `op_class=read` carries `reqs` and `msgs=0`.
  A tenant consuming a million records through Kafka is billed for the requests
  that carried them, not for the records. Produce **is** message-metered.
* **The txn classification asymmetry.** `POST /api/v1/transaction` stays
  Produce-classified even though its top-level `kv` rider carries the same
  `qk:` keys that, sent to `/api/v1/kv`, are Consume-classified.
* **The parked gauge cannot see a Kafka consumer.** A Fetch takes no parked
  slot, so the gauge an operator watches for consumer pressure reads zero
  however many Kafka consumers are long-polling. The margin between the
  facade's 30 s clamp and `QUEEN_PROXY_UPSTREAM_TIMEOUT_MS` is what must not be
  shrunk.

## Two gates that are easy to miss

Both cost a full debugging session the first time, so the rig handles them and
says so:

1. **`queen.kv_quota` in the CELL's database.** With `QUEEN_TENANCY_HEADER=true`
   the broker derives `kv_require_grant`, and the **absence** of a `kv_quota`
   row is a denial, not a permission. Without one row per
   `clusters.broker_tenant_uuid`, every offset commit is 403 `feature_gated` at
   the broker, past everything the proxy decided.
2. **That grant is a snapshot on a timer.** `QUEEN_KV_QUOTA_REFRESH_MS`
   defaults to **thirty seconds**. The rig sets it to 2 s and then waits for a
   real `qk:` put to be accepted before starting the suite — a run that jumps
   the gun reports a Cloud gate that is really a rig in a hurry.

## Wiring it into `compat/rig.sh` (NOT applied)

`compat/rig.sh` is the plaintext/M5 rig and is deliberately left alone: this
suite needs a proxy, a pxdb and a control plane, which is a different stack, not
another flag. If it is ever folded in, this is the shape — a `--cloud` flag that
delegates rather than duplicating anything:

```bash
# in compat/rig.sh's argument loop
    --cloud) CLOUD=1;;

# ...and at the end, after the franz-go suite has run
if [ "$CLOUD" = 1 ]; then
  say "cloud acceptance (its own cell: pxdb, cell PG, broker, proxy, facade)"
  # Its own ports (33040-33059) and its own containers (qkc-t2-*), so it can
  # run beside this rig's Postgres on 55432 without either seeing the other.
  # It builds, starts, asserts and tears down its whole stack itself; passing
  # --keep through would leave TWO stacks up, so it deliberately is not.
  "$SCRIPT_DIR/cloud/rig-cloud.sh" || RESULT=1
fi
```

Nothing else in `rig.sh` changes. The two rigs share no port, no container name
and no process.
