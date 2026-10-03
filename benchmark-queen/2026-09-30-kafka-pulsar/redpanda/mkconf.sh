#!/usr/bin/env bash
# mkconf.sh (node) — render THIS node's Redpanda 26.2.3 configuration from hosts.env + PARTS (rc.sh render calls it):
#   $RP/conf/redpanda.yaml    NODE config, written by rpk itself: `rpk redpanda config bootstrap --self <this node's VPC IP>
#                             --ips <the 3 B_PRIV>` (seed servers; rpc 33145 / kafka 9092 / admin 9644 on the VPC IP, never the
#                             public one nor DO's 10.19.x anchor), `rpk redpanda mode production` (developer_mode off + the
#                             production tuner set), then `rpk redpanda config set` for the node rows of overrides.txt;
#                             the pandaproxy / schema_registry sections are removed (unused: nothing listens on 8081/8082)
#   $RP/conf/.bootstrap.yaml  CLUSTER properties. Redpanda reads the .bootstrap.yaml next to redpanda.yaml once, when the
#                             cluster is first formed; every reset wipes all 3 nodes, so every point starts with exactly these
#   $RP/conf/io-config.yaml   this node's `rpk iotune` result (rc.sh iotune keeps it in $RP/state/io-config.yaml); rpk start
#                             hands the one next to redpanda.yaml to Seastar's I/O scheduler (--io-properties-file)
#   $RP/conf/overrides.txt    every override (node, cluster, start flags, OS tuners) with Redpanda's default and the reason
# The node finds its index by matching its own addresses against B_PRIV (never a NIC name); node_id = index + 1 (as Kafka's
# node.id). Env: PARTS (total partitions of the coming run: sizes segment_fallocation_step and the partition memory
# reservation), FALLOC (bytes) / MEMPCT (percent): override those two rules, RPROPS (opt-in cluster properties k=v,k=v: NOT part of the expert set, appended in a marked block), START_FLAGS
# (opt-in Seastar/redpanda start flags, e.g. "--smp=15": NOT part of the expert set, marked).
set -u
main() {
  local D; D=$(cd "$(dirname "$0")/.." && pwd)
  [ -f "$D/hosts.env" ] || { echo "mkconf: no $D/hosts.env" >&2; return 2; }
  . "$D/hosts.env"
  local PROF=${PROFILE:-vm} RP=$D/redpanda
  [ "$PROF" = vm ] || { echo "mkconf: PROFILE=$PROF: the Redpanda part was built for the droplets only (PROFILE=vm)" >&2; return 2; }
  local MYIPS IDX="" i
  MYIPS=" $(hostname -I 2>/dev/null) $(ip -o -4 addr show 2>/dev/null | awk '{split($4, a, "/"); printf "%s ", a[1]}') "
  for i in "${!B_PRIV[@]}"; do case "$MYIPS" in *" ${B_PRIV[$i]} "*) IDX=$i;; esac; done
  [ -n "$IDX" ] || { echo "mkconf: none of this host's addresses ($MYIPS) is in B_PRIV (${B_PRIV[*]})" >&2; return 2; }
  local ID=$((IDX + 1)) IP=${B_PRIV[$IDX]} N=${#B_PRIV[@]} IPS DATA=$D/data/redpanda C=$RP/conf
  IPS=$(echo "${B_PRIV[*]}" | tr ' ' ',')
  mkdir -p "$C" "$RP/state" "$DATA"
  local T=$C/.redpanda.yaml.$$ B=$C/.bootstrap.yaml.$$ O=$C/.overrides.txt.$$
  rm -f "$T" "$B" "$O"
  rp() { rpk "$@" --config "$T" > /dev/null || { echo "mkconf: rpk $* failed" >&2; return 1; }; }

  # ---- the two per-point values, by partition count (like Kafka's heap rule) -----------------------------------------------
  # Every node holds a replica of every partition (RF 3 on 3 nodes): replicas per node = PARTS.
  # (1) segment_fallocation_step: Redpanda fallocates every active segment ahead in steps of this size (default 32 MiB; the
  #     file really is 32 MiB on ext4 the moment a partition is born, measured 10-01: 200 partitions = 6.3 GB), so the disk
  #     held is PARTS x step: 32 MiB x 10k = 320 GB of the 387 GB disk (Redpanda rejects producers below 5 GiB free),
  #     50k = 1.6 TB. Rule: the largest power of two <= 32 GiB / PARTS, in [64 KiB, 32 MiB]: up to 1000 partitions keep the
  #     default; 2k: 16 MiB, 10k: 2 MiB, 50k: 512 KiB, 100k: 256 KiB (at 1M msg/s over >= 10k partitions a partition
  #     receives KB/s: one fallocate every few minutes).
  # (2) topic_partitions_memory_allocation_percent (default 10, max 80) is the share of Redpanda's memory RESERVED for
  #     partition state before the Kafka/RPC/chunk-cache budgets are cut (memory_groups.cc), and with
  #     topic_memory_per_partition (default 204800 B) it is the admission check: replicas per node <= memory x pct /
  #     memory_per_partition (10% admits ~14.9k replicas per node). Rule (measured 10-01, setup logs in runs/redpanda-setup-1001):
  #     the default 10 while it admits PARTS (+64 internal) with 10% headroom; else the need rounded up to a multiple of 5,
  #     CAPPED AT 40: at 75% (100k) the per-shard RPC budget fell to 77 MiB and the 100k creation never converged (40k
  #     leaderless after 12.5 min, logs at 7 MB/s, RSS climbing); at 40% (RPC 227 MiB/shard) 100k was ready in ~60 s, 50k at
  #     40% in ~36 s, 75k at 55% in ~49 s. Where the cap binds (75k, 100k), topic_memory_per_partition is lowered just enough
  #     for admission (75k: 144 KiB, 100k: 108 KiB); the memory partitions really take is measured: ~155-180 KB per replica
  #     at rest (RSS 11.1 / 14.0 / 17.2 GB at 50k / 75k / 100k of the 29 GiB Redpanda holds). Small points keep 10%: a
  #     global raise would cut every point's Kafka budget (80%: 84 MiB per shard instead of ~517 MiB).
  local P=${PARTS:-0} FA FAWHY s MEMB SEAMEM NEED PCT PCTWHY MPP=204800 MPPWHY CAP=40
  if [ -n "${FALLOC:-}" ]; then FA=$FALLOC; FAWHY="FALLOC=$FALLOC set explicitly"
  else
    FA=33554432
    if [ "$P" -gt 0 ] 2>/dev/null; then
      s=$(( 34359738368 / P )); FA=65536
      while [ $((FA * 2)) -le "$s" ] && [ $((FA * 2)) -le 33554432 ]; do FA=$((FA * 2)); done
    fi
    FAWHY="PARTS=${PARTS:-unset} replicas per node: largest power of two <= 32 GiB / PARTS in [64 KiB, 32 MiB] (32 MiB default x PARTS = disk held by fallocation)"
  fi
  MEMB=$(awk '/^MemTotal:/{print $2 * 1024}' /proc/meminfo)
  SEAMEM=$(( MEMB / 1000 * 927 ))                       # Seastar's share after its reserve: 29.06 of 31.34 GiB here
  NEED=$(( (P + 64) * 204800 / 10 * 11 ))               # replicas per node x 200 KiB, + 10% headroom
  PCT=$(( (NEED * 100 + SEAMEM - 1) / SEAMEM ))
  MPPWHY="stated: Redpanda's own at-rest estimate per partition replica (admission check only)"
  if [ -n "${MEMPCT:-}" ]; then PCT=$MEMPCT; PCTWHY="MEMPCT=$MEMPCT set explicitly (NOT the rule)"
  elif [ "$PCT" -le 10 ]; then PCT=10; PCTWHY="default kept: 10% admits PARTS=${PARTS:-unset} replicas per node (+64 internal) with 10% headroom"
  else
    PCT=$(( (PCT + 4) / 5 * 5 ))
    PCTWHY="PARTS=$P replicas per node x 204800 B (+10%) = $((NEED / 1048576)) MiB of ~$((SEAMEM / 1048576)) MiB Seastar memory -> $PCT%"
    if [ "$PCT" -gt "$CAP" ]; then
      PCTWHY="$PCTWHY, capped at $CAP% (75% at 100k starved raft's RPC budget: the creation never converged; 40% converged in ~60 s)"
      PCT=$CAP
      MPP=$(( SEAMEM / 100 * CAP / ((P + 64) / 10 * 11) / 4096 * 4096 ))
      MPPWHY="lowered so that $CAP% admits PARTS=$P replicas per node (+64, +10%): admission only; measured at rest ~155-180 KB per replica (RSS 17.2 GB per node at 100k of the 29 GiB Redpanda holds)"
    fi
    PCTWHY="$PCTWHY; the reserve also shrinks the per-shard Kafka/RPC/chunk-cache budgets (40%: 341/227/170 MiB vs 533/355/267 at 10%): the cost of this partition density on 31 GB"
  fi

  # ---- node config: rpk bootstrap + production mode + node overrides --------------------------------------------------------
  rp redpanda config bootstrap --self "$IP" --ips "$IPS" || return 1
  rp redpanda mode production || return 1
  rp redpanda config set redpanda.node_id "$ID" || return 1
  rp redpanda config set redpanda.data_directory "$DATA" || return 1
  rp redpanda config set redpanda.empty_seed_starts_cluster false || return 1
  rp redpanda config set rpk.ballast_file_path "$DATA/ballast" || return 1
  if [ -n "${START_FLAGS:-}" ]; then
    local fl="" f
    for f in $START_FLAGS; do fl="$fl${fl:+, }\"$f\""; done
    rp redpanda config set rpk.additional_start_flags "[$fl]" || return 1
  fi
  sed -i -e '/^pandaproxy: {}$/d' -e '/^schema_registry: {}$/d' "$T"
  if grep -qE '^(pandaproxy|schema_registry):' "$T"; then echo "mkconf: could not drop pandaproxy/schema_registry from $T" >&2; return 1; fi
  { echo "# Redpanda 26.2.3 node config: node $ID of $N ($IP), PARTS=${PARTS:-unset}. Rendered by redpanda/mkconf.sh on $(hostname)"
    echo "# at $(date -u +%FT%TZ) with rpk (bootstrap + mode production + config set). Do not edit: re-render (rc.sh render)."
    echo "# Reasons: overrides.txt next to this file. Cluster properties: .bootstrap.yaml next to this file."
    cat "$T"; } > "$T.h" && mv "$T.h" "$T"

  # ---- cluster properties: name|value|Redpanda 26.2.3 default|why -----------------------------------------------------------
  # (defaults: src/v/config/configuration.cc of v26.2.x and `rpk cluster config export --all` on a fresh 26.2.3 cluster;
  # "stated" rows keep the default on purpose because the durability class or the comparison depends on them)
  local CLF=$C/.cluster-props.$$
  cat > "$CLF" <<EOF
default_topic_replications|3|1|3 copies of every partition: the durability class (kload also asks RF 3 explicitly)
internal_topic_replication_factor|3|3|stated: __consumer_offsets and the other internal topics in the same class
write_caching_default|false|false|stated: acks=all is acknowledged only once a raft majority (2 of 3 replicas) has fsynced the batch; write caching (ack before fsync) stays off
kafka_batch_max_bytes|16777216|1048576|16 MiB = Kafka's message.max.bytes in the Kafka runs (09-30: group metadata > 1 MiB at 100k partitions); producer batches stay <= 1 MiB (kload -batch-max-bytes)
log_retention_ms|600000|604800000|10 min = Kafka's log.retention.ms: a soak cannot fill the 387 GB disk (kload sets retention.ms=600000 per topic too)
rpc_server_listen_backlog|4096|unset (Seastar's 100)|~1k client connections arrive at once at READY; = net.core.somaxconn from os-tune.sh, as Kafka's socket.listen.backlog.size
topic_partitions_per_shard|7000|5000|admission only: replicas per node / 16 shards must stay <= this; 100k partitions RF 3 = 100k replicas per node = 6250 per shard (16 x 5000 = 80k per node cannot admit the 100k points)
topic_partitions_memory_allocation_percent|$PCT|10|$PCTWHY
topic_memory_per_partition|$MPP|204800|$MPPWHY
segment_fallocation_step|$FA|33554432|$FAWHY
auto_create_topics_enabled|false|false|stated: topics are created on purpose by kload -create-only (RF 3)
EOF
  {
    echo "# Redpanda 26.2.3 CLUSTER properties for the Kafka-matrix re-run on Redpanda (rendered by redpanda/mkconf.sh at $(date -u +%FT%TZ),"
    echo "# PARTS=${PARTS:-unset}). Read by Redpanda when the cluster forms (every reset wipes the 3 nodes first). Reasons: overrides.txt."
    while IFS='|' read -r k v def why; do [ -n "$k" ] || continue; echo "$k: $v"; done < "$CLF"
  } > "$B"
  if [ -n "${RPROPS:-}" ]; then
    local kv k v
    echo "# ---- opt-in RPROPS: NOT part of the expert set, set for this run on purpose" >> "$B"
    for kv in $(echo "$RPROPS" | tr ',' ' '); do
      case $kv in *=*) k=${kv%%=*}; v=${kv#*=}; echo "$k: $v" >> "$B";; *) echo "mkconf: bad RPROPS entry '$kv' (want k=v)" >&2; return 2;; esac
    done
  fi

  # ---- overrides.txt: every override in one place --------------------------------------------------------------------------
  {
    echo "# Redpanda 26.2.3 overrides on node $ID ($IP), rendered $(date -u +%FT%TZ), PARTS=${PARTS:-unset}"
    echo "# format: scope  key = value  (Redpanda default)  why"
    echo "node     redpanda.seed_servers = ${IPS//,/:33145,}:33145  (none)  rpk bootstrap: the 3 brokers' VPC IPs (eth1), never the public IP nor DO's 10.19.x anchor"
    echo "node     redpanda.{rpc_server,kafka_api,admin} = $IP:{33145,9092,9644}  (0.0.0.0)  rpk bootstrap --self: all traffic on the private VPC address, plaintext like every system in the harness"
    echo "node     redpanda.node_id = $ID  (auto)  deterministic: n1..n3 = node 1..3, as Kafka's node.id (every reset wipes all nodes, so ids never repeat on a different box)"
    echo "node     redpanda.empty_seed_starts_cluster = false  (true)  Redpanda's production bootstrap: the 3 seed servers form the cluster together"
    echo "node     redpanda.developer_mode = false  (true in the packaged redpanda.yaml)  rpk redpanda mode production: full checks, real fsyncs"
    echo "node     redpanda.data_directory = $DATA  (/var/lib/redpanda/data)  the harness layout (SPEC §1): the droplet's one local ext4 disk, same disk as every other system"
    echo "node     rpk.ballast_file_path = $DATA/ballast  (/var/lib/redpanda/data/ballast)  the 1 GB ballast lives (and is wiped) with the data"
    echo "node     pandaproxy, schema_registry = removed  (on, :8082 / :8081)  unused HTTP services: not part of a Kafka-protocol benchmark"
    echo "node     rpk.tune_{network,disk_scheduler,disk_nomerges,disk_write_cache,disk_irq,cpu,aio_events,clocksource,swappiness,ballast_file} = true  (false)  rpk redpanda mode production; applied by rc.sh tune (= rpk redpanda tune all), before/after in tune.txt"
    echo "start    rpk redpanda start --check=true  (the unit's /etc/default/redpanda)  production checks run at start (warnings only here: ext4 not XFS, 2005 MB/CPU, no swap)"
    echo "start    smp / memory = Seastar defaults: all 16 vCPUs as 16 shards, all RAM minus Seastar's reserve  (same)  one Redpanda process per 16-core node, nothing else runs there"
    echo "start    overprovisioned = false  (false)  dedicated box: pinned reactors that poll (CPU shows as busy while polling; see rc.sh busy)"
    echo "start    io-properties-file = conf/io-config.yaml  (none)  rpk iotune on this node's data disk (rc.sh iotune), Seastar's I/O scheduler sized to the measured disk"
    echo "process  as root under nohup (rc.sh), ulimit -n 1048576, -l unlimited, oom_score_adj -950  (systemd unit: user redpanda, LimitNOFILE 800000, LimitMEMLOCK infinity, OOMScoreAdjust -950)  the harness layout under /root/bench; the unit's limits kept or raised"
    while IFS='|' read -r k v def why; do [ -n "$k" ] || continue; printf 'cluster  %s = %s  (%s)  %s\n' "$k" "$v" "$def" "$why"; done < "$CLF"
    [ -n "${RPROPS:-}" ] && echo "cluster  RPROPS = $RPROPS  (-)  opt-in, NOT part of the expert set"
    [ -n "${START_FLAGS:-}" ] && echo "start    START_FLAGS = $START_FLAGS  (-)  opt-in, NOT part of the expert set"
    true
  } > "$O"

  rm -f "$CLF"
  mv "$T" "$C/redpanda.yaml" && mv "$B" "$C/.bootstrap.yaml" && mv "$O" "$C/overrides.txt" || return 1
  if [ -s "$RP/state/io-config.yaml" ]; then cp "$RP/state/io-config.yaml" "$C/io-config.yaml"; else rm -f "$C/io-config.yaml"; fi
  echo "mkconf: node $ID/$N ip=$IP PARTS=${PARTS:-unset} segment_fallocation_step=$FA partitions_memory_pct=$PCT memory_per_partition=$MPP io-config=$([ -s "$C/io-config.yaml" ] && echo yes || echo MISSING)$([ -n "${RPROPS:-}" ] && echo " +RPROPS=$RPROPS")$([ -n "${START_FLAGS:-}" ] && echo " +START_FLAGS=$START_FLAGS") -> $C"
}
main "$@"; exit $?
