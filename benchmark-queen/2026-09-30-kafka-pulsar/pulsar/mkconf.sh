#!/usr/bin/env bash
# mkconf.sh (node) — render THIS node's ZooKeeper, bookie and broker configs and the per-process JVM env by COPYING the
# Pulsar 4.2.4 defaults (pulsar/conf-defaults/*.conf, byte-identical to the tarball's conf/; entry_location_rocksdb.conf
# from the installed dist) and overriding keys: every untouched setting stays the Pulsar default. SPEC.md §5 is the
# source of every value; README.md says why.
# Output ($REMOTE_ROOT/pulsar/node/): zookeeper.conf bookkeeper.conf broker.conf entry_location_rocksdb.conf,
#   zk.env bookie.env broker.env (sourced by pc.sh start), knobs.env, overrides.txt (file key: default -> value),
#   overrides.diff (diff -u against the defaults: the overrides and nothing else).
# Knobs (env): PROFILE=vm|local (memory; local = the laptop rehearsal), JOURNALS=1|2 (journal dirs on the data disk),
#   ZK_SET / BOOKIE_SET / BROKER_SET = "k=v;k=v" extra overrides (opt-in, recorded like the rest),
#   MEM_ZK / MEM_BOOKIE / MEM_BROKER = whole JVM memory strings (replace the profile's).
# Which env var sizes which JVM (read from 4.2.4's bin/pulsar, conf/pulsar_env.sh, conf/bkenv.sh):
#   zookeeper, broker: PULSAR_MEM   (pulsar_env.sh default "-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g")
#   bookie:            BOOKIE_MEM   (bkenv.sh; FALLS BACK TO PULSAR_MEM when that is set, else "-Xms2g -Xmx2g
#                      -XX:MaxDirectMemorySize=2g"; bin/pulsar does not source pulsar_env.sh for the bookie)
#   GC: PULSAR_GC / BOOKIE_GC left unset = Pulsar's default on JDK 21: -XX:+UseZGC -XX:+ZGenerational
#       -XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch; GC logs (-Xlog:async -Xlog:gc*,safepoint) land in PULSAR_LOG_DIR
#       as pulsar_gc_<pid>.log (zk, broker) and pulsar_bookie_gc_<pid>.log (bookie).
set -u
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
DIST=$R/pulsar/dist DEF=$PDIR/conf-defaults NODE=$R/pulsar/node NEW=$R/pulsar/node.new
DATA=$R/data/pulsar LOGS=$R/logs/pulsar
JOURNALS=${JOURNALS:-1}
case $PROFILE in vm|local) ;; *) die "PROFILE must be vm|local (got $PROFILE)";; esac
case $JOURNALS in 1|2) ;; *) die "JOURNALS must be 1|2 (got $JOURNALS)";; esac
[ -x "$DIST/bin/pulsar" ] || die "no Pulsar dist in $DIST (run pulsar/install.sh first)"
for f in zookeeper.conf bookkeeper.conf broker.conf; do [ -f "$DEF/$f" ] || die "missing $DEF/$f"; done

# ---- who am I: match this host's addresses against B_PRIV (SPEC §1); never a NIC name
IDX=0 IP=""
MYIPS=" $(hostname -I 2>/dev/null) "
for i in "${!B_PRIV[@]}"; do case "$MYIPS" in *" ${B_PRIV[$i]} "*) IDX=$((i + 1)); IP=${B_PRIV[$i]};; esac; done
[ "$IDX" -gt 0 ] || die "this host ($(hostname -I 2>/dev/null)) holds none of B_PRIV (${B_PRIV[*]})"
ZKSEMI=${ZKS//,/;}


# ---- memory budget per node (SPEC §5): vm = 31 GB droplet, local = the rehearsal container (--memory 1300m)
if [ "$PROFILE" = vm ]; then
  M_ZK="-Xms1g -Xmx1g"
  M_BK="-Xms3g -Xmx3g -XX:MaxDirectMemorySize=6g"
  M_BR="-Xms6g -Xmx6g -XX:MaxDirectMemorySize=6g"
  WCACHE=1536 RCACHE=1024 BCACHE=536870912 MLCACHE=2048
else
  # local: the brief said ZK 128m, bookie 256m + 384m direct, broker 384m + 384m direct. ZGC's heap is a memfd, fully
  # committed at start (Xms=Xmx, AlwaysPreTouch) and charged to the cgroup as shmem: 768 MB of heaps + ~490 MB of
  # non-heap for the three JVMs left no room, and the first catch-up read OOM-killed all three brokers (1300m cgroup).
  M_ZK="-Xms64m -Xmx64m"
  M_BK="-Xms160m -Xmx160m -XX:MaxDirectMemorySize=256m"
  M_BR="-Xms256m -Xmx256m -XX:MaxDirectMemorySize=256m"
  WCACHE=32 RCACHE=32 BCACHE=33554432 MLCACHE=32
fi
M_ZK=${MEM_ZK:-$M_ZK} M_BK=${MEM_BOOKIE:-$M_BK} M_BR=${MEM_BROKER:-$M_BR}
if [ "$JOURNALS" = 2 ]; then JDIRS="$DATA/bookie/journal-0,$DATA/bookie/journal-1"; else JDIRS="$DATA/bookie/journal"; fi

rm -rf "$NEW"; mkdir -p "$NEW"
cp "$DEF/zookeeper.conf" "$DEF/bookkeeper.conf" "$DEF/broker.conf" "$NEW/"
cp "$DIST/conf/entry_location_rocksdb.conf" "$NEW/"
: > "$NEW/overrides.txt"

# setconf <file> <key> <value>: replace the key's uncommented line (dropping uncommented duplicates: commons-config
# would turn them into a list), else its LAST commented line (the one closest to the default), else append.
setconf() {
  local f=$NEW/$1 k=$2 v=$3 old
  old=$(K="$k" awk 'BEGIN { k = ENVIRON["K"] }
    { t = $0; c = 0
      if (t ~ /^[ \t]*#/) { c = 1; sub(/^[ \t]*#+[ \t]*/, "", t) } else sub(/^[ \t]+/, "", t)
      i = index(t, "="); if (i == 0) next
      kk = substr(t, 1, i - 1); sub(/[ \t]+$/, "", kk); if (kk != k) next
      if (!c && !found) { found = 1; val = substr(t, i + 1) } else if (c) { cfound = 1; cval = substr(t, i + 1) } }
    END { if (found) print (val == "" ? "(empty)" : val); else if (cfound) print "(commented: " cval ")"; else print "(absent)" }' "$f")
  K="$k" V="$v" awk 'BEGIN { k = ENVIRON["K"]; v = ENVIRON["V"] }
    function keyof(s,   t, i, kk) {
      t = s; if (t ~ /^[ \t]*#/) sub(/^[ \t]*#+[ \t]*/, "", t); else sub(/^[ \t]+/, "", t)
      i = index(t, "="); if (i == 0) return ""
      kk = substr(t, 1, i - 1); sub(/[ \t]+$/, "", kk); return kk }
    { L[NR] = $0
      if (keyof($0) == k) { if ($0 ~ /^[ \t]*#/) lastc = NR; else if (!un) un = NR; else dup[NR] = 1 } }
    END { tgt = un ? un : lastc
      for (i = 1; i <= NR; i++) { if (i == tgt) print k "=" v; else if (!(i in dup)) print L[i] }
      if (!tgt) { print ""; print "# added by mkconf.sh (key not in the 4.2.4 default file)"; print k "=" v } }' \
    "$f" > "$f.tmp" && mv "$f.tmp" "$f" || die "setconf $1 $k failed"
  printf '%-27s %-45s %s -> %s\n' "$1" "$k" "$old" "$v" >> "$NEW/overrides.txt"
}
apply_set() {  # apply_set <file> "k=v;k=v" : the opt-in knobs
  local f=$1 kv arr
  IFS=';' read -r -a arr <<< "$2"
  for kv in "${arr[@]}"; do
    kv=${kv#"${kv%%[![:space:]]*}"}; [ -n "$kv" ] || continue
    case $kv in *=*) setconf "$f" "${kv%%=*}" "${kv#*=}";; *) die "bad override '$kv' in $f knob (want k=v)";; esac
  done
}

# ---- ZooKeeper: 3-server ensemble on the private IPs, data + txn log on the data disk
Z=zookeeper.conf
setconf $Z dataDir "$DATA/zk/data"
setconf $Z dataLogDir "$DATA/zk/txlog"
setconf $Z metricsProvider.httpPort 8001          # default 8000 = the bookie's http port on the same box
setconf $Z 4lw.commands.whitelist "ruok,mntr,srvr,stat,conf,isro,envi,cons,wchs"   # health; no dump/wchp on big trees
for i in "${!B_PRIV[@]}"; do setconf $Z "server.$((i + 1))" "${B_PRIV[$i]}:2888:3888"; done
# private IP only (default 0.0.0.0 = the droplet's public IP too: an open, unauthenticated ZooKeeper on the internet)
setconf $Z clientPortAddress "$IP"
setconf $Z admin.serverAddress "$IP"
setconf $Z metricsProvider.httpHost "$IP"
apply_set $Z "${ZK_SET:-}"

# ---- bookie: journal fsync on (SPEC durability class), DbLedgerStorage caches explicit, http API on 8000
B=bookkeeper.conf
setconf $B advertisedAddress "$IP"
# listen on the private NIC only (found from the private IP, never a hardcoded name: eth1 on droplets, eth0 in containers)
NIC=$(ip -o -4 addr show 2>/dev/null | awk -v ip="$IP" '{split($4, a, "/"); if (a[1] == ip) {print $2; exit}}')
[ -n "$NIC" ] && setconf $B listeningInterface "$NIC"
setconf $B httpServerHost "$IP"
setconf $B metadataServiceUri "zk+hierarchical://$ZKSEMI/ledgers"
setconf $B zkServers "$ZKS"                       # deprecated key, kept equal so no tool falls back to localhost
setconf $B journalDirectories "$JDIRS"
setconf $B journalDirectory "${JDIRS%%,*}"        # deprecated single-dir key, kept consistent
setconf $B ledgerDirectories "$DATA/bookie/ledgers"
setconf $B journalSyncData true                   # the default, explicit: fsync before ack (standalone.conf has false)
setconf $B journalPreAllocSizeMB 128
setconf $B dbStorage_writeCacheMaxSizeMb "$WCACHE"
setconf $B dbStorage_readAheadCacheMaxSizeMb "$RCACHE"
setconf $B dbStorage_rocksDB_blockCacheSize "$BCACHE"   # IGNORED by BK 4.17 while entryLocationRocksdbConf exists:
setconf $B entryLocationRocksdbConf "$NODE/entry_location_rocksdb.conf"   # ... the real knob is block_cache= in there
# gcWaitTime stays at the 15-min default (09-30 review: a 1-min GC scan of every ledger is churn; the soak fits the disk)
setconf $B httpServerEnabled true
setconf $B httpServerPort 8000
apply_set $B "${BOOKIE_SET:-}"
old=$(awk -F= '/^[[:space:]]*block_cache=/{print $2; exit}' "$NEW/entry_location_rocksdb.conf")
sed -i "s/^\([[:space:]]*block_cache=\).*/\1$BCACHE/" "$NEW/entry_location_rocksdb.conf"
printf '%-27s %-45s %s -> %s\n' entry_location_rocksdb.conf "block_cache (RocksDB entry-location index)" "$old" "$BCACHE" >> "$NEW/overrides.txt"

# ---- broker
K=broker.conf
setconf $K clusterName bench
setconf $K metadataStoreUrl "zk:$ZKS"
setconf $K configurationMetadataStoreUrl "zk:$ZKS"
setconf $K advertisedAddress "$IP"
setconf $K bindAddress "$IP"                      # private IP only (default 0.0.0.0: public IP too)
setconf $K managedLedgerDefaultEnsembleSize 3
setconf $K managedLedgerDefaultWriteQuorum 3
setconf $K managedLedgerDefaultAckQuorum 2
# ledger rollover stays at the 10/240-min defaults (09-30 review: 2/5 min = ~333 rollovers/s of ZooKeeper churn at 100k
# partitions; a 70-s point never rolls, and a 15-min soak at 1M msg/s writes ~135 GB (lz4) per bookie: fits 387 GB)
setconf $K managedLedgerCacheSizeMB "$MLCACHE"
setconf $K exposeTopicLevelMetricsInPrometheus false
setconf $K brokerDeleteInactiveTopicsEnabled false
setconf $K allowAutoTopicCreation false
setconf $K defaultNumberOfNamespaceBundles 48
setconf $K loadBalancerSheddingEnabled false
setconf $K loadBalancerAutoBundleSplitEnabled false
setconf $K loadBalancerAutoUnloadSplitBundlesEnabled false
setconf $K forceDeleteNamespaceAllowed true
setconf $K forceDeleteTenantAllowed true
apply_set $K "${BROKER_SET:-}"

# ---- per-process JVM env (sourced by pc.sh start; the tools get their own small heaps in pc.sh)
JH=$(ls -d /usr/lib/jvm/java-21-openjdk-* 2>/dev/null | head -1)
common_env() {
  echo "# $1 JVM env for bin/pulsar $2, rendered by mkconf.sh $(ts) on node $IDX ($IP), PROFILE=$PROFILE"
  if [ -n "$JH" ]; then echo "export JAVA_HOME=$JH"; fi
  echo "export PULSAR_LOG_DIR=$LOGS/$1 PULSAR_LOG_FILE=$3 PULSAR_LOG_APPENDER=RollingFile"
}
{ common_env zk zookeeper zookeeper.log
  echo "export PULSAR_MEM=\"$M_ZK\""
  echo "unset PULSAR_GC PULSAR_GC_LOG PULSAR_GC_LOG_DIR PULSAR_EXTRA_OPTS   # Pulsar's JDK 21 default: generational ZGC + AlwaysPreTouch"
  echo "export PULSAR_ZK_CONF=$NODE/zookeeper.conf"; } > "$NEW/zk.env"
{ common_env bookie bookie bookie.log
  echo "export BOOKIE_MEM=\"$M_BK\""
  echo "unset PULSAR_MEM BOOKIE_GC BOOKIE_GC_LOG BOOKIE_GC_LOG_DIR PULSAR_GC PULSAR_GC_LOG PULSAR_GC_LOG_DIR BOOKIE_EXTRA_OPTS PULSAR_EXTRA_OPTS   # BOOKIE_MEM would fall back to PULSAR_MEM"
  echo "export BOOKIE_LOG_DIR=$LOGS/bookie"
  echo "export PULSAR_BOOKKEEPER_CONF=$NODE/bookkeeper.conf"; } > "$NEW/bookie.env"
{ common_env broker broker broker.log
  echo "export PULSAR_MEM=\"$M_BR\""
  echo "unset PULSAR_GC PULSAR_GC_LOG PULSAR_GC_LOG_DIR PULSAR_EXTRA_OPTS   # Pulsar's JDK 21 default: generational ZGC + AlwaysPreTouch"
  echo "export PULSAR_BROKER_CONF=$NODE/broker.conf"; } > "$NEW/broker.env"
for r in zk bookie broker; do
  m=$(grep -oE '_MEM="[^"]*"' "$NEW/$r.env")
  if [ $r = bookie ]; then d="BOOKIE_MEM -Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g"; else d="PULSAR_MEM -Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"; fi
  printf '%-27s %-45s %s -> %s\n' "$r.env" "${r} JVM memory" "($d)" "${m#*=}" >> "$NEW/overrides.txt"
done
{ echo "NODE_IDX=$IDX"; echo "NODE_IP=$IP"; echo "PROFILE=$PROFILE"; echo "JOURNALS=$JOURNALS"
  echo "ZK_SET=\"${ZK_SET:-}\""; echo "BOOKIE_SET=\"${BOOKIE_SET:-}\""; echo "BROKER_SET=\"${BROKER_SET:-}\""
  echo "MEM_ZK=\"$M_ZK\""; echo "MEM_BOOKIE=\"$M_BK\""; echo "MEM_BROKER=\"$M_BR\""
  echo "RENDERED=$(ts)"; echo "PULSAR_VERSION=$(grep '^version=' "$DIST/.installed" 2>/dev/null | cut -d= -f2)"; } > "$NEW/knobs.env"
{ for f in zookeeper.conf bookkeeper.conf broker.conf; do
    diff -u --label "conf-defaults/$f" --label "node$IDX/$f" "$DEF/$f" "$NEW/$f"; done
  diff -u --label "dist/conf/entry_location_rocksdb.conf" --label "node$IDX/entry_location_rocksdb.conf" \
    "$DIST/conf/entry_location_rocksdb.conf" "$NEW/entry_location_rocksdb.conf"; } > "$NEW/overrides.diff"

running=$(pgrep -f "org.apache.(zookeeper.server.quorum.QuorumPeerMain|bookkeeper.server.Main|pulsar.PulsarBrokerStarter) .*$NODE/" | tr '\n' ' ')
[ -n "$running" ] && log "WARN mkconf on node $IDX while pids $running run: they keep the config they started with"
rm -rf "$NODE.old"; [ -d "$NODE" ] && mv "$NODE" "$NODE.old"
mv "$NEW" "$NODE" && rm -rf "$NODE.old"
log "mkconf node $IDX ($IP) PROFILE=$PROFILE JOURNALS=$JOURNALS: $(wc -l < "$NODE/overrides.txt") overrides -> $NODE" \
    "| zk $M_ZK | bookie $M_BK wcache=${WCACHE}M rcache=${RCACHE}M rocksdb=$((BCACHE / 1048576))M | broker $M_BR mlcache=${MLCACHE}M"
