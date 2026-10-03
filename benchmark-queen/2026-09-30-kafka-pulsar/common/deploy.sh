#!/usr/bin/env bash
# deploy.sh <step>... — run from the Mac, harness root = this file's parent's parent. Steps, in any order:
#   harness   copy common/ kafka/ pulsar/ redpanda/ hosts.env and mqload/bin/$BIN_ARCH/* (-> bin/) to $REMOTE_ROOT on all hosts
#   tune      common/os-tune.sh broker|loader on every host (runtime sysctls, SPEC.md §5 OS)
#   kafka     kafka/install.sh on the 3 brokers        pulsar   pulsar/install.sh on the 3 brokers
#   redpanda  redpanda/install.sh on the 3 brokers (official apt repo, pinned 26.2.3-1; installs only, the Redpanda tuners
#             run later and on purpose: redpanda/cluster.sh reset applies them, redpanda/cluster.sh untune restores the OS)
#   clock     clock offsets on every host (e2e crosses loaders: skew shows up as latency)
#   check     who listens on the system ports on each broker (nothing may run while another system is measured):
#             Queen 6632/7400, Kafka 9092/9093, Pulsar 2181/3181/6650/8080, Redpanda 9092/9644/33145 (+8081/8082 if its
#             HTTP proxy / schema registry were on; the harness turns them off)
# Env: BIN_ARCH (linux-amd64 on the droplets, linux-arm64 in the local rehearsal), CACHE passed to install.sh.
set -u
D=$(cd "$(dirname "$0")/.." && pwd)
. "$D/hosts.env" || { echo "no $D/hosts.env (copy hosts.env.example)"; exit 2; }
R=${REMOTE_ROOT:-/root/bench}; BIN_ARCH=${BIN_ARCH:-linux-amd64}
LOG=$D/deploy-logs; mkdir -p "$LOG"
ALL=("${B_PUB[@]}" "${L_PUB[@]}")
role() { local h=$1; for b in "${B_PUB[@]}"; do [ "$b" = "$h" ] && { echo broker; return; }; done; echo loader; }
each() {  # each <label> <hosts...> -- <remote command>: in parallel, one log per host, summary line per host
  local label=$1; shift; local hosts=(); while [ "$1" != "--" ]; do hosts+=("$1"); shift; done; shift
  local cmd="$*" pids=() h
  for h in "${hosts[@]}"; do
    ( $SSH root@$h "$(printf '%s' "$cmd" | sed "s#@ROLE@#$(role $h)#g")" > "$LOG/$label-$h.log" 2>&1; echo $? > "$LOG/$label-$h.rc" ) &
    pids+=($!)
  done
  wait "${pids[@]}"
  for h in "${hosts[@]}"; do printf '  %-8s %-16s rc=%s  %s\n' "$label" "$h" "$(cat "$LOG/$label-$h.rc")" "$(tail -1 "$LOG/$label-$h.log" | cut -c1-110)"; done
}
for step in "$@"; do
  echo "[$(date -u +%T)] deploy $step"
  case $step in
    harness)
      [ -d "$D/mqload/bin/$BIN_ARCH" ] || echo "  WARN no $D/mqload/bin/$BIN_ARCH (build mqload first): loaders get no binaries"
      TMP=$(mktemp -d); mkdir -p "$TMP/bench/bin"
      ( cd "$D" && COPYFILE_DISABLE=1 tar --no-xattrs -cf - common kafka pulsar redpanda report hosts.env SPEC.md 2>/dev/null ) | ( cd "$TMP/bench" && tar -xf - 2>/dev/null )
      [ -d "$D/mqload/bin/$BIN_ARCH" ] && cp "$D/mqload/bin/$BIN_ARCH/"* "$TMP/bench/bin/"
      for h in "${ALL[@]}"; do
        ( cd "$TMP" && COPYFILE_DISABLE=1 tar --no-xattrs -czf - bench 2>/dev/null ) \
          | $SSH root@$h "mkdir -p $R && tar -xzf - -C $(dirname $R) --no-same-owner && chmod +x $R/*/*.sh $R/bin/* 2>/dev/null; echo copied \$(du -sh $R | cut -f1)" > "$LOG/harness-$h.log" 2>&1 &
      done; wait; rm -rf "$TMP"
      for h in "${ALL[@]}"; do printf '  harness  %-16s %s\n' "$h" "$(tail -1 "$LOG/harness-$h.log")"; done ;;
    tune)   each tune "${ALL[@]}" -- "$R/common/os-tune.sh @ROLE@" ;;
    kafka)  each kafka "${B_PUB[@]}" -- "CACHE=${CACHE:-} PROFILE=${PROFILE:-vm} $R/kafka/install.sh" ;;
    pulsar) each pulsar "${B_PUB[@]}" -- "CACHE=${CACHE:-} PROFILE=${PROFILE:-vm} $R/pulsar/install.sh" ;;
    redpanda) each redpanda "${B_PUB[@]}" -- "PROFILE=${PROFILE:-vm} bash $R/redpanda/install.sh" ;;
    clock)  each clock "${ALL[@]}" -- "chronyc tracking 2>/dev/null | awk -F': ' '/System time|Last offset/{printf \"%s=%s \", \$1, \$2} END{print \"\"}' || timedatectl show -p NTPSynchronized" ;;
    check)  each check "${B_PUB[@]}" -- "ss -ltnH 2>/dev/null | awk '{print \$4}' | grep -oE ':(6632|7400|9092|9093|2181|3181|6650|8080|9644|33145|8081|8082)\$' | sort -u | tr '\n' ' '; echo listening" ;;
    *) sed -n '2,14p' "$0"; exit 2 ;;
  esac
done
