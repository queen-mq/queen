#!/usr/bin/env bash
# txn/deploy.sh (Mac) — ship what the transactional benchmark needs, without touching the throughput matrix's loaders:
#   mqload/bin/linux-amd64/{kload,pload,qload} -> $REMOTE_ROOT/bin/txn/ on the 3 loaders (the matrix's $REMOTE_ROOT/bin stays)
#   common/sampler.sh (role qload) -> every broker and loader
# Build first: (cd mqload && GOWORK=off ./build.sh). Refuses while a kload/pload/qload runs on a loader. Bash 3.2-clean.
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
. "$HOSTS_ENV" || exit 2
R=${REMOTE_ROOT:-/root/bench}; BIN_ARCH=${BIN_ARCH:-linux-amd64}
B=$H/mqload/bin/$BIN_ARCH
for t in kload pload qload; do [ -x "$B/$t" ] || { echo "no $B/$t: build mqload first"; exit 2; }; done
log() { echo "[$(date -u +%FT%TZ)] txn/deploy: $*"; }
for h in "${L_PUB[@]}"; do
  if $SSH root@$h "pgrep -x kload >/dev/null || pgrep -x pload >/dev/null || pgrep -x qload >/dev/null"; then
    log "loader $h runs a load process: refusing (nothing may change under a running point)"; exit 1
  fi
done
for h in "${L_PUB[@]}"; do
  ( for t in kload pload qload; do
      $SSH root@$h "mkdir -p $R/bin/txn && cat > $R/bin/txn/.$t.tmp && chmod +x $R/bin/txn/.$t.tmp && mv $R/bin/txn/.$t.tmp $R/bin/txn/$t" < "$B/$t" || exit 1
    done
    $SSH root@$h "cat > $R/common/.sampler.sh.tmp && mv $R/common/.sampler.sh.tmp $R/common/sampler.sh && chmod +x $R/common/sampler.sh" < "$H/common/sampler.sh"
    echo "  loader $h: $($SSH root@$h "cd $R/bin/txn && sha256sum kload pload qload | cut -c1-16 | tr '\n' ' '")" ) &
done
for h in "${B_PUB[@]}"; do
  ( $SSH root@$h "mkdir -p $R/common && cat > $R/common/.sampler.sh.tmp && mv $R/common/.sampler.sh.tmp $R/common/sampler.sh && chmod +x $R/common/sampler.sh" < "$H/common/sampler.sh" \
      && echo "  broker $h: sampler.sh updated" ) &
done
wait
log "local shas: $(cd "$B" && shasum -a 256 kload pload qload | cut -c1-16 | tr '\n' ' ')"
