#!/usr/bin/env bash
# m-usebin.sh base|M — install one of the two archived TASK M builds as the
# broker binary and restart it.
#
# Every measured point in this phase records which build produced it, because
# the phase CHANGES THE BROKER: `base` is the pre-feature build (the one every
# earlier phase measured), `M` adds queen.queues.min_pop_wait_time. The two are
# byte-for-byte archived under /root/campaign/M/bin so a number can always be
# re-attributed to a build by hash.
set -euo pipefail
BIN=/root/queen/server/target/release/queen-seg
case "${1:?base|M}" in
  base) SRC=/root/campaign/M/bin/queen-seg-base;;
  M)    SRC=/root/campaign/M/bin/queen-seg-M;;
  *) echo "usage: m-usebin.sh base|M" >&2; exit 2;;
esac
cp -f "$SRC" "$BIN"
systemctl restart queen-broker
for _ in $(seq 1 120); do
  curl -sf http://127.0.0.1:6632/health >/dev/null 2>&1 && break
  sleep 0.5
done
echo "[usebin] $1 $(sha256sum "$BIN" | cut -c1-16) broker=$(systemctl is-active queen-broker)"
