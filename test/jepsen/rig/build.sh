#!/bin/sh
# The brokers under test, on the controller:
#   /root/bin/queen-master    the source in /root/src (cargo build --release --locked)
#   /root/bin/queen-old       --bin-old of the upgrade tests: the previous release,
#                             from its image (docker), or a binary you copy there
# A build of another tree (the routing snapshot) goes the same way:
#   SRC=/root/homes2 OUT=/root/bin/queen-homes2 sh build.sh
set -e
SRC=${SRC:-/root/src}
OUT=${OUT:-/root/bin/queen-master}
mkdir -p /root/bin
(cd "$SRC/server" && CARGO_INCREMENTAL=0 /root/.cargo/bin/cargo build --release --locked 2>&1 | tail -2)
cp "$SRC/server/target/release/queen" "$OUT"
md5sum "$OUT"
if [ -n "${OLD_IMAGE:-}" ] && [ ! -f /root/bin/queen-old ]; then
  docker pull -q "$OLD_IMAGE" >/dev/null
  id=$(docker create "$OLD_IMAGE")
  docker cp "$id:/app/bin/queen" /root/bin/queen-old
  docker rm "$id" >/dev/null
  md5sum /root/bin/queen-old
fi
