#!/usr/bin/env bash
# buildfp.sh (Mac): the sched3 source already on n1 (/root/p3/build-sched3), built again with frame pointers
# (-C force-frame-pointers=yes) into its own target dir, for `perf record --call-graph fp`. Handed to all three
# brokers as /root/p3/bin/sched3fp/queen (through the Mac) and kept in ../bin/sched3fp/queen.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts-big.env}"
  local h=${BP[0]}
  $SSH root@$h "set -e; cd /root/p3/build-sched3/server; t0=\$(date +%s)
    RUSTFLAGS='-C force-frame-pointers=yes' CARGO_TARGET_DIR=/root/p3/build-sched3/server/target-fp CARGO_INCREMENTAL=0 \
      /root/.cargo/bin/cargo build --release --locked > /root/p3/build-sched3fp.log 2>&1
    echo \"sched3fp built in \$(( \$(date +%s) - t0 )) s: \$(md5sum target-fp/release/queen | cut -c1-8)\"" || { echo "BUILD FAILED"; exit 1; }
  mkdir -p "$HERE/../bin/sched3fp"
  $SCP root@$h:/root/p3/build-sched3/server/target-fp/release/queen "$HERE/../bin/sched3fp/queen"
  for i in 0 1 2; do
    ( $SSH root@${BP[$i]} "mkdir -p /root/p3/bin/sched3fp"
      $SCP "$HERE/../bin/sched3fp/queen" root@${BP[$i]}:/root/p3/bin/sched3fp/queen
      $SSH root@${BP[$i]} "chmod +x /root/p3/bin/sched3fp/queen; echo n$((i+1)): \$(md5sum /root/p3/bin/sched3fp/queen | cut -c1-8)" ) &
  done
  wait
  echo BUILDFP_DONE
}
main "$@"; exit $?
