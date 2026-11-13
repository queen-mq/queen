#!/usr/bin/env bash
# build1.sh <name> <src.tar> <dist.tar> (Mac): one broker binary, built on the first broker of HOSTS_ENV with the
# toolchain builds.sh used (`cargo build --release --locked`), then handed to all three as /root/p3/bin/<name>/queen
# (through the Mac: the brokers do not hold each other's keys) and kept in ../bin/<name>/queen.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
  NAME=${1:?}; TSRC=${2:?}; TDIST=${3:?}
  local h=${BP[0]}
  $SCP "$TSRC" root@$h:/root/p3/src-$NAME.tar
  $SCP "$TDIST" root@$h:/root/p3/dist.tar
  $SSH root@$h "set -e; rm -rf /root/p3/build-$NAME; mkdir -p /root/p3/build-$NAME; cd /root/p3/build-$NAME
    tar -xf /root/p3/src-$NAME.tar 2>/dev/null; tar -xf /root/p3/dist.tar 2>/dev/null
    cd server; t0=\$(date +%s)
    CARGO_INCREMENTAL=0 /root/.cargo/bin/cargo build --release --locked > /root/p3/build-$NAME.log 2>&1
    echo \"$NAME built in \$(( \$(date +%s) - t0 )) s: \$(md5sum target/release/queen | cut -c1-8)\"" || { echo "BUILD FAILED"; exit 1; }
  mkdir -p "$HERE/../bin/$NAME"
  $SCP root@$h:/root/p3/build-$NAME/server/target/release/queen "$HERE/../bin/$NAME/queen"
  for i in 0 1 2; do
    ( $SSH root@${BP[$i]} "mkdir -p /root/p3/bin/$NAME"
      $SCP "$HERE/../bin/$NAME/queen" root@${BP[$i]}:/root/p3/bin/$NAME/queen
      $SSH root@${BP[$i]} "chmod +x /root/p3/bin/$NAME/queen; echo n$((i+1)): \$(md5sum /root/p3/bin/$NAME/queen | cut -c1-8)" ) &
  done
  wait
  echo BUILD1_DONE
}
main "$@"; exit $?
