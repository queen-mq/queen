#!/usr/bin/env bash
# buildx.sh <base> <name> <file> <sed-expr> (Mac): an EXPERIMENT binary for the bench only. On the first broker of
# HOSTS_ENV, in the build dir build1.sh left for <base> (/root/p3/build-<base>, target/ warm), apply one sed
# expression to <file> (a path under the build dir), build, undo the edit, and hand the binary to all three brokers
# as /root/p3/bin/<name>/queen (kept in ../bin/<name>/queen). The source on the Mac is not touched.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
  BASE=${1:?}; NAME=${2:?}; FILE=${3:?}; EXPR=${4:?}
  local h=${BP[0]}
  $SSH root@$h "set -e; cd /root/p3/build-$BASE
    cp $FILE /root/p3/buildx-orig.rs
    sed -i -E '$EXPR' $FILE
    if cmp -s $FILE /root/p3/buildx-orig.rs; then echo 'the sed expression changed nothing'; exit 1; fi
    diff /root/p3/buildx-orig.rs $FILE | head -6 || true
    cd server; t0=\$(date +%s)
    CARGO_INCREMENTAL=0 /root/.cargo/bin/cargo build --release --locked > /root/p3/build-$NAME.log 2>&1 || { cp /root/p3/buildx-orig.rs ../$FILE; tail -5 /root/p3/build-$NAME.log; exit 1; }
    cp target/release/queen /root/p3/queen-$NAME
    cp /root/p3/buildx-orig.rs ../$FILE
    echo \"$NAME built in \$(( \$(date +%s) - t0 )) s: \$(md5sum /root/p3/queen-$NAME | cut -c1-8)\"" || { echo "BUILD FAILED"; exit 1; }
  mkdir -p "$HERE/../bin/$NAME"
  $SCP root@$h:/root/p3/queen-$NAME "$HERE/../bin/$NAME/queen"
  for i in 0 1 2; do
    ( $SSH root@${BP[$i]} "mkdir -p /root/p3/bin/$NAME"
      $SCP "$HERE/../bin/$NAME/queen" root@${BP[$i]}:/root/p3/bin/$NAME/queen
      $SSH root@${BP[$i]} "chmod +x /root/p3/bin/$NAME/queen; echo n$((i+1)): \$(md5sum /root/p3/bin/$NAME/queen | cut -c1-8)" ) &
  done
  wait
  echo BUILDX_DONE $NAME
}
main "$@"; exit $?
