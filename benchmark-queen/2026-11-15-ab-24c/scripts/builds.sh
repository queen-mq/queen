#!/usr/bin/env bash
# builds.sh (Mac): the two broker binaries, built at the same time with the same toolchain (rustup stable,
# `cargo build --release --locked`): 2.8.1 (tag v2.8.1, 6cfe112d3) on broker 1, the build under test (jepsen-all,
# 585112e1d) on broker 2, each with ITS OWN console (bin/dist-<name>.tar: server/webapp/dist built by `vite build`
# from that revision's app/ + shared/; proxy/console/dist is committed and comes with the sources). Each build runs detached (nohup) and leaves /root/p3/build-<name>.done or .failed;
# handround.sh (on the controller) then gives both to all three as /root/p3/bin/<name>/queen.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts-mac.env}"; B=$HERE/../bin
build() {  # build <broker index> <name> <tgz>
  local h=${BP[$1]} name=$2 tgz=$3
  $SCP "$tgz" "$B/dist-$name.tar" root@$h:/root/p3/
  $SSH root@$h "rm -f /root/p3/build-$name.done /root/p3/build-$name.failed; rm -rf /root/p3/build-$name; mkdir -p /root/p3/build-$name; cd /root/p3/build-$name
    tar -xzf /root/p3/$(basename $tgz); rm -rf server/webapp/dist; tar -xf /root/p3/dist-$name.tar; test -n \"\$(ls server/webapp/dist/assets/index-*.js)\" || { echo NO CONSOLE in $name; exit 1; }
    nohup bash -c 'cd server; t0=\$(date +%s); if CARGO_INCREMENTAL=0 /root/.cargo/bin/cargo build --release --locked > /root/p3/build-$name.log 2>&1; then echo \"$name built in \$(( \$(date +%s) - t0 )) s: \$(md5sum target/release/queen | cut -c1-8)\" > /root/p3/build-$name.done; else tail -5 /root/p3/build-$name.log > /root/p3/build-$name.failed; fi' > /dev/null 2>&1 < /dev/null &
    echo started $name on \$(hostname)"
}
build 0 281 "$B/src-281.tgz" &
build 1 new "$B/src-new.tgz" &
wait
