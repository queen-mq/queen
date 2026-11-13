#!/usr/bin/env bash
# builds.sh <src-281.tar> <src-sched.tar> <dist.tar> (Mac): the two broker binaries, built at the same time on two of
# the brokers with the same toolchain (rustup stable, `cargo build --release --locked`): 2.8.1 on n1, the footprint
# scheduler's tree on n2. Then both are handed round: /root/p3/bin/281/queen and /root/p3/bin/sched/queen on all three.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
T281=${1:?}; TSCHED=${2:?}; TDIST=${3:?}
build() {  # build <broker index> <name> <tar>
  local h=${BP[$1]} name=$2 tar=$3
  $SCP "$tar" root@$h:/root/p3/src-$name.tar
  $SCP "$TDIST" root@$h:/root/p3/dist.tar
  $SSH root@$h "set -e; rm -rf /root/p3/build-$name; mkdir -p /root/p3/build-$name; cd /root/p3/build-$name
    tar -xf /root/p3/src-$name.tar; tar -xf /root/p3/dist.tar
    cd server; t0=\$(date +%s)
    CARGO_INCREMENTAL=0 /root/.cargo/bin/cargo build --release --locked > /root/p3/build-$name.log 2>&1
    echo \"$name built in \$(( \$(date +%s) - t0 )) s: \$(md5sum target/release/queen | cut -c1-8)\""
}
build 0 281 "$T281" &
build 1 sched "$TSCHED" &
wait
# Hand both round through the Mac (the brokers do not hold each other's keys).
mkdir -p "$HERE/../bin/281" "$HERE/../bin/sched"
$SCP root@${BP[0]}:/root/p3/build-281/server/target/release/queen "$HERE/../bin/281/queen"
$SCP root@${BP[1]}:/root/p3/build-sched/server/target/release/queen "$HERE/../bin/sched/queen"
for i in 0 1 2; do
  ( $SSH root@${BP[$i]} 'mkdir -p /root/p3/bin/281 /root/p3/bin/sched'
    $SCP "$HERE/../bin/281/queen" root@${BP[$i]}:/root/p3/bin/281/queen
    $SCP "$HERE/../bin/sched/queen" root@${BP[$i]}:/root/p3/bin/sched/queen
    $SSH root@${BP[$i]} 'chmod +x /root/p3/bin/*/queen; echo "n'$((i+1))': $(md5sum /root/p3/bin/281/queen | cut -c1-8) $(md5sum /root/p3/bin/sched/queen | cut -c1-8)"' ) &
done
wait
