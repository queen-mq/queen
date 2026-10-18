#!/usr/bin/env bash
# build-lazyfs.sh: lazyfs 0.3.0 built on n1..n6 before the first test, by the harness's own recipe
# (test/jepsen/src/jepsen/queen/lazyfs.clj install!: jepsen.lazyfs's repo, commit, dir and marker).
PUB=(${JEPSEN_PUB:?the six node addresses, n1 first})
R='set -e; mkdir -p /opt/jepsen; [ -d /opt/jepsen/lazyfs ] || git clone -q https://github.com/dsrhaslab/lazyfs.git /opt/jepsen/lazyfs
cd /opt/jepsen/lazyfs; git fetch -q; git checkout -q 0.3.0; git clean -fxq
(cd libs/libpcache && ./build.sh) > /root/lazyfs-build.log 2>&1; (cd lazyfs && ./build.sh) >> /root/lazyfs-build.log 2>&1
echo 0.3.0 > /opt/jepsen/lazyfs/.queen-built; sed -i "/\s*user_allow_other/s/^#//g" /etc/fuse.conf
echo "$(hostname) lazyfs $(cat /opt/jepsen/lazyfs/.queen-built) $(ls -la /opt/jepsen/lazyfs/lazyfs/build/lazyfs | awk "{print \$5}") bytes"'
for i in 0 1 2 3 4 5; do ( ssh -n -o BatchMode=yes -o ConnectTimeout=15 root@${PUB[$i]} "$R" 2>&1 | tail -1 | sed "s/^/n$((i+1)): /" ) & done; wait
