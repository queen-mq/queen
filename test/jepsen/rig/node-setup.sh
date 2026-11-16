#!/bin/sh
# A Jepsen node, as root: the controller's key for root, the rig's names in
# /etc/hosts, the nemeses' tools (iptables, tc for the packet faults, ntpdate
# for the clock), lazyfs's build dependencies, python3 (the Python feed
# runner; the rest of the runners come in their bundle).
#   CTL_KEY="<the controller's id_ed25519.pub>" HOSTS="<ip> n1 ..." sh node-setup.sh
set -e
export DEBIAN_FRONTEND=noninteractive
: "${CTL_KEY:?set CTL_KEY}" "${HOSTS:?set HOSTS}"
mkdir -p /root/.ssh && chmod 700 /root/.ssh
grep -qF "$CTL_KEY" /root/.ssh/authorized_keys 2>/dev/null || echo "$CTL_KEY" >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
echo 'PermitRootLogin prohibit-password' > /etc/ssh/sshd_config.d/99-jepsen.conf
systemctl reload ssh 2>/dev/null || systemctl reload sshd 2>/dev/null || true
grep -q ' n1$' /etc/hosts || printf '%s\n' "$HOSTS" >> /etc/hosts
apt-get -qq update >/dev/null
apt-get -qq install -y iptables iproute2 psmisc curl wget faketime netcat-openbsd fuse3 libfuse3-dev git \
  build-essential cmake pkg-config tar bzip2 unzip rsyslog logrotate ntpdate python3 >/dev/null 2>&1
# lazyfs, built here once (jepsen.queen.lazyfs/install! skips a commit it finds
# built): built during a test instead, on a node no lazyfs test had used yet,
# the build outlasted the test's setup (campaign 3, 2026-11-15: all four lazyfs
# tests on six nodes broke on n6).
LAZYFS=/opt/jepsen/lazyfs LAZYFS_COMMIT=0.3.0
if [ "$(cat $LAZYFS/.queen-built 2>/dev/null)" != "$LAZYFS_COMMIT" ] || [ ! -x $LAZYFS/lazyfs/build/lazyfs ]; then
  [ -d $LAZYFS ] || { mkdir -p /opt/jepsen && git clone -q https://github.com/dsrhaslab/lazyfs.git $LAZYFS; }
  ( cd $LAZYFS && git fetch -q && git checkout -q $LAZYFS_COMMIT && git clean -fxq \
    && (cd libs/libpcache && ./build.sh) >/dev/null 2>&1 && (cd lazyfs && ./build.sh) >/dev/null 2>&1 ) \
    && echo $LAZYFS_COMMIT > $LAZYFS/.queen-built
fi
sed -i '/\s*user_allow_other/s/^#//g' /etc/fuse.conf
echo "$(hostname) ready, lazyfs $(cat $LAZYFS/.queen-built 2>/dev/null || echo NOT built)"
