#!/bin/sh
# A Jepsen node: root logs in with the controller's key, n1..n5 resolve, the nemeses' tools and lazyfs's build deps.
set -e
export DEBIAN_FRONTEND=noninteractive
mkdir -p /root/.ssh && chmod 700 /root/.ssh
grep -q 'root@jepsen-ctl' /root/.ssh/authorized_keys 2>/dev/null || echo 'ssh-ed25519 <the controller key, /root/.ssh/id_ed25519.pub> root@jepsen-ctl' >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
echo 'PermitRootLogin prohibit-password' > /etc/ssh/sshd_config.d/99-jepsen.conf
systemctl reload ssh 2>/dev/null || systemctl reload sshd 2>/dev/null || true
grep -q ' n1$' /etc/hosts || cat >> /etc/hosts <<'H'
10.250.0.216 jepsen-ctl
10.250.0.217 n1
10.250.0.218 n2
10.250.0.219 n3
10.250.0.220 n4
10.250.0.221 n5
H
apt-get -qq update >/dev/null
apt-get -qq install -y iptables psmisc curl wget faketime netcat-openbsd iproute2 fuse3 libfuse3-dev git build-essential cmake pkg-config tar bzip2 unzip rsyslog logrotate >/dev/null 2>&1
echo "$(hostname) ready"
