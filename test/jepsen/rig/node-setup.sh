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
echo "$(hostname) ready"
