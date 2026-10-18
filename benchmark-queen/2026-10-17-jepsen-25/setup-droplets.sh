#!/usr/bin/env bash
# setup-droplets.sh: from Alice's Mac, prepare the 7 droplets of the 2.5.0 campaign (fra1, one VPC, Ubuntu 24.04).
# n1..n6 in the order she listed them, the control as ctl; names map to private addresses in /etc/hosts everywhere.
set -u
CTL=${JEPSEN_CTL:?the control droplet address}
PUB=(${JEPSEN_PUB:?the six node addresses, n1 first})
PRIV=(10.114.0.6 10.114.0.5 10.114.0.4 10.114.0.3 10.114.0.2 10.114.0.7); CTL_PRIV=10.114.0.8
SSH="ssh -n -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new"
HOSTS=$(for i in 0 1 2 3 4 5; do echo "${PRIV[$i]} n$((i+1))"; done; echo "$CTL_PRIV ctl")
log() { echo "[$(date -u +%T)] $*"; }

log "1. /etc/hosts on all seven"
for ip in $CTL "${PUB[@]}"; do
  $SSH root@$ip "sed -i '/ n[1-6]\$/d; / ctl\$/d' /etc/hosts; printf '%s\n' \"$HOSTS\" >> /etc/hosts" &
done; wait

log "2. the control's key on every node"
$SSH root@$CTL "[ -f /root/.ssh/id_ed25519 ] || ssh-keygen -q -t ed25519 -N '' -C jepsen-control -f /root/.ssh/id_ed25519"
K=$($SSH root@$CTL "cat /root/.ssh/id_ed25519.pub")
for ip in "${PUB[@]}"; do $SSH root@$ip "grep -qF '$K' /root/.ssh/authorized_keys || echo '$K' >> /root/.ssh/authorized_keys" & done; wait
$SSH root@$CTL "for n in n1 n2 n3 n4 n5 n6; do ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new root@\$n hostname; done" | tr '\n' ' '; echo

log "3. packages: the control (JDK, gnuplot, Leiningen), the nodes (lazyfs build deps)"
$SSH root@$CTL "export DEBIAN_FRONTEND=noninteractive; apt-get -qq update && apt-get -qq install -y openjdk-21-jdk-headless gnuplot-nox graphviz git curl unzip python3 > /root/apt.log 2>&1; \
  [ -x /usr/local/bin/lein ] || { curl -fsSL -o /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/2.11.2/bin/lein && chmod +x /usr/local/bin/lein; }; \
  LEIN_ROOT=1 lein version 2>&1 | tail -1" &
for ip in "${PUB[@]}"; do
  $SSH root@$ip "export DEBIAN_FRONTEND=noninteractive; apt-get -qq update && apt-get -qq install -y g++ cmake libfuse3-dev fuse3 git > /root/apt.log 2>&1; echo \$(hostname) apt rc=\$?" &
done; wait
log "setup done"
