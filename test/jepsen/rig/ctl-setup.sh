#!/bin/sh
# The Jepsen controller, as root: JDK 21 + Leiningen, Rust (the brokers are
# built here), graphviz (Elle's cycle plots), gnuplot (Jepsen's plots),
# Docker (the connector services), a root SSH key for the nodes, the source
# from /tmp/src.tgz in /root/src.
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get -qq update >/dev/null
apt-get -qq install -y openjdk-21-jdk-headless leiningen git build-essential pkg-config libssl-dev \
  clang curl gnuplot-nox graphviz docker.io ntpdate >/dev/null 2>&1
[ -f /root/.ssh/id_ed25519 ] || ssh-keygen -q -t ed25519 -N '' -f /root/.ssh/id_ed25519
mkdir -p /root/src /root/bin
# The nodes and this JVM both reach the connector services as `ctl`.
grep -q ' ctl$' /etc/hosts || echo "$(hostname -I | awk '{print $1}') ctl" >> /etc/hosts
[ -f /tmp/src.tgz ] && tar xzf /tmp/src.tgz -C /root/src
if [ ! -x /root/.cargo/bin/cargo ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.99.0 --profile minimal >/dev/null 2>&1
fi
(cd /root/src/test/jepsen && lein deps >/dev/null)
echo "ctl ready: $(java -version 2>&1 | head -1) | $(lein version 2>/dev/null | head -1) | $(/root/.cargo/bin/rustc --version) | $(docker --version)"
echo "public key for node-setup.sh: $(cat /root/.ssh/id_ed25519.pub)"
