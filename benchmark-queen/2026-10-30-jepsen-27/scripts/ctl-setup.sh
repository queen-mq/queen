#!/bin/sh
# Jepsen controller: JDK + Leiningen, Rust 1.99.0 for the queen binary, root SSH key, the source.
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get -qq update >/dev/null
apt-get -qq install -y openjdk-21-jdk-headless leiningen git build-essential pkg-config libssl-dev clang curl gnuplot-nox >/dev/null 2>&1
[ -f /root/.ssh/id_ed25519 ] || ssh-keygen -q -t ed25519 -N '' -f /root/.ssh/id_ed25519
mkdir -p /root/src && tar xzf /tmp/src.tgz -C /root/src
if [ ! -x /root/.cargo/bin/cargo ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.99.0 --profile minimal >/dev/null 2>&1
fi
echo "ctl ready: $(java -version 2>&1 | head -1) | $(lein version 2>/dev/null | head -1) | $(/root/.cargo/bin/rustc --version)"
