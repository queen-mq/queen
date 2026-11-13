#!/usr/bin/env bash
# qload-install.sh <qload binary> (Mac): put a new qload on the loader as /root/p3/bin/qload (the old one kept as
# qload.prev), and check it knows -push-spread.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts-big.env}"
BIN=${1:?}
$SCP "$BIN" root@${LP[0]}:/root/p3/bin/qload.new
$SSH root@${LP[0]} "chmod +x /root/p3/bin/qload.new && cp /root/p3/bin/qload /root/p3/bin/qload.prev && mv /root/p3/bin/qload.new /root/p3/bin/qload && /root/p3/bin/qload -h 2>&1 | grep -c push-spread"
