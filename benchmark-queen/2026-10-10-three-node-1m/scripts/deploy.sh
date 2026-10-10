#!/usr/bin/env bash
# deploy.sh <dir with queen-210, queen-rc7 and qload> : binaries, scripts, OS settings and chrony on the six machines.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"; BIN=${1:?a directory with the binaries}
PEERS="1=${BV[0]}:7400/${BV[0]}:6632,2=${BV[1]}:7400/${BV[1]}:6632,3=${BV[2]}:7400/${BV[2]}:6632"
for i in 0 1 2; do
  ( n=$((i+1)); h=${BP[$i]}
    ssh root@$h 'mkdir -p /root/p3/bin /root/p3/logs /root/p3/run /root/p3/runs'
    scp -q "$BIN/queen-210" "$BIN/queen-rc7" root@$h:/root/p3/bin/
    scp -q "$HERE/qc.sh" "$HERE/sampler.py" "$HERE/os-tune.sh" root@$h:/root/p3/
    printf 'NODE=%s\nPRIV=%s\nPEERS="%s"\n' $n ${BV[$i]} "$PEERS" | ssh root@$h 'cat > /root/p3/node.env'
    ssh root@$h 'chmod +x /root/p3/bin/* /root/p3/*.sh; bash /root/p3/os-tune.sh broker >/dev/null; DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony >/dev/null 2>&1; systemctl is-active chrony' ) &
done
for h in "${LP[@]}"; do
  ( ssh root@$h 'mkdir -p /root/p3/bin /root/p3/runs'
    scp -q "$BIN/qload" root@$h:/root/p3/bin/; scp -q "$HERE/ld.sh" "$HERE/os-tune.sh" root@$h:/root/p3/
    ssh root@$h 'chmod +x /root/p3/bin/qload /root/p3/ld.sh; bash /root/p3/os-tune.sh loader >/dev/null; DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony >/dev/null 2>&1; systemctl is-active chrony' ) &
done; wait
