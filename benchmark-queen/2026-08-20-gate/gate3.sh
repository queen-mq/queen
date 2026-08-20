#!/bin/bash
# Rate-MATCHED pass: push below each shape's admit ceiling so no backlog forms
# and e2e latency measures the path, not the queue depth. Ceilings measured in
# the previous pass: 1 hop 893/s, 2 hops 367/s, capped shapes 158/s.
set -uo pipefail
Q=46.101.193.166; L=209.38.206.19; QPRIV=10.114.0.2
B="$(dirname "$0")"; OUT="$B/out"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"
for cfg in "r1-1hop-600s:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -rate 600 -graph gr1" \
           "r2-2hop-250s:-hops 2 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -mid-cap 0 -rate 250 -graph gr2" \
           "r3-1hop-paced150:-hops 1 -cap 2000 -period 10 -lease 5 -pace-batch 1000 -rate 150 -graph gr3" \
           "r4-2hop-paced150:-hops 2 -cap 2000 -period 10 -lease 5 -pace-batch 2000 -mid-cap 3000 -rate 150 -graph gr4"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ==="
  $SSH root@$L "/root/gateload -gate http://$QPRIV:8788 -app bench -duration 100 \
      -pushers 16 -consumers 8 -batch 100 $args" 2>&1 | tee "$OUT/$name.load.txt" | grep -E "^\[final\]|declare failed" 
done
echo; echo "=== RATE-MATCHED DONE ==="
