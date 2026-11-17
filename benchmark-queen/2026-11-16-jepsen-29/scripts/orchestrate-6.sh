#!/bin/bash
# Campaign 4 (2026-11-16), one of four rigs, detached: the WHOLE campaign again
# on next-release 4b641a866 (broker = 2.9.0-beta.3 df952a5da, the stage image;
# harness with every fix of campaign 3), from /tmp/src-c4.tgz:
#   1. the source in /root/src6;  2. the broker and the SDK runners built on
#   rig 1 (ROLE=builder), copied to rigs 2-4;  3. one short test, then this
#   rig's share: the gate (/root/share6-gate.txt, RUNS=/root/runs-6-gate), the
#   rest on five nodes (/root/share6-rest5.txt, RUNS=/root/runs-6), the six-node
#   tests (/root/share6-six.txt, RUNS=/root/runs-6-six);  4. the rig powered
#   off, not deleted.
set -u
exec >> /root/ORCHESTRATE-6.log 2>&1
export PATH=/root/.cargo/bin:$PATH
say() { echo "$(date -u +%FT%TZ) $*"; }
BIN=/root/bin/queen-c4
ROLE=${ROLE:-runner}
O="-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
say "orchestrate-6 start: $(hostname -s) role $ROLE, src md5 $(md5sum /tmp/src-c4.tgz | cut -d' ' -f1)"
rm -rf /root/src6 && mkdir -p /root/src6 && tar -xzf /tmp/src-c4.tgz -C /root/src6 2>/dev/null \
  || { say "extract failed: stopping, nothing powered off"; exit 1; }
if [ "$ROLE" = builder ]; then
  say "build start"
  if (cd /root/src6/server && CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/root/src2/server/target \
        cargo build --release --locked 2>&1 | tail -3) \
     && cp /root/src2/server/target/release/queen $BIN.tmp && mv $BIN.tmp $BIN; then
    say "broker: $(md5sum $BIN | cut -d' ' -f1) $($BIN --help 2>/dev/null | head -0)"
  else
    say "broker build FAILED: stopping, nothing powered off"; exit 1
  fi
  if SRC=/root/src6 OUT=/root/runners-c4.tgz bash /root/src6/test/jepsen/rig/build-runners.sh > /root/build-6-runners.log 2>&1; then
    cp /root/runners-c4.tgz /root/runners.tgz; say "runners: $(md5sum /root/runners.tgz | cut -d' ' -f1)"
  else
    say "runners build FAILED ($(tail -2 /root/build-6-runners.log | tr '\n' ' ')): stopping, nothing powered off"; exit 1
  fi
  for ip in 10.250.0.22 10.250.0.35 10.250.0.253; do
    if scp -q $O /root/runners.tgz root@$ip:/root/runners.tgz.tmp < /dev/null \
       && ssh -n $O root@$ip "mv /root/runners.tgz.tmp /root/runners.tgz" \
       && scp -q $O $BIN root@$ip:$BIN.tmp < /dev/null && ssh -n $O root@$ip "mv $BIN.tmp $BIN"; then
      say "copied to $ip"
    else
      say "copy to $ip FAILED"
    fi
  done
else
  for _ in $(seq 1 720); do [ -s $BIN ] && break; sleep 10; done
  [ -s $BIN ] || { say "no broker from rig 1 after 2 h: stopping, nothing powered off"; exit 1; }
  sleep 5
  say "broker here: $(md5sum $BIN | cut -d' ' -f1), runners $(md5sum /root/runners.tgz | cut -d' ' -f1)"
fi
say "lazyfs on n6: $(ssh -n $O root@n6 'cat /opt/jepsen/lazyfs/.queen-built 2>/dev/null || echo NOT-built')"
# Old node logs are compressed whenever the disk passes 70% (nothing deleted).
( while true; do
    use=$(df --output=pcent /root | tail -1 | tr -dc 0-9)
    if [ "${use:-0}" -gt 70 ]; then
      find /root/src/test/jepsen/store /root/src2/test/jepsen/store /root/src3/test/jepsen/store /root/src4/test/jepsen/store /root/src5/test/jepsen/store /root/src6/test/jepsen/store -name '*.log' -size +5M -mmin +60 -exec nice -n 19 gzip -9 {} \; 2>/dev/null
      say "disk ${use}%: compressed old logs, now $(df --output=pcent /root | tail -1)"
    fi
    sleep 600
  done ) &
guard=$!
J=/root/src6/test/jepsen
cd "$J" || { say "no harness in $J"; exit 1; }
lein run test --nodes n1,n2,n3,n4,n5 --username root --ssh-private-key /root/.ssh/id_ed25519 \
  --bin "$BIN" --concurrency 2n --workload duppush --nemesis none \
  --time-limit 60 --rate 100 > /root/first-test-6.log 2>&1
say "first test: $(grep -E 'Everything looks good|Analysis invalid|Errors occurred during analysis' /root/first-test-6.log | head -1)"
if ! grep -q "Everything looks good" /root/first-test-6.log; then
  say "the first test is not valid: stopping here, nothing is powered off"; exit 1
fi
for d in /root/runs-6-gate /root/runs-6 /root/runs-6-six; do mkdir -p $d; cp /root/runs-3/progress.py $d/ 2>/dev/null; done
RUNS=/root/runs-6-gate BIN=$BIN ./run-campaign.sh /root/share6-gate.txt
say "gate done: $(grep ' done  ' /root/runs-6-gate/summary.txt | grep -c ': valid') valid of $(grep -c ' done  ' /root/runs-6-gate/summary.txt)"
RUNS=/root/runs-6 BIN=$BIN ./run-campaign.sh /root/share6-rest5.txt
NODES=n1,n2,n3,n4,n5,n6 RUNS=/root/runs-6-six BIN=$BIN ./run-campaign.sh /root/share6-six.txt
say "campaign 4 done: $(cat /root/runs-6*/summary.txt | grep -c ' done  ') tests, $(cat /root/runs-6*/summary.txt | grep ' done  ' | grep -c ': valid') valid; powering the rig off"
kill "$guard"
for n in n1 n2 n3 n4 n5 n6; do ssh -n $O root@$n 'shutdown -h now' || true; done
shutdown -h +2
