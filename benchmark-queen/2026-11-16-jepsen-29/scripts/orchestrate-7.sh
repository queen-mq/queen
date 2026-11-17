#!/bin/bash
# Campaign 4 reruns (2026-11-17), rig 1 only, detached: the four ephemeral tests
# and seek-kill on next-release 72d5e6360 (the 2.9.0 server code with the
# ephemeral hand-over fix 5543add64; the seek harness of fc0b6f978), from
# /tmp/src-c5.tgz:
#   1. the source in /root/src7;  2. the broker built here (no SDK runners: no
#   rerun test uses them);  3. one short test, then /root/share7.txt
#   (RUNS=/root/runs-7);  4. the rig powered off, not deleted.
set -u
exec >> /root/ORCHESTRATE-7.log 2>&1
export PATH=/root/.cargo/bin:$PATH
say() { echo "$(date -u +%FT%TZ) $*"; }
BIN=/root/bin/queen-c5
OLD=/root/bin/queen-c4
O="-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
say "orchestrate-7 start: $(hostname -s), src md5 $(md5sum /tmp/src-c5.tgz | cut -d' ' -f1)"
rm -rf /root/src7 && mkdir -p /root/src7 && tar -xzf /tmp/src-c5.tgz -C /root/src7 2>/dev/null \
  || { say "extract failed: stopping, nothing powered off"; exit 1; }
say "build start"
# pipefail: a failed build must not leave the old binary in place unnoticed.
if (set -o pipefail; cd /root/src7/server && CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/root/src2/server/target \
      cargo build --release --locked 2>&1 | tail -3) \
   && cp /root/src2/server/target/release/queen $BIN.tmp && mv $BIN.tmp $BIN; then
  new=$(md5sum $BIN | cut -d' ' -f1); old=$(md5sum $OLD 2>/dev/null | cut -d' ' -f1)
  say "broker: $new (campaign 4's: $old)"
  [ "$new" != "$old" ] || { say "the build gave campaign 4's binary again: stopping, nothing powered off"; exit 1; }
  grep -q -a 'beta.3' $BIN && say "WARNING: the binary still names beta.3"
else
  say "broker build FAILED: stopping, nothing powered off"; exit 1
fi
for n in n1 n2 n3 n4 n5; do
  for _ in $(seq 1 60); do ssh -n $O root@$n true 2>/dev/null && break; sleep 5; done
done
say "nodes: $(for n in n1 n2 n3 n4 n5; do ssh -n $O root@$n hostname -s 2>/dev/null || echo "$n-DOWN"; done | tr '\n' ' ')"
J=/root/src7/test/jepsen
cd "$J" || { say "no harness in $J"; exit 1; }
lein run test --nodes n1,n2,n3,n4,n5 --username root --ssh-private-key /root/.ssh/id_ed25519 \
  --bin "$BIN" --concurrency 2n --workload duppush --nemesis none \
  --time-limit 60 --rate 100 > /root/first-test-7.log 2>&1
say "first test: $(grep -E 'Everything looks good|Analysis invalid|Errors occurred during analysis' /root/first-test-7.log | head -1)"
if ! grep -q "Everything looks good" /root/first-test-7.log; then
  say "the first test is not valid: stopping here, nothing is powered off"; exit 1
fi
mkdir -p /root/runs-7; cp /root/runs-3/progress.py /root/runs-7/ 2>/dev/null
RUNS=/root/runs-7 BIN=$BIN ./run-campaign.sh /root/share7.txt
say "reruns done: $(grep -c ' done  ' /root/runs-7/summary.txt) tests, $(grep ' done  ' /root/runs-7/summary.txt | grep -c ': valid') valid; powering the rig off"
for n in n1 n2 n3 n4 n5 n6; do ssh -n $O root@$n 'shutdown -h now' || true; done
shutdown -h +2
