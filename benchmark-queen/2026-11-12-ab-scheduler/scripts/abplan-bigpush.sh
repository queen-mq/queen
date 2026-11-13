#!/usr/bin/env bash
# abplan-bigpush.sh (Mac): the push ladder on the bigger rig, once abplan-big.sh is done (it waits for its last line):
# pushes of 100 messages to 1,000 partitions, popped and acked, eight qload processes on the 24-vCPU loader, 2.8.1 then
# the scheduler. Why: on the first rig the loader (8 vCPU) was at 7 of 8 cores from 1M msg/s, so the tails there
# measured the loader. Lines to ../runs/abplan-big.out after the transaction ladders'.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-big.out
  until grep -q "ABPLAN_BIG_DONE" "$OUTF" 2>/dev/null; do sleep 15; done
  say "abplan-bigpush start"
  for b in 281 sched; do
    say "== push on $b"
    PARTS=1000 CONS=17 PROCS=8 bash "$HERE/ab.sh" big-push-$b $b push 40 500000 1000000 1500000 2000000 2500000 3000000 2>&1 \
      | tee "$HERE/../runs/big-push-$b.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
      | sed -l "s/^/big-push-$b /" >> "$OUTF"
  done
  say "ABPLAN_BIGPUSH_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
main "$@"; exit $?
