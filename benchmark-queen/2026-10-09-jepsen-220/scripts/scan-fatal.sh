#!/usr/bin/env bash
# scan-fatal.sh [minutes]: node logs of the tests that ended in the last N minutes (all, without N)
# that hold a line a healthy node never writes: a refusal to start, a panic, a render gap.
# The corruption tests damage a node's files on purpose: their refusals are expected and left out.
cd /root
M=${1:-}
for d in jepsen-220a jepsen-220b; do
  if [ -n "$M" ]; then L=$(find $d/store -name queen.log -mmin -"$M" -not -path "*corrupt*" -not -path "*/latest/*" 2>/dev/null); else L=$(find $d/store -name queen.log -not -path "*corrupt*" -not -path "*/latest/*" 2>/dev/null); fi
  [ -z "$L" ] && continue
  echo "$L" | xargs grep -l -E "FATAL|panicked at|RENDER_GAP" 2>/dev/null | while read -r f; do
    echo "BAD $f: $(grep -m1 -E "FATAL|panicked at|RENDER_GAP" "$f" | cut -c1-260)"
  done
done
