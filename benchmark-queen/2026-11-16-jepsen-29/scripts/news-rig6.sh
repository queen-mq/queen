#!/bin/bash
# Campaign 4, run on a rig's controller: one status line, then every finished test of its share.
t=0
for f in /root/share6-gate.txt /root/share6-rest5.txt /root/share6-six.txt; do
  t=$((t + $(grep -c -v -e '^#' -e '^ *$' "$f")))
done
c=0; v=0
for f in /root/runs-6-gate/summary.txt /root/runs-6/summary.txt /root/runs-6-six/summary.txt; do
  [ -f "$f" ] || continue
  c=$((c + $(grep -c ' done  ' "$f"))); v=$((v + $(grep ' done  ' "$f" | grep -c ': valid')))
done
bad=$(cat /root/runs-6-gate/summary.txt /root/runs-6/summary.txt /root/runs-6-six/summary.txt 2>/dev/null \
  | grep ' done  ' | grep -v ': valid' | awk '{print $3, $4}' | tr '\n' ' ' | cut -c1-400)
echo "$(hostname -s) | $(tail -1 /root/ORCHESTRATE-6.log | cut -c1-100) | done $c of $t, valid $v | not valid: ${bad:-none}"
echo "--- finished tests"
cat /root/runs-6-gate/summary.txt /root/runs-6/summary.txt /root/runs-6-six/summary.txt 2>/dev/null | grep ' done  '
