#!/usr/bin/env bash
# Node logs of tests that ended after the rc6 swap, corruption tests left out, with a line a healthy node never writes.
cd /root
for d in jepsen-220a jepsen-220b; do
  find $d/store -name queen.log -newer /root/runs-220a/swap-rc6.done -not -path "*corrupt*" -not -path "*/latest/*" 2>/dev/null | xargs -r grep -l -E "FATAL|panicked at|RENDER_GAP" 2>/dev/null | while read -r f; do
    echo "BAD $f: $(grep -m1 -E "FATAL|panicked at|RENDER_GAP" "$f" | cut -c1-240)"
  done
done
