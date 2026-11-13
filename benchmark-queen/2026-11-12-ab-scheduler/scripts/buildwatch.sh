#!/usr/bin/env bash
# buildwatch.sh (Mac): one line per build when it finishes or fails (2.8.1 on n1, the scheduler on n2), then exit.
HERE=$(cd "$(dirname "$0")" && pwd)
seen=""
while true; do
  for n in n1:281 n2:sched; do
    vm=${n%%:*}; b=${n##*:}
    case "$seen" in *"$vm:"*) continue ;; esac
    l=$(bash "$HERE/rx.sh" "$vm" "grep -a -E '^(error|warning: build failed)|Finished|could not compile' /root/p3/build-$b.log | tail -1" 2>/dev/null | head -1)
    if [ -n "$l" ]; then echo "$b: $l"; seen="$seen $vm:"; fi
  done
  case "$seen" in *n1:*n2:*|*n2:*n1:*) break ;; esac
  sleep 30
done
