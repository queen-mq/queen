#!/usr/bin/env bash
# Boot against fork for the bench application image, 2026-10-06.
set -euo pipefail
img=queen-laravel-supervisor-bench:auto
out=/root/boot-fork-results.jsonl
: > "$out"
run() { docker run --rm --cpus 8 -v /root/boot-fork.php:/tmp/boot-fork.php "$img" bash -c "$1"; }
for opcache in 0 1; do
  # Each boot is a new PHP process, timed from the shell around it too.
  run "for i in \$(seq 1 30); do s=\$(date +%s%N); line=\$(php -d opcache.enable_cli=$opcache /tmp/boot-fork.php boot); e=\$(date +%s%N); echo \"\${line%\}},\\\"process_ms\\\":\$(( (e-s)/1000 ))e-3}\"; done" >> "$out"
done
for i in 1 2 3 4 5; do
  run "php -d opcache.enable_cli=1 /tmp/boot-fork.php fork 50" >> "$out"
done
echo done
