#!/bin/bash
# reanalyze-wgl.sh: check p11-w3-lead-deaf's stored history again with knossos' WGL search and a 56 GB heap,
# in the harness copy /root/jepsen-26-check (its register.clj differs from the campaign's in :algorithm only).
D="/root/jepsen-26/store/queen-register-leader-deaf_kv-leader_offload=on_lanes=16/2026-10-21T22:58:43.149704411Z"
L=/root/reanalyze-26-wgl.log
date -u +%FT%TZ > $L
cp /root/register.clj.wgl /root/jepsen-26-check/src/jepsen/queen/workload/register.clj
grep -n ":algorithm" /root/jepsen-26-check/src/jepsen/queen/workload/register.clj >> $L
cd /root/jepsen-26-check && lein run analyze -t "$D" >> $L 2>&1
echo "analyze exit $?" >> $L
date -u +%FT%TZ >> $L
