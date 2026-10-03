#!/usr/bin/env bash
# cpu2.sh <tool> <tag> <args...>: like cpu1.sh, plus the VM-wide busy CPU (all containers, /proc/stat) over the run
set -u
TOOL=$1; TAG=$2; shift 2
read -r _ u1 n1 s1 i1 w1 q1 sq1 st1 _ < /proc/stat
/bench/bin/$TOOL -rate 20000 -duration 40s -ramp 10s -report 5s -consumers 6 -out /work/$TAG.json "$@" > /work/$TAG.log 2>&1
read -r _ u2 n2 s2 i2 w2 q2 sq2 st2 _ < /proc/stat
busy=$(( (u2+n2+s2+q2+sq2+st2) - (u1+n1+s1+q1+sq1+st1) )); idle=$(( (i2+w2) - (i1+w1) ))
echo "$TAG: $(grep -h steady /work/$TAG.log | sed 's/.*popped/popped/' | cut -c1-120) | VM busy $(( 100*busy/(busy+idle) ))% of 10 CPUs"
