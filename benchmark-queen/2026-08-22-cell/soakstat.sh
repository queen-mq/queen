#!/bin/bash
# One compact status line. Field extraction uses a leading space so eph_push_s
# does not masquerade as push_s, and sed takes the VALUE so the digits in a
# field NAME (ready_age_p95) are not mistaken for data.
R=$(docker logs cell-broker-a --since 45s 2>&1 | grep 'scope="global"' | tail -1)
v() { echo "$R" | grep -oE " $1=\"?[0-9./]+\"?" | head -1 | sed -E 's/.*=//; s/"//g'; }
# Track the NEWEST db csv, never a hardcoded label. The previous version read
# soak-db.csv while the live run wrote gate-db.csv, so it reported a frozen
# 9MB / 0-partition cell for a database that was actually at 343MB and growing.
DBCSV=$(ls -t /root/samples/*-db.csv 2>/dev/null | head -1)
D=$(tail -1 "${DBCSV:-/dev/null}")
DB=$(echo "$D" | cut -d, -f2); PT=$(echo "$D" | cut -d, -f3)
SG=$(echo "$D" | cut -d, -f4); DT=$(echo "$D" | cut -d, -f5); DK=$(echo "$D" | cut -d, -f6)
printf 'push=%-5s pop=%-5s readyAge95=%-7s pool=%-8s wait=%-3s | parts=%-7s segs=%-7s db=%sMB dead=%-7s disk=%s%%' \
  "$(v push_s)" "$(v pop_s)" "$(v ready_age_p95)ms" "$(v pool)" "$(v pool_waiting)" \
  "${PT:-?}" "${SG:-?}" "$(( ${DB:-0} / 1048576 ))" "${DT:-?}" "${DK:-?}"
