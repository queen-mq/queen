#!/usr/bin/env bash
# watch-campaign.sh <state file>: from the Mac, poll the control every 5 minutes; exit (so the session reports) when a
# list ends, the archive is collected, or a test ends other than valid. The remote command ends with `; true`: a
# missing summary must not skip the poll (the 2.4 watcher trap).
ST=${1:?state file}; touch $ST
while :; do
  out=$(ssh -n -o BatchMode=yes -o ConnectTimeout=20 root@<jepsen-control> 'for d in /root/runs-25-smoke /root/runs-25 /root/runs-25-fo /root/runs-25-batch; do [ -f $d/summary.txt ] && grep -E " done |ALL DONE|NOT VALID|AFTER DONE" $d/summary.txt | sed "s|^|$(basename $d) |"; done; true')
  if [ -n "$out" ]; then
    # every list end, and every test that ended other than valid (INVALID, unknown, CRASHED, REFUSED, any case)
    new=$( { echo "$out" | grep -E "ALL DONE|AFTER DONE|NOT VALID"; echo "$out" | grep " done " | grep -v ": valid "; } | grep -vxF -f $ST)
    if [ -n "$new" ]; then echo "$new" >> $ST; echo "$(date -u +%T) progress: $(echo "$out" | awk '{print $1}' | sort | uniq -c | tr '\n' ' ')"; echo "$new"; exit 0; fi
  fi
  sleep 300
done
