set -u
for f in /tmp/src-c4.tgz /root/orchestrate-6.sh /root/share6-gate.txt /root/share6-rest5.txt /root/share6-six.txt; do
  [ -s "$f" ] || { echo "$(hostname -s): missing $f, not started"; exit 1; }
done
[ "$(ps -eo args | grep -c '^bash /root/orchestrate')" -eq 0 ] || { echo "$(hostname -s): an orchestrator is running, not started"; exit 1; }
ROLE=$1 setsid nohup bash /root/orchestrate-6.sh > /dev/null 2>&1 < /dev/null &
sleep 4
echo "$(hostname -s): $(tail -1 /root/ORCHESTRATE-6.log)"
