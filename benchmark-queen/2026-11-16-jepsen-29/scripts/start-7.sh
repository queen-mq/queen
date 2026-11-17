set -u
# On rig 1: put the rerun's files in place and start orchestrate-7 detached.
install -m 644 /tmp/c7/src-c5.tgz /tmp/src-c5.tgz && install -m 755 /tmp/c7/orchestrate-7.sh /root/orchestrate-7.sh \
  && install -m 644 /tmp/c7/share7.txt /root/share7.txt || { echo "install failed, not started"; exit 1; }
for f in /tmp/src-c5.tgz /root/orchestrate-7.sh /root/share7.txt; do
  [ -s "$f" ] || { echo "$(hostname -s): missing $f, not started"; exit 1; }
done
[ "$(ps -eo args | grep -c '^bash /root/orchestrate')" -eq 0 ] || { echo "$(hostname -s): an orchestrator is running, not started"; exit 1; }
setsid nohup bash /root/orchestrate-7.sh > /dev/null 2>&1 < /dev/null &
sleep 4
echo "$(hostname -s): src $(md5sum /tmp/src-c5.tgz | cut -c1-12), tests $(grep -vc '^#' /root/share7.txt): $(tail -1 /root/ORCHESTRATE-7.log)"
