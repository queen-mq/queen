# On rig 1: copy campaign 4's files to rigs 2-4 over the internal network, put each rig's in place.
set -u
O="-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
cd /tmp/c6 || exit 1
put() {
  k=$1; t=$2
  run() { if [ -z "$t" ]; then bash -c "$1"; else ssh -n $O root@$t "$1"; fi; }
  if [ -n "$t" ]; then
    scp -q $O src-c4.tgz orchestrate-6.sh share6-r$k-gate.txt share6-r$k-rest5.txt share6-r$k-six.txt root@$t:/tmp/ < /dev/null || { echo "rig $k: copy FAILED"; return; }
    D=/tmp
  else D=/tmp/c6; fi
  run "cp $D/src-c4.tgz /tmp/src-c4.tgz 2>/dev/null; install -m 755 $D/orchestrate-6.sh /root/orchestrate-6.sh && install -m 644 $D/share6-r$k-gate.txt /root/share6-gate.txt && install -m 644 $D/share6-r$k-rest5.txt /root/share6-rest5.txt && install -m 644 $D/share6-r$k-six.txt /root/share6-six.txt && echo \"rig $k: \$(hostname -s) src \$(md5sum /tmp/src-c4.tgz | cut -c1-12) shares \$(grep -vc '^#' /root/share6-gate.txt)+\$(grep -vc '^#' /root/share6-rest5.txt)+\$(grep -vc '^#' /root/share6-six.txt) disk \$(df --output=pcent /root | tail -1 | tr -d ' ')\""
}
put 1 ""
put 2 10.250.0.22
put 3 10.250.0.35
put 4 10.250.0.253
