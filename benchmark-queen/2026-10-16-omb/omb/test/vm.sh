#!/usr/bin/env bash
# Functional tests of driver-queen on TWO VMs only (10-17): A = broker, B = OMB workers + bin/benchmark.
# Usage: bash vm.sh <cmd> [args]. Never touches the brokers of the facade investigation or loader 1.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/../../hosts.env"
A=<host-33>; A_PRIV=10.250.0.42      # broker
B=<host-36>;  B_PRIV=10.250.0.38      # OMB workers + driver
SRC_BIN_HOST=<host-37>               # read ONE file there (scp -3), nothing else
SCRATCH=/private/tmp/claude-502/-Users-alice-Work-queen/9a561819-3d85-429b-8a6b-c43fdbb02338/scratchpad
TARBALL=$SCRATCH/src/omb/package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
rx() { local h=$1; shift; $SSH -n root@$h "$@"; }
case "${1:-}" in
  prep)
    for h in $A $B; do echo "== $h"; rx $h 'hostname; nproc; free -g | head -2; df -h / /root 2>/dev/null | sort -u; java -version 2>&1 | head -1; ulimit -n; ps -eo pid,etime,comm,args | grep -E "[j]ava|[q]ueen" | cut -c1-160'; done ;;
  copy-bin)
    $SCP -3 root@$SRC_BIN_HOST:/root/p3/bin/nr0/queen root@$A:/root/queen-nr0 && rx $A 'chmod +x /root/queen-nr0; ls -la /root/queen-nr0; /root/queen-nr0 --version 2>&1 | head -2' ;;
  broker-start)   # broker-start <shards> [disk|shm] : a FRESH data dir every time; shm = tmpfs (/dev/shm), no disk fsync
    SH=${2:-0}; WHERE=${3:-disk}
    if [ "$WHERE" = shm ]; then DD=/dev/shm/qdata; else DD=/root/qdata-disk; fi
    rx $A "pkill -x queen-nr0 ; sleep 1; rm -rf /root/qdata /dev/shm/qdata /root/qdata-disk; mkdir -p $DD; ln -sfn $DD /root/qdata; cd /root; ulimit -n 1048576; \
      QUEEN_RAFT_DIR=/root/qdata PORT=6632 QUEEN_QLOG_SHARDS=$SH LOG_LEVEL=info setsid nohup /root/queen-nr0 > /root/queen.log 2>&1 < /dev/null & sleep 3; \
      curl -s -m 3 http://127.0.0.1:6632/health; echo" ;;
  broker-stop)
    rx $A "pkill -x queen-nr0; sleep 1; ps -eo pid,comm | grep -c queen-nr" ;;
  broker-stat)    # RSS, threads, fds, data dir size
    rx $A 'p=$(pgrep -x queen-nr0 | head -1); [ -n "$p" ] && { echo "pid $p rss_mb $(( $(awk "/VmRSS/{print \$2}" /proc/$p/status) / 1024 )) threads $(ls /proc/$p/task | wc -l) fds $(ls /proc/$p/fd | wc -l) data $(du -sh /root/qdata | cut -f1) qlog_files $(find /root/qdata -type f | wc -l)"; }' ;;
  deploy-omb)
    $SCP $TARBALL root@$B:/root/omb-new.tgz && rx $B "pkill -f '[i]o.openmessaging' ; sleep 1; rm -rf /root/bench/omb; mkdir -p /root/bench && cd /root/bench && tar xzf /root/omb-new.tgz && mv openmessaging-benchmark-0.0.1-SNAPSHOT omb && ls omb/lib | grep -i queen" ;;
  workers-start)  # workers-start <heap> : two workers on B (8080 = consumers, 8082 = producers)
    HEAP=${2:-8G}
    rx $B "pkill -f '[i]o.openmessaging' ; sleep 1; cd /root/bench/omb && mkdir -p /root/bench/logs && ulimit -n 1048576 && for p in 8080 8082; do HEAP_OPTS='-Xms$HEAP -Xmx$HEAP' setsid nohup bin/benchmark-worker --port \$p --stats-port \$((p+1)) > /root/bench/logs/worker-\$p.log 2>&1 < /dev/null & done; sleep 4; ss -ltn | grep -E ':808[02]'" ;;
  workers-stat)   # threads + RSS of each worker JVM
    rx $B 'for p in $(pgrep -f "[i]o.openmessaging.benchmark.worker.BenchmarkWorker"); do echo "pid $p rss_mb $(( $(awk "/VmRSS/{print \$2}" /proc/$p/status) / 1024 )) threads $(ls /proc/$p/task | wc -l) port $(tr "\0" " " < /proc/$p/cmdline | grep -o "\-\-port [0-9]*")"; done; echo "tcp to broker: $(ss -tn state established "( dport = :6632 )" | wc -l)"' ;;
  *) echo "usage: $0 prep|copy-bin|broker-start <shards>|broker-stop|broker-stat|deploy-omb|workers-start <heap>|workers-stat" ;;
esac
