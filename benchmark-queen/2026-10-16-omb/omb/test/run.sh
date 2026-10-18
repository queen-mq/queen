#!/usr/bin/env bash
# run.sh start <name> <driver.yaml> <workload.yaml> : starts bin/benchmark on B (2 workers: 8080 consumers, 8082 producers)
#   plus 5-s samplers of the broker (A) and the worker JVMs (B); returns at once.
# run.sh done <name>   : 0 when the run is over
# run.sh collect <name>: the run's output, OMB result JSON, worker-log slices and samples into runs/<name>/
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/../../hosts.env"
A=<host-33>; B=<host-36>
rx() { local h=$1; shift; $SSH -n root@$h "$@"; }
cmd=$1; name=$2
case $cmd in
  start)
    drv=$3; wl=$4
    rx $B "pkill -f '[b]enchmark\.Benchmark --' ; rm -rf /root/bench/runs/$name; mkdir -p /root/bench/omb/test /root/bench/runs/$name" && $SCP "$drv" root@$B:/root/bench/omb/test/$name-driver.yaml && $SCP "$wl" root@$B:/root/bench/omb/test/$name-workload.yaml || exit 1
    $SCP "$HERE/sample-workers.sh" root@$B:/root/bench/sample-workers.sh; $SCP "$HERE/sample-broker.sh" root@$A:/root/sample-broker.sh
    rx $B "cd /root/bench/runs/$name && for p in 8080 8082; do wc -l < /root/bench/logs/worker-\$p.log > start-\$p.lines; done; \
      setsid nohup bash /root/bench/sample-workers.sh /root/bench/runs/$name > /root/bench/runs/$name/workers.samples 2>&1 < /dev/null & \
      cd /root/bench/omb; setsid nohup bash -c 'bin/benchmark --drivers test/$name-driver.yaml --workers http://10.250.0.38:8080,http://10.250.0.38:8082 --output /root/bench/runs/$name/result.json test/$name-workload.yaml > /root/bench/runs/$name/benchmark.out 2>&1; touch /root/bench/runs/$name/DONE' > /dev/null 2>&1 < /dev/null &"
    rx $A "mkdir -p /root/samples; rm -f /root/samples/$name.stop; setsid nohup bash /root/sample-broker.sh /root/samples/$name.stop > /root/samples/$name.broker 2>&1 < /dev/null &"
    echo started $name ;;
  done)
    rx $B "test -f /root/bench/runs/$name/DONE" ;;
  collect)
    L=$HERE/runs/$name; mkdir -p $L
    rx $A "touch /root/samples/$name.stop"
    rx $B "cd /root/bench/runs/$name && for p in 8080 8082; do s=\$(cat start-\$p.lines); tail -n +\$((s+1)) /root/bench/logs/worker-\$p.log > worker-\$p.slice; done; ls"
    $SCP "root@$B:/root/bench/runs/$name/*" $L/ 2>/dev/null
    $SCP root@$A:/root/samples/$name.broker $L/broker.samples 2>/dev/null
    grep -E "Created|ready|Completed|Exception|ERROR" $L/benchmark.out | sed -E 's/^.{0,40}(INFO|WARN|ERROR)/\1/' | cut -c1-200 | tail -12
    for p in 8080 8082; do echo "-- worker $p"; grep -E "Queen driver closed|Queen verify|Queen: |WARN|ERROR|Exception" $L/worker-$p.slice | sed -E 's/^.{0,60}(INFO|WARN|ERROR) //' | cut -c1-700 | head -12; done
    python3 $HERE/summary.py $L ;;
esac
