set -u
# On a rig's controller: every finished test's results.edn (as <test>.edn), the campaign's summary
# files and orchestrator logs, as one gzipped tar in base64 on stdout.
O=/tmp/c4-archive/$(hostname -s)
rm -rf /tmp/c4-archive && mkdir -p $O/results $O/summary
for s in /root/runs-6-gate/summary.txt /root/runs-6/summary.txt /root/runs-6-six/summary.txt /root/runs-7/summary.txt; do
  [ -f "$s" ] || continue
  cp "$s" "$O/summary/$(basename "$(dirname "$s")").txt"
  grep ' done  ' "$s" | while read -r _ _ name verdict rest; do
    name=${name%:}
    store=$(printf '%s' "$rest" | grep -o 'store=[^ ]*' | cut -d= -f2-)
    d=$O/results; [ "$(basename "$(dirname "$s")")" = runs-7 ] && d=$O/results-rerun && mkdir -p $d
    [ -f "$store/results.edn" ] && cp "$store/results.edn" "$d/$name.edn" || echo "$name: no results.edn ($verdict)" >> $O/missing.txt
  done
done
for f in /root/ORCHESTRATE-6.log /root/ORCHESTRATE-7.log /root/first-test-6.log; do [ -f "$f" ] && cp "$f" $O/; done
cd /tmp/c4-archive && tar -czf - . | base64
