set -u
# On rig 1's controller (nodes off): run the checks of campaign 4's x-lock-all and long-pipeline
# again from their stored histories, on the harness of 72d5e6360 with the lock change
# (/tmp/c8/locks.clj). `lein run analyze -t <dir>` rebuilds the test from its stored CLI args with
# the current checker and writes its results back into that test's own folder, found under this
# harness's store/ by the test's name and start time: so each copy is put there first, and the
# campaign's own results are kept beside it as results-campaign4.edn.
R=/root/recheck-c4
[ -s /tmp/c8/locks.clj ] || { echo "no /tmp/c8/locks.clj"; exit 1; }
rm -rf $R && mkdir -p $R/test && cp -a /root/src7/test/jepsen $R/test/jepsen && rm -rf $R/test/jepsen/store \
  || { echo "copy failed"; exit 1; }
install -m 644 /tmp/c8/locks.clj $R/test/jepsen/src/jepsen/queen/workload/locks.clj
echo "locks.clj md5 $(md5sum $R/test/jepsen/src/jepsen/queen/workload/locks.clj | cut -c1-12)"
cd $R/test/jepsen || exit 1
for t in x-lock-all long-pipeline; do
  from=$(cat /root/c4-stores/$t/FROM-rig*.txt)
  rel=store/$(basename "$(dirname "$from")")/$(basename "$from")
  mkdir -p "$(dirname "$rel")" && cp -a /root/c4-stores/$t "$rel" && rm -f "$rel"/FROM-rig*.txt
  cp "$rel/results.edn" "$rel/results-campaign4.edn"
  lein run analyze -t "$rel" > /root/recheck-$t.log 2>&1
  echo "$t: $(grep -E 'Everything looks good|Analysis invalid|Errors occurred during analysis|Jepsen broke' /root/recheck-$t.log | head -1 | cut -c1-120)"
  f="$rel/results.edn"
  echo "  results.edn $(stat -c %s "$f") bytes; $(grep -o ':phantom-count [0-9]*' "$f" | tr '\n' ' ')$(grep -o ':phantom-after-clock-jump {:count [0-9]*' "$f" | tr '\n' ' ')"
  echo "  not valid: $(grep -B1 ':valid? false' "$f" | grep -o '^ *:[a-z-]*' | tr -s ' \n' ' ')"
  echo "  campaign 4 said: $(grep -c ':valid? false' "$rel/results-campaign4.edn") false; now: $(grep -c ':valid? false' "$f") false"
done
