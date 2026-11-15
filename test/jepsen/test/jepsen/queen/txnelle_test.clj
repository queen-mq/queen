(ns jepsen.queen.txnelle-test
  "W32 (Elle over transactions): how an op is sent and read back, the checker
  on a clean crafted history, then one history per planted anomaly. No
  cluster; Elle gets no directory, so it writes and plots nothing."
  (:require [clojure.test :refer :all]
            [jepsen [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.generator.test :as gt]
            [jepsen.queen.workload.txnelle :as w]))

(def test-map
  {:name "txnelle-synthetic", :start-time (util/local-time)
   :nodes ["n1" "n2"], :queue-names ["jepsen-0" "jepsen-1"]})

(defn hist
  [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn inv
  [p t kv & {:keys [appends loser?] :or {appends [], loser? false}}]
  {:process p, :type :invoke, :f :txn, :value kv, :t t, :appends appends, :loser? loser?})

(defn done
  [type p t kv & {:keys [appends loser? error] :or {appends [], loser? false}}]
  (cond-> {:process p, :type type, :f :txn, :value kv, :t t, :appends appends, :loser? loser?}
    error (assoc :error error)))

(defn log-of
  "A final read's value: the list and marker partitions holding the pushes of
  the transactions ts ({:t :appends}), in that order."
  [ts]
  (reduce (fn [m {:keys [t appends]}]
            (let [m (update-in m ["jepsen-1" (w/marker-partition t)]
                               (fnil conj []) {:txn (w/marker-id t)})]
              (reduce (fn [m [_ p v]]
                        (update-in m ["jepsen-0" p] (fnil conj []) {:txn (w/append-id v), :v v}))
                      m appends)))
          {} ts))

(defn final
  [p value]
  [{:process p, :type :invoke, :f :final, :value nil}
   {:process p, :type :ok, :f :final, :value value, :node (str "n" p)}])

(defn run
  [ops]
  (w/analyze test-map (hist ops)))

;; T1 writes key 0 and appends to l0; T3, after it, reads key 0, writes key 1
;; and appends to l0 and l1; T6 is a loser and rolls back; T8 reads every key.
(def t1 {:t 1, :appends [[:append "l0" 2]]})
(def t3 {:t 3, :appends [[:append "l0" 4] [:append "l1" 5]]})
(def t6 {:t 6, :appends [[:append "l1" 7]]})
(def t8 {:t 8, :appends []})

(defn base-ops
  []
  [(inv 0 1 [[:w 0 1]] :appends (:appends t1))
   (done :ok 0 1 [[:w 0 1]] :appends (:appends t1))
   (inv 1 3 [[:r 0 nil] [:w 1 1]] :appends (:appends t3))
   (done :ok 1 3 [[:r 0 1] [:w 1 1]] :appends (:appends t3))
   (inv 2 6 [[:w 2 1]] :appends (:appends t6) :loser? true)
   (done :fail 2 6 [[:w 2 1]] :appends (:appends t6) :loser? true
         :error [:rolled-back "kv_precondition"])
   (inv 0 8 [[:r 0 nil] [:r 1 nil] [:r 2 nil]])
   (done :ok 0 8 [[:r 0 1] [:r 1 1] [:r 2 nil]])])

(def clean-log (log-of [t1 t3 t8]))

(deftest w32-op-shape
  (testing "normalize: reads first, each key once, a read of a written key kept; then the last write of each key"
    (is (= [[:r 2 nil] [:r 1 nil] [:w 3 1] [:w 1 4]]
           (w/normalize [[:w 1 3] [:r 2 nil] [:r 1 nil] [:w 3 1] [:w 1 4] [:r 2 nil]]))))
  (testing "the body: the marker, the appends, the KV part in order, the loser last"
    (let [b (w/body test-map {:t 7, :value [[:r 0 nil] [:w 0 3]]
                              :appends [[:append "l2" 9]], :loser? true})]
      (is (= [{:queue "jepsen-1", :partition "m7", :payload {:t 7}, :transactionId "m7"}]
             (:items (first (:operations b)))))
      (is (= [{:queue "jepsen-0", :partition "l2", :payload {:v 9}, :transactionId "a9"}]
             (:items (second (:operations b)))))
      (is (= ["get" "put" "put"] (mapv :op (:kv b))))
      (is (= {:key "exists", :expect 0, :required true}
             (select-keys (last (:kv b)) [:key :expect :required])))))
  (testing "read-values: each read from its result by opIndex, and nothing made up"
    (let [results [{:index 0, :type "push", :success true}
                   {:index 1, :type "kv", :opIndex 0, :op "get", :found true, :value 5}
                   {:index 2, :type "kv", :opIndex 1, :op "get", :found false}
                   {:index 3, :type "kv", :opIndex 2, :op "put", :applied true}]]
      (is (= [[:r 0 5] [:r 1 nil] [:w 1 2]]
             (w/read-values [[:r 0 nil] [:r 1 nil] [:w 1 2]] results)))
      (is (nil? (w/read-values [[:r 0 nil] [:r 4 nil] [:w 1 2]] (take 2 results)))))))

(deftest w32-generator
  (testing "the main phase: normalised transactions, each with its own number, unique appended values, some losers"
    (let [ops     (gt/perfect (gen/clients (gen/limit 400 (:generator (w/workload {})))))
          vs      (mapcat #(map last (:appends %)) ops)]
      (is (= 400 (count ops)))
      (is (every? #(= :txn (:f %)) ops))
      (is (every? #(= (:value %) (w/normalize (:value %))) ops))
      (is (apply distinct? (concat (map :t ops) vs)))
      (is (every? #(<= 0 (count (:appends %)) 2) ops))
      (is (< 0 (count (filter :loser? ops)) 120))))
  (testing "the final phase: Elle's reads with no appends and no loser, then one read of the logs a thread"
    (let [wl  (w/workload {})
          ; As core.clj composes it: the tracker around both phases, the
          ; final phase on the client threads (two in the default context).
          ops (gt/perfect ((:wrap-generator wl)
                           (gen/phases (gen/clients (gen/limit 200 (:generator wl)))
                                       (gen/clients (:final-generator wl)))))
          fin (drop 200 ops)]
      (is (seq (filter #(= :txn (:f %)) fin)))
      (is (every? #(and (empty? (:appends %)) (not (:loser? %))) (filter #(= :txn (:f %)) fin)))
      (is (every? (fn [op] (every? #(= :r (first %)) (:value op))) (filter #(= :txn (:f %)) fin)))
      (is (= 2 (count (filter #(= :final (:f %)) fin)))))))

(deftest w32-clean
  (let [res (run (concat (base-ops) (final 0 clean-log) (final 1 clean-log)))]
    (is (true? (:valid? res)) (pr-str res))
    (is (= 3 (:committed res)))
    (is (true? (:valid? (:kv res))))
    (is (true? (:valid? (:lists res))))))

(deftest w32-anomalies
  (testing "two transactions that read each other's writes: G1c"
    (let [res (run (concat [(inv 0 1 [[:r 1 nil] [:w 0 1]])
                            (inv 1 2 [[:r 0 nil] [:w 1 1]])
                            (done :ok 0 1 [[:r 1 1] [:w 0 1]])
                            (done :ok 1 2 [[:r 0 1] [:w 1 1]])]
                           (final 0 (log-of [{:t 1, :appends []} {:t 2, :appends []}]))))]
      (is (false? (:valid? res)))
      (is (some #{:G1c :G1c-realtime} (:anomaly-types (:kv res))) (pr-str (:kv res)))))

  (testing "a read that misses a write completed before it began"
    (let [res (run (concat [(inv 0 1 [[:w 0 1]]) (done :ok 0 1 [[:w 0 1]])
                            (inv 1 2 [[:r 0 nil]]) (done :ok 1 2 [[:r 0 nil]])]
                           (final 0 (log-of [{:t 1, :appends []} {:t 2, :appends []}]))))]
      (is (false? (:valid? res)))
      (is (false? (:valid? (:kv res))))))

  (testing "two partitions that order two transactions both ways: G0"
    (let [a {:t 1, :appends [[:append "l0" 10] [:append "l1" 11]]}
          b {:t 2, :appends [[:append "l0" 12] [:append "l1" 13]]}
          lg (-> (log-of [a b])
                 (assoc-in ["jepsen-0" "l1"] [{:txn "a13", :v 13} {:txn "a11", :v 11}]))
          res (run (concat [(inv 0 1 [[:w 0 1]] :appends (:appends a))
                            (inv 1 2 [[:w 1 1]] :appends (:appends b))
                            (done :ok 0 1 [[:w 0 1]] :appends (:appends a))
                            (done :ok 1 2 [[:w 1 1]] :appends (:appends b))]
                           (final 0 lg)))]
      (is (false? (:valid? res)))
      (is (some #{:G0 :G0-realtime} (:anomaly-types (:lists res))) (pr-str (:lists res)))))

  (testing "a rolled-back transaction in the log"
    (let [lg  (log-of [t1 t3 t6 t8])
          res (run (concat (base-ops) (final 0 lg)))]
      (is (false? (:valid? res)))
      (is (= [6] (vec (:phantom res))))
      (is (= [{:t 6, :type :fail}] (vec (:loser-committed res))))
      (is (some #{:G1a} (:anomaly-types (:lists res))) (pr-str (:lists res)))))

  (testing "a loser answered as committed"
    (let [ops (assoc (vec (base-ops)) 5 (done :ok 2 6 [[:w 2 1]] :appends (:appends t6) :loser? true))
          res (run (concat ops (final 0 (log-of [t1 t3 t6 t8]))))]
      (is (false? (:valid? res)))
      (is (= [{:t 6, :type :ok}] (vec (:loser-committed res))))))

  (testing "an acknowledged transaction whose marker is not in the log"
    (let [res (run (concat (base-ops) (final 0 (log-of [t1 t8]))))]
      (is (false? (:valid? res)))
      (is (= [3] (vec (:lost res))))))

  (testing "a transaction partly visible: its marker without one of its appends"
    (let [lg  (update-in clean-log ["jepsen-0" "l1"] (comp vec butlast))
          res (run (concat (base-ops) (final 0 lg)))]
      (is (false? (:valid? res)))
      (is (= [3] (map :t (:torn res))))))

  (testing "an id twice in the log"
    (let [lg  (update-in clean-log ["jepsen-1" (w/marker-partition 1)] conj {:txn (w/marker-id 1)})
          res (run (concat (base-ops) (final 0 lg)))]
      (is (false? (:valid? res)))
      (is (= ["m1"] (vec (:ids-twice res))))))

  (testing "two nodes that read different logs"
    (let [res (run (concat (base-ops) (final 0 clean-log) (final 1 (log-of [t1 t8]))))]
      (is (false? (:valid? res)))
      (is (seq (:nodes-differ res))))))
