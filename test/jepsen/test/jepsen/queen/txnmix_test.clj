(ns jepsen.queen.txnmix-test
  "W18 (transactions with rollbacks on purpose): the checker on a clean
  crafted history, then one history per planted bug. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.txnmix :as w]))

(def test-map
  {:name "txnmix-synthetic", :start-time (util/local-time)
   :nodes ["n1" "n2"], :queue-names ["jepsen-0" "jepsen-1"]})

(defn hist
  [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn txn
  "A transaction t completed as type, with its record."
  [t type & {:keys [data incrs extra dup-of cas error] :or {data [], incrs []}}]
  (let [m {:process (mod t 3), :f :txn, :value t}]
    [(assoc m :type :invoke)
     (cond-> (assoc m :type type, :t t, :data data, :incrs incrs, :extra extra
                    :dup-of dup-of, :cas (or cas [nil nil]))
       error (assoc :error error))]))

(defn records
  "{queue {partition [records]}} holding the marker and data pushes of the
  transactions ts (each {:t :data})."
  [ts]
  (reduce (fn [m {:keys [t data]}]
            (let [m (update-in m ["jepsen-0" (w/marker-partition t)]
                               (fnil conj []) {:txn (w/marker-id t)})]
              (reduce (fn [m {:keys [queue partition i]}]
                        (update-in m [queue partition] (fnil conj []) {:txn (w/data-id t i)}))
                      m data)))
          {} ts))

(defn read-op
  [p value kv]
  [{:process p, :f :read, :value nil, :type :invoke}
   {:process p, :f :read, :type :ok, :value value, :kv kv}])

(def d1 [{:queue "jepsen-0", :partition "d1", :i 0} {:queue "jepsen-1", :partition "d2", :i 1}])

(def committed
  [{:t 0, :data d1} {:t 1, :data []} {:t 3, :data []}])

(def kv-clean
  {"c:0" {:value 2}, "c:1" {:value 1}, "cas:0" {:value 3}})

(defn base-ops
  []
  (concat (txn 0 :ok :data d1 :incrs [0])
          (txn 1 :ok :incrs [0 1])
          (txn 2 :fail :extra :loser :incrs [1] :error [:rolled-back "kv_precondition"])
          (txn 3 :ok :extra :cas :cas [0 0])
          (txn 4 :fail :extra :cas :cas [0 0] :error [:rolled-back "kv_precondition"])
          (txn 5 :fail :extra :dup :dup-of 1 :error [:rolled-back "duplicate"])))

(defn run
  [ops]
  (checker/check (w/checker) test-map (hist ops) {}))

(deftest w18-clean
  (let [res (run (concat (base-ops)
                         (read-op 0 (records committed) kv-clean)
                         (read-op 1 (records committed) kv-clean)))]
    (is (:valid? res) (pr-str res))
    (is (= 3 (:committed res)))))

(deftest w18-anomalies
  (testing "an acknowledged transaction whose marker is not in the log"
    (let [res (run (concat (base-ops) (read-op 0 (records (rest committed)) kv-clean)))]
      (is (not (:valid? res)))
      (is (= [0] (vec (:lost res))))))

  (testing "a rolled-back transaction whose marker is in the log"
    (let [res (run (concat (base-ops)
                           (read-op 0 (records (conj committed {:t 4, :data []})) kv-clean)))]
      (is (= [4] (vec (:phantom res))))))

  (testing "a transaction partly visible: its marker without its data"
    (let [res (run (concat (base-ops)
                           (read-op 0 (records (assoc-in committed [0 :data] (take 1 d1))) kv-clean)))]
      (is (seq (:torn res)))))

  (testing "a transaction partly visible: data without its marker"
    (let [ts  (conj committed {:t 9, :data [{:queue "jepsen-0", :partition "d3", :i 0}]})
          r   (update-in (records ts) ["jepsen-0" (w/marker-partition 9)] pop)
          res (run (concat (base-ops)
                           (txn 9 :info :data [{:queue "jepsen-0", :partition "d3", :i 0}])
                           (read-op 0 r kv-clean)))]
      (is (= [9] (map :t (:torn res))))))

  (testing "a transaction carrying a reused id committed"
    (let [res (run (concat (base-ops)
                           (txn 6 :ok :extra :dup :dup-of 0)
                           (read-op 0 (records (conj committed {:t 6, :data []})) kv-clean)))]
      (is (= [6] (map :t (:gate-committed res))))))

  (testing "a transaction whose required precondition lost committed"
    (let [res (run (concat (base-ops)
                           (txn 7 :info :extra :loser)
                           (read-op 0 (records (conj committed {:t 7, :data []})) kv-clean)))]
      (is (= [7] (map :t (:gate-committed res))))))

  (testing "a counter that is not the number of committed increments"
    (let [res (run (concat (base-ops)
                           (read-op 0 (records committed) (assoc kv-clean "c:1" {:value 2}))))]
      (is (= ["c:1"] (map :key (:counters res))))))

  (testing "two committed cas writes that expected one version: a lost update"
    (let [res (run (concat (base-ops)
                           (txn 8 :ok :extra :cas :cas [0 0])
                           (read-op 0 (records (conj committed {:t 8, :data []})) kv-clean)))]
      (is (= [{:key "cas:0", :expected-version 0, :committed [3 8]}]
             (:cas-lost-update res)))))

  (testing "a cas key holding a value no committed cas write wrote"
    (let [res (run (concat (base-ops)
                           (read-op 0 (records committed) (assoc kv-clean "cas:0" {:value 4}))))]
      (is (= ["cas:0"] (map :key (:cas-value-not-written res))))))

  (testing "two nodes that read different logs"
    (let [res (run (concat (base-ops)
                           (read-op 0 (records committed) kv-clean)
                           (read-op 1 (records (rest committed)) kv-clean)))]
      (is (not (:valid? res)))
      (is (seq (:nodes-differ res))))))
