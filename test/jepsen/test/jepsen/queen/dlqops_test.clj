(ns jepsen.queen.dlqops-test
  "W23 (ack retry, DLQ replay, dead-letter delete): the checker on crafted
  histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.dlqops :as w]))

(def test-map {:name "dlqops-synthetic", :start-time (util/local-time), :nodes ["n1"]})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn enq [v] [{:process 0, :type :invoke, :f :enqueue, :value v}
               {:process 0, :type :ok, :f :enqueue, :value v}])

(defn m [v offset action & {:keys [ack txn pid] :or {ack :acked, pid "1"}}]
  {:v v, :txn (or txn (str "q" v)), :pid pid, :offset offset, :lease "l", :action action, :ack ack})

(defn deq [& ms] [{:process 1, :type :invoke, :f :dequeue, :value nil}
                  {:process 1, :type :ok, :f :dequeue, :value nil, :msgs (vec ms)}])

(defn op [f type & {:as kv}] [{:process 2, :type :invoke, :f f, :value nil}
                              (merge {:process 2, :type type, :f f} kv)])

(defn dlq [& entries] (op :read-dlq :ok :value (vec entries)))

;; v 0 forced, 1 retry, 2 normal, 4 forced (replayed), 8 forced (deleted)
(def clean
  (concat (enq 0) (enq 1) (enq 2) (enq 4) (enq 8)
          (deq (m 1 0 :retry) (m 2 1 :completed))        ; 2 comes back: batch rule
          (deq (m 1 0 :retry) (m 2 1 :completed))
          (deq (m 1 0 :completed) (m 2 1 :completed))
          (deq (m 0 0 :dlq :pid "2") (m 4 1 :dlq :pid "2") (m 8 2 :dlq :pid "2"))
          (op :replay :ok :id "d4" :txn "q4" :value 4)
          (deq (m 4 3 :completed :txn "dlq:d4" :pid "2"))
          (op :delete-dlq :ok :id "d8" :txn "q8" :value 8)
          (dlq {:id "d0", :txn "q0", :v 0})))

(defn run [ops] (checker/check (w/checker) test-map (hist ops) {}))

(deftest w23-dlqops
  (testing "clean, with the batch rule's redeliveries"
    (let [r (run clean)]
      (is (:valid? r) (pr-str r))
      (is (pos? (:batch-rule-redeliveries r)))))
  (testing "a value completed twice with no retry ahead of it in its batch"
    (let [r (run (concat clean (deq (m 2 1 :completed))))]
      (is (= [2] (:completed-twice r)))))
  (testing "a retry value in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d1", :txn "q1", :v 1})))]
      (is (seq (:retry-in-dlq r)))))
  (testing "a value in the DLQ nobody acked dlq"
    (let [r (run (concat (enq 6) (butlast (butlast clean))
                         (dlq {:id "d0", :txn "q0", :v 0} {:id "d6", :txn "q6", :v 6})))]
      (is (= ["q6"] (:dlq-unasked r)))))
  (testing "a replayed dead letter still in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d4", :txn "q4", :v 4})))]
      (is (some #(= :still-in-dlq (:problem %)) (:replay-problems r)))))
  (testing "a replay whose copy came at two offsets"
    (let [r (run (concat clean (deq (m 4 5 :completed :txn "dlq:d4" :pid "2"))))]
      (is (some #(= :two-copies (:problem %)) (:replay-problems r)))))
  (testing "a deleted dead letter still in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d8", :txn "q8", :v 8})))]
      (is (seq (:deleted-still-in-dlq r)))))
  (testing "an acknowledged push that ended nowhere"
    (let [r (run (concat (enq 10) clean))]
      (is (= [10] (:lost r))))))
