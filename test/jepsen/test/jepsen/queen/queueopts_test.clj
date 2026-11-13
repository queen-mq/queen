(ns jepsen.queen.queueopts-test
  "W24 (delayedProcessing, windowBuffer, maxWaitTimeSeconds): the timing
  rules on crafted histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.queueopts :as w]))

(def s 1000000000)

(defn test-map [opt faults]
  {:name "qo", :start-time (util/local-time), :nodes ["n1"], :queue-opt opt, :faults faults})

(defn enq [v t] [{:process 0, :type :invoke, :f :enqueue, :value v, :time t}
                 {:process 0, :type :ok, :f :enqueue, :value v, :time (+ t 1000)}])

(defn deq [t0 t1 & vps]
  [{:process 1, :type :invoke, :f :dequeue, :value nil, :time t0}
   {:process 1, :type :ok, :f :dequeue, :value (mapv first vps), :time t1
    :msgs (mapv (fn [[v p]] {:v v, :partition p}) vps)}])

(defn run [opt faults ops]
  (checker/check (w/timing-checker) (test-map opt faults) (h/history (vec (apply concat ops))) {}))

(deftest delayed
  (testing "delivered 3 s or more after the push was sent"
    (is (:valid? (run :delayed #{} [(enq 1 0) (deq (* 3 s) (* 3 s) [1 "w0"])]))))
  (testing "delivered 1 s after: early"
    (let [r (run :delayed #{} [(enq 1 0) (deq s s [1 "w0"])])]
      (is (not (:valid? r)))
      (is (= 1 (:count r)))))
  (testing "under clock faults it is counted, not judged"
    (is (:valid? (run :delayed #{:clock} [(enq 1 0) (deq s s [1 "w0"])])))))

(deftest window
  (testing "a batch whose oldest message is 2 s old"
    (is (:valid? (run :window #{} [(enq 1 0) (enq 2 s) (deq (* 2 s) (* 2 s) [1 "w0"] [2 "w0"])]))))
  (testing "a batch handed over before its oldest message waited 2 s"
    (is (not (:valid? (run :window #{} [(enq 1 0) (deq s s [1 "w0"])]))))))

(deftest maxwait
  (testing "delivered within 4 s + scan + margin"
    (is (:valid? (run :maxwait #{} [(enq 1 0) (deq (* 5 s) (* 5 s) [1 "w0"])]))))
  (testing "a pop sent well after the message should have been removed"
    (let [r (run :maxwait #{} [(enq 1 0) (deq (* 9 s) (* 9 s) [1 "w0"])])]
      (is (not (:valid? r)))
      (is (= 1 (:count r))))))
