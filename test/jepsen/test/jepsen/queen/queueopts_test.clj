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

(defn enq-info [v t0 t1] [{:process 2, :type :invoke, :f :enqueue, :value v, :time t0}
                          {:process 2, :type :info, :f :enqueue, :value v, :time t1}])

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
    (is (:valid? (run :delayed #{:clock} [(enq 1 0) (deq s s [1 "w0"])]))))
  (testing "a push that timed out, delivered 1 s after it was sent: early all the same"
    (is (not (:valid? (run :delayed #{} [(enq-info 1 0 (* 5 s)) (deq s s [1 "w0"])]))))))

(deftest window
  (testing "a batch whose oldest message is 2 s old"
    (is (:valid? (run :window #{} [(enq 1 0) (enq 2 s) (deq (* 2 s) (* 2 s) [1 "w0"] [2 "w0"])]))))
  (testing "a batch handed over before its oldest message waited 2 s"
    (is (not (:valid? (run :window #{} [(enq 1 0) (deq s s [1 "w0"])])))))
  ;; qo-window-part 2026-11-14: 7027's push timed out after 5 s in a partition
  ;; and landed ahead of 7033, sent as it timed out; one pop took both.
  (testing "the batch's oldest message is a push that timed out, sent 5 s before"
    (is (:valid? (run :window #{} [(enq-info 7027 0 (* 5 s)) (enq 7033 (* 5 s))
                                   (deq (* 5 s) (+ (* 5 s) (quot s 5)) [7027 "w5"] [7033 "w5"])]))))
  (testing "a push that timed out, delivered alone 1 s after it was sent: early"
    (is (not (:valid? (run :window #{} [(enq-info 9 0 (* 5 s)) (deq s s [9 "w0"])]))))))

(deftest maxwait
  (testing "delivered within 4 s + scan + margin"
    (is (:valid? (run :maxwait #{} [(enq 1 0) (deq (* 5 s) (* 5 s) [1 "w0"])]))))
  (testing "a pop sent well after the message should have been removed"
    (let [r (run :maxwait #{} [(enq 1 0) (deq (* 9 s) (* 9 s) [1 "w0"])])]
      (is (not (:valid? r)))
      (is (= 1 (:count r)))))
  (testing "a push that timed out may land any time after: not judged"
    (is (:valid? (run :maxwait #{} [(enq-info 1 0 (* 5 s)) (deq (* 20 s) (* 20 s) [1 "w0"])])))))

(deftest maxwait-skips
  ; Partition 1 is "w1". Offsets 0 and 1 were pushed; a pop answered at
  ; `at` starts at offset 2 with nothing acked before it.
  (let [excused? (w/maxwait-skip-excused? 4000)
        push     (fn [v off t] [{:process 0, :type :invoke, :f :enqueue, :value v, :time t}
                                {:process 0, :type :ok, :f :enqueue, :value v, :time (+ t 1000)
                                 :partition 1, :offset off}])
        pop-at   (fn [at] {:process 1, :type :ok, :f :dequeue, :value [], :time at, :index 99})
        hist     (fn [ops] (h/history (vec (apply concat ops)) {:have-indices? false}))]
    (testing "every skipped message pushed 4 s or more before the pop answered: it may be removed"
      (is (excused? (hist [(push 1 0 0) (push 2 1 (* 1 s))]) (pop-at (* 5 s)) "w1" 2 -1)))
    (testing "one pushed 2 s before: it cannot have been removed yet, the skip stands"
      (is (not (excused? (hist [(push 1 0 0) (push 2 1 (* 3 s))]) (pop-at (* 5 s)) "w1" 2 -1))))
    (testing "a skipped offset no acknowledged push names: its time is unknown"
      (is (excused? (hist [(push 1 0 0)]) (pop-at (* 5 s)) "w1" 2 -1)))
    (testing "another partition's pushes say nothing about this one"
      (is (not (excused? (hist [(push 1 0 (* 4 s)) (push 2 1 (* 4 s))]) (pop-at (* 5 s)) "w1" 2 -1))))))
