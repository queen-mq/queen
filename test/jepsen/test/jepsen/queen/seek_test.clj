(ns jepsen.queen.seek-test
  "W26 (seek to the end, seek back in time): the rules on crafted histories.
  No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.seek :as w]))

(def s 1000000000)

(defn tm [faults] {:name "seek", :start-time (util/local-time), :nodes ["n1"], :faults faults})

(defn enq [v t] [{:process 0, :type :invoke, :f :enqueue, :value v, :time t}
                 {:process 0, :type :ok, :f :enqueue, :value v, :time (+ t 1000), :partition 0, :offset v}])

(defn deq [t vs]
  [{:process 1, :type :invoke, :f :dequeue, :value nil, :time t}
   {:process 1, :type :ok, :f :dequeue, :value vs, :time (+ t 1000), :lease-s 3
    :msgs (mapv (fn [v] {:v v, :txn (str "w" v), :pid "1", :partition "w0", :offset v
                         :lease (str "l" t), :attempt 1, :ack :acked}) vs)}])

(defn seek-op [v t & {:keys [point]}]
  [{:process 2, :type :invoke, :f :seek, :value v, :time t}
   {:process 2, :type :ok, :f :seek, :value v, :time (+ t 1000), :point point}])

;; 0 and 1 before the seek to the end (t=10 s); 2 and 3 after; the seek back
;; at t=30 s points at t=25 s: 3 (pushed at 26 s) must come again.
(def base
  [(enq 0 0) (enq 1 s)
   (seek-op :end (* 10 s))
   (enq 2 (* 12 s)) (enq 3 (* 26 s))
   (deq (* 27 s) [2 3])
   (seek-op :time (* 30 s) :point (* 25 s))])

(defn run [faults & extra] (checker/check (w/checker) (tm faults) (h/history (vec (apply concat (concat base extra)))) {}))

(deftest seek
  (testing "clean: after the seek back, 3 comes again"
    (is (:valid? (run #{} (deq (* 31 s) [3])))))
  (testing "a message older than the seek to the end, delivered after it"
    (let [r (run #{} (deq (* 15 s) [0]) (deq (* 31 s) [3]))]
      (is (= 1 (:not-skipped-count r)))))
  (testing "a message after the seek back's point never read again"
    (let [r (run #{})]
      (is (= [3] (:not-read-again r)))))
  (testing "a message pushed after the seek to the end, never delivered"
    (let [r (checker/check (w/checker) (tm #{})
                           (h/history (vec (apply concat [(enq 0 0) (seek-op :end (* 10 s)) (enq 5 (* 12 s))])))
                           {})]
      (is (= [5] (:lost-after-seek-to-end r)))))
  (testing "under clock faults the seek rules are counted, not judged"
    (is (:valid? (run #{:clock})))))
