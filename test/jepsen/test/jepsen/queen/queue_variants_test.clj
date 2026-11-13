(ns jepsen.queen.queue-variants-test
  "W2's options (queue mode, --w2-groups fan-out, --w2-extend renewals): the
  rules they add, on crafted histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.queue :as q]))

(def s 1000000000)

(defn hist
  "Ops at explicit times (nanoseconds)."
  [ops]
  (h/history (vec ops)))

(defn enq [t v]
  [{:process 0, :type :invoke, :f :enqueue, :value v, :time t}
   {:process 0, :type :ok, :f :enqueue, :value v, :time (inc t), :partition 0, :offset (dec v)}])

(defn deq
  "A pop invoked at t0, completed at t1, of v (partition w0, offset v - 1),
  acked as `ack`, by group g; `ext` a renewal."
  [p t0 t1 v & {:keys [ack g ext] :or {ack :acked, g "g1"}}]
  [{:process p, :type :invoke, :f :dequeue, :value nil, :time t0, :group g}
   (cond-> {:process p, :type :ok, :f :dequeue, :time t1, :group g, :lease-s 3
            :value (if (= :acked ack) [v] [])
            :msgs [{:v v, :txn (str "w" v), :pid "1", :partition "w0", :offset (dec v)
                    :lease (str "l" p "-" v), :attempt 1, :ack ack}]}
     ext (assoc :extend ext))])

(defn qc [test ops] (checker/check (q/queue-checker) test (hist (apply concat ops)) {}))

(def test-map {:name "w2-variants", :start-time (util/local-time), :nodes ["n1"]})

(deftest renewal-windows
  (let [renewed {:renewed true, :seconds 6, :invoked-ns (* 2 s), :pop-inv-nano 0}]
    (testing "without a renewal, a second delivery after the lease is fine"
      (is (:valid? (qc test-map [(enq 0 1) (deq 1 s (+ s 1000) 1 :ack :rejected)
                                 (deq 2 (* 5 s) (+ (* 5 s) 1000) 1)]))))
    (testing "a renewal answered success holds the message: a second delivery
              inside it is an overlap of two live leases"
      (let [r (qc test-map [(enq 0 1) (deq 1 s (+ s 1000) 1 :ack :rejected :ext renewed)
                            (deq 2 (* 5 s) (+ (* 5 s) 1000) 1)])]
        (is (not (:valid? r)))
        (is (= 1 (:lease-overlap-count r)))))
    (testing "an ack refused although its lease had just been renewed"
      (let [r (qc test-map [(enq 0 1) (deq 1 s (+ s 1000) 1 :ack :rejected :ext renewed)])]
        (is (not (:valid? r)))
        (is (seq (:ack-refused-under-renewal r)))))
    (testing "a renewal the broker refused extends nothing"
      (let [r (qc test-map [(enq 0 1) (deq 1 s (+ s 1000) 1 :ack :rejected
                                           :ext (assoc renewed :renewed false))
                            (deq 2 (* 5 s) (+ (* 5 s) 1000) 1)])]
        (is (:valid? r))))))

(deftest fan-out
  (let [test (assoc test-map :w2-groups 2)
        run  (fn [ops] (checker/check (q/per-group-checker) test (hist (apply concat ops)) {}))]
    (testing "every group got every message"
      (is (:valid? (run [(enq 0 1) (deq 1 s (+ s 10) 1 :g "g1") (deq 2 (* 2 s) (+ (* 2 s) 10) 1 :g "g2")]))))
    (testing "a group that never got a message"
      (let [r (run [(enq 0 1) (enq 100 2)
                    (deq 1 s (+ s 10) 1 :g "g1") (deq 1 (* 2 s) (+ (* 2 s) 10) 2 :g "g1")
                    (deq 2 (* 3 s) (+ (* 3 s) 10) 1 :g "g2")])]
        (is (not (:valid? r)))
        (is (get-in r [:groups "g1" :valid?]))
        (is (not (get-in r [:groups "g2" :valid?])))))))

(deftest groups-option
  (is (= ["g1"] (q/groups {})))
  (is (= [nil] (q/groups {:w2-group "none"})))
  (is (= ["g1" "g2" "g3"] (q/groups {:w2-groups 3}))))
