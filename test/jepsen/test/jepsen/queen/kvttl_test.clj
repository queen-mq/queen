(ns jepsen.queen.kvttl-test
  "W25 (KV ttl): the expiry rules on crafted histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.kvttl :as w]))

(def s 1000000000)

(defn tm [faults] {:name "kvttl", :start-time (util/local-time), :nodes ["n1"], :faults faults})

(defn write [n ttl t] [{:process 0, :type :invoke, :f :write, :value n, :time t}
                       {:process 0, :type :ok, :f :write, :value n, :key (str "t:" n), :ttl ttl, :time (+ t 1000)}])

(defn read-op [n t0 t1 found v]
  [{:process 1, :type :invoke, :f :read, :value nil, :time t0}
   {:process 1, :type :ok, :f :read, :key (str "t:" n), :time t1, :value {:found found, :value v}}])

(defn claim [k won t0 t1]
  [{:process 2, :type :invoke, :f :claim, :value 0, :key k, :time t0}
   {:process 2, :type :ok, :f :claim, :claim-key (str "claim:" k), :won won, :time t1}])

(defn run [faults & ops]
  (checker/check (w/checker) (tm faults) (h/history (vec (apply concat ops))) {}))

(deftest kvttl
  (testing "found before its ttl, absent well after"
    (is (:valid? (run #{} (write 1 2 0) (read-op 1 s (+ s 10) true 1) (read-op 1 (* 4 s) (* 4 s) false nil)))))
  (testing "an expired key still served"
    (let [r (run #{} (write 1 2 0) (read-op 1 (* 4 s) (* 4 s) true 1))]
      (is (= 1 (count (:expired-served r))))))
  (testing "a key gone before its ttl"
    (let [r (run #{} (write 1 4 0) (read-op 1 s (+ s 10) false nil))]
      (is (= 1 (count (:expired-early r))))))
  (testing "two holders of one claim"
    (let [r (run #{} (claim 0 true 0 10) (claim 0 true (/ s 2) (/ s 2)))]
      (is (= 1 (count (:two-holders r))))))
  (testing "a claim won again after the first expired"
    (is (:valid? (run #{} (claim 0 true 0 10) (claim 0 true (* 3 s) (* 3 s))))))
  (testing "under clock faults: counted, not judged"
    (is (:valid? (run #{:clock} (write 1 2 0) (read-op 1 (* 4 s) (* 4 s) true 1))))))
