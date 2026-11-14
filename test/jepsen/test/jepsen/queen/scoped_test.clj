(ns jepsen.queen.scoped-test
  "W22 (scoped keys, client tokens): the outcome table and the checker on
  crafted histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.core :as core]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.scoped :as w]))

(deftest outcomes
  (testing "a forbidden op refused 403: the expected :fail"
    (is (= :fail (:type (w/outcome :push-other {:status 403, :body {:code "scope_denied"}} #{201})))))
  (testing "a forbidden op answered 2xx: :ok, which the checker calls accepted"
    (is (= :ok (:type (w/outcome :push-other {:status 201} #{201})))))
  (testing "no answer: unknown"
    (is (= :info (:type (w/outcome :push-other {:exception {:type ::qh/timeout}} #{201})))))
  (testing "a refused connection: nothing was sent"
    (is (= :fail (:type (w/outcome :push-own {:exception {:type ::qh/refused}} #{201}))))))

(def test-map {:name "scoped-synthetic", :start-time (util/local-time), :nodes ["n1"]})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn op [f type & {:as kv}]
  [(merge {:process 0, :type :invoke, :f f} (dissoc kv :value))
   (merge {:process 0, :type type, :f f} kv)])

(defn rec [v by partition] {:offset 0, :txn (str "s" v), :data {:v v, :by by}, :partition partition})

(def clean
  (concat (op :push-own :ok :value 0 :device "d0" :to "d0")
          (op :push-other :fail :value 1 :device "d0" :to "d1")
          (op :kv-own :ok :value 0 :device "d0" :whose "d0")
          (op :kv-other :fail :value 0 :device "d0" :whose "d1")
          (op :read :ok :value {:records [(rec 0 "d0" "d0")], :kv {"d0" {:by "d0", :v 0}}})))

(defn run [ops] (checker/check (w/checker) test-map (hist ops) {}))

(deftest w22-scoped
  (testing "clean"
    (is (:valid? (run clean))))
  (testing "a forbidden push answered as accepted"
    (let [res (run (concat (op :push-other :ok :value 3 :device "d0" :to "d2") clean))]
      (is (seq (:accepted-forbidden res)))))
  (testing "a forbidden push in the log, although it answered a timeout"
    (let [res (run (concat (op :push-other :info :value 5 :device "d0" :to "d3")
                           (op :read :ok :value {:records [(rec 0 "d0" "d0") (rec 5 "d0" "d3")], :kv {}})))]
      (is (seq (:landed-forbidden res)))))
  (testing "a device's KV key written by another device"
    (let [res (run (concat clean (op :read :ok :value {:records [(rec 0 "d0" "d0")]
                                                       :kv {"d1" {:by "d0", :v 2}}})))]
      (is (seq (:kv-foreign res)))))
  (testing "an acknowledged own push missing from the log"
    (let [res (run (concat (op :push-own :ok :value 8 :device "d1" :to "d1") clean))]
      (is (= [8] (:lost-own res))))))

(deftest refused-by-design-is-not-a-stats-failure
  ; Every :push-other refused, as it must be: Jepsen's stats checker calls an
  ; :f that never answered :ok invalid, unless the workload says to leave it out.
  (let [refused (vec (mapcat (fn [i] [{:process 0, :type :invoke, :f :push-other, :value i}
                                      {:process 0, :type :fail, :f :push-other, :value i}])
                             (range 3)))
        owned   [{:process 1, :type :invoke, :f :push-own, :value 0}
                 {:process 1, :type :ok, :f :push-own, :value 0}]
        check   (fn [fs] (checker/check (core/stats-checker-without fs)
                                        test-map (hist (into refused owned)) {}))]
    (is (false? (:valid? (check #{}))))
    (is (true? (:valid? (check #{:push-other}))))))
