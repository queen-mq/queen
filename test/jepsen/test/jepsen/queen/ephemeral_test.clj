(ns jepsen.queen.ephemeral-test
  "W20 (ephemeral queues): the checker on crafted histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.ephemeral :as w]))

(defn test-map [faults]
  {:name "eph-synthetic", :start-time (util/local-time), :nodes ["n1"], :faults faults})

(defn hist [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn push [v p]
  [{:process 0, :type :invoke, :f :push, :value v}
   {:process 0, :type :ok, :f :push, :value v, :partition p}])

(defn pop-op [g p vs]
  [{:process 1, :type :invoke, :f :pop, :value nil, :group g, :partition p}
   {:process 1, :type :ok, :f :pop, :value nil, :group g, :partition p
    :msgs (mapv (fn [v] {:id (str "e:x:" p ":" v), :v v, :partition p, :attempts 1}) vs)}])

(def clean
  (concat (push 0 "e0") (push 1 "e0") (push 2 "e1")
          (pop-op "gl" "e0" [0 1]) (pop-op "gc" "e0" [0 1])
          (pop-op "gl" "e1" [2]) (pop-op "gc" "e1" [2])))

(defn run
  ([ops] (run #{} ops))
  ([faults ops] (checker/check (w/checker) (test-map faults) (hist ops) {})))

(deftest w20-ephemeral
  (testing "clean"
    (is (:valid? (run clean))))
  (testing "a delivered message nobody pushed"
    (let [res (run (concat clean (pop-op "gl" "e1" [9])))]
      (is (= [9] (:invented res)))))
  (testing "a commitOnDelivery message delivered twice"
    (let [res (run (concat clean (pop-op "gc" "e0" [1])))]
      (is (= [1] (:cod-delivered-twice res)))))
  (testing "two messages of a partition in one answer, against real time"
    (let [res (run (concat (push 0 "e0") (push 1 "e0")
                           (pop-op "gl" "e0" [1 0]) (pop-op "gc" "e0" [0 1])))]
      (is (seq (:out-of-order res)))))
  (testing "an acknowledged push a group never got: judged without kills"
    (let [res (run (concat clean (push 3 "e2") (pop-op "gl" "e2" [3])))]
      (is (not (:valid? res)))
      (is (= 1 (get-in res [:missing "gc" :count])))))
  (testing "the same loss under kills: by design, counted only"
    (let [res (run #{:kill} (concat clean (push 3 "e2") (pop-op "gl" "e2" [3])))]
      (is (:valid? res))
      (is (false? (:loss-judged? res))))))
