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

(deftest reordered-at-a-handover
  ; Every campaign: eph-restart, -kill, -part, -pause failed on pairs pushed
  ; across a hand-over, which ephemeral queues do not keep in order.
  (let [nem (fn [f] [{:process :nemesis, :type :info, :f f, :value nil}])]
    (testing "an inversion across a restart: counted, not judged"
      (let [res (run #{:restart} (concat (push 0 "e0") (nem :restart) (push 1 "e0")
                                          (pop-op "gl" "e0" [1 0]) (pop-op "gc" "e0" [0 1])))]
        (is (:valid? res) (pr-str res))
        (is (empty? (:out-of-order res)))
        (is (= 1 (get-in res [:reordered-at-handover :count])))))
    (testing "and across a partition, from its start to its heal"
      (let [res (run #{:partition} (concat (nem :start-partition) (push 0 "e0") (push 1 "e0")
                                            (nem :stop-partition)
                                            (pop-op "gl" "e0" [1 0]) (pop-op "gc" "e0" [0 1])))]
        (is (:valid? res) (pr-str res))))
    (testing "an inversion with no fault near it is still judged"
      (let [at  (fn [t op] (assoc op :time (* 1000000000 t)))
            ops (concat [(at 0 {:process :nemesis, :type :info, :f :restart, :value nil})]
                        (map at [30 31 32 33] (concat (push 0 "e0") (push 1 "e0")))
                        (map at [34 35 36 37] (concat (pop-op "gl" "e0" [1 0]) (pop-op "gc" "e0" [0 1]))))
            res (checker/check (w/checker) (test-map #{:restart}) (h/history ops) {})]
        (is (not (:valid? res)) (pr-str res))
        (is (seq (:out-of-order res)))))))
