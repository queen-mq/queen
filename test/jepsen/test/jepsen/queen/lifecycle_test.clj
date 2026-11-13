(ns jepsen.queen.lifecycle-test
  "W27 (a queue deleted and made again): the rules on crafted histories."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.lifecycle :as w]))

(def s 1000000000)
(def tm {:name "lc", :start-time (util/local-time), :nodes ["n1"]})

(defn enq [v t] [{:process 0, :type :invoke, :f :enqueue, :value v, :time t}
                 {:process 0, :type :ok, :f :enqueue, :value v, :time (+ t 1000)}])
(defn deq [t vs] [{:process 1, :type :invoke, :f :dequeue, :value nil, :time t}
                  {:process 1, :type :ok, :f :dequeue, :value vs, :time (+ t 1000)
                   :msgs (mapv (fn [v] {:v v}) vs)}])
(defn recreate [t done] [{:process 2, :type :invoke, :f :recreate, :value nil, :time t}
                         {:process 2, :type :ok, :f :recreate, :value nil, :time (+ done 2000), :deleted-at done}])

(defn run [& ops] (checker/check (w/checker) tm (h/history (vec (apply concat ops))) {}))

(deftest lifecycle
  (testing "clean: the new queue delivers what it took"
    (is (:valid? (run (enq 1 0) (recreate (* 2 s) (* 3 s)) (enq 2 (* 4 s)) (deq (* 5 s) [2])))))
  (testing "a deleted queue's message coming back"
    (let [r (run (enq 1 0) (recreate (* 2 s) (* 3 s)) (enq 2 (* 4 s)) (deq (* 5 s) [1 2]))]
      (is (= 1 (:came-back-count r)))))
  (testing "a message of the new queue never delivered"
    (let [r (run (enq 1 0) (recreate (* 2 s) (* 3 s)) (enq 2 (* 4 s)))]
      (is (= [2] (:lost-after-last-recreate r)))))
  (testing "a message pushed while the delete ran may go either way"
    (is (:valid? (run (enq 1 (* 2 s)) (recreate (* 2 s) (* 3 s)) (deq (* 5 s) [1]))))))
