(ns jepsen.queen.workload.lifecycle
  "W27 --workload lifecycle: a queue deleted and made again while clients
  push and consume (DELETE /api/v1/resources/queues/:queue, then POST
  /api/v1/configure): W2's client on the test queue, and at 25%, 50% and
  75% of the time limit one :recreate op, the delete and the configure in a
  row.

  The checker rejects:
    - a message pushed (acknowledged) before a delete was sent, delivered by
      a pop sent after that delete answered: a deleted queue's message
      coming back;
    - a message pushed after the last delete answered that no pop delivered
      by the end (the queue made again must keep what it takes).
  A push between a delete and its configure makes the queue again on its
  own (the first push creates queue and partition): such messages belong to
  the new queue."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.db :as qdb]
            [jepsen.queen.workload.queue :as queue])
  (:import (java.net URLEncoder)))

(defn- enc [x] (URLEncoder/encode (str x) "UTF-8"))

(defrecord Client [inner]
  client/Client
  (open! [this test node] (assoc this :inner (client/open! inner test node)))
  (setup! [this test])
  (invoke! [this test op]
    (if (= :recreate (:f op))
      (let [c   (:inner this)
            q   (queue/queue-name test)
            t0  (System/nanoTime)
            del (try (qh/request! (:http c) :delete (str (:base c) "/api/v1/resources/queues/" (enc q))
                                  nil (:client-timeout-ms test))
                     (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))
            ; The delete's answer, in the history's time base.
            dc  (+ (:time op) (- (System/nanoTime) t0))
            cfg (try (qh/request! (:http c) :post (str (:base c) "/api/v1/configure")
                                  {:queue q, :options (assoc qdb/queue-options :dedupWindowSeconds 86400)}
                                  15000)
                     (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))]
        (assoc op
               :type (cond (= 200 (:status del)) :ok
                           (= ::qh/refused (:type (:exception del))) :fail
                           (:exception del) :info
                           (<= 400 (:status del) 499) :fail
                           :else :info)
               :deleted-at dc
               :delete [(:status del) (or (:body del) (:type (:exception del)))]
               :configured (:status cfg)))
      (client/invoke! (:inner this) test op)))
  (teardown! [this test])
  (close! [this test] (client/close! (:inner this) test))
  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            inv-of  (fn [op] (:time (h/invocation hist op)))
            dels    (->> hist (h/filter #(and (= :recreate (:f %)) (= :ok (:type %))))
                         (map (fn [op] {:inv (inv-of op), :done (:deleted-at op)}))
                         (sort-by :inv) vec)
            pushes  (->> hist (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                         (map (fn [op] [(:value op) {:inv (inv-of op), :comp (:time op)}]))
                         (into {}))
            pops    (->> hist (h/filter #(and (= :dequeue (:f %)) (#{:ok :info} (:type %)))) vec)
            back    (vec (for [op pops
                               :let [pinv (inv-of op)]
                               m (:msgs op)
                               :let [p (pushes (:v m))]
                               :when p
                               d dels
                               :when (and (< (:comp p) (:inv d)) (< (:done d) pinv))]
                           {:v (:v m), :pop (:index op), :delete-sent (:inv d)}))
            last-d  (last dels)
            got     (set (for [op pops, m (:msgs op)] (:v m)))
            lost    (if-not last-d
                      []
                      (->> pushes (filter (fn [[v p]] (< (:done last-d) (:inv p))))
                           (map key) (remove got) sort vec))]
        {:valid?          (and (empty? back) (empty? lost))
         :recreates       (count dels)
         :came-back       (take 16 back)
         :came-back-count (count back)
         :lost-after-last-recreate (take 16 lost)
         :lost-count      (count lost)}))))

(defn workload
  [opts]
  (let [inner (queue/workload opts)
        tl    (long (:time-limit opts 300))
        q     (long (/ tl 4))]
    {:client          (->Client (:client inner))
     :checker         (checker)
     :generator       (gen/any
                        (gen/phases (gen/sleep q) (gen/once {:f :recreate, :value nil})
                                    (gen/sleep q) (gen/once {:f :recreate, :value nil})
                                    (gen/sleep q) (gen/once {:f :recreate, :value nil}))
                        (:generator inner))
     :final-generator (:final-generator inner)}))
