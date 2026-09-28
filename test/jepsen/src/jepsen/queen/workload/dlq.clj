(ns jepsen.queen.workload.dlq
  "W7: dead letters. One queue with a dead-letter queue and a small retry
  budget (retryLimit 2), one consumer group; what a consumer does with a
  delivery depends on the value (its fate, v mod 8):

    :poison  (0, 1)  always acked `failed`: dead-lettered once the budget is
                     spent (the budget lives on the group's cursor and only an
                     explicit `failed` charges it)
    :forced  (2)     acked `dlq`: dead-lettered at once
    :flaky   (3)     `failed` the first time any client sees it, `completed`
                     after (it may still be dead-lettered: a batch nacked on a
                     spent budget goes to the DLQ as a batch). Not by
                     deliveryAttempt: that counts deliveries of a BATCH from
                     the same first offset, and resets to 1 when the batch
                     starts elsewhere (planner/pop.rs), so one message can be
                     delivered a third time at attempt 1.
    :normal  (4-7)   `completed`

  Ops: :enqueue (one push), :dequeue (one pop, then ONE batch ack of what it
  delivered, each item with its fate's status), and in the final phase, after
  every thread drained the queue, :read-dlq (the whole DLQ of the group, from
  the client's own node).

  Checker, rules that hold whatever the budget did:
    - every acknowledged push ends exactly once: completed (an ack `completed`
      that succeeded) or dead-lettered (in the DLQ at the end), never both,
      never neither;
    - a value no consumer ever nacked (:normal) is never dead-lettered, and a
      :poison or :forced one never completes;
    - an ack item that answered dlq:true is in the DLQ; the DLQ holds no value
      twice and nothing that was never pushed; every node's DLQ is the same;
    - a value completed by an ack that nacked nothing is never delivered
      again (W2's rule). An ack that releases a `failed` head with budget left
      settles nothing behind it: those messages come back with it (the cursor
      only moves over a contiguous prefix), although the ack answered
      success:true for them. That is counted, not judged."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.queue :as queue])
  (:import (java.net URLEncoder)))

(def group "g1")
(def queue-name "jepsen-dlq")
(def retry-limit 2)
(def partitions 6)

(def queue-options
  {:leaseTime                 5
   :retryLimit                retry-limit
   :retryDelay                0
   :ttl                       0
   :deadLetterQueue           true
   :dlqAfterMaxRetries        true
   :retentionEnabled          false
   :retentionSeconds          0
   :completedRetentionSeconds 0
   :dedupWindowSeconds        86400})

(defn fate
  [v]
  (case (int (mod v 8))
    (0 1) :poison
    2     :forced
    3     :flaky
    :normal))

(defn status-for
  "The ack status a consumer sends for a delivery of `v`. `nacked` (an atom
  shared by every client) holds the :flaky values already nacked once."
  [nacked v]
  (case (fate v)
    :poison "failed"
    :forced "dlq"
    :flaky  (let [[before _] (swap-vals! nacked conj v)]
              (if (contains? before v) "completed" "failed"))
    "completed"))

(defn- enc
  [s]
  (URLEncoder/encode (str s) "UTF-8"))

(defn- txn [v] (str "d" v))

;; ---------------------------------------------------------------------------
;; Client

(defn- call
  "request!, with the connection failures as data."
  [client method url body timeout-ms]
  (try (qh/request! (:http client) method url body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- enqueue!
  [client test op]
  (let [v    (:value op)
        p    (rand/long partitions)
        body {:items [{:queue         queue-name
                       :partition     (str "d" p)
                       :payload       v
                       :transactionId (txn v)}]}
        r    (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                          body (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (case (:status item)
          ("queued" "duplicate") (assoc op :type :ok, :partition p, :offset (:offset item))
          (assoc op :type :fail, :error [:item-error item])))
      (assoc op
             :type  (log/push-failure test (:status r) (:body r))
             :error [(:status r) (:body r)]))))

(defn- pop-url
  [client pinned batch wait-ms]
  (str (:base client) "/api/v1/pop/queue/" (enc queue-name)
       (when pinned (str "/partition/" (enc (str "d" pinned))))
       "?consumerGroup=" group
       "&batch=" batch
       "&leaseSeconds=3"
       "&subscriptionMode=all"
       (if wait-ms (str "&wait=true&timeout=" wait-ms) "&wait=false")))

(defn- dequeue!
  [client test op]
  (let [pinned  (when (and (not (:drain? op)) (< (rand/double) 0.5))
                  (rand/long partitions))
        wait-ms (if (:drain? op) 1000 (rand/nth [nil 200 1000]))
        r       (call client :get (pop-url client pinned (inc (rand/long 5)) wait-ms) nil
                      (+ (or wait-ms 0) (:client-timeout-ms test)))]
    (cond
      (:exception r)
      (assoc op :type :fail, :msgs [], :error [:pop (:type (:exception r))])

      (= 204 (:status r))
      (assoc op :type :ok, :msgs [])

      (not= 200 (:status r))
      (assoc op :type :fail, :msgs [], :error [:pop (:status r) (:body r)])

      :else
      (let [msgs (mapv (fn [m]
                         (let [v (:data m)]
                           {:v       v
                            :txn     (:transactionId m)
                            :pid     (:partitionId m)
                            :offset  (:offset m)
                            :lease   (:leaseId m)
                            :attempt (:deliveryAttempt m)
                            :sent    (status-for (:nacked client) v)}))
                       (:messages (:body r)))]
        (if (empty? msgs)
          (assoc op :type :ok, :msgs [])
          (let [body {:consumerGroup   group
                      :acknowledgments (mapv (fn [m]
                                               (cond-> {:transactionId (:txn m)
                                                        :partitionId   (:pid m)
                                                        :leaseId       (:lease m)
                                                        :status        (:sent m)}
                                                 (not= "completed" (:sent m))
                                                 (assoc :error (str "jepsen " (:sent m)))))
                                             msgs)}
                ar   (call client :post (str (:base client) "/api/v1/ack/batch")
                           body (:client-timeout-ms test))]
            (cond
              (:exception ar)
              (assoc op :type (if (= ::qh/refused (:type (:exception ar))) :fail :info)
                     :msgs (mapv #(assoc % :ack :unknown) msgs)
                     :error [:ack (:type (:exception ar))])

              (not= 200 (:status ar))
              (assoc op :type (log/push-failure test (:status ar) (:body ar))
                     :msgs (mapv #(assoc % :ack :unknown) msgs)
                     :error [:ack (:status ar) (:body ar)])

              :else
              (let [by-idx (into {} (map (juxt :index identity) (:body ar)))]
                (assoc op :type :ok
                       :msgs (vec (map-indexed
                                    (fn [i m]
                                      (let [a (by-idx i)]
                                        (assoc m
                                               :ack (cond (nil? a)                     :missing
                                                          (and (:success a) (:noop a)) :noop
                                                          (:success a)                 :acked
                                                          :else                        :rejected)
                                               :dlq-flag (boolean (:dlq a))
                                               :ack-error (:error a))))
                                    msgs)))))))))))

(defn- read-dlq!
  [client test op]
  (loop [offset 0, entries []]
    (let [r (qh/request! (:http client) :get
                         (str (:base client) "/api/v1/dlq?queue=" (enc queue-name)
                              "&consumerGroup=" group "&limit=1000&offset=" offset)
                         nil (:client-timeout-ms test))]
      (if (not= 200 (:status r))
        (assoc op :type :fail, :error [:dlq (:status r) (:body r)])
        (let [page    (:messages (:body r))
              entries (into entries
                            (map (fn [m] {:v      (:data m)
                                          :txn    (:transactionId m)
                                          :offset (:offset m)
                                          :pid    (:partitionId m)
                                          :retry  (:retryCount m)}))
                            page)]
          (if (and (seq page) (< (count entries) (:total (:body r) 0)))
            (recur (+ offset (count page)) entries)
            (assoc op :type :ok, :node (:node client), :value (mapv :v entries)
                   :entries entries)))))))

(defn- configure!
  [client]
  (util/await-fn
    (fn []
      (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/configure")
                           {:queue queue-name, :options queue-options} 10000)]
        (when-not (= 200 (:status r))
          (throw (ex-info "configure failed" r)))
        r))
    {:timeout 60000, :retry-interval 1000, :log-interval 10000,
     :log-message (str "configuring " queue-name)}))

(defrecord Client [nacked node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (configure! this))

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue  (enqueue! this test op)
        :dequeue  (dequeue! this test op)
        :read-dlq (read-dlq! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op
                   :type  (if (or (= ::qh/refused type) (not= :enqueue (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Checker

(defn dlq-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops       (h/client-ops history)
            pushed    (->> ops (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                           (map :value) set)
            maybe     (->> ops (h/filter #(and (= :enqueue (:f %)) (= :info (:type %))))
                           (map :value) set)
            dq-ops    (->> ops (h/filter #(and (= :dequeue (:f %)) (not= :invoke (:type %)))) vec)
            ds        (vec (mapcat (fn [op]
                                     (let [inv (h/invocation history op)]
                                       (map #(assoc % :op (:index op), :inv-time (:time inv)
                                                    :comp-time (:time op))
                                            (:msgs op))))
                                   dq-ops))
            done      (filter #(and (= "completed" (:sent %)) (= :acked (:ack %))) ds)
            completed (set (map :v done))
            ; An ack call that nacked nothing: its successes are settled.
            pure-op   (set (keep (fn [op] (when (every? #(= "completed" (:sent %)) (:msgs op))
                                            (:index op)))
                                 dq-ops))
            first-at  (fn [xs] (reduce (fn [m d] (update m (:v d) (fnil min Long/MAX_VALUE)
                                                          (:comp-time d)))
                                       {} xs))
            first-done (first-at done)
            first-pure (first-at (filter #(pure-op (:op %)) done))
            after-any (->> ds (filter #(some-> (first-done (:v %)) (< (:inv-time %)))) count)
            after     (->> ds (filter #(some-> (first-pure (:v %)) (< (:inv-time %))))
                           (map #(select-keys % [:v :offset :op :attempt])) vec)
            flagged   (set (map :v (filter :dlq-flag ds)))
            reads     (->> ops (h/filter #(and (= :read-dlq (:f %)) (= :ok (:type %)))) vec)
            per-node  (into (sorted-map)
                            (map (fn [op] [(:node op) (frequencies (:value op))]) reads))
            dlq       (set (mapcat (comp keys val) per-node))
            dups      (into (sorted-map)
                            (keep (fn [[n fr]]
                                    (let [d (sort (keep (fn [[v c]] (when (< 1 c) v)) fr))]
                                      (when (seq d) [n (vec (take 16 d))])))
                                  per-node))
            views     (distinct (map (comp set keys val) per-node))
            lost      (sort (remove #(or (completed %) (dlq %)) pushed))
            both      (sort (filter dlq completed))
            normal    (sort (filter #(= :normal (fate %)) dlq))
            wrong-done (sort (filter #(#{:poison :forced} (fate %)) completed))
            missing   (sort (remove dlq flagged))
            unexpected (sort (remove #(or (pushed %) (maybe %)) dlq))
            bad-txn   (->> reads (mapcat :entries)
                           (remove #(= (:txn %) (txn (:v %))))
                           (take 8) vec)]
        {:valid?             (and (seq reads)
                                  (empty? lost) (empty? both) (empty? normal)
                                  (empty? wrong-done) (empty? missing) (empty? unexpected)
                                  (empty? dups) (<= (count views) 1)
                                  (empty? after) (empty? bad-txn))
         :pushed             (count pushed)
         :completed          (count completed)
         :dead-lettered      (count dlq)
         :by-fate            (frequencies (map fate dlq))
         :dlq-reads          (into (sorted-map) (map (fn [[n fr]] [n (count fr)]) per-node))
         :nodes-disagree?    (< 1 (count views))
         :lost               (take 16 lost)
         :lost-count         (count lost)
         :completed-and-dead (take 16 both)
         :normal-dead        (take 16 normal)
         :poison-completed   (take 16 wrong-done)
         :flagged-not-in-dlq (take 16 missing)
         :unexpected-in-dlq  (take 16 unexpected)
         :dlq-duplicates     dups
         :redelivered-after-a-success after-any
         :delivered-after-a-settled-completion (take 16 after)
         :bad-transaction-id bad-txn}))))

(defn workload
  [opts]
  {:client          (map->Client {:nacked (atom #{})})
   :checker         (dlq-checker)
   :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                              (repeat {:f :dequeue, :value nil})])
   :final-generator (gen/phases
                      (gen/each-thread (queue/->DrainUntilEmpty 8 0))
                      (gen/sleep 5)
                      (gen/each-thread {:f :read-dlq, :value nil}))})
