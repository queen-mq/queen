(ns jepsen.queen.workload.streams
  "W9: streams, exactly once. One query reads source queue jepsen-src and
  writes sink queue jepsen-sink; its state is one cell per source partition.

    :enqueue v   one push of v to a random source partition s<p>
    :process     pop a batch (pinned to a random s<p>, a short lease, group
                 gs), read the partition's state (POST /streams/v1/state/get,
                 linearizable), then ONE POST /streams/v1/cycle that acks the
                 batch (positional, under the lease), pushes one sink record
                 {p, values} to partition o<p>, and upserts the state cell
                 agg = old + the batch: {count, sum, sq}. The sink record's
                 transactionId is unique per attempt, so a cycle committed
                 twice cannot hide behind push dedup.

  A cycle is one transaction: the planner refuses it whole when the ack's lease
  is gone (bad_lease), so a processor whose lease expired writes nothing, and
  its batch is processed again by whoever holds it next.

  Final phase: processors drain the source; then every thread reads the whole
  sink (POST /api/v1/fetch, every o<p> from offset 0) and every state cell
  from its own node.

  Checker:
    - every acknowledged push is in exactly one sink record (no loss, no
      double processing); nothing in the sink was never pushed;
    - each partition's state cell equals the sum of its sink records (count,
      sum, sum of squares): the ack, the sink push and the state move together;
    - a sink record's values all come from its own partition; every node reads
      the same sink."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.db :as db]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log])
  (:import (java.net URLEncoder)))

(def source "jepsen-src")
(def sink "jepsen-sink")
(def group "gs")
(def partitions 6)

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn- call
  [client method url body timeout-ms]
  (try (qh/request! (:http client) method url body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- unknown?
  "A response that may or may not have been applied."
  [r]
  (and (:exception r) (not= ::qh/refused (:type (:exception r)))))

;; ---------------------------------------------------------------------------
;; Client

(defn- configure!
  [client q]
  (util/await-fn
    (fn []
      (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/configure")
                           {:queue q, :options db/queue-options} 10000)]
        (when-not (= 200 (:status r)) (throw (ex-info "configure failed" r)))
        r))
    {:timeout 60000, :retry-interval 1000, :log-message (str "configuring " q)}))

(defn- register!
  "The query id, registered once for the whole test (every client shares it)."
  [client]
  (let [shared (:shared client)]
    (locking shared
      (or (:query-id @shared)
          (let [r (util/await-fn
                    (fn []
                      (let [r (qh/request! (:http client) :post
                                           (str (:base client) "/streams/v1/queries")
                                           {:name         "jepsen"
                                            :source_queue source
                                            :sink_queue   sink
                                            :config_hash  "w9"}
                                           10000)]
                        (when-not (and (= 200 (:status r)) (:success (:body r)))
                          (throw (ex-info "register failed" r)))
                        r))
                    {:timeout 60000, :retry-interval 1000, :log-message "registering the query"})
                id (:query_id (:body r))]
            (info "streams query" id)
            (swap! shared assoc :query-id id)
            id)))))

(defn- enqueue!
  [client test op]
  (let [v (:value op)
        p (rand/long partitions)
        r (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                       {:items [{:queue source, :partition (str "s" p), :payload v
                                 :transactionId (str "s" v)}]}
                       (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (if (#{"queued" "duplicate"} (:status item))
          (assoc op :type :ok, :partition p)
          (assoc op :type :fail, :error [:item-error item])))
      (assoc op :type (log/push-failure test (:status r) (:body r))
             :error [(:status r) (:body r)]))))

(defn- state-of
  "The partition's state cell, or {} when there is none; nil when the read failed."
  [client test qid pid]
  (let [r (call client :post (str (:base client) "/streams/v1/state/get")
                {:query_id qid, :partition_id (str pid), :keys ["agg"]}
                (:client-timeout-ms test))]
    (when (and (= 200 (:status r)) (:success (:body r)))
      (or (some #(when (= "agg" (:key %)) (:value %)) (:rows (:body r))) {}))))

(defn- process!
  [client test op]
  (let [qid  (register! client)
        p    (or (:p op) (rand/long partitions))
        wait (if (:drain? op) 1000 (rand/nth [nil 500]))
        r    (call client :get
                   (str (:base client) "/api/v1/pop/queue/" (enc source)
                        "/partition/" (enc (str "s" p))
                        "?consumerGroup=" group "&batch=" (inc (rand/long 4))
                        "&leaseSeconds=3&subscriptionMode=all"
                        (if wait (str "&wait=true&timeout=" wait) "&wait=false"))
                   nil (+ (or wait 0) (:client-timeout-ms test)))
        msgs (when (= 200 (:status r)) (:messages (:body r)))]
    (cond
      (:exception r)        (assoc op :type :fail, :p p, :error [:pop (:type (:exception r))])
      (= 204 (:status r))   (assoc op :type :ok, :p p, :values [])
      (not= 200 (:status r)) (assoc op :type :fail, :p p, :error [:pop (:status r) (:body r)])
      (empty? msgs)         (assoc op :type :ok, :p p, :values [])
      :else
      (let [pid    (:partitionId (first msgs))
            vs     (mapv :data msgs)
            _      (swap! (:shared client) update :pids assoc p pid)
            state  (state-of client test qid pid)]
        (if (nil? state)
          ; Nothing committed: the lease runs out and the batch comes back.
          (assoc op :type :fail, :p p, :values vs, :error :state-read)
          (let [agg  {:count (+ (:count state 0) (count vs))
                      :sum   (+ (:sum state 0) (reduce + vs))
                      :sq    (+ (:sq state 0) (reduce + (map #(* % %) vs)))}
                body {:query_id       qid
                      :partition_id   (str pid)
                      :consumer_group group
                      :push_items     [{:queue         sink
                                        :partition     (str "o" p)
                                        :payload       {:p p, :values vs}
                                        :transactionId (str "o" p "-" (random-uuid))}]
                      :ack            {:status  "completed"
                                       :leaseId (:leaseId (first msgs))
                                       :count   (count vs)}
                      :state_ops      [{:key "agg", :type "upsert", :value agg}]}
                cr   (call client :post (str (:base client) "/streams/v1/cycle") body
                           (:client-timeout-ms test))]
            (cond
              (unknown? cr)            (assoc op :type :info, :p p, :values vs, :agg agg
                                              :error [:cycle (:type (:exception cr))])
              (:exception cr)          (assoc op :type :fail, :p p, :values vs
                                              :error [:cycle (:type (:exception cr))])
              (not= 200 (:status cr))  (assoc op :type (log/push-failure test (:status cr) (:body cr))
                                              :p p, :values vs, :error [:cycle (:status cr) (:body cr)])
              (:success (:body cr))    (assoc op :type :ok, :p p, :values vs, :agg agg)
              :else                    (assoc op :type :fail, :p p, :values vs
                                              :error [:cycle (:reason (:body cr)) (:error (:body cr))]))))))))

(defn- read-sink!
  [client test op]
  (let [records (for [p (range partitions)]
                  (loop [offset 0, out []]
                    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                                         {:entries [{:queue sink, :partition (str "o" p), :offset offset}]
                                          :maxWaitMs 0}
                                         (:client-timeout-ms test))
                          e (first (:entries (:body r)))]
                      (cond
                        (not= 200 (:status r))
                        (throw (ex-info "sink read failed" {:status (:status r), :body (:body r)}))

                        (= "UNKNOWN_TOPIC_OR_PARTITION" (:error e)) out

                        (:error e)
                        (throw (ex-info "sink read failed" {:entry e}))

                        :else
                        (let [recs (:records e)
                              out  (into out (map (fn [rec] {:partition p, :offset (:offset rec)
                                                             :txn (:transactionId rec)
                                                             :payload (:payload rec)}))
                                         recs)]
                          (if (and (seq recs) (< (inc (:offset (last recs))) (:highWatermark e 0)))
                            (recur (long (inc (:offset (last recs)))) out)
                            out))))))]
    (assoc op :type :ok, :node (:node client), :value (vec (apply concat records)))))

(defn- read-state!
  [client test op]
  (let [qid  (register! client)
        pids (:pids @(:shared client))
        vals (into (sorted-map)
                   (for [[p pid] pids]
                     [p (or (state-of client test qid pid)
                            (throw (ex-info "state read failed" {:p p, :pid pid})))]))]
    (assoc op :type :ok, :node (:node client), :value vals)))

(defrecord Client [shared node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (configure! this source)
    (configure! this sink)
    (register! this))

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue    (enqueue! this test op)
        :process    (process! this test op)
        :read-sink  (read-sink! this test op)
        :read-state (read-state! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type (if (and (= :enqueue (:f op)) (not= ::qh/refused type)) :info :fail)
                   :error [(keyword (name type)) msg])
            (if (#{:read-sink :read-state} (:f op))
              (assoc op :type :fail, :error [:read (ex-data e)])
              (throw e)))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defrecord DrainUntilEmpty [n empties]
  ; One thread's final drain: pop the partitions in turn until two whole
  ; rounds in a row came back empty (a random pick could miss the one partition
  ; left).
  gen/Generator
  (op [this test ctx]
    (when (< empties (* 2 partitions))
      (let [op (gen/fill-in-op {:f :process, :value nil, :drain? true
                                :p (mod n partitions)}
                               ctx)]
        (if (= :pending op) [:pending this] [op (DrainUntilEmpty. (inc n) empties)]))))
  (update [this test ctx event]
    (if (and (= :process (:f event)) (:drain? event) (not= :invoke (:type event)))
      (if (and (= :ok (:type event)) (empty? (:values event)))
        (DrainUntilEmpty. n (inc empties))
        (DrainUntilEmpty. n 0))
      this)))

;; ---------------------------------------------------------------------------
;; Checker

(defn- agg-of
  [values]
  {:count (count values)
   :sum   (reduce + 0 values)
   :sq    (reduce + 0 (map #(* % %) values))})

(defn streams-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops     (h/client-ops history)
            pushed  (->> ops (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                         (map (juxt :value :partition)) (into {}))
            maybe   (->> ops (h/filter #(and (= :enqueue (:f %)) (= :info (:type %))))
                         (map :value) set)
            sinks   (->> ops (h/filter #(and (= :read-sink (:f %)) (= :ok (:type %)))) vec)
            states  (->> ops (h/filter #(and (= :read-state (:f %)) (= :ok (:type %)))) vec)
            recs    (:value (first sinks))
            views   (distinct (map (comp set #(map (juxt :partition :offset :txn) %) :value) sinks))
            in-sink (frequencies (mapcat (comp :values :payload) recs))
            dup     (sort (keep (fn [[v c]] (when (< 1 c) v)) in-sink))
            lost    (sort (remove in-sink (keys pushed)))
            unexp   (sort (remove #(or (pushed %) (maybe %)) (keys in-sink)))
            foreign (->> recs
                         (mapcat (fn [r] (for [v (:values (:payload r))
                                               :let [p (pushed v)]
                                               :when (and p (not= p (:partition r)))]
                                           {:v v, :pushed-to p, :sink-partition (:partition r)})))
                         (take 16) vec)
            by-p    (group-by :partition recs)
            want    (into (sorted-map)
                          (map (fn [[p rs]] [p (agg-of (mapcat (comp :values :payload) rs))]) by-p))
            bad-state (->> states
                           (mapcat (fn [op]
                                     (for [[p got] (:value op)
                                           :let [w (get want p (agg-of []))
                                                 got (select-keys got [:count :sum :sq])]
                                           :when (not= w (merge (agg-of []) got))]
                                       {:node (:node op), :partition p, :state got, :sink w})))
                           (take 16) vec)]
        {:valid?          (and (seq sinks) (seq states)
                               (empty? dup) (empty? lost) (empty? unexp) (empty? foreign)
                               (empty? bad-state) (<= (count views) 1))
         :pushed          (count pushed)
         :in-sink         (count in-sink)
         :sink-records    (count recs)
         :cycles-ok       (count (h/filter #(and (= :process (:f %)) (= :ok (:type %))
                                                 (seq (:values %)))
                                           ops))
         :cycles-unknown  (count (h/filter #(and (= :process (:f %)) (= :info (:type %))) ops))
         :processed-twice (take 16 dup)
         :processed-twice-count (count dup)
         :lost            (take 16 lost)
         :lost-count      (count lost)
         :unexpected      (take 16 unexp)
         :wrong-partition foreign
         :state-vs-sink   bad-state
         :nodes-disagree? (< 1 (count views))}))))

(defn workload
  [opts]
  (let [shared (atom {:pids {}})]
    {:client          (map->Client {:shared shared})
     :checker         (streams-checker)
     :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                                (repeat {:f :process, :value nil})])
     :final-generator (gen/phases
                        (gen/each-thread (->DrainUntilEmpty 0 0))
                        (gen/sleep 3)
                        (gen/each-thread {:f :read-sink, :value nil})
                        (gen/each-thread {:f :read-state, :value nil}))}))
