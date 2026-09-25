(ns jepsen.queen.workload.queue
  "W2: Queen as a lease queue, one named consumer group.

    :enqueue v   one-item POST /api/v1/push to a random partition \"w<p>\" of the
                 first test queue, transactionId \"w<v>\", payload v
    :dequeue     one pop for consumer group g1 with a short leaseSeconds, then
                 ONE batch ack of everything popped, WITH each message's
                 leaseId. Half the pops are wildcard (no partition), half are
                 pinned to a random partition; long polls (wait=true) and plain
                 pops are mixed.

  A dequeue completes :ok (value = the acked values) only when every item of
  the ack succeeded; :fail when no item did (or the pop failed); :info when the
  ack's outcome is unknown. Every completion carries :msgs, what the pop
  delivered (value, partition, offset, lease, deliveryAttempt), so the checker
  sees deliveries whatever became of their ack.

  Checkers: jepsen's total-queue (lost, unexpected) over the acked values, and
  a queue checker for what total-queue cannot see:
    - a message acked successfully twice (a noop ack counts separately);
    - a message redelivered by a pop invoked after its ack succeeded;
    - two deliveries of one message under overlapping unexpired leases
      (lease windows: [pop completed, pop invoked + leaseSeconds]);
    - per partition: one offset delivered with different values; a pop with a
      gap inside its own messages; a pop that starts past (the highest offset
      acked, or possibly acked, before it) + 1.
  The final phase drains the queue from every node."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log])
  (:import (java.net URLEncoder)))

(def group
  "The one consumer group."
  "g1")

(defn queue-name
  [test]
  (first (:queue-names test)))

(defn partition-name
  [p]
  (str "w" p))

(defn- enc
  [s]
  (URLEncoder/encode (str s) "UTF-8"))

;; ---------------------------------------------------------------------------
;; Client

(defn- enqueue!
  [client test op]
  (let [v    (:value op)
        p    (rand/long (:w2-partitions test 8))
        body {:items [{:queue         (queue-name test)
                       :partition     (partition-name p)
                       :payload       v
                       :transactionId (str "w" v)}]}
        r    (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                          body (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (case (:status item)
          ("queued" "duplicate")
          (assoc op :type :ok, :partition p, :offset (:offset item)
                 :dup? (= "duplicate" (:status item)))
          (assoc op :type :fail, :error [:item-error item])))
      (assoc op
             :type  (log/push-failure test (:status r) (:body r))
             :error [(:status r) (:body r)]))))

(defn- pop-url
  [client test {:keys [pinned batch lease wait-ms]}]
  (str (:base client) "/api/v1/pop/queue/" (enc (queue-name test))
       (when pinned (str "/partition/" (enc (partition-name pinned))))
       "?consumerGroup=" group
       "&batch=" batch
       "&leaseSeconds=" lease
       "&subscriptionMode=all"
       (if wait-ms
         (str "&wait=true&timeout=" wait-ms)
         "&wait=false")))

(defn- ack-type
  "The completion type for an ack that did not answer 200."
  [test status body]
  (log/push-failure test status body))

(defn- dequeue!
  [client test op]
  (let [pinned  (when (and (not (:drain? op)) (< (rand/double) 0.5))
                  (rand/long (:w2-partitions test 8)))
        how     {:pinned  pinned
                 :batch   (inc (rand/long 5))
                 :lease   (:w2-lease test 3)
                 :wait-ms (if (:drain? op)
                            1000
                            (rand/nth [nil 200 1000]))}
        op      (assoc op :pop how, :lease-s (:lease how))
        r       (try (qh/request! (:http client) :get (pop-url client test how) nil
                                  (+ (or (:wait-ms how) 0) (:client-timeout-ms test)))
                     (catch clojure.lang.ExceptionInfo e
                       (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                         {:exception (ex-data e)}
                         (throw e))))]
    (cond
      ; A pop that never answered took no lease we could ever use; whatever
      ; it claimed is redelivered after the lease.
      (:exception r)
      (assoc op :type :fail, :value [], :msgs []
             :error [:pop (keyword (name (:type (:exception r)))) (:msg (:exception r))])

      (= 204 (:status r))
      (assoc op :type :ok, :value [], :msgs [])

      (not= 200 (:status r))
      (assoc op :type :fail, :value [], :msgs [], :error [:pop (:status r) (:body r)])

      :else
      (let [msgs (->> (:messages (:body r))
                      (mapv (fn [m]
                              {:v         (:data m)
                               :txn       (:transactionId m)
                               :pid       (:partitionId m)
                               :partition (:partition m)
                               :offset    (:offset m)
                               :lease     (:leaseId m)
                               :attempt   (:deliveryAttempt m)})))]
        (if (empty? msgs)
          (assoc op :type :ok, :value [], :msgs [])
          (let [body {:consumerGroup   group
                      :acknowledgments (mapv (fn [m]
                                               {:transactionId (:txn m)
                                                :partitionId   (:pid m)
                                                :leaseId       (:lease m)
                                                :status        "completed"})
                                             msgs)}
                ar   (try (qh/request! (:http client) :post
                                       (str (:base client) "/api/v1/ack/batch")
                                       body (:client-timeout-ms test))
                          (catch clojure.lang.ExceptionInfo e
                            (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                              {:exception (ex-data e)}
                              (throw e))))]
            (cond
              (:exception ar)
              (assoc op
                     :type  (if (= ::qh/refused (:type (:exception ar))) :fail :info)
                     :value [], :msgs msgs
                     :error [:ack (keyword (name (:type (:exception ar))))
                             (:msg (:exception ar))])

              (not= 200 (:status ar))
              (assoc op :type (ack-type test (:status ar) (:body ar))
                     :value [], :msgs msgs, :error [:ack (:status ar) (:body ar)])

              :else
              (let [res      (vec (:body ar))
                    by-idx   (into {} (map (juxt :index identity) res))
                    msgs     (vec (map-indexed
                                    (fn [i m]
                                      (let [a (by-idx i)]
                                        (assoc m :ack (cond (nil? a)                     :missing
                                                            (and (:success a) (:noop a)) :noop
                                                            (:success a)                 :acked
                                                            :else                        :rejected)
                                               :ack-error (:error a))))
                                    msgs))
                    acked    (filterv #(= :acked (:ack %)) msgs)]
                (assoc op
                       :type  (cond (= (count acked) (count msgs)) :ok
                                    (empty? acked)                 :fail
                                    :else                          :ok)
                       :value (mapv :v acked)
                       :msgs  msgs
                       :partial-ack? (< 0 (count acked) (count msgs)))))))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue (enqueue! this test op)
        :dequeue (dequeue! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op
                   :type  (if (or (= ::qh/refused type) (= :dequeue (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; Generators

(defrecord DrainUntilEmpty [need empties]
  ; One thread's final drain: dequeue (long polls of 1 s) until `need` in a
  ; row come back empty.
  gen/Generator
  (op [this test ctx]
    (when (< empties need)
      (let [op (gen/fill-in-op {:f :dequeue, :value nil, :drain? true} ctx)]
        (if (= :pending op)
          [:pending this]
          [op this]))))

  (update [this test ctx event]
    (if (and (= :dequeue (:f event)) (:drain? event) (not= :invoke (:type event)))
      (if (and (= :ok (:type event)) (empty? (:msgs event)))
        (DrainUntilEmpty. need (inc empties))
        (DrainUntilEmpty. need 0))
      this)))

;; ---------------------------------------------------------------------------
;; Checkers

(defn- deliveries
  "Every message a pop delivered, with its op's times, lease and ack outcome."
  [history]
  (->> history
       h/client-ops
       (h/filter (fn [op] (and (= :dequeue (:f op)) (not= :invoke (:type op)))))
       (mapcat (fn [op]
                 (let [inv (h/invocation history op)]
                   (map (fn [m]
                          (assoc m
                                 :op-index  (:index op)
                                 :op-type   (:type op)
                                 :process   (:process op)
                                 :inv-time  (:time inv)
                                 :comp-time (:time op)
                                 :lease-ns  (* 1000000000 (long (:lease-s op 3)))
                                 :ack       (if (= :info (:type op)) :unknown (:ack m))))
                        (:msgs op)))))
       vec))

(defn- window
  "The time a delivery is sure to hold its lease: from the pop's completion to
  the pop's invocation + leaseSeconds."
  [d]
  [(:comp-time d) (+ (:inv-time d) (:lease-ns d))])

(defn queue-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ds        (deliveries history)
            by-v      (group-by :v ds)
            acked     (filter #(= :acked (:ack %)) ds)
            ; A message acked successfully twice.
            twice     (->> acked (group-by :v) (filter #(< 1 (count (val %))))
                           (map (fn [[v xs]] {:v v, :ops (mapv :op-index xs)})) vec)
            ; A message redelivered by a pop invoked after its ack succeeded.
            first-ack (reduce (fn [m d] (update m (:v d) (fnil min Long/MAX_VALUE) (:comp-time d)))
                              {} acked)
            redeliv   (->> ds
                           (keep (fn [d]
                                   (when-let [t (first-ack (:v d))]
                                     (when (< t (:inv-time d))
                                       {:v (:v d), :partition (:partition d), :offset (:offset d)
                                        :op (:op-index d), :ack (:ack d), :attempt (:attempt d)}))))
                           vec)
            ; Two deliveries of one message under overlapping unexpired leases.
            overlaps  (->> by-v
                           (mapcat (fn [[v xs]]
                                     (let [xs (vec (sort-by :comp-time xs))]
                                       (for [i (range (count xs))
                                             j (range (inc i) (count xs))
                                             :let [a (xs i), b (xs j)
                                                   [a0 a1] (window a)
                                                   [b0 b1] (window b)]
                                             :when (and (not= (:op-index a) (:op-index b))
                                                        (< (max a0 b0) (min a1 b1)))]
                                         {:v v, :ops [(:op-index a) (:op-index b)]
                                          :overlap-ms (quot (- (min a1 b1) (max a0 b0)) 1000000)}))))
                           vec)
            ; One offset delivered with different values.
            mismatch  (->> ds
                           (group-by (juxt :partition :offset))
                           (keep (fn [[k xs]] (let [vs (set (map :v xs))]
                                                (when (< 1 (count vs)) {:at k, :values vs}))))
                           vec)
            ; A pop with a gap inside its own messages, per partition.
            ops       (->> history h/client-ops
                           (h/filter (fn [op] (and (= :dequeue (:f op)) (seq (:msgs op)))))
                           vec)
            internal  (->> ops
                           (mapcat (fn [op]
                                     (for [[p ms] (group-by :partition (:msgs op))
                                           :let [offs (sort (map :offset ms))]
                                           :when (not= offs (range (first offs) (inc (last offs))))]
                                       {:op (:index op), :partition p, :offsets (vec offs)})))
                           vec)
            ; A pop that starts past the highest offset acked (or maybe acked)
            ; before it + 1.
            acks-by-p (->> ds
                           (filter #(#{:acked :unknown :noop} (:ack %)))
                           (group-by :partition))
            skips     (->> ops
                           (mapcat
                             (fn [op]
                               (let [inv (h/invocation history op)]
                                 (for [[p ms] (group-by :partition (:msgs op))
                                       :let [first-off (reduce min (map :offset ms))
                                             before    (->> (acks-by-p p)
                                                            (filter #(and (not= (:op-index %) (:index op))
                                                                          (< (:inv-time %) (:time op))))
                                                            (map :offset)
                                                            (reduce max -1))]
                                       :when (< (inc before) first-off)]
                                   {:op (:index op), :partition p, :first-offset first-off
                                    :highest-acked-before before}))))
                           vec)
            noops     (->> ds (filter #(= :noop (:ack %))) (mapv #(select-keys % [:v :partition :offset :op-index])))
            ; A payload that is not the value its transactionId names.
            payload   (->> ds
                           (remove #(= (:txn %) (str "w" (:v %))))
                           (mapv #(select-keys % [:v :txn :partition :offset :op-index])))
            rejected  (count (filter #(= :rejected (:ack %)) ds))
            partial   (count (filter :partial-ack? ops))]
        {:valid?             (and (empty? twice) (empty? redeliv) (empty? overlaps)
                                  (empty? mismatch) (empty? internal) (empty? skips)
                                  (empty? payload))
         :payload-mismatch   (take 16 payload)
         :deliveries         (count ds)
         :redeliveries       (count (filter #(< 1 (or (:attempt %) 1)) ds))
         :acked              (count acked)
         :ack-rejected       rejected
         :ack-unknown        (count (filter #(= :unknown (:ack %)) ds))
         :partial-acks       partial
         :noop-acks          (count noops)
         :noop-ack-examples  (take 8 noops)
         :acked-twice        (take 16 twice)
         :acked-twice-count  (count twice)
         :redelivered-after-ack       (take 16 redeliv)
         :redelivered-after-ack-count (count redeliv)
         :lease-overlaps       (take 16 overlaps)
         :lease-overlap-count  (count overlaps)
         :offset-value-mismatch (take 16 mismatch)
         :pop-internal-gaps    (take 16 internal)
         :pop-skips            (take 16 skips)
         :pop-skip-count       (count skips)}))))

(defn total-queue-checker
  "jepsen.checker/total-queue over the acked values: each :ok dequeue becomes
  one :dequeue pair per acked value."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops (->> history
                     h/client-ops
                     (mapcat (fn [op]
                               (case (:f op)
                                 :enqueue [(dissoc op :index)]
                                 ; An ack of unknown outcome may have applied:
                                 ; its values count as dequeued (a message whose
                                 ; ack did not apply is redelivered and acked
                                 ; later, so this hides no loss the drain can see).
                                 :dequeue (when-let [vs (case (:type op)
                                                          :ok   (:value op)
                                                          :info (map :v (:msgs op))
                                                          nil)]
                                            (mapcat (fn [v]
                                                      [{:type :invoke, :f :dequeue, :value nil
                                                        :process (:process op), :time (:time op)}
                                                       {:type :ok, :f :dequeue, :value v
                                                        :process (:process op), :time (:time op)}])
                                                    vs))
                                 nil)))
                     vec)]
        (checker/check (checker/total-queue) test
                       (h/history ops {:have-indices? false})
                       opts)))))

(defn workload
  [opts]
  {:client          (client)
   :checker         (checker/compose {:total-queue (total-queue-checker)
                                      :queue       (queue-checker)})
   :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                              (repeat {:f :dequeue, :value nil})])
   :final-generator (gen/each-thread (->DrainUntilEmpty 8 0))})
