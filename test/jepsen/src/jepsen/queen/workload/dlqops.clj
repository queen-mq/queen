(ns jepsen.queen.workload.dlqops
  "W23 --workload dlqops: the dead-letter and message operations W7 leaves
  out (writing-applications/ack.mdx, retries.mdx): the `retry` ack, DLQ
  replay, and deleting a message a worker holds.

  One queue, jepsen-dq (DLQ on, retryLimit 1, group g1). What a consumer
  does with a delivery depends on v mod 4:
    0 forced  ack `dlq`: filed in the dead-letter queue at once
    1 retry   ack `retry` the first two times any client sees it (back at
              once, no retry spent), `completed` after
    2, 3      ack `completed`
  A replayed copy (transactionId dlq:<id>) is acked `completed`.

  :delete-dlq reads a page of the group's DLQ and deletes one dead letter
  (DELETE /api/v1/messages/:partitionId/:transactionId: it deletes dead
  letters only; a live message cannot be deleted, it lives in the log).

  :replay reads a page of the group's DLQ, replays one dead letter (POST
  /api/v1/dlq/:id/replay), and half the time replays it again at once: a
  replay moves the dead letter back as a new message dlq:<id> and removes
  it in the same step, so the second must create nothing.

  The checker rejects: a retry value in the DLQ (retry spends nothing); a
  value acked `completed` twice, except where the documented batch rule
  explains it (in a batch the cursor stops at the first message that comes
  back, so a `completed` one behind a `retry` of its partition comes back
  too); a value in the DLQ that no consumer acked `dlq`; a replayed dead
  letter still in the DLQ at the end, its copy delivered at two offsets, or
  never completed; a deleted dead letter still in the DLQ; an acknowledged
  push that ended neither completed, nor in the DLQ, nor replayed, nor
  deleted."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log])
  (:import (java.net URLEncoder)))

(def queue "jepsen-dq")
(def group "g1")
(def parts 4)

(def queue-options
  {:leaseTime 5, :retryLimit 1, :retryDelay 0, :ttl 0, :deadLetterQueue true
   :dlqAfterMaxRetries true, :retentionEnabled false, :retentionSeconds 0
   :completedRetentionSeconds 0, :dedupWindowSeconds 86400})

(defn fate [v] ([:forced :retry :normal :normal] (mod v 4)))

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn- call
  [client method url body timeout]
  (try (qh/request! (:http client) method url body timeout)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- unknown-or-fail [r] (if (= ::qh/refused (:type (:exception r))) :fail :info))

(defn- enqueue!
  [client test op]
  (let [v (:value op)
        r (call client :post (str (:base client) "/api/v1/push")
                {:items [{:queue queue, :partition (str "p" (rand/long parts)), :payload v
                          :transactionId (str "q" v)}]}
                (:client-timeout-ms test))]
    (cond
      (:exception r) (assoc op :type (unknown-or-fail r) :error [:push (:type (:exception r))])
      (= 201 (:status r)) (if (#{"queued" "duplicate"} (:status (first (:body r))))
                            (assoc op :type :ok)
                            (assoc op :type :fail :error [:item (first (:body r))]))
      :else (assoc op :type (log/push-failure test (:status r) (:body r)) :error [:push (:status r)]))))

(defn- action
  "What to do with a delivery: :dlq, :retry, :completed or :delete."
  [seen m]
  (if (str/starts-with? (str (:txn m)) "dlq:")
    :completed
    (case (fate (:v m))
      :forced :dlq
      :retry  (let [[before] (swap-vals! seen update (:v m) (fnil inc 0))]
                (if (< (get before (:v m) 0) 2) :retry :completed))
      :normal :completed)))

(defn- dequeue!
  [client test op]
  (let [wait (if (:drain? op) 1000 (rand/nth [0 200]))
        r    (call client :get (str (:base client) "/api/v1/pop/queue/" (enc queue)
                                    "?consumerGroup=" group "&batch=" (inc (rand/long 4))
                                    "&subscriptionMode=all"
                                    (if (pos? wait) (str "&wait=true&timeout=" wait) "&wait=false"))
                   nil (+ wait (:client-timeout-ms test)))]
    (cond
      (:exception r) (assoc op :type :fail, :msgs [], :error [:pop (:type (:exception r))])
      (= 204 (:status r)) (assoc op :type :ok, :msgs [])
      (not= 200 (:status r)) (assoc op :type :fail, :msgs [], :error [:pop (:status r)])
      :else
      (let [msgs  (mapv (fn [m] {:v (:data m), :txn (:transactionId m), :pid (:partitionId m)
                                 :offset (:offset m), :lease (:leaseId m)})
                        (:messages (:body r)))
            plan  (mapv #(assoc % :action (action (:seen client) %)) msgs)
            acks  plan
            ar    (when (seq acks)
                    (call client :post (str (:base client) "/api/v1/ack/batch")
                          {:consumerGroup group
                           :acknowledgments (mapv (fn [m] {:transactionId (:txn m), :partitionId (:pid m)
                                                           :leaseId (:lease m), :status (name (:action m))})
                                                  acks)}
                          (:client-timeout-ms test)))
            res   (when (= 200 (:status ar)) (into {} (map (juxt :index identity) (:body ar))))
            known (and ar (not (:exception ar)) (= 200 (:status ar)))
            plan  (let [ix (zipmap (map :txn acks) (range))]
                    (mapv (fn [m]
                            (if-let [i (ix (:txn m))]
                              (assoc m :ack (cond (not known)            :unknown
                                                  (:success (res i))     :acked
                                                  :else                  :rejected))
                              m))
                          plan))]
        (assoc op :type :ok, :msgs plan)))))

(defn- read-dlq!
  [client test]
  (loop [offset 0, out []]
    (let [r (qh/request! (:http client) :get
                         (str (:base client) "/api/v1/dlq?queue=" (enc queue) "&consumerGroup=" group
                              "&limit=1000&offset=" offset)
                         nil (:client-timeout-ms test))]
      (when-not (= 200 (:status r))
        (throw (ex-info "dlq read failed" {:status (:status r)})))
      (let [page (:messages (:body r))
            out  (into out (map (fn [m] {:id (:id m), :txn (:transactionId m), :v (:data m)
                                         :pid (:partitionId m)})
                                page))]
        (if (and (seq page) (< (count out) (:total (:body r) 0)))
          (recur (+ offset (count page)) out)
          out)))))

(defn- replay!
  [client test op]
  (let [page (try (read-dlq! client test) (catch clojure.lang.ExceptionInfo e nil))]
    (if-let [d (and (seq page) (rand/nth page))]
      (let [once  (fn [] (call client :post (str (:base client) "/api/v1/dlq/" (enc (:id d)) "/replay")
                               nil (:client-timeout-ms test)))
            r1    (once)
            r2    (when (< (rand/double) 0.5) (once))
            judge (fn [r] (cond (:exception r) (unknown-or-fail r)
                                (and (= 200 (:status r)) (get-in r [:body :replayedAs])) :ok
                                :else :fail))]
        (assoc op :type (judge r1), :id (:id d), :value (:v d), :txn (:txn d)
               :again (when r2 (judge r2))
               :replayed-as (get-in r1 [:body :replayedAs :offset])))
      (assoc op :type :fail, :error :empty-dlq))))

(defn- delete-dlq!
  [client test op]
  (let [page (try (read-dlq! client test) (catch clojure.lang.ExceptionInfo e nil))]
    (if-let [d (and (seq page) (rand/nth page))]
      (let [r (call client :delete (str (:base client) "/api/v1/messages/" (enc (:pid d)) "/" (enc (:txn d)))
                    nil (:client-timeout-ms test))]
        (assoc op :type (cond (:exception r) (unknown-or-fail r)
                              (and (#{200 204} (:status r)) (not (false? (get-in r [:body :success])))) :ok
                              :else :fail)
               :id (:id d), :txn (:txn d), :value (:v d)
               :answer (when-not (:exception r) [(:status r) (:body r)])))
      (assoc op :type :fail, :error :empty-dlq))))

(defrecord Client [node base http seen]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      (let [r (qh/request! http :post (str base "/api/v1/configure")
                           {:queue queue, :options queue-options} 15000)]
        (when-not (= 200 (:status r)) (throw (ex-info "configure failed" {:answer r}))))))

  (invoke! [this test op]
    (case (:f op)
      :enqueue (enqueue! this test op)
      :dequeue (dequeue! this test op)
      :replay  (replay! this test op)
      :delete-dlq (delete-dlq! this test op)
      :read-dlq (try (assoc op :type :ok, :value (read-dlq! this test))
                     (catch clojure.lang.ExceptionInfo e (assoc op :type :fail, :error (ex-data e))))))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist     (h/client-ops history)
            done     (->> hist (h/filter #(not= :invoke (:type %))) vec)
            pushed   (->> done (filter #(and (= :enqueue (:f %)) (= :ok (:type %)))) (map :value) set)
            dlq      (:value (last (filter #(and (= :read-dlq (:f %)) (= :ok (:type %))) done)))
            dlq-txns (set (map :txn dlq))
            deqs     (->> hist (h/filter #(and (= :dequeue (:f %)) (= :ok (:type %)))) vec)
            ds       (for [op deqs, m (:msgs op)]
                       (assoc m :inv (:time (h/invocation hist op)) :comp (:time op) :index (:index op)))
            acked    (fn [action] (->> ds (filter #(and (= action (:action %)) (= :acked (:ack %))))))
            completed (->> (acked :completed) (remove #(str/starts-with? (str (:txn %)) "dlq:"))
                           (map :v) frequencies)
            ; The documented batch rule: in one ack batch the cursor stops at
            ; the first message that comes back (here a `retry`), so a
            ; `completed` one behind it in its partition is delivered again.
            shadowed (set (for [op deqs
                                :let [ms (:msgs op)]
                                m ms
                                :when (and (= :completed (:action m))
                                           (some #(and (= :retry (:action %)) (= (:pid %) (:pid m))
                                                       (< (:offset %) (:offset m)))
                                                 ms))]
                            [(:v m) (:index op)]))
            ; A retry value: never dead-lettered, never completed twice.
            retry-dlq (->> dlq (filter #(= :retry (fate (:v %)))) (mapv :txn))
            ; Completions that count: not shadowed by the batch rule.
            counted  (->> (acked :completed)
                          (remove #(str/starts-with? (str (:txn %)) "dlq:"))
                          (remove #(shadowed [(:v %) (:index %)]))
                          (map :v) frequencies)
            twice    (->> counted (filter #(< 1 (val %))) (map key) sort vec)
            ; In the DLQ although nobody acked it `dlq`.
            dlq-acked (set (map :txn (acked :dlq)))
            unasked  (->> dlq (remove #(or (dlq-acked (:txn %))
                                           ; an ack `dlq` whose answer was lost
                                           (some (fn [d] (and (= (:txn d) (:txn %)) (= :dlq (:action d))
                                                              (= :unknown (:ack d))))
                                                 ds)))
                          (mapv :txn))
            ; Replays.
            replays  (->> done (filter #(and (= :replay (:f %)) (= :ok (:type %)))))
            copies   (fn [id] (->> ds (filter #(= (str "dlq:" id) (:txn %)))))
            replay-bad (->> replays
                            (keep (fn [r]
                                    (let [cs   (copies (:id r))
                                          offs (set (map :offset cs))
                                          done? (some #(= :acked (:ack %)) cs)]
                                      (cond
                                        (dlq-txns (:txn r)) {:id (:id r), :problem :still-in-dlq}
                                        (< 1 (count offs))  {:id (:id r), :problem :two-copies, :offsets offs}
                                        (not done?)         {:id (:id r), :problem :copy-never-completed}))))
                            vec)
            ; Deletes of dead letters: gone from the DLQ.
            deletes  (->> done (filter #(= :delete-dlq (:f %))))
            del-ok   (->> deletes (filter #(= :ok (:type %))))
            after-del (->> del-ok (filter #(dlq-txns (:txn %))) (mapv #(select-keys % [:id :txn])))
            ; Every acknowledged push ended somewhere.
            ended?   (fn [v]
                       (or (contains? completed v)
                           (dlq-txns (str "q" v))
                           (some #(and (= v (:value %)) (= :ok (:type %))) replays)
                           (some #(and (= v (:value %)) (#{:ok :info} (:type %))) deletes)
                           ; an ack whose answer was lost may have applied
                           (some #(and (= v (:v %)) (= :unknown (:ack %))) ds)))
            lost     (->> pushed (remove ended?) sort vec)]
        {:valid?          (boolean (and dlq (empty? retry-dlq) (empty? twice) (empty? unasked)
                                        (empty? replay-bad) (empty? after-del) (empty? lost)))
         :replays         (count replays)
         :second-replays  (frequencies (keep :again replays))
         :deletes         (frequencies (map :type deletes))
         :batch-rule-redeliveries (count shadowed)
         :retry-in-dlq    (take 16 retry-dlq)
         :completed-twice (take 16 twice)
         :dlq-unasked     (take 16 unasked)
         :replay-problems (take 16 replay-bad)
         :deleted-still-in-dlq (take 16 after-del)
         :lost            (take 16 lost)
         :lost-count      (count lost)}))))

(defrecord Drain [need empties]
  gen/Generator
  (op [this test ctx]
    (when (< empties need)
      (let [op (gen/fill-in-op {:f :dequeue, :value nil, :drain? true} ctx)]
        (if (= :pending op) [:pending this] [op this]))))
  (update [this test ctx event]
    (if (and (= :dequeue (:f event)) (:drain? event) (not= :invoke (:type event)))
      (Drain. need (if (and (= :ok (:type event)) (empty? (:msgs event))) (inc empties) 0))
      this)))

(defn workload
  [opts]
  {:client          (map->Client {:seen (atom {})})
   :checker         (checker)
   :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                              (repeat {:f :dequeue, :value nil})
                              (repeat {:f :dequeue, :value nil})
                              (repeat {:f :replay, :value nil})
                              (repeat {:f :delete-dlq, :value nil})])
   :final-generator (gen/phases (gen/each-thread (->Drain 6 0))
                                (gen/once {:f :read-dlq, :value nil}))})
