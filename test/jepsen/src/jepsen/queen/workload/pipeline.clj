(ns jepsen.queen.workload.pipeline
  "W5 --workload pipeline: exactly-once processing through /api/v1/transaction.

    :enqueue m      push m to `in` (the first test queue), partition i<p>,
                    transactionId \"i<m>\"
    :process        pop ONE message from `in` for consumer group w5 with a
                    short lease, then ONE /transaction:
                      ack m in `in` WITH its leaseId,
                      push d(m) = {m, k, id} to `out` (the second test queue)
                        with a RANDOM transactionId (id), so dedup cannot hide
                        a second commit,
                      kv incr c:<k>, k = m mod K.
                    :ok when the transaction answers success:true; :fail when
                    it rolled back (success:false) or was refused; :info when
                    the outcome is unknown.
    :read-counters  getMany of every c:<k>
    final phase     :process until `in` is empty, then :final-read: every
                    record of `out` (fetch from offset 0) and every counter.

  Checker (the last :final-read is the end state):
    1. no input lost: every acknowledged enqueue has an `out` record;
    2. no input committed twice: at most one `out` record per m;
    3. no phantom: no `out` record comes from a transaction that failed
       (or from no transaction at all), and every :ok transaction's record is
       there;
    4. every final counter equals the number of `out` records of its key;
    5. counter bounds: every read of c:<k> lies between the transactions of k
       known to have committed before the read began and those that may have
       committed before it ended."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]
            [jepsen.queen.workload [log :as log]
                                   [queue :as queue]])
  (:import (java.net URLEncoder)))

(def group "w5")

(defn in-queue  [test] (first (:queue-names test)))
(defn out-queue [test] (second (:queue-names test)))
(defn counter-keys [test] (:w5-keys test 5))
(defn partitions [test] (:w5-partitions test 4))

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn- enqueue!
  [client test op]
  (let [m (:value op)
        r (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                       {:items [{:queue         (in-queue test)
                                 :partition     (str "i" (rand/long (partitions test)))
                                 :payload       m
                                 :transactionId (str "i" m)}]}
                       (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (if (#{"queued" "duplicate"} (:status item))
          (assoc op :type :ok)
          (assoc op :type :fail, :error [:item-error item])))
      (assoc op :type (log/push-failure test (:status r) (:body r))
             :error [(:status r) (:body r)]))))

(defn- process!
  [client test op]
  (let [url (str (:base client) "/api/v1/pop/queue/" (enc (in-queue test))
                 "?consumerGroup=" group "&batch=1&leaseSeconds=" (:w2-lease test 3)
                 "&subscriptionMode=all&wait=true&timeout=500")
        r   (qh/request! (:http client) :get url nil (+ 500 (:client-timeout-ms test)))]
    (cond
      (= 204 (:status r)) (assoc op :type :ok, :value nil)
      (not= 200 (:status r)) (assoc op :type :fail, :value nil, :error [:pop (:status r) (:body r)])
      :else
      (let [msg (first (:messages (:body r)))]
        (if-not msg
          (assoc op :type :ok, :value nil)
          (let [m    (:data msg)
                k    (mod (long m) (counter-keys test))
                id   (str (random-uuid))
                v    {:m m, :k k, :id id}
                body {:operations
                      [{:type          "ack"
                        :transactionId (:transactionId msg)
                        :partitionId   (:partitionId msg)
                        :leaseId       (:leaseId msg)
                        :consumerGroup group}
                       {:type  "push"
                        :items [{:queue         (out-queue test)
                                 :partition     (str "o" (mod (long m) (partitions test)))
                                 :payload       v
                                 :transactionId id}]}]
                      :kv [{:op "incr", :ns kv/ns-name*, :key (str "c:" k)
                            :delta 1, :forever true}]}
                op   (assoc op :value v, :attempt (:deliveryAttempt msg))
                t    (try (qh/request! (:http client) :post
                                       (str (:base client) "/api/v1/transaction")
                                       body (:client-timeout-ms test))
                          (catch clojure.lang.ExceptionInfo e
                            (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                              {:exception (ex-data e)}
                              (throw e))))]
            (cond
              (:exception t)
              (assoc op :type (kv/exception-type (:type (:exception t)))
                     :error [:txn (keyword (name (:type (:exception t))))
                             (:msg (:exception t))])

              (and (= 200 (:status t)) (true? (:success (:body t))))
              (assoc op :type :ok)

              (= 200 (:status t))
              (assoc op :type :fail
                     :error [:rolled-back (select-keys (:body t) [:reason :error])])

              :else
              (assoc op :type (kv/write-failure [(:status t) (:body t)])
                     :error [:txn (:status t) (:body t)]))))))))

(defn- read-counters!
  [client test op]
  (let [ks (mapv #(str "c:" %) (range (counter-keys test)))
        r  (kv/batch! test client [{:op "getMany", :keys ks}])]
    (if (:error r)
      (assoc op :type :fail, :error (:error r))
      (let [rows (into {} (map (juxt :key :value)) (:rows (first (:results r))))]
        (assoc op :type :ok
               :value (into (sorted-map)
                            (map (fn [k] [k (long (or (get rows (str "c:" k)) 0))]))
                            (range (counter-keys test))))))))

(defn- fetch-partition
  "Every record of one partition of `queue`, from offset 0."
  [client test queue partition]
  (loop [off 0, out []]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                         {:entries [{:queue queue, :partition partition, :offset off}]}
                         (:client-timeout-ms test))
          e (first (:entries (:body r)))]
      (cond
        (not= 200 (:status r)) (throw (ex-info "fetch failed" {:status (:status r)}))
        (seq (:records e))     (recur (inc (long (:offset (peek (:records e)))))
                                      (into out (:records e)))
        :else                  out))))

(defn- final-read!
  [client test op]
  (let [out (->> (range (partitions test))
                 (mapcat (fn [p]
                           (map (fn [rec] (assoc (:payload rec)
                                                 :partition (str "o" p)
                                                 :offset (:offset rec)
                                                 :transactionId (:transactionId rec)))
                                (fetch-partition client test (out-queue test) (str "o" p)))))
                 vec)
        c   (read-counters! client test {:f :read-counters})]
    (if (= :ok (:type c))
      (assoc op :type :ok, :value {:out out, :counters (:value c)})
      (assoc op :type :fail, :error [:counters (:error c)]))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue       (enqueue! this test op)
        :process       (process! this test op)
        :read-counters (read-counters! this test op)
        :final-read    (final-read! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op
                   :type  (if (#{:enqueue} (:f op)) (kv/exception-type type) :fail)
                   :error [(keyword (name type)) msg])
            (if (= "fetch failed" (.getMessage e))
              (assoc op :type :fail, :error [:fetch (ex-data e)])
              (throw e)))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; Checker

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops      (h/client-ops history)
            enq-ok   (->> ops (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                          (map :value) set)
            ; Completed transactions (a pop that found nothing has no
            ; value), each with its invocation time.
            procs    (->> ops (h/filter #(and (= :process (:f %)) (not= :invoke (:type %))
                                              (some? (:value %))))
                          (mapv #(assoc % ::inv-time (:time (h/invocation history %)))))
            by-id    (into {} (map (juxt (comp :id :value) identity)) procs)
            final    (->> ops (h/filter #(and (= :final-read (:f %)) (= :ok (:type %))))
                          vec)]
        (if (empty? final)
          {:valid? :unknown, :error "no final read succeeded"}
          ; The end state is the final read invoked LAST: every thread reads
          ; once its own drain is over, and the last to start saw every other
          ; thread's processing finished.
          (let [last-rd  (apply max-key #(:time (h/invocation history %)) final)
                {:keys [out counters]} (:value last-rd)
                agree?   (apply = (map (comp set :out :value) final))
                out-by-m (group-by :m out)
                ; Transactions that finished after the end state was read
                ; (a thread whose own final read failed kept going): the end
                ; state cannot answer for them.
                end-inv  (:time (h/invocation history last-rd))
                late     (filter #(< end-inv (:time %)) procs)
                late-m   (set (map (comp :m :value) late))
                late-k   (set (map (comp :k :value) late))
                ; 1. lost
                lost     (->> enq-ok (remove out-by-m) (remove late-m) sort vec)
                ; 2. committed twice
                twice    (->> out-by-m (filter #(< 1 (count (val %))))
                              (map (fn [[m rs]] {:m m, :ids (mapv :id rs)})) vec)
                ; 3. phantoms, and :ok transactions whose record is missing
                out-ids  (set (map :id out))
                phantom  (->> out
                              (keep (fn [r]
                                      (let [p (by-id (:id r))]
                                        (cond (nil? p)            {:out r, :why :no-such-transaction}
                                              (= :fail (:type p)) {:out r, :why :failed-transaction
                                                                   :error (:error p)}))))
                              vec)
                missing  (->> procs
                              (filter #(and (= :ok (:type %)) (<= (:time %) end-inv)))
                              (remove #(out-ids (:id (:value %))))
                              (mapv :value))
                ; 4. final counters against the records
                per-k    (frequencies (map :k out))
                counts   (->> (keys (merge per-k counters))
                              (remove late-k)
                              (keep (fn [k]
                                      (let [c (get counters k 0), n (get per-k k 0)]
                                        (when (not= c n) {:k k, :counter c, :out-records n}))))
                              vec)
                ; 5. counter bounds: [committed before the read began,
                ;    maybe-committed before it ended]
                reads    (->> ops (h/filter #(and (= :read-counters (:f %)) (= :ok (:type %))))
                              vec)
                procs-k  (group-by (comp :k :value) procs)
                bounds   (->> reads
                              (mapcat
                                (fn [rd]
                                  (let [inv (h/invocation history rd)]
                                    (for [[k v] (:value rd)
                                          :let [ps    (procs-k k)
                                                lower (count (filter #(and (= :ok (:type %))
                                                                           (< (:time %) (:time inv)))
                                                                     ps))
                                                upper (count (filter #(and (#{:ok :info} (:type %))
                                                                           (< (::inv-time %) (:time rd)))
                                                                     ps))]
                                          :when (not (<= lower v upper))]
                                      {:k k, :read v, :lower lower, :upper upper
                                       :op (:index rd)}))))
                              vec)]
            {:valid?            (and (empty? lost) (empty? twice) (empty? phantom)
                                     (empty? missing) (empty? counts) (empty? bounds))
             :inputs            (count enq-ok)
             :out-records       (count out)
             :transactions-ok   (count (filter #(= :ok (:type %)) procs))
             :transactions-fail (count (filter #(= :fail (:type %)) procs))
             :transactions-info (count (filter #(= :info (:type %)) procs))
             :final-reads-agree agree?
             :late-transactions (count late)
             :lost-count        (count lost)
             :lost              (take 16 lost)
             :twice-count       (count twice)
             :twice             (take 16 twice)
             :phantom-count     (count phantom)
             :phantom           (take 16 phantom)
             :ok-txn-missing    (take 16 missing)
             :counter-mismatch  counts
             :bound-violations  (take 16 bounds)
             :bound-violation-count (count bounds)
             :counter-reads     (count reads)}))))))

;; ---------------------------------------------------------------------------
;; Generators

(defrecord ProcessUntilEmpty [need empties]
  ; One thread's drain: :process until `need` in a row found nothing, then
  ; one :final-read.
  gen/Generator
  (op [this test ctx]
    (cond
      (< empties need)
      (let [op (gen/fill-in-op {:f :process, :value nil, :drain? true} ctx)]
        (if (= :pending op) [:pending this] [op this]))

      (= empties need)
      (let [op (gen/fill-in-op {:f :final-read, :value nil} ctx)]
        (if (= :pending op) [:pending this] [op (ProcessUntilEmpty. need (inc empties))]))

      :else nil))

  (update [this test ctx event]
    (if (and (= :process (:f event)) (:drain? event) (not= :invoke (:type event))
             (< empties need))
      (if (and (= :ok (:type event)) (nil? (:value event)))
        (ProcessUntilEmpty. need (inc empties))
        (ProcessUntilEmpty. need 0))
      this)))

(defn workload
  [opts]
  {:client          (client)
   :checker         (checker)
   :generator       (gen/any
                      (gen/mix [(map (fn [m] {:f :enqueue, :value m}) (range))
                                (repeat {:f :process, :value nil})])
                      (gen/stagger 1 (repeat {:f :read-counters, :value nil})))
   :final-generator (gen/each-thread (->ProcessUntilEmpty 8 0))
   :db-env          kv/db-env})
