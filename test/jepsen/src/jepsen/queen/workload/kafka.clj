(ns jepsen.queen.workload.kafka
  "W14 --workload kafka: QueenMQ's Kafka facade, driven by the Java Kafka
  client (org.apache.kafka/kafka-clients) and judged by Jepsen's own Kafka
  workload (jepsen.tests.kafka): the test Jepsen ran on Redpanda and
  Bufstream, over the port Kafka applications use.

  An abstract key k is one Kafka topic-partition: topic \"jepsen-t<k div 8>\",
  partition k mod 8. The topics are created through the facade's admin API
  (CreateTopics, 8 partitions each) before the first op that needs them.

  Each client has a producer and a consumer, bound to its own node as their
  bootstrap server; the facade then sends them to the raft leader, which
  leads every partition, group and transactional id.

    producer  enable.idempotence, acks=all; with --txn a transactional.id
              per worker thread (\"jepsen-txn-<thread>\"), so a crashed
              client's successor fences it with InitProducerId
    consumer  group \"jepsen-group\", isolation.level=read_committed, no
              auto commit, auto.offset.reset=earliest

  Ops (the Kafka workload's):

    :send / :poll / :txn  micro-ops [:send k v] and [:poll]. A :send without
                          --txn is one ProducerRecord to k's partition, waited
                          for; completes as [:send k [offset v]]. A :poll is
                          one consumer.poll; completes as [:poll {k [[o v]]}].
                          Subscribed (not assigned), a poll commits its
                          offsets with commitSync before it completes.
                          With --txn every op is one producer transaction:
                          its polls' offsets go in with
                          sendOffsetsToTransaction, then commitTransaction. A
                          send inside a transaction answers offset -1 on the
                          facade (its offsets exist only at the commit), so it
                          completes as [:send k [nil v]].
    :assign / :subscribe  consumer.assign / consumer.subscribe (classic
                          groups); :seek-to-beginning? seeks every assigned
                          partition to its start. Rebalances seen during an op
                          go in its :rebalance-log.
    :crash                closes everything and answers :info, so Jepsen opens
                          a fresh client (and, with --txn, a fresh producer
                          that fences the old transactional epoch).
    :debug-topic-partitions  the facade's view of the keys' partitions.

  A failed op leaves no trace: the consumer's positions go back to where the
  op found them (Kyle Kingsbury's with-consumer-rollback), so a later poll
  reads those records again.

  Outcomes (the table the crafted-history tests pin down):

    an exception thrown by producer.send itself (metadata wait, serializer)
                                          :fail  nothing was sent
    a send's future failing with a non-retriable error the broker answered
    (RecordTooLarge, InvalidTopic, TopicAuthorization, InvalidRequired)
                                          :fail
    a send's future failing otherwise (timeouts, expiry, connection, not
    leader after retries, unknown)        :info
    a transaction that failed and was then ABORTED by the facade (abort
    answered)                             :fail  an abort is definite
    a transaction whose commit or abort did not answer, or a fenced producer
                                          :info
    a poll-only op that failed            :fail  (reads change nothing; the
                                                  positions roll back)

  The checker is jepsen.tests.kafka's analysis judged STRICTER than Kafka's
  own model: a facade commit is one raft entry holding every record and
  offset of the transaction (reference/kafka.mdx), so G0, G1c and
  int-send-skip, which Kafka's transactions allow, are anomalies here. With
  --sub-via subscribe, poll skips and nonmonotonic polls between ops are
  allowed (a rebalance moves partitions between consumers), as Jepsen allows
  them for Kafka. The verdict under Kafka's own allowances is reported too,
  as :kafka-model-valid?."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.tests.kafka :as kafka]
            [jepsen.tests.cycle.append :as append])
  (:import (java.time Duration)
           (java.util Optional Properties)
           (java.util.concurrent ExecutionException Future TimeUnit)
           (org.apache.kafka.clients.admin Admin NewTopic TopicDescription)
           (org.apache.kafka.clients.consumer ConsumerRebalanceListener
                                              ConsumerRecord KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord
                                              RecordMetadata)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringDeserializer
                                                  StringSerializer)))

;; ---------------------------------------------------------------------------
;; Keys, topics, ports

(def kafka-port
  "The facade's port on every node: 9092. QUEEN_JEPSEN_KAFKA_PORT moves it,
  for a smoke run against a scratch broker."
  (or (some-> (System/getenv "QUEEN_JEPSEN_KAFKA_PORT") parse-long)
      9092))

(def partitions-per-topic 8)

(def topic-prefix "jepsen-t")

(defn topic-of
  ^String [k]
  (str topic-prefix (quot k partitions-per-topic)))

(defn tp-of
  ^TopicPartition [k]
  (TopicPartition. (topic-of k) (int (mod k partitions-per-topic))))

(defn k-of
  "The abstract key of a topic-partition, or nil for a topic not ours."
  [^String topic partition]
  (when (str/starts-with? topic topic-prefix)
    (when-let [t (parse-long (subs topic (count topic-prefix)))]
      (+ (* partitions-per-topic t) (long partition)))))

(defn bootstrap
  [node]
  (str (cn/ip node) ":" kafka-port))

(defn- props
  ^Properties [m]
  (let [p (Properties.)]
    (doseq [[k v] m :when (some? v)]
      (.put p (str k) (str v)))
    p))

;; ---------------------------------------------------------------------------
;; Topics: created once per test, by whichever client needs one first.

(defonce ^:private topics
  ; [test start time, topic] -> promise of true (exists) or an exception
  (atom {}))

(defn- create-topic!
  "CreateTopics for one topic (an existing one is fine), then waits until the
  facade describes all its partitions. True, or throws."
  [^Admin admin ^String topic]
  (try (-> (.createTopics admin [(NewTopic. topic
                                            (Optional/of (int partitions-per-topic))
                                            (Optional/empty))])
           (.all)
           (.get 10 TimeUnit/SECONDS))
       (catch ExecutionException e
         (when-not (re-find #"TopicExists" (str (class (.getCause e))))
           (throw e))))
  (util/await-fn
    (fn []
      (let [^TopicDescription d (-> (.describeTopics admin [topic])
                                    (.allTopicNames)
                                    (.get 5 TimeUnit/SECONDS)
                                    (get topic))]
        (when-not (= partitions-per-topic (count (.partitions d)))
          (throw (ex-info "topic not complete yet" {:topic topic})))
        true))
    {:timeout 15000, :retry-interval 250, :log-interval 5000
     :log-message (str "waiting for topic " topic)}))

(defn ensure-topics!
  "Every topic the keys `ks` live in exists. The first client to need a
  topic creates it; the others wait for its answer. A creation that fails is
  forgotten, so a later op tries again."
  [admin test ks]
  (doseq [t (distinct (map topic-of ks))]
    (let [k    [(:start-time test) t]
          p    (promise)
          [m]  (swap-vals! topics #(if (contains? % k) % (assoc % k p)))
          mine (not (contains? m k))
          p    (if mine p (get m k))]
      (when mine
        (deliver p (try (create-topic! admin t)
                        (catch Exception e e))))
      (let [r (deref p 30000 ::timeout)]
        (when-not (true? r)
          (when mine (swap! topics dissoc k))
          (throw (ex-info "topic not available"
                          {:type ::no-topic, :topic t
                           :cause (if (instance? Throwable r) (str r) r)})))))))

;; ---------------------------------------------------------------------------
;; Kafka clients

(defn- thread-of
  [test process]
  (mod process (:concurrency test)))

(defn make-producer
  ^KafkaProducer [test node process]
  (KafkaProducer.
    (props {"bootstrap.servers"     (bootstrap node)
            "acks"                  "all"
            "enable.idempotence"    "true"
            "key.serializer"        (.getName StringSerializer)
            "value.serializer"      (.getName StringSerializer)
            "linger.ms"             "0"
            "request.timeout.ms"    "5000"
            "delivery.timeout.ms"   "10000"
            "max.block.ms"          "10000"
            "metadata.max.age.ms"   "5000"
            "retry.backoff.ms"      "100"
            "transactional.id"      (when (:txn? test)
                                      (str "jepsen-txn-" (thread-of test process)))
            "transaction.timeout.ms" (when (:txn? test) "15000")})))

(defn make-consumer
  ^KafkaConsumer [test node]
  (KafkaConsumer.
    (props {"bootstrap.servers"       (bootstrap node)
            "group.id"                "jepsen-group"
            "enable.auto.commit"      "false"
            "auto.offset.reset"       "earliest"
            "isolation.level"         "read_committed"
            "key.deserializer"        (.getName StringDeserializer)
            "value.deserializer"      (.getName StringDeserializer)
            "max.poll.records"        "500"
            "session.timeout.ms"      "10000"
            "heartbeat.interval.ms"   "1000"
            "max.poll.interval.ms"    "60000"
            "request.timeout.ms"      "5000"
            "default.api.timeout.ms"  "10000"
            "metadata.max.age.ms"     "5000"
            "group.protocol"          "classic"})))

(defn make-admin
  ^Admin [node]
  (Admin/create
    (props {"bootstrap.servers"        (bootstrap node)
            "request.timeout.ms"       "5000"
            "default.api.timeout.ms"   "10000"})))

(defn- close-quietly!
  [x]
  (when x
    (try (cond
           (instance? KafkaProducer x) (.close ^KafkaProducer x (Duration/ofSeconds 2))
           (instance? KafkaConsumer x) (.close ^KafkaConsumer x (Duration/ofSeconds 2))
           (instance? Admin x)         (.close ^Admin x (Duration/ofSeconds 2)))
         (catch Throwable t
           (warn "closing" (class x) "threw" (str t))))))

;; ---------------------------------------------------------------------------
;; Outcomes

(defn root-cause
  ^Throwable [^Throwable t]
  (loop [t t]
    (let [c (.getCause t)]
      (if (and c (not (identical? c t))) (recur c) t))))

(defn error-name
  "The simple class name of an exception's root cause (ExecutionException
  and KafkaException wrappers peeled off)."
  [^Throwable t]
  (.getSimpleName (class (root-cause t))))

(def definite-send-errors
  "Errors a send's future can end with that mean the broker refused the
  record, or the producer never sent it: nothing was written."
  #{"RecordTooLargeException" "InvalidTopicException"
    "TopicAuthorizationException" "SerializationException"
    "InvalidRequiredAcksException" "RecordBatchTooLargeException"
    "UnsupportedVersionException" "ClusterAuthorizationException"})

(def producer-fatal-errors
  "Errors after which a producer must be closed and a new one made."
  #{"ProducerFencedException" "OutOfOrderSequenceException"
    "InvalidProducerEpochException" "UnsupportedVersionException"
    "AuthorizationException" "TransactionalIdAuthorizationException"
    "ClusterAuthorizationException" "IllegalStateException"
    "InvalidTxnStateException" "FencedInstanceIdException"
    "UnknownProducerIdException" "TransactionAbortedException"})

(defn send-outcome
  "The completion type for a non-transactional send that threw `t`:
  `synchronous?` when producer.send itself threw (nothing left the client)."
  [t synchronous?]
  (cond synchronous?                                :fail
        (definite-send-errors (error-name t))       :fail
        :else                                       :info))

(defn txn-outcome
  "{:type :error} for a transaction that threw `why` (an error-name), given
  whether the abort that followed was answered. A producer-fatal error is
  :info whatever happened before it (its epoch is gone, its commit may have
  landed); an answered abort is :fail (the facade says nothing of it is
  visible: if a poll ever shows its records, the checker calls it an aborted
  read, which is what that would be); an abort that did not answer is :info."
  [why aborted?]
  (cond (producer-fatal-errors why) {:type :info, :error [:txn why :fatal]}
        aborted?                    {:type :fail, :error [:txn why :aborted]}
        :else                       {:type :info, :error [:txn why :abort-failed]}))

;; ---------------------------------------------------------------------------
;; The client

(defn- poll-once
  "One consumer.poll; its records by key: {k [[offset v] ...]}."
  [^KafkaConsumer consumer ms]
  (let [records (.poll consumer (Duration/ofMillis (long ms)))]
    (reduce (fn [m ^ConsumerRecord r]
              (if-let [k (k-of (.topic r) (.partition r))]
                (update m k (fnil conj []) [(.offset r) (parse-long (str (.value r)))])
                m))
            (sorted-map)
            records)))

(defn- positions
  "The consumer's position of every partition it holds, or nil if one cannot
  be read in time (then nothing is rolled back)."
  [^KafkaConsumer consumer]
  (try (into {} (for [^TopicPartition tp (.assignment consumer)]
                  [tp (.position consumer tp (Duration/ofSeconds 5))]))
       (catch Exception _ nil)))

(defn- rollback!
  "Puts back the positions an op found, for every partition still held."
  [^KafkaConsumer consumer pos]
  (when (and consumer pos)
    (try (let [held (set (.assignment consumer))]
           (doseq [[tp o] pos :when (held tp)]
             (.seek consumer ^TopicPartition tp (long o))))
         (catch Exception e
           (warn "rollback failed" (str e))))))

(defn- next-offsets
  "{TopicPartition OffsetAndMetadata} after the reads of `polls`."
  [polls]
  (reduce (fn [m [k pairs]]
            (if-let [o (some->> pairs (map first) (remove nil?) seq (reduce max))]
              (assoc m (tp-of k) (OffsetAndMetadata. (inc (long o))))
              m))
          {}
          (mapcat identity polls)))

(defn- hwm
  "{k last offset} of the assigned partitions (endOffsets - 1), for the final
  polls of keys whose only writes have unknown offsets."
  [^KafkaConsumer consumer]
  (try (->> (.endOffsets consumer (.assignment consumer) (Duration/ofSeconds 5))
            (keep (fn [[^TopicPartition tp end]]
                    (when-let [k (k-of (.topic tp) (.partition tp))]
                      [k (dec (long end))])))
            (into {}))
       (catch Exception _ nil)))

(declare close-all! admin! consumer! producer! txn-op! plain-op!)

(defrecord Client [node process state]
  ; state: atom {:producer :consumer :admin :mode (:assign | :subscribe)
  ;              :rebalances [...]}
  client/Client
  (open! [this test node]
    (assoc this :node node, :state (atom {:rebalances []})))

  (setup! [this test]
    ; The first node's client creates the topics of the first keys, so the
    ; first ops do not all wait on one CreateTopics.
    (when (= node (first (:nodes test)))
      (let [admin (make-admin node)]
        (try (ensure-topics! admin test (range (* 2 partitions-per-topic)))
             (catch Exception e (warn "precreating topics:" (str e)))
             (finally (close-quietly! admin))))))

  (invoke! [this test op]
    (let [process (:process op)]
      (try
        (case (:f op)
          :crash (do (close-all! this) (assoc op :type :info))

          :debug-topic-partitions
          (let [admin  (admin! this)
                ts     (distinct (map topic-of (:value op)))
                descrs (-> (.describeTopics ^Admin admin ^java.util.Collection ts)
                           (.allTopicNames)
                           (.get 5 TimeUnit/SECONDS))]
            (assoc op :type :ok
                   :value (into (sorted-map)
                                (for [[t ^TopicDescription d] descrs]
                                  [t (mapv (fn [p] [(.partition p) (str (.leader p))])
                                           (.partitions d))]))))

          :assign
          (let [ks (vec (:value op))
                _  (ensure-topics! (admin! this) test ks)
                ^KafkaConsumer c (consumer! this test)
                tps (mapv tp-of ks)]
            (.assign c tps)
            (when (:seek-to-beginning? op)
              (.seekToBeginning c tps))
            (swap! state assoc :mode :assign)
            (assoc op :type :ok))

          :subscribe
          (let [ks (vec (:value op))
                _  (ensure-topics! (admin! this) test ks)
                ^KafkaConsumer c (consumer! this test)
                ts (vec (distinct (map topic-of ks)))
                st state]
            (.subscribe c ^java.util.Collection ts
                        (reify ConsumerRebalanceListener
                          (onPartitionsRevoked [_ tps]
                            (swap! st update :rebalances conj
                                   {:revoked (vec (keep #(k-of (.topic ^TopicPartition %)
                                                                (.partition ^TopicPartition %))
                                                        tps))}))
                          (onPartitionsAssigned [_ tps]
                            (swap! st update :rebalances conj
                                   {:keys (vec (keep #(k-of (.topic ^TopicPartition %)
                                                             (.partition ^TopicPartition %))
                                                     tps))}))))
            (swap! state assoc :mode :subscribe)
            (assoc op :type :ok))

          (:send :poll :txn)
          (if (:txn? test)
            (txn-op! this test op)
            (plain-op! this test op)))
        (catch clojure.lang.ExceptionInfo e
          (if (= ::no-topic (:type (ex-data e)))
            (assoc op :type :fail, :error [:no-topic (:topic (ex-data e))])
            (throw e)))
        ; assign, subscribe and the debug read write nothing: any other
        ; failure of theirs is definite. (Sends, polls and transactions
        ; classify their own failures.)
        (catch Exception e
          (if (#{:assign :subscribe :debug-topic-partitions} (:f op))
            (assoc op :type :fail, :error [(:f op) (error-name e) (str (root-cause e))])
            (throw e))))))

  (teardown! [this test])

  (close! [this test]
    (close-all! this))

  client/Reusable
  (reusable? [this test] false))

(defn close-all!
  [client]
  (let [{:keys [producer consumer admin]} @(:state client)]
    (swap! (:state client) dissoc :producer :consumer :admin)
    (close-quietly! producer)
    (close-quietly! consumer)
    (close-quietly! admin)))

(defn admin!
  ^Admin [client]
  (or (:admin @(:state client))
      (:admin (swap! (:state client) assoc :admin (make-admin (:node client))))))

(defn consumer!
  ^KafkaConsumer [client test]
  (or (:consumer @(:state client))
      (:consumer (swap! (:state client) assoc :consumer
                        (make-consumer test (:node client))))))

(defn producer!
  "The client's producer, made on first use; with --txn initTransactions has
  run on it (a failure there closes it and throws: nothing was sent)."
  ^KafkaProducer [client test process]
  (or (:producer @(:state client))
      (let [p (make-producer test (:node client) process)]
        (when (:txn? test)
          (try (.initTransactions p)
               (catch Throwable t
                 (close-quietly! p)
                 (throw (ex-info "initTransactions failed"
                                 {:type ::init-failed, :error (error-name t)
                                  :msg (str t)})))))
        (swap! (:state client) assoc :producer p)
        p)))

(defn- drop-producer!
  [client]
  (let [p (:producer @(:state client))]
    (swap! (:state client) dissoc :producer)
    (close-quietly! p)))

(defn- take-rebalances!
  [client]
  (let [[old] (swap-vals! (:state client) assoc :rebalances [])]
    (:rebalances old)))

(defn- complete
  "op completed :ok with the micro-op results, its rebalance log, and the
  high watermarks when the op asked for them."
  [client op done extra]
  (let [rl (take-rebalances! client)]
    (cond-> (merge (assoc op :type :ok, :value done) extra)
      (seq rl) (assoc :rebalance-log rl))))

(defn plain-op!
  "An op of single sends and polls, each on its own: a send is waited for, a
  poll (subscribed) commits its offsets with commitSync. A failure ends the
  op: a send's by its outcome table, a poll's as :fail with the positions
  rolled back. Sends already done in a failing op are written: such an op is
  :info."
  [client test op]
  (let [^KafkaConsumer c (consumer! client test)
        pos              (positions c)
        mode             (:mode @(:state client))]
    (loop [mops  (:value op)
           done  []
           sent? false]
      (if-let [[f k v] (first mops)]
        (case f
          :send
          (let [res (try
                      (ensure-topics! (admin! client) test [k])
                      (let [^KafkaProducer p (producer! client test (:process op))
                            ^Future fut (try (.send p (ProducerRecord. (topic-of k)
                                                                       (int (mod k partitions-per-topic))
                                                                       nil
                                                                       (str v)))
                                             (catch Exception e
                                               (throw (ex-info "send threw" {:type ::sync-send
                                                                             :error (error-name e)
                                                                             :msg (str e)}))))
                            ^RecordMetadata md (.get fut 15 TimeUnit/SECONDS)]
                        {:mop [:send k [(.offset md) v]]})
                      (catch clojure.lang.ExceptionInfo e
                        (if (#{::sync-send ::no-topic ::init-failed} (:type (ex-data e)))
                          {:type :fail, :error [(:type (ex-data e)) (:error (ex-data e) (:topic (ex-data e)))]}
                          (throw e)))
                      (catch java.util.concurrent.TimeoutException e
                        {:type :info, :error [:send-timeout]})
                      (catch ExecutionException e
                        (when (producer-fatal-errors (error-name e))
                          (drop-producer! client))
                        {:type (send-outcome e false), :error [:send (error-name e)]}))]
            (if (:mop res)
              (recur (next mops) (conj done (:mop res)) true)
              (do (rollback! c pos)
                  (assoc op :type (if sent? :info (:type res)) :error (:error res)))))

          :poll
          (let [res (try (let [reads (poll-once c (:poll-ms op 100))]
                           (when (and (= :subscribe mode) (seq reads))
                             (.commitSync c ^java.util.Map (next-offsets [reads])
                                          (Duration/ofSeconds 5)))
                           {:reads reads})
                         (catch Exception e
                           {:error [:poll (error-name e)]}))]
            (if (:reads res)
              (recur (next mops) (conj done [:poll (:reads res)]) sent?)
              (do (rollback! c pos)
                  (assoc op :type (if sent? :info :fail) :error (:error res))))))
        (complete client op done (when (:hwm? op) {:hwm (hwm c)}))))))

(defn txn-op!
  "An op as ONE producer transaction: begin, the micro-ops in order (sends
  return futures, polls read), the polls' next offsets with
  sendOffsetsToTransaction, commit. Any failure tries an abort: an abort the
  facade answered makes the op :fail (it is definite: nothing of the
  transaction is visible), anything else :info. A fatal producer error drops
  the producer; the next op makes a new one, which fences the old epoch."
  [client test op]
  (let [^KafkaConsumer c (consumer! client test)
        pos              (positions c)
        ks               (keep (fn [[f k]] (when (= :send f) k)) (:value op))]
    (try
      (ensure-topics! (admin! client) test ks)
      (let [^KafkaProducer p (producer! client test (:process op))]
        (.beginTransaction p)
        (let [[done futs polls]
              (reduce (fn [[done futs polls] [f k v :as mop]]
                        (case f
                          :send (let [fut (.send p (ProducerRecord. (topic-of k)
                                                                    (int (mod k partitions-per-topic))
                                                                    nil
                                                                    (str v)))]
                                  [(conj done mop) (assoc futs (count done) fut) polls])
                          :poll (let [reads (poll-once c (:poll-ms op 100))]
                                  [(conj done [:poll reads]) futs (conj polls reads)])))
                      [[] {} []]
                      (:value op))
              offsets (next-offsets polls)]
          (when (seq offsets)
            (.sendOffsetsToTransaction p ^java.util.Map offsets (.groupMetadata c)))
          (.commitTransaction p)
          (let [done (vec (map-indexed
                            (fn [i [f k v :as mop]]
                              (if-let [^Future fut (get futs i)]
                                (let [^RecordMetadata md (.get fut 1 TimeUnit/SECONDS)
                                      o (.offset md)]
                                  [:send k [(when (<= 0 o) o) v]])
                                mop))
                            done))]
            (complete client op done (when (:hwm? op) {:hwm (hwm c)})))))
      (catch clojure.lang.ExceptionInfo e
        (rollback! c pos)
        (if (#{::no-topic ::init-failed} (:type (ex-data e)))
          (assoc op :type :fail, :error [(:type (ex-data e)) (:error (ex-data e) (:topic (ex-data e)))])
          (throw e)))
      (catch Exception e
        (rollback! c pos)
        (let [why      (error-name e)
              fatal?   (producer-fatal-errors why)
              aborted? (when-not fatal?
                         (let [^KafkaProducer p (:producer @(:state client))]
                           (try (when p (.abortTransaction p) true)
                                (catch Exception _ false))))
              out      (txn-outcome why aborted?)]
          (when-not aborted? (drop-producer! client))
          (merge op out))))))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; Generators

(defrecord TrackCommitted [gen offsets]
  ; Every key an :ok or :info op wrote joins the final polls (as log.clj's
  ; TrackCommitted, also for :txn ops): a transactional send names no offset,
  ; so the Kafka tracker never sees its key unless a poll did.
  gen/Generator
  (op [this test context]
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        [op (TrackCommitted. gen' offsets)])))

  (update [this test context event]
    (when (and (#{:ok :info} (:type event)) (#{:send :txn} (:f event)))
      (let [ks (keep (fn [[f k _]] (when (= :send f) k)) (:value event))]
        (when (seq ks)
          (swap! offsets
                 (fn [m] (reduce (fn [m k] (update m k (fnil max -1) -1)) m ks))))))
    (when (and (= :ok (:type event)) (seq (:hwm event)))
      (swap! offsets #(merge-with max % (:hwm event))))
    (TrackCommitted. (gen/update gen test context event) offsets)))

(defrecord FinalPolls [target-offsets gen]
  ; kafka/FinalPolls, except that a key whose target is -1 (written only by
  ; sends of unknown offset) is satisfied once a final poll reports its high
  ; watermark: there may be nothing to read.
  gen/Generator
  (op [this test context]
    (when-not (empty? target-offsets)
      (when-let [[op gen'] (gen/op gen test context)]
        [op (assoc this :gen gen')])))

  (update [this test context {:keys [type f] :as event}]
    (if (and (= :ok type) (= :poll f))
      (let [read     (kafka/op->max-offsets event)
            hw       (:hwm event)
            ; A key is done when this poll read up to its target, or (a
            ; target of -1: offsets unknown) up to its high watermark.
            done?    (fn [k target]
                       (let [r (get read k -1)]
                         (or (<= 0 target r)
                             (and (contains? hw k)
                                  (<= target (get hw k))
                                  (<= (get hw k) r)))))
            offsets' (reduce-kv (fn [t k target]
                                  (if (done? k target) (dissoc t k) t))
                                target-offsets
                                target-offsets)]
        (when-not (= (count target-offsets) (count offsets'))
          (info "Process" (:process event) "now waiting for" offsets'))
        (FinalPolls. offsets' gen))
      this)))

(defn final-polls
  "Every thread, from its own node: a fresh client, the facade's view of the
  partitions, every tracked key assigned from its start, and polls (with the
  high watermarks) until it has read up to every target."
  [offsets]
  (delay
    (let [offsets @offsets]
      (info "Polling up to offsets" offsets)
      (->> [{:f :crash}
            {:f :debug-topic-partitions, :value (keys offsets)}
            {:f :assign, :value (keys offsets), :seek-to-beginning? true}
            (->> {:f :poll, :value [[:poll]], :poll-ms 1000, :hwm? true}
                 repeat
                 (gen/stagger 1/5))]
           (gen/time-limit 10000)
           repeat
           (->FinalPolls offsets)))))

;; ---------------------------------------------------------------------------
;; Checker

(defn allowed-error-types
  "What W14 forgives: with subscribe, the poll skips and nonmonotonic polls
  of a rebalance between ops. Nothing else: one raft entry per commit."
  [test]
  (cond-> #{}
    (:subscribe (:sub-via test)) (conj :poll-skip :nonmonotonic-poll)))

(def plot-limit 100000)

(defn verdict
  "The verdict on a Kafka analysis's errors: :valid? by W14's allowances,
  :kafka-model-valid? by Kafka's own (jepsen.tests.kafka/allowed-error-types)."
  [test errors]
  (let [bad       (->> (keys errors) (remove (allowed-error-types test)) sort)
        bad-kafka (->> (keys errors) (remove (kafka/allowed-error-types test)) sort)]
    {:valid?             (empty? bad)
     :bad-error-types    (vec bad)
     :kafka-model-valid? (empty? bad-kafka)
     :error-types        (vec (sort (keys errors)))}))

(defn facade-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [{:keys [errors worst-realtime-lag] :as analysis}
            (kafka/analysis history {:directory (store/path! test)
                                     :ww-deps   (:ww-deps test)})
            plots?   (<= (count history) plot-limit)
            ; Plots are pictures: a missing gnuplot must not decide a test.
            _        (when plots?
                       (try (kafka/render-order-viz! test analysis)
                            (kafka/plot-unseen! test (:unseen analysis) opts)
                            (kafka/plot-realtime-lags! test (:realtime-lag analysis) opts)
                            (catch Throwable t (warn "kafka plots failed:" (str t)))))]
        (->> errors
             (map (partial kafka/condense-error test))
             (into (sorted-map))
             (merge (verdict test errors)
                    {:plots?             plots?
                     :worst-realtime-lag (some-> worst-realtime-lag
                                                 (update :time util/nanos->secs)
                                                 (update :lag util/nanos->secs))
                     :info-causes        (->> history
                                              h/infos
                                              (h/filter (h/has-f? #{:send :poll :txn}))
                                              (h/map :error)
                                              frequencies)
                     :fail-causes        (->> history
                                              h/fails
                                              (h/filter (h/has-f? #{:send :poll :txn}))
                                              (h/map :error)
                                              frequencies)}))))))

;; ---------------------------------------------------------------------------
;; Workload

(defn test-opts
  "What the test map needs for this workload (core.clj merges it): the Kafka
  workload's :txn? and :sub-via, and the facade turned on in the nodes."
  [opts]
  {:txn?           (boolean (:txn-sends? opts))
   :sub-via        (set (or (seq (:sub-via-kafka opts)) [:assign]))
   :kafka-facade?  true})

(defn workload
  "jepsen.tests.kafka's generator and final polls with the facade client.
  Options as for jepsen.tests.kafka/workload; --txn makes every op a producer
  transaction, --sub-via-kafka assign,subscribe picks how consumers take
  partitions."
  [opts]
  (let [topts       (test-opts opts)
        opts        (merge opts topts {:crash-clients? true
                                       :crash-client-interval 30
                                       :max-txn-length (if (:txn? topts) 4 1)})
        la          (append/test opts)
        ops         (kafka/txn-generator (:generator la))
        max-offsets (atom (sorted-map))]
    {:client          (client)
     :checker         (facade-checker)
     :test-opts       topts
     :generator       (gen/any
                        (kafka/crash-client-gen opts)
                        (->> ops
                             kafka/tag-rw
                             (kafka/interleave-subscribes opts)
                             kafka/poll-unseen))
     :final-generator (gen/each-thread (final-polls max-offsets))
     :wrap-generator  (fn [gen]
                        (->> gen
                             (kafka/track-key-offsets max-offsets)
                             (#(->TrackCommitted % max-offsets))))}))
