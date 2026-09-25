(ns jepsen.queen.workload.log
  "W1: Queen as a log, checked with jepsen.tests.kafka.

  An abstract key k is one Queen partition: queue (queue-name k), partition
  \"k<k>\". The operations are the Kafka workload's, over Queen's own HTTP API:

    :send [:send k v]  one-item POST /api/v1/push, transactionId \"k<k>-v<v>\";
                       completes as [:send k [offset v]]
    :poll [:poll]      one POST /api/v1/fetch of every assigned key from the
                       client's own positions (no lease, no cursor);
                       completes as [:poll {k [[offset v] ...]}]
    :assign            sets the assigned keys; kept keys keep their positions,
                       new keys start at 0 (offsets are 0-based, gap-free)

  With --txn every :send op is instead ONE POST /api/v1/transaction of 1-4
  pushes over several partitions and queues (one raft entry, all or nothing);
  its answer names no offsets, so its sends complete as [:send k [nil v]].

  The client is bound to one node and never retries; a poll's positions are
  committed only when the whole op succeeds. The checker is the Kafka one with
  nothing allowed (every push or bundle is one raft entry, so G0 and G1c are
  real anomalies here), plus what the Kafka checker does not check: every
  fetch returns records contiguous from the requested offset, each under the
  key and value its transactionId names, and no bundle is partly visible."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util]]
            [jepsen.tests.kafka :as kafka]
            [jepsen.tests.cycle.append :as append]
            [jepsen.queen.http :as qh]))

;; ---------------------------------------------------------------------------
;; Key mapping

(defn queue-name
  [test k]
  (let [qs (:queue-names test)]
    (nth qs (mod k (count qs)))))

(defn partition-name
  [k]
  (str "k" k))

(defn txn-id
  [k v]
  (str "k" k "-v" v))

(defn parse-txn-id
  "[k v] from \"k<k>-v<v>\", or nil."
  [s]
  (when (string? s)
    (when-let [[_ k v] (re-matches #"k(\d+)-v(\d+)" s)]
      [(parse-long k) (parse-long v)])))

;; ---------------------------------------------------------------------------
;; Outcomes

(defn push-failure
  "The completion type for a push that did not answer 201: :fail when it
  definitely did not take effect, else :info."
  [test status body]
  (let [code (when (map? body) (:code body))]
    (cond
      ; Refused before planning: a bad request or a name too long.
      (#{400 404 413} status) :fail
      ; Admission budget, disk full, no leader: definite only with offload
      ; off, where the node that answers is the one that would plan (or its
      ; forwarding layer, before the request goes anywhere). With offload on
      ; a follower retries a lost forward with the same request id, so the
      ; refusal can answer a second attempt whose first one committed.
      (and (not (:offload test))
           (or (#{429 507} status)
               (and (= 503 status) (= "no_leader" code))
               ; The follower could not even connect to the leader it
               ; forwards to (handlers/raft.rs forward): nothing was sent.
               (and (= 503 status) (= "retry" code)
                    (re-find #"unreachable: client error \(Connect\)"
                             (str (:error body)))))) :fail
      :else :info)))

(defn- send!
  [client test k v]
  (let [body {:items [{:queue         (queue-name test k)
                       :partition     (partition-name k)
                       :payload       v
                       :transactionId (txn-id k v)}]}
        r    (qh/request! (:http client) :post
                          (str (:base client) "/api/v1/push")
                          body (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (case (:status item)
          ("queued" "duplicate")
          {:mop [:send k [(:offset item) v]], :status (:status item)}
          ; A per-item "error" is a refusal of the item.
          {:fail [:item-error item]}))
      {:error [(:status r) (:body r)]
       :type  (push-failure test (:status r) (:body r))})))

(defn- send-txn!
  "All of an op's sends as ONE POST /api/v1/transaction (one raft entry, all
  or nothing). Its answer names no offsets, so each completes as
  [:send k [nil v]]."
  [client test mops]
  (let [body {:operations
              (mapv (fn [[_ k v]]
                      {:type  "push"
                       :items [{:queue         (queue-name test k)
                                :partition     (partition-name k)
                                :payload       v
                                :transactionId (txn-id k v)}]})
                    mops)}
        r    (qh/request! (:http client) :post
                          (str (:base client) "/api/v1/transaction")
                          body (:client-timeout-ms test))
        b    (:body r)]
    (cond
      (and (= 200 (:status r)) (true? (:success b)))
      {:mops (mapv (fn [[f k v]] [f k [nil v]]) mops)}

      (= 200 (:status r))
      ; Rolled back. A duplicate refusal can only answer a second attempt of
      ; a bundle whose first attempt committed (no id is ever pushed twice),
      ; so it is not a definite failure.
      {:error [:rolled-back (select-keys b [:reason :error])]
       :type  (if (re-find #"(?i)dup" (str (:reason b) " " (:error b)))
                :info
                :fail)}

      :else
      {:error [(:status r) b]
       :type  (push-failure test (:status r) b)})))

(defn- check-records
  "Anomalies in one key's fetched records: not starting at the requested
  offset, not contiguous, or a record whose transactionId names another key
  or value."
  [k requested records]
  (let [offsets (mapv :offset records)
        expect  (vec (range requested (+ requested (count records))))]
    (cond-> []
      (not= offsets expect)
      (conj {:type :offset-gap, :key k, :requested requested,
             :offsets offsets})

      true
      (into (keep (fn [r]
                    (let [[tk tv] (parse-txn-id (:transactionId r))]
                      (when-not (and (= tk k) (= tv (:payload r)))
                        {:type :record-mismatch, :key k,
                         :offset (:offset r), :payload (:payload r),
                         :transactionId (:transactionId r)})))
                  records)))))

(def fetch-chunk
  "Queen caps a fetch at 1024 entries."
  512)

(defn- poll!
  "Fetches every assigned key from `positions`. Returns {:reads {k [[o v]]},
  :positions' {...}, :anomalies [...], :notes [...], :hwm {...}}, or throws /
  returns {:error ...} on failure."
  [client test positions poll-ms]
  (loop [chunks    (partition-all fetch-chunk (seq positions))
         reads     (sorted-map)
         pos'      positions
         anomalies []
         notes     []
         hwm       {}]
    (if-let [chunk (first chunks)]
      (let [by-qp (into {} (map (fn [[k _]]
                                  [[(queue-name test k) (partition-name k)] k])
                                chunk))
            body  {:entries   (mapv (fn [[k o]]
                                      {:queue     (queue-name test k)
                                       :partition (partition-name k)
                                       :offset    o})
                                    chunk)
                   :maxWaitMs poll-ms}
            r     (qh/request! (:http client) :post
                               (str (:base client) "/api/v1/fetch")
                               body (+ poll-ms (:client-timeout-ms test)))]
        (if-not (= 200 (:status r))
          {:error [(:status r) (:body r)]}
          (let [hwm (into hwm
                          (keep (fn [e]
                                  (let [k (by-qp [(:queue e)
                                                  (str (:partition e))])
                                        h (:highWatermark e)]
                                    (cond
                                      (nil? k) nil
                                      ; No such partition here: nothing
                                      ; committed to it that this node applied.
                                      (= "UNKNOWN_TOPIC_OR_PARTITION" (:error e))
                                      [k -1]
                                      (:error e) nil
                                      (integer? h) [k (dec h)])))
                                (:entries (:body r))))
                [reads pos' anomalies notes]
                (reduce
                  (fn [[reads pos' anomalies notes] e]
                    (let [k (by-qp [(:queue e) (str (:partition e))])]
                      (cond
                        (nil? k)
                        [reads pos' anomalies
                         (conj notes [:unknown-entry (dissoc e :records)])]

                        (:error e)
                        [reads pos' anomalies
                         (if (= "UNKNOWN_TOPIC_OR_PARTITION" (:error e))
                           notes
                           (conj notes (assoc (dissoc e :records)
                                              :key k
                                              :requested (get positions k))))]

                        (empty? (:records e))
                        ; Nothing at an offset below the high watermark the
                        ; same answer reports: the node says the records exist
                        ; and serves none of them (a hole it cannot see).
                        (let [req (get positions k)
                              h   (:highWatermark e)]
                          [reads pos'
                           (cond-> anomalies
                             (and (integer? h) (integer? req) (< req h))
                             (conj {:type :empty-below-hwm, :key k,
                                    :requested req, :highWatermark h}))
                           notes])

                        :else
                        (let [recs (:records e)
                              req  (get positions k)]
                          [(assoc reads k (mapv (juxt :offset :payload) recs))
                           (assoc pos' k (inc (:offset (peek recs))))
                           (into anomalies (check-records k req recs))
                           notes]))))
                  [reads pos' anomalies notes]
                  (:entries (:body r)))]
            (recur (next chunks) reads pos' anomalies notes hwm))))
      {:reads reads, :positions' pos', :anomalies anomalies, :notes notes,
       :hwm hwm})))

(defn- error-completion
  "Turns a qh/request! exception into a completion for op."
  [test op e]
  (let [{:keys [type msg]} (ex-data e)
        writes? (= :send (:f op))]
    (assoc op
           :type  (cond (not writes?)      :fail
                        (= type ::qh/refused) :fail
                        :else              :info)
           :error [(keyword (name (or type :unknown))) msg])))

(declare send-or-poll)

(defrecord Client [node base http positions]
  client/Client
  (open! [this test node]
    (assoc this
           :node      node
           :base      (qh/base-url node)
           :http      (qh/client)
           :positions (atom (sorted-map))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :crash (assoc op :type :info)

        :debug-topic-partitions
        (assoc op :type :ok, :value {:node   node
                                     :health (qh/health http node 2000)})

        :assign
        (let [ks (set (:value op))]
          (swap! positions
                 (fn [ps]
                   (into (sorted-map)
                         (map (fn [k]
                                [k (if (:seek-to-beginning? op)
                                     0
                                     (get ps k 0))]))
                         ks)))
          (assoc op :type :ok))

        :subscribe
        (assoc op :type :fail, :error :subscribe-unsupported)

        :send
        (if (:txn-sends? test)
          (let [res (send-txn! this test (:value op))]
            (if (:mops res)
              (assoc op :type :ok, :value (:mops res))
              (assoc op :type (:type res), :error (:error res))))
          (send-or-poll this test op))

        :poll
        (send-or-poll this test op))
      (catch clojure.lang.ExceptionInfo e
        (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
          (error-completion test op e)
          (throw e)))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] false))

(defn- send-or-poll
  "An op's micro-ops one by one: each send is its own push, each poll a fetch
  from the client's positions. The positions a poll advances are committed
  only when the WHOLE op completes :ok: a failed op records no reads, so a
  later poll must read its records again (as the Redpanda client's
  with-consumer-rollback does), or the checker sees a poll-skip."
  [this test op]
  (let [positions (:positions this)]
    (loop [mops  (:value op)
           done  []
           extra {}
           pos   @positions]
      (if-let [[f k v] (first mops)]
        (case f
          :send
          (let [res (send! this test k v)]
            (cond
              (:mop res)
              (recur (next mops) (conj done (:mop res))
                     (cond-> extra
                       (= "duplicate" (:status res))
                       (update :duplicate-acks (fnil conj []) (:mop res)))
                     pos)

              (:fail res)
              (assoc op :type :fail, :error (:fail res))

              :else
              (assoc op :type (:type res), :error (:error res))))

          :poll
          (let [res (poll! this test pos (:poll-ms op 100))]
            (if (:error res)
              (assoc op :type :fail, :error (:error res))
              (recur (next mops)
                     (conj done [:poll (:reads res)])
                     (cond-> extra
                       (seq (:anomalies res))
                       (update :anomalies (fnil into []) (:anomalies res))
                       (seq (:notes res))
                       (update :notes (fnil into []) (:notes res))
                       (seq (:hwm res))
                       (update :hwm #(merge-with max % (:hwm res))))
                     (:positions' res)))))
        (do (reset! positions pos)
            (merge (assoc op :type :ok, :value done) extra))))))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; Checkers

(def allowed-error-types
  "Nothing: a push is one raft entry and a fetch reads committed state, so the
  Kafka transaction allowances (G0, G1c, int-send-skip) do not apply."
  #{})

(def plot-limit
  "Above this many ops the Kafka checker's plots are skipped: its unseen plot
  alone ran a 12 GB heap out of memory on a 225k-op history."
  100000)

(defn strict-checker
  "jepsen.tests.kafka/checker's analysis, with :valid? computed over
  allowed-error-types, and its plots only for histories up to plot-limit."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [{:keys [errors worst-realtime-lag] :as analysis}
            (kafka/analysis history {:directory (store/path! test)
                                     :ww-deps   (:ww-deps test)})
            plots? (<= (count history) plot-limit)
            _      (when plots?
                     (kafka/render-order-viz! test analysis)
                     (kafka/plot-unseen! test (:unseen analysis) opts)
                     (kafka/plot-realtime-lags! test (:realtime-lag analysis)
                                                opts))
            bad    (->> (keys errors) (remove allowed-error-types) sort)]
        (->> errors
             (map (partial kafka/condense-error test))
             (into (sorted-map))
             (merge {:valid?             (empty? bad)
                     :bad-error-types    bad
                     :error-types        (sort (keys errors))
                     :plots?             plots?
                     :worst-realtime-lag (some-> worst-realtime-lag
                                                 (update :time util/nanos->secs)
                                                 (update :lag util/nanos->secs))
                     :info-txn-causes    (->> history
                                              h/infos
                                              (h/filter (h/has-f? #{:send :poll}))
                                              (h/map :error)
                                              distinct)}))))))

(defn fetch-checker
  "Every :ok poll's fetch anomalies (gaps, records under the wrong id), and the
  pushes answered 'duplicate' although no id is ever pushed twice."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [oks        (->> history h/oks (h/filter (h/has-f? #{:poll :send})))
            anomalies  (->> oks
                            (mapcat (fn [op]
                                      (map #(assoc % :op-index (:index op)
                                                     :process (:process op))
                                           (:anomalies op))))
                            vec)
            dups       (->> oks
                            (filter :duplicate-acks)
                            (map #(select-keys % [:index :process :value]))
                            vec)
            notes      (->> oks (mapcat :notes))
            ; Transactions (txn mode): every value of a committed or unknown
            ; bundle is read, or none is. The Kafka checker only checks the
            ; :ok ones (as unseen), one value at a time.
            read-set   (->> oks
                            (h/filter (h/has-f? #{:poll}))
                            (mapcat :value)
                            (mapcat (fn [[_ reads]]
                                      (for [[k pairs] reads, [_ v] pairs]
                                        [k v])))
                            set)
            torn       (->> history
                            (h/filter (fn [op]
                                        (and (= :send (:f op))
                                             (#{:ok :info} (:type op))
                                             (< 1 (count (:value op))))))
                            (keep (fn [op]
                                    (let [kvs  (map (fn [[_ k v]]
                                                      [k (if (vector? v)
                                                           (second v)
                                                           v)])
                                                    (:value op))
                                          seen (filter read-set kvs)]
                                      (when (< 0 (count seen) (count kvs))
                                        {:op-index (:index op)
                                         :type     (:type op)
                                         :values   (vec kvs)
                                         :seen     (vec seen)}))))
                            vec)]
        ; A 'duplicate' answer to a first push is reported, not failed: the
        ; message is stored once, at the offset the answer names.
        {:valid?          (and (empty? anomalies) (empty? torn))
         :torn-count      (count torn)
         :torn            (take 16 torn)
         :anomaly-count   (count anomalies)
         :anomaly-types   (frequencies (map :type anomalies))
         :anomalies       (take 32 anomalies)
         :duplicate-acks  (take 32 dups)
         :duplicate-count (count dups)
         :fetch-note-types (frequencies (map #(if (map? %) (:error %) (first %))
                                             notes))
         :fetch-notes     (take 16 notes)}))))

(defrecord TrackCommitted [gen offsets]
  ; Feeds the final polls' targets (the atom kafka/track-key-offsets fills)
  ; with what the Kafka tracker cannot see: the high watermark each fetch
  ; reported (committed, applied state: every node must serve it in the end),
  ; and every key an :ok or :info send wrote (a /transaction answer names no
  ; offsets, and an :info write is invisible to the Kafka tracker).
  gen/Generator
  (op [this test context]
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        [op (TrackCommitted. gen' offsets)])))

  (update [this test context event]
    (case [(:type event) (:f event)]
      [:ok :poll]
      (when-let [h (seq (:hwm event))]
        (swap! offsets #(merge-with max % (into {} h))))

      ; Every key an :ok or :info send wrote joins the final polls, so its
      ; values are read at the end even if no poll ever assigned it: an :info
      ; push may have committed, and a bundle's :info half-visibility (torn)
      ; is only decidable if all its keys are read.
      ([:ok :send] [:info :send])
      (let [ks (keep (fn [[f k _]] (when (= :send f) k)) (:value event))]
        (when (seq ks)
          (swap! offsets
                 (fn [m] (reduce (fn [m k] (update m k (fnil max -1) -1))
                                 m ks)))))
      nil)
    (TrackCommitted. (gen/update gen test context event) offsets)))

(defrecord FinalPolls [target-offsets gen]
  ; kafka/FinalPolls, except that a key whose target is -1 (only ever written
  ; by sends of unknown outcome, or by a /transaction) is satisfied once a poll
  ; reports its high watermark: there may be nothing to read, and waiting for a
  ; record would hold every final poll until --final-time-limit.
  gen/Generator
  (op [this test context]
    (when-not (empty? target-offsets)
      (when-let [[op gen'] (gen/op gen test context)]
        [op (assoc this :gen gen')])))

  (update [this test context {:keys [type f] :as event}]
    (if (and (= :ok type) (= :poll f))
      (let [read     (kafka/op->max-offsets event)
            hwm      (:hwm event)
            offsets' (reduce-kv (fn [t k target]
                                  (if (or (<= target (get read k -2))
                                          (and (= -1 target) (contains? hwm k)))
                                    (dissoc t k)
                                    t))
                                target-offsets
                                target-offsets)]
        (when-not (= (count target-offsets) (count offsets'))
          (info "Process" (:process event) "now waiting for" offsets'))
        (FinalPolls. offsets' gen))
      this)))

(defn final-polls
  "kafka/final-polls with the FinalPolls above: every thread, from its own
  node, crashes its client, assigns every key from offset 0 and polls until it
  has read every tracked offset."
  [offsets]
  (delay
    (let [offsets @offsets]
      (info "Polling up to offsets" offsets)
      (->> [{:f :crash}
            {:f :debug-topic-partitions, :value (keys offsets)}
            {:f :assign, :value (keys offsets), :seek-to-beginning? true}
            (->> {:f :poll, :value [[:poll]], :poll-ms 1000}
                 repeat
                 (gen/stagger 1/5))]
           (gen/time-limit 10000)
           repeat
           (->FinalPolls offsets)))))

(defn sends-only
  "A /transaction bundles pushes (and acks, KV), never a fetch: strip the polls
  out of mixed transactions, so every :send op is one bundle of 1-4 pushes."
  [gen]
  (gen/map (fn [op]
             (let [mops  (:value op)
                   sends (filterv (comp #{:send} first) mops)]
               (if (and (seq sends) (< (count sends) (count mops)))
                 (assoc op :value sends)
                 op)))
           gen))

(defn workload
  "jepsen.tests.kafka/workload's generator, final polls and checker, with the
  highest-offset tracking moved OUT of the main phase: :wrap-generator must
  wrap the whole phased generator. Inside the main phase (as kafka/workload
  has it) a push that completes after the phase's time limit, e.g. one held
  by a lost quorum until the nodes restart, never reaches the tracker, so the
  final polls never target its key and the checker reports it :unseen.

  Options as for jepsen.tests.kafka/workload (:key-count,
  :max-writes-per-key, :sub-via #{:assign}, ...)."
  [opts]
  (let [txn?        (:txn-sends? opts)
        opts        (assoc opts :txn? false, :max-txn-length (if txn? 4 1))
        la          (append/test opts)
        ops         (kafka/txn-generator (:generator la))
        ops         (if txn? (sends-only ops) ops)
        max-offsets (atom (sorted-map))]
    {:client          (client)
     :checker         (checker/compose {:kafka (strict-checker)
                                        :fetch (fetch-checker)})
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
