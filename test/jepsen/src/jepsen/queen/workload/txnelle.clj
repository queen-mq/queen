(ns jepsen.queen.workload.txnelle
  "W32 --workload txnelle: Elle over POST /api/v1/transaction, the call an
  application's step makes. W4 runs Elle on KV batches (POST /api/v1/kv); no
  other workload runs a cycle check on transactions.

  Each op is ONE transaction, number t:
    kv       Elle's rw-register transaction: a get of each key it reads,
             then a put of each key it writes (forever: true)
    marker   a push of {t} to the second queue, partition m<t mod 8>,
             transactionId \"m<t>\": in the final log if and only if t
             committed
    appends  0 to 2 pushes of a unique value v to partitions l0..l7 of the
             first queue (transactionId \"a<v>\"): Elle's list-append, a
             partition being a list
    loser    one time in ten, a put of \"exists\" with expect 0, required:
             the key exists from setup, so the transaction MUST roll back
             (kv_precondition) with everything above

  Every transaction pushes, so every one is a log entry; its keys and
  partitions hash to different planner homes, so with QUEEN_PLANNER_THREADS
  above 1 most transactions are planned by shares and voted on, and a loser
  takes back the whole second round of its run (batcher_scheduler.rs, S8).

  A transaction's reads come first, each key once, then the last write of
  each key. A read of a key the transaction also writes stays: it comes
  before the write, and the planner answers it at plan time, before the
  write (planner/kv.rs, KvOpOutcome::Got). Every other read is rendered by
  apply at the transaction's own position in the log (rsm/kv_reads; a
  transaction that reads logs a no-op last for it). Before that fix
  (2026-11-15) the answering node read its state once the entry had
  applied: this workload's first run found 141 G1c cycles with no faults.

  At the end: Elle's final read of every key, as transactions, then every
  client reads every list and marker partition by offset from its own node.

  The checker runs Elle twice, at strict serializability: rw-register on the
  KV part (keys linearizable, writes follow reads inside a transaction) and
  list-append on the appends with the final reads. It rejects, besides
  Elle's anomalies: an :ok transaction whose marker is not in the log; a
  :fail one whose marker is; a transaction partly visible (its marker
  without all its appends, or an append without its marker); a loser that
  committed; an id twice in the log; two nodes that read different logs."
  (:require [clojure.tools.logging :refer [info warn]]
            [elle [list-append :as la]
                  [rw-register :as rr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [store :as store]]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]
            ; Loads elle.clj's fix for bifurcan's union, which rw-register needs.
            [jepsen.queen.workload.elle]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.routing :as routing]))

(def lists 8)
(def markers 8)
(def loser-share 0.1)

(defn list-partition [i] (str "l" i))
(defn marker-partition [t] (str "m" (mod t markers)))
(defn marker-id [t] (str "m" t))
(defn append-id [v] (str "a" v))

(defn- key-str
  [k]
  (str "e" k))

(defn normalize
  "Reads first, each key once; then the last write of each key. A read of a
  key the transaction writes stays, before the write."
  [txn]
  (let [once   (fn [mops]
                 (->> mops
                      (reduce (fn [[seen out] [_ k _ :as mop]]
                                (if (seen k) [seen out] [(conj seen k) (conj out mop)]))
                              [#{} []])
                      second))
        reads  (once (filter (comp #{:r} first) txn))
        writes (->> txn (filter (comp #{:w} first)) reverse once reverse vec)]
    (into (vec reads) writes)))

(defn with-parts
  "The op as it is sent: its transaction normalised, its number t, and on the
  main phase its appends and, one time in ten, a loser. `counter` numbers
  both transactions and appended values, so every value is unique."
  [counter main? op]
  (let [t       (swap! counter inc)
        appends (if main?
                  (vec (for [_ (range (rand/long 3))]
                         [:append (list-partition (rand/long lists)) (swap! counter inc)]))
                  [])]
    (assoc op
           :value   (normalize (:value op))
           :t       t
           :appends appends
           :loser?  (boolean (and main? (< (rand/double) loser-share))))))

(defn body
  "The POST /api/v1/transaction body for op."
  [test {:keys [t value appends loser?]}]
  (let [[lq mq] (take 2 (:queue-names test))
        pushes  (cond-> [{:type  "push"
                          :items [{:queue mq, :partition (marker-partition t)
                                   :payload {:t t}, :transactionId (marker-id t)}]}]
                  (seq appends)
                  (conj {:type  "push"
                         :items (mapv (fn [[_ p v]]
                                        {:queue lq, :partition p, :payload {:v v}
                                         :transactionId (append-id v)})
                                      appends)}))
        kvops   (cond-> (mapv (fn [[f k v]]
                                (case f
                                  :r {:op "get", :ns kv/ns-name*, :key (key-str k)}
                                  :w {:op "put", :ns kv/ns-name*, :key (key-str k)
                                      :value v, :forever true}))
                              value)
                  loser?
                  (conj {:op "put", :ns kv/ns-name*, :key "exists", :value t
                         :expect 0, :required true, :forever true}))]
    {:operations pushes, :kv kvops}))

(defn read-values
  "The transaction with each read's value from a committed answer's results,
  or nil when a read's result is missing: a read is never made up."
  [txn results]
  (let [by-op (into {} (for [r results :when (= "kv" (:type r))] [(:opIndex r) r]))
        vals  (map-indexed (fn [i [f k _ :as mop]]
                             (if (= :r f)
                               (let [r (get by-op i)]
                                 (when (and (map? r) (contains? r :found))
                                   [:r k (when (true? (:found r)) (:value r))]))
                               mop))
                           txn)]
    (when (every? some? vals)
      (vec vals))))

(defn- request
  [client method url body timeout]
  (try (qh/request! (:http client) method url body timeout)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- txn!
  [client test op]
  (let [r (request client :post (str (:base client) "/api/v1/transaction")
                   (body test op) (:client-timeout-ms test))]
    (cond
      (:exception r)
      (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info)
             :error [:txn (:type (:exception r))])

      (and (= 200 (:status r)) (true? (:success (:body r))))
      (if-let [v (read-values (:value op) (:results (:body r)))]
        (assoc op :type :ok, :value v)
        ; It committed, but an answer without a read's result says nothing
        ; of what that read saw.
        (assoc op :type :info, :error [:read-result-missing (:results (:body r))]))

      (= 200 (:status r))
      ; Rolled back. A "duplicate" can answer a second attempt of a
      ; transaction whose first attempt committed (a forward retried with the
      ; same request id): unknown.
      (let [reason (:reason (:body r))]
        (assoc op :type (if (= "duplicate" reason) :info :fail)
               :error [:rolled-back reason]))

      :else
      (assoc op :type (log/push-failure test (:status r) (:body r))
             :error [:txn (:status r) (:body r)]))))

(defn- final!
  [client test op]
  (let [[lq mq] (take 2 (:queue-names test))]
    (routing/read-queues! client test op
                          [[lq (mapv list-partition (range lists))]
                           [mq (mapv #(str "m" %) (range markers))]])))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      (let [r (kv/put! test this "exists" 0)]
        (info "the key every loser expects absent:" r))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :txn   (txn! this test op)
        :final (final! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type (if (or (= ::qh/refused type) (= :final (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Checker

(def kv-opts
  "rw-register: keys are linearizable (W3), and a transaction's write of a key
  follows its read of it, which the planner answers before the write."
  {:consistency-models [:strict-serializable]
   :linearizable-keys? true
   :wfr-keys?          true})

(def list-opts
  {:consistency-models [:strict-serializable]})

(defn kv-history
  "The transactions, their KV part as Elle's rw-register sees it."
  [hist]
  (h/filter #(= :txn (:f %)) hist))

(defn list-history
  "The transactions with appends, as Elle's list-append sees them, and the
  final reads of every list (queue lq). A final read that failed reads
  nothing."
  [hist lq]
  (->> hist
       (h/filter #(or (and (= :txn (:f %)) (seq (:appends %)))
                      (= :final (:f %))))
       (h/map (fn [op]
                (if (= :txn (:f op))
                  (assoc op :value (:appends op))
                  (assoc op :value
                         (mapv (fn [i]
                                 (let [p (list-partition i)]
                                   [:r p (when (= :ok (:type op))
                                           (mapv :v (get-in op [:value lq p])))]))
                               (range lists))))))))

(defn analyze
  "The verdict on a history. `dirs` may name Elle's output directories
  (:kv-dir, :list-dir); without them Elle writes and plots nothing."
  ([test history]
   (analyze test history {}))
  ([test history {:keys [kv-dir list-dir]}]
   (let [hist      (h/client-ops history)
         lq        (first (:queue-names test))
         finals   (->> hist (h/filter #(and (= :final (:f %)) (= :ok (:type %)))) vec)
         read      (last finals)
         differ    (->> finals
                        (remove #(= (:value %) (:value read)))
                        (mapv #(select-keys % [:node :index])))
         in-log    (->> (vals (:value read)) (mapcat vals) (mapcat identity) (map :txn) frequencies)
         seen?     (fn [id] (pos? (get in-log id 0)))
         twice     (->> in-log (filter #(< 1 (val %))) (map key) sort vec)
         txns      (->> hist (h/filter #(and (= :txn (:f %)) (not= :invoke (:type %)))) vec)
         commit?   (fn [op] (seen? (marker-id (:t op))))
         lost      (->> txns (filter #(and (= :ok (:type %)) (not (commit? %)))) (mapv :t))
         phantom   (->> txns (filter #(and (= :fail (:type %)) (commit? %))) (mapv :t))
         torn      (->> txns
                        (keep (fn [op]
                                (let [c   (commit? op)
                                      ids (map (comp append-id #(nth % 2)) (:appends op))
                                      bad (if c (remove seen? ids) (filter seen? ids))]
                                  (when (seq bad)
                                    {:t (:t op), :marker-visible c, :appends-wrong (vec bad)}))))
                        vec)
         gate      (->> txns
                        (filter #(and (:loser? %) (or (= :ok (:type %)) (commit? %))))
                        (mapv #(select-keys % [:t :type])))
         own-valid (boolean (and read (empty? differ) (empty? lost) (empty? phantom)
                                 (empty? torn) (empty? gate) (empty? twice)))
         kv-res    (rr/check (cond-> kv-opts kv-dir (assoc :directory kv-dir))
                             (kv-history hist))
         list-res  (la/check (cond-> list-opts list-dir (assoc :directory list-dir))
                             (list-history hist lq))]
     {:valid?         (checker/merge-valid [own-valid (:valid? kv-res) (:valid? list-res)])
      :kv             kv-res
      :lists          list-res
      :final-reads    (count finals)
      :nodes-differ   differ
      :committed      (count (filter commit? txns))
      :outcomes       (frequencies (map (juxt :type :loser?) txns))
      :rollback-reasons (->> txns (filter #(= :fail (:type %)))
                             (map #(second (:error %))) frequencies)
      :lost           (take 16 lost)
      :phantom        (take 16 phantom)
      :torn           (take 16 torn)
      :ids-twice      (take 16 twice)
      :loser-committed (take 16 gate)})))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (analyze test history
               {:kv-dir   (store/path! test (:subdirectory opts) "elle-kv")
                :list-dir (store/path! test (:subdirectory opts) "elle-lists")}))))

(defn workload
  "Elle's rw-register generator and final reads (with its key tracker), each
  op given its marker, appends and loser by `with-parts`."
  [opts]
  (let [w       (wr/test {:key-count          (:key-count opts 8)
                          :min-txn-length     1
                          :max-txn-length     4
                          :max-writes-per-key (:max-writes-per-key opts 256)})
        counter (atom 0)]
    ; gen/map picks (f op) or (f op test ctx) by f's arity: a one-argument fn,
    ; not a partial (variadic, so called with three).
    {:client          (->Client nil nil nil)
     :checker         (checker)
     :db-env          kv/db-env
     :generator       (gen/map (fn [op] (with-parts counter true op)) (:generator w))
     :final-generator (gen/phases
                        (gen/map (fn [op] (with-parts counter false op)) (:final-generator w))
                        (gen/each-thread (gen/once {:f :final, :value nil})))
     :wrap-generator  (:wrap-generator w)}))
