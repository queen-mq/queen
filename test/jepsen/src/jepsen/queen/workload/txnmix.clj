(ns jepsen.queen.workload.txnmix
  "W18 --workload txnmix: transactions across partitions, queues and KV keys,
  with rollbacks on purpose: the path the footprint scheduler takes when a
  transaction's keys live on several planner threads (each plans its share,
  they vote, one no and the cycle thread plans it again). Run it at
  QUEEN_PLANNER_THREADS 1 and 8.

  Each op is ONE POST /api/v1/transaction, number t:
    marker   a push of {t} to jepsen-0 / partition m<t mod 8>, transactionId
             \"m<t>\": in the final log if and only if t committed
    data     0 to 3 pushes of {t, i} to random partitions d0..d7 of
             jepsen-0 and jepsen-1, transactionIds \"d<t>-<i>\"
    counters incr of 1 or 2 KV keys c:0..c:4
    then, at random, at most one of:
      dup    a push that reuses the marker id of an earlier committed t' in
             its partition: the transaction MUST roll back (duplicate)
      cas    put cas:<j> = t with expect = the version a GET read just
             before, required: concurrent writers race, losers roll back
      loser  put \"exists\" with expect 0, required: the key exists from
             setup, so the transaction MUST roll back (kv_precondition)

  At the end every marker and data partition is read by offset, and every
  counter and cas key read.

  The checker rejects: an :ok transaction whose marker is not in the log; a
  :fail (rolled back) one whose marker is; a transaction partly visible (its
  marker without all its data pushes, or data without its marker); a dup or
  loser transaction that committed; a counter that is not the number of
  committed transactions that incremented it; two committed cas writes that
  expected the same version (a lost update); a cas key whose final value no
  committed cas write of it wrote."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.routing :as routing]))

(def counters 5)
(def cas-keys 4)
(def parts 8)

(defn marker-partition [t] (str "m" (mod t parts)))
(defn marker-id [t] (str "m" t))
(defn data-id [t i] (str "d" t "-" i))

(defn- request
  [client method url body timeout]
  (try (qh/request! (:http client) method url body timeout)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- plan
  "The transaction body for op t, and what the op records of it."
  [test client t committed]
  (let [qs     (take 2 (:queue-names test))
        data   (vec (for [i (range (rand/long 4))]
                      {:queue (rand/nth qs), :partition (str "d" (rand/long parts)), :i i}))
        incrs  (vec (distinct (repeatedly (inc (rand/long 2)) #(rand/long counters))))
        extra  (let [x (rand/double)]
                 (cond (and (< x 0.15) (seq @committed)) :dup
                       (< x 0.45)                        :cas
                       (< x 0.55)                        :loser
                       :else                             nil))
        dup-of (when (= :dup extra) (rand/nth (vec @committed)))
        cas-j  (when (= :cas extra) (rand/long cas-keys))
        cas-v  (when cas-j
                 (let [r (kv/get! test client (str "cas:" cas-j))]
                   (when-not (:error r)
                     (if (:found? r) (:version r) 0))))
        extra  (if (and (= :cas extra) (nil? cas-v)) nil extra)
        pushes (cond-> [{:type  "push"
                         :items [{:queue (first qs), :partition (marker-partition t)
                                  :payload {:t t}, :transactionId (marker-id t)}]}]
                 (seq data)
                 (conj {:type  "push"
                        :items (mapv (fn [{:keys [queue partition i]}]
                                       {:queue queue, :partition partition
                                        :payload {:t t, :i i}, :transactionId (data-id t i)})
                                     data)})
                 dup-of
                 (conj {:type  "push"
                        :items [{:queue (first qs), :partition (marker-partition dup-of)
                                 :payload {:t dup-of}, :transactionId (marker-id dup-of)}]}))
        kvops  (cond-> (mapv (fn [k] {:op "incr", :ns kv/ns-name*, :key (str "c:" k)
                                      :delta 1, :forever true})
                             incrs)
                 (= :cas extra)
                 (conj {:op "put", :ns kv/ns-name*, :key (str "cas:" cas-j), :value t
                        :expect cas-v, :required true, :forever true})
                 (= :loser extra)
                 (conj {:op "put", :ns kv/ns-name*, :key "exists", :value t
                        :expect 0, :required true, :forever true}))]
    {:body   {:operations pushes, :kv kvops}
     :record {:t t, :data (mapv #(select-keys % [:queue :partition :i]) data)
              :incrs incrs, :extra extra, :dup-of dup-of, :cas [cas-j cas-v]}}))

(defn- txn!
  [client test op]
  (let [t   (:value op)
        {:keys [body record]} (plan test client t (:committed client))
        r   (request client :post (str (:base client) "/api/v1/transaction")
                     body (:client-timeout-ms test))
        op  (merge op record)]
    (cond
      (:exception r)
      (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info)
             :error [:txn (:type (:exception r))])

      (and (= 200 (:status r)) (true? (:success (:body r))))
      (do (swap! (:committed client) conj t)
          (assoc op :type :ok
                 :cas-version (some #(when (and (= "kv" (:type %)) (= "put" (:op %))) (:version %))
                                    (:results (:body r)))))

      (= 200 (:status r))
      ; Rolled back. A "duplicate" reason may also answer a second attempt of
      ; a transaction whose first attempt committed: unknown, unless this op
      ; carried a dup on purpose (then the duplicate is its own).
      (let [reason (:reason (:body r))]
        (assoc op :type (if (and (= "duplicate" reason) (not= :dup (:extra record))) :info :fail)
               :error [:rolled-back reason]))

      :else
      (assoc op :type (log/push-failure test (:status r) (:body r))
             :error [:txn (:status r) (:body r)]))))

(defn- read-all!
  [client test op]
  (let [qs (take 2 (:queue-names test))
        ; One entry per queue (read-queues! builds a map by queue): jepsen-0
        ; holds the markers and data, jepsen-1 data only.
        q  (routing/read-queues! client test op
                                 [[(first qs) (vec (concat (map #(str "m" %) (range parts))
                                                           (map #(str "d" %) (range parts))))]
                                  [(second qs) (mapv #(str "d" %) (range parts))]])
        k  (when (= :ok (:type q))
             (kv/batch! test client [{:op "getMany"
                                      :keys (vec (concat (map #(str "c:" %) (range counters))
                                                         (map #(str "cas:" %) (range cas-keys))))}]))]
    (cond
      (not= :ok (:type q)) q
      (:error k) (assoc op :type :fail, :error [:kv (:error k)])
      :else (let [rows (into {} (map (juxt :key identity)) (:rows (first (:results k))))]
              (assoc q :kv (into (sorted-map)
                                 (for [[key row] rows] [key (select-keys row [:value :version])])))))))

(defrecord Client [node base http committed]
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
        :txn  (txn! this test op)
        :read (read-all! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type (if (or (= ::qh/refused type) (= :read (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            read    (last reads)
            differ  (->> reads (remove #(= (select-keys % [:value :kv]) (select-keys read [:value :kv])))
                         (mapv #(select-keys % [:node :index])))
            ids     (->> (:value read) vals (mapcat vals) (mapcat identity) (map :txn) frequencies)
            seen?   (fn [id] (pos? (get ids id 0)))
            txns    (->> hist (h/filter #(and (= :txn (:f %)) (not= :invoke (:type %)))) vec)
            commit? (fn [op] (seen? (marker-id (:t op))))
            lost    (->> txns (filter #(and (= :ok (:type %)) (not (commit? %)))) (mapv :t))
            phantom (->> txns (filter #(and (= :fail (:type %)) (commit? %))) (mapv :t))
            torn    (->> txns
                         (keep (fn [op]
                                 (let [c  (commit? op)
                                       ds (map #(data-id (:t op) (:i %)) (:data op))
                                       bad (if c (remove seen? ds) (filter seen? ds))]
                                   (when (seq bad)
                                     {:t (:t op), :marker-visible c, :data-wrong (vec bad)}))))
                         vec)
            twice   (->> ids (filter #(< 1 (val %))) (map key) sort vec)
            gate    (->> txns
                         (filter #(and (#{:dup :loser} (:extra %))
                                       (or (= :ok (:type %)) (commit? %))))
                         (mapv #(select-keys % [:t :extra :dup-of :type])))
            committed (filter commit? txns)
            want-c  (->> committed (mapcat :incrs) frequencies)
            counter (->> (range counters)
                         (keep (fn [k]
                                 (let [got  (or (get-in read [:kv (str "c:" k) :value]) 0)
                                       want (get want-c k 0)]
                                   (when (not= got want)
                                     {:key (str "c:" k), :value got, :committed-incrs want}))))
                         vec)
            cas-c   (->> committed (filter #(= :cas (:extra %))))
            cas-dup (->> cas-c
                         (group-by :cas)
                         (filter #(< 1 (count (val %))))
                         (mapv (fn [[[j v] ops]] {:key (str "cas:" j), :expected-version v
                                                  :committed (mapv :t ops)})))
            cas-val (->> (range cas-keys)
                         (keep (fn [j]
                                 (let [got     (get-in read [:kv (str "cas:" j) :value])
                                       writers (set (keep #(when (= j (first (:cas %))) (:t %)) cas-c))]
                                   (when (and (some? got) (not (writers got)))
                                     {:key (str "cas:" j), :value got
                                      :committed-writers (count writers)}))))
                         vec)]
        {:valid?        (boolean (and read (empty? differ) (empty? lost) (empty? phantom) (empty? torn)
                             (empty? twice) (empty? gate) (empty? counter) (empty? cas-dup)
                             (empty? cas-val)))
         :final-reads   (count reads)
         :nodes-differ  differ
         :committed     (count committed)
         :outcomes      (frequencies (map (juxt :type :extra) txns))
         :rollback-reasons (->> txns (filter #(= :fail (:type %)))
                                (map #(second (:error %))) frequencies)
         :lost          (take 16 lost)
         :phantom       (take 16 phantom)
         :torn          (take 16 torn)
         :ids-twice     (take 16 twice)
         :gate-committed (take 16 gate)
         :counters      counter
         :cas-lost-update cas-dup
         :cas-value-not-written cas-val}))))

(defn workload
  [opts]
  {:client          (map->Client {:committed (atom #{})})
   :checker         (checker)
   :db-env          kv/db-env
   :generator       (map (fn [t] {:f :txn, :value t}) (range))
   :final-generator (gen/each-thread (gen/once {:f :read, :value nil}))})
