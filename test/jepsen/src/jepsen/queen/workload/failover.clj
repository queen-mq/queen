(ns jepsen.queen.workload.failover
  "W12 --workload failover: a standby cluster replays its source's log and
  takes over on a promotion (server/src/rsm/link, POST
  /api/v1/system/link/promote).

  Needs --standby-nodes N: the last N nodes of the node list are the STANDBY,
  a cluster of its own started empty with QUEEN_LINK_STANDBY; the others are
  the SOURCE (jepsen.queen.db). Every client thread is bound to one node, so
  half of them talk to the source and half to the standby, for the whole test.

  What the link promises, and what this checks:

    - a standby takes no client write before it is promoted;
    - what it holds is what the source committed, in the source's order: the
      promoted cluster's log is a prefix of the source's, record for record,
      and never half an entry;
    - a planned switch (the clients stop, the standby has read everything,
      the source is stopped, the standby is promoted) loses nothing;
    - after a crash of the source only the last moment is lost, and nothing
      older: no write the source acknowledged is missing while a write sent
      after that answer is there;
    - the promoted cluster then serves writes of its own, and keeps them.

  It is tested on the log (pushes read back by offset), where every record
  has a place to compare, and with --w12-kv on KV counters written in the
  same entries. Acks and leases across a promotion are not checked here.

    :send [[k v] ...]  one pair: POST /api/v1/push of v to partition f<k>,
                       transactionId \"f<k>-v<v>\". Several pairs (--txn), or
                       any send with --w12-kv: ONE POST /api/v1/transaction,
                       one log entry. A thread of the source sends until the
                       failover begins and then stops for good (two clusters
                       that both serve are a documented hazard, not what is
                       tested). A thread of the standby sends for the whole
                       test: refused 503 `standby` until the promotion, served
                       after it, as a client that keeps retrying is.
    :read              the records this thread has not read yet, of every
                       partition, from its own node. On a standby, before the
                       promotion, each must be the source's record at that
                       offset.
    final phase        :final-read, every partition from offset 0 (and with
                       --w12-kv every counter, read first), on every node of
                       both clusters (the source is started again for it).

  The failover is the nemesis's (`package`), at --failover-at seconds:

    --failover crash    :stop-source (kill -9 every node of the source, a
                        power loss with --lazyfs), :heal-standby, :promote.
    --failover crash-behind
                        :cut-link first: for 10 s no node of one cluster hears
                        a node of the other, the source goes on taking writes
                        and the standby reads none of them. Then as crash. On
                        a healthy pair the standby is milliseconds behind and
                        a crash loses nothing; here it loses about 10 s, which
                        is what check 6 is for.
    --failover planned  :heal-all, :quiesce (the source's threads stop
                        sending, and every send in flight is given the time
                        to end), :await-standby (the standby has read, in
                        every raft group, all the source applied),
                        :stop-source, :promote.

  Other faults (--nemesis kill, partition, pause, ...) run on all the nodes of
  both clusters for the whole test: while the standby follows, and on the
  promoted cluster afterwards. The failover's own steps run one after the
  other on the nemesis's thread, so no new fault begins inside them, and the
  heal steps undo the ones in force: an operator who promotes a standby
  starts its nodes first, and one who plans a switch does it on a healthy
  pair. A crash still finds the standby wherever the faults left it, as far
  behind as they made it.

  Variants, each a flag, any of them together:

    --w12-kv        every send is ONE transaction that also increments the KV
                    counter c:<k> of each partition it pushes to: a record and
                    its count are one log entry, so a replay, a seed and a
                    promotion carry both or neither.
    --tenants T     partition k is tenant (k mod T)'s (x-queen-tenant; the
    --raft-groups G nodes of both clusters pin tenant i to raft group
                    (i mod G), jepsen.queen.workload.tenants/db-env). The
                    standby follows each group on its own and a promotion
                    cuts each group at its own point, so check 6 compares the
                    sends of one group only. A transaction stays inside one
                    tenant, as one never spans two groups.
    --standby-seed  a SEEDED standby, for a source with a history
                    (jepsen.queen.db/link-env: QUEEN_LINK_SEED=1, and a source
                    that keeps 16 entries of its log): the standby's nodes are
                    down until :seed-standby, at --seed-at, starts them all;
                    once the others have answered that they hold nothing,
                    node 1 takes a source node's snapshot and boots on it as
                    the only voter of its cluster. :grow-standby, 15 s later,
                    adds the others as learners, then voters.
                    Other faults go on between the two steps and after them;
                    between them only the node that sent the snapshot holds
                    the source's log for the standby.
    --nemesis upgrade --upgrade-mode standby-first | source-first
                    a rolling upgrade of both clusters in the documented order
                    or the other one (jepsen.queen.nemesis/upgrade-pick): the
                    standby waits (`waiting`) while its source writes what it
                    does not read yet. Counted, not judged.

  Checker (P is the promoted cluster's final read, S the source's; a fetch is
  a node's own read, so each is the longest log its nodes read, and every
  other node of that cluster must have read a prefix of it, with offsets
  0, 1, 2, ...):
    1. no send was answered by a standby node before the promotion was asked;
    2. P and S hold only records that were sent, each once, under its own
       partition; a send answered with an offset is at that offset;
    3. prefix: in every partition, P begins with records the source's threads
       sent, identical to S at the same offsets, and holds nothing of the
       source after its first record of its own;
    4. a transaction of the source is in P whole or not at all;
    5. planned: every send the source acknowledged is in P;
    6. crash: no send the source acknowledged is missing from P while a send
       of the same raft group INVOKED AFTER that answer is in P (the source
       logged it later, so a prefix that has the later one has the earlier
       one);
    7. every send the promoted cluster acknowledged is in P; and every send
       the source acknowledged is in S (the source's own durability);
    8. a send that was refused is in neither;
    9. a standby's read before the promotion agrees with S;
   10. --w12-kv: in P and in S every counter c:k equals the number of records
       of partition k;
   11. --w12-kv: the nodes of one cluster read the same counters (a KV read
       waits for the cluster's read index, and nothing writes any more);
   12. --standby-seed: the seed was taken and the standby grew to all its
       nodes, as voters of every raft group;
   13. no view of the link a failover step took shows a follower `halted`
       (its source purged what it needs: every node of the source keeps its
       log for a standby that reads, so in a test of minutes a halt is a
       fault), unless --nemesis disk-full filled the source's disks.

  Reported and not judged: how many acknowledged sends a crash lost and over
  how long, the refusals `standby` answered AFTER the promotion had been
  answered (the endpoint says whoever asked can write next), and how many
  link views showed a follower `waiting`."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as n]
                    [net :as net]
                    [random :as rand]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.queen [db :as qdb]
                          [http :as qh]
                          [kv :as kv]
                          [membership :as ms]]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.tenants :as tenants]))

(defn partitions [test] (:w12-partitions test 8))
(defn- queue [test] (first (:queue-names test)))
(defn- partition-name [k] (str "f" k))
(defn- txn-id [k v] (str "f" k "-v" v))
(defn- counter-key [k] (str "c:" k))

(defn- parse-partition
  "k from \"f<k>\", or nil."
  [s]
  (when-let [[_ k] (re-matches #"f(\d+)" (str s))]
    (parse-long k)))

(defn- parse-counter
  "k from \"c:<k>\", or nil."
  [s]
  (when-let [[_ k] (re-matches #"c:(\d+)" (str s))]
    (parse-long k)))

;; ---------------------------------------------------------------------------
;; Tenants and raft groups

(defn- tenant-count [test] (long (:tenants test 0)))

(defn tenant-index
  "--tenants T: partition k belongs to tenant (k mod T). 0 without tenants."
  [test k]
  (let [n (tenant-count test)]
    (if (pos? n) (mod (long k) n) 0)))

(defn group-of
  "The raft group partition k lives in: its tenant's, tenant i being pinned
  to group (i mod G) (jepsen.queen.workload.tenants/db-env). 0 with one
  group, and without tenants (every request is the default tenant's)."
  [test k]
  (let [g (long (:raft-groups test 1))]
    (if (and (< 1 g) (pos? (tenant-count test)))
      (mod (tenant-index test k) g)
      0)))

(defn- tenant-headers
  "The x-queen-tenant header of tenant index i; nil without tenants."
  [test i]
  (let [n (tenant-count test)]
    (when (pos? n)
      {"x-queen-tenant" (nth (tenants/tenant-ids n) i)})))

(defn- headers
  "Partition k's tenant header, or nil."
  [test k]
  (tenant-headers test (tenant-index test k)))

(defn- by-tenant
  "[[headers [k ...]] ...]: each tenant's partitions with its header; one
  entry, without a header, when there are no tenants."
  [test]
  (->> (range (partitions test))
       (group-by #(tenant-index test %))
       (sort-by key)
       (mapv (fn [[i ks]] [(tenant-headers test i) (vec ks)]))))

;; ---------------------------------------------------------------------------
;; Client

(defn- standby-refusal?
  "503 with code `standby`: the answer of a standby to every client write."
  [r]
  (and (= 503 (:status r)) (map? (:body r)) (= "standby" (:code (:body r)))))

(defn- item
  [test [k v]]
  {:queue (queue test), :partition (partition-name k), :payload v
   :transactionId (txn-id k v)})

(defn- push!
  "One pair as POST /api/v1/push: {:offsets {v offset}} when it is queued."
  [client test [k _ :as pair]]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                       {:items [(item test pair)]} (:client-timeout-ms test)
                       (headers test k))
        it (when (sequential? (:body r)) (first (:body r)))]
    (cond
      (and (= 201 (:status r)) (#{"queued" "duplicate"} (:status it)))
      {:type :ok, :offsets {(second pair) (:offset it)}, :item-status (:status it)}

      (= 201 (:status r))
      {:type :fail, :error [:item-error it]}

      (standby-refusal? r)
      {:type :fail, :error [:standby]}

      :else
      {:type (log/push-failure test (:status r) (:body r)), :error [(:status r) (:body r)]})))

(defn- transact!
  "Several pairs as ONE POST /api/v1/transaction: one log entry, no offsets
  in its answer. --w12-kv: the same entry increments c:<k> of every pair's
  partition. The pairs are one tenant's (the generator's)."
  [client test pairs]
  (let [body (cond-> {:operations (mapv (fn [p] {:type "push", :items [(item test p)]}) pairs)}
               (:w12-kv test)
               (assoc :kv (mapv (fn [[k _]] {:op "incr", :ns kv/ns-name*, :key (counter-key k)
                                             :delta 1, :forever true})
                                pairs)))
        r (qh/request! (:http client) :post (str (:base client) "/api/v1/transaction")
                       body (:client-timeout-ms test) (headers test (ffirst pairs)))
        b (:body r)]
    (cond
      (and (= 200 (:status r)) (true? (:success b)))
      {:type :ok}

      ; Rolled back. A duplicate refusal can only answer a second attempt of
      ; a bundle whose first one committed (workload/log.clj).
      (= 200 (:status r))
      {:type  (if (re-find #"(?i)dup" (str (:reason b) " " (:error b))) :info :fail)
       :error [:rolled-back (select-keys b [:reason :error])]}

      (standby-refusal? r)
      {:type :fail, :error [:standby]}

      :else
      {:type (log/push-failure test (:status r) b), :error [(:status r) b]})))

(defn- send!
  [client test op]
  (let [pairs (:value op)]
    (if (and (= :source (:cluster client))
             (not= :following (:phase @(:state client))))
      ; The failover has begun: the source's clients are gone for good.
      (assoc op :type :fail, :error [:retired])
      (merge op (if (and (= 1 (count pairs)) (not (:w12-kv test)))
                  (push! client test (first pairs))
                  (transact! client test pairs))))))

(defn- fetch!
  "One fetch of every partition from `positions` ({k offset}), one request
  per tenant: {k [[offset payload] ...]} for the partitions that answered
  records."
  [client test positions]
  (into (sorted-map)
        (mapcat (fn [[hdr ks]]
                  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                                       {:entries (mapv (fn [k] {:queue (queue test), :partition (partition-name k)
                                                                :offset (get positions k 0)})
                                                       ks)}
                                       (:client-timeout-ms test) hdr)]
                    (if-not (= 200 (:status r))
                      (throw (ex-info "fetch failed" {:status (:status r), :body (:body r)}))
                      (keep (fn [e]
                              (when-let [k (parse-partition (:partition e))]
                                (when (seq (:records e))
                                  [k (mapv (juxt :offset :payload) (:records e))])))
                            (:entries (:body r)))))))
        (by-tenant test)))

(defn- counters!
  "--w12-kv: every counter c:k, {k value} (0 for a key never written), one
  getMany per tenant. A KV read waits for the cluster's read index on the
  node it lands on: it sees every write committed before it."
  [client test]
  (into (sorted-map)
        (mapcat (fn [[hdr ks]]
                  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/kv")
                                       {:operations [{:op "getMany", :ns kv/ns-name*
                                                      :keys (mapv counter-key ks)}]}
                                       (:client-timeout-ms test) hdr)]
                    (when-not (= 200 (:status r))
                      (throw (ex-info "kv read failed" {:status (:status r), :body (:body r)})))
                    (let [got (into {}
                                    (keep (fn [row]
                                            (when-let [k (parse-counter (:key row))]
                                              (when (some? (:value row))
                                                [k (:value row)]))))
                                    (:rows (first (:results (:body r)))))]
                      (map (fn [k] [k (get got k 0)]) ks)))))
        (by-tenant test)))

(defn- read!
  [client test op]
  (let [recs (fetch! client test @(:positions client))]
    (swap! (:positions client)
           (fn [pos] (reduce (fn [pos [k rs]] (assoc pos k (inc (long (first (peek rs))))))
                             pos recs)))
    (assoc op :type :ok, :value recs)))

(defn- final-read!
  "Every partition from offset 0: {k [payload ...]}, and the partitions whose
  offsets were not 0, 1, 2, ... as answered. --w12-kv: the counters under
  :kv, read first."
  [client test op]
  (let [counters (when (:w12-kv test) (counters! client test))]
    (loop [positions {}, logs (sorted-map), gaps (sorted-map)]
      (let [recs (fetch! client test positions)]
        (if (empty? recs)
          (cond-> (assoc op :type :ok, :value logs, :gaps gaps)
            counters (assoc :kv counters))
          (recur (reduce (fn [pos [k rs]] (assoc pos k (inc (long (first (peek rs)))))) positions recs)
                 (reduce (fn [logs [k rs]] (update logs k (fnil into []) (map second rs))) logs recs)
                 (reduce (fn [gaps [k rs]]
                           (let [from (get positions k 0)]
                             (if (= (map first rs) (range from (+ from (count rs))))
                               gaps
                               (assoc gaps k {:requested from, :offsets (mapv first rs)}))))
                         gaps recs)))))))

(defrecord Client [state node base http cluster positions]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)
           :cluster (if (qdb/standby? test node) :standby :source)
           :positions (atom {})))

  (setup! [this test]
    ; --tenants: the test queue in every tenant, configured as the db
    ; configures the default tenant's. The standby gets it from the log.
    (when (and (pos? (tenant-count test)) (= node (first (qdb/source-nodes test))))
      ; Retried for a minute: a group just elected answers 503 for a moment.
      (doseq [[hdr _] (by-tenant test)]
        (util/await-fn
          (fn []
            (let [r (qh/request! http :post (str base "/api/v1/configure")
                                 {:queue (queue test), :options (qdb/queue-config test)}
                                 15000 hdr)]
              (info "W12 queue configured for" hdr (:status r))
              (when-not (= 200 (:status r))
                (throw (ex-info "tenant configure failed" {:tenant hdr, :answer r})))
              r))
          {:timeout 60000, :retry-interval 1000, :log-interval 10000
           :log-message (str "configuring the W12 queue for " hdr)}))))

  (invoke! [this test op]
    (let [op (assoc op :cluster cluster, :node node)]
      (try
        (case (:f op)
          :send       (send! this test op)
          :read       (read! this test op)
          :final-read (final-read! this test op))
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type msg]} (ex-data e)]
            (cond
              (#{"fetch failed" "kv read failed"} (.getMessage e))
              (assoc op :type :fail, :error [(if (= "fetch failed" (.getMessage e)) :fetch :kv)
                                             (ex-data e)])

              (#{::qh/refused ::qh/timeout ::qh/io} type)
              (assoc op
                     ; A send that never left is not there; one that may have
                     ; arrived is unknown. A read changes nothing.
                     :type (if (= :send (:f op)) (kv/exception-type type) :fail)
                     :error [(keyword (name type)) msg])

              :else (throw e)))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Generator

(defn generator
  "Sends of values never used twice, and a read in ten. With --txn two sends
  in ten are single pushes and the others are transactions of 2 or 3
  partitions, all of one tenant (--tenants)."
  [opts]
  (let [n     (long (:w12-partitions opts 8))
        t     (long (:tenants opts 0))
        ks-of (fn [i] (if (pos? t)
                        (filterv #(= i (mod % t)) (range n))
                        (vec (range n))))
        v     (atom 0)
        one   (fn [] {:f :send, :value [[(rand/long n) (swap! v inc)]]})
        many  (fn [] (let [ks (ks-of (if (pos? t) (rand/long t) 0))]
                       {:f :send
                        :value (mapv (fn [k] [k (swap! v inc)])
                                     (take (min (count ks) (+ 2 (rand/long 2))) (rand/shuffle ks)))}))
        read  (fn [] {:f :read, :value nil})]
    (gen/mix (concat (if (:txn-sends? opts)
                       (concat (repeat 7 many) (repeat 2 one))
                       (repeat 9 one))
                     [read]))))

;; ---------------------------------------------------------------------------
;; Views of the link

(defn- part-brief
  "What matters of one raft group's part of a link status."
  [part]
  {:role     (:role part)
   :position (:index (:position part))
   :follower (select-keys (:follower part) [:state :source :scanned :sourceApplied
                                            :lagEntries :lagMs :lastAnswerMs :error])})

(defn- link-brief
  "{group {:node, :role, :position, :follower}}: each raft group's leader in
  the standby (qdb/standby-leaders), for the groups whose leader answered."
  [leaders]
  (into (sorted-map)
        (map (fn [[g [node part]]] [g (assoc (part-brief part) :node node)]))
        leaders))

(defn- applied-by-group
  "{group applied} of one node, from /metrics/prometheus: queen_raft_index
  kind=\"applied\", with a group label on a node of several groups."
  [http node]
  (try (let [r (qh/request! http :get (str (qh/broker-url node) "/metrics/prometheus") nil 2000)]
         (when (string? (:body r))
           (into {}
                 (keep (fn [line]
                         (when-let [[_ labels v] (re-matches #"queen_raft_index\{([^}]*)\} (\d+)" line)]
                           (when (str/includes? labels "kind=\"applied\"")
                             [(if-let [[_ g] (re-find #"group=\"(\d+)\"" labels)] (parse-long g) 0)
                              (parse-long v)]))))
                 (str/split-lines (:body r)))))
       (catch clojure.lang.ExceptionInfo _ nil)))

(defn- source-applied
  "{group applied}: how far the source has applied each raft group's log.
  One group: the index its leader reports in /health. Several: the highest
  applied index any node of the source reports for the group."
  [http test]
  (if (< 1 (long (:raft-groups test 1)))
    (->> (qdb/source-nodes test)
         (util/real-pmap #(applied-by-group http %))
         (apply merge-with max {}))
    (if-let [a (->> (qdb/source-nodes test)
                    (util/real-pmap (fn [n] (:raft (qh/health http n 2000))))
                    (filter #(= "leader" (:role %)))
                    (keep :applied)
                    first)]
      {0 a}
      {})))

;; ---------------------------------------------------------------------------
;; The failover (a nemesis package)

(def ^:private promote-timeout-ms 120000)
(def ^:private caught-up-timeout-ms 120000)
(def ^:private seed-timeout-ms 180000)
(def ^:private behind-s
  "--failover crash-behind: how long the standby is cut off from its source
  before the source dies."
  10)
(def grow-after-s
  "--standby-seed: seconds between the seed and the growth of the standby."
  15)

(defn- await-standby!
  "Planned switch, step 2: the source's clients are silent; wait until every
  raft group's leader in the standby follows, has nothing left to read and
  has read at least what the source had applied in that group when asked,
  three looks in a row."
  [http test]
  (let [t0     (System/currentTimeMillis)
        groups (range (long (:raft-groups test 1)))]
    (loop [streak 0, last-seen nil]
      (let [applied (source-applied http test)
            leaders (qdb/standby-leaders http test)
            ok?     (every? (fn [g]
                              (let [f (:follower (second (get leaders g)))
                                    a (get applied g)]
                                (and a
                                     (= "following" (:state f))
                                     (= 0 (:lagEntries f))
                                     (<= (long a) (long (:scanned f 0))))))
                            groups)
            streak  (if ok? (inc streak) 0)
            seen    {:source-applied applied, :standby (link-brief leaders)}
            waited  (- (System/currentTimeMillis) t0)]
        (cond
          (<= 3 streak)                  (assoc seen :caught-up? true, :waited-ms waited)
          (< caught-up-timeout-ms waited) (assoc (or last-seen seen) :caught-up? false
                                                 :waited-ms waited)
          :else (do (Thread/sleep 300)
                    (recur streak seen)))))))

(defn- gauges
  "The node's own counters of its log and of its planning pipeline, from
  /metrics/prometheus: the series that say whether it proposes and commits."
  [http node]
  (try (let [r (qh/request! http :get (str (qh/broker-url node) "/metrics/prometheus") nil 2000)]
         (when (string? (:body r))
           (->> (str/split-lines (:body r))
                (filter #(re-find #"^queen_(raft_(inflight|index|proposals_total|plan_deferred|apply_channel_depth)|link_)" %))
                (take 80)
                vec)))
       (catch clojure.lang.ExceptionInfo _ nil)))

(defn- standby-view
  "Every standby node's own view of its raft group and of the link (each
  group's part): what to read when a promotion does not go through."
  [http test]
  (into (sorted-map)
        (util/real-pmap
          (fn [n]
            (let [st (qdb/link-status http n 2000)]
              [n {:raft   (select-keys (:raft (qh/health http n 2000))
                                       [:role :leader :term :applied :commit :lag])
                  :link   (mapv (fn [part]
                                  (assoc (part-brief part) :group (:group part), :leader (:leader part)))
                                (qdb/link-parts st))
                  :gauges (gauges http n)}]))
          (qdb/standby-nodes test))))

(declare heal!)

(defn- promote!
  "POST /api/v1/system/link/promote on the standby's nodes in turn until one
  answers 200: the standby's leader may be dead, paused or cut off right now.
  With several raft groups the answer is 200 only once every group is
  promoted (each by its own leader), and promoting a promoted group again
  changes nothing. A promotion that needed more than one request says what
  each node answered (the first six, then how many of each kind) and what
  the standby's nodes showed, at the first refusal and every 20 s after it.

  Every 20 s without an answer the standby's nodes are healed again, as
  whoever asks for a promotion that does not go through would do. The nemesis
  runs one op at a time, so while this one waits no other package restarts a
  node it killed a moment before: a kill of two of the three nodes that came
  between :heal-standby and :promote left the standby without a quorum for as
  long as the promotion was asked for."
  [http test]
  (let [t0 (System/currentTimeMillis)]
    (loop [attempt 1
           nodes   (cycle (rand/shuffle (qdb/standby-nodes test)))
           answers []
           views   []]
      (let [node   (first nodes)
            r      (try (qh/request! http :post
                                     (str (qh/broker-url node) "/api/v1/system/link/promote")
                                     {} 10000)
                        (catch clojure.lang.ExceptionInfo e
                          {:exception (:type (ex-data e))}))
            waited (- (System/currentTimeMillis) t0)
            answer [node (or (:exception r) (:status r)) (:code (:body r)) waited]
            story  (fn [answers views]
                     (when (seq answers)
                       {:first-answers (vec (take 6 answers))
                        :answers       (frequencies (map (fn [[n s c _]] [n s c]) answers))
                        :views         views}))]
        (cond
          (= 200 (:status r))
          (merge {:promoted? true, :node node, :attempts attempt, :ms waited
                  :role (:role (:body r)), :position (:index (:position (:body r)))
                  :positions (into (sorted-map)
                                   (map (juxt :group (comp :index :position)))
                                   (qdb/link-parts (:body r)))}
                 (story answers views))

          (< promote-timeout-ms waited)
          (merge {:promoted? false, :attempts attempt, :ms waited, :last r}
                 (story (conj answers answer) (conj views [waited (standby-view http test)])))

          :else
          (let [views (if (<= (* 20000 (count views)) waited)
                        (let [view (standby-view http test)]
                          ; Not at the first refusal: a leader change answers
                          ; one too, and needs no healing.
                          (when (seq views)
                            (heal! test (qdb/standby-nodes test)))
                          (conj views [waited view]))
                        views)]
            (Thread/sleep 250)
            (recur (inc attempt) (rest nodes) (conj answers answer) views)))))))

(defn- heal!
  "Undoes the faults in force on `nodes`: the network is whole again (for
  every node: a partition has no owner), stopped processes continue and dead
  ones are started."
  [test nodes]
  (net/heal! (:net test) test)
  {:healed (c/on-nodes test nodes
                       (fn [test node]
                         (c/su (util/meh (c/exec :pkill :-CONT :-x :queen)))
                         (qdb/start-node! test node)))})

;; --standby-seed: the operator's guide (webdoc manage/standby.mdx), step by
;; step.

(defn- seed-node [test] (first (qdb/standby-nodes test)))

(defn- answers-200?
  "Does `node` answer /health 200 (it knows a leader)?"
  [http node]
  (try (= 200 (:status (qh/request! http :get (str (qh/broker-url node) "/health") nil 2000)))
       (catch clojure.lang.ExceptionInfo _ false)))

(defn- log-tail
  "The last lines of `node`'s log about the link, a seed or an error."
  [test node]
  (get (c/on-nodes test [node]
                   (fn [_ _]
                     (try (c/su (c/exec :bash :-c
                                        (str "grep -a 'rsm link\\|seed\\|ERROR' " qdb/log-file
                                             " | cut -c1-300 | tail -20")))
                          (catch Exception e (str "no log: " (.getMessage e))))))
       node))

(defn- seed-standby!
  "--standby-seed, step 1, on a running source and a whole network, as an
  operator seeds: every node of the standby starts, fresh. Node 1 seeds only
  once every other node has answered that it holds no cluster state (a node
  emptied under a cluster that lives on must join it, never seed a second
  one: replicator/raft peer_holding_state); it then reads a source node's
  snapshot, stages it as the first state of a cluster of which it is the
  only voter, and boots on it. The others wait to be added (:grow-standby).
  A node whose seed could not be read stops; the nodes are started again
  until node 1 answers /health 200 or seed-timeout-ms passes."
  [http test]
  (let [node   (seed-node test)
        sb     (qdb/standby-nodes test)
        t0     (System/currentTimeMillis)
        el     #(- (System/currentTimeMillis) %)
        healed (heal! test (qdb/source-nodes test))]
    (c/on-nodes test sb (fn [_ _] (qdb/mark-seed!)))
    (loop [starts 0]
      (let [t1     (System/currentTimeMillis)
            s      (get (c/on-nodes test sb (fn [test node] (qdb/start-node! test node))) node)
            starts (cond-> starts (= :started s) inc)
            up?    (loop []
                     (cond (answers-200? http node) true
                           (< 30000 (el t1))        false
                           :else (do (Thread/sleep 500) (recur))))]
        (cond
          up?
          {:seeded true, :node node, :ms (el t0), :starts starts
           :source-healed (:healed healed)
           :link (link-brief (qdb/standby-leaders http test))}

          (< seed-timeout-ms (el t0))
          {:seeded false, :node node, :ms (el t0), :starts starts
           :source-healed (:healed healed)
           :log (log-tail test node)}

          :else
          (recur starts))))))

(defn- answer-codes
  "The refusal codes of an admin answer: its own, or each raft group's (a
  node of several groups answers {groups: [{group, status, body}]})."
  [body]
  (vec (if (sequential? (:groups body))
         (keep #(get-in % [:body :code]) (:groups body))
         (keep identity [(when (map? body) (:code body))]))))

(defn- seed-change!
  "One membership change of the seeded standby, sent to its node 1, as the
  operator's guide does (node 1 is the only voter until the change is made,
  so it leads), until it answers 200 or seed-timeout-ms passes. Every refusal
  is tried again: a learner that is behind is not promoted yet, and every
  change is idempotent (a learner that is a voter already, after a change
  that timed out and went through, counts as added). The record of the last
  answer.

  Not through a learner: a learner restarts (exit 75) to load the snapshot
  it receives, and a change it forwarded to the leader then dies with its
  connection, between openraft's two steps, leaving a joint configuration
  that refuses every later change `in_flight` (local smoke, 2026-11-13: a
  promote through the learner being promoted)."
  [http test step suffix body]
  (let [t0 (System/currentTimeMillis)]
    (loop [nodes (repeat (seed-node test)), tries 1]
      (let [node   (first nodes)
            r      (try (qh/request! http :post
                                     (str (qh/broker-url node) ms/path suffix
                                          "?timeoutMs=" ms/change-timeout-ms)
                                     body (+ 5000 ms/change-timeout-ms))
                        (catch clojure.lang.ExceptionInfo e
                          {:exception (:type (ex-data e))}))
            codes  (answer-codes (:body r))
            waited (- (System/currentTimeMillis) t0)
            done?  (or (= 200 (:status r))
                       (and (= 409 (:status r)) (seq codes) (every? #{"already_voter"} codes)))
            rec    {:step step, :via node, :tries tries, :ms waited
                    :status (or (:status r) (:exception r)), :codes codes}]
        (if (or done? (< seed-timeout-ms waited))
          (assoc rec :done? (boolean done?))
          (do (Thread/sleep 1000)
              (recur (rest nodes) (inc tries))))))))

(defn membership-of
  "{group {:voters [ids], :learners [ids], :joint [[ids] [ids]] or nil}} from
  a GET of the membership: one group's answer is {membership}, several
  groups' {groups: [{group, status, body: {membership}}]}."
  [body]
  (let [one (fn [m] {:voters   (vec (sort (:voters m)))
                     :learners (vec (sort (:learners m)))
                     :joint    (:joint m)})]
    (if (sequential? (:groups body))
      (into (sorted-map)
            (map (fn [gp] [(:group gp) (one (get-in gp [:body :membership]))]))
            (:groups body))
      (sorted-map 0 (one (:membership body))))))

(defn- standby-voters
  "{group [voter ids]} of the standby as its node 1 answers the membership
  (one group: its own; several: each group's), or {:status s}."
  [http test]
  (let [r (try (qh/request! http :get (str (qh/broker-url (seed-node test)) ms/path) nil 5000)
               (catch clojure.lang.ExceptionInfo _ nil))]
    (if (= 200 (:status r))
      (membership-of (:body r))
      {:status (:status r)})))

(defn- grow-standby!
  "--standby-seed, step 2, on a whole network: the standby's other nodes
  (started by the seed, fresh, waiting to be added; started again if a fault
  stopped them) are added to the seeded cluster as learners (each receives
  node 1's snapshot and restarts once to load it), and are promoted to
  voters together. :grown? when every group's voters are all the standby's
  nodes, and no group is left between the two steps of a change (:joint)."
  [http test]
  (let [sb      (qdb/standby-nodes test)
        others  (vec (rest sb))
        healed  (heal! test sb)
        adds    (mapv (fn [n]
                        (seed-change! http test [:add-learner n] "/learners"
                                      {:id   (qdb/node-id test n)
                                       :raft (str (cn/ip n) ":" qdb/raft-port)
                                       :http (str (cn/ip n) ":" qh/http-port)}))
                      others)
        promote (when (seq others)
                  (seed-change! http test :promote "/promote"
                                {:ids (mapv #(qdb/node-id test %) others)}))
        members (standby-voters http test)
        want    (vec (sort (map #(qdb/node-id test %) sb)))
        voters  (if (:status members)
                  members
                  (into (sorted-map) (map (fn [[g m]] [g (:voters m)])) members))
        joint   (when-not (:status members)
                  (into (sorted-map) (keep (fn [[g m]] (when (:joint m) [g (:joint m)]))) members))]
    (cond-> {:grown?  (and (every? :done? (remove nil? (conj adds promote)))
                           (not (contains? voters :status))
                           (= (long (:raft-groups test 1)) (count voters))
                           (every? #(= want %) (vals voters))
                           (empty? joint))
             :adds    adds
             :promote promote
             :voters  voters
             :healed  (:healed healed)
             :link    (link-brief (qdb/standby-leaders http test))}
      (seq joint) (assoc :joint joint))))

(defn nemesis
  "  :seed-standby   --standby-seed: the standby's node 1 takes the source's
                    snapshot and boots on it (seed-standby!).
     :grow-standby   --standby-seed: the other nodes join it (grow-standby!).
     :heal-all       every node of both clusters is up and reachable.
     :heal-standby   the same for the standby's nodes only: the source stays
                    as dead as :stop-source left it.
     :quiesce        the source's clients stop sending (the planned switch),
                    and every send in flight is given the time to end.
     :await-standby  every node is up and reachable again, and the standby has
                    read everything the source applied: a planned switch is
                    made on two whole clusters.
     :stop-source    kill -9 every node of the source; with --lazyfs a power
                    loss. Its value carries the standby's link status as it
                    was just before.
     :promote        the standby becomes an ordinary cluster.
     :start-source   every node of the source is started again, for the final
                    reads: it is the cluster it was, and nobody writes to it."
  [state]
  (let [http (delay (qh/client))]
    (reify
      n/Reflection
      (fs [_] #{:seed-standby :grow-standby :heal-all :heal-standby :cut-link :quiesce
                :await-standby :stop-source :promote :start-source})

      n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (assoc op :value
               (case (:f op)
                 :seed-standby
                 (seed-standby! @http test)

                 :grow-standby
                 (grow-standby! @http test)

                 :heal-all
                 (heal! test (:nodes test))

                 ; No node of one cluster hears a node of the other, for
                 ; `behind-s` seconds: the source goes on taking writes that
                 ; the standby does not read.
                 :cut-link
                 (let [source  (set (qdb/source-nodes test))
                       standby (set (qdb/standby-nodes test))]
                   (net/drop-all! test (merge (zipmap source (repeat standby))
                                              (zipmap standby (repeat source))))
                   (Thread/sleep (* 1000 behind-s))
                   {:cut-for-s behind-s
                    :link     (link-brief (qdb/standby-leaders @http test))})

                 :heal-standby
                 (heal! test (qdb/standby-nodes test))

                 :quiesce
                 (do (swap! state assoc :phase :quiesced)
                     (Thread/sleep (long (:client-timeout-ms test)))
                     {:waited-ms (:client-timeout-ms test)})

                 ; Whole clusters first: the other packages' faults go on
                 ; between this package's ops, and one that came after
                 ; :heal-all (a kill of two nodes of the standby, in
                 ; `fo-planned-txn-kill` on 2026-10-08) stays in force for as
                 ; long as this op waits, because the nemesis runs one op at
                 ; a time and the start that would undo it waits behind it.
                 :await-standby
                 (let [healed (heal! test (:nodes test))]
                   (assoc (await-standby! @http test) :healed-first (:healed healed)))

                 :stop-source
                 (let [before (link-brief (qdb/standby-leaders @http test))]
                   (swap! state assoc :phase :stopped)
                   {:link-before before
                    :killed (c/on-nodes test (qdb/source-nodes test)
                                        (fn [test node] (qdb/kill-node! test node)))})

                 :promote
                 (let [res (promote! @http test)]
                   (when (:promoted? res)
                     (swap! state assoc :phase :promoted))
                   res)

                 :start-source
                 {:started (c/on-nodes test (qdb/source-nodes test)
                                       (fn [test node]
                                         (let [s (qdb/start-node! test node)]
                                           [s (try (qdb/await-healthy! node 120000) :healthy
                                                   (catch Exception _ :not-healthy))])))})))

      (teardown! [this test]))))

(defn package
  "The failover as a nemesis package, beside whatever other faults run.
  `mode` is :crash, :crash-behind or :planned; it begins `at` seconds into the
  test. --standby-seed (`seed-at`): the seed at seed-at, the growth
  grow-after-s later. Each first op of a phase carries its own time, so the
  other packages' faults go on until then and the nemesis is not kept
  waiting."
  [state {:keys [mode at seed-at]}]
  (let [op      (fn [f] {:type :info, :f f, :value nil})
        at-time (fn [f s] (assoc (op f) :time (long (* 1e9 (double s)))))
        steps   (case mode
                  :crash        [:stop-source :heal-standby :promote]
                  :crash-behind [:cut-link :stop-source :heal-standby :promote]
                  :planned      [:heal-all :quiesce :await-standby :stop-source :promote])]
    {:nemesis         (nemesis state)
     :generator       (concat (when seed-at
                                [(at-time :seed-standby seed-at)
                                 (at-time :grow-standby (+ seed-at grow-after-s))])
                              [(at-time (first steps) at)]
                              (map op (rest steps)))
     :final-generator (op :start-source)
     :perf            #{{:name  "failover"
                         :fs    #{:seed-standby :grow-standby :heal-all :heal-standby :cut-link
                                  :quiesce :await-standby :stop-source :promote}
                         :start #{}
                         :stop  #{}
                         :color "#C8A0E9"}}}))

;; ---------------------------------------------------------------------------
;; Checker

(defn- nemesis-op
  "The nemesis's first op `f`: {:inv its time, :done the time of its answer,
  :value what it answered}, or nil when it never ran."
  [history f]
  (let [[a b] (->> history
                   (filter #(and (= :nemesis (:process %)) (= f (:f %)))))]
    (when a
      {:inv (:time a), :done (:time b), :value (:value b)})))

(defn- ms [nanos] (when nanos (quot (long nanos) 1000000)))

(def ^:private link-steps
  "The failover's steps whose values carry views of the link."
  #{:seed-standby :grow-standby :cut-link :await-standby :stop-source :promote})

(defn- link-states
  "Every follower state the failover's steps saw: [{:step :state :follower}].
  A follower's map is a plain one; the views around it are sorted maps keyed
  by group or node, which a keyword lookup would compare with their keys
  (ClassCastException), so those are only walked through."
  [history]
  (vec (for [op    history
             :when (and (= :nemesis (:process op)) (link-steps (:f op)) (coll? (:value op)))
             m     (tree-seq coll? seq (:value op))
             :when (and (map? m) (not (sorted? m)) (string? (:state m)))]
         {:step (:f op), :state (:state m), :follower m})))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [done     (->> history h/client-ops (h/remove h/invoke?) vec)
            inv      (fn [op] (:time (h/invocation history op)))
            ok?      #(= :ok (:type %))
            mode     (:failover test :crash)
            promote  (nemesis-op history :promote)
            stop     (nemesis-op history :stop-source)
            await    (nemesis-op history :await-standby)
            seed     (nemesis-op history :seed-standby)
            grow     (nemesis-op history :grow-standby)
            promoted? (true? (:promoted? (:value promote)))
            ; 12. the seed and the growth
            seed-bad (when (:standby-seed test)
                       (cond-> []
                         (not (true? (:seeded (:value seed))))
                         (conj {:step :seed-standby, :value (:value seed)})
                         (not (true? (:grown? (:value grow))))
                         (conj {:step :grow-standby
                                :value (select-keys (:value grow) [:adds :promote :voters :joint])})))
            ; 13. a follower that halted
            states   (link-states history)
            halted   (->> states (filter #(= "halted" (:state %))) vec)
            waiting  (->> states (filter #(= "waiting" (:state %))) count)
            disk-full? (contains? (set (:faults test)) :disk-full)
            finals   (filterv #(and (ok? %) (= :final-read (:f %))) done)
            ; A fetch is a node's own read, so a node may be a little behind:
            ; a cluster's log is the longest any of its nodes read, and every
            ; other node's must be a prefix of that.
            final-of (fn [cluster]
                       (let [fs (filter #(= cluster (:cluster %)) finals)]
                         (when (seq fs)
                           (let [logs (apply merge-with
                                             (fn [a b] (if (< (count a) (count b)) b a))
                                             (map :value fs))
                                 kvs  (keep :kv fs)
                                 kv   (when (seq kvs) (apply merge-with max kvs))]
                             {:logs  logs
                              :kv    kv
                              :split (vec (for [f      fs
                                                [k vs] (:value f)
                                                :let   [full (get logs k)]
                                                :when  (not= vs (subvec full 0 (count vs)))]
                                            {:cluster cluster, :node (:node f), :k k
                                             :first-difference
                                             (first (keep-indexed (fn [o [a b]] (when (not= a b) o))
                                                                  (map vector vs full)))}))
                              ; 11. the nodes of one cluster read one count
                              :kv-split (vec (for [f      fs
                                                   :when  (:kv f)
                                                   [k n]  (:kv f)
                                                   :when  (not= n (get kv k))]
                                               {:cluster cluster, :node (:node f), :k k
                                                :counter n, :highest (get kv k)}))
                              :gaps  (into {} (keep (fn [f] (when (seq (:gaps f)) [(:node f) (:gaps f)]))) fs)}))))
            P        (final-of :standby)
            S        (final-of :source)]
        (cond
          (seq seed-bad)
          {:valid? false, :error "the standby was not seeded and grown", :seed seed-bad
           :halted (take 8 halted), :promote (:value promote)}

          (not promoted?)
          {:valid? :unknown, :error "the standby was never promoted", :promote (:value promote)
           :halted (take 8 halted)}

          (nil? P)
          {:valid? :unknown, :error "no final read of the promoted cluster succeeded"}

          :else
          (let [; Every value that was sent, by the send that carried it.
                sends    (filterv #(= :send (:f %)) done)
                by-v     (into {}
                               (mapcat (fn [op]
                                         (let [pairs (:value op)
                                               i     (inv op)]
                                           (map (fn [[k v]]
                                                  [v {:k k, :v v, :cluster (:cluster op), :type (:type op)
                                                      :inv i, :comp (:time op), :index (:index op)
                                                      :offset (get (:offsets op) v)
                                                      :retired? (= [:retired] (:error op))
                                                      :group (when (< 1 (count pairs)) (mapv second pairs))}])
                                                pairs))))
                               sends)
                src?     (fn [v] (= :source (:cluster (by-v v))))
                grp      (fn [s] (group-of test (:k s)))
                p-logs   (:logs P)
                s-logs   (:logs S)
                ks       (range (partitions test))
                in-log   (fn [logs] (into #{} (mapcat val) logs))
                in-p     (in-log p-logs)
                in-s     (in-log s-logs)

                ; 2. only what was sent, once, under its own partition
                strange  (fn [cluster logs]
                           (vec (for [[k vs] logs
                                      [o v]  (map-indexed vector vs)
                                      :let   [s (by-v v)]
                                      :when  (or (nil? s) (not= k (:k s)))]
                                  {:cluster cluster, :k k, :offset o, :value v
                                   :why (if s :other-partition :never-sent)})))
                twice    (fn [cluster logs]
                           (vec (for [[k vs] logs
                                      [v n]  (frequencies vs)
                                      :when  (< 1 n)]
                                  {:cluster cluster, :k k, :value v, :times n})))
                phantom  (into (strange :promoted p-logs) (strange :source s-logs))
                dup      (into (twice :promoted p-logs) (twice :source s-logs))
                at       (fn [logs k o] (get (get logs k) o))
                bad-ack  (vec (for [s     (vals by-v)
                                    :when (and (= :ok (:type s)) (:offset s))
                                    :let  [{:keys [k v offset cluster]} s
                                           there (if (= :source cluster)
                                                   (cond-> []
                                                     S        (conj [:source (at s-logs k offset)])
                                                     (in-p v) (conj [:promoted (at p-logs k offset)]))
                                                   [[:promoted (at p-logs k offset)]])]
                                    [where found] there
                                    :when (not= found v)]
                                {:k k, :value v, :acked-offset offset, :sent-to cluster
                                 :log where, :found found}))

                ; 3. prefix, per partition
                prefix   (into (sorted-map)
                               (map (fn [k] [k (count (take-while src? (get p-logs k [])))]))
                               ks)
                diverged (vec (for [k     ks
                                    :when S
                                    o     (range (prefix k))
                                    :let  [pv (at p-logs k o), sv (at s-logs k o)]
                                    :when (not= pv sv)]
                                {:k k, :offset o, :promoted pv, :source sv}))
                late     (vec (for [k      ks
                                    [o v]  (map-indexed vector (get p-logs k []))
                                    :when  (and (<= (prefix k) o) (src? v))]
                                {:k k, :offset o, :value v, :own-from (prefix k)}))
                misplaced (vec (for [[k vs] s-logs
                                     [o v]  (map-indexed vector vs)
                                     :when  (= :standby (:cluster (by-v v)))]
                                 {:k k, :offset o, :value v, :why :a-standby-send-in-the-source}))

                ; 4. a transaction is whole or absent
                groups   (->> sends (filter #(< 1 (count (:value %)))))
                torn-in  (fn [where present? cluster]
                           (vec (for [op    groups
                                      :when (= cluster (:cluster op))
                                      :let  [vs  (map second (:value op))
                                             got (filter present? vs)]
                                      :when (and (seq got) (not= (count got) (count vs)))]
                                  {:log where, :index (:index op), :values (vec vs)
                                   :present (vec got)})))
                torn     (-> (torn-in :promoted in-p :source)
                             (into (torn-in :promoted in-p :standby))
                             (into (when S (torn-in :source in-s :source))))

                ; 1. a standby answered a send before the promotion was asked
                early    (->> sends
                              (filter #(and (ok? %) (= :standby (:cluster %))
                                            (< (:time %) (:inv promote))))
                              (mapv #(select-keys % [:index :node :value :offsets])))

                ; 5 and 6. what the promotion lost, raft group by raft group:
                ; each group was promoted at its own point.
                acked    (->> (vals by-v) (filter #(and (= :source (:cluster %)) (= :ok (:type %)))))
                lost     (remove (comp in-p :v) acked)
                survivors (->> (vals by-v)
                               (filter #(and (= :source (:cluster %)) (in-p (:v %))))
                               (group-by grp)
                               (into (sorted-map)
                                     (map (fn [[g ss]] [g (apply max-key :inv ss)]))))
                older    (vec (for [l     lost
                                    :let  [survivor (get survivors (grp l))]
                                    :when (and survivor (< (:comp l) (:inv survivor)))]
                                {:k (:k l), :value (:v l), :index (:index l), :group (grp l)
                                 :acked-ms (ms (:comp l))
                                 :survivor (select-keys survivor [:k :v :index])
                                 :survivor-invoked-ms (ms (:inv survivor))}))
                caught-up? (true? (:caught-up? (:value await)))
                planned-lost (when (= :planned mode)
                               (mapv #(select-keys % [:k :v :index :offset]) lost))

                ; 7. durability of each cluster's own acknowledged sends
                own      (->> (vals by-v) (filter #(and (= :standby (:cluster %)) (= :ok (:type %)))))
                own-lost (->> own (remove (comp in-p :v)) (mapv #(select-keys % [:k :v :index :offset])))
                src-lost (when S
                           (->> acked (remove (comp in-s :v)) (mapv #(select-keys % [:k :v :index :offset]))))

                ; 8. a refused send is nowhere (a retired one was never sent)
                refused  (->> (vals by-v)
                              (filter #(= :fail (:type %)))
                              (filter #(or (in-p (:v %)) (in-s (:v %))))
                              (mapv #(assoc (select-keys % [:k :v :index :cluster :retired?])
                                            :in (cond-> [] (in-p (:v %)) (conj :promoted)
                                                           (in-s (:v %)) (conj :source)))))

                ; 9. a standby's reads before the promotion agree with S
                reads    (->> done (filter #(and (ok? %) (= :read (:f %)) (= :standby (:cluster %))
                                                 (< (:time %) (:inv promote)))))
                bad-read (vec (for [op    reads
                                    [k rs] (:value op)
                                    [o v]  rs
                                    :let   [s (by-v v)]
                                    :when  (or (nil? s) (not= k (:k s)) (not= :source (:cluster s))
                                               (and S (not= v (at s-logs k o))))]
                                {:index (:index op), :node (:node op), :k k, :offset o, :value v
                                 :source (when S (at s-logs k o))}))

                ; 10. a counter and its partition's records
                kv?      (boolean (:w12-kv test))
                kv-bad   (vec (for [[cluster C] [[:promoted P] [:source S]]
                                    :when (and kv? (:kv C))
                                    k     ks
                                    :let  [n (get (:kv C) k 0)
                                           r (count (get (:logs C) k []))]
                                    :when (not= n r)]
                                {:cluster cluster, :k k, :counter n, :records r}))
                kv-split (into (:kv-split P) (:kv-split S))

                ; Reported, not judged.
                refusals (->> sends (filter #(and (= :standby (:cluster %)) (= [:standby] (:error %)))))
                after    (->> refusals (filter #(< (:done promote) (inv %))))
                first-own (->> own (map :comp) (reduce min Long/MAX_VALUE))
                split    (into (:split P) (:split S))
                gaps     (merge (:gaps P) (:gaps S))
                judged   [early phantom dup bad-ack diverged late misplaced torn
                          own-lost src-lost refused bad-read split gaps kv-bad kv-split
                          (when-not disk-full? halted)
                          (if (= :planned mode) (when caught-up? planned-lost) older)]
                clean?   (every? empty? judged)]
            {:valid? (cond
                       (not clean?)                             false
                       (nil? S)                                 :unknown
                       (and (= :planned mode) (not caught-up?)) :unknown
                       ; Nothing of the source reached the standby, or the
                       ; promoted cluster took nothing: the run showed nothing.
                       (or (empty? survivors) (empty? own))     :unknown
                       ; --w12-kv: a counter read on each cluster.
                       (and kv? (not (and (:kv P) (:kv S))))    :unknown
                       :else                                    true)
             :mode                 mode
             :raft-groups          (long (:raft-groups test 1))
             :tenants              (tenant-count test)
             :seeded               (when (:standby-seed test)
                                     (select-keys (:value seed) [:node :ms :starts]))
             :grown                (when (:standby-seed test)
                                     (select-keys (:value grow) [:voters :promote]))
             :promote              (:value promote)
             :await-standby        (:value await)
             :link-before-stop     (:link-before (:value stop))
             :source-final-read?   (some? S)
             :nodes-of-one-cluster-disagree (take 16 split)
             :final-read-gaps      gaps
             :source-acked         (count acked)
             :replayed             (count (filter src? in-p))
             :replayed-per-partition prefix
             :survivor-per-group   (into (sorted-map) (map (fn [[g s]] [g (:v s)])) survivors)
             :own-acked            (count own)
             :own-records          (count (remove src? in-p))
             :lost-count           (count lost)
             :lost-over-ms         (when (and (seq lost) (:inv stop))
                                     (ms (- (:inv stop) (reduce min (map :comp lost)))))
             :standby-refusals     (count refusals)
             :standby-refusals-after-promote (count after)
             :last-refusal-after-promote-ms
             (when (seq after) (ms (- (reduce max (map :time after)) (:done promote))))
             :first-own-write-ms   (when (seq own) (ms (- first-own (:done promote))))
             :standby-reads-judged (count reads)
             :link-waiting-seen    waiting
             :halted               (take 8 halted)
             :standby-wrote-early  (take 16 early)
             :phantom              (take 16 phantom)
             :duplicate            (take 16 dup)
             :ack-offset-mismatch  (take 16 bad-ack)
             :diverged-count       (count diverged)
             :diverged             (take 16 diverged)
             :source-record-after-own (take 16 late)
             :standby-send-in-source (take 16 misplaced)
             :torn-transaction     (take 16 torn)
             :lost-before-a-survivor-count (count older)
             :lost-before-a-survivor (take 16 older)
             :planned-lost-count   (count planned-lost)
             :planned-lost         (take 16 planned-lost)
             :own-lost             (take 16 own-lost)
             :source-lost          (take 16 src-lost)
             :refused-but-present  (take 16 refused)
             :standby-read-diverged (take 16 bad-read)
             :kv-counted           (when kv? {:promoted (:kv P), :source (:kv S)})
             :kv-counter-not-records (take 16 kv-bad)
             :kv-nodes-disagree    (take 16 kv-split)}))))))

;; ---------------------------------------------------------------------------
;; Workload

(defn workload
  "W12: the standby follows, the failover happens once, the promoted cluster
  serves. opts: --standby-nodes (at least 1), --failover crash|crash-behind|
  planned, --failover-at seconds (default: half the time limit), --w12-kv,
  --tenants / --raft-groups, --standby-seed and --seed-at (default: half of
  --failover-at)."
  [opts]
  (let [state (atom {:phase :following})
        at    (long (or (:failover-at opts) (quot (long (:time-limit opts 300)) 2)))
        seed  (when (:standby-seed opts)
                (long (or (:seed-at opts) (quot at 2))))]
    (when (and seed (< (- at grow-after-s) seed))
      (throw (ex-info (str "--seed-at must come at least " grow-after-s
                           " s before --failover-at")
                      {:seed-at seed, :failover-at at})))
    (when (and (pos? (long (:tenants opts 0)))
               (< (long (:w12-partitions opts 8)) (long (:tenants opts 0))))
      (throw (ex-info "--w12-partitions must be at least --tenants: every tenant needs a partition"
                      {:partitions (:w12-partitions opts 8), :tenants (:tenants opts)})))
    (cond-> {:client          (map->Client {:state state})
             :checker         (checker)
             :generator       (generator opts)
             :final-generator (gen/each-thread {:f :final-read, :value nil})
             :nemesis-package (package state {:mode    (:failover opts :crash)
                                              :at      at
                                              :seed-at seed})}
      ; The counters run far above the default per-tenant KV rates.
      (:w12-kv opts) (assoc :db-env kv/db-env))))
