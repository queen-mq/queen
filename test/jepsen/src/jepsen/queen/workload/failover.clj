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
  has a place to compare. Acks, leases and KV rows across a promotion are not
  checked here.

    :send [[k v] ...]  one pair: POST /api/v1/push of v to partition f<k>,
                       transactionId \"f<k>-v<v>\". Several pairs (--txn): ONE
                       POST /api/v1/transaction, one log entry. A thread of
                       the source sends until the failover begins and then
                       stops for good (two clusters that both serve are a
                       documented hazard, not what is tested). A thread of the
                       standby sends for the whole test: refused 503 `standby`
                       until the promotion, served after it, as a client that
                       keeps retrying is.
    :read              the records this thread has not read yet, of every
                       partition, from its own node. On a standby, before the
                       promotion, each must be the source's record at that
                       offset.
    final phase        :final-read, every partition from offset 0, on every
                       node of both clusters (the source is started again
                       for it).

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
                        to end), :await-standby (the standby's leader has
                        read all the source applied), :stop-source, :promote.

  Other faults (--nemesis kill, partition, pause, ...) run on all the nodes of
  both clusters for the whole test: while the standby follows, and on the
  promoted cluster afterwards. The failover's own steps run one after the
  other on the nemesis's thread, so no new fault begins inside them, and the
  heal steps undo the ones in force: an operator who promotes a standby
  starts its nodes first, and one who plans a switch does it on a healthy
  pair. A crash still finds the standby wherever the faults left it, as far
  behind as they made it.

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
       INVOKED AFTER that answer is in P (the source logged it later, so a
       prefix that has the later one has the earlier one);
    7. every send the promoted cluster acknowledged is in P; and every send
       the source acknowledged is in S (the source's own durability);
    8. a send that was refused is in neither;
    9. a standby's read before the promotion agrees with S.

  Reported and not judged: how many acknowledged sends a crash lost and over
  how long, and the refusals `standby` answered AFTER the promotion had been
  answered (the endpoint says whoever asked can write next)."
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
            [jepsen.queen [db :as qdb]
                          [http :as qh]
                          [kv :as kv]]
            [jepsen.queen.workload.log :as log]))

(defn partitions [test] (:w12-partitions test 8))
(defn- queue [test] (first (:queue-names test)))
(defn- partition-name [k] (str "f" k))
(defn- txn-id [k v] (str "f" k "-v" v))

(defn- parse-partition
  "k from \"f<k>\", or nil."
  [s]
  (when-let [[_ k] (re-matches #"f(\d+)" (str s))]
    (parse-long k)))

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
  [client test pair]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/push")
                       {:items [(item test pair)]} (:client-timeout-ms test))
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
  in its answer."
  [client test pairs]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/transaction")
                       {:operations (mapv (fn [p] {:type "push", :items [(item test p)]}) pairs)}
                       (:client-timeout-ms test))
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
      (merge op (if (= 1 (count pairs))
                  (push! client test (first pairs))
                  (transact! client test pairs))))))

(defn- fetch!
  "One fetch of every partition from `positions` ({k offset}): {k [[offset
  payload] ...]} for the partitions that answered records."
  [client test positions]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                       {:entries (mapv (fn [k] {:queue (queue test), :partition (partition-name k)
                                                :offset (get positions k 0)})
                                       (range (partitions test)))}
                       (:client-timeout-ms test))]
    (if-not (= 200 (:status r))
      (throw (ex-info "fetch failed" {:status (:status r), :body (:body r)}))
      (into (sorted-map)
            (keep (fn [e]
                    (when-let [k (parse-partition (:partition e))]
                      (when (seq (:records e))
                        [k (mapv (juxt :offset :payload) (:records e))]))))
            (:entries (:body r))))))

(defn- read!
  [client test op]
  (let [recs (fetch! client test @(:positions client))]
    (swap! (:positions client)
           (fn [pos] (reduce (fn [pos [k rs]] (assoc pos k (inc (long (first (peek rs))))))
                             pos recs)))
    (assoc op :type :ok, :value recs)))

(defn- final-read!
  "Every partition from offset 0: {k [payload ...]}, and the partitions whose
  offsets were not 0, 1, 2, ... as answered."
  [client test op]
  (loop [positions {}, logs (sorted-map), gaps (sorted-map)]
    (let [recs (fetch! client test positions)]
      (if (empty? recs)
        (assoc op :type :ok, :value logs, :gaps gaps)
        (recur (reduce (fn [pos [k rs]] (assoc pos k (inc (long (first (peek rs)))))) positions recs)
               (reduce (fn [logs [k rs]] (update logs k (fnil into []) (map second rs))) logs recs)
               (reduce (fn [gaps [k rs]]
                         (let [from (get positions k 0)]
                           (if (= (map first rs) (range from (+ from (count rs))))
                             gaps
                             (assoc gaps k {:requested from, :offsets (mapv first rs)}))))
                       gaps recs))))))

(defrecord Client [state node base http cluster positions]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)
           :cluster (if (qdb/standby? test node) :standby :source)
           :positions (atom {})))

  (setup! [this test])

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
              (= "fetch failed" (.getMessage e))
              (assoc op :type :fail, :error [:fetch (ex-data e)])

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
  partitions."
  [opts]
  (let [n    (long (:w12-partitions opts 8))
        v    (atom 0)
        one  (fn [] {:f :send, :value [[(rand/long n) (swap! v inc)]]})
        many (fn [] {:f :send
                     :value (mapv (fn [k] [k (swap! v inc)])
                                  (take (min n (+ 2 (rand/long 2))) (rand/shuffle (range n))))})
        read (fn [] {:f :read, :value nil})]
    (gen/mix (concat (if (:txn-sends? opts)
                       (concat (repeat 7 many) (repeat 2 one))
                       (repeat 9 one))
                     [read]))))

;; ---------------------------------------------------------------------------
;; The failover (a nemesis package)

(def ^:private promote-timeout-ms 120000)
(def ^:private caught-up-timeout-ms 120000)
(def ^:private behind-s
  "--failover crash-behind: how long the standby is cut off from its source
  before the source dies."
  10)

(defn- source-applied
  "The applied index the source's leader reports, or nil without a leader."
  [http test]
  (->> (qdb/source-nodes test)
       (util/real-pmap (fn [n] (:raft (qh/health http n 2000))))
       (filter #(= "leader" (:role %)))
       (keep :applied)
       first))

(defn- link-brief
  "What matters of a standby leader's link status."
  [[node st]]
  (when st
    {:node     node
     :role     (:role st)
     :position (:index (:position st))
     :follower (select-keys (:follower st) [:state :source :scanned :sourceApplied
                                            :lagEntries :lagMs :lastAnswerMs :error])}))

(defn- await-standby!
  "Planned switch, step 2: the source's clients are silent; wait until the
  standby's leader follows, has nothing left to read and has read at least
  what the source's leader had applied when asked, three looks in a row."
  [http test]
  (let [t0 (System/currentTimeMillis)]
    (loop [streak 0, last-seen nil]
      (let [applied (source-applied http test)
            st      (qdb/standby-leader-status http test)
            f       (:follower (second st))
            ok?     (and applied
                         (= "following" (:state f))
                         (= 0 (:lagEntries f))
                         (<= (long applied) (long (:scanned f 0))))
            streak  (if ok? (inc streak) 0)
            seen    {:source-applied applied, :standby (link-brief st)}
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
  (try (let [r (qh/request! http :get (str (qh/base-url node) "/metrics/prometheus") nil 2000)]
         (when (string? (:body r))
           (->> (str/split-lines (:body r))
                (filter #(re-find #"^queen_(raft_(inflight|index|proposals_total|plan_deferred|apply_channel_depth)|link_)" %))
                (take 40)
                vec)))
       (catch clojure.lang.ExceptionInfo _ nil)))

(defn- standby-view
  "Every standby node's own view of its raft group and of the link: what to
  read when a promotion does not go through."
  [http test]
  (into (sorted-map)
        (util/real-pmap
          (fn [n]
            (let [st (qdb/link-status http n 2000)]
              [n {:raft (select-keys (:raft (qh/health http n 2000))
                                     [:role :leader :term :applied :commit :lag])
                  :link {:role     (:role st)
                         :leader   (:leader st)
                         :position (:index (:position st))
                         :follower (select-keys (:follower st) [:state :lagEntries :error])}
                  :gauges (gauges http n)}]))
          (qdb/standby-nodes test))))

(declare heal!)

(defn- promote!
  "POST /api/v1/system/link/promote on the standby's nodes in turn until one
  answers 200: the standby's leader may be dead, paused or cut off right now.
  A promotion that needed more than one request says what each node answered
  (the first six, then how many of each kind) and what the standby's nodes
  showed, at the first refusal and every 20 s after it.

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
                                     (str (qh/base-url node) "/api/v1/system/link/promote")
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
                  :role (:role (:body r)), :position (:index (:position (:body r)))}
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

(defn nemesis
  "  :heal-all       every node of both clusters is up and reachable.
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
      (fs [_] #{:heal-all :heal-standby :cut-link :quiesce :await-standby :stop-source
                :promote :start-source})

      n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (assoc op :value
               (case (:f op)
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
                    :link     (link-brief (qdb/standby-leader-status @http test))})

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
                 (let [before (link-brief (qdb/standby-leader-status @http test))]
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
  `mode` is :crash or :planned; it begins `at` seconds into the test. The
  first op carries its own time, so the other packages' faults go on until
  then and the nemesis is not kept waiting."
  [state {:keys [mode at]}]
  (let [op    (fn [f] {:type :info, :f f, :value nil})
        steps (case mode
                :crash        [:stop-source :heal-standby :promote]
                :crash-behind [:cut-link :stop-source :heal-standby :promote]
                :planned      [:heal-all :quiesce :await-standby :stop-source :promote])]
    {:nemesis         (nemesis state)
     :generator       (cons (assoc (op (first steps)) :time (long (* 1e9 (double at))))
                            (map op (rest steps)))
     :final-generator (op :start-source)
     :perf            #{{:name  "failover"
                         :fs    #{:heal-all :heal-standby :cut-link :quiesce :await-standby
                                  :stop-source :promote}
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
            promoted? (true? (:promoted? (:value promote)))
            finals   (filterv #(and (ok? %) (= :final-read (:f %))) done)
            ; A fetch is a node's own read, so a node may be a little behind:
            ; a cluster's log is the longest any of its nodes read, and every
            ; other node's must be a prefix of that.
            final-of (fn [cluster]
                       (let [fs (filter #(= cluster (:cluster %)) finals)]
                         (when (seq fs)
                           (let [logs (apply merge-with
                                             (fn [a b] (if (< (count a) (count b)) b a))
                                             (map :value fs))]
                             {:logs  logs
                              :split (vec (for [f      fs
                                                [k vs] (:value f)
                                                :let   [full (get logs k)]
                                                :when  (not= vs (subvec full 0 (count vs)))]
                                            {:cluster cluster, :node (:node f), :k k
                                             :first-difference
                                             (first (keep-indexed (fn [o [a b]] (when (not= a b) o))
                                                                  (map vector vs full)))}))
                              :gaps  (into {} (keep (fn [f] (when (seq (:gaps f)) [(:node f) (:gaps f)]))) fs)}))))
            P        (final-of :standby)
            S        (final-of :source)]
        (cond
          (not promoted?)
          {:valid? :unknown, :error "the standby was never promoted", :promote (:value promote)}

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

                ; 5 and 6. what the promotion lost
                acked    (->> (vals by-v) (filter #(and (= :source (:cluster %)) (= :ok (:type %)))))
                lost     (remove (comp in-p :v) acked)
                survivor (->> (vals by-v)
                              (filter #(and (= :source (:cluster %)) (in-p (:v %))))
                              (sort-by :inv)
                              last)
                older    (when survivor
                           (->> lost
                                (filter #(< (:comp %) (:inv survivor)))
                                (mapv (fn [l] {:k (:k l), :value (:v l), :index (:index l)
                                               :acked-ms (ms (:comp l))
                                               :survivor (select-keys survivor [:k :v :index])
                                               :survivor-invoked-ms (ms (:inv survivor))}))))
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

                ; Reported, not judged.
                refusals (->> sends (filter #(and (= :standby (:cluster %)) (= [:standby] (:error %)))))
                after    (->> refusals (filter #(< (:done promote) (inv %))))
                first-own (->> own (map :comp) (reduce min Long/MAX_VALUE))
                split    (into (:split P) (:split S))
                gaps     (merge (:gaps P) (:gaps S))
                judged   [early phantom dup bad-ack diverged late misplaced torn
                          own-lost src-lost refused bad-read split gaps
                          (if (= :planned mode) (when caught-up? planned-lost) older)]
                clean?   (every? empty? judged)]
            {:valid? (cond
                       (not clean?)                             false
                       (nil? S)                                 :unknown
                       (and (= :planned mode) (not caught-up?)) :unknown
                       ; Nothing of the source reached the standby, or the
                       ; promoted cluster took nothing: the run showed nothing.
                       (or (nil? survivor) (empty? own))        :unknown
                       :else                                    true)
             :mode                 mode
             :promote              (:value promote)
             :await-standby        (:value await)
             :link-before-stop     (:link-before (:value stop))
             :source-final-read?   (some? S)
             :nodes-of-one-cluster-disagree (take 16 split)
             :final-read-gaps      gaps
             :source-acked         (count acked)
             :replayed             (count (filter src? in-p))
             :replayed-per-partition prefix
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
             :standby-read-diverged (take 16 bad-read)}))))))

;; ---------------------------------------------------------------------------
;; Workload

(defn workload
  "W12: the standby follows, the failover happens once, the promoted cluster
  serves. opts: --standby-nodes (at least 1), --failover crash|planned,
  --failover-at seconds (default: half the time limit)."
  [opts]
  (let [state (atom {:phase :following})]
    {:client          (map->Client {:state state})
     :checker         (checker)
     :generator       (generator opts)
     :final-generator (gen/each-thread {:f :final-read, :value nil})
     :nemesis-package (package state {:mode (:failover opts :crash)
                                      :at   (or (:failover-at opts)
                                                (quot (long (:time-limit opts 300)) 2))})}))
