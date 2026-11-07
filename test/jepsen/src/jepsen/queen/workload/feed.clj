(ns jepsen.queen.workload.feed
  "W13: the tenant feed, POST /api/v1/feed: every push to a tenant's queues,
  in one order, read by position (the raft entry that wrote it).

  Writers push to partitions k0..k47 of the test queues (key k in queue
  jepsen-(k mod N)), every value unique in the test, transactionId k<k>-v<v>:

    :send :push   one POST /api/v1/push of one message
    :send :batch  one POST /api/v1/push of 2-4 messages to one partition
    :send :txn    one POST /api/v1/transaction of 2-4 pushes to distinct
                  partitions (and queues): one raft entry, so its messages
                  share one position, all or nothing

  Readers read the feed from \"earliest\" to the end, sending each answer's
  `next` back as the next `from`, with small pages (1 to 256 records) and
  short long polls. A third of the client threads write, the rest read: the
  first readers (one per node) read whole, the others one shard each ({2 0},
  {2 1}, {3 0}, {3 1}, {3 2}). A reader starts on its own node and moves to
  the next node when a read fails (a leader answers 421 unless
  QUEEN_FEED_ON_LEADER=true; a node that is down refuses), keeping its
  position: positions are the raft log's, the same on every node. With
  --feed-relay every read sends x-queen-feed-relay: 1, the header the proxy
  sends: a leader relays the read to a follower instead of answering 421, so
  readers read on every node, the leader too.

  The final phase pushes a fence (one /transaction to partitions that cover
  every shard) once every write of the main phase has completed, and each
  reader reads until it has read the fence's messages of its selection.

  Checks, over every answer of every reader:
    - every acknowledged message is read once by every reader that selects
      it; a message whose push definitely failed is never read; nothing is
      read that no push sent;
    - one order: a message has one position, one stamp and one partition on
      every reader; no reader misses a message of its selection that any
      reader read below the reader's last `next` (a hole), nor part of a
      transaction; a push acknowledged before another was invoked has the
      lower position;
    - an answer holds whole positions: every position it holds comes with
      all of that position's messages of the reader's selection;
    - every entry of an answer lies in [from, next), in position order, and
      `next` is not below `from`; each reader's answers chain (from = the
      previous next);
    - resolvedTs is safe: no message at or after an answer's `next` is
      stamped at or before its resolvedTs. It does not go back between two
      answers of one reader from one node with no node restart in between
      (judged); a reader that moves to a node behind, or a node that restarts
      behind its last answer, may give an older one (listed: a consumer keeps
      the newest). A relayed read is answered by a follower the leader picks,
      and the answer names none: with --feed-relay a step back is listed too;
    - stamps rise with position: every message of a position is stamped
      above every message of an earlier position;
    - a shard reader reads only its shard's partitions (FNV-1a 32 of the
      partition name, modulo count);
    - every reader reaches the fence (when the fence was acknowledged)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log])
  (:import (java.time Instant)
           (java.util Collections List)))

;; ---------------------------------------------------------------------------
;; Keys, shards, the fence

(def key-count
  "Partitions written: k0 .. k47."
  48)

(def shard-specs
  "[count index] of the shard readers, in reader order."
  [[2 0] [2 1] [3 0] [3 1] [3 2]])

(defn fnv1a32
  "FNV-1a, 32 bit, over the UTF-8 bytes of s: the broker's shard hash."
  [^String s]
  (reduce (fn [h b] (bit-and (* (bit-xor h (bit-and b 0xff)) 16777619) 0xffffffff))
          2166136261
          (.getBytes s "UTF-8")))

(defn shard-of
  [partition n]
  (mod (fnv1a32 partition) n))

(defn selects?
  "Does a reader of `shard` ([count index], or nil: the whole feed) read this
  partition?"
  [shard partition]
  (or (nil? shard)
      (and (string? partition)
           (= (long (second shard)) (long (shard-of partition (first shard)))))))

(def fence-partitions
  "Partitions fence-<i>, the fewest in order, such that every shard of
  shard-specs holds at least one."
  (loop [i 0, covered #{}, out []]
    (if (every? covered shard-specs)
      out
      (let [p    (str "fence-" i)
            hits (filter (fn [[n ix]] (= ix (shard-of p n))) shard-specs)]
        (if (some (complement covered) hits)
          (recur (inc i) (into covered hits) (conj out p))
          (recur (inc i) covered out))))))

(def fence-values
  "Fence partition -> its message's value: -1, -2, ..."
  (into (sorted-map) (map-indexed (fn [i p] [p (- (inc i))]) fence-partitions)))

(def fence-value?
  (set (vals fence-values)))

(defn fence-wanted
  "The fence values a reader of `shard` reads."
  [shard]
  (set (keep (fn [[p v]] (when (selects? shard p) v)) fence-values)))

(defn writers
  "Client threads 0 .. (writers test)-1 write; the others read."
  [test]
  (max 1 (quot (long (:concurrency test)) 3)))

(defn reader-shard
  "The shard of reader r: the first (count nodes) readers read whole."
  [test r]
  (let [n (count (:nodes test))]
    (when (>= r n)
      (nth shard-specs (mod (- r n) (count shard-specs))))))

(defn ts-us
  "Microseconds since the epoch of an ISO-8601 stamp, or nil."
  [s]
  (when (string? s)
    (try (let [i (Instant/parse s)]
           (+ (* 1000000 (.getEpochSecond i)) (quot (.getNano i) 1000)))
         (catch Exception _ nil))))

;; ---------------------------------------------------------------------------
;; Client

(defn- call
  ([client url body timeout-ms]
   (call client url body timeout-ms nil))
  ([client url body timeout-ms headers]
   (try (qh/request! (:http client) :post url body timeout-ms headers)
        (catch clojure.lang.ExceptionInfo e
          (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
            {:exception (ex-data e)}
            (throw e))))))

(def relay-header
  "With --feed-relay every feed read carries it: a leader relays the read to a
  follower instead of answering 421 (handlers/raft.rs relay_feed_from_leader)."
  {"x-queen-feed-relay" "1"})

(defn- failure
  "The completion type of a write that got no usable answer."
  [test r]
  (cond (= ::qh/refused (:type (:exception r))) :fail
        (:exception r)                           :info
        :else (log/push-failure test (:status r) (:body r))))

(defn- error-of
  [r]
  (if-let [e (:exception r)]
    [(keyword (name (:type e)))]
    [(:status r) (:body r)]))

(defn- item
  [test k v]
  {:queue         (log/queue-name test k)
   :partition     (log/partition-name k)
   :payload       v
   :transactionId (log/txn-id k v)})

(defn- push!
  "Modes :push and :batch: one POST /api/v1/push of the op's messages."
  [client test op]
  (let [mops (:value op)
        r    (call client (str (:base client) "/api/v1/push")
                   {:items (mapv (fn [[k v]] (item test k v)) mops)}
                   (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [items (when (sequential? (:body r)) (vec (:body r)))
            ok?   (fn [i] (contains? #{"queued" "duplicate"} (:status i)))
            all?  (= (count items) (count mops))]
        (cond
          (and all? (every? ok? items))
          (assoc op :type :ok)

          (and all? (not-any? ok? items))
          (assoc op :type :fail
                 :error [:refused (vec (distinct (map #(select-keys % [:status :error]) items)))])

          :else
          (assoc op :type :info, :error [:mixed items])))
      (assoc op :type (failure test r), :error (error-of r)))))

(defn- txn!
  "One POST /api/v1/transaction of one push per item: {:type :ok} on success,
  :fail on a rollback, :info on a rollback for a duplicate (only a second
  attempt of a bundle whose first attempt committed meets one: no value is
  pushed twice)."
  [client test items]
  (let [r (call client (str (:base client) "/api/v1/transaction")
                {:operations (mapv (fn [i] {:type "push", :items [i]}) items)}
                (:client-timeout-ms test))
        b (:body r)]
    (cond
      (and (= 200 (:status r)) (map? b) (true? (:success b)))
      {:type :ok}

      (= 200 (:status r))
      {:type  (if (re-find #"(?i)dup" (str (:reason b) " " (:error b))) :info :fail)
       :error [:rolled-back (when (map? b) (select-keys b [:reason :error]))]}

      :else
      {:type (failure test r), :error (error-of r)})))

(defn- fence!
  "The fence, retried by the final generator until :ok. A rollback for a
  duplicate means an earlier attempt committed: :ok, with :dup? true."
  [client test op]
  (let [q   (first (:queue-names test))
        res (txn! client test (mapv (fn [[p v]] {:queue q, :partition p, :payload v,
                                                 :transactionId p})
                                    fence-values))]
    (cond
      (= :ok (:type res))
      (assoc op :type :ok)

      (= :rolled-back (first (:error res)))
      (if (= :info (:type res))
        (assoc op :type :ok, :dup? true)
        (do (Thread/sleep 1000) (merge op res)))

      :else
      (do (Thread/sleep 1000) (merge op res)))))

(defn- entry
  "[position payload partition ts-us] of one feed entry."
  [e]
  [(:position e) (:payload e) (:partition e) (ts-us (:ts e))])

(defn- mismatch
  "A feed entry whose queue, payload or transactionId does not agree with its
  partition, or nil."
  [test e]
  (let [p  (str (:partition e))
        v  (:payload e)
        fv (get fence-values p)
        [q txn ok-v?]
        (if fv
          [(first (:queue-names test)) p (= fv v)]
          (when-let [[_ k] (re-matches #"k(\d+)" p)]
            (let [k (parse-long k)]
              [(log/queue-name test k) (when (integer? v) (log/txn-id k v)) (integer? v)])))]
    (when-not (and q ok-v? (= q (:queue e)) (= txn (:transactionId e)))
      (select-keys e [:position :queue :partition :payload :transactionId]))))

(defn- reader!
  "This client's reader, made on its first read: its number (its thread less
  the writers), its shard, its position (nil: earliest), the number of its
  next answer, the node it reads from and the fence values it has read."
  [client test op]
  (or @(:reader client)
      (let [r (- (mod (long (:process op)) (long (:concurrency test))) (writers test))]
        (reset! (:reader client)
                {:id     r
                 :shard  (reader-shard test r)
                 :cursor nil
                 :seq    0
                 :node-i (max 0 (.indexOf ^List (vec (:nodes test)) (:node client)))
                 :fence  #{}}))))

(def page-sizes [1 1 2 3 5 8 64 256])
(def waits-ms [0 0 100 500 2000])

(defn- feed!
  "One read from the reader's position. Reads have no effect: every failure
  is :fail, the position stays, and the reader moves to the next node."
  [client test op]
  (let [st     (reader! client test op)
        nodes  (vec (:nodes test))
        node   (nth nodes (:node-i st))
        final? (boolean (:final? op))
        m      (if final? 1000 (rand/nth page-sizes))
        w      (if final? 1000 (rand/nth waits-ms))
        shard  (:shard st)
        relay? (boolean (:feed-relay test))
        body   (cond-> {:from       (or (:cursor st) "earliest")
                        :queues     ["jepsen-*"]
                        :maxRecords m
                        :maxWaitMs  w}
                 shard (assoc :shard {:count (first shard), :index (second shard)}))
        r      (call client (str (qh/base-url node) "/api/v1/feed") body
                     (+ w (:client-timeout-ms test))
                     (when relay? relay-header))
        b      (:body r)
        op     (cond-> (assoc op :reader (:id st), :seq (:seq st), :node node, :shard shard,
                                 :from (:cursor st), :max m, :wait w)
                 relay? (assoc :relay? true))]
    (if (and (= 200 (:status r)) (map? b) (integer? (:next b)))
      (let [raw   (:entries b)
            es    (mapv entry raw)
            bad   (vec (keep #(mismatch test %) raw))
            fence (into (:fence st) (keep (fn [[_ v p]] (when (get fence-values p) v)) es))
            done? (and final? (every? fence (fence-wanted shard)))
            ; A final read caught up on a node that has not applied the
            ; fence: after 5 in a row (a node out of the cluster stays
            ; there), read on from the next node.
            stale (if (and final? (not done?) (true? (:caughtUp b)) (empty? es))
                    (inc (:stale st 0))
                    0)]
        (swap! (:reader client) assoc :cursor (:next b), :seq (inc (:seq st)), :fence fence
               :stale (if (<= 5 stale) 0 stale)
               :node-i (if (<= 5 stale) (mod (inc (:node-i st)) (count nodes)) (:node-i st)))
        (cond-> (assoc op :type :ok, :value es, :next (:next b), :applied (:applied b)
                          :caught-up? (:caughtUp b), :resolved (ts-us (:resolvedTs b)))
          (seq bad) (assoc :mismatch bad)
          done?     (assoc :done? true)))
      (do (swap! (:reader client) update :node-i #(mod (inc %) (count nodes)))
          (assoc op :type :fail
                 :error (if (= 421 (:status r)) [:feed-on-leader] (error-of r)))))))

(defrecord Client [node base http reader]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client), :reader (atom nil)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :send  (if (= :txn (:mode op))
               (merge op (txn! this test (mapv (fn [[k v]] (item test k v)) (:value op))))
               (push! this test op))
      :fence (fence! this test op)
      :feed  (feed! this test op)))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Generators

(defn generator
  "Writers: pushes, batches and transactions of fresh values to random keys.
  Readers: reads."
  [opts]
  (let [next-v (atom 0)
        v!     #(swap! next-v inc)
        send   (fn []
                 (let [mode (rand/nth [:push :push :push :batch :txn :txn])]
                   {:f     :send
                    :mode  mode
                    :value (case mode
                             :push  [[(rand/long key-count) (v!)]]
                             :batch (let [k (rand/long key-count)]
                                      (vec (repeatedly (+ 2 (rand/long 3)) (fn [] [k (v!)]))))
                             :txn   (->> (repeatedly #(rand/long key-count))
                                         distinct
                                         (take (+ 2 (rand/long 3)))
                                         (mapv (fn [k] [k (v!)]))))}))]
    (gen/reserve (writers opts) send
                 (repeat {:f :feed}))))

(defrecord Fence [state]
  ; One fence at a time until one is :ok (state :idle, :busy, :done).
  gen/Generator
  (op [this test ctx]
    (case state
      :done nil
      :busy [:pending this]
      :idle (let [op (gen/fill-in-op {:f :fence, :value (vec (vals fence-values))} ctx)]
              (if (= :pending op) [:pending this] [op (Fence. :busy)]))))

  (update [this test ctx event]
    (if (and (= :fence (:f event)) (not= :invoke (:type event)))
      (Fence. (if (= :ok (:type event)) :done :idle))
      this)))

(defrecord Drain [done?]
  ; One reader's final reads: until an answer completes its fence.
  gen/Generator
  (op [this test ctx]
    (when-not done?
      (let [op (gen/fill-in-op {:f :feed, :final? true} ctx)]
        (if (= :pending op) [:pending this] [op this]))))

  (update [this test ctx event]
    (if (and (= :feed (:f event)) (= :ok (:type event)) (:done? event))
      (Drain. true)
      this)))

(defn final-generator
  [opts]
  (gen/reserve (writers opts) (->Fence :idle)
               (gen/each-thread (->Drain false))))

;; ---------------------------------------------------------------------------
;; Checker

(defn- restart-event?
  "A nemesis event that may bring a node back behind what it served."
  [op]
  (and (= :nemesis (:process op))
       (keyword? (:f op))
       (boolean (re-find #"kill|restart|^start$|member|join|leave|remove|add|corrupt|snap|wipe"
                         (name (:f op))))))

(defn- answer-anomalies
  "What is wrong with one answer by itself."
  [op]
  (let [from  (or (:from op) 1)
        nxt   (:next op)
        es    (:value op)
        ps    (map first es)
        shard (:shard op)
        where {:reader (:reader op), :seq (:seq op), :node (:node op)}]
    (cond-> []
      (not (every? integer? ps))
      (conj (assoc where :type :bad-position, :positions (vec (take 8 ps))))

      (< nxt from)
      (conj (assoc where :type :next-before-from, :from from, :next nxt))

      (and (every? integer? ps) (some #(or (< % from) (>= % nxt)) ps))
      (conj (assoc where :type :entry-out-of-range, :from from, :next nxt,
                   :positions (vec (take 8 ps))))

      (and (every? integer? ps) (seq ps) (not (apply <= ps)))
      (conj (assoc where :type :out-of-order, :positions (vec (take 16 ps))))

      (some (fn [[_ _ part]] (not (selects? shard part))) es)
      (conj (assoc where :type :wrong-shard, :shard shard
                   :partitions (vec (take 8 (distinct (keep (fn [[_ _ part]]
                                                              (when-not (selects? shard part) part))
                                                            es))))))

      (some (fn [e] (nil? (nth e 3))) es)
      (conj (assoc where :type :no-stamp))

      (seq (:mismatch op))
      (conj (assoc where :type :mismatch, :entries (vec (take 4 (:mismatch op))))))))

(defn- chain-anomalies
  [r rs]
  (let [a (first rs)]
    (cond-> []
      (some? (:from a)) (conj {:type :first-not-earliest, :reader r, :from (:from a)})
      (not= 0 (:seq a)) (conj {:type :first-seq, :reader r, :seq (:seq a)})
      true (into (keep (fn [[a b]]
                         (when (or (not= (:next a) (:from b)) (not= (inc (:seq a)) (:seq b)))
                           {:type :chain-gap, :reader r, :seq [(:seq a) (:seq b)]
                            :next (:next a), :from (:from b)}))
                       (partition 2 1 rs))))))

(defn- index-of-first-at-or-after
  "The index of the first position in sorted `ps` at or after n."
  [ps n]
  (let [i (Collections/binarySearch ^List ps (long n))]
    (if (neg? i) (- (inc i)) i)))

(defn feed-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops      (h/client-ops history)
            done?    #(not= :invoke (:type %))
            sends    (->> ops (h/filter #(and (= :send (:f %)) (done? %))) vec)
            fences   (->> ops (h/filter #(and (= :fence (:f %)) (done? %))) vec)
            feeds    (->> ops (h/filter #(and (= :feed (:f %)) (done? %))) vec)
            reads    (filterv #(= :ok (:type %)) feeds)
            fence-ok? (boolean (some #(= :ok (:type %)) fences))
            ; value -> {:type :k :mode}
            sent     (persistent!
                       (reduce (fn [m op]
                                 (reduce (fn [m [k v]]
                                           (assoc! m v {:type (:type op), :k k, :mode (:mode op)}))
                                         m (:value op)))
                               (transient {}) sends))
            ; Everything read, from every answer.
            {pos-of :pos ts-of :ts part-of :part at :at}
            (reduce (fn [acc op]
                      (reduce (fn [acc [p v part ts]]
                                (-> acc
                                    (update-in [:pos v] (fnil conj #{}) p)
                                    (update-in [:ts v] (fnil conj #{}) ts)
                                    (update-in [:part v] (fnil conj #{}) part)
                                    (update-in [:at p] (fnil conj #{}) v)))
                              acc (:value op)))
                    {:pos {}, :ts {}, :part {}, :at {}}
                    reads)
            pos1     (fn [v] (when-let [ps (get pos-of v)] (reduce min ps)))
            part1    (fn [v] (first (get part-of v)))
            ; Readers, their answers in order.
            all-readers (set (keep :reader feeds))
            by-reader (into (sorted-map)
                            (map (fn [[r rs]] [r (vec (sort-by :seq rs))]))
                            (group-by :reader reads))
            readers  (into (sorted-map)
                           (map (fn [[r rs]]
                                  [r {:shard     (:shard (first rs))
                                      :view      (reduce (fn [m op]
                                                           (reduce (fn [m [p v]]
                                                                     (update m v (fnil conj []) p))
                                                                   m (:value op)))
                                                         {} rs)
                                      :last-next (:next (peek rs))
                                      :finished? (boolean (some :done? rs))
                                      :answers   (count rs)}]))
                           by-reader)
            finished (set (keep (fn [[r i]] (when (:finished? i) r)) readers))
            ; Answers by themselves, and each reader's chain.
            answer-bad (vec (mapcat answer-anomalies reads))
            chain-bad  (vec (mapcat (fn [[r rs]] (chain-anomalies r rs)) by-reader))
            dups     (vec (for [[r {:keys [view]}] readers
                                [v ps] view
                                :when (< 1 (count ps))]
                            {:reader r, :value v, :positions ps}))
            moved    (vec (for [[v ps] pos-of :when (< 1 (count ps))]
                            {:value v, :positions (sort ps)}))
            restamped (vec (for [[v ts] ts-of :when (< 1 (count ts))]
                             {:value v, :stamps (sort (remove nil? ts))}))
            reparted (vec (for [[v ps] part-of :when (< 1 (count ps))]
                            {:value v, :partitions (sort ps)}))
            ; Transactions: one position, all or nothing.
            bundles  (vec (keep (fn [op]
                                  (when (and (= :txn (:mode op)) (#{:ok :info} (:type op)))
                                    (mapv second (:value op))))
                                sends))
            split-bundles (vec (for [vs bundles
                                     :let [ps (distinct (mapcat #(get pos-of %) vs))]
                                     :when (< 1 (count ps))]
                                 {:values vs, :positions (sort ps)}))
            implied  (for [vs bundles
                           :let [seen (filter pos-of vs)]
                           :when (seq seen)
                           :let [p (reduce min (map pos1 seen))]
                           v vs
                           :when (not (pos-of v))]
                       [p v (log/partition-name (:k (sent v))) true])
            ; Every message any reader read (at its lowest position), and the
            ; unread parts of transactions others read: what a reader that
            ; read past it must have read.
            expected (sort-by first (concat (map (fn [[v _]] [(pos1 v) v (part1 v) false]) pos-of)
                                            implied))
            holes    (vec (for [[r {:keys [shard view last-next]}] readers
                                [p v part implied?] expected
                                :while (< p last-next)
                                :when (and (selects? shard part) (not (contains? view v)))]
                            {:reader r, :position p, :value v, :partition part
                             :type (if implied? :partial-transaction :hole)}))
            ; Whole positions in every answer.
            splits   (vec (for [op reads
                                :let [shard (:shard op)]
                                [p group] (group-by first (:value op))
                                :let [got  (set (map second group))
                                      want (set (filter #(selects? shard (part1 %)) (get at p)))]
                                :when (not= got want)]
                            {:reader (:reader op), :seq (:seq op), :position p
                             :got (sort got), :want (sort want)}))
            ; Acknowledged, failed, never sent.
            lost     (vec (for [[v s] (sort-by key sent)
                                :when (= :ok (:type s))
                                :let [part    (log/partition-name (:k s))
                                      missing (vec (keep (fn [[r {:keys [shard view finished?]}]]
                                                           (when (and finished? (selects? shard part)
                                                                      (not (contains? view v)))
                                                             r))
                                                         readers))]
                                :when (seq missing)]
                            {:value v, :partition part, :readers missing}))
            failed-read (vec (sort (filter #(= :fail (:type (sent %))) (keys pos-of))))
            never-sent  (vec (sort (remove #(or (sent %) (fence-value? %)) (keys pos-of))))
            ; Real-time order: a push acknowledged before another was invoked
            ; has the lower position. A fence attempt that met a duplicate
            ; wrote nothing of its own: only its acknowledgement counts.
            sendlike (concat (map (fn [op] [op (mapv second (:value op))]) sends)
                             (map (fn [op] [op (vec (vals fence-values))]) fences))
            events   (->> sendlike
                          (mapcat (fn [[op vs]]
                                    (let [ps (keep pos1 vs)]
                                      (when (seq ps)
                                        (cond-> []
                                          (not (:dup? op))
                                          (conj [(:time (h/invocation history op)) 0
                                                 (reduce min ps) (:index op)])
                                          (= :ok (:type op))
                                          (conj [(:time op) 1 (reduce max ps) (:index op)]))))))
                          (sort-by (juxt first second)))
            realtime (loop [es events, top 0, top-op nil, bad []]
                       (if-let [[_ kind p i] (first es)]
                         (if (= 0 kind)
                           (recur (next es) top top-op
                                  (if (<= p top)
                                    (conj bad {:op i, :position p, :acked-before-op top-op
                                               :acked-position top})
                                    bad))
                           (if (> p top)
                             (recur (next es) p i bad)
                             (recur (next es) top top-op bad)))
                         bad))
            ; Stamps against positions.
            ps       (vec (sort (keys at)))
            stamps   (fn [p] (keep #(some->> (get ts-of %) (remove nil?) seq) (get at p)))
            lo       (mapv (fn [p] (reduce min Long/MAX_VALUE (apply concat (stamps p)))) ps)
            hi       (mapv (fn [p] (reduce max Long/MIN_VALUE (apply concat (stamps p)))) ps)
            suffix   (if (seq lo) (vec (reverse (reductions min (reverse lo)))) [])
            not-rising (vec (for [i (range (dec (count ps)))
                                  :when (not (< (nth hi i) (nth lo (inc i))))]
                              {:position (nth ps i), :highest-stamp (nth hi i)
                               :next-position (nth ps (inc i)), :lowest-stamp (nth lo (inc i))}))
            unsafe   (vec (for [op reads
                                :let [res (:resolved op)
                                      i   (index-of-first-at-or-after ps (:next op))]
                                :when (and res (< i (count ps)) (<= (nth suffix i) res))
                                :let [j (first (filter #(<= (nth lo %) res) (range i (count ps))))]]
                            {:reader (:reader op), :seq (:seq op), :node (:node op)
                             :next (:next op), :resolved res
                             :position (nth ps j), :stamp (nth lo j)}))
            restarts (vec (keep #(when (restart-event? %) (:time %)) history))
            ; A relayed read is answered by a follower the leader picked, and
            ; nothing names it: two such answers may come from two nodes.
            backs    (vec (for [[r rs] by-reader
                                [a b] (partition 2 1 rs)
                                :when (and (:resolved a) (:resolved b)
                                           (< (:resolved b) (:resolved a)))
                                :let [relay? (boolean (or (:relay? a) (:relay? b)))]]
                            (cond-> {:reader r, :nodes [(:node a) (:node b)], :seq [(:seq a) (:seq b)]
                                     :resolved [(:resolved a) (:resolved b)]
                                     :judged? (and (= (:node a) (:node b))
                                                   (not relay?)
                                                   (not (some #(<= (:time a) % (:time b)) restarts)))}
                              relay? (assoc :relay? true))))
            back-judged (filterv :judged? backs)
            unfinished (vec (sort (remove finished all-readers)))
            violations {:answer-anomalies   answer-bad
                        :chain-anomalies    chain-bad
                        :read-twice         dups
                        :moved              moved
                        :restamped          restamped
                        :repartitioned      reparted
                        :split-transactions split-bundles
                        :holes              holes
                        :split-positions    splits
                        :lost               lost
                        :failed-but-read    failed-read
                        :never-sent         never-sent
                        :realtime-order     realtime
                        :stamps-not-rising  not-rising
                        :resolved-unsafe    unsafe
                        :resolved-back      back-judged
                        :unfinished         (if fence-ok? unfinished [])}
            bad?     (some (comp seq val) violations)]
        (merge
          {:valid?            (cond bad?                 false
                                    (empty? reads)       :unknown
                                    (not fence-ok?)      :unknown
                                    :else                true)
           :fence-ok?         fence-ok?
           :fence-position    (some->> (keep pos1 (vals fence-values)) seq (reduce min))
           :sends             (frequencies (map :type sends))
           :values            {:ok   (count (filter #(= :ok (:type (val %))) sent))
                               :info (count (filter #(= :info (:type (val %))) sent))
                               :fail (count (filter #(= :fail (:type (val %))) sent))
                               :read (count pos-of)}
           :positions         (count ps)
           :answers           (count reads)
           :answers-failed    (count (remove #(= :ok (:type %)) feeds))
           :answers-caught-up (count (filter :caught-up? reads))
           :answers-by-node   (frequencies (map :node reads))
           :readers           (into (sorted-map)
                                    (map (fn [[r i]] [r (select-keys i [:shard :answers :last-next
                                                                        :finished?])]))
                                    readers)
           :unfinished-readers unfinished
           :resolved-back-listed (count (remove :judged? backs))
           :resolved-back-relayed (count (filter :relay? backs))
           :resolved-back-relayed-sample (vec (take 4 (filter :relay? backs)))}
          (into {} (map (fn [[k v]] [k (vec (take 16 v))])) violations)
          (into {} (map (fn [[k v]] [(keyword (str (name k) "-count")) (count v)])) violations))))))

(defn workload
  [opts]
  (when (< (long (:concurrency opts)) 2)
    (throw (ex-info "--workload feed needs --concurrency 2 or more (writers and readers)"
                    {:concurrency (:concurrency opts)})))
  {:client          (map->Client {})
   :checker         (feed-checker)
   :generator       (generator opts)
   :final-generator (final-generator opts)})
