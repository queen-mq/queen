(ns jepsen.queen.workload.elle
  "W4 --workload elle: Elle rw-register transactions as ONE KV batch each
  (POST /api/v1/kv): a getMany of the keys the transaction reads, then a put
  of each key it writes (forever:true), checked at strict serializability.

  Queen renders a batch's reads right after the batch's own writes
  (rsm/facade/real/kv.rs, rsm/kv_reads.rs), so a read of a key the same batch
  writes would see that write whatever its position. Transactions are
  therefore normalised before they are sent: reads first, then writes; one
  write per key (the last); no read of a key the transaction writes. Elle sees
  exactly the normalised transaction.

  A read-only batch waits for the read index and reads at an entry boundary; a
  batch with writes is one raft entry, its reads rendered at its own position
  in the log."
  (:require [clojure.tools.logging :refer [info warn]]
            [elle.graph]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.queen [http :as qh]
                          [kv :as kv]])
  (:import (io.lacuna.bifurcan ISet)
           (java.util.function BinaryOperator)))

(def ^:private non-mutating-union-bset
  "elle.graph/union-bset without the corruption. Elle 0.2.7 runs on bifurcan
  0.2.0-alpha7, whose Set.union of two forked sets merges under the set's own
  editor - null for every forked collection - so nodes an earlier forked union
  left behind (also editor null) count as owned and are written in place.
  elle.rw-register's ext-key-graph shares one downstream set among many ops
  (downstream-ops-by-ext-key-transitive!, rw_register.clj:326), so a later
  union adds ops to the downstream sets of ops already done: false
  :linearizable-keys version edges and spurious :cyclic-versions (P8: 6 of 8
  W4 runs; every reported cycle held an edge no realtime order justifies). A
  union through a fresh linear copy runs under a brand-new editor that owns no
  existing node, so neither argument is touched. Upstream: elle README warning
  (244b151, unreleased), bifurcan fix 33d7030 (master, unreleased; 0.2.0-rc1
  still has the bug). Drop this once elle ships on a fixed bifurcan."
  (reify BinaryOperator
    (apply [_ a b]
      (cond (nil? a) b
            (nil? b) a
            true     (.forked (.union (.linear ^ISet a) ^ISet b))))))

(alter-var-root #'elle.graph/union-bset (constantly non-mutating-union-bset))

(defn normalize
  "Reads first, then writes; the last write of a key only; no read of a key
  the transaction writes; each key read once."
  [txn]
  (let [writes  (->> txn
                     (filter (comp #{:w} first))
                     reverse
                     (reduce (fn [[seen out] [_ k _ :as mop]]
                               (if (seen k) [seen out] [(conj seen k) (conj out mop)]))
                             [#{} []])
                     second
                     reverse
                     vec)
        wkeys   (set (map second writes))
        reads   (->> txn
                     (filter (comp #{:r} first))
                     (remove (comp wkeys second))
                     (reduce (fn [[seen out] [_ k _ :as mop]]
                               (if (seen k) [seen out] [(conj seen k) (conj out mop)]))
                             [#{} []])
                     second)]
    (into (vec reads) writes)))

(defn- key-str
  [k]
  (str "e" k))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (let [txn    (:value op)
          reads  (filterv (comp #{:r} first) txn)
          writes (filterv (comp #{:w} first) txn)
          ops    (cond-> []
                   (seq reads)  (conj {:op "getMany", :keys (mapv (comp key-str second) reads)})
                   true         (into (map (fn [[_ k v]] {:op "put", :key (key-str k)
                                                          :value v, :forever true})
                                           writes)))
          write? (boolean (seq writes))]
      (try
        (let [r (kv/batch! test this ops)]
          (if (:error r)
            (assoc op :type (if write? (kv/write-failure (:error r)) :fail)
                   :error (:error r))
            (let [results (:results r)
                  rows    (when (seq reads)
                            (->> (:rows (first results))
                                 (map (juxt :key :value))
                                 (into {})))
                  puts    (if (seq reads) (rest results) results)
                  not-applied (remove :applied puts)]
              (if (seq not-applied)
                ; A plain put (no expect) cannot lose; if one does, say so.
                (assoc op :type :info, :error [:put-not-applied (vec not-applied)])
                (assoc op :type :ok
                       :value (mapv (fn [[f k v :as mop]]
                                      (if (= :r f)
                                        [:r k (get rows (key-str k))]
                                        mop))
                                    txn))))))
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type msg]} (ex-data e)]
            (if (#{::qh/refused ::qh/timeout ::qh/io} type)
              (assoc op :type (if write? (kv/exception-type type) :fail)
                     :error [(keyword (name type)) msg])
              (throw e)))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn workload
  "Elle's rw-register test (generator, final reads of every key, key tracker,
  checker) with every transaction normalised. The checker assumes
  :linearizable-keys? - implied by the strict serializability under test - to
  order each key's versions by real time: a batch cannot read a key before
  writing it, so Elle has no writes-follow-reads inference to fall back on."
  [opts]
  (let [w    (wr/test {:key-count          (:key-count opts 8)
                       :min-txn-length     1
                       :max-txn-length     4
                       :max-writes-per-key (:max-writes-per-key opts 256)
                       :consistency-models [:strict-serializable]
                       :linearizable-keys? true})
        norm (fn [g] (gen/map (fn [op] (update op :value normalize)) g))]
    (cond-> {:client    (->Client nil nil nil)
             :checker   (:checker w)
             :generator (norm (:generator w))
             :db-env    kv/db-env}
      (:final-generator w) (assoc :final-generator (norm (:final-generator w)))
      (:wrap-generator w)  (assoc :wrap-generator (:wrap-generator w)))))
