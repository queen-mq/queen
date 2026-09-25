(ns jepsen.queen.workload.elle
  "W4 --workload elle: Elle rw-register transactions as ONE KV batch each
  (POST /api/v1/kv): a getMany of the keys the transaction reads, then a put
  of each key it writes (forever:true), checked at strict serializability.

  Queen evaluates a batch's reads AFTER its own entry applied on the node that
  answers (rsm/facade/real/kv.rs), so a read of a key the same batch writes
  would see that write whatever its position. Transactions are therefore
  normalised before they are sent: reads first, then writes; one write per key
  (the last); no read of a key the transaction writes. Elle sees exactly the
  normalised transaction.

  A read-only batch waits for the read index; a batch with writes is one raft
  entry, its reads taken after that entry applied - possibly after LATER
  entries too, which is what RESEARCH.md expected Elle to see as G-single."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]))

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
