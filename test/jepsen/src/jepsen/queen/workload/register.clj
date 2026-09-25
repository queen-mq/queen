(ns jepsen.queen.workload.register
  "KV workloads on single keys.

  W3 --workload register: a linearizable CAS register per key (jepsen
    independent keys, Knossos cas-register). read = GET; write = PUT
    forever:true; cas [old new] = GET, then (only when the value read is old)
    PUT with expect:<the version read>. The cas is :ok only when that
    conditional put applied, so it is atomic on the version.
    --kv-route leader sends every request to the current leader; the default
    spreads the clients over the nodes (each client on its own node).

  W3b --workload counter: one key incremented by POST /api/v1/kv incr delta 1,
    read by GET; checked by jepsen.checker/counter (every read between the
    increments known to have committed and those that may have).

  W3c --workload claim: putIfAbsent (PUT expect:0) on independent keys, every
    thread claiming each key once with its own id, then reading it. At most
    one claim per key may win; every read of a claimed key, and every loser's
    report of the current value, must name the winner."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [independent :as independent]
                    [random :as rand]]
            [knossos.model :as model]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]))

(defn- key-of
  [prefix k]
  (str prefix k))

(defn- read-op
  "A GET; value nil when the key does not exist."
  [test client op k]
  (let [r (kv/get! test client k)]
    (if (:error r)
      (do (kv/forget-leader!)
          (assoc op :type :fail, :error (:error r)))
      (assoc op :type :ok, :value (:value r) :version (:version r)))))

(defn- write-result
  "A completion from a put!/batch! write answer."
  [op r ok-value]
  (cond
    (:error r)    (do (kv/forget-leader!)
                      (assoc op :type (kv/write-failure (:error r)), :error (:error r)))
    (:applied? r) (assoc op :type :ok, :value ok-value, :version (:version r))
    :else         (assoc op :type :fail, :error [:not-applied (:reason r)]
                         :current (:value r), :current-version (:version r))))

(def absent
  "What W3 reads for a key that does not exist (Knossos takes a nil read as
  'value unknown', so absence needs a value of its own); the model's initial
  value. Written values are 0-4."
  -1)

(defn- invoke-register
  [test client op]
  (let [[k v] (:value op)
        key   (key-of "r" k)
        wrap  (fn [op' value] (assoc op' :value (independent/tuple k value)))]
    (case (:f op)
      :read  (let [op' (read-op test client op key)]
               (wrap op' (when (= :ok (:type op'))
                           (if (nil? (:value op')) absent (:value op')))))
      :write (let [op' (write-result op (kv/put! test client key v) v)]
               (wrap op' v))
      :cas   (let [[old new] v
                   cur (kv/get! test client key)]
               (cond
                 (:error cur)
                 (do (kv/forget-leader!)
                     (wrap (assoc op :type :fail, :error [:cas-read (:error cur)]) v))

                 (not (and (:found? cur) (= old (:value cur))))
                 (wrap (assoc op :type :fail, :error [:cas-mismatch (:value cur)]) v)

                 :else
                 (wrap (write-result op (kv/put! test client key new (:version cur)) v)
                       v))))))

(defn- invoke-counter
  [test client op]
  (case (:f op)
    :add  (let [r (kv/batch! test client [{:op "incr", :key "counter",
                                           :delta (:value op), :forever true}])]
            (if (:error r)
              (do (kv/forget-leader!)
                  (assoc op :type (kv/write-failure (:error r)), :error (:error r)))
              (let [res (first (:results r))]
                (if (:applied res)
                  (assoc op :type :ok, :total (:value res))
                  (assoc op :type :fail, :error [:not-applied (:reason res)])))))
    :read (let [op' (read-op test client op "counter")]
            (if (= :ok (:type op'))
              (assoc op' :value (long (or (:value op') 0)))
              op'))))

(defn- invoke-claim
  [test client op]
  (let [[k _] (:value op)
        key   (key-of "c" k)]
    (case (:f op)
      :claim (let [id  (str "p" (:process op) "-" (System/nanoTime))
                   op' (write-result op (kv/put! test client key id 0) id)]
               (assoc op' :value (independent/tuple k id)))
      :read  (let [op' (read-op test client op key)]
               (assoc op' :value (independent/tuple k (when (= :ok (:type op'))
                                                         (:value op'))))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:workload test)
        :register (invoke-register test this op)
        :counter  (invoke-counter test this op)
        :claim    (invoke-claim test this op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (do (kv/forget-leader!)
                (assoc op
                       :type  (if (#{:read} (:f op)) :fail (kv/exception-type type))
                       :error [(keyword (name type)) msg]))
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; W3c's checker

(defn claim-checker
  "Per key: at most one :ok claim; every :ok read of the key, and every
  loser's reported current value, equals the winner's id (when a claim won);
  a read invoked after the winning claim completed must not find the key
  absent; a read value that no claim could have written is an anomaly."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops     (->> history h/client-ops (h/remove h/invoke?) vec)
            by-key  (group-by (fn [op] (first (:value op))) ops)
            results
            (->> by-key
                 (map (fn [[k ops]]
                        (let [claims   (filter #(= :claim (:f %)) ops)
                              winners  (filter #(= :ok (:type %)) claims)
                              possible (->> claims
                                            (filter #(#{:ok :info} (:type %)))
                                            (map (comp second :value))
                                            set)
                              winner   (when (= 1 (count winners))
                                         (second (:value (first winners))))
                              reads    (->> ops
                                            (filter #(and (= :read (:f %)) (= :ok (:type %))))
                                            (map (comp second :value)))
                              losers   (->> claims
                                            (filter #(and (= :fail (:type %))
                                                          (= [:not-applied "exists"] (:error %))))
                                            (map :current))
                              stale    (when (= 1 (count winners))
                                         (let [won-at (:time (first winners))]
                                           (->> ops
                                                (filter #(and (= :read (:f %)) (= :ok (:type %))
                                                              (nil? (second (:value %)))
                                                              (< won-at (:time (h/invocation history %)))))
                                                (map (fn [op] {:type :absent-after-claim
                                                               :read (:index op)
                                                               :winner winner})))))
                              errs     (cond-> (vec stale)
                                         (< 1 (count winners))
                                         (conj {:type :two-winners
                                                :winners (mapv (comp second :value) winners)})

                                         winner
                                         (into (->> (concat reads losers)
                                                    (remove nil?)
                                                    (remove #{winner})
                                                    (map (fn [v] {:type :not-the-winner, :value v
                                                                  :winner winner}))))

                                         (not winner)
                                         (into (->> (concat reads losers)
                                                    (remove nil?)
                                                    (remove possible)
                                                    (map (fn [v] {:type :value-from-nowhere
                                                                  :value v})))))]
                          [k errs])))
                 (filter (comp seq second))
                 (into (sorted-map)))]
        {:valid?     (empty? results)
         :keys       (count by-key)
         :winners    (count (filter #(and (= :claim (:f %)) (= :ok (:type %))) ops))
         :bad-keys   (count results)
         :errors     (take 16 results)}))))

;; ---------------------------------------------------------------------------
;; Workloads

(defn register-workload
  [opts]
  (let [n (:threads-per-key opts 5)]
    {:client    (client)
     :checker   (independent/checker
                  (checker/linearizable {:model     (model/cas-register absent)
                                         :algorithm :linear}))
     :generator (independent/concurrent-generator
                  n
                  (range)
                  (fn [k]
                    (->> (gen/mix [(repeat {:f :read})
                                   (fn [] {:f :write, :value (rand/long 5)})
                                   (fn [] {:f :cas, :value [(rand/long 5) (rand/long 5)]})])
                         (gen/limit (:ops-per-key opts 200)))))
     :db-env    kv/db-env}))

(defn counter-workload
  [opts]
  {:client          (client)
   :checker         (checker/counter)
   :generator       (gen/mix [(repeat {:f :add, :value 1})
                              (repeat {:f :read})])
   :final-generator (gen/each-thread {:f :read})
   :db-env          kv/db-env})

(defn claim-workload
  [opts]
  (let [n (:threads-per-key opts 5)]
    {:client    (client)
     :checker   (claim-checker)
     :generator (independent/concurrent-generator
                  n
                  (range)
                  (fn [k]
                    (gen/phases (gen/each-thread {:f :claim})
                                (gen/each-thread {:f :read}))))
     :db-env    kv/db-env}))
