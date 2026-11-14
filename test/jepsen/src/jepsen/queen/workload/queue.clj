(ns jepsen.queen.workload.queue
  "W2: Queen as a lease queue, one named consumer group.

    :enqueue v   one-item POST /api/v1/push to a random partition \"w<p>\" of the
                 first test queue, transactionId \"w<v>\", payload v
    :dequeue     one pop for consumer group g1 with a short leaseSeconds, then
                 ONE batch ack of everything popped, WITH each message's
                 leaseId. Half the pops are wildcard (no partition), half are
                 pinned to a random partition; long polls (wait=true) and plain
                 pops are mixed.

  A dequeue completes :ok (value = the acked values) only when every item of
  the ack succeeded; :fail when no item did (or the pop failed); :info when the
  ack's outcome is unknown. Every completion carries :msgs, what the pop
  delivered (value, partition, offset, lease, deliveryAttempt), so the checker
  sees deliveries whatever became of their ack.

  Checkers: jepsen's total-queue (lost, unexpected) over the acked values, and
  a queue checker for what total-queue cannot see:
    - a message acked successfully twice (a noop ack counts separately);
    - a message redelivered by a pop invoked after its ack succeeded;
    - two deliveries of one message under overlapping unexpired leases
      (lease windows: [pop completed, pop invoked + leaseSeconds]);
    - per partition: one offset delivered with different values; a pop with a
      gap inside its own messages; a pop that starts past (the highest offset
      acked, or possibly acked, before it) + 1.
  The final phase drains the queue from every node."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen.binary :as bin]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log])
  (:import (java.net URLEncoder)))

(def group
  "The one consumer group (the default)."
  "g1")

(defn groups
  "The consumer groups a test pops with: [\"g1\"] by default; --w2-groups N:
  g1..gN on the one queue, every group getting every message (fan-out);
  --w2-group none: [nil], queue mode (no consumerGroup at all, the broker's
  __QUEUE_MODE__ cursor)."
  [test]
  (cond (= "none" (:w2-group test))           [nil]
        (< 1 (long (:w2-groups test 1)))      (mapv #(str "g" (inc %)) (range (:w2-groups test)))
        :else                                 [group]))

(defn queue-name
  [test]
  (first (:queue-names test)))

(defn partition-name
  [p]
  (str "w" p))

(defn- enc
  [s]
  (URLEncoder/encode (str s) "UTF-8"))

;; ---------------------------------------------------------------------------
;; Client

(defn- enqueue!
  [client test op]
  (let [v    (:value op)
        p    (rand/long (:w2-partitions test 8))
        body {:items [{:queue         (queue-name test)
                       :partition     (partition-name p)
                       :payload       v
                       :transactionId (str "w" v)}]}
        url  (str (:base client) "/api/v1/push")
        r    (if (:binary test)
               (bin/request! (:http client) :post url
                             (bin/push-body [{:queue (queue-name test), :partition (partition-name p)
                                              :items [{:transactionId (str "w" v), :payload v}]}])
                             "application/x-queen-push" (:client-timeout-ms test))
               (qh/request! (:http client) :post url body (:client-timeout-ms test)))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (case (:status item)
          ("queued" "duplicate")
          (assoc op :type :ok, :partition p, :offset (:offset item)
                 :dup? (= "duplicate" (:status item)))
          (assoc op :type :fail, :error [:item-error item])))
      (assoc op
             :type  (log/push-failure test (:status r) (:body r))
             :error [(:status r) (:body r)]))))

(defn- pop-url
  [client test {:keys [pinned batch lease wait-ms g]}]
  (str (:base client) "/api/v1/pop/queue/" (enc (queue-name test))
       (when pinned (str "/partition/" (enc (partition-name pinned))))
       "?batch=" batch
       (when g (str "&consumerGroup=" (enc g)))
       "&leaseSeconds=" lease
       "&subscriptionMode=all"
       (if wait-ms
         (str "&wait=true&timeout=" wait-ms)
         "&wait=false")
       ; --binary: the stored records (QPF1), unpacked by jepsen.queen.binary.
       (when (:binary test) "&frames=true")))

(defn- ack-type
  "The completion type for an ack that did not answer 200."
  [test status body]
  (log/push-failure test status body))

(defn- extend-lease!
  "--w2-extend: work past the lease. Sleep until 1 s before it ends, renew it
  for lease + 3 s (POST /api/v1/lease/:id/extend), then sleep 1.5 s more, so
  the ack that follows comes after the ORIGINAL lease ran out. A renewal the
  broker answered success holds the messages until at least its invocation
  plus those seconds (the broker counts from when it applies it)."
  [client test lease-id lease-s]
  (Thread/sleep (long (* 1000 (max 0 (dec lease-s)))))
  (let [secs (+ lease-s 3)
        t0   (System/nanoTime)
        r    (try (qh/request! (:http client) :post
                               (str (:base client) "/api/v1/lease/" (enc lease-id) "/extend")
                               {:seconds secs} (:client-timeout-ms test))
                  (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))
        ok?  (and (= 200 (:status r)) (true? (:success (:body r))))]
    (Thread/sleep 1500)
    {:renewed ok?, :seconds secs, :invoked-ns t0
     :answer (if (:exception r) [:exception (:type (:exception r))] (select-keys (:body r) [:success :renewed :newExpiresAt]))}))

(defn- dequeue!
  [client test op]
  (let [pinned  (when (and (not (:drain? op)) (< (rand/double) 0.5))
                  (rand/long (:w2-partitions test 8)))
        g       (if (contains? op :group) (:group op) (first (groups test)))
        how     {:pinned  pinned
                 :batch   (inc (rand/long 5))
                 :lease   (:w2-lease test 3)
                 :g       g
                 :wait-ms (if (:drain? op)
                            1000
                            (rand/nth [nil 200 1000]))}
        op      (assoc op :pop how, :lease-s (:lease how), :group g)
        ; The pop's start on the clock renewal times are read on: the
        ; checker puts a renewal into the history's time base through it.
        pop-ns  (System/nanoTime)
        r       (try (if (:binary test)
                       (bin/request! (:http client) :get (pop-url client test how) nil
                                     "application/json"
                                     (+ (or (:wait-ms how) 0) (:client-timeout-ms test)))
                       (qh/request! (:http client) :get (pop-url client test how) nil
                                    (+ (or (:wait-ms how) 0) (:client-timeout-ms test))))
                     (catch clojure.lang.ExceptionInfo e
                       (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                         {:exception (ex-data e)}
                         (throw e))))]
    (cond
      ; A pop that never answered took no lease we could ever use; whatever
      ; it claimed is redelivered after the lease.
      (:exception r)
      (assoc op :type :fail, :value [], :msgs []
             :error [:pop (keyword (name (:type (:exception r)))) (:msg (:exception r))])

      (= 204 (:status r))
      (assoc op :type :ok, :value [], :msgs [])

      (not= 200 (:status r))
      (assoc op :type :fail, :value [], :msgs [], :error [:pop (:status r) (:body r)])

      :else
      (let [msgs (->> (:messages (:body r))
                      (mapv (fn [m]
                              {:v         (:data m)
                               :txn       (:transactionId m)
                               :pid       (:partitionId m)
                               :partition (:partition m)
                               :offset    (:offset m)
                               :lease     (:leaseId m)
                               :attempt   (:deliveryAttempt m)})))]
        (if (empty? msgs)
          (assoc op :type :ok, :value [], :msgs [])
          (let [ext  (when (and (:w2-extend test) (not (:drain? op)) (< (rand/double) 0.3))
                       (assoc (extend-lease! client test (:lease (first msgs)) (:lease how))
                              :pop-inv-nano pop-ns))
                op   (cond-> op ext (assoc :extend ext))
                body (cond-> {:acknowledgments (mapv (fn [m]
                                               {:transactionId (:txn m)
                                                :partitionId   (:pid m)
                                                :leaseId       (:lease m)
                                                :status        "completed"})
                                             msgs)}
                       g (assoc :consumerGroup g))
                ar   (try (qh/request! (:http client) :post
                                       (str (:base client) "/api/v1/ack/batch")
                                       body (:client-timeout-ms test))
                          (catch clojure.lang.ExceptionInfo e
                            (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                              {:exception (ex-data e)}
                              (throw e))))]
            (cond
              (:exception ar)
              (assoc op
                     :type  (if (= ::qh/refused (:type (:exception ar))) :fail :info)
                     :value [], :msgs msgs
                     :error [:ack (keyword (name (:type (:exception ar))))
                             (:msg (:exception ar))])

              (not= 200 (:status ar))
              (assoc op :type (ack-type test (:status ar) (:body ar))
                     :value [], :msgs msgs, :error [:ack (:status ar) (:body ar)])

              :else
              (let [res      (vec (:body ar))
                    by-idx   (into {} (map (juxt :index identity) res))
                    msgs     (vec (map-indexed
                                    (fn [i m]
                                      (let [a (by-idx i)]
                                        (assoc m :ack (cond (nil? a)                     :missing
                                                            (and (:success a) (:noop a)) :noop
                                                            (:success a)                 :acked
                                                            :else                        :rejected)
                                               :ack-error (:error a))))
                                    msgs))
                    acked    (filterv #(= :acked (:ack %)) msgs)]
                (assoc op
                       :type  (cond (= (count acked) (count msgs)) :ok
                                    (empty? acked)                 :fail
                                    :else                          :ok)
                       :value (mapv :v acked)
                       :msgs  msgs
                       :partial-ack? (< 0 (count acked) (count msgs)))))))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue (enqueue! this test op)
        :dequeue (dequeue! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op
                   :type  (if (or (= ::qh/refused type) (= :dequeue (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  []
  (map->Client {}))

;; ---------------------------------------------------------------------------
;; Generators

(defrecord DrainUntilEmpty [need empties]
  ; One thread's final drain: dequeue (long polls of 1 s) until `need` in a
  ; row come back empty.
  gen/Generator
  (op [this test ctx]
    (when (< empties need)
      (let [op (gen/fill-in-op {:f :dequeue, :value nil, :drain? true} ctx)]
        (if (= :pending op)
          [:pending this]
          [op this]))))

  (update [this test ctx event]
    (if (and (= :dequeue (:f event)) (:drain? event) (not= :invoke (:type event)))
      (if (and (= :ok (:type event)) (empty? (:msgs event)))
        (DrainUntilEmpty. need (inc empties))
        (DrainUntilEmpty. need 0))
      this)))

;; ---------------------------------------------------------------------------
;; Checkers

(defn- deliveries
  "Every message a pop delivered, with its op's times, lease and ack outcome."
  [history]
  (->> history
       h/client-ops
       (h/filter (fn [op] (and (= :dequeue (:f op)) (not= :invoke (:type op)))))
       (mapcat (fn [op]
                 (let [inv (h/invocation history op)]
                   (map (fn [m]
                          (assoc m
                                 :op-index  (:index op)
                                 :op-type   (:type op)
                                 :process   (:process op)
                                 :inv-time  (:time inv)
                                 :comp-time (:time op)
                                 :lease-ns  (* 1000000000 (long (:lease-s op 3)))
                                 :extend    (:extend op)
                                 :ack       (if (= :info (:type op)) :unknown (:ack m))))
                        (:msgs op)))))
       vec))

(defn- window
  "The time a delivery is sure to hold its lease: from the pop's completion to
  the pop's invocation + leaseSeconds, or, renewed (--w2-extend, answered
  success), to the renewal's invocation + its seconds when that is later.
  Renewal times are the client's System/nanoTime, as history times are
  (Jepsen's relative time is nanoTime minus the test's start; both are
  compared as offsets from the pop's invocation)."
  [d]
  (let [base [(:comp-time d) (+ (:inv-time d) (:lease-ns d))]
        e    (:extend d)]
    (if (and e (:renewed e) (:pop-inv-nano e))
      [(first base)
       (max (second base)
            (+ (:inv-time d) (- (:invoked-ns e) (:pop-inv-nano e)) (* 1000000000 (long (:seconds e)))))]
      base)))

(defn queue-checker
  "W2's rules over the deliveries. `skip-excused?` (history, the pop op, its
  partition, the pop's first offset there, the highest offset acked before
  it) excuses a pop that starts past the next offset: W24 maxwait, whose
  skipped messages may have been removed."
  ([] (queue-checker {}))
  ([{:keys [skip-excused?] :or {skip-excused? (constantly false)}}]
  (reify checker/Checker
    (check [this test history opts]
      (let [ds        (deliveries history)
            by-v      (group-by :v ds)
            acked     (filter #(= :acked (:ack %)) ds)
            ; A message acked successfully twice.
            twice     (->> acked (group-by :v) (filter #(< 1 (count (val %))))
                           (map (fn [[v xs]] {:v v, :ops (mapv :op-index xs)})) vec)
            ; A message redelivered by a pop invoked after its ack succeeded.
            first-ack (reduce (fn [m d] (update m (:v d) (fnil min Long/MAX_VALUE) (:comp-time d)))
                              {} acked)
            redeliv   (->> ds
                           (keep (fn [d]
                                   (when-let [t (first-ack (:v d))]
                                     (when (< t (:inv-time d))
                                       {:v (:v d), :partition (:partition d), :offset (:offset d)
                                        :op (:op-index d), :ack (:ack d), :attempt (:attempt d)}))))
                           vec)
            ; Two deliveries of one message under overlapping unexpired leases.
            overlaps  (->> by-v
                           (mapcat (fn [[v xs]]
                                     (let [xs (vec (sort-by :comp-time xs))]
                                       (for [i (range (count xs))
                                             j (range (inc i) (count xs))
                                             :let [a (xs i), b (xs j)
                                                   [a0 a1] (window a)
                                                   [b0 b1] (window b)]
                                             :when (and (not= (:op-index a) (:op-index b))
                                                        (< (max a0 b0) (min a1 b1)))]
                                         {:v v, :ops [(:op-index a) (:op-index b)]
                                          :overlap-ms (quot (- (min a1 b1) (max a0 b0)) 1000000)}))))
                           vec)
            ; One offset delivered with different values.
            mismatch  (->> ds
                           (group-by (juxt :partition :offset))
                           (keep (fn [[k xs]] (let [vs (set (map :v xs))]
                                                (when (< 1 (count vs)) {:at k, :values vs}))))
                           vec)
            ; A pop with a gap inside its own messages, per partition.
            ops       (->> history h/client-ops
                           (h/filter (fn [op] (and (= :dequeue (:f op)) (seq (:msgs op)))))
                           vec)
            internal  (->> ops
                           (mapcat (fn [op]
                                     (for [[p ms] (group-by :partition (:msgs op))
                                           :let [offs (sort (map :offset ms))]
                                           :when (not= offs (range (first offs) (inc (last offs))))]
                                       {:op (:index op), :partition p, :offsets (vec offs)})))
                           vec)
            ; A pop that starts past the highest offset acked (or maybe acked)
            ; before it + 1.
            acks-by-p (->> ds
                           (filter #(#{:acked :unknown :noop} (:ack %)))
                           (group-by :partition))
            all-skips (->> ops
                           (mapcat
                             (fn [op]
                               (let [inv (h/invocation history op)]
                                 (for [[p ms] (group-by :partition (:msgs op))
                                       :let [first-off (reduce min (map :offset ms))
                                             before    (->> (acks-by-p p)
                                                            (filter #(and (not= (:op-index %) (:index op))
                                                                          (< (:inv-time %) (:time op))))
                                                            (map :offset)
                                                            (reduce max -1))]
                                       :when (< (inc before) first-off)]
                                   {:op (:index op), :partition p, :first-offset first-off
                                    :highest-acked-before before
                                    :excused (boolean (skip-excused? history op p first-off before))}))))
                           vec)
            skips     (filterv (complement :excused) all-skips)
            noops     (->> ds (filter #(= :noop (:ack %))) (mapv #(select-keys % [:v :partition :offset :op-index])))
            ; A payload that is not the value its transactionId names.
            payload   (->> ds
                           (remove #(= (:txn %) (str "w" (:v %))))
                           (mapv #(select-keys % [:v :txn :partition :offset :op-index])))
            rejected  (count (filter #(= :rejected (:ack %)) ds))
            partial   (count (filter :partial-ack? ops))
            ; --w2-extend: an ack refused although the broker had just
            ; renewed its lease (answered success) for longer than the work.
            renewed-refused (->> ds
                                 (filter #(and (get-in % [:extend :renewed]) (= :rejected (:ack %))))
                                 (mapv #(select-keys % [:v :partition :offset :op-index :extend])))]
        {:valid?             (and (empty? twice) (empty? redeliv) (empty? overlaps)
                                  (empty? mismatch) (empty? internal) (empty? skips)
                                  (empty? payload) (empty? renewed-refused))
         :renewals           (->> ds (keep :extend) distinct (map :renewed) frequencies)
         :ack-refused-under-renewal (take 16 renewed-refused)
         :payload-mismatch   (take 16 payload)
         :deliveries         (count ds)
         :redeliveries       (count (filter #(< 1 (or (:attempt %) 1)) ds))
         :acked              (count acked)
         :ack-rejected       rejected
         :ack-unknown        (count (filter #(= :unknown (:ack %)) ds))
         :partial-acks       partial
         :noop-acks          (count noops)
         :noop-ack-examples  (take 8 noops)
         :acked-twice        (take 16 twice)
         :acked-twice-count  (count twice)
         :redelivered-after-ack       (take 16 redeliv)
         :redelivered-after-ack-count (count redeliv)
         :lease-overlaps       (take 16 overlaps)
         :lease-overlap-count  (count overlaps)
         :offset-value-mismatch (take 16 mismatch)
         :pop-internal-gaps    (take 16 internal)
         :pop-skips            (take 16 skips)
         :pop-skip-count       (count skips)
         :pop-skips-excused    (count (filter :excused all-skips))})))))

(defn total-queue-checker
  "jepsen.checker/total-queue over the acked values: each :ok dequeue becomes
  one :dequeue pair per acked value."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops (->> history
                     h/client-ops
                     (mapcat (fn [op]
                               (case (:f op)
                                 :enqueue [(dissoc op :index)]
                                 ; An ack of unknown outcome may have applied:
                                 ; its values count as dequeued (a message whose
                                 ; ack did not apply is redelivered and acked
                                 ; later, so this hides no loss the drain can see).
                                 :dequeue (when-let [vs (case (:type op)
                                                          :ok   (:value op)
                                                          :info (map :v (:msgs op))
                                                          nil)]
                                            (mapcat (fn [v]
                                                      [{:type :invoke, :f :dequeue, :value nil
                                                        :process (:process op), :time (:time op)}
                                                       {:type :ok, :f :dequeue, :value v
                                                        :process (:process op), :time (:time op)}])
                                                    vs))
                                 nil)))
                     vec)]
        (checker/check (checker/total-queue) test
                       (h/history ops {:have-indices? false})
                       opts)))))

(defrecord DrainGroups [gs need empties]
  ; --w2-groups N: one thread's final drain of every group in turn, until each
  ; came back empty `need` times in a row.
  gen/Generator
  (op [this test ctx]
    (let [left (remove #(<= need (get empties % 0)) gs)]
      (when (seq left)
        (let [g  (first left)
              op (gen/fill-in-op {:f :dequeue, :value nil, :drain? true, :group g} ctx)]
          (if (= :pending op)
            [:pending this]
            [op (DrainGroups. (vec (concat (rest gs) [(first gs)])) need empties)])))))
  (update [this test ctx event]
    (if (and (= :dequeue (:f event)) (:drain? event) (not= :invoke (:type event)))
      (DrainGroups. gs need (if (and (= :ok (:type event)) (empty? (:msgs event)))
                              (update empties (:group event) (fnil inc 0))
                              (assoc empties (:group event) 0)))
      this)))

(defn per-group-checker
  "--w2-groups N: W2's checkers once per group, over every enqueue and that
  group's dequeues: each group must get every message (fan-out)."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist (h/client-ops history)
            res  (into (sorted-map)
                       (for [g (groups test)]
                         (let [ops (h/filter #(or (= :enqueue (:f %))
                                                  (and (= :dequeue (:f %)) (= g (:group %))))
                                             hist)]
                           [g (checker/check (checker/compose {:total-queue (total-queue-checker)
                                                               :queue       (queue-checker)})
                                             test ops opts)])))]
        {:valid? (every? :valid? (vals res))
         :groups res}))))

(defn workload
  [opts]
  (let [gs (groups opts)]
    (if (< 1 (count gs))
      {:client          (client)
       :checker         (per-group-checker)
       :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                                  (fn [] {:f :dequeue, :value nil, :group (rand/nth gs)})])
       :final-generator (gen/each-thread (->DrainGroups gs 8 {}))}
      {:client          (client)
       :checker         (checker/compose {:total-queue (total-queue-checker)
                                          :queue       (queue-checker)})
       :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v}) (range))
                                  (repeat {:f :dequeue, :value nil})])
       :final-generator (gen/each-thread (->DrainUntilEmpty 8 0))})))
