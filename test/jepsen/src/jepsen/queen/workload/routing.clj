(ns jepsen.queen.workload.routing
  "Routing without copies (cluster version 8): W15 filtered groups, W16
  reading queues, W17 `link` in a transaction. Headers ride on every message
  and are checked on every path that returns them.

  Every pushed message v (a unique integer) carries
    payload  {v, kind: a|b|c (v mod 3), cust: c0..c7 ((v div 3) mod 8)}
    headers  {region: x (v even) | y, src: jepsen}
  under transactionId \"w<v>\", so a checker knows from v alone which
  `where` takes it, where `partitionBy` puts it and what it must look like.

  W15 --workload filtered: one queue (jepsen-0, partitions w0..w7) and three
  groups that start with history (subscriptionMode all): ga (where
  data.kind = 'a', on the payload), gx (where headers.region = 'x', on the
  headers) and gall (no where: an ordinary group, the control). Pops always
  send the group's where; acks are W2's (one batch, with every lease). Per
  group the checker runs W2's checkers over the messages the group's where
  takes (nothing acked lost, none acked twice, no redelivery after an ack,
  exclusive leases, order per partition), and rejects a message the where
  does not take, and a filtered group's answer without \"filtered\": true
  (a 204 included).

  W16 --workload reading: reading queues of the test queues jepsen-0 and
  jepsen-1 (partitions w0..w7):
    jepsen-r1  from jepsen-0, where data.kind = 'a', partitionBy same,
               start now (made at setup)
    jepsen-r2  from jepsen-0 and jepsen-1, partitionBy data.cust, start now
               (made at setup: two sources merged and re-keyed)
    jepsen-r3  from jepsen-0, where headers.region = 'x', partitionBy same,
               start all, made a third of the way into the test (catch-up
               under the faults), configured again at the end until it
               answers catchingUp false
  Clients push to both sources and consume the readers (group g). At the end
  every partition of every source and reader is read by offset from every
  node. The checker rejects: an acknowledged push missing from its source; a
  reader that lacks a message its sources hold and its where takes, holds
  one twice, or holds one nothing pushed or its where refuses; a message in
  the wrong reader partition; a reader partition whose messages from one
  source partition are out of the source's order; for the readers made
  before the pushes (r1, r2), a message placed after one whose push was sent
  after it had been acknowledged (live links follow real time); a payload or
  headers that differ from the source's; two nodes that read different logs;
  and, per reader, W2's consumer rules.

  W17 --workload link: pushes to jepsen-0; a worker pops one message (group
  fw) and commits ONE /api/v1/transaction that acks it and links it into
  jepsen-1 (same partition) with the header hop=1. Now and then a worker
  links a message it already forwarded again: the broker must answer
  duplicate: true and keep one. At the end the input is drained and jepsen-1
  read. The checker rejects: an acknowledged push not in jepsen-1 exactly
  once; anything in jepsen-1 nothing pushed; a target message whose id,
  payload or headers (the source's, then hop=1) differ; a repeated link that
  committed without duplicate: true."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.binary :as bin]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.queue :as queue])
  (:import (java.net URLEncoder)))

;; ---------------------------------------------------------------------------
;; Messages

(def kinds ["a" "b" "c"])

(defonce ^{:doc "--pad-bytes: a `pad` of this many characters in every payload, so a
  record of one message is past the 512 bytes the queue log compresses from
  (rsm/qlog/codec.rs MIN_BYTES). Set by each workload as it is built; one
  test runs at a time."}
  pad-bytes
  (atom 0))

(def ^:private pad-of (memoize (fn [n] (apply str (repeat n \x)))))

(defn payload
  [v]
  (cond-> {:v v, :kind (kinds (mod v 3)), :cust (str "c" (mod (quot v 3) 8))}
    (pos? (long @pad-bytes)) (assoc :pad (pad-of (long @pad-bytes)))))

(defn headers
  [v]
  {:region (if (even? v) "x" "y"), :src "jepsen"})

(defn txn-id [v] (str "w" v))

(defn parse-txn
  [s]
  (when-let [[_ v] (and (string? s) (re-matches #"w(\d+)" s))]
    (parse-long v)))

(def partitions 8)

(defn partition-name [p] (str "w" p))

(defn- enc
  [s]
  (URLEncoder/encode (str s) "UTF-8"))

(defn- request
  "qh/request!, with a refused/timeout/io exception returned as
  {:exception {...}} instead of thrown."
  [client method url body timeout-ms]
  (try (qh/request! (:http client) method url body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- exception-type
  [r]
  (if (= ::qh/refused (:type (:exception r))) :fail :info))

;; ---------------------------------------------------------------------------
;; Shared calls

(defn push!
  "One message v to queue/partition. {:type :ok|:fail|:info, :offset, :error}."
  [client test queue partition v]
  (let [r (request client :post (str (:base client) "/api/v1/push")
                   {:items [{:queue         queue
                             :partition     partition
                             :payload       (payload v)
                             :headers       (headers v)
                             :transactionId (txn-id v)}]}
                   (:client-timeout-ms test))]
    (cond
      (:exception r)
      {:type (exception-type r), :error [:push (:type (:exception r))]}

      (= 201 (:status r))
      (let [item (first (:body r))]
        (if (#{"queued" "duplicate"} (:status item))
          {:type :ok, :offset (:offset item)}
          {:type :fail, :error [:item-error item]}))

      :else
      {:type (log/push-failure test (:status r) (:body r))
       :error [:push (:status r) (:body r)]})))

(defn msg->delivery
  "A popped message as W2's checkers read a delivery."
  [m]
  {:v         (some-> m :data :v)
   :txn       (:transactionId m)
   :pid       (:partitionId m)
   :partition (:partition m)
   :offset    (:offset m)
   :lease     (:leaseId m)
   :attempt   (:deliveryAttempt m)
   :headers   (:headers m)
   :data      (:data m)
   ; Set only on a framed (--binary) answer: the frame's flags.
   :frame-flags (:frameFlags m)})

(defn pop-and-ack!
  "One pop of `queue` for `group` (with `where` when given) and ONE batch ack
  of what it delivered, each with its lease: W2's dequeue, for any queue and
  group. The completion carries :msgs (every delivery and its ack outcome),
  :value (the acked v's), and the answer's :filtered and :status."
  [client test op queue group where]
  (let [drain?  (:drain? op)
        batch   (inc (rand/long 5))
        lease   (:w2-lease test 3)
        wait-ms (if drain? 1000 (rand/nth [nil 200 1000]))
        url     (str (:base client) "/api/v1/pop/queue/" (enc queue)
                     "?consumerGroup=" (enc group)
                     "&batch=" batch "&leaseSeconds=" lease
                     "&subscriptionMode=all"
                     (when where (str "&where=" (enc where)))
                     (if wait-ms (str "&wait=true&timeout=" wait-ms) "&wait=false"))
        op      (assoc op :pop {:batch batch, :lease lease, :wait-ms wait-ms}
                          :lease-s lease)
        ; A filtered group's first pops run its catch-up, up to 10 s.
        tmo     (+ (or wait-ms 0) 12000 (:client-timeout-ms test))
        ; --binary: the framed pop. A record that holds headers or a link is
        ; the JSON render's: the answer then comes as JSON.
        r       (if (:binary test)
                  (try (bin/request! (:http client) :get (str url "&frames=true") nil
                                     "application/json" tmo)
                       (catch clojure.lang.ExceptionInfo e
                         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
                           {:exception (ex-data e)}
                           (throw e))))
                  (request client :get url nil tmo))]
    (cond
      (:exception r)
      (assoc op :type :fail, :value [], :msgs []
             :error [:pop (:type (:exception r)) (:msg (:exception r))])

      (= 204 (:status r))
      (assoc op :type :ok, :value [], :msgs [], :status 204, :filtered nil)

      (not= 200 (:status r))
      (assoc op :type :fail, :value [], :msgs [], :error [:pop (:status r) (:body r)])

      :else
      (let [body (:body r)
            msgs (mapv msg->delivery (:messages body))
            op   (cond-> (assoc op :status 200, :filtered (:filtered body))
                   (:binary test)
                   (assoc :framed (true? (:framed body))
                          :json-only-frames (bin/json-only-frames (:messages body))))]
        (if (empty? msgs)
          (assoc op :type :ok, :value [], :msgs [])
          (let [ar (request client :post (str (:base client) "/api/v1/ack/batch")
                            {:consumerGroup   group
                             :acknowledgments (mapv (fn [m]
                                                      {:transactionId (:txn m)
                                                       :partitionId   (:pid m)
                                                       :leaseId       (:lease m)
                                                       :status        "completed"})
                                                    msgs)}
                            (:client-timeout-ms test))]
            (cond
              (:exception ar)
              (assoc op :type (exception-type ar), :value [], :msgs msgs
                     :error [:ack (:type (:exception ar))])

              (not= 200 (:status ar))
              (assoc op :type (log/push-failure test (:status ar) (:body ar))
                     :value [], :msgs msgs, :error [:ack (:status ar) (:body ar)])

              :else
              (let [by-idx (into {} (map (juxt :index identity) (:body ar)))
                    msgs   (vec (map-indexed
                                  (fn [i m]
                                    (let [a (by-idx i)]
                                      (assoc m :ack (cond (nil? a)                     :missing
                                                          (and (:success a) (:noop a)) :noop
                                                          (:success a)                 :acked
                                                          :else                        :rejected))))
                                  msgs))
                    acked  (filterv #(= :acked (:ack %)) msgs)]
                (assoc op
                       :type  (if (and (empty? acked) (seq msgs)) :fail :ok)
                       :value (mapv :v acked)
                       :msgs  msgs
                       :partial-ack? (< 0 (count acked) (count msgs)))))))))))

(defn fetch-partition
  "Every record of queue/partition from offset 0: [{:offset :txn :v :data
  :headers}]. Throws when a fetch does not answer 200."
  [client test queue partition]
  (loop [off 0, out []]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                         {:entries [{:queue queue, :partition partition, :offset off}]}
                         (:client-timeout-ms test))
          e (first (:entries (:body r)))]
      (cond
        (not= 200 (:status r))
        (throw (ex-info "fetch failed" {:queue queue, :partition partition
                                        :status (:status r), :body (:body r)}))

        (seq (:records e))
        (recur (inc (long (:offset (peek (:records e)))))
               (into out (map (fn [rec]
                                {:offset  (:offset rec)
                                 :txn     (:transactionId rec)
                                 :v       (some-> rec :payload :v)
                                 :data    (:payload rec)
                                 :headers (:headers rec)}))
                     (:records e)))

        :else out))))

(defn read-queues!
  "Every partition of each [queue partitions] pair, read through this
  client's node: {queue {partition [records]}}, as an :ok op, or :fail."
  [client test op queues]
  (try
    (assoc op :type :ok
           :value (into (sorted-map)
                        (for [[q ps] queues]
                          [q (into (sorted-map)
                                   (for [p ps] [p (fetch-partition client test q p)]))]))
           :node (:node client))
    (catch clojure.lang.ExceptionInfo e
      (if (or (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
              (:status (ex-data e)))
        (assoc op :type :fail, :error [:read (dissoc (ex-data e) :body)])
        (throw e)))))

(defn cluster-version-8!
  "Waits until every node's /health says raft.clusterVersion 8 or more: the
  routing features are refused (cluster_version) below it, and a fresh
  cluster raises its version a second or so after every member answers."
  [test]
  (let [http (qh/client)]
    (util/await-fn
      (fn []
        (let [vs (mapv #(get-in (qh/health http % 2000) [:raft :clusterVersion])
                       (:nodes test))]
          (when-not (every? #(and (integer? %) (<= 8 %)) vs)
            (throw (ex-info "cluster version below 8" {:versions vs})))
          vs))
      {:timeout 90000, :retry-interval 1000, :log-interval 10000
       :log-message "waiting for cluster version 8"})))

(defn configure!
  "POST /api/v1/configure; the answer's body on 200, else throws."
  [client test queue options]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/configure")
                       {:queue queue, :options options}
                       (+ 15000 (:client-timeout-ms test)))]
    (if (= 200 (:status r))
      (:body r)
      (throw (ex-info "configure failed" {:queue queue, :status (:status r), :body (:body r)})))))

(defrecord Client [node base http invoke-fn]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when-let [f (:setup-fn this)]
      (f this test)))

  (invoke! [this test op]
    (try
      ((:invoke-fn this) this test op)
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op
                   :type  (if (or (= ::qh/refused type) (#{:dequeue :read} (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Shared checks

(defn header-mismatches
  "Deliveries or records whose headers are not `expect` of their v."
  [expect xs]
  (->> xs
       (remove (fn [x] (or (nil? (:v x))
                           (= (expect (:v x))
                              (into {} (map (fn [[k v]] [(keyword (name k)) v]))
                                    (:headers x))))))
       (mapv #(select-keys % [:v :txn :partition :offset :headers]))))

(defn framed-checker
  "--binary: no framed pop answer holds a frame with headers or a link. Those
  are the JSON render's alone: the framed pop leaves a record holding one to
  it, reading a compressed record's mark instead of opening it (the fix1
  snapshot, 2026-11-13). Also how many pops with messages came back framed
  and how many as JSON, to show which paths ran."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [pops (->> history h/client-ops (h/filter #(contains? % :framed)) vec)
            bad  (vec (for [op pops, m (:json-only-frames op)]
                        (merge (select-keys op [:index :process :node :reader :group])
                               (select-keys m [:transactionId :partition :offset :frameFlags]))))]
        {:valid?                             (empty? bad)
         :framed-pops                        (count (filter :framed pops))
         :framed-pops-with-messages          (count (filter #(and (:framed %) (seq (:msgs %))) pops))
         :json-pops-with-messages            (count (filter #(and (not (:framed %)) (seq (:msgs %))) pops))
         :json-only-frames-in-framed-answers (take 16 bad)}))))

(defn payload-mismatches
  [xs]
  (->> xs
       (remove (fn [x] (and (:v x)
                            (= (txn-id (:v x)) (:txn x))
                            (= (payload (:v x)) (:data x)))))
       (mapv #(select-keys % [:v :txn :partition :offset :data]))))

(defn consumer-check
  "W2's queue and total-queue checkers over the sub-history `ops` (a
  history): what one group sees of what it should."
  [test ops opts]
  (let [q  (checker/check (queue/queue-checker) test ops opts)
        tq (checker/check (queue/total-queue-checker) test ops opts)]
    {:valid?      (and (:valid? q) (:valid? tq))
     :queue       q
     :total-queue (select-keys tq [:valid? :lost-count :unexpected-count :duplicated-count
                                   :lost :unexpected :ok-count :attempt-count
                                   :recovered-count])}))

(defrecord DrainEach [targets need empties]
  ; One thread's final drain of several consumers: dequeue ops naming each
  ; target in turn, until every target came back empty `need` times in a
  ; row. `targets` are maps merged into the op (e.g. {:group "ga"}).
  gen/Generator
  (op [this test ctx]
    (let [left (remove #(<= need (get empties % 0)) targets)]
      (when (seq left)
        (let [t  (first left)
              op (gen/fill-in-op (merge {:f :dequeue, :value nil, :drain? true} t) ctx)]
          (if (= :pending op)
            [:pending this]
            [op (DrainEach. (vec (concat (rest targets) [(first targets)])) need empties)])))))

  (update [this test ctx event]
    (if (and (= :dequeue (:f event)) (:drain? event) (not= :invoke (:type event)))
      (let [t (some #(when (= % (select-keys event (keys %))) %) targets)]
        (if-not t
          this
          (DrainEach. targets need
                      (if (and (= :ok (:type event)) (empty? (:msgs event)))
                        (update empties t (fnil inc 0))
                        (assoc empties t 0)))))
      this)))

(defn enqueue-gen
  "Pushes of v = 0, 1, 2, ... (each op may add a :queue)."
  ([] (enqueue-gen nil))
  ([queues]
   (map (fn [v] (cond-> {:f :enqueue, :value v}
                  queues (assoc :queue (nth queues (mod (* 7 v) (count queues))))))
        (range))))

;; ---------------------------------------------------------------------------
;; W15: filtered groups

(def filtered-groups
  [{:group "ga",   :where "data.kind = 'a'",      :matches? #(= 0 (mod % 3))}
   {:group "gx",   :where "headers.region = 'x'", :matches? even?}
   {:group "gall", :where nil,                    :matches? (constantly true)}])

(defn- filtered-invoke!
  [client test op]
  (case (:f op)
    :enqueue
    (let [p   (rand/long partitions)
          res (push! client test (first (:queue-names test)) (partition-name p) (:value op))]
      (merge (assoc op :partition p) res))

    :dequeue
    (let [g (some #(when (= (:group op) (:group %)) %) filtered-groups)]
      (pop-and-ack! client test op (first (:queue-names test)) (:group g) (:where g)))))

(defn filtered-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist  (h/client-ops history)
            per-g (into (sorted-map)
                        (for [{:keys [group where matches?]} filtered-groups]
                          (let [ops     (h/filter (fn [op]
                                                    (case (:f op)
                                                      :enqueue (matches? (:value op))
                                                      :dequeue (= group (:group op))
                                                      false))
                                                  hist)
                                cons    (consumer-check test ops opts)
                                comps   (->> ops (h/filter #(and (= :dequeue (:f %))
                                                                 (not= :invoke (:type %)))))
                                ds      (mapcat :msgs comps)
                                foreign (->> ds (remove #(and (:v %) (matches? (:v %))))
                                             (mapv #(select-keys % [:v :txn :partition :offset])))
                                no-echo (when where
                                          (->> comps
                                               (filter #(and (= :ok (:type %))
                                                             (not (true? (:filtered %)))))
                                               (mapv #(select-keys % [:index :process :status :filtered]))))
                                hdrs    (header-mismatches headers ds)
                                pays    (payload-mismatches ds)]
                            [group {:valid?             (and (:valid? cons) (empty? foreign)
                                                             (empty? no-echo) (empty? hdrs)
                                                             (empty? pays))
                                    :where              where
                                    :consumer           cons
                                    :not-matching       (take 16 foreign)
                                    :not-matching-count (count foreign)
                                    :no-filtered-echo   (take 16 no-echo)
                                    :no-filtered-echo-count (count no-echo)
                                    :header-mismatch    (take 16 hdrs)
                                    :payload-mismatch   (take 16 pays)}])))]
        {:valid? (every? :valid? (vals per-g))
         :groups per-g}))))

(defn filtered-workload
  [opts]
  (reset! pad-bytes (long (:pad-bytes opts 0)))
  {:client          (map->Client {:invoke-fn filtered-invoke!
                                  :setup-fn  (fn [client test]
                                               (when (= (:node client) (first (:nodes test)))
                                                 (cluster-version-8! test)))})
   :checker         (checker/compose {:groups (filtered-checker), :framed (framed-checker)})
   :generator       (gen/mix [(enqueue-gen)
                              (fn [] {:f :dequeue, :value nil
                                      :group (:group (rand/nth filtered-groups))})])
   :final-generator (gen/each-thread
                      (->DrainEach (mapv #(hash-map :group (:group %)) filtered-groups) 8 {}))})

;; ---------------------------------------------------------------------------
;; W16: reading queues

(def readers
  [{:queue "jepsen-r1", :from ["jepsen-0"], :where "data.kind = 'a'"
    :partition-by "same", :start "now", :at :setup, :matches? #(= 0 (mod % 3))}
   {:queue "jepsen-r2", :from ["jepsen-0" "jepsen-1"], :where nil
    :partition-by "data.cust", :start "now", :at :setup, :matches? (constantly true)}
   {:queue "jepsen-r3", :from ["jepsen-0"], :where "headers.region = 'x'"
    :partition-by "same", :start "all", :at :mid, :matches? even?}])

(def reader-group "g")

(defn reader-options
  [r]
  (cond-> {:from        (if (= 1 (count (:from r))) (first (:from r)) (:from r))
           :partitionBy (:partition-by r)
           :start       (:start r)
           :leaseTime   5
           :retryLimit  1000000}
    (:where r) (assoc :where (:where r))))

(defn reader-partition
  "Where reader r must hold message v pushed to source partition p."
  [r p v]
  (if (= "same" (:partition-by r))
    p
    (:cust (payload v))))

(defn reader-partitions
  [r]
  (if (= "same" (:partition-by r))
    (mapv partition-name (range partitions))
    (mapv #(str "c" %) (range 8))))

(defn all-queues
  "[queue partitions] of every source and reader."
  [test]
  (concat (for [q (take 2 (:queue-names test))]
            [q (mapv partition-name (range partitions))])
          (for [r readers] [(:queue r) (reader-partitions r)])))

(defn- make-reader!
  "Configures reader r (create, or a no-op update that also continues its
  catch-up). :ok with :catching-up, or :fail / :info."
  [client test op r]
  (let [res (request client :post (str (:base client) "/api/v1/configure")
                     {:queue (:queue r), :options (reader-options r)}
                     (+ 15000 (:client-timeout-ms test)))]
    (cond
      (:exception res) (assoc op :type (exception-type res) :error [:configure (:type (:exception res))])
      (= 200 (:status res)) (assoc op :type :ok
                                   :catching-up (boolean (or (:catchingUp (:body res))
                                                             (get-in (:body res) [:route :catchingUp])))
                                   :route (get-in (:body res) [:route]))
      :else (assoc op :type (if (#{400 404 409} (:status res)) :fail :info)
                   :error [:configure (:status res) (:body res)]))))

(defn- reading-invoke!
  [client test op]
  (case (:f op)
    :enqueue
    (let [p   (rand/long partitions)
          res (push! client test (:queue op) (partition-name p) (:value op))]
      (merge (assoc op :partition (partition-name p)) res))

    :dequeue
    (pop-and-ack! client test op (:reader op) reader-group nil)

    :make-reader
    (let [; The ask is on its way: the generator stops offering it. It is
          ; marked here, where the op really runs, and not where it is
          ; generated (see reading-workload).
          _   (when-let [a (:asked client)] (reset! a true))
          res (make-reader! client test op (some #(when (= (:reader op) (:queue %)) %) readers))]
      ; From now on it is popped (it is not before: a pop would make it an
      ; ordinary queue).
      (when (and (= :ok (:type res)) (:made client))
        (swap! (:made client) conj (:reader op)))
      ; Not made (a kill, a 503): ask again, a second later (rt-reading-lz-kill
      ; 2026-11-14: the one ask failed, the reader was made only at the final
      ; catch-up, and :make-reader never answered :ok). A refusal the broker
      ; decided (400, 404, 409) is not asked again: it would be refused for
      ; ever, and :make-reader without an :ok is then the finding.
      (when (and (not= :ok (:type res)) (:asked client)
                 (not (#{400 404 409} (second (:error res)))))
        (when-let [t (:again client)]
          (reset! t (+ (System/nanoTime) 1000000000)))
        (reset! (:asked client) false))
      res)

    :finish-catch-up
    ; Configure again until the reader answers catchingUp false (each call
    ; spends up to 10 s on the catch-up): only then does it take live links,
    ; and only then is it whole.
    (let [r (some #(when (= (:reader op) (:queue %)) %) readers)]
      (loop [i 0, last-op nil]
        (let [res (make-reader! client test op r)]
          (cond
            (and (= :ok (:type res)) (not (:catching-up res))) (assoc res :tries (inc i))
            (< i 12) (do (Thread/sleep 500) (recur (inc i) res))
            :else (assoc (or res op) :type :fail, :error [:still-catching-up (inc i)])))))

    :read
    (read-queues! client test op (all-queues test))))

(defn- reading-setup!
  [client test]
  (when (= (:node client) (first (:nodes test)))
    (cluster-version-8! test)
    (doseq [r readers :when (= :setup (:at r))]
      (info "reader" (:queue r) (configure! client test (:queue r) (reader-options r))))))

(defn- source-records
  "{txn {:queue :partition :offset :v ...}} of the sources in a final read."
  [read sources]
  (into {}
        (for [q sources
              [p recs] (get read q)
              rec recs]
          [(:txn rec) (assoc rec :queue q :partition p)])))

(defn- order-violations
  "In one reader partition's records, the messages of one source partition
  out of their source order."
  [src recs]
  (->> recs
       (keep (fn [rec] (when-let [s (src (:txn rec))]
                         [[(:queue s) (:partition s)] (:offset s) (:txn rec)])))
       (group-by first)
       (keep (fn [[sp xs]]
               (let [offs (map second xs)]
                 (when-not (apply <= offs)
                   {:source sp, :source-offsets (vec offs)}))))
       vec))

(defn- realtime-violations
  "Records placed after one whose push was invoked after theirs completed
  (both :ok pushes): live links follow commit order, and commit order
  follows real time."
  [times recs]
  (loop [recs recs, max-inv nil, max-at nil, out []]
    (if-let [rec (first recs)]
      (let [{:keys [inv comp]} (times (:txn rec))
            out (if (and comp max-inv (< comp max-inv))
                  (conj out {:txn (:txn rec), :completed comp
                             :after max-at, :that-push-invoked max-inv})
                  out)
            [max-inv max-at] (if (and inv (or (nil? max-inv) (< max-inv inv)))
                               [inv (:txn rec)]
                               [max-inv max-at])]
        (recur (next recs) max-inv max-at out))
      out)))

(defn reading-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            read    (:value (last reads))
            ; Every node must read the same logs.
            differ  (->> reads
                         (remove #(= (:value %) read))
                         (mapv #(select-keys % [:node :index])))
            pushes  (->> hist (h/filter #(= :enqueue (:f %))))
            oks     (->> pushes (h/filter #(= :ok (:type %))))
            times   (into {}
                          (for [op oks]
                            [(txn-id (:value op))
                             {:inv (:time (h/invocation hist op)), :comp (:time op)}]))
            all-src (source-records read (take 2 (:queue-names test)))
            lost    (->> oks (remove #(all-src (txn-id (:value %))))
                         (mapv #(select-keys % [:value :queue :partition])))
            per-r   (into (sorted-map)
                          (for [r readers]
                            (let [src      (source-records read (:from r))
                                  want     (->> (vals src)
                                                (filter #((:matches? r) (:v %)))
                                                (map :txn) set)
                                  parts    (get read (:queue r))
                                  recs     (for [[p rs] parts, rec rs] (assoc rec :partition p))
                                  counts   (frequencies (map :txn recs))
                                  missing  (->> want (remove counts) sort vec)
                                  twice    (->> counts (filter #(< 1 (val %))) (map key) sort vec)
                                  extra    (->> (keys counts) (remove want) sort vec)
                                  misplaced (->> recs
                                                 (keep (fn [rec]
                                                         (when-let [s (src (:txn rec))]
                                                           (let [want-p (reader-partition r (:partition s) (:v s))]
                                                             (when (not= want-p (:partition rec))
                                                               {:txn (:txn rec), :in (:partition rec)
                                                                :belongs-in want-p})))))
                                                 vec)
                                  order    (->> parts
                                                (mapcat (fn [[p rs]]
                                                          (map #(assoc % :reader-partition p)
                                                               (order-violations src rs))))
                                                vec)
                                  realtime (if (= :setup (:at r))
                                             (->> parts
                                                  (mapcat (fn [[p rs]]
                                                            (map #(assoc % :reader-partition p)
                                                                 (realtime-violations times rs))))
                                                  vec)
                                             [])
                                  hdrs     (header-mismatches headers recs)
                                  pays     (payload-mismatches recs)
                                  cons-ops (h/filter #(and (= :dequeue (:f %)) (= (:queue r) (:reader %)))
                                                     hist)
                                  ; Consumer rules: no message acked twice,
                                  ; no redelivery after an ack, exclusive
                                  ; leases, order; and every message the
                                  ; reader holds acked by the end.
                                  qc       (checker/check (queue/queue-checker) test cons-ops opts)
                                  ; An :info pop answered but its ack's
                                  ; answer was lost (an I/O error, a timeout,
                                  ; 503 outcome_unknown): the ack may have
                                  ; applied, as W2's own checker counts it
                                  ; (rt-reading-kill/-pause/-deaf 2026-11-14:
                                  ; each such message never came back).
                                  acked    (->> cons-ops
                                                (h/filter #(#{:ok :info} (:type %)))
                                                (mapcat (fn [op] (if (= :info (:type op))
                                                                   (map #(update % :ack (fnil identity :unknown)) (:msgs op))
                                                                   (:msgs op))))
                                                (filter #(#{:acked :noop :unknown} (:ack %)))
                                                (map :txn) set)
                                  unconsumed (->> (keys counts) (remove acked) sort vec)
                                  made?    (or (= :setup (:at r))
                                               (some #(and (= :finish-catch-up (:f %))
                                                           (= :ok (:type %))
                                                           (= (:queue r) (:reader %)))
                                                     hist))]
                              [(:queue r)
                               {:valid?        (boolean (and made? (empty? missing) (empty? twice)
                                                    (empty? extra) (empty? misplaced)
                                                    (empty? order) (empty? realtime)
                                                    (empty? hdrs) (empty? pays)
                                                    (:valid? qc) (empty? unconsumed)))
                                :whole?        (boolean made?)
                                :held          (count counts)
                                :expected      (count want)
                                :missing       (take 16 missing)
                                :missing-count (count missing)
                                :twice         (take 16 twice)
                                :twice-count   (count twice)
                                :extra         (take 16 extra)
                                :extra-count   (count extra)
                                :misplaced     (take 16 misplaced)
                                :out-of-order  (take 16 order)
                                :realtime      (take 16 realtime)
                                :realtime-count (count realtime)
                                :header-mismatch  (take 16 hdrs)
                                :payload-mismatch (take 16 pays)
                                :consumer      (select-keys qc [:valid? :acked-twice-count
                                                                :redelivered-after-ack-count
                                                                :lease-overlap-count :pop-skip-count
                                                                :deliveries :acked])
                                :unconsumed    (take 16 unconsumed)
                                :unconsumed-count (count unconsumed)}])))]
        {:valid?       (boolean (and (seq reads) (empty? differ) (empty? lost)
                                     (every? :valid? (vals per-r))))
         :final-reads  (count reads)
         :nodes-differ differ
         :lost-at-source (take 16 lost)
         :lost-at-source-count (count lost)
         :readers      per-r}))))

(defn reading-workload
  [opts]
  (reset! pad-bytes (long (:pad-bytes opts 0)))
  (let [mid   (long (/ (:time-limit opts 300) 3))
        ; The readers that exist: those made at setup, and jepsen-r3 once its
        ; :make-reader answered. A pop of a queue that does not exist makes
        ; it an ordinary queue (answers 204), and a reading queue is then
        ; refused that name ("exists as an ordinary queue": the rig's first
        ; W16 runs, 2026-11-13).
        made  (atom (set (map :queue (filter #(= :setup (:at %)) readers))))
        ; jepsen-r3 is made a third of the way in, by the wall clock: a
        ; gen/phases inside gen/any waited for every thread to be free, which
        ; the mix never left them, and the op often never ran.
        t0    (atom nil)
        ; True from the moment a :make-reader runs until it fails, and for
        ; good once it answered :ok. The generator only READS it: the
        ; interpreter asks for an op, drops it when its time has not come
        ; (gen/stagger puts it in the future) and asks again, so a
        ; generator that marked the ask as made lost it. The gate of
        ; 2026-11-14 sent :make-reader in 6 of 13 reading runs and made the
        ; reader mid-test in 3; the others made it only at the final catch-up.
        asked (atom false)
        ; Not before this instant (nanoTime): one ask a second after a failed
        ; one, not one per generated op while a fault lasts.
        again (atom 0)
        due?  (fn []
                (let [now (System/nanoTime)]
                  (compare-and-set! t0 nil now)
                  (and (<= (* 1000000000 mid) (- now (long @t0)))
                       (not @asked)
                       (<= (long @again) now))))]
    {:client          (map->Client {:invoke-fn reading-invoke!
                                    :setup-fn  reading-setup!
                                    :made      made
                                    :asked     asked
                                    :again     again})
     :checker         (checker/compose {:readers (reading-checker), :framed (framed-checker)})
     :generator       (gen/mix [(enqueue-gen ["jepsen-0" "jepsen-1"])
                                (fn [] (if (due?)
                                         {:f :make-reader, :value nil, :reader "jepsen-r3"}
                                         {:f :dequeue, :value nil
                                          :reader (rand/nth (vec (sort @made)))}))])
     :final-generator (gen/phases
                        (gen/once {:f :finish-catch-up, :value nil, :reader "jepsen-r3"})
                        (gen/each-thread
                          (->DrainEach (mapv #(hash-map :reader (:queue %)) readers) 8 {}))
                        (gen/each-thread (gen/once {:f :read, :value nil})))}))

;; ---------------------------------------------------------------------------
;; W17: link in a transaction

(def link-group "fw")

(defn- link-op
  "A link of popped message m into the target queue, same partition."
  [test m]
  {:type      "link"
   :queue     (second (:queue-names test))
   :partition (:partition m)
   :from      {:partitionId   (:pid m)
               :offset        (:offset m)
               :transactionId (:txn m)
               :consumerGroup link-group}
   :headers   {:hop "1"}})

(defn- txn!
  "POST /api/v1/transaction: {:type ... :body ...}."
  [client test body]
  (let [t (request client :post (str (:base client) "/api/v1/transaction")
                   body (:client-timeout-ms test))]
    (cond
      (:exception t) {:type (exception-type t), :error [:txn (:type (:exception t))]}
      (and (= 200 (:status t)) (true? (:success (:body t)))) {:type :ok, :body (:body t)}
      (= 200 (:status t)) {:type :fail, :body (:body t)
                           :error [:rolled-back (select-keys (:body t) [:reason :error])]}
      :else {:type (log/push-failure test (:status t) (:body t))
             :error [:txn (:status t) (:body t)]})))

(defn- link-results
  "The link operations' results of a transaction answer."
  [body]
  (->> (:results body)
       (filter #(or (contains? % :duplicate) (contains? % :messageId)))
       vec))

(defn- forward!
  [client test op]
  (let [url (str (:base client) "/api/v1/pop/queue/" (enc (first (:queue-names test)))
                 "?consumerGroup=" link-group "&batch=1&leaseSeconds=" (:w2-lease test 3)
                 "&subscriptionMode=all&wait=true&timeout=" (if (:drain? op) 1000 300))
        r   (request client :get url nil (+ 1000 (:client-timeout-ms test)))]
    (cond
      (:exception r) (assoc op :type :fail, :msgs [], :error [:pop (:type (:exception r))])
      (= 204 (:status r)) (assoc op :type :ok, :msgs [])
      (not= 200 (:status r)) (assoc op :type :fail, :msgs [], :error [:pop (:status r) (:body r)])
      :else
      (if-let [m (some-> (:messages (:body r)) first msg->delivery)]
        (let [t (txn! client test
                      {:operations [{:type          "ack"
                                     :transactionId (:txn m)
                                     :partitionId   (:pid m)
                                     :leaseId       (:lease m)
                                     :consumerGroup link-group}
                                    (link-op test m)]})]
          (when (= :ok (:type t))
            (swap! (:forwarded client) conj m))
          (cond-> (assoc op :type (:type t), :value (:v m), :msgs [m]
                         :links (link-results (:body t)))
            (:error t) (assoc :error (:error t))))
        (assoc op :type :ok, :msgs [])))))

(defn- relink!
  "Links a message this client already forwarded once more: the broker must
  answer duplicate: true and keep one."
  [client test op]
  (if-let [m (when (seq @(:forwarded client)) (rand/nth (vec @(:forwarded client))))]
    (let [t (txn! client test {:operations [(link-op test m)]})]
      (cond-> (assoc op :type (:type t), :value (:v m), :links (link-results (:body t)))
        (:error t) (assoc :error (:error t))))
    (assoc op :type :fail, :error :nothing-forwarded-yet)))

(defn- link-invoke!
  [client test op]
  (case (:f op)
    :enqueue (let [p (rand/long partitions)]
               (merge (assoc op :partition (partition-name p))
                      (push! client test (first (:queue-names test)) (partition-name p) (:value op))))
    :forward (forward! client test op)
    :relink  (relink! client test op)
    :read    (read-queues! client test op [[(second (:queue-names test))
                                            (mapv partition-name (range partitions))]])))

(defn link-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            read    (:value (last reads))
            differ  (->> reads (remove #(= (:value %) read)) (mapv #(select-keys % [:node :index])))
            target  (second (:queue-names test))
            recs    (for [[p rs] (get read target), rec rs] (assoc rec :partition p))
            counts  (frequencies (map :txn recs))
            pushes  (->> hist (h/filter #(= :enqueue (:f %))))
            ok-vs   (->> pushes (h/filter #(= :ok (:type %))) (map :value) set)
            maybe   (->> pushes (h/filter #(#{:ok :info} (:type %))) (map :value) set)
            missing (->> ok-vs (remove #(counts (txn-id %))) sort vec)
            twice   (->> counts (filter #(< 1 (val %))) (map key) sort vec)
            extra   (->> (keys counts) (remove #(maybe (parse-txn %))) sort vec)
            ; Where each push went (its completion names the partition).
            pushed-to (->> pushes
                           (h/filter #(#{:ok :info} (:type %)))
                           (map (juxt :value :partition))
                           (into {}))
            placed  (->> recs
                         (remove (fn [rec] (= (pushed-to (:v rec)) (:partition rec))))
                         (mapv #(select-keys % [:txn :partition])))
            hdrs    (header-mismatches #(assoc (headers %) :hop "1") recs)
            pays    (payload-mismatches recs)
            relinks (->> hist (h/filter #(and (= :relink (:f %)) (= :ok (:type %)))) vec)
            not-dup (->> relinks
                         (remove #(every? (comp true? :duplicate) (:links %)))
                         (mapv #(select-keys % [:index :value :links])))]
        {:valid?          (boolean (and (seq reads) (empty? differ) (empty? missing) (empty? twice)
                                        (empty? extra) (empty? placed) (empty? hdrs) (empty? pays)
                                        (empty? not-dup)))
         :final-reads     (count reads)
         :nodes-differ    differ
         :held            (count counts)
         :acknowledged    (count ok-vs)
         :missing         (take 16 missing)
         :missing-count   (count missing)
         :twice           (take 16 twice)
         :twice-count     (count twice)
         :extra           (take 16 extra)
         :misplaced       (take 16 placed)
         :header-mismatch (take 16 hdrs)
         :payload-mismatch (take 16 pays)
         :relinks         (count relinks)
         :relink-not-duplicate (take 16 not-dup)
         :forwards        (->> hist (h/filter #(= :forward (:f %))) (map :type) frequencies)
         :forward-errors  (->> hist (h/filter #(and (= :forward (:f %)) (:error %)))
                               (map #(let [e (:error %)] (if (vector? e) (vec (take 2 e)) e)))
                               frequencies)}))))

(defrecord DrainForward [need empties]
  gen/Generator
  (op [this test ctx]
    (when (< empties need)
      (let [op (gen/fill-in-op {:f :forward, :value nil, :drain? true} ctx)]
        (if (= :pending op) [:pending this] [op this]))))
  (update [this test ctx event]
    (if (and (= :forward (:f event)) (:drain? event) (not= :invoke (:type event)))
      (if (and (= :ok (:type event)) (empty? (:msgs event)))
        (DrainForward. need (inc empties))
        (DrainForward. need 0))
      this)))

(defn link-workload
  [opts]
  (reset! pad-bytes (long (:pad-bytes opts 0)))
  {:client          (map->Client {:invoke-fn link-invoke!
                                  :forwarded (atom [])
                                  :setup-fn  (fn [client test]
                                               (when (= (:node client) (first (:nodes test)))
                                                 (cluster-version-8! test)))})
   :checker         (link-checker)
   ; One enqueue stream (two would push the same values twice): a quarter of
   ; the ops push, half forward, a quarter link again.
   :generator       (gen/mix [(enqueue-gen)
                              (repeat {:f :forward, :value nil})
                              (repeat {:f :forward, :value nil})
                              (repeat {:f :relink, :value nil})])
   :final-generator (gen/phases
                      (gen/each-thread (->DrainForward 8 0))
                      (gen/each-thread (gen/once {:f :read, :value nil})))})
