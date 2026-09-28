(ns jepsen.queen.workload.timers
  "W10: timers. Timers on queue jepsen-tm, keyed t<k>; every schedule carries
  a version unique in the test (txn t<k>-<ver>, payload {k, ver}), so a
  delivered message names the one schedule it came from.

    :schedule [k ver d]  POST /api/v1/timers, one `schedule` op, delayMs d
                         (0-3 s). A new key, or a recent one (then it is a
                         reschedule if the timer is still pending).
    :cancel k            DELETE /api/v1/timers/jepsen-tm/t<k>: `cancelled`
                         names the txn it removed; `absent` means no timer was
                         pending (it may have fired).
    :consume             pop + ONE batch ack (`completed`) of what came.

  A timer's fire appends its message and deletes it in one entry, so:
    - a version is delivered from at most one offset (fired once);
    - a version a cancel reported removing is never delivered;
    - per key, what fired plus what was cancelled accounts exactly for the
      timers created (a `scheduled` answer creates one; `rescheduled` replaces
      the pending one): fires + cancels(ok) <= created(ok) + created(unknown),
      and fires + cancels(ok) + cancels(unknown) >= created(ok) — the final
      phase waits past every delay and drains, so none is left pending;
    - nothing is delivered before its delay has passed since the schedule was
      invoked (less a 300 ms margin); not judged under clock faults, where the
      cluster's clock itself jumps;
    - no message on the queue that no schedule sent, and none from a schedule
      that definitely failed."
  (:require [clojure.data.json :as json]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.db :as db]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.queue :as queue])
  (:import (java.net URLEncoder)
           (java.util Base64)))

(def queue-name "jepsen-tm")
(def group "g1")
(def max-delay-ms 3000)

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn- b64 [^String s]
  (.encodeToString (Base64/getEncoder) (.getBytes s "UTF-8")))

(defn- txn [k ver] (str "t" k "-" ver))

(defn- call
  [client method url body timeout-ms]
  (try (qh/request! (:http client) method url body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- failure
  "The completion type of a request that got no usable answer."
  [test r]
  (cond (= ::qh/refused (:type (:exception r))) :fail
        (:exception r)                           :info
        :else (log/push-failure test (:status r) (:body r))))

;; ---------------------------------------------------------------------------
;; Client

(defn- configure!
  [client]
  (util/await-fn
    (fn []
      (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/configure")
                           {:queue queue-name, :options db/queue-options} 10000)]
        (when-not (= 200 (:status r)) (throw (ex-info "configure failed" r)))
        r))
    {:timeout 60000, :retry-interval 1000, :log-message (str "configuring " queue-name)}))

(defn- schedule!
  [client test op]
  (let [[k ver d] (:value op)
        r (call client :post (str (:base client) "/api/v1/timers")
                [{:op       "schedule"
                  :queue    queue-name
                  :timerKey (str "t" k)
                  :delayMs  d
                  :payload  (b64 (json/write-str {:k k, :ver ver}))
                  :txn      (txn k ver)}]
                (:client-timeout-ms test))
        res (when (= 200 (:status r)) (first (:results (:body r))))]
    (cond
      (and res (:ok res)) (assoc op :type :ok, :status (:status res))
      res                 (assoc op :type :fail, :error [:verdict res])
      :else               (assoc op :type (failure test r)
                                 :error [:schedule (or (:type (:exception r)) (:status r)) (:body r)]))))

(defn- cancel!
  [client test op]
  (let [k (:value op)
        r (call client :delete (str (:base client) "/api/v1/timers/" (enc queue-name)
                                    "/" (enc (str "t" k)))
                nil (:client-timeout-ms test))]
    (if (= 200 (:status r))
      (assoc op :type :ok, :status (:status (:body r)), :txn (:txn (:body r)))
      (assoc op :type (failure test r)
             :error [:cancel (or (:type (:exception r)) (:status r)) (:body r)]))))

(defn- consume!
  [client test op]
  (let [wait (if (:drain? op) 1000 (rand/nth [nil 500]))
        r    (call client :get
                   (str (:base client) "/api/v1/pop/queue/" (enc queue-name)
                        "?consumerGroup=" group "&batch=5&leaseSeconds=3&subscriptionMode=all"
                        (if wait (str "&wait=true&timeout=" wait) "&wait=false"))
                   nil (+ (or wait 0) (:client-timeout-ms test)))
        msgs (when (= 200 (:status r))
               (mapv (fn [m] {:k       (:k (:data m))
                              :ver     (:ver (:data m))
                              :txn     (:transactionId m)
                              :pid     (:partitionId m)
                              :offset  (:offset m)
                              :lease   (:leaseId m)})
                     (:messages (:body r))))]
    (cond
      (:exception r)         (assoc op :type :fail, :msgs [], :error [:pop (:type (:exception r))])
      (= 204 (:status r))    (assoc op :type :ok, :msgs [])
      (not= 200 (:status r)) (assoc op :type :fail, :msgs [], :error [:pop (:status r)])
      (empty? msgs)          (assoc op :type :ok, :msgs [])
      :else
      (let [ar (call client :post (str (:base client) "/api/v1/ack/batch")
                     {:consumerGroup   group
                      :acknowledgments (mapv (fn [m] {:transactionId (:txn m)
                                                      :partitionId   (:pid m)
                                                      :leaseId       (:lease m)
                                                      :status        "completed"})
                                             msgs)}
                     (:client-timeout-ms test))]
        ; Whatever became of the ack, the pop delivered these: the checker
        ; counts deliveries, not acks.
        (assoc op :type :ok, :msgs msgs
               :acked? (= 200 (:status ar)))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (configure! this))

  (invoke! [this test op]
    (try
      (case (:f op)
        :schedule (schedule! this test op)
        :cancel   (cancel! this test op)
        (:consume :dequeue) (consume! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (if (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type (if (or (= ::qh/refused type) (#{:consume :dequeue} (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Generator

(defn generator
  "New timers, schedules and cancels of recent keys, and consumers."
  []
  (let [next-k   (atom -1)
        next-ver (atom 0)
        recent   (fn [] (let [top @next-k] (max 0 (- top (rand/long 20)))))
        new-one  (fn [] {:f :schedule
                         :value [(swap! next-k inc) (swap! next-ver inc)
                                 (rand/long (inc max-delay-ms))]})
        again    (fn [] (if (neg? @next-k)
                          (new-one)
                          {:f :schedule
                           :value [(recent) (swap! next-ver inc) (rand/long (inc max-delay-ms))]}))
        cancel   (fn [] (if (neg? @next-k)
                          (new-one)
                          {:f :cancel, :value (recent)}))
        consume  {:f :consume, :value nil}]
    (gen/mix [new-one new-one new-one again cancel
              (repeat consume) (repeat consume) (repeat consume) (repeat consume)])))

;; ---------------------------------------------------------------------------
;; Checker

(defn timers-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops      (h/client-ops history)
            clock?   (contains? (set (:nemesis test)) :clock)
            scheds   (->> ops (h/filter #(and (= :schedule (:f %)) (not= :invoke (:type %)))) vec)
            inv-of   (fn [op] (h/invocation history op))
            ; version -> {:k :d :type :status :inv}
            vers     (into {} (map (fn [op] (let [[k ver d] (:value op)]
                                              [ver {:k k, :d d, :type (:type op)
                                                    :status (:status op)
                                                    :inv (:time (inv-of op))}]))
                               scheds))
            cancels  (->> ops (h/filter #(and (= :cancel (:f %)) (not= :invoke (:type %)))) vec)
            ; versions a cancel reported removing
            removed  (->> cancels
                          (filter #(and (= :ok (:type %)) (= "cancelled" (:status %))))
                          (keep (fn [op] (some->> (:txn op) (re-find #"^t\d+-(\d+)$") second parse-long)))
                          set)
            dels     (->> ops (h/filter #(and (= :consume (:f %)) (= :ok (:type %)))) ;; drains too
                          (concat (h/filter #(and (= :dequeue (:f %)) (= :ok (:type %))) ops))
                          (mapcat (fn [op] (map #(assoc % :at (:time op)) (:msgs op))))
                          vec)
            fired    (->> dels (group-by :ver)
                          (map (fn [[ver ds]] [ver (set (map (juxt :pid :offset) ds))]))
                          (into {}))
            twice    (sort (keep (fn [[ver offs]] (when (< 1 (count offs)) ver)) fired))
            unknown  (sort (remove #(contains? vers %) (keys fired)))
            bad-txn  (->> dels (remove #(= (:txn %) (txn (:k %) (:ver %)))) (take 8) vec)
            cancelled-fired (sort (filter removed (keys fired)))
            failed-fired (sort (filter #(= :fail (:type (vers %))) (keys fired)))
            early    (if clock?
                       []
                       (->> dels
                            (keep (fn [d]
                                    (when-let [s (vers (:ver d))]
                                      (let [due (+ (:inv s) (* 1000000 (- (:d s) 300)))]
                                        (when (< (:at d) due)
                                          {:ver (:ver d), :k (:k d), :delay-ms (:d s)
                                           :early-ms (quot (- due (:at d)) 1000000)})))))
                            (take 16) vec))
            ; Per key: accounting of the timers created against their ends.
            by-k     (group-by (comp :k val) vers)
            fires-k  (frequencies (map (comp :k vers) (filter vers (keys fired))))
            cancel-k (fn [t] (->> cancels
                                  (filter #(case t
                                             :ok   (and (= :ok (:type %)) (= "cancelled" (:status %)))
                                             :info (= :info (:type %))))
                                  (map :value) frequencies))
            c-ok     (cancel-k :ok)
            c-info   (cancel-k :info)
            books    (->> by-k
                          (keep (fn [[k vs]]
                                  (let [vs      (map val vs)
                                        created (count (filter #(and (= :ok (:type %))
                                                                     (= "scheduled" (:status %))) vs))
                                        maybe   (count (filter #(= :info (:type %)) vs))
                                        f       (fires-k k 0)
                                        c       (c-ok k 0)
                                        ci      (c-info k 0)]
                                    (when (or (< (+ created maybe) (+ f c))
                                              (< (+ f c ci) created))
                                      {:k k, :created created, :maybe-created maybe
                                       :fired f, :cancelled c, :maybe-cancelled ci}))))
                          (sort-by :k) vec)]
        {:valid?            (and (seq dels) (empty? twice) (empty? unknown) (empty? bad-txn)
                                 (empty? cancelled-fired) (empty? failed-fired)
                                 (empty? early) (empty? books))
         :schedules         (count (filter #(= :ok (:type %)) scheds))
         :rescheduled       (count (filter #(= "rescheduled" (:status %)) scheds))
         :cancelled         (count removed)
         :absent            (count (filter #(= "absent" (:status %)) cancels))
         :fired             (count fired)
         :fired-twice       (take 16 twice)
         :fired-unscheduled (take 16 unknown)
         :bad-transaction-id bad-txn
         :cancelled-but-fired (take 16 cancelled-fired)
         :failed-schedule-fired (take 16 failed-fired)
         :early-fires       early
         :early-judged?     (not clock?)
         :accounting-errors (take 16 books)
         :accounting-error-count (count books)}))))

(defn workload
  [opts]
  {:client          (map->Client {})
   :checker         (timers-checker)
   :generator       (generator)
   :final-generator (gen/phases
                      (gen/sleep (+ 2 (quot max-delay-ms 1000)))
                      (gen/each-thread (queue/->DrainUntilEmpty 8 0)))})
