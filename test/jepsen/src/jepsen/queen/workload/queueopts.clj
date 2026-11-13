(ns jepsen.queen.workload.queueopts
  "W24 --workload queueopts --queue-opt delayed|window|maxwait: W2 (the lease
  queue, its clients and checkers) on a queue with one of the timing options
  no other test sets (writing-applications/queues.mdx):

    delayed  delayedProcessing 3 s: every new message waits 3 s after its
             push before anyone can take it
    window   windowBuffer 2 s: a partition's new messages wait until the
             first of them is 2 s old, then go together
    maxwait  maxWaitTimeSeconds 4: a message older than 4 s is removed,
             consumed or not (by the retention scan, every second here)

  On top of W2's rules (with maxwait, a message never delivered is not lost:
  it may have been removed), the checker rejects, judged only in tests
  without clock faults:
    delayed  a delivery whose pop completed less than 3 s after the push was
             sent
    window   a pop whose oldest message, of a partition, was pushed less
             than 2 s before the pop completed
    maxwait  a delivery by a pop sent more than 4 s + the scan (1 s) + 2 s
             of margin after the push was acknowledged
  Times are the client's; the broker counts from when it applies the push,
  which is later, so each rule can only be broken by the broker."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.db :as qdb]
            [jepsen.queen.workload.queue :as queue]))

(def s 1000000000)

(def options
  {:delayed {:delayedProcessing 3}
   :window  {:windowBuffer 2}
   :maxwait {:maxWaitTimeSeconds 4}})

(defn db-env
  [opt]
  (when (= :maxwait opt) {"RETENTION_INTERVAL" "1000"}))

(defrecord Client [inner]
  client/Client
  (open! [this test node]
    (assoc this :inner (client/open! inner test node)))

  (setup! [this test]
    ; The test queue again, with the option: db.clj configured it plain.
    (when (= (:node (:inner this)) (first (:nodes test)))
      (let [r (qh/request! (qh/client) :post (str (qh/base-url (first (:nodes test))) "/api/v1/configure")
                           {:queue (queue/queue-name test)
                            :options (merge qdb/queue-options (options (:queue-opt test)))}
                           15000)]
        (when-not (= 200 (:status r))
          (throw (ex-info "configure with the option failed" {:answer r}))))))

  (invoke! [this test op] (client/invoke! (:inner this) test op))
  (teardown! [this test])
  (close! [this test] (client/close! (:inner this) test))

  client/Reusable
  (reusable? [this test] true))

(defn timing-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [opt     (:queue-opt test)
            judged? (not (some #{:clock} (:faults test)))
            hist    (h/client-ops history)
            pushes  (->> hist (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                         (map (fn [op] [(:value op) {:inv (:time (h/invocation hist op)), :comp (:time op)}]))
                         (into {}))
            pops    (->> hist (h/filter #(and (= :dequeue (:f %)) (#{:ok :info} (:type %)))) vec)
            bad     (case opt
                      :delayed
                      (for [op pops, m (:msgs op)
                            :let [p (pushes (:v m))]
                            :when (and p (< (:time op) (+ (:inv p) (* 3 s))))]
                        {:v (:v m), :delivered-after-ms (quot (- (:time op) (:inv p)) 1000000)})
                      :window
                      (for [op pops
                            [part ms] (group-by :partition (:msgs op))
                            :let [first-inv (->> ms (keep #(:inv (pushes (:v %)))) (reduce min Long/MAX_VALUE))]
                            :when (and (< first-inv Long/MAX_VALUE)
                                       (< (:time op) (+ first-inv (* 2 s))))]
                        {:partition part, :values (mapv :v ms)
                         :oldest-pushed-ms-before (quot (- (:time op) first-inv) 1000000)})
                      :maxwait
                      (for [op pops, m (:msgs op)
                            :let [p   (pushes (:v m))
                                  inv (:time (h/invocation hist op))]
                            :when (and p (< (+ (:comp p) (* 7 s)) inv))]
                        {:v (:v m), :pop-sent-ms-after-push (quot (- inv (:comp p)) 1000000)}))
            bad     (vec bad)]
        {:valid?   (or (not judged?) (empty? bad))
         :judged?  judged?
         :option   opt
         :early-or-late (take 16 bad)
         :count    (count bad)}))))

(defn workload
  [opts]
  (let [opt   (keyword (or (:queue-opt opts) :delayed))
        inner (queue/workload opts)]
    {:client          (->Client (:client inner))
     :test-opts       {:queue-opt opt}
     :db-env          (db-env opt)
     :checker         (checker/compose
                        (cond-> {:timing (timing-checker)
                                 :queue  (queue/queue-checker)}
                          ; With maxwait an unread message may be removed:
                          ; total-queue would call it lost.
                          (not= :maxwait opt) (assoc :total-queue (queue/total-queue-checker))))
     :generator       (:generator inner)
     :final-generator (:final-generator inner)}))
