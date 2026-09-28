(ns jepsen.queen.workload.retention
  "W8: W2 (the lease queue, jepsen.queen.workload.queue) with completed
  retention on: a message every consumer group has consumed is deleted 2 s
  after it was pushed, by the background retention scan (retention_scan.rs,
  leader only), and the queue-log files it lived in are reclaimed per file
  (small files, a 1 s txn window, a 1 s maintenance tick).

  Retention may only delete what is consumed, so W2's checkers are the safety
  check: a message retention removed before its ack is a lost one
  (total-queue), and a pop past it a skip (queue).

  The final phase drains the queue, waits past the retention time, then reads
  every partition's logStartOffset from the client's node (POST /api/v1/fetch
  at offset 0): retention checker, the liveness half — the watermark moved on
  at least one partition."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.db :as db]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.queue :as queue]))

(def completed-retention-s 2)

(def queue-options
  (merge db/queue-options
         {:retentionEnabled          true
          :retentionSeconds          0
          :completedRetentionSeconds completed-retention-s
          :dedupWindowSeconds        5}))

(def db-env
  "Retention every second, queue-log files that roll at 64 KiB, and a txn
  window short enough that their records' hash lists expire in the test."
  {"RETENTION_INTERVAL"          "1000"
   "QUEEN_RAFT_SEGMENT_BYTES"    "65536"
   "QUEEN_RAFT_TXN_WINDOW_MIN_S" "1"})

(defn- configure!
  [client test]
  (let [q (queue/queue-name test)]
    (util/await-fn
      (fn []
        (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/configure")
                             {:queue q, :options queue-options} 10000)]
          (when-not (= 200 (:status r))
            (throw (ex-info "configure failed" r)))
          r))
      {:timeout 60000, :retry-interval 1000, :log-interval 10000,
       :log-message (str "configuring retention on " q)})))

(defn- read-watermarks!
  [client test op]
  (let [q     (queue/queue-name test)
        parts (map queue/partition-name (range (:w2-partitions test 8)))
        r     (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                           {:entries   (mapv (fn [p] {:queue q, :partition p, :offset 0}) parts)
                            :maxWaitMs 0}
                           (:client-timeout-ms test))]
    (if (= 200 (:status r))
      (assoc op :type :ok, :node (:node client)
             :value (into (sorted-map)
                          ; OFFSET_OUT_OF_RANGE (offset 0 is below the log
                          ; start: retention moved it) still carries both.
                          (keep (fn [e]
                                  (when (not= "UNKNOWN_TOPIC_OR_PARTITION" (:error e))
                                    [(str (:partition e))
                                     {:log-start (:logStartOffset e)
                                      :hwm       (:highWatermark e)}])))
                          (:entries (:body r))))
      (assoc op :type :fail, :error [(:status r) (:body r)]))))

(defrecord Client [inner]
  client/Client
  (open! [this test node]
    (let [c (client/open! (queue/client) test node)]
      (assoc this :inner c, :node node, :base (:base c), :http (:http c))))

  (setup! [this test]
    (configure! this test))

  (invoke! [this test op]
    (if (= :watermarks (:f op))
      (try (read-watermarks! this test op)
           (catch clojure.lang.ExceptionInfo e
             (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
               (assoc op :type :fail, :error [(:type (ex-data e))])
               (throw e))))
      (client/invoke! inner test op)))

  (teardown! [this test])

  (close! [this test]
    (client/close! inner test))

  client/Reusable
  (reusable? [this test] true))

(defn retention-checker
  "The watermark moved on: some partition's log start is above 0 after the
  drain and the wait (retention ran), and no log start is above its
  partition's high watermark."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [reads (->> history h/client-ops
                       (h/filter #(and (= :watermarks (:f %)) (= :ok (:type %))))
                       vec)
            moved (->> reads (mapcat (comp vals :value))
                       (filter #(some-> (:log-start %) pos?))
                       count)
            past  (->> reads
                       (mapcat (fn [op] (for [[p w] (:value op)
                                              :when (and (:log-start w) (:hwm w)
                                                         (< (:hwm w) (:log-start w)))]
                                          {:node (:node op), :partition p, :w w})))
                       vec)]
        {:valid?     (and (seq reads) (pos? moved) (empty? past))
         :reads      (into (sorted-map) (map (juxt :node :value) reads))
         :partitions-moved moved
         :log-start-past-hwm past}))))

(defn workload
  [opts]
  (let [w2 (queue/workload opts)]
    {:client          (map->Client {})
     :db-env          db-env
     :checker         (checker/compose {:w2        (:checker w2)
                                        :retention (retention-checker)})
     :generator       (:generator w2)
     :final-generator (gen/phases
                        (:final-generator w2)
                        (gen/sleep (+ 6 completed-retention-s))
                        (gen/each-thread {:f :watermarks, :value nil}))}))
