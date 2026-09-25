(ns jepsen.queen.core
  "Entry point: lein run test --bin /path/to/queen --workload log --nemesis ..."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [history :as h]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.tests.kafka :as kafka]
            [jepsen.queen [db :as db]]
            [jepsen.queen.workload [log :as log]]))

(def workloads
  {:log log/workload})

(def all-faults
  #{:pause :kill :partition :clock})

(def special-nemeses
  {:none []
   :all  [:pause :kill :partition :clock]})

(def db-targets
  #{:one :primaries :minority :majority :all})

(def partition-targets
  #{:one :primaries :majority :majorities-ring :minority-third})

(defn parse-comma-kws
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-nemesis-spec
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(defn perf-checker
  "The perf checker, without the bookkeeping ops."
  [perf-opts]
  (let [c (checker/perf perf-opts)]
    (reify checker/Checker
      (check [this test history opts]
        (checker/check c test
                       (h/remove (h/has-f? #{:assign :crash
                                             :debug-topic-partitions})
                                 history)
                       opts)))))

(defn test-name
  "A name without spaces (it is the store directory)."
  [opts]
  (str "queen-" (name (:workload opts))
       "-" (if (seq (:nemesis opts))
             (->> (:nemesis opts) (map name) sort (str/join ","))
             "none")
       (when (some #{:partition} (:nemesis opts))
         (str "_p=" (->> (:partition-targets opts) (map name) (str/join ","))))
       (when (some #{:kill :pause :clock} (:nemesis opts))
         (str "_t=" (->> (:db-targets opts) (map name) (str/join ","))))
       (when (:txn-sends? opts) "_txn")
       "_offload=" (if (:offload opts) "on" "off")
       "_lanes=" (:lanes opts)
       (when (seq (:env opts))
         (str "_env=" (->> (:env opts)
                           (map (fn [[k v]] (str (str/replace k #"^QUEEN_(RAFT_)?" "")
                                                 "=" v)))
                           (str/join ","))))))

(defn queen-test
  "A test map from parsed CLI options."
  [opts]
  (let [workload ((workloads (:workload opts))
                  (assoc opts :sub-via #{:assign}))
        db       (db/db)
        nopts    {:db        db
                  :nodes     (:nodes opts)
                  :faults    (set (:nemesis opts))
                  :partition {:targets (:partition-targets opts)}
                  :pause     {:targets (:db-targets opts)}
                  :kill      {:targets (:db-targets opts)}
                  :clock     {:targets (:db-targets opts)}
                  :interval  (:nemesis-interval opts)}
        ; Only the fault families in use: the packet and file-corruption
        ; packages are not part of P1, and the clock package's setup steps
        ; every node's clock, so it joins only when clock faults are asked for.
        nemesis  (nc/compose-packages
                   (cond-> [(nc/partition-package nopts)
                            (nc/db-package nopts)]
                     (contains? (:faults nopts) :clock)
                     (conj (nc/clock-package nopts))))
        fg       (:final-generator workload)]
    (merge tests/noop-test
           opts
           {:name        (test-name opts)
            :os          debian/os
            :db          db
            :client      (:client workload)
            :nemesis     (:nemesis nemesis)
            :sub-via     #{:assign}
            :txn?        false
            :ww-deps     true
            :raft-token  (str (random-uuid))
            :extra-env   (:env opts)
            :queue-names (mapv #(str "jepsen-" %) (range (:queues opts)))
            :client-timeout-ms (+ (:server-timeout-ms opts) 5000)
            :generator
            ((:wrap-generator workload identity)
             (gen/phases
               (->> (:generator workload)
                    (gen/stagger (/ (:rate opts)))
                    (gen/nemesis (:generator nemesis))
                    (gen/time-limit (:time-limit opts)))
               (gen/nemesis (:final-generator nemesis))
               (gen/log "Healed; waiting for recovery")
               (gen/sleep (:recovery-time opts))
               (gen/time-limit (:final-time-limit opts)
                               (gen/clients fg))))
            :checker     (checker/compose
                           {:stats    (kafka/stats-checker)
                            :perf     (perf-checker {:nemeses (:perf nemesis)})
                            :clock    (checker/clock-plot)
                            :ex       (checker/unhandled-exceptions)
                            :panic    (checker/log-file-pattern
                                        #"panicked at|NA-QLOG-I1|poison"
                                        "queen.log")
                            :workload (:checker workload)})
            :perf-opts   {:nemeses (:perf nemesis)}})))

(def cli-opts
  [[nil "--bin PATH" "Path (on the control node) of the queen binary to upload."
    :missing "--bin is required"]

   [nil "--db-targets TARGETS" "Comma-separated node specs for kill/pause/clock: one,primaries,minority,majority,all."
    :default [:one :primaries :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--dedup-index MODE" "QUEEN_RAFT_DEDUP_INDEX: txns (the product default), rows or segment. P0/P1 ran segment, as qc.sh did."
    :default "txns"]

   [nil "--env K=V,..." "Extra environment for every node, e.g. QUEEN_RAFT_PURGE_HOLD_S=5,QUEEN_RAFT_LOG_KEEP=16."
    :default {}
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (map #(str/split % #"=" 2))
                     (into (sorted-map))))]

   [nil "--final-time-limit SECONDS" "Upper bound on the final polls."
    :default 120
    :parse-fn read-string]

   [nil "--key-count N" "Active keys (partitions) at a time."
    :default 12
    :parse-fn parse-long]

   [nil "--lanes N" "QUEEN_LANES (planner lanes)."
    :default 16
    :parse-fn parse-long]

   [nil "--max-writes-per-key N" "Writes per key before it retires."
    :default 256
    :parse-fn parse-long]

   [nil "--nemesis FAULTS" "Comma-separated faults: pause,kill,partition,clock, or none/all."
    :default #{}
    :parse-fn parse-nemesis-spec
    :validate [(partial every? all-faults)
               (str "Faults must be in " all-faults " or " (keys special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations."
    :default 10
    :parse-fn read-string]

   [nil "--[no-]offload" "QUEEN_RAFT_CLIENT_OFFLOAD: followers serve their own clients (default on)."
    :default true]

   [nil "--partition-targets TARGETS" "Comma-separated: one,primaries,majority,majorities-ring,minority-third."
    :default [:one :primaries :majority :majorities-ring]
    :parse-fn parse-comma-kws
    :validate [(partial every? partition-targets) (cli/one-of partition-targets)]]

   [nil "--queues N" "Test queues; key k lives in queue jepsen-(k mod N)."
    :default 2
    :parse-fn parse-long]

   [nil "--rate HZ" "Target ops/sec across all clients."
    :default 100
    :parse-fn read-string]

   [nil "--recovery-time SECONDS" "Sleep between healing and the final polls."
    :default 10
    :parse-fn read-string]

   [nil "--server-timeout-ms MS" "POP_DEFAULT_TIMEOUT_MS on the nodes (the push deadline); the client waits 5 s more."
    :default 5000
    :parse-fn parse-long]

   [nil "--txn" "Log workload: every send op is ONE /api/v1/transaction of 1-4 pushes (several partitions and queues)."
    :id :txn-sends?
    :default false]

   ["-w" "--workload NAME" "Workload: log."
    :default :log
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  queen-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
