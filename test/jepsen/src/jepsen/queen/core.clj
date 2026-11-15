(ns jepsen.queen.core
  "Entry point: lein run test --bin /path/to/queen --workload log --nemesis ..."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.tests.kafka :as kafka]
            [jepsen.queen [db :as db]
                          [http :as qh]
                          [nemesis :as qn]]
            [jepsen.queen.workload [dedup :as dedup]
                                   [duppush :as duppush]
                                   [dlq :as dlq]
                                   [elle :as elle]
                                   [failover :as failover]
                                   [feed :as feed]
                                   [kafka :as kafka-w]
                                   [routing :as routing]
                                   [txnmix :as txnmix]
                                   [txnelle :as txnelle]
                                   [feedstream :as feedstream]
                                   [ephemeral :as ephemeral]
                                   [tenants :as tenants]
                                   [scoped :as scoped]
                                   [dlqops :as dlqops]
                                   [queueopts :as queueopts]
                                   [kvttl :as kvttl]
                                   [seek :as seek]
                                   [lifecycle :as lifecycle]
                                   [connectors :as connectors]
                                   [locks :as locks]
                                   [log :as log]
                                   [pipeline :as pipeline]
                                   [queue :as queue]
                                   [register :as register]
                                   [retention :as retention]
                                   [streams :as streams]
                                   [timers :as timers]]))

(def workloads
  {:log      log/workload         ; W1
   :queue    queue/workload       ; W2
   :register register/register-workload  ; W3
   :counter  register/counter-workload   ; W3b
   :claim    register/claim-workload     ; W3c
   :elle     elle/workload        ; W4
   :pipeline pipeline/workload    ; W5
   :dedup    dedup/workload       ; W6
   :dlq      dlq/workload         ; W7
   :retention retention/workload  ; W8
   :streams  streams/workload     ; W9
   :timers   timers/workload      ; W10
   :locks    locks/workload       ; W11
   :semaphore locks/semaphore-workload   ; W11b
   :failover failover/workload    ; W12, needs --standby-nodes
   :feed     feed/workload        ; W13
   :kafka    kafka-w/workload     ; W14, the Kafka facade
   :filtered routing/filtered-workload   ; W15, filtered groups (cluster version 8)
   :reading  routing/reading-workload    ; W16, reading queues (cluster version 8)
   :link     routing/link-workload       ; W17, link in a transaction (cluster version 8)
   :txnmix   txnmix/workload             ; W18, transactions with rollbacks on purpose
   :feedstream feedstream/workload       ; W19, the SDKs' feed streams as processes under test
   :ephemeral ephemeral/workload         ; W20, ephemeral queues (in memory, request/reply)
   :tenants  tenants/workload            ; W21, tenants (and raft groups) side by side
   :scoped   scoped/workload             ; W22, scoped keys and client tokens (--via-proxy)
   :dlqops   dlqops/workload             ; W23, ack retry, DLQ replay, message delete
   :queueopts queueopts/workload         ; W24, delayedProcessing, windowBuffer, maxWaitTimeSeconds
   :kvttl    kvttl/workload              ; W25, KV keys and claims with a ttl
   :seek     seek/workload               ; W26, a group sought to the end and back in time
   :lifecycle lifecycle/workload         ; W27, a queue deleted and made again under load
   :pgsource connectors/pgsource-workload ; W28, the PostgreSQL source (services on the control node)
   :pgsink   connectors/pgsink-workload   ; W29, the PostgreSQL sink
   :s3sink   connectors/s3sink-workload   ; W30, the S3 sink
   :duppush  duppush/workload   ; W31, copies of one push sent together (dedup in flight)
   :txnelle  txnelle/workload}) ; W32, Elle over /api/v1/transaction

(def all-faults
  #{:pause :kill :partition :clock :pause-kill :part-kill :bridge :leader-deaf
    :snap-kill :corrupt :restart :membership :packet :disk-full :upgrade
    :runner})

(def packet-behaviors
  "What --nemesis packet draws from (tc netem on the nodes' interface, all
  traffic between nodes): delay, loss, duplication, reordering and corruption
  on their own and together. jepsen.net/all-packet-behaviors has the default
  of each (50 ms +- 10 ms delay; 20% loss, duplication, reordering,
  corruption, correlated 75%)."
  [{:delay {}}
   {:delay {:time :200ms, :jitter :100ms}}
   {:loss {}}
   {:loss {:percent :50%}}
   {:duplicate {}}
   {:delay {}, :reorder {}}
   {:corrupt {}}
   {:delay {}, :loss {}, :duplicate {}, :reorder {}}
   {:rate {:rate :1mbit}}])

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

(defn lazyfs-checker
  "With --lazyfs: every node's lazyfs.log must show the cache never full (a
  full cache writes straight through and the power losses lose nothing), and
  says how many power losses actually dropped un-fsynced bytes."
  []
  (reify checker/Checker
    (check [this test history opts]
      (if-not (:lazyfs test)
        {:valid? true}
        (let [per-node
              (into (sorted-map)
                    (for [node (:nodes test)
                          :let [f (store/path test node "lazyfs.log")]
                          :when (.exists f)]
                      (let [lines  (str/split-lines (slurp f))
                            usages (keep #(some-> (re-find #"cache usage .* is ([0-9.]+)%" %)
                                                  second parse-double)
                                         lines)
                            ; One report per power loss; lazyfs prints a
                            ; running total after each inode, so a report's
                            ; total is its last one.
                            losses (->> lines
                                        (partition-by #(boolean (re-find #"report request submitted" %)))
                                        (remove #(re-find #"report request submitted" (first %)))
                                        (keep (fn [chunk]
                                                (some->> chunk
                                                         (keep #(re-find #"un-fsynced: (\d+) bytes" %))
                                                         last
                                                         second
                                                         parse-long))))]
                        [node {:max-usage-pct (reduce max 0.0 usages)
                               :clears        (count (filter #(re-find #"cache is cleared" %)
                                                             lines))
                               :lossy-clears  (count (filter pos? losses))
                               :lost-bytes    (reduce + 0 losses)}])))
              worst (reduce max 0.0 (map :max-usage-pct (vals per-node)))]
          {:valid?        (and (= (count per-node) (count (:nodes test)))
                               (< worst 99.0))
           :max-usage-pct worst
           :nodes         per-node})))))

(defn leftovers-checker
  "No snapshot directory left on a node after the test (db/log-files waits up
  to 30 s for transfers to finish before it lists the data directory): a
  `snapshots/send-*` left by a send openraft cancelled pins an LMDB copy and
  hard links to the queue logs until a restart; a `recv-*` is a received
  snapshot that was neither installed nor dropped."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [left (into (sorted-map)
                       (for [node (:nodes test)
                             :let [f (store/path test node "data-ls.txt")]
                             :when (.exists f)
                             :let [dirs (->> (str/split-lines (slurp f))
                                             (keep #(second (re-find #"^(\S*/snapshots/(?:send|recv)-[^/\s]+):$" %)))
                                             distinct
                                             vec)]
                             :when (seq dirs)]
                         [node dirs]))]
        {:valid?    (empty? left)
         :leftovers left}))))

(defn panic-checker
  "No node's queen.log has a panic, a poisoned lock or NA-QLOG-I1 (a committed
  queue-log record missing). One exception: the corrupt nemesis truncates its
  victim's queue logs on purpose, and NA-QLOG-I1 is how that node refuses to
  boot on them. A NA-QLOG-I1 line the nemesis recorded as the :why of a
  :refused trial on that node is listed under :excused and does not fail the
  test. One anywhere else (at runtime, after a restore, on another node) does,
  and so does a panic or a poisoned lock on any node.

  A second exception, with --nemesis disk-full only: the line of a node that
  stops because its raft log could not be written for lack of space
  (qn/full-disk-stop-line?). It says \"poisoned\" of the log writer, and it is
  the stop the docs promise at 100%, not a panic."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; :why is the refusal's log line, cut to 400 characters.
            ; With --corrupt-nodes N an op carries one trial per victim.
            refusals (->> history
                          (filter #(= :corrupt-file (:f %)))
                          (mapcat qn/corrupt-trials)
                          (filter #(= :refused (:outcome %)))
                          (keep (fn [v]
                                  (when-not (str/blank? (:why v))
                                    [(:node v) (:why v)])))
                          vec)
            refused? (fn [{:keys [node line]}]
                       (and (re-find #"NA-QLOG-I1" line)
                            (not (re-find #"panicked at|poison" line))
                            (some (fn [[n why]]
                                    (and (= n node) (str/starts-with? line why)))
                                  refusals)))
            found    (for [node (:nodes test)
                           :let [f (store/path test node "queen.log")]
                           :when (.exists f)
                           line (with-open [r (io/reader f)]
                                  (doall (filter #(re-find #"panicked at|NA-QLOG-I1|poison" %)
                                                 (line-seq r))))]
                       {:node node, :line line})
            excused? (fn [m] (or (refused? m)
                                 (qn/full-disk-stop-line? (:faults test) (:line m))))
            {excused true, matches false} (group-by (comp boolean excused?) found)]
        {:valid?  (empty? matches)
         :count   (count matches)
         :matches (vec matches)
         :excused (vec excused)}))))

(defn encryption-checker
  "With --encrypt: every node's log says the encryption service started. A
  missing or malformed QUEEN_ENCRYPTION_KEY stores payloads in plaintext
  without failing anything else, so a test that meant to run encrypted must
  prove it did."
  []
  (reify checker/Checker
    (check [this test history opts]
      (if-not (:encrypt test)
        {:valid? true}
        (let [seen (into (sorted-map)
                         (for [node (:nodes test)
                               :let [f (store/path test node "queen.log")]]
                           [node (and (.exists f)
                                      (with-open [r (io/reader f)]
                                        (boolean (some #(str/includes? % "encryption service initialized")
                                                       (line-seq r)))))]))]
          {:valid?      (every? true? (vals seen))
           :initialized seen})))))

(defn planner-checker
  "What the footprint scheduler (2.9 builds) says it did, summed over every
  node's queen.log: its line every 10 s, 'planner: cycles since the last
  line', carries parallel, serial and fallback cycles, transactions planned
  by shares (spread), second rounds taken back (taken_back) and commands
  planned late. A fallback is a conflict the route missed and a re-plan on
  one thread caught: safe, reported, not judged. Builds without the line
  report nothing."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [fields #{"parallel" "serial" "fallback" "spread" "taken_back" "late"}
            per-node
            (into (sorted-map)
                  (for [node (:nodes test)
                        :let [f (store/path test node "queen.log")]
                        :when (.exists f)]
                    [node (with-open [r (io/reader f)]
                            (reduce (fn [acc line]
                                      (if (str/includes? line "planner: cycles since the last line")
                                        (reduce (fn [acc [_ k v]]
                                                  (if (fields k) (update acc (keyword k) (fnil + 0) (parse-long v)) acc))
                                                acc
                                                (re-seq #"\b([a-z_]+)=(\d+)\b" line))
                                        acc))
                                    {}
                                    (line-seq r)))]))
            total  (apply merge-with + {} (vals per-node))]
        ; A fallback is safe (the run is planned again on one thread) but means
        ; the router sent a command to the wrong thread: worth a warning.
        (when (pos? (get total :fallback 0))
          (warn "planner fallbacks in" (name (:workload test)) ":" (:fallback total) total))
        ; --require-planner-spread: the test exists to drive transactions that
        ; span planner threads (spread > 0 shows the path ran at all).
        {:valid?   (or (not (:require-planner-spread test))
                       (pos? (get total :spread 0)))
         :total    total
         :per-node per-node}))))

(defn membership-agreement-checker
  "Every node's OWN voter set, read from it after the test (raft-state.json),
  is the same. A node that installed a snapshot naming an older membership
  kept that voter set until the next membership change, and votes by it
  (review 2026-09-28). Nodes that did not answer, or hold no cluster state,
  are listed, not judged."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [views  (into (sorted-map)
                         (for [node (:nodes test)
                               :let [f  (store/path test node "raft-state.json")
                                     st (when (.exists f)
                                          (try (json/read-str (slurp f) :key-fn keyword)
                                               (catch Exception _ nil)))]
                               :when (map? st)]
                           [node st]))
            judged (filter (comp :initialized val) views)
            ; With --standby-nodes there are two clusters, and each agrees
            ; with itself.
            sets   (->> judged
                        (group-by (fn [[node _]] (db/standby? test node)))
                        vals
                        (map (fn [views] (distinct (map (comp set :voters val) views)))))]
        {:valid?          (every? #(<= (count %) 1) sets)
         :voters          (into (sorted-map)
                                (map (fn [[n st]] [n (vec (sort (:voters st)))]) views))
         :not-initialized (vec (keep (fn [[n st]] (when-not (:initialized st) n)) views))
         :no-answer       (vec (remove (set (keys views)) (:nodes test)))}))))

(defn stats-checker-without
  "Jepsen's stats checker (an :f that never answered :ok is invalid), over
  the history without the ops of `fs`: those a workload sends expecting a
  refusal (W22's :push-other and :kv-other: one answered :ok there is the
  workload's own violation, which its checker judges)."
  [fs]
  (let [c (kafka/stats-checker)]
    (if (empty? fs)
      c
      (reify checker/Checker
        (check [this test history opts]
          (checker/check c test (h/remove #(contains? fs (:f %)) history) opts))))))

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
       (when (and (= :kafka (:workload opts)) (seq (:sub-via-kafka opts)))
         (str "_via=" (->> (:sub-via-kafka opts) (map name) sort (str/join ","))))
       (when (pos? (long (:batch-sends opts 0)))
         (str "_batch=" (:batch-sends opts)))
       (when (pos? (long (:standby-nodes opts 0)))
         (str "_standby=" (:standby-nodes opts) "_" (name (:failover opts))
              (when (:standby-seed opts) "_seeded")
              (when (:w12-kv opts) "_kv")))
       (when (= :leader (:kv-route opts)) "_kv-leader")
       (when-not (:kv-lift opts) "_kv-default-limits")
       (when (some #{:corrupt} (:nemesis opts))
         (str "_c=" (->> (:corrupt-classes opts) distinct (map name) (str/join ","))
              "_" (name (:corrupt-remedy opts))
              (when (< 1 (long (:corrupt-nodes opts 1)))
                (str "_x" (:corrupt-nodes opts)))))
       (when (some #{:upgrade} (:nemesis opts))
         (str "_up=" (name (:upgrade-mode opts :once))))
       (when (:lazyfs opts) "_lazyfs")
       (when (:feed-relay opts) "_relay")
       (when (pos? (long (:tenants opts 0))) (str "_tenants=" (:tenants opts)))
       (when (< 1 (long (:raft-groups opts 1))) (str "_groups=" (:raft-groups opts)))
       (when (:encrypt opts) "_encrypted")
       (when (:via-proxy opts) "_proxy")
       (when (:binary opts) "_binary")
       (when (pos? (long (:pad-bytes opts 0))) (str "_pad=" (:pad-bytes opts)))
       (when (= "none" (:w2-group opts)) "_queue-mode")
       (when (< 1 (long (:w2-groups opts 1))) (str "_groups=" (:w2-groups opts)))
       (when (:w2-extend opts) "_extend")
       (when (= :queueopts (:workload opts)) (str "_opt=" (name (:queue-opt opts))))
       (when (= :duppush (:workload opts))
         (str "_copies=" (:dup-copies opts 4)
              (when (not= 0.5 (double (:dup-fresh opts 0.5))) (str "_fresh=" (:dup-fresh opts)))
              (when (seq (:dup-kinds opts))
                (str "_kinds=" (->> (:dup-kinds opts) (map name) (str/join ","))))))
       (when (:timers-in-txn opts) "_in-txn")
       (when (= :feedstream (:workload opts))
         (str "_sdks=" (->> (:feed-sdks opts) (map name) (str/join ","))))
       (when (:disk-hog opts) "_hog")
       (when (seq (:slow-fsync-nodes opts))
         (str "_slowfsync=" (str/join "," (:slow-fsync-nodes opts))
              ":" (:slow-fsync-ms opts)))
       (when (not= 86400 (:dedup-window opts 86400))
         (str "_dedupwin=" (:dedup-window opts)))
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
  (let [standby  (long (:standby-nodes opts 0))
        _        (when (and (= :failover (:workload opts))
                            (not (< 0 standby (count (:nodes opts)))))
                   (throw (ex-info (str "--workload failover needs --standby-nodes between 1 and "
                                        (dec (count (:nodes opts))))
                                   {:standby-nodes standby, :nodes (:nodes opts)})))
        _        (when (pos? (long (:batch-sends opts 0)))
                   (when-not (#{:log :dedup} (:workload opts))
                     (throw (ex-info "--batch-sends is for the log and dedup workloads"
                                     {:workload (:workload opts)})))
                   (when (:txn-sends? opts)
                     (throw (ex-info "--batch-sends and --txn exclude each other" {}))))
        workload ((workloads (:workload opts))
                  (assoc opts :sub-via #{:assign}))
        ; A workload's own test-map keys (W14: :txn?, :sub-via and the facade
        ; turned on in the nodes); every other workload keeps assign, no txn.
        topts    (merge {:sub-via #{:assign}, :txn? false}
                        (:test-opts workload))
        db       (db/db)
        ; --via-proxy: every client request goes to the embedded proxy's port
        ; with the bootstrap API key; the harness's own calls stay on 6632.
        proxy-key (str "qk_jepsen_" (apply str (repeatedly 43 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"))))
        _        (if (:via-proxy opts)
                   (do (reset! qh/client-port db/proxy-port)
                       (reset! qh/client-headers {"authorization" (str "Bearer " proxy-key)}))
                   (do (reset! qh/client-port qh/http-port)
                       (reset! qh/client-headers {})))
        nopts    {:db        db
                  :nodes     (:nodes opts)
                  :faults    (set (:nemesis opts))
                  :partition {:targets (:partition-targets opts)}
                  :pause     {:targets (:db-targets opts)}
                  :kill      {:targets (:db-targets opts)}
                  :clock     {:targets (:db-targets opts)}
                  :packet    {:targets   (:db-targets opts)
                              :behaviors packet-behaviors}
                  :interval  (:nemesis-interval opts)
                  :phase     (:composite-phase opts)
                  :corrupt-classes (:corrupt-classes opts)
                  :upgrade-interval (:upgrade-interval opts)
                  :upgrade-mode     (:upgrade-mode opts)}
        ; Only the fault families in use: the packet and file-corruption
        ; packages are not part of P1, and the clock package's setup steps
        ; every node's clock, so it joins only when clock faults are asked for.
        nemesis  (nc/compose-packages
                   (cond-> [(nc/partition-package nopts)
                            (nc/db-package nopts)]
                     (contains? (:faults nopts) :clock)
                     (conj (nc/clock-package nopts))

                     (contains? (:faults nopts) :pause-kill)
                     (conj (qn/pause-kill-package nopts))

                     (contains? (:faults nopts) :part-kill)
                     (conj (qn/part-kill-package nopts))

                     (contains? (:faults nopts) :bridge)
                     (conj (qn/bridge-package nopts))

                     (contains? (:faults nopts) :leader-deaf)
                     (conj (qn/leader-deaf-package nopts))

                     (contains? (:faults nopts) :snap-kill)
                     (conj (qn/snap-kill-package nopts))

                     (contains? (:faults nopts) :corrupt)
                     (conj (qn/corrupt-package nopts))

                     (contains? (:faults nopts) :restart)
                     (conj (qn/graceful-restart-package nopts))

                     (contains? (:faults nopts) :membership)
                     (conj (qn/membership-package nopts))

                     (contains? (:faults nopts) :packet)
                     (conj (nc/packet-package nopts))

                     (contains? (:faults nopts) :disk-full)
                     (conj (qn/disk-full-package nopts))

                     (contains? (:faults nopts) :upgrade)
                     (conj (qn/upgrade-package nopts))

                     ; A workload's own nemesis steps (W12: the failover).
                     (:nemesis-package workload)
                     (conj (:nemesis-package workload))))
        fg       (:final-generator workload)]
    (merge tests/noop-test
           opts
           ; A workload's other test-map keys (W21: :tenants).
           (dissoc topts :sub-via :txn? :kafka-facade?)
           {:name        (test-name opts)
            :os          debian/os
            :db          db
            :client      (:client workload)
            :nemesis     (:nemesis nemesis)
            :sub-via     (:sub-via topts)
            :txn?        (:txn? topts)
            :kafka-facade? (boolean (:kafka-facade? topts))
            :ww-deps     true
            :raft-token  (str (random-uuid))
            ; --standby-nodes: what a standby presents to read its source.
            :link-token  (str (random-uuid))
            ; --encrypt: one at-rest key for every node of the test.
            :encryption-key (apply str (repeatedly 64 #(rand-nth "0123456789abcdef")))
            ; --via-proxy: the embedded proxy's secrets and its bootstrap key.
            :proxy-key      proxy-key
            :proxy-cp-token (str (random-uuid))
            :proxy-secret   (str (random-uuid) (random-uuid))
            ; The workload's own node settings (the KV workloads lift the KV
            ; rate ladder, unless --no-kv-lift), then --env on top.
            :extra-env   (merge (when (:kv-lift opts) (:db-env workload))
                                (:env opts))
            :queue-names (mapv #(str "jepsen-" %) (range (:queues opts)))
            ; The fault names, for a checker that must know them: :nemesis
            ; above is the nemesis itself.
            :faults      (set (:nemesis opts))
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
                           {:stats    (stats-checker-without (:refused-by-design workload #{}))
                            :perf     (perf-checker {:nemeses (:perf nemesis)})
                            :clock    (checker/clock-plot)
                            :ex       (checker/unhandled-exceptions)
                            :panic    (panic-checker)
                            :lazyfs   (lazyfs-checker)
                            :leftovers (leftovers-checker)
                            :members  (membership-agreement-checker)
                            :planner  (planner-checker)
                            :encryption (encryption-checker)
                            :faults   (qn/fault-summary-checker)
                            :workload (:checker workload)})
            :perf-opts   {:nemeses (:perf nemesis)}})))

(def cli-opts
  [[nil "--bin PATH" "Path (on the control node) of the queen binary to upload."
    :missing "--bin is required"]

   [nil "--bin-old PATH" "--nemesis upgrade: the release every node starts on; the nemesis restarts them one at a time on --bin while the clients run."]

   [nil "--upgrade-interval SECONDS" "--nemesis upgrade: seconds between two node upgrades."
    :default 40
    :parse-fn read-string]

   [nil "--upgrade-mode MODE" "--nemesis upgrade: once (each node old -> new once, the realistic rolling upgrade) or flap (random nodes, both ways: only for two releases of one cluster version). With --standby-nodes: standby-first (the standby's nodes, then the source's: the documented order) or source-first (the other way: the standby waits until its own nodes are new), each among the nodes that run."
    :default :once
    :parse-fn keyword
    :validate [#{:once :flap :standby-first :source-first}
               "once, flap, standby-first or source-first"]]

   [nil "--corrupt-nodes N" "The corrupt nemesis damages N nodes at the same time (2 of 5: a minority, which must lose nothing)."
    :default 1
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--disk-full-keep BYTES" "--nemesis disk-full: bytes left free when a disk is filled (0: the last block)."
    :default 0
    :parse-fn parse-long]

   [nil "--batch-sends N" "log and dedup workloads: pushes of up to N messages, all to ONE partition (log: an op's sends to its most used key; dedup: 2 to N distinct ids of one partition), so the broker writes their outcomes as runs (catalogue version 7). 0: one message a push."
    :default 0
    :parse-fn parse-long
    :validate [#(or (zero? %) (<= 2 % 64)) "0, or 2 to 64"]]

   [nil "--db-targets TARGETS" "Comma-separated node specs for kill/pause/clock: one,primaries,minority,majority,all."
    :default [:one :primaries :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--composite-phase SECONDS" "pause-kill / part-kill: how long each phase (minority behind; new majority) lasts."
    :default 20
    :parse-fn read-string]

   [nil "--disk-hog" "Run a loop of O_DSYNC writes on every node's disk for the whole test (slow fsyncs)."
    :default false]

   [nil "--disk-hog-bs SIZE" "Block size of the --disk-hog writes (dd bs=)."
    :default "256k"]

   [nil "--corrupt-node NODE" "The one node the corrupt nemesis damages (default: random)."]

   [nil "--corrupt-classes CLASSES" "Comma-separated file classes the corrupt nemesis draws from (repeats weigh): qlog, store (data.mdb), state, seg."
    :default qn/default-corrupt-classes
    :parse-fn parse-comma-kws
    :validate [(partial every? #{:qlog :store :state :seg}) "qlog, store, state or seg"]]

   [nil "--corrupt-remedy REMEDY" "What a node that refused its damage gets: restore (the copy taken before the damage), rejoin (removed, wiped, rejoins through the membership API) or mix."
    :default :mix
    :parse-fn keyword
    :validate [#{:restore :rejoin :mix} "restore, rejoin or mix"]]

   [nil "--corrupt-watch SECONDS" "How long the corrupt nemesis watches a node that booted on damage for a runtime exit."
    :default 15
    :parse-fn read-string]

   [nil "--dedup-index MODE" "QUEEN_RAFT_DEDUP_INDEX: txns (the product default), rows or segment. P0/P1 ran segment, as qc.sh did."
    :default "txns"]

   [nil "--dedup-window SECONDS" "dedupWindowSeconds of the test queues (default 86400: longer than any test). A short one, with QUEEN_RAFT_TXN_WINDOW_MIN_S as short, lets a push's index row leave the store during the test (cluster version 6), so a consumer behind it reads from the queue log."
    :default 86400
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

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

   [nil "--lazyfs" "Put QUEEN_RAFT_DIR on lazyfs: every kill becomes a power loss (un-fsynced writes are lost)."
    :default false]

   [nil "--feed-relay" "W13: every feed read sends x-queen-feed-relay: 1 (as the proxy does), so a leader relays it to a follower instead of answering 421."
    :default false]

   [nil "--lazyfs-cache SIZE" "lazyfs cache size; each node pre-allocates it in RAM. A full cache writes through (toothless)."
    :default "1GB"]

   [nil "--lanes N" "QUEEN_LANES (planner lanes)."
    :default 16
    :parse-fn parse-long]

   [nil "--max-writes-per-key N" "Writes per key before it retires."
    :default 256
    :parse-fn parse-long]

   [nil "--nemesis FAULTS" "Comma-separated faults: pause,kill,partition,clock,pause-kill,part-kill,bridge,leader-deaf,snap-kill,corrupt,restart (graceful), membership (remove, wipe, rejoin through the admin API), runner (feedstream: kill or pause the active SDK runner), packet (tc netem: delay, loss, duplication, reordering, corruption, rate), disk-full (the data disk filled to the last block, then freed), upgrade (a rolling upgrade from --bin-old to --bin while the clients run), or none/all."
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

   [nil "--slow-fsync-nodes NODES" "A slow disk on these nodes only (QUEEN_TEST_FSYNC_DELAY_MS: the queue-log syncer waits before every fsync), so they apply entries well before their fsync. With --lazyfs and kills: the slow-disk power loss."
    :default []
    :parse-fn #(vec (remove str/blank? (str/split % #",")))]

   [nil "--slow-fsync-ms MS" "--slow-fsync-nodes: the wait before each fsync."
    :default 300
    :parse-fn parse-long]

   [nil "--server-timeout-ms MS" "POP_DEFAULT_TIMEOUT_MS on the nodes (the push deadline); the client waits 5 s more."
    :default 5000
    :parse-fn parse-long]

   [nil "--txn" "Log workload: every send op is ONE /api/v1/transaction of 1-4 pushes (several partitions and queues). Kafka workload: every op is ONE producer transaction (sends, polls and their offsets)."
    :id :txn-sends?
    :default false]

   [nil "--tenants N" "Tenants side by side (QUEEN_TENANCY_HEADER, x-queen-tenant): the tenants workload uses this many (default 3 for it)."
    :default 0
    :parse-fn parse-long]

   [nil "--raft-groups N" "QUEEN_RAFT_GROUPS: raft groups per node (group g on the raft port + g); with --tenants, tenant i is pinned to group i mod N."
    :default 1
    :parse-fn parse-long
    :validate [#(<= 1 % 64) "1 to 64"]]

   [nil "--binary" "log, queue and pipeline workloads: the binary bodies of 2.3.0 (a binary push, a framed pop, a binary transaction) in place of JSON. filtered and reading workloads: framed pops (a record with headers or a link must come back as JSON)."
    :default false]

   [nil "--pad-bytes N" "filtered, reading and link workloads: a pad of N characters in every payload, so a record of one message is compressed in the queue log (past 512 bytes)."
    :default 0
    :parse-fn parse-long
    :validate [#(<= 0 % 65536) "0 to 65536"]]

   [nil "--via-proxy" "Clients go through the embedded proxy (QUEEN_PROXY_EMBEDDED, port 6711) with its bootstrap API key, as applications are told to; health, membership and link calls stay on the broker's port."
    :default false]

   [nil "--encrypt" "QUEEN_ENCRYPTION_KEY on every node and encryptionEnabled on the test queues; the encryption checker proves every node started its encryption service."
    :default false]

   [nil "--require-planner-spread" "Fail the test unless the footprint scheduler's log lines show transactions planned by shares (spread > 0): for the tests that exist to drive that path (txnmix and txnelle with QUEEN_PLANNER_THREADS above 1)."
    :default false]

   [nil "--feed-sdks SDKS" "feedstream workload: the SDKs whose runners run the queries, spread over the nodes (js, python, go, rust)."
    :default [:js]
    :parse-fn parse-comma-kws
    :validate [(partial every? #{:js :python :go :rust}) "js, python, go and/or rust"]]

   [nil "--runner-bundle PATH" "feedstream workload: the runner bundle on the control node (test/jepsen/rig/build-runners.sh)."
    :default "/root/runners.tgz"]

   [nil "--sub-via-kafka METHODS" "Kafka workload: how consumers take partitions, assign and/or subscribe (classic consumer groups)."
    :default [:assign]
    :parse-fn parse-comma-kws
    :validate [(partial every? #{:assign :subscribe}) "assign and/or subscribe"]]

   [nil "--kv-route ROUTE" "KV workloads: spread (each client on its own node) or leader (every request to the current leader)."
    :default :spread
    :parse-fn keyword
    :validate [#{:spread :leader} "spread or leader"]]

   [nil "--[no-]kv-lift" "KV workloads: lift the per-tenant KV rate limits on the nodes (default); --no-kv-lift keeps the product's QUEEN_KV_* defaults."
    :default true]

   [nil "--ops-per-key N" "register workload: operations per key."
    :default 200
    :parse-fn parse-long]

   [nil "--threads-per-key N" "register / claim workloads: client threads per key (must divide the concurrency)."
    :default 5
    :parse-fn parse-long]

   [nil "--w5-keys N" "pipeline workload: counter keys c:0..N-1 (input m counts on m mod N)."
    :default 5
    :parse-fn parse-long]

   [nil "--w5-partitions N" "pipeline workload: partitions of the in and out queues."
    :default 4
    :parse-fn parse-long]

   [nil "--w6-ids N" "dedup workload: transactionIds per partition (sends pick one at random)."
    :default 40
    :parse-fn parse-long]

   [nil "--w6-partitions N" "dedup workload: partitions of the test queue."
    :default 4
    :parse-fn parse-long]

   [nil "--w2-lease SECONDS" "queue workload: leaseSeconds of every pop (the pipeline workload's too)."
    :default 3
    :parse-fn parse-long]

   [nil "--w2-partitions N" "queue workload: partitions of the test queue."
    :default 8
    :parse-fn parse-long]

   [nil "--w2-group NAME" "queue workload: none = queue mode (pops and acks name no consumer group: the broker's __QUEUE_MODE__ cursor)."]

   [nil "--w2-groups N" "queue workload: N consumer groups g1..gN on the one queue; each must get every message (fan-out), checked group by group."
    :default 1
    :parse-fn parse-long]

   [nil "--services-host HOST" "Connector workloads: the control node's address as the nodes reach it (PostgreSQL :5432 and the S3 server :9000 run there; test/jepsen/rig/ctl-services.sh)."
    :default (or (System/getenv "JEPSEN_SERVICES_HOST") "ctl")]

   [nil "--timers-in-txn" "timers workload: every schedule and cancel is the timers part of ONE /api/v1/transaction instead of a call to /api/v1/timers."
    :default false]

   [nil "--queue-opt OPT" "queueopts workload: delayed (delayedProcessing 3 s), window (windowBuffer 2 s) or maxwait (maxWaitTimeSeconds 4)."
    :default :delayed
    :parse-fn keyword
    :validate [#{:delayed :window :maxwait} "delayed, window or maxwait"]]

   [nil "--w2-extend" "queue workload: 3 in 10 pops work past their lease, renew it 1 s before it ends (POST /api/v1/lease/:id/extend) and ack after the original lease ran out; the lease checker counts the renewal, and an ack refused under a renewal the broker answered success is an error."
    :default false]

   [nil "--w11-locks N" "locks / semaphore workloads: how many locks (each guards one partition)."
    :default 3
    :parse-fn parse-long]

   [nil "--w11-limit N" "semaphore workload: permits of each semaphore (at least 2)."
    :default 3
    :parse-fn parse-long]

   [nil "--standby-nodes N" "The last N nodes are a cluster of their own, a standby of the cluster the other nodes form. The failover workload needs it."
    :default 0
    :parse-fn parse-long
    :validate [#(<= 0 %) "0 or more"]]

   [nil "--failover MODE" "failover workload: crash (the source's nodes are killed, then the standby is promoted), crash-behind (the same after 10 s in which the standby could not read its source, so the promotion loses writes) or planned (the source's clients stop, the standby reads everything, the source is stopped, the standby is promoted)."
    :default :crash
    :parse-fn keyword
    :validate [#{:crash :crash-behind :planned} "crash, crash-behind or planned"]]

   [nil "--failover-at SECONDS" "failover workload: when the failover begins (default: half the time limit)."
    :parse-fn read-string]

   [nil "--w12-partitions N" "failover workload: partitions of the test queue."
    :default 8
    :parse-fn parse-long]

   [nil "--w12-kv" "failover workload: every send is ONE transaction that also increments the KV counter c:<k> of each partition it pushes to; the final reads take the counters too, and in each cluster every counter must equal its partition's record count."
    :default false]

   [nil "--standby-seed" "failover workload: a SEEDED standby. Its nodes start only at --seed-at, on a source that keeps 16 entries of log (QUEEN_RAFT_LOG_KEEP=16, QUEEN_RAFT_PURGE_HOLD_S=5): node 1 takes the source's snapshot (QUEEN_LINK_SEED=1) once the others answer that they hold nothing, and 15 s later the others are added as learners and promoted, as the operator's guide does it."
    :default false]

   [nil "--seed-at SECONDS" "--standby-seed: when the seed begins (default: half of --failover-at)."
    :parse-fn read-string]

   [nil "--dup-copies N" "duppush workload: the most copies of one push sent together (2 to N)."
    :default 4
    :parse-fn parse-long
    :validate [#(<= 2 % 8) "2 to 8"]]

   [nil "--dup-fresh RATIO" "duppush workload: how often a group's partition is a new one, created by the first copy to land (0 to 1)."
    :default 0.5
    :parse-fn parse-double
    :validate [#(<= 0.0 % 1.0) "0 to 1"]]

   [nil "--dup-kinds KINDS" "duppush workload: the kinds of copy, comma separated: push (one item), multi (with a filler item), multi-twice (the same transactionId twice in one push), txn (in a transaction with a filler). Default all."
    :parse-fn #(mapv keyword (remove str/blank? (str/split % #",")))
    :validate [#(every? #{:push :multi :multi-twice :txn} %) "push, multi, multi-twice, txn"]]

   ["-w" "--workload NAME" "Workload: log (W1), queue (W2), register (W3), counter (W3b), claim (W3c), elle (W4), pipeline (W5), dedup (W6), dlq (W7), retention (W8), streams (W9), timers (W10), locks (W11), semaphore (W11b), failover (W12), feed (W13), kafka (W14), filtered (W15), reading (W16), link (W17), txnmix (W18), feedstream (W19), ephemeral (W20), tenants (W21), scoped (W22), dlqops (W23), queueopts (W24), kvttl (W25), seek (W26), lifecycle (W27), pgsource (W28), pgsink (W29), s3sink (W30), duppush (W31), txnelle (W32)."
    :default :log
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  queen-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
