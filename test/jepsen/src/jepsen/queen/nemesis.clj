(ns jepsen.queen.nemesis
  "Composite faults beyond jepsen.nemesis.combined, after the NATS suite
  (jepsen-io/nats nemesis.clj, pause-kill-package and part-kill-package).
  They emit the combined nemesis's own operations (:pause, :resume, :kill,
  :start with explicit node lists; :start-partition with an explicit grudge),
  so they need no nemesis of their own.

  Each cycle draws a fresh order of the nodes: `behind` (a minority) misses a
  stretch of writes, `crasher` loses power (with --lazyfs: its un-fsynced
  writes), and `behind` + `crasher` then form a new majority without the rest.
  A write acknowledged only after a QUORUM fsync survives that; one
  acknowledged on a node that had not fsynced it does not."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [random :as rand]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.combined :as nc]
            [jepsen.nemesis.file :as nf]
            [jepsen.queen [db :as qdb]
                          [membership :as qm]]))

(defn- roles
  "A fresh random split: {:behind #{...} :crasher n :rest #{...}}."
  [test]
  (let [nodes   (vec (rand/shuffle (:nodes test)))
        m       (util/minority (count nodes))
        behind  (set (take m nodes))
        crasher (nth nodes m)]
    {:behind  behind
     :crasher crasher
     :rest    (-> (set nodes) (set/difference behind) (disj crasher))}))

(defn pause-kill-package
  "SIGSTOP a minority (it misses writes), power-fail one of the majority,
  SIGSTOP the rest and wake the minority, so the stale minority and the
  power-failed node must form the new majority; then wake everyone."
  [{:keys [faults interval phase] :or {phase 20}}]
  (let [needed? (contains? faults :pause-kill)]
    {:generator
     ; A function generator is called again whenever its ops run out: one
     ; cycle, fresh roles, per call.
     (when needed?
       (fn pause-kill-cycle [test ctx]
              (let [{:keys [behind crasher rest]} (roles test)]
                [(gen/log (str "pause-kill: behind " (sort behind) ", crasher "
                               crasher ", rest " (sort rest)))
                 {:type :info, :f :pause, :value (vec behind)}
                 (gen/sleep phase)
                 {:type :info, :f :kill, :value [crasher]}
                 {:type :info, :f :pause, :value (vec rest)}
                 {:type :info, :f :resume, :value (vec behind)}
                 {:type :info, :f :start, :value [crasher]}
                 (gen/sleep phase)
                 {:type :info, :f :resume, :value :all}
                 (gen/sleep interval)])))
     :final-generator
     (when needed?
       [{:type :info, :f :resume, :value :all}
        {:type :info, :f :start, :value :all}])
     :perf #{{:name  "pause-kill"
              :start #{:pause}
              :stop  #{:resume}
              :color "#C0A0E9"}}}))

(defn part-kill-package
  "Partition a minority away (it misses writes), power-fail one of the
  majority, then partition so the stale minority and the power-failed node are
  the majority side; then heal."
  [{:keys [faults interval phase] :or {phase 20}}]
  (let [needed? (contains? faults :part-kill)]
    {:generator
     (when needed?
       (fn part-kill-cycle [test ctx]
              (let [{:keys [behind crasher rest]} (roles test)
                    nodes (set (:nodes test))]
                [(gen/log (str "part-kill: behind " (sort behind) ", crasher "
                               crasher ", rest " (sort rest)))
                 {:type  :info, :f :start-partition
                  :value (n/complete-grudge [behind (set/difference nodes behind)])}
                 (gen/sleep phase)
                 {:type :info, :f :kill, :value [crasher]}
                 {:type :info, :f :stop-partition, :value nil}
                 {:type  :info, :f :start-partition
                  :value (n/complete-grudge [rest (set/difference nodes rest)])}
                 {:type :info, :f :start, :value [crasher]}
                 (gen/sleep phase)
                 {:type :info, :f :stop-partition, :value nil}
                 (gen/sleep interval)])))
     :final-generator
     (when needed?
       [{:type :info, :f :stop-partition, :value nil}
        {:type :info, :f :start, :value :all}])
     :perf #{{:name  "part-kill"
              :start #{:start-partition}
              :stop  #{:stop-partition}
              :color "#E9A0C8"}}}))

(defn bridge-package
  "Jepsen's bridge grudge over a fresh random order of the nodes: two halves
  that cannot see each other, and one node that sees both. On 5 nodes the
  leader keeps a majority wherever it sits; the stale half keeps campaigning."
  [{:keys [faults interval]}]
  (let [needed? (contains? faults :bridge)]
    {:generator
     (when needed?
       (->> (gen/flip-flop
              (fn [test ctx]
                {:type  :info, :f :start-partition
                 :value (n/bridge (vec (rand/shuffle (:nodes test))))})
              (gen/repeat {:type :info, :f :stop-partition, :value nil}))
            (gen/stagger interval)))
     :final-generator
     (when needed? {:type :info, :f :stop-partition, :value nil})
     :perf #{{:name  "bridge"
              :start #{:start-partition}
              :stop  #{:stop-partition}
              :color "#E9DCA0"}}}))

(def raft-port
  "Queen's Raft RPC port (jepsen.queen.db/raft-port)."
  7400)

(defn leader-deaf-nemesis
  "Makes the current leader deaf to its followers, with iptables on the leader.

    :responses  drop the packets from a follower's Raft port that carry data
                (IP length >= 100) and let pure TCP ACKs through: the leader
                keeps sending appends and heartbeats, every follower processes
                them (so none campaigns), and no answer reaches the leader
                (openraft GH#2080's shape: nothing commits).
    :all        drop everything from the followers (a plain one-way grudge).

  :stop-leader-deaf heals every node."
  [db]
  (reify
    n/Reflection
    (fs [_] #{:start-leader-deaf :stop-leader-deaf})

    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :start-leader-deaf
        (let [leaders (db/primaries db test)
              leader  (first leaders)
              mode    (:value op :responses)]
          (if-not leader
            (assoc op :value [:no-leader mode])
            (let [others (remove #{leader} (:nodes test))]
              (c/on-nodes test [leader]
                          (fn [_ _]
                            (c/su
                              (doseq [f others]
                                (case mode
                                  :responses
                                  ; Every raft group's port: group g listens on
                                  ; the raft port + g (--raft-groups).
                                  (c/exec :iptables :-A :INPUT :-s (cn/ip f)
                                          :-p :tcp :--sport (let [g (long (:raft-groups test 1))]
                                                              (if (< 1 g)
                                                                (str raft-port ":" (+ raft-port (dec g)))
                                                                raft-port))
                                          :-m :length :--length "100:65535"
                                          :-j :DROP :-w)
                                  :all
                                  (c/exec :iptables :-A :INPUT :-s (cn/ip f)
                                          :-j :DROP :-w))))))
              (assoc op :value {:leader leader, :leaders leaders, :mode mode}))))

        :stop-leader-deaf
        (do (net/heal! (:net test) test)
            (assoc op :value :healed))))

    (teardown! [this test]
      (net/heal! (:net test) test))))

(defn leader-deaf-package
  "Flip-flops a deaf leader (:responses or :all, at random) every interval."
  [{:keys [faults interval db]}]
  (let [needed? (contains? faults :leader-deaf)]
    {:nemesis   (leader-deaf-nemesis db)
     :generator
     (when needed?
       (->> (gen/flip-flop
              (fn [test ctx]
                {:type :info, :f :start-leader-deaf
                 :value (rand/nth [:responses :responses :all])})
              (gen/repeat {:type :info, :f :stop-leader-deaf, :value nil}))
            (gen/stagger interval)))
     :final-generator
     (when needed? {:type :info, :f :stop-leader-deaf, :value nil})
     :perf #{{:name  "leader-deaf"
              :start #{:start-leader-deaf}
              :stop  #{:stop-leader-deaf}
              :color "#A0E9B8"}}}))

(defn snap-kill-package
  "Kills a random node, keeps it down long enough for the leader to purge
  past it (run with a short QUEEN_RAFT_PURGE_HOLD_S), starts it, and kills it
  again at a random moment of its catch-up: while the snapshot is streamed,
  staged, or swapped in at the exit-75 restart."
  [{:keys [faults interval phase] :or {phase 20}}]
  (let [needed? (contains? faults :snap-kill)]
    {:generator
     (when needed?
       (fn snap-kill-cycle [test ctx]
         (let [node (rand/nth (vec (sort (:nodes test))))]
           [{:type :info, :f :kill, :value [node]}
            (gen/sleep phase)
            {:type :info, :f :start, :value [node]}
            (gen/sleep (rand/double 2.5))
            {:type :info, :f :kill, :value [node]}
            {:type :info, :f :start, :value [node]}
            (gen/sleep interval)])))
     :final-generator
     (when needed? {:type :info, :f :start, :value :all})
     :perf #{{:name  "snap-kill"
              :start #{:kill}
              :stop  #{:start}
              :color "#E9A4A0"}}}))

(def data-dir
  "Queen's QUEEN_RAFT_DIR (jepsen.queen.db/data-dir)."
  "/opt/queen/data")

(def file-classes
  "Which files a corruption may hit, by class."
  {:qlog  #"/qlog/q\d+/r\d+\.qlog$"
   :seg   #"/seg/b\d+/"
   :store #"/store/data\.mdb$"
   :state #"/raft/state\.json$"})

(defn- corrupt-one!
  "On the bound node (Queen already dead): damages one random file of `class`."
  [class mode]
  (let [files (->> (c/su (cu/ls data-dir {:recursive? true, :full-path? true,
                                          :types [:file]}))
                   (filter (partial re-find (file-classes class)))
                   vec)]
    (if (empty? files)
      {:class class, :mode mode, :file nil}
      (let [file (rand/nth files)
            size (parse-long (str/trim (c/su (c/exec :stat :-c "%s" file))))]
        (if (zero? size)
          {:class class, :mode mode, :file file, :size 0}
          (let [r (case mode
                    ; About two flipped bits in the whole file.
                    :bitflip (nf/corrupt-file!
                               {:mode        "bitflip"
                                :file        file
                                :chunk-size  size
                                :probability (/ 2.0 (* 8 size))})
                    ; Half the time a torn tail (up to the last 64 KiB gone;
                    ; a queue log's tail is often preallocated zeros), half
                    ; the time a cut anywhere.
                    :truncate (nf/corrupt-file!
                                {:mode "truncate"
                                 :file file
                                 :size (if (< (rand/double) 0.5)
                                         (max 0 (- size (inc (rand/long (min size 65536)))))
                                         (rand/long size))}))]
            {:class class, :mode mode, :file file, :size size,
             :result (str r)}))))))

(defn- await-up?
  "Whether queen on the bound node answers /health (any status: answering at
  all means it booted) within timeout-s. False early once the process has
  been dead for 2 s (it refused to boot)."
  [node timeout-s]
  (let [t0  (System/currentTimeMillis)
        url (str "http://" (cn/ip node) ":6632/health")]
    (loop []
      (let [elapsed (- (System/currentTimeMillis) t0)
            up?     (try (c/su (c/exec :curl :-s :--max-time 1 :-o "/dev/null" url))
                         true
                         (catch Exception _ false))
            alive?  (try (c/su (c/exec :pgrep :-x :queen)) true
                         (catch Exception _ false))]
        (cond up?                                 true
              (and (not alive?) (< 2000 elapsed)) false
              (< (* 1000 timeout-s) elapsed)      false
              :else (do (Thread/sleep 250) (recur)))))))

(defn- log-lines
  "How many lines queen.log has now (on the bound node)."
  []
  (try (parse-long (str/trim (c/su (c/exec :bash :-c "wc -l < /opt/queen/queen.log"))))
       (catch Exception _ 0)))

(defn- why-since
  "The last fatal line logged after line `lines` (on the bound node)."
  [lines]
  (try (c/su (c/exec :bash :-c
                     (str "tail -n +" (inc lines) " /opt/queen/queen.log"
                          " | grep -E 'FATAL|poison|panicked|corruption|CORRUPT'"
                          " | tail -1 | cut -c1-400")))
       (catch Exception _ nil)))

(defn- exit-rc-since
  "The exit code in the wrapper's last 'queen exited rc=N' line after line
  `lines` (135 = SIGBUS), or nil."
  [lines]
  (try (some->> (c/su (c/exec :bash :-c
                              (str "tail -n +" (inc lines) " /opt/queen/queen.log"
                                   " | grep -o 'queen exited rc=[0-9]*' | tail -1")))
                (re-find #"rc=(\d+)")
                second
                parse-long)
       (catch Exception _ nil)))

(defn- alive?
  []
  (try (c/su (c/exec :pgrep :-x :queen)) true
       (catch Exception _ false)))

(defn- watch-exit
  "Watches the bound node for watch-s seconds: the ms after which queen died,
  or nil. A process gone for 2 s is dead (an exit 75 comes back at once)."
  [watch-s]
  (let [t0 (System/currentTimeMillis)]
    (loop []
      (let [el (- (System/currentTimeMillis) t0)]
        (cond (< (* 1000 watch-s) el) nil
              (alive?)                (do (Thread/sleep 500) (recur))
              :else (do (Thread/sleep 2000)
                        (if (alive?) (recur) el)))))))

(defn- rejoin!
  "Replace the node through the membership API: remove, wipe, rejoin."
  [test node]
  (let [r (qm/replace! test [node])]
    {:rejoined (boolean (some :rejoined (:rejoins r)))
     :detail   r}))

(defn corrupt-trials
  "The per-node trials of a :corrupt-file or :corrupt-heal completion: its
  value itself with one victim, the maps under :trials with several."
  [op]
  (let [v (:value op)]
    (cond (not (map? v)) []
          (:trials v)    (:trials v)
          :else          [v])))

(defn corrupt-nemesis
  "Damages files on the victim nodes, fixed for the whole test: ONE node by
  default (--corrupt-node names it), or --corrupt-nodes N nodes damaged at
  the same time. Two of five is a minority: the cluster keeps its quorum, and
  must lose nothing. Each :corrupt-file op is one independent trial on every
  victim at once: kill them, copy each data directory aside, bitflip or
  truncate one random file of a class (qlog, seg, store, state) on each,
  start them, and watch them for --corrupt-watch seconds. Outcomes, per
  node:

    :booted        it came up and stayed up (the damage was refused or
                   repaired, or lies where nothing read it yet);
    :refused       it did not come up (the log says why);
    :runtime-exit  it came up and then exited (a damaged value found at
                   runtime ends the process).

  A node that refused never ran on the damage, so the copy may be put back
  byte-for-byte (remedy :restore). A node that ran may not be rolled back:
  it is removed from the cluster, wiped and rejoins (remedy :rejoin, the
  operator's path through the membership API). --corrupt-remedy picks for a
  refusal: restore, rejoin, or mix (either, at random). A node found dead
  at the start of a trial or by :corrupt-heal (it exited later) rejoins
  too. Queen must refuse or repair the damage; it must never serve it or
  lead with it.

  With one victim an op's value is that node's trial (the shape every
  campaign before 2026-11-13 recorded); with several it is {:trials [...]},
  one map per node (`corrupt-trials` reads both)."
  [db]
  (let [victims   (atom [])
        boot-line (atom {})
        lines-of  (fn [node] (get @boot-line node 0))
        late!     (fn [test node]
                    ; Did this victim die on its own since its last start?
                    (let [dead (get (c/on-nodes test [node]
                                                (fn [test node]
                                                  (when-not (alive?)
                                                    {:why (why-since (lines-of node))})))
                                    node)]
                      (cond
                        (nil? dead)  nil
                        (:why dead)  (assoc dead :rejoin (rejoin! test node))
                        :else        (do (c/on-nodes test [node]
                                                     (fn [test node] (db/start! db test node)))
                                         (assoc dead :started true)))))
        bak       (str data-dir ".bak")
        ; One victim's trial, on the bound node.
        trial!    (fn [test node class mode]
                    (db/kill! db test node)
                    (c/su (c/exec :rm :-rf bak)
                          (c/exec :cp :-a data-dir bak))
                    (let [r     (corrupt-one! class mode)
                          lines (log-lines)
                          _     (swap! boot-line assoc node lines)
                          _     (db/start! db test node)
                          up?   (await-up? node 20)]
                      (if up?
                        (if-let [ms (watch-exit (:corrupt-watch test 15))]
                          (assoc r :outcome :runtime-exit, :exit-after-ms ms
                                 :why (why-since lines), :rc (exit-rc-since lines))
                          (do (c/su (c/exec :rm :-rf bak))
                              (assoc r :outcome :booted)))
                        (assoc r :outcome :refused, :why (why-since lines)
                               :rc (exit-rc-since lines)))))
        remedy-of (fn [test trial]
                    (case (:outcome trial)
                      :booted       nil
                      :runtime-exit :rejoin
                      :refused      (case (:corrupt-remedy test :mix)
                                      :restore :restore
                                      :rejoin  :rejoin
                                      (rand/nth [:restore :rejoin]))))
        restore!  (fn [test node]
                    (db/kill! db test node)
                    (c/su (c/exec :rm :-rf data-dir)
                          (c/exec :mv bak data-dir))
                    (swap! boot-line assoc node (log-lines))
                    (db/start! db test node)
                    {:restored-up? (await-up? node 30)})]
    (reify
      n/Reflection
      (fs [_] #{:corrupt-file :corrupt-heal})

      n/Nemesis
      (setup! [this test]
        (c/with-test-nodes test (n/compile-c-resource! "corrupt-file.c" "corrupt-file"))
        (let [n (long (:corrupt-nodes test 1))]
          (reset! victims
                  (cond
                    (and (= 1 n) (:corrupt-node test)) [(:corrupt-node test)]
                    :else (vec (sort (take n (rand/shuffle (vec (sort (:nodes test))))))))))
        (info "file corruption victims:" @victims)
        this)

      (invoke! [this test op]
        (let [nodes @victims
              shape (fn [per-node]
                      ; One victim: its map, as before; several: {:trials ...}.
                      (if (= 1 (count nodes))
                        (first per-node)
                        {:trials (vec per-node)}))]
          (case (:f op)
            :corrupt-heal
            (assoc op :value (shape (for [node nodes]
                                      {:node node, :late-exit (late! test node)})))

            :corrupt-file
            (let [{:keys [class mode]} (:value op)
                  late    (into {} (for [node nodes] [node (late! test node)]))
                  ; Every victim down and damaged at the same time.
                  trials  (c/on-nodes test nodes
                                      (fn [test node] (trial! test node class mode)))
                  remedy  (into {} (for [node nodes]
                                     [node (remedy-of test (get trials node))]))
                  restore (->> nodes (filter #(= :restore (remedy %))))
                  rejoin  (->> nodes (filter #(= :rejoin (remedy %))))
                  restored (when (seq restore)
                             (c/on-nodes test restore (fn [test node] (restore! test node))))
                  rejoined (when (seq rejoin)
                             (c/on-nodes test rejoin
                                         (fn [test node] (c/su (c/exec :rm :-rf bak))))
                             ; One replace! for all of them: it removes them one
                             ; after another, then wipes and rejoins each.
                             (let [r (if (= 1 (count rejoin))
                                       {(first rejoin) (rejoin! test (first rejoin))}
                                       (let [r (qm/replace! test (vec rejoin))]
                                         (into {} (for [node rejoin]
                                                    [node {:rejoined (boolean
                                                                       (some #(and (= node (:node %)) (:rejoined %))
                                                                             (:rejoins r)))
                                                           :detail   r}]))))]
                               (c/on-nodes test rejoin
                                           (fn [test node] (swap! boot-line assoc node (log-lines))))
                               r))]
              (assoc op :value
                     (shape (for [node nodes]
                              (cond-> (merge (get trials node)
                                             (get restored node)
                                             (when-let [r (get rejoined node)] {:rejoin r})
                                             {:node node})
                                (remedy node) (assoc :remedy (remedy node))
                                (late node)   (assoc :late-exit (late node))))))))))

      (teardown! [this test]))))

(def default-corrupt-classes
  "File classes a corrupt op draws from, with repeats as weights. The seg
  files are empty on the raft engine (the queue logs are the only WAL): the
  data lives in qlog and store."
  [:qlog :qlog :qlog :store :store :state])

(defn corrupt-package
  [{:keys [faults interval db corrupt-classes]}]
  (let [needed? (contains? faults :corrupt)
        classes (vec (or (seq corrupt-classes) default-corrupt-classes))]
    {:nemesis (corrupt-nemesis db)
     :generator
     (when needed?
       (->> (fn [test ctx]
              {:type  :info, :f :corrupt-file
               :value {:class (rand/nth classes)
                       :mode  (rand/nth [:bitflip :truncate])}})
            (gen/stagger interval)))
     :final-generator
     (when needed? [{:type :info, :f :corrupt-heal, :value nil}
                    {:type :info, :f :start, :value :all}])
     :perf #{{:name  "corrupt"
              :start #{:corrupt-file}
              :stop  #{}
              :color "#D2E9A0"}}}))

;; ---------------------------------------------------------------------------
;; Graceful restart: SIGTERM, wait for the exit, start again.

(defn graceful-restart-nemesis
  "  :restart          SIGTERM one node's queen (random, or :value), wait for
                     it to exit (killed after 30 s: :forced), start it, wait
                     for /health.
     :rolling-restart  the same for every node in turn, each healthy before
                     the next.
     :restart-heal     start every node that is not running.
  A leader that gets SIGTERM hands leadership off before it exits (commit
  74818982); a restart of the leader should therefore cost no election
  timeout."
  []
  (reify
    n/Reflection
    (fs [_] #{:restart :rolling-restart :restart-heal})

    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :restart
        (let [node (or (:value op) (rand/nth (vec (:nodes test))))
              res  (c/on-nodes test [node]
                               (fn [test node] (qdb/graceful-restart! test node 30)))]
          (assoc op :value (get res node)))

        :rolling-restart
        (assoc op :value
               (mapv (fn [node]
                       (get (c/on-nodes test [node]
                                        (fn [test node] (qdb/graceful-restart! test node 30)))
                            node))
                     (rand/shuffle (vec (:nodes test)))))

        :restart-heal
        (assoc op :value (c/on-nodes test (fn [test node] (qdb/start-node! test node))))))

    (teardown! [this test])))

(defn graceful-restart-package
  [{:keys [faults interval]}]
  (let [needed? (contains? faults :restart)]
    {:nemesis (graceful-restart-nemesis)
     :generator
     (when needed?
       (->> (gen/mix [(repeat {:type :info, :f :restart, :value nil})
                      (repeat {:type :info, :f :restart, :value nil})
                      (repeat {:type :info, :f :rolling-restart, :value nil})])
            (gen/stagger interval)))
     :final-generator
     (when needed? {:type :info, :f :restart-heal, :value nil})
     :perf #{{:name  "restart"
              :fs    #{:restart :rolling-restart}
              :start #{}
              :stop  #{}
              :color "#A0C8E9"}}}))

;; ---------------------------------------------------------------------------
;; Membership changes, through the admin API (jepsen.queen.membership).

(defn membership-nemesis
  "  :member-cycle   one voter out and back (a follower, then the leader on
                   alternate cycles): remove it, kill it, wipe its data
                   directory, start it empty with QUEEN_RAFT_JOIN=true, add it
                   as a learner, wait until it has caught up, promote it.
     :member-double  two voters out (the cluster commits on three), then both
                   back the same way. Only from five voters.
     :member-heal    every node back as a voter.
  A change the API refuses (409 no_quorum while a majority is not live, 409
  in_flight while another change runs) is recorded and the cycle stops there:
  refusing is the API's job. Each op's value holds every step with the
  membership its answer carried."
  []
  (let [cycles (atom 0)]
    (reify
      n/Reflection
      (fs [_] #{:member-cycle :member-double :member-heal})

      n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (assoc op :value
               (case (:f op)
                 :member-cycle  (qm/cycle! test (swap! cycles inc))
                 :member-double (qm/double-cycle! test)
                 :member-heal   (qm/heal! test))))

      (teardown! [this test]))))

(defn membership-package
  [{:keys [faults interval]}]
  (let [needed? (contains? faults :membership)]
    {:nemesis   (membership-nemesis)
     :generator (when needed?
                  (->> (gen/mix [(repeat {:type :info, :f :member-cycle, :value nil})
                                 (repeat {:type :info, :f :member-cycle, :value nil})
                                 (repeat {:type :info, :f :member-double, :value nil})])
                       (gen/stagger interval)))
     :final-generator (when needed? {:type :info, :f :member-heal, :value nil})
     :perf      #{{:name  "membership"
                   :fs    #{:member-cycle :member-double :member-heal}
                   :start #{}
                   :stop  #{}
                   :color "#E9C0A0"}}}))

;; ---------------------------------------------------------------------------
;; A full disk: the filesystem under the data directory filled to the last
;; block, then freed.

(def fill-file
  "The file that fills a node's disk: beside the data directory, on the same
  filesystem, never inside it."
  "/opt/queen/fill")

(defn- free-disk!
  "On the bound node: remove the fill."
  []
  (c/su (util/meh (c/exec :rm :-f fill-file)))
  :freed)

(defn- fill-disk!
  "On the bound node: fallocate one file over every free block of the
  filesystem that holds /opt/queen, root's reserved blocks included (queen
  runs as root, so `df`'s available space is not the limit it meets), less
  `keep-bytes`."
  [keep-bytes]
  (free-disk!)
  (c/su
    (let [[free bsize] (->> (str/split (str/trim (c/exec :stat :-f :-c "%f %S" "/opt/queen"))
                                       #"\s+")
                            (map parse-long))
          size         (max 0 (- (* free bsize) (long keep-bytes)))]
      (when (pos? size)
        (c/exec :fallocate :-l size fill-file))
      {:filled-bytes size
       :left         (str/trim (c/exec :df :-B1 :--output=avail "/opt/queen"))})))

(defn disk-full-nemesis
  "  :fill-disk  fill the disk under the data directory of the nodes in
                :value (a node spec, resolved like the kill nemesis's), to the
                last free block but --disk-full-keep bytes;
     :free-disk  remove the fill on every node.
  A full disk must cost availability, never an acknowledged write: a node
  that cannot write refuses or stops, and once the disk has room again the
  cluster goes on. A panic is reported by the panic checker like any other."
  [db]
  (reify
    n/Reflection
    (fs [_] #{:fill-disk :free-disk})

    n/Nemesis
    (setup! [this test]
      (c/with-test-nodes test (free-disk!))
      this)

    (invoke! [this test op]
      (case (:f op)
        :fill-disk
        (let [nodes (vec (nc/db-nodes test db (:value op)))]
          (assoc op :value (c/on-nodes test nodes
                                       (fn [test node]
                                         (fill-disk! (:disk-full-keep test 0))))))
        :free-disk
        (assoc op :value (c/on-nodes test (fn [test node] (free-disk!))))))

    (teardown! [this test]
      (c/with-test-nodes test (free-disk!)))))

(defn disk-full-package
  [{:keys [faults interval db kill]}]
  (let [needed? (contains? faults :disk-full)
        targets (vec (or (seq (:targets kill)) [:one :minority]))]
    {:nemesis (disk-full-nemesis db)
     :generator
     (when needed?
       (->> (gen/flip-flop (fn [_ _] {:type :info, :f :fill-disk, :value (rand/nth targets)})
                           (repeat {:type :info, :f :free-disk, :value nil}))
            (gen/stagger interval)))
     :final-generator
     (when needed? {:type :info, :f :free-disk, :value nil})
     :perf #{{:name  "disk-full"
              :start #{:fill-disk}
              :stop  #{:free-disk}
              :color "#C9A0E9"}}}))

;; ---------------------------------------------------------------------------
;; A rolling upgrade while the clients run: every node starts on --bin-old and
;; is restarted on --bin, one at a time (jepsen.queen.db/swap-release!).

(defn- releases
  "{node :old | :new | nil}: which release each node's binary is."
  [test]
  (c/on-nodes test (fn [test node] (qdb/running-release))))

(defn upgrade-candidates
  "The nodes :upgrade-node picks from (at random), of `olds` (on the old
  release) and `running` (a set of the nodes whose process runs). Two
  clusters (--standby-nodes) are upgraded in an order: --upgrade-mode
  standby-first (the documented one: a standby reads a source on an older
  release, never the other way) or source-first (the wrong one: the standby
  waits, losing nothing, until its own nodes are new), each among the nodes
  that run, so a source the failover stopped is not started again by an
  upgrade. Any other mode: every old node."
  [test olds running]
  (let [mode (:upgrade-mode test)]
    (if-not (#{:standby-first :source-first} mode)
      (vec olds)
      (let [up          (filter running olds)
            [sb src]    ((juxt filter remove) #(qdb/standby? test %) up)
            [now later] (if (= :standby-first mode) [sb src] [src sb])]
        (vec (if (seq now) now later))))))

(defn- upgrade-pick
  "The node :upgrade-node restarts on the new release, of `olds`, or nil."
  [test olds]
  (let [running (if (and (seq olds) (#{:standby-first :source-first} (:upgrade-mode test)))
                  (->> (c/on-nodes test olds (fn [_ _] (qdb/running?)))
                       (keep (fn [[node up?]] (when up? node)))
                       set)
                  (set olds))
        cands   (upgrade-candidates test olds running)]
    (when (seq cands) (rand/nth cands))))

(defn upgrade-nemesis
  "  :upgrade-node    one node still on the old release (random) restarted
                     on the new one; :all-new when none is left
                     (:none-running when the old ones are all down, with two
                     clusters: see upgrade-pick for their order);
     :downgrade-node  one node on the new release back to the old one
                     (--upgrade-mode flap: only for two releases of one
                     cluster version, which can run side by side both ways);
     :upgrade-all     every node still on the old release, one after another,
                     each healthy before the next.
  Each restart is graceful (SIGTERM: a leader hands leadership off first),
  as an operator's rolling upgrade is. Its value holds the restart's exit
  code, how long the node took to exit and to answer /health again, and the
  release before and after."
  []
  (let [move! (fn [test node to]
                (get (c/on-nodes test [node]
                                 (fn [test node] (qdb/swap-release! test node to)))
                     node))
        on    (fn [test release]
                (->> (releases test)
                     (keep (fn [[node r]] (when (= release r) node)))
                     sort
                     vec))]
    (reify
      n/Reflection
      (fs [_] #{:upgrade-node :downgrade-node :upgrade-all})

      n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (case (:f op)
          :upgrade-node
          (let [olds (on test :old)
                pick (upgrade-pick test olds)]
            (assoc op :value (cond pick       (move! test pick :new)
                                   (seq olds) :none-running
                                   :else      :all-new)))

          :downgrade-node
          (let [news (on test :new)]
            (assoc op :value (if (seq news)
                               (move! test (rand/nth news) :old)
                               :all-old)))

          :upgrade-all
          (assoc op :value (mapv #(move! test % :new) (on test :old)))))

      (teardown! [this test]))))

(defn upgrade-package
  [{:keys [faults upgrade-interval upgrade-mode]}]
  (let [needed? (contains? faults :upgrade)
        every   (or upgrade-interval 40)]
    {:nemesis (upgrade-nemesis)
     :generator
     (when needed?
       (->> (if (= :flap upgrade-mode)
              (gen/mix [(repeat {:type :info, :f :upgrade-node, :value nil})
                        (repeat {:type :info, :f :downgrade-node, :value nil})])
              (repeat {:type :info, :f :upgrade-node, :value nil}))
            (gen/stagger every)))
     :final-generator
     (when needed? {:type :info, :f :upgrade-all, :value nil})
     :perf #{{:name  "upgrade"
              :fs    #{:upgrade-node :downgrade-node :upgrade-all}
              :start #{}
              :stop  #{}
              :color "#E9E0A0"}}}))

;; ---------------------------------------------------------------------------
;; What the fault ops did, counted (informational: always valid).

(defn fault-summary-checker
  "Counts the corrupt-file trials by class, outcome and remedy, and the
  membership cycles by result, from the nemesis ops' values."
  []
  (reify checker/Checker
    (check [this test history opts]
      ; A nemesis op is :info twice; the completion is the one whose value
      ; carries what happened.
      (let [done    (->> history
                         (filter #(and (= :nemesis (:process %)) (= :info (:type %))
                                       (map? (:value %)))))
            ; One op per trial with one victim; one op holding a trial per
            ; victim with --corrupt-nodes N. Counted per node either way.
            per-node (fn [f pred]
                       (->> done
                            (filter #(= f (:f %)))
                            (mapcat (fn [op] (map #(assoc op :value %) (corrupt-trials op))))
                            (filter (comp pred :value))))
            corrupt (per-node :corrupt-file :outcome)
            heals   (per-node :corrupt-heal #(contains? % :node))
            members (filter #(and (#{:member-cycle :member-double :member-heal} (:f %))
                                  (some (:value %) [:removals :healed :skipped]))
                            done)
            rejoins (fn [v] (concat (:rejoins v) (:healed v)))]
        {:valid? true
         :corrupt-trials   (count corrupt)
         :corrupt-outcomes (->> corrupt
                                (map (comp (juxt :class :outcome :remedy) :value))
                                frequencies
                                (into (sorted-map-by #(compare (str %1) (str %2)))))
         :corrupt-exit-rcs (frequencies (keep (comp :rc :value) corrupt))
         :corrupt-late-exits (->> (concat corrupt heals)
                                  (keep (comp :late-exit :value))
                                  (mapv #(select-keys % [:why :started])))
         :corrupt-late-rejoined (->> (concat corrupt heals)
                                     (keep (comp :rejoin :late-exit :value))
                                     (map :rejoined)
                                     frequencies)
         :corrupt-not-back (->> corrupt
                                (keep (fn [op]
                                        (let [v (:value op)]
                                          (when (or (false? (:restored-up? v))
                                                    (and (:rejoin v)
                                                         (not (:rejoined (:rejoin v)))))
                                            (select-keys v [:class :mode :file :outcome :remedy :why])))))
                                vec)
         :member-ops       (frequencies (map :f members))
         :member-skipped   (frequencies (keep (comp :skipped :value) members))
         :member-removals  (->> members
                                (mapcat (comp :removals :value))
                                (map (juxt :status :code))
                                frequencies)
         :member-rejoins   (->> members
                                (mapcat (comp rejoins :value))
                                (map :rejoined)
                                frequencies)
         :member-final     (:after (:value (last (filter #(= :member-heal (:f %)) members))))
         :disk-fills       (count (filter #(= :fill-disk (:f %)) done))
         :upgrades         (->> done
                                (filter #(#{:upgrade-node :downgrade-node} (:f %)))
                                (map (comp (juxt :from :to :rc) :value))
                                frequencies)
         :upgrade-final    (some->> done
                                    (filter #(= :upgrade-all (:f %)))
                                    last :value
                                    (mapv #(select-keys % [:node :from :to :rc :healthy-after-ms])))}))))
