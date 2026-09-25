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
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [random :as rand]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.file :as nf]))

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
                                  (c/exec :iptables :-A :INPUT :-s (cn/ip f)
                                          :-p :tcp :--sport raft-port
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

(defn corrupt-nemesis
  "Damages files on ONE node, fixed for the whole test. Each operation is one
  independent trial: kill the node, copy its data directory aside, bitflip or
  truncate one random file of a class (qlog, seg, store, state), start it.
  If it does not come up (it refused the damage), the copy is put back
  byte-for-byte - the node never ran in between, so nothing rolls back - and
  it starts again. Queen must refuse or repair the damage; it must never
  serve it or lead with it."
  [db]
  (let [victim (atom nil)]
    (reify
      n/Reflection
      (fs [_] #{:corrupt-file})

      n/Nemesis
      (setup! [this test]
        (c/with-test-nodes test (n/compile-c-resource! "corrupt-file.c" "corrupt-file"))
        (reset! victim (or (:corrupt-node test)
                           (rand/nth (vec (sort (:nodes test))))))
        (info "file corruption victim:" @victim)
        this)

      (invoke! [this test op]
        (let [{:keys [class mode]} (:value op)
              node @victim
              bak  (str data-dir ".bak")
              res  (c/on-nodes
                     test [node]
                     (fn [test node]
                       (db/kill! db test node)
                       (c/su (c/exec :rm :-rf bak)
                             (c/exec :cp :-a data-dir bak))
                       (let [r     (corrupt-one! class mode)
                             lines (try (parse-long
                                          (str/trim (c/su (c/exec :bash :-c "wc -l < /opt/queen/queen.log"))))
                                        (catch Exception _ 0))
                             _     (db/start! db test node)
                             up?   (await-up? node 20)]
                         (if up?
                           (do (c/su (c/exec :rm :-rf bak))
                               (assoc r :outcome :booted))
                           ; Only this boot's lines: why it refused.
                           (let [why (try (c/su (c/exec :bash :-c
                                                        (str "tail -n +" (inc lines) " /opt/queen/queen.log"
                                                             " | grep -E 'FATAL|poison|panicked|corruption'"
                                                             " | tail -1 | cut -c1-400")))
                                          (catch Exception _ nil))]
                             (db/kill! db test node)
                             (c/su (c/exec :rm :-rf data-dir)
                                   (c/exec :mv bak data-dir))
                             (db/start! db test node)
                             (assoc r :outcome :refused, :why why
                                    :restored-up? (await-up? node 30)))))))]
          (assoc op :value (assoc (get res node) :node node))))

      (teardown! [this test]))))

(defn corrupt-package
  [{:keys [faults interval db]}]
  (let [needed? (contains? faults :corrupt)]
    {:nemesis (corrupt-nemesis db)
     :generator
     (when needed?
       (->> (fn [test ctx]
              {:type  :info, :f :corrupt-file
               ; The seg files are empty on the raft engine (the queue
               ; logs are the only WAL): the data lives in qlog and store.
               :value {:class (rand/nth [:qlog :qlog :qlog :store :store :state])
                       :mode  (rand/nth [:bitflip :truncate])}})
            (gen/stagger interval)))
     :final-generator
     (when needed? {:type :info, :f :start, :value :all})
     :perf #{{:name  "corrupt"
              :start #{:corrupt-file}
              :stop  #{}
              :color "#D2E9A0"}}}))
