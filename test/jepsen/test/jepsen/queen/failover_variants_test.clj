(ns jepsen.queen.failover-variants-test
  "W12's variants (--w12-kv, --tenants/--raft-groups, --standby-seed, the
  upgrade orders): the checker on crafted histories, a clean one and one per
  planted bug, and the pieces that need no cluster. The plain W12 checks are
  in synthetic-test."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.generator.context :as gctx]
            [jepsen.queen [db :as qdb]
                          [nemesis :as qn]]
            [jepsen.queen.workload.failover :as f]))

(def test-map
  {:name "failover-variants", :start-time (util/local-time)
   :nodes ["n1" "n2" "n3" "n4" "n5" "n6"], :standby-nodes 3
   :w12-partitions 2, :failover :crash})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000 i))) ops)))

(defn check
  [test ops]
  (let [r (checker/check (f/checker) (merge test-map test) (hist ops) {})]
    (println (pr-str (select-keys r [:valid? :error :seed :kv-counter-not-records
                                     :kv-nodes-disagree :lost-before-a-survivor :halted
                                     :link-waiting-seen :survivor-per-group])))
    r))

(defn snd
  "A send of `pairs` by process p, a thread of `cluster`, answered `type`."
  [p cluster pairs type & kvs]
  [{:process p, :type :invoke, :f :send, :value pairs}
   (merge {:process p, :type type, :f :send, :value pairs, :cluster cluster
           :node (name cluster)}
          (apply hash-map kvs))])

(defn nem
  [f answer]
  [{:process :nemesis, :type :info, :f f, :value nil}
   {:process :nemesis, :type :info, :f f, :value answer}])

(defn fin
  "A final read of `cluster` by process p: logs, and counters when given."
  ([p cluster logs] (fin p cluster logs nil))
  ([p cluster logs counters]
   [{:process p, :type :invoke, :f :final-read, :value nil}
    (cond-> {:process p, :type :ok, :f :final-read, :value logs, :cluster cluster
             :node (str (name cluster) "-" p), :gaps {}}
      counters (assoc :kv counters))]))

(defn history
  "The source takes 1, 3 and 4 on partition 0 and 2 on partition 1, the
  standby refuses 100, the source dies with 4 not read, the promoted cluster
  takes 101 and 102. `over` replaces steps by name; `extra` steps go before
  the stop."
  ([over] (history over []))
  ([over extra]
   (let [steps (merge
                 {:s1      (snd 0 :source [[0 1]] :ok)
                  :s2      (snd 0 :source [[1 2]] :ok)
                  :probe   (snd 1 :standby [[0 100]] :fail :error [:standby])
                  :s3      (snd 0 :source [[0 3]] :ok)
                  :s4      (snd 0 :source [[0 4]] :ok)
                  :stop    (nem :stop-source {:killed {}})
                  :promote (nem :promote {:promoted? true})
                  :o1      (snd 1 :standby [[0 101]] :ok)
                  :o2      (snd 1 :standby [[1 102]] :ok)
                  :p       (fin 1 :standby {0 [1 3 101], 1 [2 102]} {0 3, 1 2})
                  :s       (fin 0 :source {0 [1 3 4], 1 [2]} {0 3, 1 1})}
                 over)]
     (vec (concat (mapcat steps [:s1 :s2 :probe :s3 :s4])
                  (apply concat extra)
                  (mapcat steps [:stop :promote :o1 :o2 :p :s]))))))

;; ---------------------------------------------------------------------------
;; --w12-kv

(deftest kv-and-log
  (let [kv {:w12-kv true}]
    (testing "clean: each cluster counts what its log holds"
      (let [r (check kv (history {}))]
        (is (true? (:valid? r)))
        (is (= {:promoted {0 3, 1 2}, :source {0 3, 1 1}} (:kv-counted r)))))
    (testing "the promoted cluster has a record without its count"
      (let [r (check kv (history {:p (fin 1 :standby {0 [1 3 101], 1 [2 102]} {0 2, 1 2})}))]
        (is (false? (:valid? r)))
        (is (= [{:cluster :promoted, :k 0, :counter 2, :records 3}]
               (:kv-counter-not-records r)))))
    (testing "the source has a count without its record"
      (let [r (check kv (history {:s (fin 0 :source {0 [1 3 4], 1 [2]} {0 3, 1 2})}))]
        (is (false? (:valid? r)))
        (is (= [:source 1] ((juxt :cluster :k) (first (:kv-counter-not-records r)))))))
    (testing "two nodes of one cluster read different counts"
      (let [r (check kv (history {:p (into (fin 1 :standby {0 [1 3 101], 1 [2 102]} {0 3, 1 2})
                                           (fin 2 :standby {0 [1 3 101], 1 [2 102]} {0 2, 1 2}))}))]
        (is (false? (:valid? r)))
        (is (= [{:cluster :standby, :node "standby-2", :k 0, :counter 2, :highest 3}]
               (:kv-nodes-disagree r)))))
    (testing "a counter never written is 0, and so is its partition"
      (let [r (check (assoc kv :w12-partitions 3)
                     (history {:p (fin 1 :standby {0 [1 3 101], 1 [2 102]} {0 3, 1 2, 2 0})
                               :s (fin 0 :source {0 [1 3 4], 1 [2]} {0 3, 1 1, 2 0})}))]
        (is (true? (:valid? r)))))
    (testing "no counters read on the source: nothing to judge them by"
      (is (= :unknown (:valid? (check kv (history {:s (fin 0 :source {0 [1 3 4], 1 [2]})}))))))
    (testing "without --w12-kv the counters are not judged"
      (is (true? (:valid? (check {} (history {:p (fin 1 :standby {0 [1 3 101], 1 [2 102]} {0 0})}))))))))

;; ---------------------------------------------------------------------------
;; --tenants / --raft-groups: each group is promoted at its own point

(deftest groups-cut-on-their-own
  (let [groups {:tenants 2, :raft-groups 2}]
    (testing "partition k is tenant k mod T's, and tenant i is group i mod G's"
      (let [t (merge test-map {:tenants 3, :raft-groups 2, :w12-partitions 7})]
        (is (= [0 1 2 0 1 2 0] (map #(f/tenant-index t %) (range 7))))
        (is (= [0 1 0 0 1 0 0] (map #(f/group-of t %) (range 7))))
        (is (= [0 0 0] (map #(f/group-of (dissoc t :tenants) %) (range 3))))
        (is (= [[{"x-queen-tenant" "00000000-0000-4000-8000-000000000001"} [0 3 6]]
                [{"x-queen-tenant" "00000000-0000-4000-8000-000000000002"} [1 4]]
                [{"x-queen-tenant" "00000000-0000-4000-8000-000000000003"} [2 5]]]
               (#'f/by-tenant t)))
        (is (= [[nil [0 1 2]]] (#'f/by-tenant (merge test-map {:w12-partitions 3}))))))
    (testing "a loss in one group beside a later survivor of the other is fine"
      ; 2 (partition 1, group 1) was answered before 3 (partition 0, group 0)
      ; was sent; 3 is in the promoted cluster, 2 is not.
      (let [lossy {:p  (fin 1 :standby {0 [1 3 101], 1 [102]})
                   :o2 (snd 1 :standby [[1 102]] :ok)}]
        (is (false? (:valid? (check {} (history lossy)))))
        (let [r (check groups (history lossy))]
          (is (true? (:valid? r)))
          (is (= {0 3} (:survivor-per-group r))))))
    (testing "a loss before a later survivor of the same group is not"
      ; partitions 0 and 2 are tenant 0's, group 0's.
      (let [r (check (assoc groups :w12-partitions 4)
                     (history {:s2 (snd 0 :source [[2 2]] :ok)
                               :p  (fin 1 :standby {0 [1 3 101], 1 [102]})
                               :s  (fin 0 :source {0 [1 3 4], 2 [2]})}))]
        (is (false? (:valid? r)))
        (is (= [[2 0]] (map (juxt :value :group) (:lost-before-a-survivor r))))))
    (testing "the generator keeps a transaction inside one tenant"
      (let [opts {:w12-partitions 8, :tenants 3, :txn-sends? true}
            ctx  (gctx/context {:concurrency 2, :nodes ["n1" "n2"]})
            ops  (loop [g (f/generator opts), out [], i 0]
                   (if (= i 400)
                     out
                     (let [[op g'] (gen/op g {} ctx)]
                       (recur g' (conj out op) (inc i)))))
            sends (filter #(= :send (:f %)) ops)]
        (is (< 300 (count sends)))
        (is (some #(< 1 (count (:value %))) sends))
        (is (every? (fn [op] (apply = (map #(mod (first %) 3) (:value op)))) sends))
        (is (= (count (distinct (mapcat #(map second (:value %)) sends)))
               (count (mapcat :value sends))))))))

;; ---------------------------------------------------------------------------
;; --standby-seed

(def seeded {:standby-seed true})

(defn seed-steps
  [seed grow]
  [(nem :seed-standby seed) (nem :grow-standby grow)])

(def seed-ok {:seeded true, :node "n4", :ms 3100, :starts 1})
(def grow-ok {:grown? true, :voters {0 [1 2 3]}})

(deftest seeded-standby
  (testing "clean: seeded, grown, promoted"
    (let [r (check seeded (history {} (seed-steps seed-ok grow-ok)))]
      (is (true? (:valid? r)))
      (is (= {:voters {0 [1 2 3]}} (:grown r)))))
  (testing "the seed was not taken"
    (let [r (check seeded (history {} (seed-steps {:seeded false, :log "..."} grow-ok)))]
      (is (false? (:valid? r)))
      (is (= [:seed-standby] (map :step (:seed r))))))
  (testing "the standby did not grow to all its nodes"
    (let [r (check seeded (history {} (seed-steps seed-ok {:grown? false, :voters {0 [1]}})))]
      (is (false? (:valid? r)))
      (is (= [:grow-standby] (map :step (:seed r))))))
  (testing "a seeded test that never seeded"
    (is (false? (:valid? (check seeded (history {}))))))
  (testing "the seed and the growth come first, each at its own time"
    (let [g (:generator (f/package (atom {}) {:mode :crash, :at 150, :seed-at 60}))]
      (is (= [:seed-standby :grow-standby :stop-source :heal-standby :promote] (map :f g)))
      (is (= [60000000000 75000000000 150000000000 nil nil] (map :time g)))))
  (testing "--seed-at must leave the growth its time before the failover"
    (is (thrown? clojure.lang.ExceptionInfo
                 (f/workload {:time-limit 300, :failover-at 100, :seed-at 90, :standby-seed true})))
    (is (map? (f/workload {:time-limit 300, :failover-at 100, :seed-at 85, :standby-seed true})))
    (is (= 75 (-> (f/workload {:time-limit 300, :standby-seed true})
                  :nemesis-package :generator first :time (quot 1000000000))))))

;; ---------------------------------------------------------------------------
;; A halted follower; a waiting one

(deftest halted-and-waiting
  ; The views are sorted maps keyed by group, as link-brief makes them (a
  ; keyword lookup on one throws: the local smoke's first checker run).
  (let [halted  {:link-before (sorted-map 0 {:node "n4", :role "standby"
                                             :follower {:state "halted", :error "purged"}})
                 :killed {}}
        waiting {:link-before (sorted-map 0 {:node "n4", :follower {:state "waiting"
                                                                    :error "newer release"}}
                                          1 {:node "n5", :follower {:state "following"}})
                 :killed {}}]
    (testing "a halted follower in any step's view"
      (let [r (check {} (history {:stop (nem :stop-source halted)}))]
        (is (false? (:valid? r)))
        (is (= [[:stop-source "halted"]] (map (juxt :step :state) (:halted r))))))
    (testing "halted while the source's disk was full: reported, not judged"
      (let [r (check {:faults #{:disk-full}} (history {:stop (nem :stop-source halted)}))]
        (is (true? (:valid? r)))
        (is (= 1 (count (:halted r))))))
    (testing "a waiting follower is counted"
      (let [r (check {} (history {:stop (nem :stop-source waiting)}))]
        (is (true? (:valid? r)))
        (is (= 1 (:link-waiting-seen r)))))))

;; ---------------------------------------------------------------------------
;; Pieces without a cluster

(deftest offline-pieces
  (testing "each raft group's part of a link status"
    (is (= [{:group 0, :role "standby", :leader true}]
           (qdb/link-parts {:role "standby", :leader true})))
    (is (= [{:group 0, :role "standby"} {:group 1, :role "standby", :leader true}]
           (qdb/link-parts {:role "standby"
                            :groups [{:group 0, :role "standby"}
                                     {:group 1, :role "standby", :leader true}]})))
    (is (= [] (qdb/link-parts nil))))
  (testing "a membership answer, one group or several, a joint configuration in one"
    ; The shape a broker of two groups answered in the local smoke, 2026-11-13,
    ; with group 0 left between the two steps of a promotion.
    (is (= {0 {:voters [1 2], :learners [], :joint [[1] [1 2]]}
            1 {:voters [1], :learners [2], :joint nil}}
           (f/membership-of
             {:engine "raft"
              :groups [{:group 0, :status 200
                        :body {:engine "raft"
                               :membership {:changeInFlight true, :joint [[1] [1 2]], :leader 1
                                            :learners [], :voters [1 2]}}}
                       {:group 1, :status 200
                        :body {:engine "raft"
                               :membership {:changeInFlight false, :joint nil, :leader 1
                                            :learners [2], :voters [1]}}}]})))
    (is (= {0 {:voters [1 2 3], :learners [], :joint nil}}
           (f/membership-of {:membership {:voters [3 1 2], :learners []}}))))
  (testing "the refusal codes of a membership answer, one group or several"
    (is (= ["learner_behind"] (#'f/answer-codes {:ok false, :code "learner_behind"})))
    (is (= ["in_flight"] (#'f/answer-codes {:groups [{:group 0, :status 200, :body {:ok true}}
                                                     {:group 1, :status 409
                                                      :body {:code "in_flight"}}]})))
    (is (= [] (#'f/answer-codes {:ok true})))
    (is (= [] (#'f/answer-codes "not json"))))
  (testing "the upgrade order of two clusters"
    (let [t    {:nodes ["n1" "n2" "n3" "n4" "n5" "n6"], :standby-nodes 3}
          all  (:nodes t)
          up   (set all)]
      (is (= ["n4" "n5" "n6"] (qn/upgrade-candidates (assoc t :upgrade-mode :standby-first) all up)))
      (is (= ["n1" "n2" "n3"] (qn/upgrade-candidates (assoc t :upgrade-mode :source-first) all up)))
      ; The standby is new: the source next.
      (is (= ["n1" "n3"] (qn/upgrade-candidates (assoc t :upgrade-mode :standby-first)
                                                ["n1" "n3"] up)))
      ; The failover stopped the source: its nodes wait for the final phase.
      (is (= [] (qn/upgrade-candidates (assoc t :upgrade-mode :standby-first)
                                       ["n1" "n2"] #{"n4" "n5" "n6"})))
      (is (= ["n5"] (qn/upgrade-candidates (assoc t :upgrade-mode :source-first)
                                           ["n2" "n5"] #{"n4" "n5" "n6"})))
      ; One cluster, or another mode: any old node.
      (is (= all (qn/upgrade-candidates (assoc t :upgrade-mode :once) all #{})))))
  (testing "a seeded standby's env, and its source's"
    (let [t  {:nodes ["127.0.0.1" "127.0.0.2" "127.0.0.3" "127.0.0.4"], :standby-nodes 2
              :link-token "tok", :standby-seed true}
          sb (qdb/link-env t "127.0.0.3")
          sr (qdb/link-env t "127.0.0.1")]
      (is (= "1" (sb "QUEEN_LINK_SEED")))
      (is (nil? (sb "QUEEN_LINK_STANDBY")))
      (is (= "127.0.0.1:7400,127.0.0.2:7400" (sb "QUEEN_LINK_SOURCE")))
      (is (= {"QUEEN_LINK_TOKEN" "tok", "QUEEN_RAFT_LOG_KEEP" "16", "QUEEN_RAFT_PURGE_HOLD_S" "5"} sr))
      (is (= "true" ((qdb/link-env (dissoc t :standby-seed) "127.0.0.3") "QUEEN_LINK_STANDBY")))
      (is (= {"QUEEN_LINK_TOKEN" "tok"} (qdb/link-env (dissoc t :standby-seed) "127.0.0.1"))))))

(deftest grown-reads-the-membership-it-was-answered
  ; Campaign 3, 2026-11-15: every --standby-seed run threw a ClassCastException
  ; on the membership answer (a map keyed by group number, looked up with
  ; :status) and never grew its standby.
  (let [one   {:groups (f/membership-of {:membership {:voters [3 1 2], :learners []}})}
        two   {:groups (f/membership-of
                         {:groups [{:group 0, :status 200, :body {:membership {:voters [1 2 3]}}}
                                   {:group 1, :status 200, :body {:membership {:voters [2 1 3]}}}]})}
        joint {:groups (f/membership-of {:membership {:voters [1 2 3], :joint [[1] [1 2 3]]}})}]
    (is (:ok? (f/grown one [1 2 3] 1)))
    (is (= {0 [1 2 3]} (:voters (f/grown one [1 2 3] 1))))
    (is (not (:ok? (f/grown one [1 2 3 4] 1))) "a voter missing")
    (is (:ok? (f/grown two [1 2 3] 2)))
    (is (not (:ok? (f/grown two [1 2 3] 1))) "not the groups the test runs")
    (is (not (:ok? (f/grown joint [1 2 3] 1))) "between two steps of a change")
    (is (not (:ok? (f/grown {:status 503} [1 2 3] 1))))
    (is (= {:status 503} (:voters (f/grown {:status 503} [1 2 3] 1))))))
