(ns jepsen.queen.synthetic-test
  "Synthetic histories for the checkers of W3-W6 and W11: one clean history
  each, and one per anomaly the checker must flag. Run with `lein test` (no
  cluster)."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [independent :as independent]
                    [nemesis :as n]
                    [util :as util]]
            [jepsen.queen [membership :as qm]
                          [nemesis :as qn]]
            [jepsen.queen.workload [dedup :as dedup]
                                   [elle :as elle]
                                   [locks :as locks]
                                   [pipeline :as pipeline]
                                   [register :as register]]))

(def test-map
  {:name       "synthetic"
   :start-time (util/local-time)
   :nodes      ["n1" "n2" "n3" "n4" "n5"]})

(defn hist
  "A history from op maps; :time is the position (so order = real time)."
  [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000 i))) ops)))

(defn run
  [c ops]
  (let [r (checker/check c test-map (hist ops) {})]
    (println (pr-str (dissoc r :reads)))
    r))

(defn- with [m kvs] (if (seq kvs) (apply assoc m kvs) m))
(defn inv [p f v & kvs] (with {:process p, :type :invoke, :f f, :value v} kvs))
(defn ok  [p f v & kvs] (with {:process p, :type :ok, :f f, :value v} kvs))
(defn fail [p f v & kvs] (with {:process p, :type :fail, :f f, :value v} kvs))
(defn info [p f v & kvs] (with {:process p, :type :info, :f f, :value v} kvs))

;; ---------------------------------------------------------------------------
;; W3: Knossos cas-register per key

(def t independent/tuple)

(defn w3-checker [] (:checker (register/register-workload {})))

(def w3-clean
  [(inv 0 :write (t 0 1))      (ok 0 :write (t 0 1))
   (inv 1 :read  (t 0 nil))    (ok 1 :read  (t 0 1))
   (inv 2 :cas   (t 0 [1 2]))  (ok 2 :cas   (t 0 [1 2]))
   (inv 3 :cas   (t 0 [1 4]))  (fail 3 :cas (t 0 [1 4]) :error [:cas-mismatch 2])
   (inv 4 :write (t 0 3))      (info 4 :write (t 0 3))
   (inv 0 :read  (t 0 nil))    (ok 0 :read  (t 0 2))
   (inv 1 :read  (t 1 nil))    (ok 1 :read  (t 1 -1))])

(deftest w3-register
  (testing "clean"
    (is (true? (:valid? (run (w3-checker) w3-clean)))))
  (testing "stale read: 1 after the write of 3 completed"
    (is (false? (:valid? (run (w3-checker)
                              [(inv 0 :write (t 0 1)) (ok 0 :write (t 0 1))
                               (inv 1 :write (t 0 3)) (ok 1 :write (t 0 3))
                               (inv 2 :read (t 0 nil)) (ok 2 :read (t 0 1))])))))
  (testing "a cas that won against a value that was not there"
    (is (false? (:valid? (run (w3-checker)
                              [(inv 0 :write (t 0 1)) (ok 0 :write (t 0 1))
                               (inv 1 :cas (t 0 [4 2])) (ok 1 :cas (t 0 [4 2]))])))))
  (testing "absent after a completed write"
    (is (false? (:valid? (run (w3-checker)
                              [(inv 0 :write (t 7 1)) (ok 0 :write (t 7 1))
                               (inv 1 :read (t 7 nil)) (ok 1 :read (t 7 register/absent))]))))))

;; ---------------------------------------------------------------------------
;; W3b: jepsen.checker/counter

(defn w3b-checker [] (:checker (register/counter-workload {})))

(deftest w3b-counter
  (testing "clean, with a read concurrent with an add and a failed add"
    (is (true? (:valid? (run (w3b-checker)
                             [(inv 0 :add 1) (ok 0 :add 1)
                              (inv 1 :read nil) (ok 1 :read 1)
                              (inv 0 :add 1) (inv 1 :read nil) (ok 1 :read 1) (ok 0 :add 1)
                              (inv 2 :add 1) (fail 2 :add 1)
                              (inv 3 :add 1) (info 3 :add 1)
                              (inv 1 :read nil) (ok 1 :read 3)])))))
  (testing "above the upper bound (a failed add counted)"
    (is (false? (:valid? (run (w3b-checker)
                              [(inv 0 :add 1) (ok 0 :add 1)
                               (inv 2 :add 1) (fail 2 :add 1)
                               (inv 1 :read nil) (ok 1 :read 2)])))))
  (testing "below the lower bound (an acknowledged add lost)"
    (is (false? (:valid? (run (w3b-checker)
                              [(inv 0 :add 1) (ok 0 :add 1)
                               (inv 0 :add 1) (ok 0 :add 1)
                               (inv 1 :read nil) (ok 1 :read 1)]))))))

;; ---------------------------------------------------------------------------
;; W3c: putIfAbsent claims

(defn w3c [ops] (run (register/claim-checker) ops))

(def w3c-clean
  [(inv 0 :claim (t 0 nil)) (inv 1 :claim (t 0 nil)) (inv 2 :claim (t 0 nil))
   (ok 0 :claim (t 0 "A"))
   (fail 1 :claim (t 0 "B") :error [:not-applied "exists"] :current "A")
   (info 2 :claim (t 0 "C"))
   (inv 0 :read (t 0 nil)) (ok 0 :read (t 0 "A"))
   (inv 1 :read (t 0 nil)) (ok 1 :read (t 0 "A"))
   ; key 1: nobody known to have won, an unknown claim did
   (inv 3 :claim (t 1 nil)) (info 3 :claim (t 1 "D"))
   (inv 4 :read (t 1 nil)) (ok 4 :read (t 1 "D"))])

(deftest w3c-claim
  (testing "clean"
    (is (true? (:valid? (w3c w3c-clean)))))
  (testing "two winners"
    (is (false? (:valid? (w3c [(inv 0 :claim (t 0 nil)) (inv 1 :claim (t 0 nil))
                               (ok 0 :claim (t 0 "A")) (ok 1 :claim (t 0 "B"))])))))
  (testing "a read names an unknown claim, not the winner"
    (is (false? (:valid? (w3c [(inv 0 :claim (t 0 nil)) (inv 2 :claim (t 0 nil))
                               (ok 0 :claim (t 0 "A")) (info 2 :claim (t 0 "C"))
                               (inv 1 :read (t 0 nil)) (ok 1 :read (t 0 "C"))])))))
  (testing "a loser reports a current value that is not the winner"
    (is (false? (:valid? (w3c [(inv 0 :claim (t 0 nil)) (inv 1 :claim (t 0 nil))
                               (ok 0 :claim (t 0 "A"))
                               (fail 1 :claim (t 0 "B") :error [:not-applied "exists"]
                                     :current "B")])))))
  (testing "a value no claim wrote"
    (is (false? (:valid? (w3c [(inv 0 :claim (t 0 nil)) (fail 0 :claim (t 0 "A") :error [:txn 429])
                               (inv 1 :read (t 0 nil)) (ok 1 :read (t 0 "Z"))])))))
  (testing "absent after the winning claim completed"
    (is (false? (:valid? (w3c [(inv 0 :claim (t 0 nil)) (ok 0 :claim (t 0 "A"))
                               (inv 1 :read (t 0 nil)) (ok 1 :read (t 0 nil))]))))))

;; ---------------------------------------------------------------------------
;; W4: Elle rw-register, strict serializable

(defn w4 [ops] (run (:checker (elle/workload {})) ops))

(deftest w4-elle
  (testing "normalize: reads first, last write per key, no read of a written key"
    (is (= [[:r 2 nil] [:w 3 1] [:w 1 4]]
           (elle/normalize [[:w 1 3] [:r 2 nil] [:r 1 nil] [:w 3 1] [:w 1 4] [:r 2 nil]]))))
  (testing "clean"
    (is (true? (:valid? (w4 [(inv 0 :txn [[:w 0 1]]) (ok 0 :txn [[:w 0 1]])
                             (inv 1 :txn [[:r 0 nil] [:w 1 1]]) (ok 1 :txn [[:r 0 1] [:w 1 1]])
                             (inv 2 :txn [[:r 1 nil] [:r 0 nil]]) (ok 2 :txn [[:r 1 1] [:r 0 1]])
                             (inv 3 :txn [[:w 0 2]]) (info 3 :txn [[:w 0 2]])
                             (inv 4 :txn [[:r 0 nil]]) (ok 4 :txn [[:r 0 2]])])))))
  (testing "G-single: a read sees half of another transaction"
    (let [r (w4 [(inv 0 :txn [[:w 0 1] [:w 1 1]]) (inv 1 :txn [[:r 0 nil] [:r 1 nil]])
                 (ok 0 :txn [[:w 0 1] [:w 1 1]]) (ok 1 :txn [[:r 0 1] [:r 1 nil]])])]
      (is (false? (:valid? r)))
      (is (some #{:G-single :G-single-realtime :G-single-item :G-single-item-realtime}
                (:anomaly-types r)))))
  (testing "stale read after the write completed (realtime)"
    (let [r (w4 [(inv 0 :txn [[:w 0 1]]) (ok 0 :txn [[:w 0 1]])
                 (inv 1 :txn [[:r 0 nil]]) (ok 1 :txn [[:r 0 nil]])])]
      (is (false? (:valid? r)))))
  (testing "G1c: two transactions read each other's writes"
    (let [r (w4 [(inv 0 :txn [[:r 0 nil] [:w 1 1]]) (inv 1 :txn [[:r 1 nil] [:w 0 1]])
                 (ok 0 :txn [[:r 0 1] [:w 1 1]]) (ok 1 :txn [[:r 1 1] [:w 0 1]])])]
      (is (false? (:valid? r)))
      (is (some #{:G1c :G1c-realtime} (:anomaly-types r))))))

;; ---------------------------------------------------------------------------
;; W5: pipeline

(defn w5 [ops] (run (pipeline/checker) ops))

(defn proc [p m k id type & kvs]
  [(inv p :process nil)
   (with {:process p, :type type, :f :process, :value {:m m, :k k, :id id}} kvs)])

(defn w5-history
  "enqueue 0 1; a processes 0 (ok); a counter read; b (info, did not commit)
  and c (ok) process 1; the final read. Options replace parts."
  [{:keys [extra-enq extra-procs out counters read]}]
  (concat
    [(inv 0 :enqueue 0) (ok 0 :enqueue 0)
     (inv 0 :enqueue 1) (ok 0 :enqueue 1)]
    (map (fn [m] [(inv 0 :enqueue m) (ok 0 :enqueue m)]) extra-enq)
    (proc 1 0 0 "a" :ok)
    [(inv 2 :read-counters nil) (ok 2 :read-counters (or read {0 1, 1 0}))]
    (proc 1 1 1 "b" :info :error [:txn :timeout])
    (proc 3 1 1 "c" :ok)
    (apply concat extra-procs)
    [(inv 2 :final-read nil)
     (ok 2 :final-read {:out      (or out [{:m 0, :k 0, :id "a"} {:m 1, :k 1, :id "c"}])
                        :counters (or counters {0 1, 1 1})})]))

(defn flat [ops] (vec (mapcat #(if (map? %) [%] %) ops)))

(deftest w5-pipeline
  (testing "clean"
    (is (true? (:valid? (w5 (flat (w5-history {})))))))
  (testing "lost input"
    (let [r (w5 (flat (w5-history {:extra-enq [2]})))]
      (is (false? (:valid? r)))
      (is (= [2] (:lost r)))))
  (testing "input committed twice (the info transaction did commit, and c too)"
    (let [r (w5 (flat (w5-history {:out [{:m 0, :k 0, :id "a"} {:m 1, :k 1, :id "b"}
                                         {:m 1, :k 1, :id "c"}]
                                   :counters {0 1, 1 2}})))]
      (is (false? (:valid? r)))
      (is (= 1 (:twice-count r)))))
  (testing "phantom: the record of a rolled-back transaction"
    (let [r (w5 (flat (w5-history {:extra-enq [2]
                                   :extra-procs [(proc 4 2 0 "f" :fail :error [:rolled-back {}])
                                                 (proc 5 2 0 "g" :ok)]
                                   :out [{:m 0, :k 0, :id "a"} {:m 1, :k 1, :id "c"}
                                         {:m 2, :k 0, :id "f"}]
                                   :counters {0 2, 1 1}})))]
      (is (false? (:valid? r)))
      (is (= 1 (:phantom-count r)))
      (is (= [{:m 2, :k 0, :id "g"}] (:ok-txn-missing r)))))
  (testing "phantom: a record no transaction wrote"
    (let [r (w5 (flat (w5-history {:out [{:m 0, :k 0, :id "zzz"} {:m 1, :k 1, :id "c"}]})))]
      (is (false? (:valid? r)))
      (is (= 1 (:phantom-count r)))))
  (testing "a final counter that disagrees with the records"
    (let [r (w5 (flat (w5-history {:counters {0 2, 1 1}})))]
      (is (false? (:valid? r)))
      (is (= [{:k 0, :counter 2, :out-records 1}] (:counter-mismatch r)))))
  (testing "a counter read above its upper bound"
    (let [r (w5 (flat (w5-history {:read {0 1, 1 1}})))]
      (is (false? (:valid? r)))
      (is (= 1 (:bound-violation-count r)))))
  (testing "a counter read below its lower bound"
    (let [r (w5 (flat (w5-history {:read {0 0, 1 0}})))]
      (is (false? (:valid? r)))
      (is (= 1 (:bound-violation-count r))))))

;; ---------------------------------------------------------------------------
;; W6: dedup

(defn w6 [ops] (run (dedup/checker) ops))

(defn send-op [p v type & kvs]
  [(inv p :send v)
   (with {:process p, :type type, :f :send, :value v} kvs)])

(def w6-recs
  [{:partition 0, :offset 0, :id "d0-1", :n 0}
   {:partition 1, :offset 0, :id "d1-2", :n 2}
   {:partition 1, :offset 1, :id "d1-3", :n 3}])

(defn w6-history
  [{:keys [recs second-status second-offset]}]
  (flat
    [(send-op 0 [0 "d0-1" 0] :ok :status "queued" :offset 0)
     (send-op 1 [0 "d0-1" 1] :ok :status (or second-status "duplicate")
              :offset (or second-offset 0))
     (send-op 2 [1 "d1-2" 2] :ok :status "queued" :offset 0)
     (send-op 3 [1 "d1-3" 3] :info :error [:timeout "x"])
     (inv 0 :final-read nil)
     (ok 0 :final-read (or recs w6-recs))]))

(deftest w6-dedup
  (testing "clean"
    (is (true? (:valid? (w6 (w6-history {}))))))
  (testing "two records for one transactionId"
    (let [r (w6 (w6-history {:recs (conj w6-recs {:partition 0, :offset 1, :id "d0-1", :n 1})}))]
      (is (false? (:valid? r)))
      (is (= 1 (:duplicate-record-count r)))))
  (testing "a duplicate answer at an offset that holds another record"
    (let [r (w6 (w6-history {:second-offset 5}))]
      (is (false? (:valid? r)))
      (is (= 1 (:wrong-offset-count r)))))
  (testing "an acknowledged id with no record"
    (let [r (w6 (w6-history {:recs (vec (remove #(= "d1-2" (:id %)) w6-recs))}))]
      (is (false? (:valid? r)))
      (is (= [[1 "d1-2"]] (:lost-ids r)))))
  (testing "two sends of one id both answered queued"
    (let [r (w6 (w6-history {:second-status "queued"}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:two-queued r))))))
  (testing "the stored record is not the queued send's"
    (let [r (w6 (w6-history {:recs (assoc-in w6-recs [0 :n] 1)}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:stored-n-not-queued r)))))))

;; ---------------------------------------------------------------------------
;; W11: locks and semaphores

(defn w11
  "Runs the locks checker. One second between ops, because check 6 compares
  completion times with lifetimes in seconds."
  ([ops] (w11 1 {} ops))
  ([limit test ops]
   (let [r (checker/check (locks/checker limit) (merge test-map test)
                          (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000000 i)))
                                                  ops))
                          {})]
     (println (pr-str r))
     r)))

(defn acq
  "An acquire of lock l by owner o (process p), answered with `token`."
  [p l o token & kvs]
  [(inv p :acquire l)
   (with {:process p, :type :ok, :f :acquire, :value l, :owner o, :ttl 2
          :token token, :slot 0, :already? false}
         kvs)])

(defn step
  [p l o token id type & kvs]
  [(inv p :step l)
   (with {:process p, :type type, :f :step, :value l, :owner o, :token token, :slot 0, :id id}
         kvs)])

(defn renew
  [p l o from token & kvs]
  [(inv p :renew l)
   (with {:process p, :type :ok, :f :renew, :value l, :owner o, :from from, :token token
          :slot 0, :ttl 2}
         kvs)])

(defn release
  [p l o token type]
  [(inv p :release l)
   {:process p, :type type, :f :release, :value l, :owner o, :token token, :slot 0}])

(defn rec [l token o id offset & kvs]
  (with {:l l, :slot 0, :token token, :owner o, :id id, :offset offset} kvs))

(def w11-records
  {0 [(rec 0 10 "A" "s1" 0) (rec 0 11 "A" "s2" 1) (rec 0 20 "B" "s3" 2)]
   1 [(rec 1 30 "D" "s5" 0)]})

(defn w11-history
  "A holds lock 0 (token 10), steps, renews (11), steps, releases; B is
  refused while A holds, then takes it (20) and steps; C's step with a token
  that is no longer the lock's rolls back; a get shows B; on lock 1, D's
  acquire is unknown and its retry is answered `already`. Options replace
  parts."
  [{:keys [b-token get-token records extra]}]
  (flat
    [(acq 0 0 "A" 10)
     (step 0 0 "A" 10 "s1" :ok)
     (renew 0 0 "A" 10 11)
     (step 0 0 "A" 11 "s2" :ok)
     [(inv 1 :acquire 0)
      {:process 1, :type :fail, :f :acquire, :value 0, :owner "B", :ttl 2
       :error [:not-acquired "held"]}]
     (release 0 0 "A" 11 :ok)
     (acq 1 0 "B" (or b-token 20))
     (step 1 0 "B" (or b-token 20) "s3" :ok)
     (step 2 0 "C" 11 "s4" :fail :error [:rolled-back "kv_precondition" "version" 20])
     [(inv 2 :step 0) {:process 2, :type :fail, :f :step, :value 0, :error [:not-held]}]
     [(inv 3 :get 0)
      {:process 3, :type :ok, :f :get, :value 0
       :holders [{:slot 0, :owner "B", :token (or get-token b-token 20)}]}]
     [(inv 4 :acquire 1)
      {:process 4, :type :info, :f :acquire, :value 1, :owner "D", :ttl 2
       :error [:acquire :timeout nil]}]
     (acq 4 1 "D" 30 :already? true)
     (step 4 1 "D" 30 "s5" :ok)
     (or extra [])
     [(inv 0 :final-read nil) (ok 0 :final-read (or records w11-records))]]))

(deftest w11-locks
  (testing "clean"
    (let [r (w11 (w11-history {}))]
      (is (true? (:valid? r)))
      (is (= 1 (:takeovers r)))
      (is (= 1 (:steps-fenced r)))
      (is (= 4 (:records r)))
      (is (true? (:early-judged? r)))))
  (testing "fencing: a replaced holder's record lands after the new holder's"
    (let [r (w11 (w11-history {:records (assoc w11-records 0
                                               [(rec 0 10 "A" "s1" 0) (rec 0 20 "B" "s3" 1)
                                                (rec 0 11 "A" "s2" 2)])}))]
      (is (false? (:valid? r)))
      (is (= 1 (:token-went-back-count r)))))
  (testing "one token, two owners"
    (let [r (w11 (w11-history {:records (assoc w11-records 0
                                               [(rec 0 10 "A" "s1" 0) (rec 0 10 "B" "s2" 1)
                                                (rec 0 20 "B" "s3" 2)])}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:two-owners r))))))
  (testing "a record carries a token its owner was never answered"
    (let [r (w11 (w11-history {:records (assoc-in w11-records [0 2 :token] 99)}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:token-from-nowhere r))))))
  (testing "phantom: the record of a step whose guard lost"
    (let [r (w11 (w11-history {:records (update w11-records 0 conj (rec 0 11 "C" "s4" 3))}))]
      (is (false? (:valid? r)))
      (is (= 1 (:phantom-count r)))))
  (testing "phantom: a record no step wrote"
    (let [r (w11 (w11-history {:records (assoc-in w11-records [0 2 :id] "zzz")}))]
      (is (false? (:valid? r)))
      (is (= 1 (:phantom-count r)))
      (is (= ["s3"] (map :id (:ok-step-missing r))))))
  (testing "a later grant with a lower token"
    (let [r (w11 (w11-history {:b-token 9
                               :records (assoc-in w11-records [0 2 :token] 9)}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:token-not-rising r))))))
  (testing "one token granted twice"
    (let [r (w11 (w11-history {:b-token 11
                               :records (assoc-in w11-records [0 2 :token] 11)}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:token-granted-twice r))))))
  (testing "`already` for an owner that never had the permit"
    (let [r (w11 (w11-history {:extra (acq 5 1 "E" 30 :already? true)}))]
      (is (false? (:valid? r)))
      (is (= ["E"] (map :owner (:already-from-nowhere r))))))
  (testing "a get shows a token older than one already granted"
    (let [r (w11 (w11-history {:get-token 11}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:stale-gets r))))))
  (testing "a lock answers a slot it does not have"
    (let [r (w11 (w11-history {:extra (acq 5 2 "E" 40 :slot 1)}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:slot-out-of-range r))))))
  (testing "nothing committed under a guard shows nothing"
    (is (= :unknown (:valid? (w11 [(inv 0 :final-read nil) (ok 0 :final-read {0 [], 1 []})]))))
    (is (= :unknown (:valid? (w11 (flat (acq 0 0 "A" 10))))))))

(defn w11-takeover
  "A takes lock 0 for `ttl` seconds; `between` happens; B takes it and steps.
  One second per op, so B's acquire completes a few seconds after A's began."
  [ttl between]
  (flat
    [(acq 0 0 "A" 10 :ttl ttl)
     between
     (acq 1 0 "B" 20)
     (step 1 0 "B" 20 "s1" :ok)
     [(inv 1 :final-read nil) (ok 1 :final-read {0 [(rec 0 20 "B" "s1" 0)]})]]))

(deftest w11-exclusive-leases
  (testing "taken over after the lease ended"
    (is (true? (:valid? (w11 (w11-takeover 2 []))))))
  (testing "taken over while the lease cannot have ended"
    (let [r (w11 (w11-takeover 30 []))]
      (is (false? (:valid? r)))
      (is (= 1 (:taken-early-count r)))
      (is (= {:holder "A", :taken-by "B"}
             (select-keys (first (:taken-early r)) [:holder :taken-by])))))
  (testing "the holder released first"
    (is (true? (:valid? (w11 (w11-takeover 30 (release 0 0 "A" 10 :ok))))))
    (is (true? (:valid? (w11 (w11-takeover 30 (release 0 0 "A" 10 :info)))))))
  (testing "a release that was refused released nothing"
    (is (false? (:valid? (w11 (w11-takeover 30 (release 0 0 "A" 10 :fail)))))))
  (testing "the holder's state is unknown: a renew that may have shortened the lease"
    (is (true? (:valid? (w11 (w11-takeover
                               30
                               [(inv 0 :renew 0)
                                {:process 0, :type :info, :f :renew, :value 0, :owner "A"
                                 :from 10, :slot 0, :ttl 2}]))))))
  (testing "not judged under clock faults"
    (let [r (w11 1 {:faults #{:clock}} (w11-takeover 30 []))]
      (is (true? (:valid? r)))
      (is (false? (:early-judged? r))))))

(deftest w11b-semaphore
  (let [history (fn [records]
                  (flat
                    [(acq 0 0 "A" 10)
                     (acq 1 0 "B" 12 :slot 1)
                     (step 0 0 "A" 10 "s1" :ok)
                     (step 1 0 "B" 12 "s2" :ok :slot 1)
                     (step 0 0 "A" 10 "s3" :ok)
                     [(inv 2 :get 0)
                      {:process 2, :type :ok, :f :get, :value 0
                       :holders [{:slot 0, :owner "A", :token 10}
                                 {:slot 1, :owner "B", :token 12}]}]
                     [(inv 0 :final-read nil) (ok 0 :final-read {0 records})]]))
        clean   [(rec 0 10 "A" "s1" 0) (rec 0 12 "B" "s2" 1 :slot 1) (rec 0 10 "A" "s3" 2)]]
    (testing "clean: two slots interleave on one resource, each with its own tokens"
      (let [r (w11 2 {} (history clean))]
        (is (true? (:valid? r)))
        (is (= 0 (:gets-with-an-owner-in-two-slots r)))))
    (testing "the same records on a lock are a slot it does not have"
      (is (false? (:valid? (w11 1 {} (history clean))))))
    (testing "fencing is per slot"
      (let [r (w11 2 {} (history (conj clean (rec 0 11 "B" "s2" 3 :slot 1))))]
        (is (false? (:valid? r)))
        (is (= 1 (:token-went-back-count r)))))))

;; ---------------------------------------------------------------------------
;; Nemeses that need no cluster to check

(deftest nemeses-offline
  (testing "the membership nemesis offers its three ops, and is off unless asked for"
    (is (= #{:member-cycle :member-double :member-heal}
           (n/fs (qn/membership-nemesis))))
    (is (nil? (:generator (qn/membership-package {:faults #{:kill}, :interval 10}))))
    (is (some? (:generator (qn/membership-package {:faults #{:membership}, :interval 10})))))
  (testing "a membership answer is recorded compactly"
    (is (= {:leader 2, :term 7, :voters [1 2 3 5], :learners [4], :in-flight true}
           (qm/summary {:leader 2, :term 7, :voters [5 3 2 1], :learners [4]
                        :changeInFlight true, :members [{:nodeId 1}]}))))
  (testing "the restart nemesis offers its three ops"
    (is (= #{:restart :rolling-restart :restart-heal}
           (n/fs (qn/graceful-restart-nemesis)))))
  (testing "the restart package is off unless asked for"
    (is (nil? (:generator (qn/graceful-restart-package {:faults #{:kill}, :interval 10}))))
    (is (some? (:generator (qn/graceful-restart-package {:faults #{:restart}, :interval 10})))))
  (testing "the corrupt package draws from the classes asked for"
    (let [g (:generator (qn/corrupt-package {:faults #{:corrupt}, :interval 1
                                             :corrupt-classes [:store]}))]
      (is (some? g))))
  (testing "fault outcomes are counted"
    (let [r (checker/check
              (qn/fault-summary-checker) test-map
              (hist [{:process :nemesis, :type :info, :f :corrupt-file, :value {:class :store}}
                     {:process :nemesis, :type :info, :f :corrupt-file
                      :value {:class :store, :outcome :refused, :remedy :restore
                              :restored-up? true}}
                     {:process :nemesis, :type :info, :f :corrupt-file, :value nil}
                     {:process :nemesis, :type :info, :f :corrupt-file
                      :value {:class :store, :outcome :runtime-exit, :remedy :rejoin
                              :rejoin {:rejoined false}}}
                     {:process :nemesis, :type :info, :f :member-cycle, :value nil}
                     {:process :nemesis, :type :info, :f :member-cycle
                      :value {:removals [{:status 200, :removed true}]
                              :rejoins  [{:rejoined true}]}}
                     {:process :nemesis, :type :info, :f :member-double, :value nil}
                     {:process :nemesis, :type :info, :f :member-double
                      :value {:removals [{:status 200, :removed true}
                                         {:status 409, :code "no_quorum"}]
                              :rejoins  [{:rejoined true}]}}
                     {:process :nemesis, :type :info, :f :member-heal, :value nil}
                     {:process :nemesis, :type :info, :f :member-heal
                      :value {:healed [], :after {:voters [1 2 3 4 5]}}}
                     {:process :nemesis, :type :info, :f :corrupt-heal, :value nil}
                     {:process :nemesis, :type :info, :f :corrupt-heal
                      :value {:node "n3", :late-exit {:why "FATAL: raft store: CORRUPT"
                                                      :rejoin {:rejoined true}}}}])
              {})]
      (println (pr-str r))
      (is (true? (:valid? r)))
      (is (= 2 (:corrupt-trials r)))
      (is (= {[:store :refused :restore] 1, [:store :runtime-exit :rejoin] 1}
             (into {} (:corrupt-outcomes r))))
      (is (= 1 (count (:corrupt-not-back r))))
      (is (= {[200 nil] 2, [409 "no_quorum"] 1} (:member-removals r)))
      (is (= {true 2} (:member-rejoins r)))
      (is (= {:voters [1 2 3 4 5]} (:member-final r)))
      (is (= [{:why "FATAL: raft store: CORRUPT"}] (:corrupt-late-exits r)))
      (is (= {true 1} (:corrupt-late-rejoined r))))))
