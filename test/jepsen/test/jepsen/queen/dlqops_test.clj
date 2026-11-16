(ns jepsen.queen.dlqops-test
  "W23 (ack retry, DLQ replay, dead-letter delete): the checker on crafted
  histories. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.dlqops :as w]))

(def test-map {:name "dlqops-synthetic", :start-time (util/local-time), :nodes ["n1"]})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn enq [v] [{:process 0, :type :invoke, :f :enqueue, :value v}
               {:process 0, :type :ok, :f :enqueue, :value v}])

(defn m [v offset action & {:keys [ack txn pid] :or {ack :acked, pid "1"}}]
  {:v v, :txn (or txn (str "q" v)), :pid pid, :offset offset, :lease "l", :action action, :ack ack})

(defn deq [& ms] [{:process 1, :type :invoke, :f :dequeue, :value nil}
                  {:process 1, :type :ok, :f :dequeue, :value nil, :msgs (vec ms)}])

(defn op [f type & {:as kv}] [{:process 2, :type :invoke, :f f, :value nil}
                              (merge {:process 2, :type type, :f f} kv)])

(defn dlq [& entries] (op :read-dlq :ok :value (vec entries)))

;; v 0 forced, 1 retry, 2 normal, 4 forced (replayed), 8 forced (deleted)
(def clean
  (concat (enq 0) (enq 1) (enq 2) (enq 4) (enq 8)
          (deq (m 1 0 :retry) (m 2 1 :completed))        ; 2 comes back: batch rule
          (deq (m 1 0 :retry) (m 2 1 :completed))
          (deq (m 1 0 :completed) (m 2 1 :completed))
          (deq (m 0 0 :dlq :pid "2") (m 4 1 :dlq :pid "2") (m 8 2 :dlq :pid "2"))
          (op :replay :ok :id "d4" :txn "q4" :value 4)
          (deq (m 4 3 :completed :txn "dlq:d4" :pid "2"))
          (op :delete-dlq :ok :id "d8" :txn "q8" :value 8)
          (dlq {:id "d0", :txn "q0", :v 0})))

(defn run [ops] (checker/check (w/checker) test-map (hist ops) {}))

(deftest w23-dlqops
  (testing "clean, with the batch rule's redeliveries"
    (let [r (run clean)]
      (is (:valid? r) (pr-str r))
      (is (pos? (:batch-rule-redeliveries r)))))
  (testing "a value completed twice with no retry ahead of it in its batch"
    (let [r (run (concat clean (deq (m 2 1 :completed))))]
      (is (= [2] (:completed-twice r)))))
  (testing "a retry value in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d1", :txn "q1", :v 1})))]
      (is (seq (:retry-in-dlq r)))))
  (testing "a value in the DLQ nobody acked dlq"
    (let [r (run (concat (enq 6) (butlast (butlast clean))
                         (dlq {:id "d0", :txn "q0", :v 0} {:id "d6", :txn "q6", :v 6})))]
      (is (= ["q6"] (:dlq-unasked r)))))
  (testing "a replayed dead letter still in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d4", :txn "q4", :v 4})))]
      (is (some #(= :still-in-dlq (:problem %)) (:replay-problems r)))))
  (testing "a replay whose copy came at two offsets"
    (let [r (run (concat clean (deq (m 4 5 :completed :txn "dlq:d4" :pid "2"))))]
      (is (some #(= :two-copies (:problem %)) (:replay-problems r)))))
  (testing "a deleted dead letter still in the DLQ"
    (let [r (run (concat (butlast (butlast clean)) (dlq {:id "d0", :txn "q0", :v 0} {:id "d8", :txn "q8", :v 8})))]
      (is (seq (:deleted-still-in-dlq r)))))
  (testing "an acknowledged push that ended nowhere"
    (let [r (run (concat (enq 10) clean))]
      (is (= [10] (:lost r)))))
  ;; dlqops-part 2026-11-14: replay of v 2224 answered while a partition began
  ;; was judged :fail, yet its copy dlq:<id> was delivered 47 ms later.
  (let [unknown (concat (enq 12) clean
                        (deq (m 12 3 :dlq :pid "2"))
                        (op :replay :info :id "d12" :txn "q12" :value 12 :again :fail))]
    (testing "a replay answered 503 whose copy came and completed: not lost"
      (let [r (run (concat unknown (deq (m 12 4 :completed :txn "dlq:d12" :pid "2"))))]
        (is (:valid? r) (pr-str r))
        (is (= 1 (:replays-unknown-applied r)))))
    (testing "a replay answered 503 whose copy came twice: held to the replay rules"
      (let [r (run (concat unknown (deq (m 12 4 :completed :txn "dlq:d12" :pid "2"))
                           (deq (m 12 6 :completed :txn "dlq:d12" :pid "2"))))]
        (is (some #(= :two-copies (:problem %)) (:replay-problems r)))))
    (testing "a replay answered 503, no copy, not in the DLQ: lost"
      (let [r (run unknown)]
        (is (= [12] (:lost r))))))
  (let [base (concat (enq 12) clean (deq (m 12 3 :dlq :pid "2")))
        copy (deq (m 12 4 :completed :txn "dlq:d12" :pid "2"))]
    (testing "refused on the first try, moved on the second: not lost"
      (let [r (run (concat base (op :replay :fail :id "d12" :txn "q12" :value 12 :again :ok) copy))]
        (is (:valid? r) (pr-str r))))
    (testing "refused for certain on both tries, yet its copy came"
      (let [r (run (concat base (op :replay :fail :id "d12" :txn "q12" :value 12 :again :fail
                                    :answer [409 {:dlqRowRemoved false}])
                           copy))]
        (is (= ["d12"] (map :id (:replay-refused-but-moved r))))
        (is (= [] (:lost r)))
        (is (false? (:valid? r)))))
    (testing "refused, while another replay of the same dead letter moved it"
      (let [r (run (concat base (op :replay :fail :id "d12" :txn "q12" :value 12 :again :fail)
                           (op :replay :ok :id "d12" :txn "q12" :value 12) copy))]
        (is (:valid? r) (pr-str r))))))

(deftest replay-answers
  (testing "200 with replayedAs: replayed"
    (is (= :ok (w/replay-outcome {:status 200, :body {:replayedAs {:offset 3}}}))))
  (testing "404 gone, 409 refused: nothing was moved"
    (is (= :fail (w/replay-outcome {:status 404, :body {:result "gone"}})))
    (is (= :fail (w/replay-outcome {:status 409, :body {:dlqRowRemoved false}}))))
  (testing "503, the request deadline elapsed: unknown, the move may commit"
    (is (= :info (w/replay-outcome {:status 503, :body {:error "the request deadline elapsed"}}))))
  (testing "no answer: unknown; a refused connection: nothing sent"
    (is (= :info (w/replay-outcome {:exception {:type :jepsen.queen.http/timeout}})))
    (is (= :fail (w/replay-outcome {:exception {:type :jepsen.queen.http/refused}})))))

(deftest a-copy-whose-ack-answer-was-lost
  ; Rerun of campaign 3, dlqops-lz-kill: the copy dlq:<id> of a replay was
  ; delivered once, acked `completed`, the answer lost, and it never came
  ; back: the ack applied. Not before the final drain, it proves nothing.
  (let [drain   (fn [] [{:process 1, :type :invoke, :f :dequeue, :value nil, :drain? true}
                        {:process 1, :type :ok, :f :dequeue, :value nil, :drain? true, :msgs []}])
        base    (concat (enq 0) (enq 1) (enq 2) (enq 4) (enq 8)
                        (deq (m 1 0 :retry) (m 2 1 :completed))
                        (deq (m 1 0 :retry) (m 2 1 :completed))
                        (deq (m 1 0 :completed) (m 2 1 :completed))
                        (deq (m 0 0 :dlq :pid "2") (m 4 1 :dlq :pid "2") (m 8 2 :dlq :pid "2"))
                        (op :replay :ok :id "d4" :txn "q4" :value 4))
        tail    (concat (op :delete-dlq :ok :id "d8" :txn "q8" :value 8)
                        (dlq {:id "d0", :txn "q0", :v 0}))
        unknown (deq (m 4 3 :completed :txn "dlq:d4" :pid "2" :ack :unknown))]
    (testing "acked with its answer lost before the drain, never back: completed"
      (let [r (run (concat base unknown (drain) (drain) tail))]
        (is (:valid? r) (pr-str r))
        (is (= 1 (:copies-completed-unknown r)))))
    (testing "the same ack inside the drain proves nothing yet"
      (let [r (run (concat base (drain) unknown (drain) tail))]
        (is (some #(= :copy-never-completed (:problem %)) (:replay-problems r)) (pr-str r))))
    (testing "with no drain in the history either"
      (let [r (run (concat base unknown tail))]
        (is (some #(= :copy-never-completed (:problem %)) (:replay-problems r)))))))
