(ns jepsen.queen.routing-test
  "W15-W17 (routing): each checker on a clean crafted history, then one
  history per planted bug, each caught by the rule it breaks. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [client :as client]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.routing :as r]))

(def test-map
  {:name        "routing-synthetic"
   :start-time  (util/local-time)
   :nodes       ["n1" "n2" "n3"]
   :queue-names ["jepsen-0" "jepsen-1"]})

(defn hist
  [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn check
  [c ops]
  (checker/check c test-map (hist ops) {}))

(defn- pair
  "An invocation and its completion of op `m`, completed as `type` with the
  completion's extra keys."
  [m type extra]
  [(assoc m :type :invoke)
   (merge (assoc m :type type) extra)])

;; ---------------------------------------------------------------------------
;; W15 filtered groups

(defn delivery
  "What pop-and-ack! records of a delivered message v (acked)."
  [v partition offset & {:as over}]
  (merge {:v v, :txn (r/txn-id v), :pid (str "p-" partition), :partition partition
          :offset offset, :lease (str "l" v "-" offset), :attempt 1, :ack :acked
          :headers (r/headers v), :data (r/payload v)}
         over))

(defn enq [p v part] (pair {:process p, :f :enqueue, :value v} :ok {:partition part}))

(defn deq
  "A dequeue by process p of group g delivering ds (acked), answered with
  filtered (true for a filtered group)."
  [p g ds & {:keys [filtered status] :or {status 200, filtered ::default}}]
  (pair {:process p, :f :dequeue, :value nil, :group g} :ok
        {:value (mapv :v (filter #(= :acked (:ack %)) ds)), :msgs (vec ds)
         :lease-s 3, :status status
         :filtered (if (= ::default filtered) (when (not= "gall" g) true) filtered)}))

(def w15-clean
  ; v: 0 (kind a, region x), 1 (b, y), 2 (c, x), 3 (a, y)
  (concat (enq 0 0 "w0") (enq 0 1 "w0") (enq 0 2 "w1") (enq 0 3 "w1")
          (deq 1 "ga"   [(delivery 0 "w0" 0) (delivery 3 "w1" 0)])
          (deq 1 "gx"   [(delivery 0 "w0" 0) (delivery 2 "w1" 0)])
          (deq 2 "gall" [(delivery 0 "w0" 0) (delivery 1 "w0" 1)])
          (deq 2 "gall" [(delivery 2 "w1" 0) (delivery 3 "w1" 1)])
          (deq 1 "ga"   [])
          (deq 1 "gx"   [])))

(deftest w15-filtered
  (testing "clean"
    (is (:valid? (check (r/filtered-checker) w15-clean))))

  (testing "a message the where does not take, delivered to the group"
    (let [res (check (r/filtered-checker)
                     (concat w15-clean (enq 0 4 "w2")
                             (deq 1 "ga" [(delivery 4 "w2" 0)])))]
      (is (not (:valid? res)))
      (is (= 1 (get-in res [:groups "ga" :not-matching-count])))))

  (testing "a filtered group's answer without filtered: true"
    (let [res (check (r/filtered-checker)
                     (concat w15-clean (deq 1 "gx" [] :filtered false)))]
      (is (not (:valid? res)))
      (is (= 1 (get-in res [:groups "gx" :no-filtered-echo-count])))))

  (testing "a filtered group answered 204 (no body, so no echo)"
    (let [res (check (r/filtered-checker)
                     (concat w15-clean (deq 1 "ga" [] :filtered nil :status 204)))]
      (is (= 1 (get-in res [:groups "ga" :no-filtered-echo-count])))))

  (testing "an acknowledged push the group's where takes, never acked by it"
    (let [res (check (r/filtered-checker)
                     (concat w15-clean (enq 0 6 "w3")))]
      (is (not (:valid? res)))
      (is (not (get-in res [:groups "ga" :consumer :total-queue :valid?])))
      (is (= 1 (get-in res [:groups "ga" :consumer :total-queue :lost-count])))
      (is (get-in res [:groups "gall" :consumer :total-queue :lost-count]))))

  (testing "a message acked twice by one group"
    (let [res (check (r/filtered-checker)
                     (concat w15-clean (deq 2 "ga" [(delivery 3 "w1" 0 :lease "l2")])))]
      (is (not (:valid? res)))
      (is (= 1 (get-in res [:groups "ga" :consumer :queue :acked-twice-count])))))

  (testing "headers that are not the pushed ones"
    (let [res (check (r/filtered-checker)
                     (concat (enq 0 9 "w4")
                             (deq 1 "ga" [(delivery 9 "w4" 0 :headers {:region "x"})])))]
      (is (not (:valid? res)))
      (is (seq (get-in res [:groups "ga" :header-mismatch]))))))

;; ---------------------------------------------------------------------------
;; W16 reading queues

(defn rec
  [v offset]
  {:offset offset, :txn (r/txn-id v), :v v, :data (r/payload v), :headers (r/headers v)})

(defn correct-read
  "The final read every node should give, for pushes [[queue partition v]]
  in commit order: the sources' logs, and each reader's log as its where,
  partitionBy and sources make it."
  [pushes]
  (let [append (fn [m path v]
                 (update-in m path (fn [xs] (conj (vec xs) (rec v (count xs))))))]
    (reduce (fn [m [q p v]]
              (let [m (append m [q p] v)]
                (reduce (fn [m rd]
                          (if (and (some #{q} (:from rd)) ((:matches? rd) v))
                            (append m [(:queue rd) (r/reader-partition rd p v)] v)
                            m))
                        m r/readers)))
            (sorted-map)
            pushes)))

(def w16-pushes
  ; v 0..11 to jepsen-0 / jepsen-1, partitions w0 / w1
  (vec (for [v (range 12)] [(if (even? v) "jepsen-0" "jepsen-1") (str "w" (mod v 2)) v])))

(defn w16-history
  "Pushes (all :ok, in order), the r3 catch-up finished, one final read per
  node giving `reads` (or the correct read), and consumers that acked every
  reader's records."
  ([] (w16-history (correct-read w16-pushes)))
  ([read & {:keys [reads]}]
   (let [read     read
         pushes   (mapcat (fn [[q p v]]
                            (pair {:process 0, :f :enqueue, :value v, :queue q} :ok {:partition p}))
                          w16-pushes)
         consume  (mapcat (fn [rd]
                            (let [ds (for [[p recs] (get read (:queue rd)), x recs]
                                       (delivery (:v x) p (:offset x)))]
                              (when (seq ds)
                                (pair {:process 1, :f :dequeue, :value nil, :reader (:queue rd)} :ok
                                      {:value (mapv :v ds), :msgs (vec ds), :lease-s 3, :status 200}))))
                          r/readers)
         finish   (pair {:process 2, :f :finish-catch-up, :value nil, :reader "jepsen-r3"} :ok
                        {:catching-up false})
         finals   (mapcat (fn [[p node value]]
                            (pair {:process p, :f :read, :value nil} :ok {:value value, :node node}))
                          (or reads [[3 "n1" read] [4 "n2" read]]))]
     (concat pushes finish consume finals))))

(defn w16 [ops] (check (r/reading-checker) ops))

(defn- drop-rec
  "read without the record of v in queue q."
  [read q v]
  (update read q (fn [parts] (into (sorted-map)
                                   (map (fn [[p xs]] [p (vec (remove #(= v (:v %)) xs))]))
                                   parts))))

(deftest w16-reading
  (testing "clean"
    (let [res (w16 (w16-history))]
      (is (:valid? res) (pr-str (select-keys res [:readers :lost-at-source-count :nodes-differ])))))

  (testing "a reader lacks a message its source holds and its where takes"
    (let [res (w16 (w16-history (drop-rec (correct-read w16-pushes) "jepsen-r1" 0)))]
      (is (not (:valid? res)))
      (is (= 1 (get-in res [:readers "jepsen-r1" :missing-count])))))

  (testing "a reader holds one message twice"
    (let [read (update-in (correct-read w16-pushes) ["jepsen-r2" "c0"]
                          (fn [xs] (conj xs (assoc (first xs) :offset (count xs)))))
          res  (w16 (w16-history read))]
      (is (= 1 (get-in res [:readers "jepsen-r2" :twice-count])))))

  (testing "a reader holds a message its where refuses"
    ; v 1 is kind b: r1 (kind a) must not hold it.
    (let [read (update-in (correct-read w16-pushes) ["jepsen-r1" "w1"]
                          (fn [xs] (conj (vec xs) (rec 1 (count xs)))))
          res  (w16 (w16-history read))]
      (is (= 1 (get-in res [:readers "jepsen-r1" :extra-count])))))

  (testing "a message in the wrong partition of a re-keyed reader"
    (let [read (-> (correct-read w16-pushes)
                   (drop-rec "jepsen-r2" 4)
                   (update-in ["jepsen-r2" "c7"] (fn [xs] (conj (vec xs) (rec 4 (count xs))))))
          res  (w16 (w16-history read))]
      (is (seq (get-in res [:readers "jepsen-r2" :misplaced])))))

  (testing "one source partition's messages out of their order in a reader"
    (let [read (update-in (correct-read w16-pushes) ["jepsen-r3" "w0"]
                          (fn [xs] (let [v (vec xs)]
                                     (-> v (assoc 0 (assoc (v 1) :offset 0))
                                         (assoc 1 (assoc (v 0) :offset 1))))))
          res  (w16 (w16-history read))]
      (is (seq (get-in res [:readers "jepsen-r3" :out-of-order])))))

  (testing "live links against real time: a push acknowledged before another
            was sent, placed after it (r2 merges two sources)"
    ; v 0 (jepsen-0) and v 3 (jepsen-1) both go to r2's c0 ((v div 3) mod 8
    ; = 0 for v < 3... v 0,1,2 -> c0): swap 0 and 1 in c0.
    (let [read (update-in (correct-read w16-pushes) ["jepsen-r2" "c0"]
                          (fn [xs] (let [v (vec xs)]
                                     (-> v (assoc 0 (assoc (v 1) :offset 0))
                                         (assoc 1 (assoc (v 0) :offset 1))))))
          res  (w16 (w16-history read))]
      (is (pos? (get-in res [:readers "jepsen-r2" :realtime-count])))))

  (testing "an acknowledged push missing from its source"
    (let [read (drop-rec (correct-read w16-pushes) "jepsen-0" 2)
          res  (w16 (w16-history read))]
      (is (= 1 (:lost-at-source-count res)))))

  (testing "two nodes read different logs"
    (let [good (correct-read w16-pushes)
          bad  (drop-rec good "jepsen-r1" 0)
          res  (w16 (w16-history good :reads [[3 "n1" bad] [4 "n2" good]]))]
      (is (not (:valid? res)))
      (is (seq (:nodes-differ res)))))

  (testing "the reader made mid-test never finished its catch-up"
    (let [ops (remove #(= :finish-catch-up (:f %)) (w16-history))
          res (w16 ops)]
      (is (not (get-in res [:readers "jepsen-r3" :valid?])))
      (is (false? (get-in res [:readers "jepsen-r3" :whole?])))))

  (testing "a message a reader holds, never acked by its consumers"
    (let [ops (->> (w16-history)
                   (remove #(and (= :dequeue (:f %)) (= "jepsen-r1" (:reader %)))))
          res (w16 ops)]
      (is (pos? (get-in res [:readers "jepsen-r1" :unconsumed-count])))))

  ;; rt-reading-kill 2026-11-14: the pop answered, its ack's answer was lost
  ;; (an I/O error), and the message never came back: the ack applied.
  (testing "a pop whose ack's answer was lost: the ack may have applied"
    (let [ops (map (fn [op] (if (and (= :dequeue (:f op)) (= "jepsen-r1" (:reader op)) (= :ok (:type op)))
                              (-> op (assoc :type :info, :error [:ack :jepsen.queen.http/io])
                                  (update :msgs (fn [ms] (mapv #(dissoc % :ack) ms))))
                              op))
                   (w16-history))
          res (w16 ops)]
      (is (zero? (get-in res [:readers "jepsen-r1" :unconsumed-count]))
          (pr-str (get-in res [:readers "jepsen-r1"])))
      (is (get-in res [:readers "jepsen-r1" :valid?])))))

;; ---------------------------------------------------------------------------
;; W17 link in a transaction

(defn link-rec
  [v offset]
  (assoc (rec v offset) :headers (assoc (r/headers v) :hop "1")))

(defn w17-history
  [target & {:keys [relinks]}]
  (concat (mapcat (fn [[v p]] (pair {:process 0, :f :enqueue, :value v} :ok {:partition p}))
                  [[0 "w0"] [1 "w0"] [2 "w1"]])
          (mapcat (fn [v] (pair {:process 1, :f :forward, :value nil} :ok
                                {:value v, :msgs [{:v v}], :links [{:success true, :transactionId (r/txn-id v)}]}))
                  [0 1 2])
          (mapcat (fn [links] (pair {:process 1, :f :relink, :value nil} :ok {:value 0, :links links}))
                  (or relinks [[{:success true, :duplicate true}]]))
          (pair {:process 2, :f :read, :value nil} :ok {:value {"jepsen-1" target}, :node "n1"})))

(def w17-target
  {"w0" [(link-rec 0 0) (link-rec 1 1)], "w1" [(link-rec 2 0)]})

(deftest w17-link
  (testing "clean"
    (is (:valid? (check (r/link-checker) (w17-history w17-target)))))

  (testing "an acknowledged push never forwarded"
    (let [res (check (r/link-checker) (w17-history (update w17-target "w1" (constantly []))))]
      (is (= 1 (:missing-count res)))))

  (testing "a message forwarded twice"
    (let [res (check (r/link-checker)
                     (w17-history (update w17-target "w1" conj (link-rec 2 1))))]
      (is (= 1 (:twice-count res)))))

  (testing "a link of something nothing pushed"
    (let [res (check (r/link-checker)
                     (w17-history (update w17-target "w1" conj (link-rec 99 1))))]
      (is (seq (:extra res)))))

  (testing "a link without its own header, or not over the source's"
    (let [res (check (r/link-checker)
                     (w17-history (assoc-in w17-target ["w0" 0 :headers] (r/headers 0))))]
      (is (seq (:header-mismatch res)))))

  (testing "a link into another partition than its source's"
    (let [res (check (r/link-checker)
                     (w17-history {"w0" [(link-rec 0 0)], "w1" [(link-rec 2 0) (link-rec 1 1)]}))]
      (is (seq (:misplaced res)))))

  (testing "a repeated link that committed without duplicate: true"
    (let [res (check (r/link-checker)
                     (w17-history w17-target :relinks [[{:success true}]]))]
      (is (not (:valid? res)))
      (is (seq (:relink-not-duplicate res))))))

;; ---------------------------------------------------------------------------
;; --binary: framed pops on W15/W16

(deftest framed-pops
  (let [deq    (fn [p framed bad msgs]
                 (pair {:process p, :f :dequeue, :value nil} :ok
                       {:value (mapv :v msgs), :msgs msgs, :framed framed, :json-only-frames bad}))
        m      (fn [v] {:v v, :txn (str "w" v), :partition "w3", :offset v})
        clean  (vec (concat (deq 0 false [] [(m 1) (m 2)])     ; headers: JSON
                            (deq 1 true [] [(m 3)])            ; plain: framed
                            (deq 2 true [] [])                 ; framed, empty
                            [{:process 3, :type :invoke, :f :dequeue, :value nil}
                             {:process 3, :type :ok, :f :dequeue, :value [], :msgs []}]))]
    (testing "framed and JSON answers, none holding a frame for the JSON render"
      (let [res (check (r/framed-checker) clean)]
        (is (true? (:valid? res)))
        (is (= 2 (:framed-pops res)))
        (is (= 1 (:framed-pops-with-messages res)))
        (is (= 1 (:json-pops-with-messages res)))))
    (testing "a framed answer that holds a frame with headers"
      (let [res (check (r/framed-checker)
                       (into clean (deq 4 true [{:transactionId "w9", :partition "w3", :offset 9
                                                 :frameFlags 8}]
                                        [(m 9)])))]
        (is (false? (:valid? res)))
        (is (= [8] (map :frameFlags (:json-only-frames-in-framed-answers res))))))
    (testing "without --binary nothing is judged"
      (is (true? (:valid? (check (r/framed-checker) [])))))))

(deftest padded-payloads
  (try
    (reset! r/pad-bytes 600)
    (is (= 600 (count (:pad (r/payload 3)))))
    (is (= (r/payload 3) (r/payload 3)))
    (reset! r/pad-bytes 0)
    (is (= {:v 3, :kind "a", :cust "c1"} (r/payload 3)))
    (finally (reset! r/pad-bytes 0))))

;; ---------------------------------------------------------------------------
;; W16: the ask for the mid-test reader

(defn- reader-ask
  "The reading workload's dequeue / :make-reader branch (the second generator
  of its mix): the function the interpreter calls for an op."
  [w]
  (second (:gens (:generator w))))

(defn- make-reader-op
  []
  {:type :invoke, :f :make-reader, :value nil, :reader "jepsen-r3", :process 0})

(deftest w16-mid-test-reader-ask
  ; :time-limit 0: the reader is due from the first op. The broker's answer
  ; is planted; everything else is the workload's own code.
  (let [answer (atom nil)
        during (atom nil)
        ask!   (atom nil)]
    (with-redefs [r/make-reader! (fn [_client _test op _r]
                                   (reset! during (:f (@ask!)))
                                   (merge op @answer))]
      (testing "an op the interpreter dropped is offered again"
        ; jepsen.generator.interpreter/run! asks for an op and, when its
        ; :time has not come, keeps the generator it had and asks again.
        ; gen/stagger puts nearly every op in the future. The gate of
        ; 2026-11-14 sent :make-reader in 6 of 13 reading runs.
        (let [ask (reader-ask (r/reading-workload {:time-limit 0}))]
          (is (= :make-reader (:f (ask))))
          (is (= :make-reader (:f (ask))))
          (is (= :make-reader (:f (ask))))))

      (testing "a 503 is asked again, a second later, and not while it runs"
        (let [w   (r/reading-workload {:time-limit 0})
              ask (reader-ask w)]
          (reset! ask! ask)
          (reset! answer {:type :info, :error [:configure 503 {:error "no quorum-ack lease; retry"}]})
          (is (= :info (:type (client/invoke! (:client w) test-map (make-reader-op)))))
          (is (= :dequeue @during) "offered while the ask ran")
          (is (= :dequeue (:f (ask))) "offered again at once: one per generated op while a fault lasts")
          (Thread/sleep 1100)
          (is (= :make-reader (:f (ask))))))

      (testing "a refusal the broker decided is not asked again"
        (let [w   (r/reading-workload {:time-limit 0})
              ask (reader-ask w)]
          (reset! ask! ask)
          (reset! answer {:type :fail, :error [:configure 400 {:error "holds messages"}]})
          (is (= :fail (:type (client/invoke! (:client w) test-map (make-reader-op)))))
          (Thread/sleep 1100)
          (is (= :dequeue (:f (ask))))))

      (testing "made: it is popped from then on, and never asked again"
        (let [w   (r/reading-workload {:time-limit 0})
              ask (reader-ask w)]
          (reset! ask! ask)
          (is (not (contains? @(:made (:client w)) "jepsen-r3")))
          (reset! answer {:type :ok, :catching-up false})
          (is (= :ok (:type (client/invoke! (:client w) test-map (make-reader-op)))))
          (is (contains? @(:made (:client w)) "jepsen-r3"))
          (Thread/sleep 1100)
          (is (= :dequeue (:f (ask)))))))))
