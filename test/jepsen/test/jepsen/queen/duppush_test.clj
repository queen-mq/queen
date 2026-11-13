(ns jepsen.queen.duppush-test
  "W31 (copies of one push sent together): the checker on crafted histories,
  a clean one and one per planted bug, and the generator. No cluster."
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.duppush :as w]))

(def test-map {:name "duppush-synthetic", :start-time (util/local-time), :nodes ["n1" "n2" "n3"]})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn check
  ([ops] (check {} ops))
  ([test ops]
   (let [r (checker/check (w/checker) (merge test-map test) (hist ops) {})]
     (println (pr-str (dissoc r :answer-patterns)))
     r)))

(defn cp
  "A copy: c, kind, and what it was answered. In flight from `sent` to
  `answered` ms after its group began."
  [c kind sent answered & {:as m}]
  (merge {:c c, :kind kind, :node "n1", :delay-ms sent, :type :ok
          :sent-ns (* 1000000 sent), :answered-ns (* 1000000 answered)}
         m))

(defn queued [tag offset] {:tag tag, :status "queued", :offset offset})
(defn dup [tag offset] {:tag tag, :status "duplicate", :offset offset})
(defn err [tag] {:tag tag, :status "error"})

(defn grp
  [p g copies]
  (let [v {:g g, :p p, :t (w/txn-id g), :copies (mapv #(select-keys % [:c :kind :node :delay-ms]) copies)}]
    [{:process g, :type :invoke, :f :dup, :value v}
     {:process g, :type :ok, :f :dup, :value v, :copies copies, :node "n1"}]))

(defn fin
  [node logs]
  [{:process 99, :type :invoke, :f :final-read, :value nil}
   {:process 99, :type :ok, :f :final-read, :value logs, :node node, :gaps {}}])

(defn rec [g c] {:t (w/txn-id g), :c c, :g g})

;; Three groups, every kind of copy:
;;   g0 in f0: two pushes in flight together, queued and duplicate;
;;   g1 in the new partition n1: a multi queued (its filler too), a
;;      transaction rolled back as a duplicate, the same id twice in one push;
;;   g2 in f1: a transaction committed (its filler stored), a late push
;;      answered duplicate.
(def g0 (grp "f0" 0 [(cp 0 :push 0 6 :answers [(queued 0 0)])
                     (cp 1 :push 1 5 :answers [(dup 1 0)])]))
(def g1 (grp "n1" 1 [(cp 0 :multi 0 7 :answers [(queued 0 0) (queued :filler 0)])
                     (cp 1 :txn 2 8 :type :fail, :committed? false, :reason "duplicate")
                     (cp 2 :multi-twice 3 9 :answers [(dup 2 0) (dup 102 0)])]))
(def g2 (grp "f1" 2 [(cp 0 :txn 0 4 :committed? true)
                     (cp 1 :push 120 125 :answers [(dup 1 0)])]))
(def logs {"f0"   [[0 (rec 0 0)]]
           "n1"   [[0 (rec 1 0)]]
           "f1"   [[0 (rec 2 0)]]
           "fill" [[0 {:x "x1-0"}] [1 {:x "x2-0"}]]})

(defn history
  ([] (history {}))
  ([over]
   (let [steps (merge {:g0 g0, :g1 g1, :g2 g2, :fin (fin "n1" logs)} over)]
     (vec (mapcat steps [:g0 :g1 :g2 :fin])))))

(deftest clean
  (let [r (check (history))]
    (is (true? (:valid? r)))
    (is (= 3 (:groups r)))
    (is (= 1 (:fresh-partitions r)))
    (is (= 2 (:groups-with-copies-in-flight-together r)))
    (is (empty? (:problems r)))))

(deftest planted-bugs
  (testing "a second copy stored as a message of its own"
    (let [r (check (history {:g0  (grp "f0" 0 [(cp 0 :push 0 6 :answers [(queued 0 0)])
                                                (cp 1 :push 1 5 :answers [(queued 1 1)])])
                             :fin (fin "n1" (assoc logs "f0" [[0 (rec 0 0)] [1 (rec 0 1)]]))}))]
      (is (false? (:valid? r)))
      (is (= #{:stored-twice :two-winners :offset-split :ack-offset-mismatch}
             (set (keys (:problems r)))))))
  (testing "two copies answered queued, one record"
    (let [r (check (history {:g0 (grp "f0" 0 [(cp 0 :push 0 6 :answers [(queued 0 0)])
                                               (cp 1 :push 1 5 :answers [(queued 1 0)])])}))]
      (is (false? (:valid? r)))
      (is (= {:two-winners 1} (:problems r)))))
  (testing "a transaction committed beside a queued push"
    (let [r (check (history {:g2 (grp "f1" 2 [(cp 0 :txn 0 4 :committed? true)
                                               (cp 1 :push 1 3 :answers [(queued 1 0)])])}))]
      (is (false? (:valid? r)))
      (is (contains? (:problems r) :two-winners))))
  (testing "a duplicate answer that names another offset"
    (let [r (check (history {:g0 (grp "f0" 0 [(cp 0 :push 0 6 :answers [(queued 0 0)])
                                               (cp 1 :push 1 5 :answers [(dup 1 3)])])}))]
      (is (false? (:valid? r)))
      (is (= #{:offset-split :ack-offset-mismatch} (set (keys (:problems r)))))))
  (testing "an acknowledged push that is not there"
    (let [r (check (history {:fin (fin "n1" (dissoc logs "f0"))}))]
      (is (false? (:valid? r)))
      (is (= {:lost 1} (:problems r)))))
  (testing "a committed transaction whose record is not there"
    (let [r (check (history {:fin (fin "n1" (assoc logs "f1" []))}))]
      (is (false? (:valid? r)))
      (is (= {:lost 1} (:problems r)))))
  (testing "the record is not the copy that won"
    (let [r (check (history {:fin (fin "n1" (assoc logs "f0" [[0 (rec 0 1)]]))}))]
      (is (false? (:valid? r)))
      (is (= {:wrong-copy 1} (:problems r)))))
  (testing "a record of a copy nobody sent"
    (let [r (check (history {:fin (fin "n1" (assoc logs "n1" [[0 (rec 1 7)]]))}))]
      (is (false? (:valid? r)))
      (is (contains? (:problems r) :phantom))))
  (testing "every copy refused, and a record all the same"
    (let [r (check (history {:g0 (grp "f0" 0 [(cp 0 :push 0 6 :type :fail, :answers [(err 0)])
                                               (cp 1 :txn 1 5 :type :fail, :committed? false
                                                   :reason "duplicate")
                                               (cp 2 :push 1 5 :type :fail, :error [:refused "x"])])}))]
      (is (false? (:valid? r)))
      (is (= {:refused-but-stored 1} (:problems r)))))
  (testing "a multi whose own item was queued and its filler refused is not a refusal"
    ; Both copies are :fail as requests; the multi's own item still stored.
    (let [r (check (history {:g1  (grp "n1" 1 [(cp 0 :multi 0 7 :type :fail
                                                    :answers [(queued 0 0) (err :filler)])
                                                (cp 1 :txn 1 6 :type :fail, :committed? false
                                                    :reason "duplicate")])
                             :fin (fin "n1" (assoc logs "fill" [[1 {:x "x2-0"}]]))}))]
      (is (true? (:valid? r)))))
  (testing "a rolled-back transaction's filler stored"
    (let [r (check (history {:fin (fin "n1" (update logs "fill" conj [2 {:x "x1-1"}]))}))]
      (is (false? (:valid? r)))
      (is (= [:rolled-back-filler-stored] (map :why (:fillers-bad r))))))
  (testing "a queued filler missing, and one stored twice"
    (let [lost  (check (history {:fin (fin "n1" (assoc logs "fill" [[1 {:x "x2-0"}]]))}))
          twice (check (history {:fin (fin "n1" (update logs "fill" conj [2 {:x "x2-0"}]))}))]
      (is (= [:filler-lost] (map :why (:fillers-bad lost))))
      (is (= [:filler-stored-twice] (map :why (:fillers-bad twice))))))
  (testing "a record nothing sent"
    (let [r (check (history {:fin (fin "n1" (update logs "f0" conj [1 (rec 99 0)]))}))]
      (is (false? (:valid? r)))
      (is (= 1 (count (:stray-records r))))))
  (testing "two nodes that read different logs"
    (let [r (check (history {:fin (into (fin "n1" logs)
                                        (fin "n2" (assoc logs "f0" [[0 (rec 0 1)]])))}))]
      (is (false? (:valid? r)))
      (is (= [{:node "n2", :partition "f0"}] (:nodes-disagree r)))))
  (testing "no copies ever in flight together: nothing shown"
    (let [apart (fn [g p copies] (grp p g copies))
          r     (check (history {:g0 (apart 0 "f0" [(cp 0 :push 0 2 :answers [(queued 0 0)])
                                                     (cp 1 :push 5 7 :answers [(dup 1 0)])])
                                 :g1 (apart 1 "n1" [(cp 0 :multi 0 2 :answers [(queued 0 0) (queued :filler 0)])
                                                     (cp 1 :txn 5 7 :type :fail, :committed? false
                                                         :reason "duplicate")])
                                 :g2 (apart 2 "f1" [(cp 0 :txn 0 4 :committed? true)
                                                     (cp 1 :push 120 125 :answers [(dup 1 0)])])}))]
      (is (= :unknown (:valid? r)))))
  (testing "no final read"
    (is (= :unknown (:valid? (check (vec (concat g0 g1 g2))))))))

(deftest the-generator
  (let [fresh (atom #{})
        g     (w/generator {:nodes ["n1" "n2" "n3"], :dup-copies 4, :dup-fresh 0.5} fresh)
        ops   (vec (repeatedly 400 g))]
    (is (every? #(= :dup (:f %)) ops))
    (is (= (range 400) (map (comp :g :value) ops)))
    (testing "2 to --dup-copies copies, the first at once, every kind drawn"
      (is (every? #(<= 2 (count (:copies (:value %))) 4) ops))
      (is (every? #(zero? (:delay-ms (first (:copies (:value %))))) ops))
      (is (= #{:push :multi :multi-twice :txn}
             (set (mapcat #(map :kind (:copies (:value %))) ops))))
      (is (every? #(<= 0 % 300) (mapcat #(map :delay-ms (:copies (:value %))) ops))))
    (testing "new partitions are named after their group and remembered"
      (let [news (filter #(str/starts-with? (:p (:value %)) "n") ops)]
        (is (< 120 (count news) 280))
        (is (= (set (map (comp :p :value) news)) @fresh))
        (is (every? #(= (str "n" (:g (:value %))) (:p (:value %))) news)))))
  (testing "--dup-kinds narrows the kinds"
    (let [g (w/generator {:nodes ["n1"], :dup-copies 3, :dup-kinds [:txn]} (atom #{}))]
      (is (= #{:txn} (set (mapcat #(map :kind (:copies (:value %))) (repeatedly 50 g))))))))
