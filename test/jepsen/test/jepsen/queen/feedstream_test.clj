(ns jepsen.queen.feedstream-test
  "W19 (feed streams): the two query checks and the prefix, on crafted
  reads. No cluster, no runner."
  (:require [clojure.test :refer :all]
            [jepsen.queen.workload.feedstream :as fs]))

(deftest prefixes
  (testing "the query prefix is the JS SDK's (FeedRunner.js outputPrefix)"
    ; Computed with node: fd- + sha256("queen-feed\n" + queryId)[:8] + -.
    (is (= "fd-8800338a-" (fs/query-prefix "jq-map")))
    (is (= "fd-9cbe0d21-" (fs/query-prefix "jq-win")))))

;; ---------------------------------------------------------------------------
;; jq-map

(def pm (fs/query-prefix "jq-map"))

(defn in [v] {:queue "jepsen-0", :partition (str "w" (mod v 8)), :txn (str "w" v)
              :data {:v v, :kind (fs/kinds (mod v 3)), :cust "c0"}})
(defn out [v & {:keys [kind txn]}]
  {:queue "jepsen-out1", :partition (str "w" (mod v 8)), :txn (or txn (str pm "x" v))
   :data {:v v, :kind (or kind (fs/kinds (mod v 3)))}})

(def ins [(in 0) (in 2) (in 4)])
(def outs [(out 0) (out 2) (out 4)])

(deftest jq-map
  (testing "clean"
    (is (:valid? (fs/check-map ins outs pm))))
  (testing "an input with no output"
    (is (= [4] (:missing (fs/check-map ins (take 2 outs) pm)))))
  (testing "an input with two outputs (two ids: e.g. an epoch that changed)"
    (is (= [2] (:twice (fs/check-map ins (conj outs (out 2 :txn (str pm "y2"))) pm)))))
  (testing "an output nothing input"
    (is (= [6] (:extra (fs/check-map ins (conj outs (out 6)) pm)))))
  (testing "an output whose content is not its input's"
    (is (seq (:wrong (fs/check-map ins (assoc outs 1 (out 2 :kind "z")) pm)))))
  (testing "an output without its query's prefix"
    (is (seq (:unprefixed (fs/check-map ins (assoc outs 0 (out 0 :txn "w0")) pm))))))

;; ---------------------------------------------------------------------------
;; jq-win

(def pw (fs/query-prefix "jq-win"))

(defn win-in [v p ts] {:queue "jepsen-1", :partition p, :txn (str "w" v), :ts-ms ts
                       :data {:v v}})
(defn win-out [p start n]
  {:queue "jepsen-out2", :partition p, :txn (str pw "e" p start)
   :data {:key p, :start (str (java.time.Instant/ofEpochMilli start))
          :end (+ start fs/window-ms), :count n}})

(def t0 1793955600000)            ; a multiple of 5 s
(def w-ins [(win-in 1 "k0" (+ t0 100)) (win-in 3 "k0" (+ t0 4999))
            (win-in 5 "k0" (+ t0 5000)) (win-in 7 "k1" (+ t0 200))])
(def w-outs [(win-out "k0" t0 2) (win-out "k0" (+ t0 5000) 1) (win-out "k1" t0 1)])

(deftest jq-win
  (testing "clean: windows aligned on multiples of 5 s, one output each"
    (is (:valid? (fs/check-window w-ins w-outs pw))))
  (testing "a window emitted twice"
    (is (seq (:twice (fs/check-window w-ins (conj w-outs (win-out "k1" t0 1)) pw)))))
  (testing "a window whose count is not its inputs'"
    (is (seq (:wrong-count (fs/check-window w-ins (assoc w-outs 0 (win-out "k0" t0 3)) pw)))))
  (testing "a window with inputs never emitted"
    (is (= [["k1" t0]] (:missing (fs/check-window w-ins (pop w-outs) pw)))))
  (testing "an output outside every window that has inputs"
    (is (seq (:phantom (fs/check-window w-ins (conj w-outs (win-out "k2" t0 1)) pw)))))
  (testing "an input exactly on a boundary belongs to the later window"
    (is (not (:valid? (fs/check-window w-ins [(win-out "k0" t0 3) (win-out "k1" t0 1)] pw))))))
