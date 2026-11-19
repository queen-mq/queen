(ns jepsen.queen.kafka-test
  "W14 (the Kafka facade): the outcome table of the client and the verdict on
  crafted histories, through jepsen.tests.kafka's own analysis. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [generator :as gen]
                    [history :as h]]
            [jepsen.generator.context :as gctx]
            [jepsen.tests.kafka :as kafka]
            [jepsen.queen.workload.kafka :as w])
  (:import (java.util.concurrent ExecutionException)
           (org.apache.kafka.common KafkaException)
           (org.apache.kafka.common.errors NotLeaderOrFollowerException
                                           ProducerFencedException
                                           RecordTooLargeException
                                           TimeoutException
                                           TopicAuthorizationException
                                           UnknownTopicOrPartitionException)))

;; ---------------------------------------------------------------------------
;; Keys and topics

(deftest key-mapping
  (testing "a key is one topic-partition, both ways"
    (doseq [k [0 1 7 8 9 63 64 1000]]
      (let [tp (w/tp-of k)]
        (is (= k (w/k-of (.topic tp) (.partition tp)))))))
  (testing "eight keys a topic"
    (is (= "jepsen-t0" (w/topic-of 7)))
    (is (= "jepsen-t1" (w/topic-of 8))))
  (testing "a topic that is not ours has no key"
    (is (nil? (w/k-of "orders" 0)))))

;; ---------------------------------------------------------------------------
;; The outcome table

(defn- wrapped
  "An exception as a send's future throws it."
  [t]
  (ExecutionException. t))

(deftest send-outcomes
  (testing "producer.send itself threw: nothing left the client"
    (is (= :fail (w/send-outcome (TimeoutException. "metadata") true))))
  (testing "refusals the broker answered: definite"
    (is (= :fail (w/send-outcome (wrapped (RecordTooLargeException. "big")) false)))
    (is (= :fail (w/send-outcome (wrapped (TopicAuthorizationException. "no")) false))))
  (testing "anything else may have been written"
    (is (= :info (w/send-outcome (wrapped (TimeoutException. "expired")) false)))
    (is (= :info (w/send-outcome (wrapped (NotLeaderOrFollowerException. "moved")) false)))
    (is (= :info (w/send-outcome (wrapped (UnknownTopicOrPartitionException. "?")) false))))
  (testing "the root cause decides, through every wrapper"
    (is (= "RecordTooLargeException"
           (w/error-name (wrapped (KafkaException. (RecordTooLargeException. "big"))))))))

(deftest txn-outcomes
  (testing "an abort the facade answered: definite"
    (is (= :fail (:type (w/txn-outcome "TimeoutException" true)))))
  (testing "an abort that did not answer: unknown"
    (is (= :info (:type (w/txn-outcome "TimeoutException" false)))))
  (testing "a fenced producer: unknown, whatever the abort did"
    (is (= :info (:type (w/txn-outcome "ProducerFencedException" nil))))
    (is (= :info (:type (w/txn-outcome "ProducerFencedException" true)))))
  (testing "the error says which path it took"
    (is (= [:txn "TimeoutException" :aborted] (:error (w/txn-outcome "TimeoutException" true))))
    (is (= [:txn "ProducerFencedException" :fatal] (:error (w/txn-outcome "ProducerFencedException" true))))))

;; ---------------------------------------------------------------------------
;; Crafted histories through jepsen.tests.kafka's analysis

(defn hist
  [ops]
  (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn- op [p type f v] {:process p, :type type, :f f, :value v})

(defn send-ok
  "An :ok send of v to k at offset o, with its invocation."
  [p k o v]
  [(op p :invoke :send [[:send k v]]) (op p :ok :send [[:send k [o v]]])])

(defn poll-ok
  [p reads]
  [(op p :invoke :poll [[:poll]]) (op p :ok :poll [[:poll reads]])])

(defn assign-ok
  [p ks]
  [(op p :invoke :assign ks) (op p :ok :assign ks)])

(defn judge
  "The W14 verdict on a crafted history."
  ([ops] (judge {} ops))
  ([test-opts ops]
   (let [test (merge {:sub-via #{:assign}, :ww-deps true} test-opts)
         {:keys [errors]} (kafka/analysis (hist (apply concat ops))
                                          {:ww-deps (:ww-deps test)})]
     (w/verdict test errors))))

(def clean
  [(assign-ok 1 [0 1])
   (send-ok 0 0 0 1)
   (send-ok 0 0 1 2)
   (send-ok 0 1 0 3)
   (poll-ok 1 {0 [[0 1] [1 2]], 1 [[0 3]]})])

(deftest w14-clean
  (let [v (judge clean)]
    (is (:valid? v) (pr-str v))
    (is (:kafka-model-valid? v))))

(deftest w14-lost-write
  (testing "an acknowledged send below the highest read offset, never read"
    (let [v (judge [(assign-ok 1 [0])
                    (send-ok 0 0 0 1)
                    (send-ok 0 0 1 2)
                    (send-ok 0 0 2 3)
                    (poll-ok 1 {0 [[0 1] [2 3]]})])]
      (is (not (:valid? v)))
      (is (some #{:lost-write} (:bad-error-types v)) (pr-str v)))))

(deftest w14-aborted-read
  (testing "a value whose send definitely failed is read"
    (let [v (judge [(assign-ok 1 [0])
                    (send-ok 0 0 0 1)
                    [(op 0 :invoke :send [[:send 0 9]])
                     (op 0 :fail :send [[:send 0 9]])]
                    (poll-ok 1 {0 [[0 1] [1 9]]})])]
      (is (not (:valid? v)))
      (is (some #{:G1a} (:bad-error-types v)) (pr-str v)))))

(deftest w14-duplicate
  (testing "one value at two offsets"
    (let [v (judge [(assign-ok 1 [0])
                    (send-ok 0 0 0 1)
                    (send-ok 0 0 1 2)
                    (poll-ok 1 {0 [[0 1] [1 2] [2 1]]})])]
      (is (not (:valid? v)))
      (is (some #{:duplicate} (:bad-error-types v)) (pr-str v)))))

(deftest w14-inconsistent-offsets
  (testing "a poll and a send disagree on the value at an offset"
    (let [v (judge [(assign-ok 1 [0])
                    (send-ok 0 0 0 1)
                    (poll-ok 1 {0 [[0 5]]})
                    (send-ok 0 0 1 5)])]
      (is (not (:valid? v)))
      (is (some #{:inconsistent-offsets} (:bad-error-types v)) (pr-str v)))))

(def skip
  [(assign-ok 1 [0])
   (send-ok 0 0 0 1)
   (send-ok 0 0 1 2)
   (send-ok 0 0 2 3)
   (poll-ok 1 {0 [[0 1]]})
   (poll-ok 1 {0 [[2 3]]})
   (poll-ok 2 {0 [[0 1] [1 2] [2 3]]})])

(deftest w14-poll-skip
  (testing "assigned: one consumer's next poll skips a record"
    (let [v (judge skip)]
      (is (not (:valid? v)))
      (is (some #{:poll-skip} (:bad-error-types v)) (pr-str v))))
  (testing "subscribed: a rebalance between two polls may skip; allowed, as
            Jepsen allows it for Kafka"
    (let [v (judge {:sub-via #{:subscribe}} skip)]
      (is (:valid? v) (pr-str v)))))

(deftest w14-write-cycle
  (testing "two transactions whose writes interleave in opposite orders on two
            keys (G0): Kafka's model allows it, a facade commit (one raft entry)
            must not"
    (let [v (judge [(assign-ok 2 [0 1])
                    [(op 0 :invoke :txn [[:send 0 1] [:send 1 2]])
                     (op 0 :ok :txn [[:send 0 [0 1]] [:send 1 [1 2]]])]
                    [(op 1 :invoke :txn [[:send 0 3] [:send 1 4]])
                     (op 1 :ok :txn [[:send 0 [1 3]] [:send 1 [0 4]]])]
                    (poll-ok 2 {0 [[0 1] [1 3]], 1 [[0 4] [1 2]]})])]
      (is (not (:valid? v)) (pr-str v))
      (is (some #{:G0} (:bad-error-types v)) (pr-str v))
      (is (:kafka-model-valid? v) (pr-str v)))))

(deftest w14-interleaved-transaction
  (testing "a transaction's two sends to one key with another write between
            them (int-send-skip): Kafka allows it, one raft entry must not"
    (let [v (judge [(assign-ok 2 [0])
                    [(op 0 :invoke :txn [[:send 0 1] [:send 0 2]])
                     (op 0 :ok :txn [[:send 0 [0 1]] [:send 0 [2 2]]])]
                    (send-ok 1 0 1 3)
                    (poll-ok 2 {0 [[0 1] [1 3] [2 2]]})])]
      (is (not (:valid? v)) (pr-str v))
      (is (some #{:int-send-skip} (:bad-error-types v)) (pr-str v))
      (is (:kafka-model-valid? v) (pr-str v)))))

;; ---------------------------------------------------------------------------
;; Final polls and tracking

(deftest final-polls-done
  (let [ctx (gctx/context {:concurrency 1})
        fp  (w/->FinalPolls {0 3, 1 -1, 2 -1} nil)
        ev  (fn [reads hwm] {:type :ok, :f :poll, :process 0
                             :value [[:poll reads]], :hwm hwm})]
    (testing "a key is done once a poll reads up to its target"
      (is (= {1 -1, 2 -1}
             (:target-offsets (gen/update fp {} ctx (ev {0 [[2 7] [3 8]]} nil))))))
    (testing "a key of unknown offsets is done once a poll reads up to its
              high watermark, or the partition is empty"
      (is (= {0 3}
             (:target-offsets (gen/update fp {} ctx (ev {1 [[0 5] [1 6]]} {1 1, 2 -1}))))))
    (testing "not done while the read stops below the high watermark"
      (is (= {0 3, 1 -1}
             (:target-offsets (gen/update fp {} ctx (ev {1 [[0 5]]} {1 1, 2 -1}))))))
    (testing "a key read past its target is not done while the read stops below
              the high watermark the same poll reports: a transactional send
              names no offset, so the target can sit below the acknowledged
              records (campaign 6, k-assign-txn-member: key 88, target 74, read
              to 83 of 96 by a poll cut at max.poll.records, 13 records unseen)"
      (is (= {0 3, 1 -1, 2 -1}
             (:target-offsets (gen/update fp {} ctx (ev {0 [[3 8] [4 9]]} {0 6})))))
      (is (= {1 -1, 2 -1}
             (:target-offsets (gen/update fp {} ctx (ev {0 [[5 10] [6 11]]} {0 6}))))))))

(deftest track-committed
  (let [ctx (gctx/context {:concurrency 1})
        a   (atom {})
        tc  (w/->TrackCommitted nil a)]
    (testing "every key an :ok or :info transaction wrote joins the final polls"
      (gen/update tc {} ctx {:type :ok, :f :txn, :process 0
                             :value [[:send 4 [nil 1]] [:poll {}] [:send 5 [nil 2]]]})
      (gen/update tc {} ctx {:type :info, :f :send, :process 0 :value [[:send 6 9]]})
      (gen/update tc {} ctx {:type :fail, :f :send, :process 0 :value [[:send 7 9]]})
      (is (= {4 -1, 5 -1, 6 -1} @a)))))
