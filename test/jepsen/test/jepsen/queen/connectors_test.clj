(ns jepsen.queen.connectors-test
  "W28-W30 (PostgreSQL source, PostgreSQL sink, S3 sink): the checkers on
  crafted histories. No database, no bucket."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.services :as svc]
            [jepsen.queen.workload.connectors :as w])
  (:import (com.github.luben.zstd ZstdOutputStream)
           (java.io ByteArrayOutputStream)))

(def tm {:name "conn", :start-time (util/local-time), :nodes ["n1"]})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))
(defn pair [f type v & {:as kv}] [{:process 0, :type :invoke, :f f, :value v}
                                  (merge {:process 0, :type type, :f f, :value v} kv)])

;; ---------------------------------------------------------------------------
;; W28 source

(defn ev [id v lsn xid p off]
  {:partition p, :offset off, :txn (str "pg:" lsn ":" id)
   :data {:op "u", :after {:id id, :v v}, :lsn lsn, :xid xid}})

(def src-clean
  (concat (pair :txn :ok 1 :rows [[1 10] [2 11]])
          (pair :txn :fail 2 :rows [[1 20]] :error :rolled-back-on-purpose)
          (pair :txn :ok 3 :rows [[1 30]])
          (pair :read :ok nil :value [(ev 1 10 "0/10" 7 "1" 0) (ev 2 11 "0/10" 7 "2" 0)
                                      (ev 1 30 "0/20" 9 "1" 1)])))

(defn src [ops] (checker/check (w/source-checker) tm (hist ops) {}))

(defn with-read
  "The source history's ops, the final read replaced by one of `events`."
  [events]
  (concat (drop-last 2 src-clean) (pair :read :ok nil :value events)))

(def e10 (ev 1 10 "0/10" 7 "1" 0))
(def e11 (ev 2 11 "0/10" 7 "2" 0))
(def e30 (ev 1 30 "0/20" 9 "1" 1))

(deftest pgsource
  (testing "clean"
    (is (:valid? (src src-clean))))
  (testing "a committed change missing"
    (is (= [[2 11]] (:missing (src (with-read [e10 e30]))))))
  (testing "a rolled-back change in the queue"
    (is (= [[1 20]] (:rolled-back-seen (src (with-read [e10 e11 e30 (ev 1 20 "0/18" 8 "1" 2)]))))))
  (testing "a change twice"
    (is (seq (:twice (src (with-read [e10 e11 e30 (assoc e30 :offset 2)]))))))
  (testing "changes of one partition out of WAL order"
    (is (seq (:out-of-wal-order (src (with-read [(assoc e30 :offset 0) (assoc e10 :offset 1) e11]))))))
  (testing "one transaction split over two commits"
    (is (seq (:split-transactions (src (with-read [e10 (ev 2 11 "0/12" 7 "2" 0) e30])))))))

;; ---------------------------------------------------------------------------
;; W29 sink

(def sink-clean
  (concat (pair :push :ok 1) (pair :push :ok 2) (pair :push :fail 3) (pair :push :info 4)
          (pair :read :ok nil :value [["s1" 1] ["s2" 2]])))

(defn sink [ops] (checker/check (w/sink-checker) tm (hist ops) {}))

(deftest pgsink
  (testing "clean (an unknown push may be there or not)"
    (is (:valid? (sink sink-clean)))
    (is (:valid? (sink (concat sink-clean (pair :read :ok nil :value [["s1" 1] ["s2" 2] ["s4" 4]]))))))
  (testing "an acknowledged push missing"
    (is (= ["s2"] (:missing (sink (concat sink-clean (pair :read :ok nil :value [["s1" 1]])))))))
  (testing "a row written twice"
    (is (= ["s1"] (:twice (sink (concat sink-clean (pair :read :ok nil :value [["s1" 1] ["s1" 1] ["s2" 2]])))))))
  (testing "a failed push written"
    (is (= ["s3"] (:failed-written (sink (concat sink-clean (pair :read :ok nil :value [["s1" 1] ["s2" 2] ["s3" 3]])))))))
  (testing "a row nothing pushed"
    (is (= ["s9"] (:extra (sink (concat sink-clean (pair :read :ok nil :value [["s1" 1] ["s2" 2] ["s9" 9]]))))))))

;; ---------------------------------------------------------------------------
;; W30 S3

(defn rec [v p off & {:keys [key payload]}]
  {:key (or key "f1.jsonl.zst"), :partition p, :offset off, :txn (str "s" v), :payload (or payload {:v v})})

(def s3-clean
  (concat (pair :push :ok 1) (pair :push :ok 2) (pair :push :fail 3)
          (pair :read :ok nil :value [(rec 1 "p0" 0) (rec 2 "p0" 1)])))

(defn s3 [ops] (checker/check (w/s3-checker) tm (hist ops) {}))

(defn s3-with [records]
  (concat (drop-last 2 s3-clean) (pair :read :ok nil :value records)))

(deftest s3sink
  (testing "clean"
    (is (:valid? (s3 s3-clean))))
  (testing "an acknowledged push in no file"
    (is (= ["s2"] (:missing (s3 (s3-with [(rec 1 "p0" 0)]))))))
  (testing "a record in two files (a window built twice under two names)"
    (is (= ["s1"] (:twice (s3 (s3-with [(rec 1 "p0" 0) (rec 2 "p0" 1) (rec 1 "p0" 0 :key "f2.jsonl.zst")]))))))
  (testing "a hole in a partition's offsets"
    (is (seq (:holes (s3 (s3-with [(rec 1 "p0" 0) (rec 2 "p0" 2)]))))))
  (testing "a failed push in the bucket"
    (is (= ["s3"] (:failed-written (s3 (s3-with [(rec 1 "p0" 0) (rec 2 "p0" 1) (rec 3 "p0" 2)]))))))
  (testing "a payload that is not what was pushed"
    (is (seq (:payload-mismatch (s3 (s3-with [(rec 1 "p0" 0 :payload {:v 9}) (rec 2 "p0" 1)])))))))

;; ---------------------------------------------------------------------------
;; W30: reading the sink's data files

(deftest a-streamed-zstd-file-reads-whole
  ; The sink compresses a data file as a stream: its zstd frame does not say
  ; how big its content is. The reader asked for that size and threw on
  ; every file ("Original size should not be negative"), so the 8 s3sink
  ; tests of campaigns 1 and 2 read no record: 14,850 acknowledged, all
  ; missing, with no fault at all.
  (let [lines (str "{\"partition\":\"p0\",\"offset\":0,\"transactionId\":\"s1\",\"payload\":{\"v\":1}}\n"
                   "{\"partition\":\"p0\",\"offset\":1,\"transactionId\":\"s2\",\"payload\":{\"v\":2}}\n")
        bos   (ByteArrayOutputStream.)
        _     (with-open [z (ZstdOutputStream. bos)] (.write z (.getBytes lines "UTF-8")))
        raw   (.toByteArray bos)
        k     "queen/queue=jepsen-s3-0/partition=p0/0-1.jsonl.zst"]
    (with-redefs [svc/s3-list (fn [_ _] [k])
                  svc/s3-get  (fn [_ _] raw)]
      (is (= [{:key k, :partition "p0", :offset 0, :txn "s1", :payload {:v 1}}
              {:key k, :partition "p0", :offset 1, :txn "s2", :payload {:v 2}}]
             (#'w/s3-records {:start-ms 0}))))))

;; ---------------------------------------------------------------------------
;; W28: waiting for the source to stream

(deftest a-source-streams-when-one-node-says-so
  ; A connector's status is the answering node's own: `streaming` on the one
  ; node that holds its lease, `standby` on the others. The gate of
  ; 2026-11-14 asked the first node only: five of six pgsource runs timed
  ; out in setup while another node streamed.
  (let [phase-of {"n1" "standby", "n2" "streaming", "n3" :refused}]
    (with-redefs [qh/base-url (fn [node] (str "http://" node ":6632"))
                  qh/request! (fn [_c _method url & _]
                                (let [p (phase-of (second (re-find #"//(n\d+):" url)))]
                                  (if (= :refused p)
                                    (throw (ex-info "refused" {:type ::qh/refused}))
                                    {:status 200, :body {:status {:phase p}}})))]
      (let [waited (future (#'w/await-streaming! {:nodes ["n1" "n2" "n3"]} "jepsen-src"))
            got    (deref waited 5000 :still-waiting)]
        (future-cancel waited)
        (is (= {"n1" "standby", "n2" "streaming", "n3" nil} got))))))
