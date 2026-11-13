(ns jepsen.queen.binary-test
  "The binary bodies' codec (--binary): frames round trip, a framed pop
  answer built by hand decoded back, claim ranges honoured. No broker."
  (:require [clojure.test :refer :all]
            [jepsen.queen.binary :as bin])
  (:import (com.github.luben.zstd Zstd)
           (java.io ByteArrayOutputStream)
           (java.nio ByteBuffer ByteOrder)))

(deftest uuid-v7
  (let [u (bin/uuid-v7)]
    (is (= 16 (alength u)))
    (is (= 0x70 (bit-and 0xf0 (aget u 6))) "version 7")
    (is (= 0x80 (bit-and 0xc0 (aget u 8))) "RFC 4122 variant")))

(deftest frames-round-trip
  (let [items [{:transactionId "w1", :payload 1} {:transactionId "w-two", :payload {:v 2, :k "x"}}]
        bs    (bin/stored-frames items)]
    (is (= [{:txn "w1", :payload 1, :flags 0} {:txn "w-two", :payload {:v 2, :k "x"}, :flags 0}]
           (bin/parse-frames bs 2)))))

(defn- le-buf [n] (doto (ByteBuffer/allocate n) (.order ByteOrder/LITTLE_ENDIAN)))

(defn- framed-pop-of
  "A QPF1 answer: one claim of partition w3 (pid 6) from `start` to `end`,
  one zstd record at base offset 10 holding the `n` frames `frames`."
  [^bytes frames n start end]
  (let [o     (ByteArrayOutputStream.)
        w     (fn [^bytes b] (.write o b 0 (alength b)))
        u8    (fn [x] (w (.array (doto (le-buf 1) (.put (unchecked-byte x))))))
        u16   (fn [x] (w (.array (doto (le-buf 2) (.putShort (unchecked-short x))))))
        u32   (fn [x] (w (.array (doto (le-buf 4) (.putInt (unchecked-int x))))))
        u64   (fn [x] (w (.array (doto (le-buf 8) (.putLong (long x))))))
        t16   (fn [s] (let [b (.getBytes ^String s "UTF-8")] (u16 (alength b)) (w b)))
        head  (.getBytes "{\"frames\":true}" "UTF-8")
        z     (Zstd/compress frames 3)]
    (w (.getBytes "QPF1" "US-ASCII")) (u8 1) (u32 (alength head)) (w head)
    (t16 "jepsen-0") (t16 "lease-1") (t16 "g1") (u32 1)
    (u64 6) (t16 "w3") (u64 start) (u64 end) (u32 2) (u32 1)
    (u64 10) (u64 1794578740000000) (u32 n) (u8 1) (u32 (alength z)) (w z)
    (.toByteArray o)))

(defn- framed-pop
  "[[framed-pop-of]] holding `items` as the broker stores them."
  [items start end]
  (framed-pop-of (bin/stored-frames items) (count items) start end))

(deftest framed-pop-decoding
  (let [items (mapv (fn [v] {:transactionId (str "w" v), :payload v}) [7 8 9])]
    (testing "every message of the claim, with its offset, lease and attempt"
      (let [a (bin/parse-pop (framed-pop items 10 12))]
        (is (true? (:frames a)))
        (is (= [[10 7 "w7"] [11 8 "w8"] [12 9 "w9"]]
               (mapv (juxt :offset :data :transactionId) (:messages a))))
        (is (every? #(and (= "lease-1" (:leaseId %)) (= 2 (:deliveryAttempt %))
                          (= "6" (:partitionId %)) (= "w3" (:partition %)))
                    (:messages a)))))
    (testing "only the offsets inside the claim belong to the delivery"
      (is (= [11] (mapv :offset (:messages (bin/parse-pop (framed-pop items 11 11)))))))
    (testing "the form is told by its first four bytes"
      (is (bin/framed-pop? (framed-pop items 10 12)))
      (is (not (bin/framed-pop? (.getBytes "{\"messages\":[]}" "UTF-8")))))))

(deftest frames-only-the-json-render-reads
  ; A framed answer whose second frame says headers (8) or a link (16): the
  ; broker must never send one (the record is the JSON render's), and the
  ; client tells it by the frame's own flags.
  (let [items  (mapv (fn [v] {:transactionId (str "w" v), :payload v}) [7 8])
        with   (fn [flag]
                 (let [bs     (bin/stored-frames items)
                       first* (+ 4 (.getInt (doto (ByteBuffer/wrap bs) (.order ByteOrder/LITTLE_ENDIAN)) 0))]
                   ; byte 4 of a frame is its flags
                   (aset-byte bs (+ first* 4) (unchecked-byte flag))
                   (bin/parse-pop (framed-pop-of bs 2 10 11))))]
    (testing "a plain answer has none"
      (let [a (bin/parse-pop (framed-pop items 10 11))]
        (is (true? (:framed a)))
        (is (= [0 0] (mapv :frameFlags (:messages a))))
        (is (empty? (bin/json-only-frames (:messages a))))))
    (testing "headers"
      (let [a (with 8)]
        (is (= [0 8] (mapv :frameFlags (:messages a))))
        (is (= ["w8"] (mapv :transactionId (bin/json-only-frames (:messages a)))))))
    (testing "a link"
      (is (= ["w8"] (mapv :transactionId (bin/json-only-frames (:messages (with 16)))))))
    (testing "the other flags are the framed pop's own (a trace id, 1)"
      (is (empty? (bin/json-only-frames [{:frameFlags 1} {:frameFlags 4} {}]))))))
