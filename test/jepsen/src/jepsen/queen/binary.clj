(ns jepsen.queen.binary
  "The binary bodies of 2.3.0 (reference/http.mdx, Binary bodies), for
  --binary: a binary push (QPB1), a binary transaction (QTB1, its pushes as
  zstd frames the client built) and a framed pop answer (QPF1, the records
  as the queue log stores them). Every integer is little-endian.

  A stored message frame:
    u32 body_len | u8 flags | u8[16] message id (UUID v7)
    | [u8[16] trace id, flag 1] | u16 len, transactionId
    | [u16 len, producer, flag 2] | payload
  body_len counts every byte after itself; flag 4 marks an encrypted
  payload (a framed pop never carries one: the broker answers JSON then)."
  (:require [clojure.data.json :as json]
            [jepsen.queen.http :as qh])
  (:import (com.github.luben.zstd Zstd)
           (java.io ByteArrayOutputStream)
           (java.net URI)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.nio ByteBuffer ByteOrder)
           (java.nio.charset StandardCharsets)
           (java.security SecureRandom)
           (java.time Duration)))

;; ---------------------------------------------------------------------------
;; Writing

(defn- out ^ByteArrayOutputStream [] (ByteArrayOutputStream.))

(defn- le
  "n little-endian bytes of x."
  ^bytes [n x]
  (let [b (ByteBuffer/allocate n)]
    (.order b ByteOrder/LITTLE_ENDIAN)
    (case (long n)
      1 (.put b (unchecked-byte x))
      2 (.putShort b (unchecked-short x))
      4 (.putInt b (unchecked-int x))
      8 (.putLong b (long x)))
    (.array b)))

(defn- w! [^ByteArrayOutputStream o ^bytes bs] (.write o bs 0 (alength bs)) o)
(defn- u8! [o x] (w! o (le 1 x)))
(defn- u16! [o x] (w! o (le 2 x)))
(defn- u32! [o x] (w! o (le 4 x)))
(defn- utf8 ^bytes [s] (.getBytes (str s) StandardCharsets/UTF_8))
(defn- text16! [o s] (let [b (utf8 s)] (u16! o (alength b)) (w! o b)))

(def ^:private rng (SecureRandom.))

(defn uuid-v7
  "16 bytes of a version-7 UUID: 48 bits of unix ms, version 7, 74 random bits
  with the RFC 4122 variant."
  ^bytes []
  (let [b  (byte-array 16)
        ms (System/currentTimeMillis)]
    (.nextBytes ^SecureRandom rng b)
    (dotimes [i 6] (aset-byte b i (unchecked-byte (bit-shift-right ms (* 8 (- 5 i))))))
    (aset-byte b 6 (unchecked-byte (bit-or 0x70 (bit-and (aget b 6) 0x0f))))
    (aset-byte b 8 (unchecked-byte (bit-or 0x80 (bit-and (aget b 8) 0x3f))))
    b))

(defn payload-bytes
  "A payload as the one JSON document the broker checks."
  ^bytes [payload]
  (utf8 (json/write-str payload)))

(defn push-body
  "A binary push (flag 4: every item carries its transactionId) of `groups`:
  [{:queue :partition :items [{:transactionId :payload}]}]."
  ^bytes [groups]
  (let [o (out)]
    (w! o (utf8 "QPB1"))
    (u8! o 4)
    (u32! o (count groups))
    (doseq [{:keys [queue partition items]} groups]
      (text16! o queue)
      (text16! o partition)
      (u32! o (count items))
      (doseq [{:keys [transactionId payload]} items]
        (text16! o transactionId)
        (let [p (payload-bytes payload)]
          (u32! o (alength p))
          (w! o p))))
    (.toByteArray o)))

(defn stored-frames
  "The stored layout of `items` (no flags: no trace id, no producer), one
  after another."
  ^bytes [items]
  (let [o (out)]
    (doseq [{:keys [transactionId payload]} items]
      (let [t (utf8 transactionId)
            p (payload-bytes payload)]
        (u32! o (+ 1 16 2 (alength t) (alength p)))
        (u8! o 0)
        (w! o (uuid-v7))
        (u16! o (alength t))
        (w! o t)
        (w! o p)))
    (.toByteArray o)))

(defn framed-groups!
  "Writes groups in the framed push layout (flag 8): per group u16 queue,
  u16 partition, u32 n, u32 z_len, z (one zstd frame of the stored frames,
  declaring its content size)."
  [o groups]
  (u32! o (count groups))
  (doseq [{:keys [queue partition items]} groups]
    (text16! o queue)
    (text16! o partition)
    (u32! o (count items))
    (let [z (Zstd/compress (stored-frames items) 3)]
      (u32! o (alength z))
      (w! o z)))
  o)

(defn txn-body
  "A binary transaction: \"QTB1\" | u8 0 | u32 len, head (the JSON body
  without its pushes) | the pushes as framed groups."
  ^bytes [head groups]
  (let [o (out)
        h (utf8 (json/write-str head))]
    (w! o (utf8 "QTB1"))
    (u8! o 0)
    (u32! o (alength h))
    (w! o h)
    (framed-groups! o groups)
    (.toByteArray o)))

;; ---------------------------------------------------------------------------
;; Reading

(defn- reader
  [^bytes bs]
  (let [b (ByteBuffer/wrap bs)]
    (.order b ByteOrder/LITTLE_ENDIAN)
    b))

(defn- r-text
  [^ByteBuffer b n]
  (let [bs (byte-array n)] (.get b bs) (String. bs StandardCharsets/UTF_8)))

(defn- r-u16 [^ByteBuffer b] (bit-and 0xffff (long (.getShort b))))
(defn- r-u32 [^ByteBuffer b] (bit-and 0xffffffff (long (.getInt b))))

(defn parse-frames
  "The messages of one record's (inflated) bytes: [{:txn :payload :flags}]."
  [^bytes bs count]
  (let [b (reader bs)]
    (vec (for [_ (range count)]
           (let [len   (r-u32 b)
                 start (.position b)
                 flags (bit-and 0xff (long (.get b)))
                 _     (.position b (+ (.position b) 16))
                 _     (when (pos? (bit-and flags 1)) (.position b (+ (.position b) 16)))
                 txn   (r-text b (r-u16 b))
                 _     (when (pos? (bit-and flags 2)) (r-text b (r-u16 b)))
                 plen  (- (+ start len) (.position b))
                 p     (r-text b plen)]
             {:txn txn, :flags flags
              :payload (try (json/read-str p :key-fn keyword) (catch Exception _ p))})))))

(defn framed-pop?
  [^bytes bs]
  (and bs (<= 4 (alength bs))
       (= "QPF1" (String. bs 0 4 StandardCharsets/US_ASCII))))

(defn parse-pop
  "A QPF1 answer as the JSON pop answer's keys: {:messages [{:data
  :transactionId :partitionId :partition :offset :leaseId :deliveryAttempt
  :createdAtUs}] plus the leading JSON object's keys}."
  [^bytes bs]
  (let [b      (reader bs)
        _      (.position b 5)
        head   (json/read-str (r-text b (r-u32 b)) :key-fn keyword)
        queue  (r-text b (r-u16 b))
        lease  (r-text b (r-u16 b))
        group  (r-text b (r-u16 b))
        claims (r-u32 b)
        msgs   (vec (apply concat
                       (for [_ (range claims)]
                         (let [pid     (.getLong b)
                               part    (r-text b (r-u16 b))
                               start   (.getLong b)
                               end     (.getLong b)
                               attempt (r-u32 b)
                               records (r-u32 b)]
                           (vec (apply concat
                                  (for [_ (range records)]
                                    (let [base  (.getLong b)
                                          at    (.getLong b)
                                          n     (r-u32 b)
                                          codec (bit-and 0xff (long (.get b)))
                                          len   (r-u32 b)
                                          raw   (let [x (byte-array len)] (.get b x) x)
                                          bytes (case (long codec)
                                                  0 raw
                                                  1 (Zstd/decompress raw (int (Zstd/decompressedSize raw)))
                                                  (throw (ex-info "unknown codec" {:codec codec})))]
                                      (keep-indexed
                                        (fn [i m]
                                          (let [off (+ base i)]
                                            (when (<= start off end)
                                              {:data            (:payload m)
                                               :transactionId   (:txn m)
                                               :partitionId     (str pid)
                                               :partition       part
                                               :offset          off
                                               :leaseId         lease
                                               :deliveryAttempt attempt
                                               :createdAtUs     at
                                               ; The frame's own flags: a
                                               ; frame with headers (8) or a
                                               ; link (16) is the JSON
                                               ; render's alone.
                                               :frameFlags      (:flags m)})))
                                        (parse-frames bytes n))))))))))]
    (assoc head :messages msgs, :queue queue, :consumerGroup group, :framed true)))

(def json-only-flags
  "The frame flags only the JSON pop renders: headers (8) and a link (16).
  The framed pop leaves a record holding one to the JSON render; a frame
  with one in a framed answer is a message the consumer misreads."
  (bit-or 8 16))

(defn json-only-frames
  "The messages of a parsed framed pop that carry a flag only the JSON
  render reads."
  [msgs]
  (filterv #(pos? (bit-and (long (or (:frameFlags %) 0)) json-only-flags)) msgs))

;; ---------------------------------------------------------------------------
;; Requests

(defn request!
  "A request whose body is bytes; answers {:status :body} where :body is the
  parsed JSON answer, or the parsed framed pop when the answer starts with
  QPF1. Exceptions as jepsen.queen.http/request! (::qh/refused, ...)."
  [^HttpClient c method url ^bytes body content-type timeout-ms]
  (try
    (let [b    (-> (HttpRequest/newBuilder (URI/create url))
                   (.timeout (Duration/ofMillis (long timeout-ms)))
                   (.header "Content-Type" content-type))
          b    (reduce-kv (fn [b k v] (.header b (str k) (str v))) b
                          (if (clojure.string/includes? url (str ":" @qh/client-port "/"))
                            (if (not= @qh/client-port qh/http-port) @qh/client-headers {})
                            {}))
          b    (case method
                 :get  (.GET b)
                 :post (.POST b (HttpRequest$BodyPublishers/ofByteArray (or body (byte-array 0)))))
          resp (.send c (.build b) (HttpResponse$BodyHandlers/ofByteArray))
          bs   ^bytes (.body resp)]
      {:status (.statusCode resp)
       :body   (cond (framed-pop? bs)        (parse-pop bs)
                     (zero? (alength bs))     nil
                     :else (try (json/read-str (String. bs StandardCharsets/UTF_8) :key-fn keyword)
                                (catch Exception _ (String. bs StandardCharsets/UTF_8))))})
    (catch java.net.http.HttpConnectTimeoutException e
      (throw (ex-info "connect timeout" {:type ::qh/refused, :msg (str e)})))
    (catch java.net.http.HttpTimeoutException e
      (throw (ex-info "request timeout" {:type ::qh/timeout, :msg (str e)})))
    (catch java.net.ConnectException e
      (throw (ex-info "connection refused" {:type ::qh/refused, :msg (str e)})))
    (catch java.io.IOException e
      (throw (ex-info "io error" {:type ::qh/io, :msg (str e)})))))
