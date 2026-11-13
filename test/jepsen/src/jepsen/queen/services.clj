(ns jepsen.queen.services
  "PostgreSQL and an S3 server on the CONTROL node, for the connector
  workloads (test/jepsen/rig/ctl-services.sh installs both). The nemesis
  never touches the control node, so the faults are the brokers'. The nodes
  reach the services at --services-host (the control node's private
  address).

  PostgreSQL: database jepsen, user jepsen, password jepsen, port 5432.
  S3: RustFS on port 9000, bucket jepsen, keys jepsen / jepsen-secret-key,
  path style. The S3 calls here are signed with AWS Signature Version 4."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.net URI URLEncoder)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.security MessageDigest)
           (java.time Duration ZonedDateTime ZoneOffset)
           (java.time.format DateTimeFormatter)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)))

;; ---------------------------------------------------------------------------
;; PostgreSQL

(defn pg-conn
  "What the broker's connector config names, and what JDBC connects to."
  [test]
  {:host (:services-host test), :port 5432, :database "jepsen", :user "jepsen"
   :password "jepsen", :sslMode "disable"})

(defn datasource
  [test]
  (jdbc/get-datasource {:dbtype "postgresql", :host (:services-host test), :port 5432
                        :dbname "jepsen", :user "jepsen", :password "jepsen"}))

(defn execute!
  [ds sql-params]
  (jdbc/execute! ds sql-params {:builder-fn rs/as-unqualified-lower-maps}))

(defn reset-postgres!
  "Drops what an earlier test left: replication slots, publications, the
  sink's progress schema (an older cluster's offsets would make a new sink
  skip messages), and the tables `tables` (\"name (columns)\" strings),
  created again empty."
  [test tables]
  (let [ds (datasource test)]
    (execute! ds ["SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active"])
    (doseq [{:keys [pubname]} (execute! ds ["SELECT pubname FROM pg_publication"])]
      (execute! ds [(str "DROP PUBLICATION IF EXISTS \"" pubname "\"")]))
    (execute! ds ["DROP SCHEMA IF EXISTS queen CASCADE"])
    (doseq [t tables]
      (let [name (first (str/split t #"\s+"))]
        (execute! ds [(str "DROP TABLE IF EXISTS " name)])
        (execute! ds [(str "CREATE TABLE " t)])))))

;; ---------------------------------------------------------------------------
;; S3, signed (SigV4), path style

(def s3-region "us-east-1")
(def s3-bucket "jepsen")
(def s3-access "jepsen")
(def s3-secret "jepsen-secret-key")

(defn s3-endpoint [test] (str "http://" (:services-host test) ":9000"))

(defn- hex [^bytes bs] (apply str (map #(format "%02x" %) bs)))
(defn- sha256 ^bytes [^bytes bs] (.digest (MessageDigest/getInstance "SHA-256") bs))
(defn- hmac ^bytes [^bytes k ^String s]
  (let [m (Mac/getInstance "HmacSHA256")]
    (.init m (SecretKeySpec. k "HmacSHA256"))
    (.doFinal m (.getBytes s "UTF-8"))))

(defn- uri-enc
  "RFC 3986 encoding, as SigV4 wants it ('/' kept in paths when `path?`)."
  [s path?]
  (let [e (-> (URLEncoder/encode (str s) "UTF-8")
              (str/replace "+" "%20") (str/replace "*" "%2A") (str/replace "%7E" "~"))]
    (if path? (str/replace e "%2F" "/") e)))

(defn sign
  "Headers for a SigV4-signed request (empty body)."
  [method host path query-params ^ZonedDateTime now]
  (let [amz    (.format now (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'"))
        day    (subs amz 0 8)
        empty  (hex (sha256 (byte-array 0)))
        cq     (->> query-params (map (fn [[k v]] [(uri-enc k false) (uri-enc v false)]))
                    (sort) (map (fn [[k v]] (str k "=" v))) (str/join "&"))
        ch     (str "host:" host "\n" "x-amz-content-sha256:" empty "\n" "x-amz-date:" amz "\n")
        sh     "host;x-amz-content-sha256;x-amz-date"
        creq   (str method "\n" (uri-enc path true) "\n" cq "\n" ch "\n" sh "\n" empty)
        scope  (str day "/" s3-region "/s3/aws4_request")
        sts    (str "AWS4-HMAC-SHA256\n" amz "\n" scope "\n" (hex (sha256 (.getBytes creq "UTF-8"))))
        k      (-> (.getBytes (str "AWS4" s3-secret) "UTF-8")
                   (hmac day) (hmac s3-region) (hmac "s3") (hmac "aws4_request"))
        sig    (hex (hmac k sts))]
    {"x-amz-date" amz
     "x-amz-content-sha256" empty
     "authorization" (str "AWS4-HMAC-SHA256 Credential=" s3-access "/" scope
                          ", SignedHeaders=" sh ", Signature=" sig)}))

(defn- s3-get-bytes
  "GET path?query on the S3 server: {:status :body bytes}."
  [test path query]
  (let [ep    (URI/create (s3-endpoint test))
        host  (str (.getHost ep) ":" (.getPort ep))
        qs    (->> query (map (fn [[k v]] (str (uri-enc k false) "=" (uri-enc v false)))) (str/join "&"))
        url   (str (s3-endpoint test) (uri-enc path true) (when (seq qs) (str "?" qs)))
        hdrs  (sign "GET" host path query (ZonedDateTime/now ZoneOffset/UTC))
        b     (reduce-kv (fn [b k v] (.header b k v))
                         (-> (HttpRequest/newBuilder (URI/create url)) (.timeout (Duration/ofSeconds 30)))
                         hdrs)
        resp  (.send (HttpClient/newHttpClient) (.build (.GET b)) (HttpResponse$BodyHandlers/ofByteArray))]
    {:status (.statusCode resp), :body (.body resp)}))

(defn s3-list
  "Every key under `prefix` in the bucket (ListObjectsV2, paged)."
  [test prefix]
  (loop [token nil, out []]
    (let [r   (s3-get-bytes test (str "/" s3-bucket)
                            (cond-> {"list-type" "2", "prefix" prefix}
                              token (assoc "continuation-token" token)))
          xml (String. ^bytes (:body r) "UTF-8")]
      (when-not (= 200 (:status r))
        (throw (ex-info "s3 list failed" {:status (:status r), :body (subs xml 0 (min 400 (count xml)))})))
      (let [keys  (mapv second (re-seq #"<Key>([^<]+)</Key>" xml))
            more  (re-find #"<IsTruncated>true</IsTruncated>" xml)
            token (second (re-find #"<NextContinuationToken>([^<]+)</NextContinuationToken>" xml))]
        (if (and more token)
          (recur token (into out keys))
          (into out keys))))))

(defn s3-get
  "One object's bytes."
  ^bytes [test key]
  (let [r (s3-get-bytes test (str "/" s3-bucket "/" key) {})]
    (when-not (= 200 (:status r))
      (throw (ex-info "s3 get failed" {:key key, :status (:status r)})))
    (:body r)))
