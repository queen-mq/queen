(ns jepsen.queen.workload.connectors
  "The in-broker connectors (integrations/postgres.mdx, s3.mdx), whose docs
  say exactly-once effects. PostgreSQL and the S3 server run on the control
  node (jepsen.queen.services); the nemesis acts on the brokers. The nodes
  need QUEEN_ENCRYPTION_KEY (the connectors seal the database password with
  it) and QUEEN_PG_ALLOW_PRIVATE_NETWORKS (the control node is private).

  W28 --workload pgsource: a source from table public.jepsen_src (id bigint
  primary key, k int, v bigint) into queue jepsen-pg. Clients run
  PostgreSQL transactions of 1 to 3 upserts of ids 0..15 (v unique per op);
  one in ten rolls back on purpose. The end waits until the queue stops
  growing, then reads it through the feed. The checker rejects: a committed
  transaction's row change missing from the queue or there twice; a change
  of a rolled-back transaction; a change nothing committed; within a
  partition, changes out of their WAL order (lsn going back by offset); one
  transaction's changes split across QueenMQ commits (two lsn for one xid).

  W29 --workload pgsink: a sink from queue jepsen-pgs into table
  public.jepsen_sink (id text, v bigint; no key, so a row written twice
  shows), append mode. Clients push {id, v}. The end waits until the table
  stops growing. The checker rejects an acknowledged push missing from the
  table or there twice, a failed push there, a row nothing pushed.

  W30 --workload s3sink: the S3 sink copies a queue named for this test
  (jepsen-s3-<start>) into bucket jepsen. Clients push. The end waits until
  the bucket stops growing, then reads every data file (JSONL, zstd). The
  checker rejects an acknowledged push in no file, a record in two files or
  twice, a failed push there, a hole in a partition's offsets below its
  highest, a record whose payload is not what was pushed."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen [http :as qh]
                          [services :as svc]]
            [jepsen.queen.workload.log :as log]
            [next.jdbc :as jdbc])
  (:import (com.github.luben.zstd Zstd)))

(def relay {"x-queen-feed-relay" "1"})

(defn db-env
  "Node env for the connector workloads."
  [test]
  (cond-> {"QUEEN_PG_ALLOW_PRIVATE_NETWORKS" "true"
           "QUEEN_PG_LEASE_TTL_MS"           "5000"}
    (= :s3sink (:workload test))
    (merge {"QUEEN_S3_EMBEDDED"     "true"
            "QUEEN_S3_QUEUES"       "*"
            "QUEEN_S3_ENDPOINT"     (svc/s3-endpoint test)
            "QUEEN_S3_REGION"       svc/s3-region
            "QUEEN_S3_BUCKET"       svc/s3-bucket
            "QUEEN_S3_ACCESS_KEY"   svc/s3-access
            "QUEEN_S3_SECRET_KEY"   svc/s3-secret
            "QUEEN_S3_PATH_STYLE"   "true"
            "QUEEN_S3_START"        "earliest"
            "QUEEN_S3_MAX_WINDOW_MS" "5000"
            "QUEEN_S3_LEASE_TTL_MS" "8000"})))

(defn- put-connector!
  [test name body]
  (let [r (qh/request! (qh/client) :put (str (qh/base-url (first (:nodes test))) "/api/v1/connectors/" name)
                       body 30000)]
    (when-not (#{200 201} (:status r))
      (throw (ex-info "connector refused" {:name name, :answer r})))
    (info "connector" name (:body r))
    r))

(defn- node-phase
  "The phase `node` reports for connector `name`: its own, not the
  cluster's. nil when the node does not answer."
  [node name]
  (try
    (get-in (qh/request! (qh/client) :get (str (qh/base-url node) "/api/v1/connectors/" name)
                         nil 10000)
            [:body :status :phase])
    (catch Exception _ nil)))

(defn- await-streaming!
  "Waits until source connector `name` reports phase streaming. Within a few
  seconds of its creation a source makes its publication and slot, copies
  the rows already there and then streams (integrations/postgres.mdx): a
  change committed before that is not streamed, only its row copied.
  pgsource 2026-11-14: all five runs began writing at once and missed
  exactly their first 1 to 12 s of changes.

  Every node is asked: a connector's status is the answering node's own,
  `streaming` on the one node that holds its lease and `standby` on the
  others (the gate of 2026-11-14 asked the first node only, and five of
  six pgsource runs timed out here while another node streamed). Returns
  each node's phase."
  [test name]
  (util/await-fn
    (fn []
      (let [phases (into {} (map (fn [node] [node (node-phase node name)]) (:nodes test)))]
        (when-not (some #{"streaming"} (vals phases))
          (throw (ex-info "connector not streaming yet" {:name name, :phases phases})))
        phases))
    {:log-message (str "waiting for connector " name " to stream"), :timeout 120000, :retry-interval 500}))

(defn- read-queue-feed
  "Every entry of `queue` from the start, through the feed (relayed)."
  [client test queue]
  (loop [from "earliest", out [], pages 0]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/feed")
                         {:from from, :queues [queue], :maxRecords 1000, :maxWaitMs 0}
                         (:client-timeout-ms test) relay)
          b (:body r)]
      (when-not (and (= 200 (:status r)) (map? b))
        (throw (ex-info "feed read failed" {:status (:status r)})))
      (let [out (into out (map (fn [e] {:partition (:partition e), :offset (:offset e)
                                        :txn (:transactionId e), :data (:payload e)})
                               (:entries b)))]
        (if (or (and (true? (:caughtUp b)) (empty? (:entries b))) (< 5000 pages))
          out
          (recur (:next b) out (inc pages)))))))

(defn- until-stable
  "Calls f (a count) until it stops changing for quiet-ms, at most max-ms."
  [f quiet-ms max-ms]
  (let [t0 (System/currentTimeMillis)]
    (loop [last nil, since (System/currentTimeMillis)]
      (let [n   (try (f) (catch Exception e (warn "while waiting:" (str e)) last))
            now (System/currentTimeMillis)]
        (cond (< max-ms (- now t0)) n
              (and (some? n) (= n last) (< quiet-ms (- now since))) n
              (= n last) (do (Thread/sleep 1000) (recur last since))
              :else (do (Thread/sleep 1000) (recur n now)))))))

;; ---------------------------------------------------------------------------
;; W28: the PostgreSQL source

(defrecord SourceClient [node base http]
  client/Client
  (open! [this test node] (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      (svc/reset-postgres! test ["public.jepsen_src (id bigint primary key, k int not null, v bigint not null)"])
      (put-connector! test "jepsen-src"
                      {:kind "source", :connection (svc/pg-conn test)
                       :source {:tables [{:table "public.jepsen_src", :queue "jepsen-pg"}]}})
      (info "connector jepsen-src:" (await-streaming! test "jepsen-src"))))

  (invoke! [this test op]
    (case (:f op)
      :txn
      (let [ids  (vec (distinct (repeatedly (inc (rand/long 3)) #(rand/long 16))))
            n    (:value op)
            back (< (rand/double) 0.1)
            ds   (svc/datasource test)]
        (try
          (jdbc/with-transaction [tx ds]
            (doseq [[i id] (map-indexed vector ids)]
              (jdbc/execute! tx ["INSERT INTO public.jepsen_src (id, k, v) VALUES (?, ?, ?)
                                  ON CONFLICT (id) DO UPDATE SET k = EXCLUDED.k, v = EXCLUDED.v"
                                 id (mod id 4) (+ (* 10 n) i)]))
            (when back (throw (ex-info "rolled back on purpose" {::rollback true}))))
          (assoc op :type :ok, :rows (vec (map-indexed (fn [i id] [id (+ (* 10 n) i)]) ids)))
          (catch clojure.lang.ExceptionInfo e
            (if (::rollback (ex-data e))
              (assoc op :type :fail, :rows (vec (map-indexed (fn [i id] [id (+ (* 10 n) i)]) ids))
                     :error :rolled-back-on-purpose)
              (throw e)))
          (catch java.sql.SQLException e
            (assoc op :type :info, :error [:sql (.getMessage e)]
                   :rows (vec (map-indexed (fn [i id] [id (+ (* 10 n) i)]) ids))))))

      :read
      (let [n (until-stable #(count (read-queue-feed this test "jepsen-pg")) 10000 120000)]
        (assoc op :type :ok, :value (read-queue-feed this test "jepsen-pg"), :count n))))

  (teardown! [this test])
  (close! [this test])
  client/Reusable
  (reusable? [this test] true))

(defn source-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            done    (->> hist (h/filter #(and (= :txn (:f %)) (not= :invoke (:type %)))) vec)
            read    (:value (last (filter #(and (= :read (:f %)) (= :ok (:type %))) hist)))
            events  (->> read (filter #(#{"c" "u"} (get-in % [:data :op]))))
            seen    (frequencies (map (fn [e] [(get-in e [:data :after :id]) (get-in e [:data :after :v])]) events))
            want    (set (mapcat :rows (filter #(= :ok (:type %)) done)))
            maybe   (set (mapcat :rows (filter #(#{:ok :info} (:type %)) done)))
            rolled  (set (mapcat :rows (filter #(= :fail (:type %)) done)))
            missing (->> want (remove seen) sort vec)
            twice   (->> seen (filter #(< 1 (val %))) (map key) sort vec)
            aborted (->> (keys seen) (filter rolled) sort vec)
            extra   (->> (keys seen) (remove maybe) (remove rolled) sort vec)
            lsn->n  (fn [s] (when (string? s)
                              (let [[a b] (str/split s #"/")]
                                (+ (* (Long/parseLong a 16) 4294967296) (Long/parseLong b 16)))))
            order   (->> read (group-by :partition)
                         (keep (fn [[p es]]
                                 (let [ls (->> es (sort-by :offset) (map #(lsn->n (get-in % [:data :lsn]))) (remove nil?))]
                                   (when-not (apply <= ls) {:partition p}))))
                         vec)
            split   (->> events (group-by #(get-in % [:data :xid]))
                         (keep (fn [[xid es]] (let [ls (set (map #(get-in % [:data :lsn]) es))]
                                                (when (and xid (< 1 (count ls))) {:xid xid, :lsns ls}))))
                         vec)]
        {:valid?            (boolean (and read (empty? missing) (empty? twice) (empty? aborted)
                                          (empty? extra) (empty? order) (empty? split)))
         :events            (count events)
         :committed-changes (count want)
         :missing           (take 16 missing)
         :missing-count     (count missing)
         :twice             (take 16 twice)
         :rolled-back-seen  (take 16 aborted)
         :extra             (take 16 extra)
         :out-of-wal-order  (take 16 order)
         :split-transactions (take 16 split)}))))

(defn pgsource-workload
  [opts]
  {:client          (map->SourceClient {})
   :checker         (source-checker)
   :test-opts       {:connectors? true, :encrypt true}
   :generator       (map (fn [n] {:f :txn, :value n}) (range))
   :final-generator (gen/once {:f :read, :value nil})})

;; ---------------------------------------------------------------------------
;; W29: the PostgreSQL sink

(defrecord SinkClient [node base http]
  client/Client
  (open! [this test node] (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      (svc/reset-postgres! test ["public.jepsen_sink (id text, v bigint)"])
      ; leaseSeconds 5, the shortest a sink takes (its default is 60): a batch
      ; a worker had popped when its node was killed comes back when its lease
      ; ends, and the final read gives up after 15 s without a new row
      ; (pgsink-lz-kill 2026-11-14: read 20 to 35 s after the last kill, the
      ; rows of the leased partitions not there yet).
      (put-connector! test "jepsen-sink"
                      {:kind "sink", :connection (svc/pg-conn test)
                       :sink {:queue "jepsen-pgs", :table "public.jepsen_sink", :mode "append"
                              :leaseSeconds 5}})))

  (invoke! [this test op]
    (case (:f op)
      :push
      (let [v (:value op)
            r (try (qh/request! http :post (str base "/api/v1/push")
                                {:items [{:queue "jepsen-pgs", :partition (str "p" (rand/long 8))
                                          :payload {:id (str "s" v), :v v}, :transactionId (str "s" v)}]}
                                (:client-timeout-ms test))
                   (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))]
        (cond (:exception r) (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info))
              (= 201 (:status r)) (if (#{"queued" "duplicate"} (:status (first (:body r))))
                                    (assoc op :type :ok)
                                    (assoc op :type :fail, :error (first (:body r))))
              :else (assoc op :type (log/push-failure test (:status r) (:body r)))))

      :read
      (let [ds (svc/datasource test)
            n  (until-stable #(:count (first (svc/execute! ds ["SELECT count(*) AS count FROM public.jepsen_sink"])))
                             15000 180000)]
        (assoc op :type :ok, :count n
               :value (mapv (juxt :id :v) (svc/execute! ds ["SELECT id, v FROM public.jepsen_sink"]))))))

  (teardown! [this test])
  (close! [this test])
  client/Reusable
  (reusable? [this test] true))

(defn sink-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            pushes  (->> hist (h/filter #(and (= :push (:f %)) (not= :invoke (:type %)))) vec)
            read    (:value (last (filter #(and (= :read (:f %)) (= :ok (:type %))) hist)))
            rows    (frequencies (map first read))
            ok      (set (map #(str "s" (:value %)) (filter #(= :ok (:type %)) pushes)))
            maybe   (set (map #(str "s" (:value %)) (filter #(#{:ok :info} (:type %)) pushes)))
            failed  (set (map #(str "s" (:value %)) (filter #(= :fail (:type %)) pushes)))
            missing (->> ok (remove rows) sort vec)
            twice   (->> rows (filter #(< 1 (val %))) (map key) sort vec)
            bad     (->> (keys rows) (filter failed) sort vec)
            extra   (->> (keys rows) (remove maybe) (remove failed) sort vec)]
        {:valid?        (boolean (and read (empty? missing) (empty? twice) (empty? bad) (empty? extra)))
         :rows          (count read)
         :acknowledged  (count ok)
         :missing       (take 16 missing)
         :missing-count (count missing)
         :twice         (take 16 twice)
         :failed-written (take 16 bad)
         :extra         (take 16 extra)}))))

(defn pgsink-workload
  [opts]
  {:client          (map->SinkClient {})
   :checker         (sink-checker)
   :test-opts       {:connectors? true, :encrypt true}
   :generator       (map (fn [v] {:f :push, :value v}) (range))
   :final-generator (gen/once {:f :read, :value nil})})

;; ---------------------------------------------------------------------------
;; W30: the S3 sink

(defn s3-queue [test] (str "jepsen-s3-" (quot (or (:start-ms test) 0) 1000)))

(defn- s3-records
  "Every record of the test queue's data files: [{:key :partition :offset
  :txn :payload}]."
  [test]
  (let [ks (->> (svc/s3-list test "queen/")
                (filter #(and (str/includes? % (str "queue=" (s3-queue test) "/"))
                              (not (str/includes? % "/_queen/"))
                              (re-find #"\.jsonl(\.zst)?$" %))))]
    (vec (for [k ks
               :let [raw   (svc/s3-get test k)
                     bytes (if (str/ends-with? k ".zst")
                             (Zstd/decompress raw (int (Zstd/decompressedSize raw)))
                             raw)]
               line (str/split-lines (String. ^bytes bytes "UTF-8"))
               :when (not (str/blank? line))
               :let [r (json/read-str line :key-fn keyword)]]
           {:key k, :partition (:partition r), :offset (:offset r), :txn (:transactionId r)
            :payload (:payload r)}))))

(defrecord S3Client [node base http]
  client/Client
  (open! [this test node] (assoc this :node node, :base (qh/base-url node), :http (qh/client)))
  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :push
      (let [v (:value op)
            r (try (qh/request! http :post (str base "/api/v1/push")
                                {:items [{:queue (s3-queue test), :partition (str "p" (rand/long 8))
                                          :payload {:v v}, :transactionId (str "s" v)}]}
                                (:client-timeout-ms test))
                   (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))]
        (cond (:exception r) (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info))
              (= 201 (:status r)) (if (#{"queued" "duplicate"} (:status (first (:body r))))
                                    (assoc op :type :ok)
                                    (assoc op :type :fail, :error (first (:body r))))
              :else (assoc op :type (log/push-failure test (:status r) (:body r)))))

      :read
      (let [n (until-stable #(count (s3-records test)) 20000 240000)]
        (assoc op :type :ok, :count n, :value (s3-records test)))))

  (teardown! [this test])
  (close! [this test])
  client/Reusable
  (reusable? [this test] true))

(defn s3-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            pushes  (->> hist (h/filter #(and (= :push (:f %)) (not= :invoke (:type %)))) vec)
            recs    (:value (last (filter #(and (= :read (:f %)) (= :ok (:type %))) hist)))
            by-txn  (group-by :txn recs)
            ok      (set (map #(str "s" (:value %)) (filter #(= :ok (:type %)) pushes)))
            failed  (set (map #(str "s" (:value %)) (filter #(= :fail (:type %)) pushes)))
            missing (->> ok (remove by-txn) sort vec)
            twice   (->> by-txn (filter #(< 1 (count (val %)))) (map key) sort vec)
            pos-dup (->> recs (group-by (juxt :partition :offset)) (filter #(< 1 (count (val %)))) (map key) vec)
            bad     (->> (keys by-txn) (filter failed) sort vec)
            holes   (->> recs (group-by :partition)
                         (keep (fn [[p rs]] (let [offs (sort (distinct (map :offset rs)))]
                                              (when-not (= offs (range 0 (inc (last offs))))
                                                {:partition p, :first (first offs), :last (last offs)
                                                 :count (count offs)}))))
                         vec)
            payload (->> recs (remove #(= (:payload %) {:v (some-> (:txn %) (subs 1) parse-long)}))
                         (mapv #(select-keys % [:txn :payload])))]
        {:valid?          (boolean (and recs (empty? missing) (empty? twice) (empty? pos-dup)
                                        (empty? bad) (empty? holes) (empty? payload)))
         :records         (count recs)
         :files           (count (distinct (map :key recs)))
         :acknowledged    (count ok)
         :missing         (take 16 missing)
         :missing-count   (count missing)
         :twice           (take 16 twice)
         :position-twice  (take 16 pos-dup)
         :failed-written  (take 16 bad)
         :holes           (take 16 holes)
         :payload-mismatch (take 16 payload)}))))

(defn s3sink-workload
  [opts]
  {:client          (map->S3Client {})
   :checker         (s3-checker)
   :test-opts       {:connectors? true, :encrypt true, :start-ms (System/currentTimeMillis)}
   :generator       (map (fn [v] {:f :push, :value v}) (range))
   :final-generator (gen/once {:f :read, :value nil})})
