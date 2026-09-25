(ns jepsen.queen.kv
  "Queen's KV over HTTP, and where to send a request.

  Routes (handlers/kv.rs): GET/PUT /api/v1/kv/:ns/*key for one key, and
  POST /api/v1/kv {\"operations\":[...]} for batches (getMany, put, incr ...).
  A precondition that loses (expect, putIfAbsent, incr bounds) is HTTP 200
  with applied:false and a reason; a read of a missing key is 200 found:false.
  A read-only call waits for the cluster's read index on the node it lands on
  (rsm/facade/real/kv.rs, linearizable), so any node may answer it.

  Outcomes of a WRITE that did not answer 200: 400/404/413 and the rate
  ladder's 429 are refusals before anything was planned (:fail); every 503
  (kv_timeout, kv_retry, kv_no_leader) and 5xx is unknown (:info)."
  (:require [clojure.string :as str]
            [jepsen.queen.http :as qh])
  (:import (java.net URLEncoder)))

(def ns-name*
  "The namespace every KV workload writes."
  "jepsen")

(defn- enc
  [s]
  (URLEncoder/encode (str s) "UTF-8"))

;; ---------------------------------------------------------------------------
;; Routing: a client bound to its node, or every request to the current leader.

(def ^:private leader-cache
  (atom {:at 0, :node nil}))

(defn leader
  "The node /health calls leader, cached for 500 ms; nil if none answers."
  [test http]
  (let [{:keys [at node]} @leader-cache
        now (System/currentTimeMillis)]
    (if (and node (< (- now at) 500))
      node
      (let [l (->> (:nodes test)
                   (filter #(= "leader" (get-in (qh/health http % 500) [:raft :role])))
                   first)]
        (reset! leader-cache {:at now, :node l})
        l))))

(defn base
  "The base URL for this request: the client's own node, or with
  :kv-route :leader, the current leader (the client's node if none is known)."
  [test client]
  (if (= :leader (:kv-route test))
    (qh/base-url (or (leader test (:http client)) (:node client)))
    (:base client)))

(defn forget-leader!
  "After a failure against the leader: look it up again next time."
  []
  (swap! leader-cache assoc :at 0))

;; ---------------------------------------------------------------------------
;; Calls

(defn get!
  "GET one key: {:found? bool :value v :version n}, or throws via qh."
  [test client k]
  (let [r (qh/request! (:http client) :get
                       (str (base test client) "/api/v1/kv/" ns-name* "/" (enc k))
                       nil (:client-timeout-ms test))]
    (if (= 200 (:status r))
      (let [b (:body r)]
        {:found?  (true? (:found b))
         :value   (:value b)
         :version (:version b)})
      {:error [(:status r) (:body r)]})))

(defn put!
  "PUT one key forever, with an optional expect version (0 = must not exist).
  {:applied? bool :reason r :value v :version n} on 200, else {:error ...}."
  ([test client k v]
   (put! test client k v nil))
  ([test client k v expect]
   (let [body (cond-> {:value v, :forever true}
                (some? expect) (assoc :expect expect))
         r    (qh/request! (:http client) :put
                           (str (base test client) "/api/v1/kv/" ns-name* "/" (enc k))
                           body (:client-timeout-ms test))]
     (if (= 200 (:status r))
       (let [b (:body r)]
         {:applied? (true? (:applied b))
          :reason   (:reason b)
          :value    (:value b)
          :version  (:version b)})
       {:error [(:status r) (:body r)]}))))

(defn batch!
  "POST /api/v1/kv with these ops (the namespace is filled in). The results
  vector on 200, else {:error ...}."
  [test client ops]
  (let [ops (mapv #(assoc % :ns ns-name*) ops)
        r   (qh/request! (:http client) :post
                         (str (base test client) "/api/v1/kv")
                         {:operations ops} (:client-timeout-ms test))]
    (if (= 200 (:status r))
      {:results (vec (:results (:body r)))}
      {:error [(:status r) (:body r)]})))

(defn write-failure
  "The completion type of a KV write that did not answer 200."
  [[status body]]
  (let [code (when (map? body) (or (:reason body) (:error body)))]
    (cond
      (#{400 404 413 429} status) :fail
      :else                       :info)))

(defn exception-type
  "The completion type of a write whose request threw (qh ex-data :type)."
  [type]
  (if (= ::qh/refused type) :fail :info))

(def db-env
  "KV workloads run far above the default per-tenant KV rates (100 writes/s,
  200 reads/s, 2000/s per cell): lift them so the rate ladder's 429s do not
  stand in for the database."
  {"QUEEN_KV_WRITE_RATE"  1000000
   "QUEEN_KV_WRITE_BURST" 1000000
   "QUEEN_KV_READ_RATE"   1000000
   "QUEEN_KV_READ_BURST"  1000000
   "QUEEN_KV_CELL_RATE"   0})
