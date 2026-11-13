(ns jepsen.queen.workload.scoped
  "W22 --workload scoped (needs --via-proxy): scoped keys and client tokens
  (2.8.1, manage/client-tokens.mdx). At setup the harness, with the
  control-plane token, makes one scoped key whose grants are
    queue jepsen-s, partition {sub}: produce, consume
    kv devices, key {sub}/*: read, write
  and mints one client token per device d0..d4 with it. Each client is a
  device (thread i is device d(i mod 5)) and sends its own token.

  Ops:
    :push-own    push {v, by} to its own partition: allowed
    :push-other  push {v, by} to another device's partition: must be
                 refused, 403 scope_denied
    :pop-own     pop its own partition (queue mode) and ack: allowed
    :kv-own      put devices/<sub>/k = {by, v}: allowed
    :kv-other    put devices/<other>/k = {by, v}: must be refused
  At the end the harness (bootstrap key) reads every partition of jepsen-s
  by offset and every device's KV key.

  The checker rejects: a refused-by-design op that was answered as
  accepted; a record of a :push-other in the log, or a KV value written by
  another device than its key's, whatever the answer was (a timeout is no
  excuse); an acknowledged own push missing from the log."
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.routing :as routing])
  (:import (java.net URLEncoder)
           (java.security MessageDigest)))

(def queue "jepsen-s")
(def devices 5)
(defn device [i] (str "d" i))

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn sha256-hex
  [^String s]
  (apply str (map #(format "%02x" %) (.digest (MessageDigest/getInstance "SHA-256") (.getBytes s "UTF-8")))))

(defonce ^:private tokens (atom {}))

(defn- cp
  "A control-plane call through the proxy, with the cp token."
  [http test method path body]
  (qh/request! http method (str (qh/base-url (first (:nodes test))) path) body 15000
               {"x-queen-cp-token" (:proxy-cp-token test)}))

(defn setup-key!
  "The scoped key and one token per device; returns {device token}."
  [test]
  (let [http    (qh/client)
        cluster (cp http test :get "/api/cp/clusters/jepsen" nil)
        cid     (get-in cluster [:body :id])
        key     (str "qk_scoped_" (apply str (repeatedly 40 #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))
        made    (cp http test :post "/api/cp/keys"
                    {:cluster_id cid, :name (str "devices-" (System/currentTimeMillis))
                     :key_hash (sha256-hex key)
                     :grants [{:queue queue, :partition "{sub}", :ops ["produce" "consume"]}
                              {:kv "devices", :key "{sub}/*", :ops ["read" "write"]}]})]
    (when-not (and cid (#{200 201} (:status made)))
      (throw (ex-info "could not make the scoped key" {:cluster cluster, :made made})))
    (into (sorted-map)
          (for [i (range devices)]
            (let [r (qh/request! http :post (str (qh/base-url (first (:nodes test))) "/api/v1/tokens")
                                 {:sub (device i), :ttlSeconds 86400} 15000
                                 {"authorization" (str "Bearer " key)})]
              (when-not (and (#{200 201} (:status r)) (get-in r [:body :token]))
                (throw (ex-info "could not mint a token" {:device (device i), :answer r})))
              [(device i) (get-in r [:body :token])])))))

(defn- request
  [client method url body timeout token]
  (try (qh/request! (:http client) method url body timeout {"authorization" (str "Bearer " token)})
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn outcome
  "{:type :error} of an answer to an op that is :allowed or :forbidden. A
  forbidden op must answer 403: 403 is its :fail (the expected refusal),
  2xx its :ok (accepted: the checker's violation), the rest unknown."
  [kind r ok-statuses]
  (cond
    (:exception r)
    {:type (if (= ::qh/refused (:type (:exception r))) :fail :info)
     :error [:exception (:type (:exception r))]}

    (ok-statuses (:status r))
    {:type :ok}

    (= 403 (:status r))
    {:type :fail, :error [:forbidden (some-> r :body :code)]}

    (<= 400 (:status r) 499)
    {:type :fail, :error [(:status r) (:body r)]}

    :else
    {:type :info, :error [(:status r) (:body r)]}))

(defn- me [test op] (device (mod (:process op) devices)))
(defn- other [d] (let [i (parse-long (subs d 1))] (device (mod (inc (+ i (rand/long (dec devices)))) devices))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      (let [r (qh/request! http :post (str base "/api/v1/configure")
                           {:queue queue, :options {:leaseTime 5, :retryLimit 1000000}} 15000)]
        (when-not (= 200 (:status r))
          (throw (ex-info "configure jepsen-s failed" {:answer r}))))
      (reset! tokens (setup-key! test))
      (info "scoped tokens minted for" (keys @tokens))))

  (invoke! [this test op]
    (let [d     (me test op)
          token (or (get @tokens d) (throw (ex-info "no token yet" {:device d})))]
      (case (:f op)
        (:push-own :push-other)
        (let [to (if (= :push-own (:f op)) d (other d))
              r  (request this :post (str base "/api/v1/push")
                          {:items [{:queue queue, :partition to, :payload {:v (:value op), :by d}
                                    :transactionId (str "s" (:value op))}]}
                          (:client-timeout-ms test) token)]
          (merge (assoc op :device d, :to to) (outcome (:f op) r #{201})))

        :pop-own
        (let [r (request this :get (str base "/api/v1/pop/queue/" (enc queue) "/partition/" (enc d)
                                        "?batch=5&wait=false")
                         nil (:client-timeout-ms test) token)]
          (if (= 200 (:status r))
            (let [msgs (:messages (:body r))
                  ar   (when (seq msgs)
                         (request this :post (str base "/api/v1/ack/batch")
                                  {:acknowledgments (mapv (fn [m] {:transactionId (:transactionId m)
                                                                   :partitionId (:partitionId m)
                                                                   :leaseId (:leaseId m)
                                                                   :status "completed"})
                                                          msgs)}
                                  (:client-timeout-ms test) token))]
              (assoc op :type :ok, :device d, :value (mapv #(get-in % [:data :v]) msgs)
                     :ack-status (:status ar)))
            (merge (assoc op :device d) (outcome :pop-own r #{204}) {:value []})))

        (:kv-own :kv-other)
        (let [whose (if (= :kv-own (:f op)) d (other d))
              r     (request this :put (str base "/api/v1/kv/devices/" (enc (str whose "/k")))
                             {:value {:by d, :v (:value op)}, :forever true}
                             (:client-timeout-ms test) token)]
          (merge (assoc op :device d, :whose whose) (outcome (:f op) r #{200})))

        :read
        (try
          (let [admin {:http http, :base base, :node node}
                recs  (vec (mapcat (fn [i] (map #(assoc % :partition (device i))
                                                (routing/fetch-partition admin test queue (device i))))
                                   (range devices)))
                kv    (into (sorted-map)
                            (for [i (range devices)]
                              (let [r (qh/request! http :get (str base "/api/v1/kv/devices/" (enc (str (device i) "/k")))
                                                   nil (:client-timeout-ms test))]
                                [(device i) (get-in r [:body :value])])))]
            (assoc op :type :ok, :value {:records recs, :kv kv}))
          (catch clojure.lang.ExceptionInfo e
            (assoc op :type :fail, :error [:read (dissoc (ex-data e) :body)]))))))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist     (h/client-ops history)
            done     (->> hist (h/filter #(not= :invoke (:type %))) vec)
            accepted (->> done (filter #(and (#{:push-other :kv-other} (:f %)) (= :ok (:type %))))
                          (mapv #(select-keys % [:f :device :to :whose :value])))
            read     (:value (last (filter #(and (= :read (:f %)) (= :ok (:type %))) done)))
            recs     (:records read)
            landed   (->> recs
                          (filter (fn [r] (let [by (get-in r [:data :by])]
                                            (and by (not= by (:partition r))))))
                          (mapv #(select-keys % [:partition :data])))
            kv-foreign (->> (:kv read)
                            (keep (fn [[d v]] (when (and (map? v) (not= d (:by v)))
                                                {:key (str d "/k"), :written-by (:by v)})))
                            vec)
            own-ok   (->> done (filter #(and (= :push-own (:f %)) (= :ok (:type %)))) (map :value) set)
            in-log   (set (keep #(get-in % [:data :v]) recs))
            lost     (->> own-ok (remove in-log) sort vec)]
        {:valid?            (boolean (and read (empty? accepted) (empty? landed)
                                          (empty? kv-foreign) (empty? lost)))
         :refusals          (->> done (filter #(#{:push-other :kv-other} (:f %)))
                                 (map (juxt :f :type)) frequencies)
         :accepted-forbidden (take 16 accepted)
         :landed-forbidden  (take 16 landed)
         :kv-foreign        kv-foreign
         :lost-own          (take 16 lost)
         :lost-own-count    (count lost)}))))

(defn workload
  [opts]
  (when-not (:via-proxy opts)
    (throw (ex-info "--workload scoped needs --via-proxy (scoped keys live in the proxy)" {})))
  {:client          (map->Client {})
   :checker         (checker)
   :generator       (gen/mix [(map (fn [v] {:f :push-own, :value v}) (range 0 Long/MAX_VALUE 2))
                              (map (fn [v] {:f :push-other, :value v}) (range 1 Long/MAX_VALUE 2))
                              (repeat {:f :pop-own, :value nil})
                              (map (fn [v] {:f :kv-own, :value v}) (range))
                              (map (fn [v] {:f :kv-other, :value v}) (range))])
   :final-generator (gen/once {:f :read, :value nil})})
