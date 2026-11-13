(ns jepsen.queen.workload.tenants
  "W21 --workload tenants: several tenants on one cluster, by the tenancy
  header (QUEEN_TENANCY_HEADER, x-queen-tenant), and with --raft-groups N
  several raft groups, each tenant pinned to group (i mod N) through
  QUEEN_TENANT_GROUPS, so every group has a leader of its own and tenants
  of their own.

  Every tenant has the SAME names: queue jepsen-t (partitions w0..w7, group
  g1) and KV keys c:0..c:3. Clients pick a tenant per op:
    :enqueue  push v (unique over all tenants) in tenant t
    :dequeue  W2's pop and ack in tenant t
    :incr     KV incr c:k by 1 in tenant t
  The end drains every tenant's queue and reads every tenant's counters.

  The checker rejects, per tenant, what W2's checkers reject (nothing acked
  lost, none acked twice, no redelivery after an ack, exclusive leases,
  order), and across tenants: a value delivered in a tenant it was not
  pushed in, and a counter outside [its tenant's acknowledged incrs, those
  plus its unknown ones]."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.queue :as queue]
            [jepsen.queen.workload.routing :as routing])
  (:import (java.net URLEncoder)))

(def queue-name "jepsen-t")
(def counters 4)

(defn tenant-ids
  "The tenants' UUIDs: n of them, fixed, so a test's env can pin them."
  [n]
  (mapv #(format "00000000-0000-4000-8000-%012d" (inc %)) (range n)))

(defn db-env
  "Node env for --tenants n and --raft-groups g."
  [test]
  (let [n (long (:tenants test 0))
        g (long (:raft-groups test 1))]
    (merge
      (when (pos? n)
        {"QUEEN_TENANCY_HEADER"          "true"
         ; The broker refuses to boot with the header and no trusted proxy
         ; declared (the header is validated against nothing): the test's
         ; clients play the proxy.
         "QUEEN_KV_TRUSTED_PROXY"        "1"
         ; A bare broker with tenancy on grants KV and ephemeral queues to no
         ; tenant by default (the proxy provisions grants): open them.
         "QUEEN_KV_REQUIRE_GRANT"        "false"
         "QUEEN_EPHEMERAL_REQUIRE_GRANT" "false"})
      (when (< 1 g)
        (cond-> {"QUEEN_RAFT_GROUPS" (str g)}
          (pos? n) (assoc "QUEEN_TENANT_GROUPS"
                          (->> (tenant-ids n)
                               (map-indexed (fn [i t] (str t "=" (mod i g))))
                               (clojure.string/join ","))))))))

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))

(defn- hdr [t] {"x-queen-tenant" t})

(defn- request
  [client method url body timeout t]
  (try (qh/request! (:http client) method url body timeout (hdr t))
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- enqueue!
  [client test op]
  (let [t (:tenant op)
        p (str "w" (rand/long 8))
        r (request client :post (str (:base client) "/api/v1/push")
                   {:items [{:queue queue-name, :partition p, :payload (:value op)
                             :transactionId (str "w" (:value op))}]}
                   (:client-timeout-ms test) t)]
    (cond
      (:exception r) (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info)
                            :error [:push (:type (:exception r))])
      (= 201 (:status r))
      (let [item (first (:body r))]
        (if (#{"queued" "duplicate"} (:status item))
          (assoc op :type :ok, :partition p, :offset (:offset item))
          (assoc op :type :fail, :error [:item-error item])))
      :else (assoc op :type (log/push-failure test (:status r) (:body r))
                   :error [:push (:status r) (:body r)]))))

(defn- dequeue!
  [client test op]
  (let [t     (:tenant op)
        lease (:w2-lease test 3)
        wait  (if (:drain? op) 1000 (rand/nth [0 200]))
        url   (str (:base client) "/api/v1/pop/queue/" (enc queue-name)
                   "?consumerGroup=g1&batch=" (inc (rand/long 5)) "&leaseSeconds=" lease
                   "&subscriptionMode=all"
                   (if (pos? wait) (str "&wait=true&timeout=" wait) "&wait=false"))
        r     (request client :get url nil (+ wait (:client-timeout-ms test)) t)
        op    (assoc op :lease-s lease)]
    (cond
      (:exception r) (assoc op :type :fail, :value [], :msgs [], :error [:pop (:type (:exception r))])
      (= 204 (:status r)) (assoc op :type :ok, :value [], :msgs [])
      (not= 200 (:status r)) (assoc op :type :fail, :value [], :msgs [], :error [:pop (:status r) (:body r)])
      :else
      (let [msgs (mapv (fn [m] {:v (:data m), :txn (:transactionId m), :pid (:partitionId m)
                                :partition (:partition m), :offset (:offset m)
                                :lease (:leaseId m), :attempt (:deliveryAttempt m)})
                       (:messages (:body r)))]
        (if (empty? msgs)
          (assoc op :type :ok, :value [], :msgs [])
          (let [ar (request client :post (str (:base client) "/api/v1/ack/batch")
                            {:consumerGroup "g1"
                             :acknowledgments (mapv (fn [m] {:transactionId (:txn m), :partitionId (:pid m)
                                                             :leaseId (:lease m), :status "completed"})
                                                    msgs)}
                            (:client-timeout-ms test) t)]
            (cond
              (:exception ar)
              (assoc op :type (if (= ::qh/refused (:type (:exception ar))) :fail :info)
                     :value [], :msgs msgs, :error [:ack (:type (:exception ar))])
              (not= 200 (:status ar))
              (assoc op :type (log/push-failure test (:status ar) (:body ar))
                     :value [], :msgs msgs, :error [:ack (:status ar) (:body ar)])
              :else
              (let [by-idx (into {} (map (juxt :index identity) (:body ar)))
                    msgs   (vec (map-indexed
                                  (fn [i m]
                                    (let [a (by-idx i)]
                                      (assoc m :ack (cond (nil? a) :missing
                                                          (and (:success a) (:noop a)) :noop
                                                          (:success a) :acked
                                                          :else :rejected))))
                                  msgs))
                    acked  (filterv #(= :acked (:ack %)) msgs)]
                (assoc op :type (if (and (empty? acked) (seq msgs)) :fail :ok)
                       :value (mapv :v acked), :msgs msgs)))))))))

(defn- incr!
  [client test op]
  (let [t (:tenant op)
        r (request client :post (str (:base client) "/api/v1/kv")
                   {:operations [{:op "incr", :ns "jepsen", :key (str "c:" (:key op)), :delta 1
                                  :forever true}]}
                   (:client-timeout-ms test) t)]
    (cond
      (:exception r) (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info)
                            :error [:incr (:type (:exception r))])
      (= 200 (:status r)) (assoc op :type :ok)
      (#{400 403 404 413 429} (:status r)) (assoc op :type :fail, :error [:incr (:status r) (:body r)])
      :else (assoc op :type :info, :error [:incr (:status r) (:body r)]))))

(defn- read-counters!
  [client test op]
  (let [out (for [t (tenant-ids (:tenants test))]
              (let [r (request client :post (str (:base client) "/api/v1/kv")
                               {:operations [{:op "getMany", :ns "jepsen"
                                              :keys (mapv #(str "c:" %) (range counters))}]}
                               (:client-timeout-ms test) t)]
                (if (= 200 (:status r))
                  [t (into {} (map (fn [row] [(:key row) (:value row)])
                                   (:rows (first (:results (:body r))))))]
                  (throw (ex-info "counter read failed" {:tenant t, :answer (dissoc r :exception)})))))]
    (try (assoc op :type :ok, :value (into (sorted-map) out))
         (catch clojure.lang.ExceptionInfo e
           (assoc op :type :fail, :error [:read (ex-data e)])))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    ; The queue in every tenant, configured as the db configures its test
    ; queues in the default one.
    (when (= node (first (:nodes test)))
      (doseq [t (tenant-ids (:tenants test))]
        (let [r (request this :post (str base "/api/v1/configure")
                         {:queue queue-name, :options (assoc (deref (requiring-resolve 'jepsen.queen.db/queue-options))
                                                             :dedupWindowSeconds 86400)}
                         15000 t)]
          (info "tenant" t "queue configured:" (:status r))
          (when-not (= 200 (:status r))
            (throw (ex-info "tenant configure failed" {:tenant t, :answer (dissoc r :exception)})))))))

  (invoke! [this test op]
    (case (:f op)
      :enqueue (enqueue! this test op)
      :dequeue (dequeue! this test op)
      :incr    (incr! this test op)
      :read    (read-counters! this test op)))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            ts      (tenant-ids (:tenants test))
            pushed  (->> hist (h/filter #(and (= :enqueue (:f %)) (#{:ok :info} (:type %))))
                         (map (juxt :value :tenant)) (into {}))
            ; Values delivered in a tenant they were not pushed in.
            leaks   (->> hist (h/filter #(= :dequeue (:f %)))
                         (mapcat (fn [op] (for [m (:msgs op)
                                                :let [home (pushed (:v m))]
                                                :when (and home (not= home (:tenant op)))]
                                            {:v (:v m), :pushed-in home, :delivered-in (:tenant op)})))
                         vec)
            unknown (->> hist (h/filter #(= :dequeue (:f %)))
                         (mapcat :msgs) (remove #(contains? pushed (:v %))) (mapv :v) distinct vec)
            per-t   (into (sorted-map)
                          (for [t ts]
                            (let [ops (h/filter #(and (#{:enqueue :dequeue} (:f %)) (= t (:tenant %))) hist)]
                              [t (routing/consumer-check test ops opts)])))
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            final   (:value (last reads))
            incrs   (->> hist (h/filter #(and (= :incr (:f %)) (not= :invoke (:type %)))))
            bounds  (fn [t k type] (count (filter #(and (= t (:tenant %)) (= k (:key %)) (= type (:type %))) incrs)))
            counter (->> (for [t ts, k (range counters)
                               :let [got (or (get-in final [t (str "c:" k)]) 0)
                                     lo  (bounds t k :ok)
                                     hi  (+ lo (bounds t k :info))]
                               :when (not (<= lo got hi))]
                           {:tenant t, :key (str "c:" k), :value got, :acknowledged lo, :at-most hi})
                         vec)]
        {:valid?        (boolean (and final (empty? leaks) (empty? unknown) (empty? counter)
                                      (every? :valid? (vals per-t))))
         :tenants       (count ts)
         :leaks         (take 16 leaks)
         :leak-count    (count leaks)
         :unknown-values (take 16 unknown)
         :counters      counter
         :per-tenant    per-t}))))

(defn workload
  [opts]
  (let [n  (if (pos? (long (:tenants opts 0))) (long (:tenants opts)) 3)
        ts (tenant-ids n)]
    {:client          (map->Client {})
     :test-opts       {:tenants n}
     :checker         (checker)
     :generator       (gen/mix [(map (fn [v] {:f :enqueue, :value v, :tenant (rand/nth ts)}) (range))
                                (fn [] {:f :dequeue, :value nil, :tenant (rand/nth ts)})
                                (fn [] {:f :incr, :value nil, :tenant (rand/nth ts), :key (rand/long counters)})])
     :final-generator (gen/phases
                        (gen/each-thread
                          (routing/->DrainEach (mapv #(hash-map :tenant %) ts) 8 {}))
                        (gen/each-thread (gen/once {:f :read, :value nil})))}))
