(ns jepsen.queen.workload.ephemeral
  "W20 --workload ephemeral: ephemeral queues (/api/v1/ephemeral/*), in
  memory and outside the raft log, for request/reply. Each partition has one
  owner node; the others forward to it. A SIGTERM hands a node's partitions
  over; a crash loses its share, by design.

  One queue, jep-eph, partitions e0..e7, two consumer groups:
    gl  leased pops, then one ack of everything popped (at least once while
        the owner lives)
    gc  commitOnDelivery pops: at most once
  Pushes carry {v} (unique). Pops always name their partition (a pop without
  one sees only one node's share).

  The checker rejects: a delivered message nobody pushed; a gc message
  delivered twice; within one pop answer, two messages of a partition in the
  wrong order (a push acknowledged before the other was sent comes first),
  unless a hand-over could have come between the two pushes; and, in a test
  whose only faults are graceful restarts (or none), an acknowledged push
  that one of the groups never received. Under kills and partitions losses
  are by design and only counted.

  Order across a hand-over is not promised (writing-applications/
  ephemeral.mdx): the new owner can take a push before the old owner's
  queue reaches it. A pair pushed while a fault was under way, a restart,
  kill, pause or partition and 10 s after it (`handover-windows`), is
  counted as :reordered-at-handover, not judged. Until 2026-11-16 every
  such pair failed the run, in every campaign (eph-restart, -kill, -part,
  -pause)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [util :as util]]
            [jepsen.queen.http :as qh])
  (:import (java.net URLEncoder)))

(def queue "jep-eph")
(def parts 8)
(def groups ["gl" "gc"])

(defn- enc [s] (URLEncoder/encode (str s) "UTF-8"))
(defn pname [p] (str "e" p))

(defn- request
  [client method url body timeout]
  (try (qh/request! (:http client) method url body timeout)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn push-type
  "The completion of a push that did not answer 201. 503 owner_moved: not
  served (the relay refused it); ephemeral_forward_failed, a timeout, any
  other 5xx: unknown. A 4xx is a refusal."
  [status body]
  (let [code (when (map? body) (or (:code body) (:error body)))]
    (cond (and (= 503 status) (= "owner_moved" code)) :fail
          (<= 400 (or status 0) 499)                  :fail
          :else                                       :info)))

(defn- push!
  [client test op]
  (let [p (rand/long parts)
        r (request client :post (str (:base client) "/api/v1/ephemeral/push")
                   {:queue queue, :partition (pname p), :messages [{:payload {:v (:value op)}}]}
                   (:client-timeout-ms test))
        op (assoc op :partition (pname p))]
    (cond
      (:exception r) (assoc op :type (if (= ::qh/refused (:type (:exception r))) :fail :info)
                            :error [:push (:type (:exception r))])
      (= 201 (:status r)) (assoc op :type :ok)
      :else (assoc op :type (push-type (:status r) (:body r)) :error [:push (:status r) (:body r)]))))

(defn- pop!
  [client test op]
  (let [g    (:group op)
        p    (if (:partition op) (:partition op) (pname (rand/long parts)))
        cod? (= "gc" g)
        wait (if (:drain? op) 500 (rand/nth [0 200]))
        url  (str (:base client) "/api/v1/ephemeral/pop?queue=" (enc queue)
                  "&partition=" (enc p) "&group=" (enc g)
                  "&batch=" (inc (rand/long 5))
                  (if (pos? wait) (str "&wait=true&timeout=" wait) "&wait=false")
                  (when cod? "&commitOnDelivery=true"))
        r    (request client :get url nil (+ wait (:client-timeout-ms test)))
        op   (assoc op :partition p)]
    (cond
      (:exception r) (assoc op :type :fail, :msgs [], :error [:pop (:type (:exception r))])
      (= 204 (:status r)) (assoc op :type :ok, :msgs [])
      (not= 200 (:status r)) (assoc op :type :fail, :msgs [], :error [:pop (:status r) (:body r)])
      :else
      (let [msgs (mapv (fn [m] {:id (:id m), :v (some-> m :payload :v), :partition (:partition m)
                                :attempts (:attempts m)})
                       (:messages (:body r)))]
        (if (or cod? (empty? msgs))
          (assoc op :type :ok, :msgs msgs)
          (let [ar (request client :post (str (:base client) "/api/v1/ephemeral/ack")
                            {:queue queue, :group g
                             :acks (mapv (fn [m] {:id (:id m), :status "completed"}) msgs)}
                            (:client-timeout-ms test))]
            (assoc op :type :ok, :msgs msgs
                   :acks (when (= 200 (:status ar)) (:body ar))
                   :ack-error (when-not (= 200 (:status ar)) (or (:exception ar) [(:status ar) (:body ar)])))))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    (when (= node (first (:nodes test)))
      ; Nothing may leave the ring but by consumption: an hour of ttl, room
      ; for every push, and refusals (not silent drops) past the bound.
      (let [r (request this :post (str base "/api/v1/ephemeral/configure")
                       {:queue queue, :options {:leaseSeconds 3, :retryLimit 5, :ttlSeconds 3600
                                                :maxLength 10000000, :policy "reject"}}
                       (:client-timeout-ms test))]
        (info "ephemeral queue configured:" (or (:status r) (:exception r)) (:body r)))
      ; Both groups on every partition's ring before the first push: a group
      ; is made at its first pop and starts at the ring's head, and a message
      ; every KNOWN group has passed is freed (ephemeral.rs, ONE RING, N GROUP
      ; CURSORS). A group that first pops later misses what was freed, by
      ; design: there is no history here.
      (doseq [g groups, p (range parts)]
        (let [r (request this :get (str base "/api/v1/ephemeral/pop?queue=" (enc queue)
                                        "&partition=" (enc (pname p)) "&group=" (enc g)
                                        "&batch=1&wait=false"
                                        (when (= "gc" g) "&commitOnDelivery=true"))
                         nil (:client-timeout-ms test))]
          (when-not (#{200 204} (:status r))
            (throw (ex-info "could not register an ephemeral group" {:group g, :partition p, :answer r})))))))

  (invoke! [this test op]
    (case (:f op)
      :push (push! this test op)
      :pop  (pop! this test op)))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defrecord Drain [targets need empties]
  ; Pops of every group and partition, in turn, until each came back empty
  ; `need` times in a row.
  gen/Generator
  (op [this test ctx]
    (let [left (remove #(<= need (get empties % 0)) targets)]
      (when (seq left)
        (let [[g p] (first left)
              op    (gen/fill-in-op {:f :pop, :value nil, :group g, :partition p, :drain? true} ctx)]
          (if (= :pending op)
            [:pending this]
            [op (Drain. (vec (concat (rest targets) [(first targets)])) need empties)])))))
  (update [this test ctx event]
    (if (and (= :pop (:f event)) (:drain? event) (not= :invoke (:type event)))
      (let [t [(:group event) (:partition event)]]
        (Drain. targets need (if (and (= :ok (:type event)) (empty? (:msgs event)))
                               (update empties t (fnil inc 0))
                               (assoc empties t 0))))
      this)))

(def handover-settle-ns
  "How long after a fault a partition's owner can still move: the ring
  settles, and the old owner's queue reaches the new one."
  (* 10 1000000000))

(def ^:private fault-ends #{:start :resume :stop-partition :heal})

(defn handover-windows
  "`[from to]` times when a partition can change its owner: from each fault
  (a restart, a kill, a pause, a partition) to its end, the next op that
  ends one (a start, a resume, a heal), and `settle-ns` after; a fault with
  no end lasts to the end of the history."
  ([history] (handover-windows history handover-settle-ns))
  ([history settle-ns]
   (let [ns (->> history (filter #(= :nemesis (:process %))) (sort-by :time) vec)]
     (->> ns
          (remove #(fault-ends (:f %)))
          (mapv (fn [s]
                  (let [end (if (= :restart (:f s))
                              (:time s)
                              (some #(when (and (fault-ends (:f %)) (<= (:time s) (:time %)))
                                       (:time %))
                                    ns))]
                    [(:time s) (if end (+ end settle-ns) Long/MAX_VALUE)])))))))

(defn checker
  ([] (checker handover-settle-ns))
  ([settle-ns]
  (reify checker/Checker
    (check [this test history opts]
      (let [hist     (h/client-ops history)
            pushes   (->> hist (h/filter #(and (= :push (:f %)) (not= :invoke (:type %)))) vec)
            pushed   (set (keep #(when (#{:ok :info} (:type %)) (:value %)) pushes))
            ok-push  (->> pushes (filter #(= :ok (:type %))))
            times    (into {} (for [op ok-push]
                                [(:value op) {:inv  (:time (h/invocation hist op))
                                              :comp (:time op)
                                              :partition (:partition op)}]))
            pops     (->> hist (h/filter #(and (= :pop (:f %)) (= :ok (:type %)))) vec)
            by-g     (group-by :group pops)
            ds       (fn [g] (mapcat :msgs (by-g g)))
            invented (->> pops (mapcat :msgs) (remove #(pushed (:v %))) (mapv :v) distinct vec)
            cod-twice (->> (ds "gc") (map :v) frequencies (filter #(< 1 (val %))) (map key) sort vec)
            ; Within one answer: per partition, a push acknowledged before
            ; another was sent comes first.
            windows  (handover-windows history settle-ns)
            inverted (->> pops
                          (mapcat (fn [op]
                                    (for [[p ms] (group-by :partition (:msgs op))
                                          [a b] (partition 2 1 ms)
                                          :let [ta (times (:v a)), tb (times (:v b))]
                                          :when (and ta tb (< (:comp tb) (:inv ta)))]
                                      {:pop (:index op), :partition p, :first (:v a), :then (:v b)
                                       ; b acknowledged, then a sent: a hand-over in between?
                                       ::span [(:comp tb) (:inv ta)]})))
                          vec)
            handover? (fn [{[from to] ::span}]
                        (some (fn [[ws we]] (and (<= from we) (<= ws to))) windows))
            order    (->> inverted (remove handover?) (mapv #(dissoc % ::span)))
            at-handover (->> inverted (filter handover?) (mapv #(dissoc % ::span)))
            graceful? (every? #{:restart} (:faults test))
            missing  (into {} (for [g groups]
                                (let [got (set (map :v (ds g)))]
                                  [g (->> ok-push (map :value) (remove got) sort vec)])))
            lost?    (some (comp seq val) missing)]
        {:valid?          (boolean (and (empty? invented) (empty? cod-twice) (empty? order)
                                        (or (not graceful?) (not lost?))))
         :pushes          (frequencies (map :type pushes))
         :deliveries      (into {} (for [g groups] [g (count (ds g))]))
         :invented        (take 16 invented)
         :cod-delivered-twice (take 16 cod-twice)
         :out-of-order    (take 16 order)
         :reordered-at-handover {:count (count at-handover), :sample (take 4 at-handover)}
         :loss-judged?    graceful?
         :missing         (into {} (for [[g vs] missing] [g {:count (count vs), :sample (take 8 vs)}]))
         :push-errors     (->> pushes (keep :error) (map #(vec (take 2 %))) frequencies)})))))

(defn workload
  [opts]
  {:client          (map->Client {})
   :checker         (checker)
   :generator       (gen/mix [(map (fn [v] {:f :push, :value v}) (range))
                              (fn [] {:f :pop, :value nil, :group (rand/nth groups)})])
   :final-generator (gen/each-thread
                      (->Drain (vec (for [g groups, p (range parts)] [g (pname p)])) 3 {}))})
