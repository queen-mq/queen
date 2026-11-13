(ns jepsen.queen.workload.kvttl
  "W25 --workload kvttl: KV keys with an expiry (ttlSeconds), which every
  other KV test leaves out (they write `forever`), and claims that expire:
  the `once(ns, key, {ttl})` gate and putIfAbsent with a ttl.

  Ops:
    :write  PUT jepsen/t:<n> = n with ttlSeconds 2, 3 or 4 (each key once)
    :read   GET of a key this client wrote earlier
    :claim  putIfAbsent jepsen/claim:<k> (k in 0..3), ttlSeconds 2: the winner
            holds the key until it expires; then a new claim can win

  The checker rejects, judged only in tests without clock faults (the
  broker counts a ttl from when it applies the write, which is after the
  client sent it; 1 s of margin on either side):
    - a read sent more than ttl + 1 s after its write was acknowledged that
      still finds the key (an expired key served);
    - a read that completed earlier than ttl - 1 s after its write was sent
      (and began after the write was acknowledged) and finds the key absent
      or holding another value (expired early);
    - two claims of one key that both won, the second acknowledged earlier
      than 2 s - 1 s after the first was sent (two holders at once)."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]])
  (:import (java.net URLEncoder)))

(def s 1000000000)
(def margin s)
(def claim-ttl 2)

(defn- enc [x] (URLEncoder/encode (str x) "UTF-8"))

(defn- call
  [client method url body timeout]
  (try (qh/request! (:http client) method url body timeout)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- write-type [r]
  (cond (= ::qh/refused (:type (:exception r))) :fail
        (:exception r) :info
        (= 200 (:status r)) :ok
        :else (kv/write-failure [(:status r) (:body r)])))

(defrecord Client [node base http written]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :write
      (let [n   (:value op)
            ttl (+ 2 (rand/long 3))
            k   (str "t:" n)
            r   (call this :put (str base "/api/v1/kv/" kv/ns-name* "/" (enc k))
                      {:value n, :ttlSeconds ttl} (:client-timeout-ms test))
            t   (write-type r)]
        (when (#{:ok :info} t) (swap! written conj [k n ttl]))
        (assoc op :type t, :key k, :ttl ttl
               :error (when-not (= :ok t) [(:status r) (or (:body r) (:type (:exception r)))])))

      :read
      (if-let [[k n ttl] (when (seq @written) (rand/nth (vec @written)))]
        (let [r (call this :get (str base "/api/v1/kv/" kv/ns-name* "/" (enc k)) nil (:client-timeout-ms test))]
          (if (= 200 (:status r))
            (assoc op :type :ok, :key k, :value {:found (true? (get-in r [:body :found]))
                                                 :value (get-in r [:body :value])})
            (assoc op :type :fail, :key k, :error [:read (or (:status r) (:type (:exception r)))])))
        (assoc op :type :fail, :error :nothing-written))

      :claim
      (let [k (str "claim:" (:key op))
            r (call this :post (str base "/api/v1/kv")
                    {:operations [{:op "putIfAbsent", :ns kv/ns-name*, :key k, :value (:value op)
                                   :ttlSeconds claim-ttl}]}
                    (:client-timeout-ms test))
            res (when (= 200 (:status r)) (first (:results (:body r))))]
        (cond
          res (assoc op :type :ok, :claim-key k, :won (true? (:applied res)), :holder (:value res))
          :else (assoc op :type (write-type r), :claim-key k
                       :error [(:status r) (or (:body r) (:type (:exception r)))])))))

  (teardown! [this test])
  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [judged? (not (some #{:clock} (:faults test)))
            hist    (h/client-ops history)
            writes  (->> hist (h/filter #(and (= :write (:f %)) (= :ok (:type %))))
                         (map (fn [op] [(:key op) {:n (:value op), :ttl (:ttl op)
                                                   :inv (:time (h/invocation hist op)) :comp (:time op)}]))
                         (into {}))
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            stale   (->> reads
                         (keep (fn [op]
                                 (when-let [w (writes (:key op))]
                                   (let [inv (:time (h/invocation hist op))]
                                     (when (and (:found (:value op))
                                                (< (+ (:comp w) (* (:ttl w) s) margin) inv))
                                       {:key (:key op), :ttl (:ttl w)
                                        :read-sent-ms-after-write (quot (- inv (:comp w)) 1000000)})))))
                         vec)
            early   (->> reads
                         (keep (fn [op]
                                 (when-let [w (writes (:key op))]
                                   (let [inv (:time (h/invocation hist op))]
                                     (when (and (< (:comp w) inv)
                                                (< (:time op) (- (+ (:inv w) (* (:ttl w) s)) margin))
                                                (not= {:found true, :value (:n w)} (:value op)))
                                       {:key (:key op), :ttl (:ttl w), :read (:value op)
                                        :read-done-ms-after-write (quot (- (:time op) (:inv w)) 1000000)})))))
                         vec)
            wins    (->> hist (h/filter #(and (= :claim (:f %)) (= :ok (:type %)) (:won %)))
                         (map (fn [op] {:key (:claim-key op), :inv (:time (h/invocation hist op))
                                        :comp (:time op), :index (:index op)}))
                         (group-by :key))
            double  (->> wins
                         (mapcat (fn [[k ws]]
                                   (let [ws (vec (sort-by :comp ws))]
                                     (for [[a b] (partition 2 1 ws)
                                           :when (< (:comp b) (- (+ (:inv a) (* claim-ttl s)) margin))]
                                       {:key k, :first (:index a), :second (:index b)
                                        :second-won-ms-after-first-sent (quot (- (:comp b) (:inv a)) 1000000)}))))
                         vec)]
        {:valid?        (or (not judged?) (and (empty? stale) (empty? early) (empty? double)))
         :judged?       judged?
         :reads         (count reads)
         :found         (count (filter (comp :found :value) reads))
         :claims-won    (reduce + (map count (vals wins)))
         :expired-served (take 16 stale)
         :expired-early (take 16 early)
         :two-holders   (take 16 double)}))))

(defn workload
  [opts]
  {:client          (map->Client {:written (atom [])})
   :checker         (checker)
   :db-env          kv/db-env
   :generator       (gen/mix [(map (fn [n] {:f :write, :value n}) (range))
                              (repeat {:f :read, :value nil})
                              (repeat {:f :read, :value nil})
                              (map (fn [n] {:f :claim, :value n, :key (rand/long 4)}) (range))])
   :final-generator nil})
