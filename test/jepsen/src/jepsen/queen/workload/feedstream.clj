(ns jepsen.queen.workload.feedstream
  "W19 --workload feedstream: the SDKs' feed streams (Stream.fromFeed) as the
  processes under test. Every node runs a runner of each query
  (jepsen.queen.runners), so one is active and the others wait on the
  query's lock; --feed-sdks picks the SDKs, spread over the nodes (node i,
  query q: the (i + q)-th SDK), so a takeover usually crosses SDKs.

  Queries:
    jq-map  jepsen-0 (partitions w0..w7) -> map {v, kind} -> jepsen-out1:
            every input exactly once into the output (`.to()` is exactly
            once: outputs, position and state commit in one transaction,
            guarded by the lock)
    jq-win  jepsen-1 (partitions k0..k3) -> tumbling windows of 5 s per
            partition, count -> jepsen-out2 {key, start, end, count}: each
            window emitted once, closed on the broker's clock

  Clients push to both sources. Faults: the broker nemeses, and --nemesis
  runner: kill -9 of the query's active runner (a standby must take over
  after the lock's 5 s), SIGSTOP of the active runner for 12 s then SIGCONT
  (a zombie that wakes up after its lock moved on must commit nothing), and
  restarts of killed runners. At the end the clients stop, wait 25 s (every
  window closed, every runner caught up), and read the whole feed (every
  queue, through the leader's relay) and the queries' position records.

  The checker rejects: an acknowledged push missing from its source; for
  jq-map, an input with no output, with two, an output nothing input, or
  one whose content is not its input's; for jq-win, a window emitted twice,
  a window whose count is not the number of its partition's inputs stamped
  inside it, a window with inputs that was never emitted, or an output
  outside every window; an output whose transaction id lacks its query's
  prefix; two final reads that disagree."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as n]
                    [random :as rand]
                    [store :as store]
                    [util :as util]]
            [jepsen.queen [http :as qh]
                          [runners :as runners]]
            [jepsen.queen.workload.log :as log]
            [jepsen.queen.workload.routing :as routing])
  (:import (java.time Instant)))

(def window-ms 5000)
(def lock-ttl "5s")

(def queries
  [{:query "jq-map", :source "jepsen-0", :out "jepsen-out1"
    :partitions (mapv #(str "w" %) (range 8))
    :case {:name   "jq-map"
           :source {:queues ["jepsen-0"], :from "earliest"}
           :chain  [{:op "map", :fn {:v {:$get "0.data.v"}, :kind {:$get "0.data.kind"}}}
                    {:op "to", :target "jepsen-out1"}]
           :run    {:queryId "jq-map", :lockTtl lock-ttl, :onPoison "stop"}}}
   {:query "jq-win", :source "jepsen-1", :out "jepsen-out2"
    :partitions (mapv #(str "k" %) (range 4))
    :case {:name   "jq-win"
           :source {:queues ["jepsen-1"], :from "earliest"}
           :chain  [{:op "windowTumbling", :opts {:seconds (quot window-ms 1000)}}
                    {:op "aggregate", :fields {:count 1}}
                    {:op "map", :fn {:key   {:$get "1.key"}
                                     :start {:$get "1.windowKey"}
                                     :end   {:$get "1.windowEnd"}
                                     :count {:$get "0.count"}}}
                    {:op "to", :target "jepsen-out2"}]
           :run    {:queryId "jq-win", :lockTtl lock-ttl, :onPoison "stop"}}}])

(defn sdk-of
  "The SDK node i runs query number q with."
  [test i q]
  (let [sdks (vec (or (seq (:feed-sdks test)) [:js]))]
    (nth sdks (mod (+ i q) (count sdks)))))

(defn instances
  "[node query sdk] of every runner of the test."
  [test]
  (for [[i node] (map-indexed vector (:nodes test))
        [q {:keys [query]}] (map-indexed vector queries)]
    [node query (sdk-of test i q)]))

(defn ts-ms
  [s]
  (cond (integer? s) s
        (string? s) (try (.toEpochMilli (Instant/parse s)) (catch Exception _ nil))))

;; ---------------------------------------------------------------------------
;; Client

(def relay {"x-queen-feed-relay" "1"})

(defn- read-feed!
  "The whole feed of the jepsen-* queues from the start, through this node
  (relayed by a leader): [{:queue :partition :txn :data :ts-ms :position}]."
  [client test]
  (loop [from "earliest", out [], pages 0]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/feed")
                         {:from from, :queues ["jepsen-*"], :maxRecords 1000, :maxWaitMs 0}
                         (:client-timeout-ms test) relay)
          b (:body r)]
      (when-not (and (= 200 (:status r)) (map? b))
        (throw (ex-info "feed read failed" {:status (:status r), :body b})))
      (let [es  (mapv (fn [e] {:queue     (:queue e)
                               :partition (:partition e)
                               :txn       (:transactionId e)
                               :data      (:payload e)
                               :ts-ms     (ts-ms (:ts e))
                               :position  (:position e)})
                      (:entries b))
            out (into out es)]
        (if (or (and (true? (:caughtUp b)) (empty? es)) (< 2000 pages))
          out
          (recur (:next b) out (inc pages)))))))

(defn- read-positions!
  "The queries' position records (KV namespace queen-feed)."
  [client test]
  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/kv")
                       {:operations [{:op "getMany", :ns "queen-feed"
                                      :keys (mapv :query queries)}]}
                       (:client-timeout-ms test))]
    (when (= 200 (:status r))
      (->> (:rows (first (:results (:body r))))
           (map (fn [row] [(:key row) (select-keys (:value row) [:epoch :position :stopped])]))
           (into (sorted-map))))))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test]
    ; This node's runners: the bundle, the case files, one runner per query.
    (let [i (.indexOf ^java.util.List (vec (:nodes test)) node)]
      (c/on-nodes test [node]
                  (fn [test node]
                    (runners/install! test)
                    (runners/kill-all!)
                    (runners/wipe!)
                    (doseq [{:keys [query case]} queries]
                      (runners/write-case! query case))
                    (doseq [[q {:keys [query]}] (map-indexed vector queries)]
                      (info node "starts" query "with" (sdk-of test i q)
                            (runners/start! test node query (sdk-of test i q))))))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue
        (let [{:keys [source partitions]} (nth queries (mod (:value op) 2))
              p   (rand/nth partitions)
              res (routing/push! this test source p (:value op))]
          (merge (assoc op :queue source, :partition p) res))

        :read
        (assoc op :type :ok, :node node
               :value (read-feed! this test)
               :positions (read-positions! this test)))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg status]} (ex-data e)]
          (cond
            (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type (if (or (= ::qh/refused type) (= :read (:f op))) :fail :info)
                   :error [(keyword (name type)) msg])
            status (assoc op :type :fail, :error [:read status])
            :else (throw e))))))

  (teardown! [this test]
    (c/on-nodes test [node] (fn [test node] (runners/kill-all!))))

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; The runner nemesis

(defn- active-node
  "The node whose runner of `query` says it is active, or nil."
  [test query]
  (->> (c/on-nodes test (fn [test node]
                          (let [[_ _ sdk] (first (filter (fn [[n q _]] (and (= n node) (= q query)))
                                                         (instances test)))]
                            (runners/role (runners/id query sdk)))))
       (filter (fn [[_ role]] (= "active" role)))
       ffirst))

(defn- iid-on
  [test node query]
  (let [[_ _ sdk] (first (filter (fn [[n q _]] (and (= n node) (= q query))) (instances test)))]
    (runners/id query sdk)))

(defn runner-nemesis
  "  :runner-kill     kill -9 the active runner of (:value op), a query;
     :runner-pause    SIGSTOP it (the generator resumes it 12 s later);
     :runner-resume   SIGCONT every runner;
     :runner-restart  start every runner that is not running.
  (Names of their own: :resume and :restart belong to the pause and
  graceful-restart faults.)"
  []
  (reify
    n/Reflection
    (fs [_] #{:runner-kill :runner-pause :runner-resume :runner-restart})

    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        (:runner-kill :runner-pause)
        (let [q    (:value op)
              node (active-node test q)]
          (if-not node
            (assoc op :value {:query q, :active nil})
            (do (c/on-nodes test [node]
                            (fn [test node]
                              (runners/signal! (iid-on test node q)
                                               (if (= :runner-kill (:f op)) :KILL :STOP))))
                (assoc op :value {:query q, :active node}))))

        :runner-resume
        (assoc op :value (c/on-nodes test (fn [test node]
                                            (doseq [[n q _] (instances test) :when (= n node)]
                                              (runners/signal! (iid-on test node q) :CONT))
                                            :runner-resumed)))

        :runner-restart
        (assoc op :value (c/on-nodes test (fn [test node]
                                            (vec (for [[i n] (map-indexed vector (:nodes test))
                                                       :when (= n node)
                                                       [qi {:keys [query]}] (map-indexed vector queries)]
                                                   (runners/start! test node query (sdk-of test i qi)))))))))

    (teardown! [this test])))

(defn runner-package
  [{:keys [faults interval]}]
  (when (contains? faults :runner)
    {:nemesis (runner-nemesis)
     :generator
     (->> (gen/mix
            [(fn [] [{:type :info, :f :runner-kill, :value (:query (rand/nth queries))}
                     (gen/sleep 8)
                     {:type :info, :f :runner-restart, :value nil}])
             (fn [] [{:type :info, :f :runner-pause, :value (:query (rand/nth queries))}
                     (gen/sleep 12)
                     {:type :info, :f :runner-resume, :value nil}])])
          (gen/stagger interval))
     :final-generator [{:type :info, :f :runner-resume, :value nil}
                       {:type :info, :f :runner-restart, :value nil}]
     :perf #{{:name "runner", :fs #{:runner-kill :runner-pause :runner-resume :runner-restart}
              :start #{} :stop #{} :color "#A0E9D0"}}}))

;; ---------------------------------------------------------------------------
;; Checker

(def kinds ["a" "b" "c"])

(defn check-map
  "jq-map: every input exactly once into the output, as {v, kind}."
  [inputs outputs prefix]
  (let [in-vs   (set (keep (comp :v :data) inputs))
        counts  (frequencies (keep (comp :v :data) outputs))
        missing (->> in-vs (remove counts) sort vec)
        twice   (->> counts (filter #(< 1 (val %))) (map key) sort vec)
        extra   (->> (keys counts) (remove in-vs) sort vec)
        wrong   (->> outputs
                     (remove #(= (:data %) {:v (:v (:data %)), :kind (kinds (mod (or (:v (:data %)) 0) 3))}))
                     (mapv #(select-keys % [:partition :txn :data])))
        unprefixed (->> outputs (remove #(str/starts-with? (str (:txn %)) prefix))
                        (mapv #(select-keys % [:txn])))]
    {:valid?        (and (empty? missing) (empty? twice) (empty? extra) (empty? wrong)
                         (empty? unprefixed))
     :inputs        (count in-vs)
     :outputs       (count outputs)
     :missing       (take 16 missing)
     :missing-count (count missing)
     :twice         (take 16 twice)
     :twice-count   (count twice)
     :extra         (take 16 extra)
     :wrong         (take 16 wrong)
     :unprefixed    (take 4 unprefixed)}))

(defn window-of [ts] (* window-ms (quot ts window-ms)))

(defn check-window
  "jq-win: per partition and 5 s window, one output whose count is the
  number of inputs stamped inside it."
  [inputs outputs prefix]
  (let [want    (->> inputs
                     (filter :ts-ms)
                     (map (fn [e] [(:partition e) (window-of (:ts-ms e))]))
                     frequencies)
        got     (->> outputs
                     (group-by (fn [o] [(:key (:data o)) (ts-ms (:start (:data o)))])))
        twice   (->> got (filter #(< 1 (count (val %)))) (mapv key))
        wrong   (->> got
                     (keep (fn [[k os]]
                             (let [n (:count (:data (first os)))]
                               (when (and (contains? want k) (not= n (want k)))
                                 {:window k, :count n, :inputs (want k)}))))
                     vec)
        missing (->> want (map key) (remove got) sort vec)
        phantom (->> got (map key) (remove want) sort vec)
        unprefixed (->> outputs (remove #(str/starts-with? (str (:txn %)) prefix))
                        (mapv #(select-keys % [:txn])))]
    {:valid?        (and (empty? twice) (empty? wrong) (empty? missing) (empty? phantom)
                         (empty? unprefixed))
     :windows       (count want)
     :outputs       (count outputs)
     :twice         (take 16 twice)
     :wrong-count   (take 16 wrong)
     :missing       (take 16 missing)
     :missing-count (count missing)
     :phantom       (take 16 phantom)
     :unprefixed    (take 4 unprefixed)}))

(defn query-prefix
  "fd-<sha256(\"queen-feed\\n\" + queryId)[:8]>- (FeedRunner.js)."
  [query]
  (let [md (java.security.MessageDigest/getInstance "SHA-256")
        hx (apply str (map #(format "%02x" %) (.digest md (.getBytes (str "queen-feed\n" query) "UTF-8"))))]
    (str "fd-" (subs hx 0 8) "-")))

(defn runner-events
  "Every runner's JSON lines from the store, counted by event (and role)."
  [test]
  (->> (for [node (:nodes test)
             :let [d (store/path test node "runners")]
             :when (.isDirectory d)
             f (.listFiles d)
             :when (str/ends-with? (.getName f) ".out")
             line (str/split-lines (slurp f))
             :let [e (try (json/read-str line :key-fn keyword) (catch Exception _ nil))]
             :when e]
         [(.getName f) (:event e) (:role e)])
       frequencies
       (into (sorted-map))))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [hist    (h/client-ops history)
            reads   (->> hist (h/filter #(and (= :read (:f %)) (= :ok (:type %)))) vec)
            read    (last reads)
            key-of  (fn [r] (set (map (juxt :queue :partition :txn) (:value r))))
            differ  (->> reads (remove #(= (key-of %) (key-of read))) (mapv #(select-keys % [:node :index])))
            by-q    (group-by :queue (:value read))
            src     (set (map :txn (concat (by-q "jepsen-0") (by-q "jepsen-1"))))
            lost    (->> hist (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                         (remove #(src (routing/txn-id (:value %))))
                         (mapv #(select-keys % [:value :queue :partition])))
            qm      (check-map (by-q "jepsen-0") (by-q "jepsen-out1") (query-prefix "jq-map"))
            qw      (check-window (by-q "jepsen-1") (by-q "jepsen-out2") (query-prefix "jq-win"))]
        {:valid?          (boolean (and read (empty? differ) (empty? lost) (:valid? qm) (:valid? qw)))
         :final-reads     (count reads)
         :nodes-differ    differ
         :lost-at-source  (take 16 lost)
         :lost-at-source-count (count lost)
         :jq-map          qm
         :jq-win          qw
         :positions       (:positions read)
         :runner-events   (try (runner-events test) (catch Exception e (str e)))}))))

;; ---------------------------------------------------------------------------
;; Workload

(defn workload
  [opts]
  {:client          (map->Client {})
   :checker         (checker)
   :nemesis-package (runner-package {:faults   (set (:nemesis opts))
                                     :interval (:nemesis-interval opts 10)})
   :generator       (map (fn [v] {:f :enqueue, :value v}) (range))
   :final-generator (gen/phases (gen/sleep 25)
                                (gen/each-thread (gen/once {:f :read, :value nil})))})
