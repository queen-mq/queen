(ns jepsen.queen.workload.duppush
  "W31 --workload duppush: copies of ONE push sent together, the duplicates
  push dedup must catch while the first is still in flight.

  A push's dedup key is its transactionId (xxh3_128, per partition). The
  planner finds a duplicate in two places: the committed dedup rows, behind a
  dedup front (a bloom filter) that may answer \"new\" and skip them, and the
  appends still in flight, which it scans only when the front does not
  answer \"new\" (the fix1 snapshot, 2026-11-13: no row per message in flight
  any more). So the front must hold every hash in flight, from plan time on,
  and a seed of it (after a leader change, or for a partition just created)
  must read the appends in flight. A miss there stores a second message.

    :dup {g p t copies}  2 to --dup-copies copies of one push of
                         transactionId t (\"d<g>\") to partition p, sent
                         together, each on its own connection and to a node
                         of its own (a follower forwards it). Copy c carries
                         payload {t, c, g}, so the stored record says which
                         copy won. Most copies leave within 3 ms of the
                         first, some within 30 ms, a tenth 50 to 300 ms after
                         it (the committed path). p is a fixed partition
                         f0..f7, or (--dup-fresh of the time) n<g>, which the
                         first copy to land creates. A copy is one of
                         (--dup-kinds):
                           push         one item;
                           multi        two items: the copy and a filler
                                        x<g>-<c> to partition fill;
                           multi-twice  the copy twice in one request, the
                                        second as copy c+100;
                           txn          ONE /api/v1/transaction pushing the
                                        copy and a filler: it commits whole,
                                        or rolls back (duplicate) whole.
    final phase          every partition used, from offset 0, on every node.

  The checker, per group (one transactionId in one partition):
    - at most one record (stored-twice), in the group's own partition;
    - at most one copy that won: answered queued, or a transaction that
      committed (two-winners);
    - every queued and duplicate answer names ONE offset (offset-split), the
      offset of the record (ack-offset-mismatch);
    - a group with a queued or duplicate answer, or a committed transaction,
      has its record (lost);
    - the record is the winner's copy (wrong-copy), and a copy of the group
      (phantom), and a group whose every copy was refused has none
      (refused-but-stored);
  and per filler: at most one record; present when its push was answered
  queued or duplicate or its transaction committed; absent when its
  transaction rolled back. The nodes' final reads must agree.

  Reported: how many groups had a copy answered while another was in flight
  (the window this workload is for), and the answer patterns. Run it at
  QUEEN_PLANNER_THREADS 1 and 8, and with QUEEN_RAFT_DEDUP_FRONT=0 (no
  front: every push scans) as the control."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]
            [jepsen.queen.workload.log :as log]))

(def fixed-partitions 8)
(def filler-partition "fill")
(def kinds-all [:push :multi :multi-twice :txn])
(def kind-weights
  "How often each kind is drawn, among those --dup-kinds allows."
  {:push 5, :multi 2, :multi-twice 1, :txn 2})

(defn- queue [test] (first (:queue-names test)))
(defn txn-id [g] (str "d" g))
(defn filler-id [g c] (str "x" g "-" c))

;; ---------------------------------------------------------------------------
;; Client

(defn- item
  [test p t g c]
  {:queue (queue test), :partition p, :payload {:t t, :c c, :g g}, :transactionId t})

(defn- filler
  [test g c]
  {:queue (queue test), :partition filler-partition, :payload {:x (filler-id g c)}
   :transactionId (filler-id g c)})

(defn- push-answers
  "A /api/v1/push answer, item by item: `tags` names each item (a copy index,
  or :filler)."
  [test r tags]
  (cond
    (= 201 (:status r))
    (let [items (when (sequential? (:body r)) (vec (:body r)))]
      (if (not= (count items) (count tags))
        {:type :info, :error [:items (count items) (:body r)]}
        (let [answers (mapv (fn [tag it] {:tag tag, :status (:status it), :offset (:offset it)
                                         :error (:error it)})
                            tags items)]
          {:type    (if (every? #(#{"queued" "duplicate"} (:status %)) answers) :ok :fail)
           :answers answers})))

    :else
    {:type (log/push-failure test (:status r) (:body r)), :error [(:status r) (:body r)]}))

(defn- txn-answer
  [test r]
  (let [b (:body r)]
    (cond
      (and (= 200 (:status r)) (true? (:success b)))
      {:type :ok, :committed? true}

      ; Rolled back: another copy (or nothing else) holds the transactionId.
      (= 200 (:status r))
      {:type :fail, :committed? false
       :reason (or (:reason b) (:error b))}

      :else
      {:type (log/push-failure test (:status r) b), :error [(:status r) b]})))

(defn- send-copy!
  "One copy, after its delay, on its own connection to its own node. Its
  record: what it sent, what it was answered, when (ns since the group
  began)."
  [client test {:keys [g p t]} {:keys [c kind node delay-ms] :as copy} t0]
  (when (pos? (long delay-ms))
    (Thread/sleep (long delay-ms)))
  (let [pool (:pool client)
        http (nth pool (mod (long c) (count pool)))
        base (qh/base-url node)
        tmo  (:client-timeout-ms test)
        sent (- (System/nanoTime) t0)
        res  (try
               (case kind
                 :push
                 (push-answers test (qh/request! http :post (str base "/api/v1/push")
                                                 {:items [(item test p t g c)]} tmo)
                               [c])
                 :multi
                 (push-answers test (qh/request! http :post (str base "/api/v1/push")
                                                 {:items [(item test p t g c) (filler test g c)]} tmo)
                               [c :filler])
                 :multi-twice
                 (push-answers test (qh/request! http :post (str base "/api/v1/push")
                                                 {:items [(item test p t g c)
                                                          (item test p t g (+ 100 (long c)))]} tmo)
                               [c (+ 100 (long c))])
                 :txn
                 (txn-answer test (qh/request! http :post (str base "/api/v1/transaction")
                                               {:operations [{:type "push", :items [(item test p t g c)]}
                                                             {:type "push", :items [(filler test g c)]}]}
                                               tmo)))
               (catch clojure.lang.ExceptionInfo e
                 (let [{:keys [type msg]} (ex-data e)]
                   (if (#{::qh/refused ::qh/timeout ::qh/io} type)
                     {:type (kv/exception-type type), :error [(keyword (name type)) msg]}
                     (throw e)))))]
    (merge copy res {:sent-ns sent, :answered-ns (- (System/nanoTime) t0)})))

(defn- dup!
  [client test op]
  (let [v      (:value op)
        t0     (System/nanoTime)
        copies (mapv (fn [copy] (future (send-copy! client test v copy t0))) (:copies v))
        wait   (+ 5000 (:client-timeout-ms test) (reduce max 0 (map :delay-ms (:copies v))))
        done   (mapv (fn [f copy]
                       (deref f wait (assoc copy :type :info, :error [:no-answer-in-ms wait])))
                     copies (:copies v))]
    ; The group is one op: its copies say what happened, each on its own.
    (assoc op :type :ok, :copies done)))

(defn- fetch!
  "Every record of `partitions` from `positions` ({p offset}), one request
  per 100 partitions: {p [[offset payload] ...]} for those that answered
  records."
  [client test partitions positions]
  (into (sorted-map)
        (mapcat (fn [ps]
                  (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                                       {:entries (mapv (fn [p] {:queue (queue test), :partition p
                                                                :offset (get positions p 0)})
                                                       ps)}
                                       (:client-timeout-ms test))]
                    (if-not (= 200 (:status r))
                      (throw (ex-info "fetch failed" {:status (:status r), :body (:body r)}))
                      (keep (fn [e]
                              (when (seq (:records e))
                                [(:partition e) (mapv (juxt :offset :payload) (:records e))]))
                            (:entries (:body r)))))))
        (partition-all 100 partitions)))

(defn- final-read!
  "Every partition used, from offset 0: {p [[offset payload] ...]}, and the
  partitions whose offsets were not 0, 1, 2, ... as answered."
  [client test op]
  (let [parts (vec (concat (map #(str "f" %) (range fixed-partitions))
                           [filler-partition]
                           (sort @(:fresh client))))]
    (loop [positions {}, logs (sorted-map), gaps (sorted-map)]
      (let [recs (fetch! client test parts positions)]
        (if (empty? recs)
          (assoc op :type :ok, :value logs, :gaps gaps, :partitions (count parts))
          (recur (reduce (fn [pos [p rs]] (assoc pos p (inc (long (first (peek rs)))))) positions recs)
                 (reduce (fn [logs [p rs]] (update logs p (fnil into []) rs)) logs recs)
                 (reduce (fn [gaps [p rs]]
                           (let [from (get positions p 0)]
                             (if (= (map first rs) (range from (+ from (count rs))))
                               gaps
                               (assoc gaps p {:requested from, :offsets (mapv first rs)}))))
                         gaps recs)))))))

(defrecord Client [node base http pool fresh]
  client/Client
  (open! [this test node]
    ; One connection pool per copy slot: the copies of a group never share
    ; a connection.
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)
           :pool (vec (repeatedly (long (:dup-copies test 4)) qh/client))))

  (setup! [this test])

  (invoke! [this test op]
    (let [op (assoc op :node node)]
      (try
        (case (:f op)
          :dup        (dup! this test op)
          :final-read (final-read! this test op))
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type msg]} (ex-data e)]
            (cond
              (= "fetch failed" (.getMessage e))
              (assoc op :type :fail, :error [:fetch (ex-data e)])

              (#{::qh/refused ::qh/timeout ::qh/io} type)
              (assoc op :type :fail, :error [(keyword (name type)) msg])

              :else (throw e)))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Generator

(defn- copy-delay
  "Milliseconds after the group's start: most copies within 3 ms (in flight
  together), some within 30 ms, a tenth 50 to 300 ms (the first committed)."
  [c]
  (if (zero? (long c))
    0
    (let [x (rand/double)]
      (cond (< x 0.7) (rand/long 4)
            (< x 0.9) (+ 3 (rand/long 28))
            :else     (+ 50 (rand/long 251))))))

(defn generator
  "Groups of copies, numbered from 0; `fresh` collects the new partitions."
  [opts fresh]
  (let [g      (atom -1)
        kinds  (vec (mapcat (fn [k] (repeat (kind-weights k) k))
                            (or (seq (:dup-kinds opts)) kinds-all)))
        nodes  (vec (:nodes opts))
        most   (max 2 (long (:dup-copies opts 4)))
        fresh% (double (:dup-fresh opts 0.5))]
    (fn []
      (let [gid    (swap! g inc)
            new?   (< (rand/double) fresh%)
            p      (if new? (str "n" gid) (str "f" (rand/long fixed-partitions)))
            n      (+ 2 (rand/long (dec most)))
            copies (vec (for [c (range n)]
                          {:c c, :kind (rand/nth kinds), :node (rand/nth nodes)
                           :delay-ms (copy-delay c)}))]
        (when new? (swap! fresh conj p))
        {:f :dup, :value {:g gid, :p p, :t (txn-id gid), :copies copies}}))))

;; ---------------------------------------------------------------------------
;; Checker

(defn- merged-reads
  "The nodes' final reads as one: per partition the longest log, and every
  node whose log is not a prefix of it."
  [finals]
  (let [logs  (apply merge-with (fn [a b] (if (< (count a) (count b)) b a)) (map :value finals))
        split (vec (for [f      finals
                         [p rs] (:value f)
                         :let   [full (get logs p)]
                         :when  (not= rs (subvec full 0 (count rs)))]
                     {:node (:node f), :partition p}))]
    {:logs logs, :split split, :gaps (into {} (keep (fn [f] (when (seq (:gaps f)) [(:node f) (:gaps f)]))) finals)}))

(defn- wins
  "The copies of a group that won: a push item answered queued, or a
  committed transaction. [{:c :kind :offset}]."
  [copies]
  (vec (concat
         (for [cp copies
               a  (:answers cp)
               :when (and (integer? (:tag a)) (= "queued" (:status a)))]
           {:c (:tag a), :kind (:kind cp), :offset (:offset a)})
         (for [cp copies :when (true? (:committed? cp))]
           {:c (:c cp), :kind :txn}))))

(defn- copy-outcome
  "What one copy says of its group's transactionId (not of a filler):
  :stored (an item answered queued or duplicate, a transaction committed),
  :refused (every item of the group answered error, the transaction rolled
  back, or the request was refused before it was sent), or :unknown."
  [cp]
  (let [mine (filter #(integer? (:tag %)) (:answers cp))]
    (cond
      (some #(#{"queued" "duplicate"} (:status %)) mine) :stored
      (true? (:committed? cp))                           :stored
      (and (seq mine) (every? #(= "error" (:status %)) mine)) :refused
      (false? (:committed? cp))                          :refused
      (and (empty? mine) (= :fail (:type cp)))           :refused
      :else                                              :unknown)))

(defn- group-check
  "What is wrong with one group, given its stored records [{:p :o :c}]."
  [{:keys [g p t copies]} recs]
  (let [answers  (for [cp copies, a (:answers cp) :when (integer? (:tag a))] a)
        acked    (filter #(#{"queued" "duplicate"} (:status %)) answers)
        offsets  (set (keep :offset acked))
        won      (wins copies)
        known-cs (set (concat (map :c copies)
                              (map #(+ 100 (long (:c %))) (filter #(= :multi-twice (:kind %)) copies))))
        rec      (first recs)
        refused? (every? #(= :refused (copy-outcome %)) copies)
        committed-or-acked? (some #(= :stored (copy-outcome %)) copies)]
    (cond-> []
      (< 1 (count recs))
      (conj {:why :stored-twice, :records recs})

      (some #(not= p (:p %)) recs)
      (conj {:why :other-partition, :records recs})

      (< 1 (count won))
      (conj {:why :two-winners, :won won})

      (< 1 (count offsets))
      (conj {:why :offset-split, :offsets (vec (sort offsets))})

      (and rec (seq offsets) (not= #{(:o rec)} offsets))
      (conj {:why :ack-offset-mismatch, :acked-offsets (vec (sort offsets)), :record rec})

      (and committed-or-acked? (empty? recs))
      (conj {:why :lost, :acked (vec acked)})

      (and rec (= 1 (count won)) (not= (:c (first won)) (:c rec)))
      (conj {:why :wrong-copy, :won (first won), :record rec})

      (some #(not (known-cs (:c %))) recs)
      (conj {:why :phantom, :records recs})

      (and refused? (seq recs))
      (conj {:why :refused-but-stored, :records recs}))))

(defn- fillers-check
  "What is wrong with the fillers, given their records {id [offset ...]}."
  [groups by-id]
  (vec (for [{:keys [g copies]} groups
             cp copies
             :when (#{:multi :txn} (:kind cp))
             :let  [id   (filler-id g (:c cp))
                    n    (count (get by-id id))
                    fa   (some #(when (= :filler (:tag %)) %) (:answers cp))
                    ok?  (or (#{"queued" "duplicate"} (:status fa)) (true? (:committed? cp)))
                    gone? (and (= :txn (:kind cp)) (false? (:committed? cp)))
                    why  (cond (< 1 n)             :filler-stored-twice
                               (and ok? (zero? n)) :filler-lost
                               (and gone? (pos? n)) :rolled-back-filler-stored)]
             :when why]
         {:why why, :filler id, :records n, :copy (select-keys cp [:c :kind :type :committed? :reason])})))

(defn- in-flight-together?
  "Was a copy of the group answered while another was still on its way (its
  request sent before that answer and answered after it)?"
  [copies]
  (let [cs (filter #(and (:sent-ns %) (:answered-ns %) (#{:ok :fail} (:type %))) copies)]
    (boolean (some (fn [[a b]] (and (< (:sent-ns b) (:answered-ns a))
                                    (< (:sent-ns a) (:answered-ns b))))
                   (for [a cs, b cs :when (< (:c a) (:c b))] [a b])))))

(defn- pattern
  "A group's answers, for the report: e.g. \"queued+duplicate+txn-rolled-back\"."
  [copies]
  (->> copies
       (mapcat (fn [cp]
                 (case (:kind cp)
                   :txn [(cond (true? (:committed? cp))  "txn-committed"
                               (false? (:committed? cp)) "txn-rolled-back"
                               :else                     (str "txn-" (name (:type cp))))]
                   (if (seq (:answers cp))
                     (keep #(when (integer? (:tag %)) (:status %)) (:answers cp))
                     [(name (:type cp))]))))
       sort
       (str/join "+")))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [done    (->> history h/client-ops (h/remove h/invoke?) vec)
            groups  (->> done (filter #(and (= :dup (:f %)) (= :ok (:type %))))
                         (mapv #(assoc (:value %) :copies (:copies %))))
            finals  (filterv #(and (= :final-read (:f %)) (= :ok (:type %))) done)]
        (if (empty? finals)
          {:valid? :unknown, :error "no final read succeeded", :groups (count groups)}
          (let [{:keys [logs split gaps]} (merged-reads finals)
                recs    (for [[p rs] logs, [o pl] rs] {:p p, :o o, :pl pl})
                by-t    (group-by #(:t (:pl %)) (filter #(:t (:pl %)) recs))
                by-x    (into {} (map (fn [[x rs]] [x (mapv :o rs)]))
                              (group-by #(:x (:pl %)) (filter #(:x (:pl %)) recs)))
                sent-ts (set (map :t groups))
                strays  (vec (for [r recs
                                   :when (or (and (nil? (:t (:pl r))) (nil? (:x (:pl r))))
                                             (and (:t (:pl r)) (not (sent-ts (:t (:pl r))))))]
                               r))
                bad     (vec (for [grp groups
                                   :let [rs (mapv (fn [r] {:p (:p r), :o (:o r), :c (:c (:pl r))})
                                                  (get by-t (:t grp)))]
                                   b (group-check grp rs)]
                               (assoc b :g (:g grp), :t (:t grp), :p (:p grp))))
                fbad    (fillers-check groups by-x)
                kinds   (frequencies (map :why bad))
                together (count (filter #(in-flight-together? (:copies %)) groups))]
            {:valid?            (cond (or (seq bad) (seq fbad) (seq split) (seq gaps) (seq strays)) false
                                      (zero? together) :unknown
                                      :else true)
             :groups            (count groups)
             :fresh-partitions  (count (filter #(str/starts-with? (:p %) "n") groups))
             :copies            (reduce + (map (comp count :copies) groups))
             :groups-with-copies-in-flight-together together
             :answer-patterns   (into (sorted-map) (frequencies (map (comp pattern :copies) groups)))
             :problems          kinds
             :bad               (take 16 bad)
             :fillers-bad       (take 16 fbad)
             :stray-records     (take 16 strays)
             :nodes-disagree    (take 16 split)
             :final-read-gaps   gaps}))))))

;; ---------------------------------------------------------------------------
;; Workload

(defn workload
  "W31: copies of one push sent together; opts --dup-copies, --dup-fresh,
  --dup-kinds."
  [opts]
  (let [fresh (atom #{})]
    {:client          (map->Client {:fresh fresh})
     :checker         (checker)
     :generator       (generator opts fresh)
     :final-generator (gen/each-thread {:f :final-read, :value nil})}))
