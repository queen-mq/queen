(ns jepsen.queen.workload.locks
  "W11 --workload locks, W11b --workload semaphore: leases with a fencing
  token (POST /api/v1/locks, server/src/locks.rs).

  A lock is NOT tested as a mutex, because it is not one: a permit expires and
  nobody tells its holder, so two clients may both believe they hold it. What
  the broker promises is narrower, and it is what this checks:

    - a later holder of a permit has a higher token than every earlier one;
    - a transaction that carries a permit's guard (a KV `check` of the
      permit's row at its token, required) commits only while that token is
      still the permit's.

  Locks jl<l>, l in 0..(--w11-locks). Each lock guards one RESOURCE: partition
  r<l> of the first test queue. Every client thread is one holder with one
  owner for the whole test, and keeps the token of its last answer until the
  broker tells it the permit is gone. It never looks at a clock, so it goes on
  using a token whose lease ran out: the stalled holder, on every thread.

    :acquire l   acquire jl<l> for 2-4 s (a semaphore: with its limit). :ok
                 with the token; `already` when this owner held it (the answer
                 to a retry); :fail when somebody else holds it.
    :step l      ONE /api/v1/transaction (on l, or on another lock this thread
                 has when it does not have l): the guard of the token it
                 has, and a push of {l, slot, token, owner, id} to r<l>. :ok
                 when it committed; :fail when it rolled back (the guard lost:
                 the thread forgets the permit) or was refused.
    :renew l     renew with the token this thread has: :ok with the NEW token.
                 An unknown outcome keeps the old one, so the next renew is
                 the retry the owner makes safe and the next step loses.
    :release l   release with the token this thread has.
    :get l       the holders the broker reports.
    final phase  :final-read, every record of every r<l> from offset 0.

  Checker (the end state is the :final-read invoked last):
    1. fencing: along a resource, per slot, tokens never go back. A record
       with a lower token after a higher one is a replaced holder's commit;
    2. one owner per token, and every record carries a token its owner was
       answered;
    3. no phantom: no record of a step that rolled back or was refused, and
       the record of every :ok step is there;
    4. per permit, a grant invoked after another completed has a higher
       token, and no two grants share one;
    5. an `already` answer names a permit that owner was granted, or may have
       been (an acquire or a renew of its own with an unknown outcome);
    6. leases are exclusive in real time: a permit is not handed to another
       owner before the lease of the grant right before it could have ended
       (unless its holder released, or its state is unknown). Durations, on
       the control node's clock, with the margin the cluster allows between
       its nodes' clocks; not judged under clock faults, where a lease's
       length is the fault;
    7. a `get` never shows a token below one granted before it was invoked;
    8. a semaphore never grants a slot outside 0..limit-1.

  Reported and not judged: an owner seen in two slots of one semaphore (an
  acquire still in flight when its retry was sent can take a second slot; it
  holds no more than the limit and ends with its lifetime)."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]))

(def margin-ns
  "QUEEN_RAFT_MAX_CLOCK_SKEW_MS: how far two nodes' clocks may differ."
  (* 500 1000000))

(defn locks [test] (:w11-locks test 3))
(defn resource-queue [test] (first (:queue-names test)))

(defn- lock-name [l] (str "jl" l))
(defn- resource [l] (str "r" l))

(defn- call
  [client method url body timeout-ms]
  (try (qh/request! (:http client) method url body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (if (#{::qh/refused ::qh/timeout ::qh/io} (:type (ex-data e)))
           {:exception (ex-data e)}
           (throw e)))))

(defn- unanswered
  "The completion type of a call that did not answer 200: :fail when it
  cannot have taken effect (never sent, or refused before anything was
  planned), else :info. A lock call is several KV calls, so a 503 may have
  applied some of it."
  [r]
  (if (:exception r)
    (kv/exception-type (:type (:exception r)))
    (kv/write-failure [(:status r) (:body r)])))

(defn- why
  [r]
  (or (:type (:exception r)) (:status r)))

(defn- lock-call
  "One operation on POST /api/v1/locks: {:result its answer} on 200, else
  {:failed the response or the exception}."
  [client test operation]
  (let [r (call client :post (str (:base client) "/api/v1/locks")
                {:operations [operation]} (:client-timeout-ms test))]
    (if (= 200 (:status r))
      {:result (first (:results (:body r)))}
      {:failed r})))

;; ---------------------------------------------------------------------------
;; Client: one holder per thread

(defn- acquire!
  [client test op]
  (let [l   (:value op)
        ttl (+ 2 (rand/long 3))
        n   (:limit client)
        {:keys [result failed]}
        (lock-call client test (cond-> {:op "acquire", :name (lock-name l)
                                        :ttlSeconds ttl, :owner (:owner client)}
                                 (< 1 n) (assoc :limit n)))
        op  (assoc op :owner (:owner client), :ttl ttl)]
    (cond
      failed
      (assoc op :type (unanswered failed), :error [:acquire (why failed) (:body failed)])

      (true? (:acquired result))
      (do (swap! (:held client) assoc l (select-keys result [:token :slot :guard]))
          (assoc op :type :ok, :token (:token result), :slot (:slot result)
                 :already? (true? (:already result))))

      :else
      (assoc op :type :fail, :error [:not-acquired (:reason result)]))))

(defn- step!
  [client test op]
  (let [held @(:held client)
        ; The lock asked for, or any lock this thread has: a thread holds one
        ; lock in ten, and a step that names another would do nothing.
        l    (if (contains? held (:value op)) (:value op) (first (keys held)))]
    (if-let [{:keys [token slot guard]} (get held l)]
      (let [id   (str (random-uuid))
            op   (assoc op :value l)
            r    (call client :post (str (:base client) "/api/v1/transaction")
                       {:operations [{:type  "push"
                                      :items [{:queue         (resource-queue test)
                                               :partition     (resource l)
                                               :payload       {:l l, :slot slot, :token token
                                                               :owner (:owner client), :id id}
                                               :transactionId id}]}]
                        ; The guard as the broker answered it: where a
                        ; permit's row lives is the broker's rule.
                        :kv [guard]}
                       (:client-timeout-ms test))
            op   (assoc op :owner (:owner client), :token token, :slot slot, :id id)
            body (:body r)]
        (cond
          (:exception r)
          (assoc op :type (kv/exception-type (:type (:exception r)))
                 :error [:txn (:type (:exception r))])

          (and (= 200 (:status r)) (true? (:success body)))
          (assoc op :type :ok)

          (= 200 (:status r))
          (do (when (= "kv_precondition" (:reason body))
                ; The guard lost: replaced, or the lease ended.
                (swap! (:held client) dissoc l))
              (assoc op :type :fail
                     :error [:rolled-back (:reason body) (:kvReason body) (:version body)]))

          ; Refused before anything ran: a member still below cluster
          ; version 5 (rsm/facade/real/kv.rs kv_ops_admitted).
          (and (= 503 (:status r)) (map? body)
               (= "kv_check_needs_cluster_version_5" (:reason body)))
          (assoc op :type :fail, :error [:not-yet])

          :else
          (assoc op :type (kv/write-failure [(:status r) body])
                 :error [:txn (:status r) body])))
      (assoc op :type :fail, :error [:not-held]))))

(defn- renew!
  [client test op]
  (let [l (:value op)]
    (if-let [{:keys [token slot]} (get @(:held client) l)]
      (let [ttl (+ 2 (rand/long 3))
            {:keys [result failed]}
            (lock-call client test (cond-> {:op "renew", :name (lock-name l), :token token
                                            :ttlSeconds ttl, :owner (:owner client)}
                                     (pos? slot) (assoc :slot slot)))
            op  (assoc op :owner (:owner client), :from token, :slot slot, :ttl ttl)]
        (cond
          ; Unknown or refused: the token is kept either way.
          failed
          (assoc op :type (unanswered failed), :error [:renew (why failed)])

          (true? (:renewed result))
          (do (swap! (:held client) assoc l
                     {:token (:token result), :slot slot, :guard (:guard result)})
              (assoc op :type :ok, :token (:token result)))

          :else
          (do (swap! (:held client) dissoc l)
              (assoc op :type :fail, :error [:lost]))))
      (assoc op :type :fail, :error [:not-held]))))

(defn- release!
  [client test op]
  (let [l (:value op)]
    (if-let [{:keys [token slot]} (get @(:held client) l)]
      (let [{:keys [result failed]}
            (lock-call client test (cond-> {:op "release", :name (lock-name l), :token token}
                                     (pos? slot) (assoc :slot slot)))
            op (assoc op :owner (:owner client), :token token, :slot slot)]
        ; Whatever became of it, this holder stops using the permit.
        (swap! (:held client) dissoc l)
        (cond
          failed                     (assoc op :type (unanswered failed)
                                            :error [:release (why failed)])
          (true? (:released result)) (assoc op :type :ok)
          :else                      (assoc op :type :fail, :error [:lost])))
      (assoc op :type :fail, :error [:not-held]))))

(defn- get!
  [client test op]
  (let [{:keys [result failed]}
        (lock-call client test {:op "get", :name (lock-name (:value op))})]
    (if failed
      (assoc op :type :fail, :error [:get (why failed)])
      (assoc op :type :ok
             :holders (mapv #(select-keys % [:slot :owner :token]) (:holders result))))))

(defn- fetch-partition
  "Every record of one partition of `queue`, from offset 0."
  [client test queue partition]
  (loop [off 0, out []]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                         {:entries [{:queue queue, :partition partition, :offset off}]}
                         (:client-timeout-ms test))
          e (first (:entries (:body r)))]
      (cond
        (not= 200 (:status r)) (throw (ex-info "fetch failed" {:status (:status r)}))
        (seq (:records e))     (recur (inc (long (:offset (peek (:records e)))))
                                      (into out (:records e)))
        :else                  out))))

(defn- final-read!
  [client test op]
  (assoc op :type :ok
         :value (into (sorted-map)
                      (map (fn [l]
                             [l (mapv (fn [rec] (assoc (:payload rec) :offset (:offset rec)))
                                      (fetch-partition client test (resource-queue test)
                                                       (resource l)))]))
                      (range (locks test)))))

(defrecord Client [limit node base http owner held]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)
           ; One owner per thread, for the whole test: a thread that got no
           ; answer to an acquire is answered its own permit when it asks again.
           :owner (str "o-" node "-" (random-uuid))
           :held  (atom {})))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :acquire    (acquire! this test op)
        :step       (step! this test op)
        :renew      (renew! this test op)
        :release    (release! this test op)
        :get        (get! this test op)
        :final-read (final-read! this test op))
      (catch clojure.lang.ExceptionInfo e
        ; Only the final read's fetches reach here: every other call turns
        ; its own transport failure into a completion.
        (let [{:keys [type msg]} (ex-data e)]
          (cond
            (= "fetch failed" (.getMessage e))
            (assoc op :type :fail, :error [:fetch (ex-data e)])

            (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type :fail, :error [(keyword (name type)) msg])

            :else (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

;; ---------------------------------------------------------------------------
;; Generator

(defn generator
  "Acquires, guarded steps, a few renews and releases, on a random lock. Few
  renews on purpose: a lease of 2-4 s that is renewed about every 2 s runs out
  often, which is where a takeover and a stale token come from."
  [opts]
  (let [n  (:w11-locks opts 3)
        on (fn [f] (fn [] {:f f, :value (rand/long n)}))]
    (gen/mix (concat (repeat 4 (on :acquire))
                     (repeat 10 (on :step))
                     [(on :renew) (on :release) (on :get)]))))

;; ---------------------------------------------------------------------------
;; Checker

(defn- with-highest-before
  "Pairs each of `asks` (maps with :inv) with the highest :token among
  `grants` (maps with :comp and :token) that completed before it was invoked,
  or 0: [[ask highest] ...]."
  [grants asks]
  (let [done (vec (sort-by :comp grants))]
    (loop [asks (sort-by :inv asks), i 0, top 0, out (transient [])]
      (if-let [a (first asks)]
        (if (and (< i (count done)) (< (:comp (nth done i)) (:inv a)))
          (recur asks (inc i) (max top (long (:token (nth done i)))) out)
          (recur (rest asks) i top (conj! out [a top])))
        (persistent! out)))))

(defn- taken-early
  "Check 6 for one permit. `grants` are its new grants; `ended` are the ops
  (releases, and anything of unknown outcome) after which a holder may no
  longer have had it, as {:l :owner :inv}."
  [grants ended]
  (let [ended-by (group-by (juxt :l :owner) ended)]
    (->> (partition 2 1 (sort-by :token grants))
         (keep (fn [[g b]]
                 (let [until (+ (:inv g) (* (long (:ttl g)) 1000000000))]
                   (when (and (= :acquire (:f b))
                              (not= (:owner g) (:owner b))
                              (< (+ (:comp b) margin-ns) until)
                              (not-any? #(< (:inv g) (:inv %) (:comp b))
                                        (ended-by [(:l g) (:owner g)])))
                     {:l (:l g), :slot (:slot g)
                      :holder (:owner g), :token (:token g), :ttl (:ttl g)
                      :taken-by (:owner b), :new-token (:token b)
                      :early-ms (quot (- until (:comp b)) 1000000)
                      :ops [(:index g) (:index b)]})))))))

(defn checker
  [limit]
  (reify checker/Checker
    (check [this test history opts]
      (let [done   (->> history h/client-ops (h/remove h/invoke?) vec)
            inv    (fn [op] (:time (h/invocation history op)))
            ok?    #(= :ok (:type %))
            of     (fn [f] (filterv #(= f (:f %)) done))
            ; The test map's :nemesis is the nemesis itself; the fault names
            ; are in :faults (core/queen-test).
            clock? (contains? (set (:faults test)) :clock)
            finals (filterv ok? (of :final-read))]
        (if (empty? finals)
          {:valid? :unknown, :error "no final read succeeded"}
          (let [last-rd  (apply max-key inv finals)
                end-inv  (inv last-rd)
                records  (:value last-rd)
                all-recs (vec (mapcat val records))
                acquires (of :acquire)
                renews   (of :renew)
                grant    (fn [op] {:l (:value op), :slot (:slot op), :token (:token op)
                                   :owner (:owner op), :ttl (:ttl op), :f (:f op)
                                   :inv (inv op), :comp (:time op), :index (:index op)})
                ; A NEW grant handed out a token nobody had: an acquire that
                ; took a free permit, or a renew.
                grants   (->> (concat (filter #(and (ok? %) (not (:already? %))) acquires)
                                      (filter ok? renews))
                              (mapv grant))
                again    (->> acquires (filter #(and (ok? %) (:already? %))) (mapv grant))
                by-key   (group-by (juxt :l :slot) grants)

                ; 1. fencing, per slot of each resource, in offset order
                went-back (->> records
                               (mapcat
                                 (fn [[l recs]]
                                   (->> (group-by :slot recs)
                                        (mapcat
                                          (fn [[slot rs]]
                                            (->> (partition 2 1 rs)
                                                 (keep (fn [[a b]]
                                                         (when (< (:token b) (:token a))
                                                           {:l l, :slot slot
                                                            :offset (:offset b), :token (:token b)
                                                            :owner (:owner b)
                                                            :after-offset (:offset a)
                                                            :after-token (:token a)
                                                            :after-owner (:owner a)})))))))))
                               vec)

                ; 2. one owner per token; no token from nowhere
                two-owners (->> (group-by (juxt :l :slot :token) all-recs)
                                (keep (fn [[k rs]]
                                        (let [owners (set (map :owner rs))]
                                          (when (< 1 (count owners))
                                            {:permit k, :owners owners}))))
                                vec)
                answered (->> (concat grants again) (map (juxt :l :slot :token :owner)) set)
                nowhere  (->> all-recs
                              (remove #(answered [(:l %) (:slot %) (:token %) (:owner %)]))
                              vec)

                ; 3. phantoms, and :ok steps whose record is missing
                steps    (filterv :id (of :step))
                by-id    (into {} (map (juxt :id identity)) steps)
                phantom  (->> all-recs
                              (keep (fn [r]
                                      (let [s (by-id (:id r))]
                                        (cond (nil? s)            {:record r, :why :no-such-step}
                                              (= :fail (:type s)) {:record r, :why :failed-step
                                                                   :error (:error s)}))))
                              vec)
                rec-ids  (set (map :id all-recs))
                missing  (->> steps
                              (filter #(and (ok? %) (<= (:time %) end-inv)))
                              (remove #(rec-ids (:id %)))
                              (mapv #(select-keys % [:value :slot :token :owner :id :index])))

                ; 4. tokens rise, and none is granted twice
                not-rising (->> by-key
                                (mapcat (fn [[k gs]]
                                          (->> (with-highest-before gs gs)
                                               (keep (fn [[g top]]
                                                       (when (<= (:token g) top)
                                                         {:permit k, :token (:token g)
                                                          :earlier-token top, :op (:index g)}))))))
                                vec)
                twice    (->> by-key
                              (mapcat (fn [[k gs]]
                                        (->> (group-by :token gs)
                                             (filter #(< 1 (count (val %))))
                                             (map (fn [[t gs]] {:permit k, :token t
                                                                :ops (mapv :index gs)})))))
                              vec)

                ; 5. `already` names a permit that owner had, or may have had
                had      (set (map (juxt :l :owner :token) grants))
                unknown  (->> (concat acquires renews)
                              (filter #(= :info (:type %)))
                              (map (juxt :value :owner))
                              set)
                already-bad (->> again
                                 (remove #(or (had [(:l %) (:owner %) (:token %)])
                                              (unknown [(:l %) (:owner %)])))
                                 (mapv #(select-keys % [:l :slot :owner :token :index])))

                ; 6. exclusive leases in real time
                ended    (->> done
                              (filter #(or (and (= :release (:f %)) (#{:ok :info} (:type %)))
                                           (and (#{:acquire :renew} (:f %)) (= :info (:type %)))))
                              (mapv (fn [op] {:l (:value op), :owner (:owner op), :inv (inv op)})))
                early    (if clock?
                           []
                           (vec (mapcat (fn [[_ gs]] (taken-early gs ended)) by-key)))

                ; 7. a get never shows less than what was granted before it
                seen     (->> (of :get)
                              (filter ok?)
                              (mapcat (fn [op]
                                        (map (fn [hd] {:l (:value op), :slot (:slot hd)
                                                       :token (:token hd), :owner (:owner hd)
                                                       :inv (inv op), :index (:index op)})
                                             (:holders op)))))
                stale    (->> (group-by (juxt :l :slot) seen)
                              (mapcat (fn [[k ss]]
                                        (->> (with-highest-before (get by-key k []) ss)
                                             (keep (fn [[s top]]
                                                     (when (< (:token s) top)
                                                       {:permit k, :read-token (:token s)
                                                        :granted top, :op (:index s)}))))))
                              vec)

                ; 8. slots of a semaphore
                bad-slot (->> (concat grants again)
                              (remove #(< -1 (:slot %) limit))
                              (mapv #(select-keys % [:l :slot :owner :token :index])))

                ; Not judged: an owner in two slots of one semaphore at once.
                doubled  (->> (of :get)
                              (filter ok?)
                              (filter (fn [op]
                                        (->> (:holders op) (keep :owner) frequencies vals
                                             (some #(< 1 %)))))
                              count)
                fenced   (filter #(and (= :fail (:type %))
                                       (= "kv_precondition" (second (:error %))))
                                 steps)
                takeovers (->> by-key
                               (mapcat (fn [[_ gs]] (partition 2 1 (sort-by :token gs))))
                               (filter (fn [[g b]] (and (= :acquire (:f b))
                                                        (not= (:owner g) (:owner b)))))
                               count)]
            {:valid?            (if (empty? all-recs)
                                  ; Nothing was ever committed under a guard:
                                  ; the run showed nothing.
                                  :unknown
                                  (and (empty? went-back) (empty? two-owners) (empty? nowhere)
                                       (empty? phantom) (empty? missing)
                                       (empty? not-rising) (empty? twice)
                                       (empty? already-bad) (empty? early)
                                       (empty? stale) (empty? bad-slot)))
             :limit             limit
             :grants            (count (filter #(= :acquire (:f %)) grants))
             :renewals          (count (filter #(= :renew (:f %)) grants))
             :already           (count again)
             :takeovers         takeovers
             :steps-ok          (count (filter ok? steps))
             :steps-fenced      (count fenced)
             :steps-info        (count (filter #(= :info (:type %)) steps))
             :records           (count all-recs)
             :final-reads-agree (apply = (map :value finals))
             :token-went-back-count (count went-back)
             :token-went-back   (take 16 went-back)
             :two-owners        (take 16 two-owners)
             :token-from-nowhere (take 16 nowhere)
             :phantom-count     (count phantom)
             :phantom           (take 16 phantom)
             :ok-step-missing   (take 16 missing)
             :token-not-rising  (take 16 not-rising)
             :token-granted-twice (take 16 twice)
             :already-from-nowhere (take 16 already-bad)
             :taken-early-count (count early)
             :taken-early       (take 16 early)
             :early-judged?     (not clock?)
             :stale-gets        (take 16 stale)
             :slot-out-of-range (take 16 bad-slot)
             :gets-with-an-owner-in-two-slots doubled}))))))

;; ---------------------------------------------------------------------------
;; Workloads

(defn- lease-workload
  [opts limit]
  {:client          (map->Client {:limit limit})
   :checker         (checker limit)
   :generator       (generator opts)
   :final-generator (gen/each-thread {:f :final-read, :value nil})
   ; A lock call spends the tenant's KV write rate, far below these rates.
   :db-env          kv/db-env})

(defn workload
  "W11: locks, one permit each."
  [opts]
  (lease-workload opts 1))

(defn semaphore-workload
  "W11b: semaphores of --w11-limit permits (at least 2)."
  [opts]
  (lease-workload opts (max 2 (:w11-limit opts 3))))
