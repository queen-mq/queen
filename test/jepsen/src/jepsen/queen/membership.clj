(ns jepsen.queen.membership
  "Queen's raft membership API, and the replacement an operator does with it.

  Routes (handlers/raft.rs, replicator/raft/admin.rs), answered by any node (a
  follower forwards a change to the leader); an operator request carries no
  x-queen-tenant header (403 otherwise):

    GET    /api/v1/system/raft/membership        {membership: {leader, term,
             voters, learners, joint, changeInFlight, members: [{nodeId,
             voter, raft, http, matched, lag, lastAckMs, live}] ...}}
    POST   .../learners  {id, raft, http}
    POST   .../promote   {ids: [..], force?}
    PUT    .../voters    {voters: [..], force?}
    DELETE .../members/:id

  A change answers {ok, membership}; a refusal is 409 with a code (in_flight,
  no_quorum, last_voter, learner_behind, already_voter, address_mismatch,
  membership_changed, leader_changed, not_a_learner, not_a_member), 503
  retry / leader_unreachable, or 504 timeout (the change may still complete).
  Every change is idempotent.

  replace! is the operator's cycle: remove the members, then for each: kill
  it, wipe its data directory, start it EMPTY with QUEEN_RAFT_JOIN=true (it
  must not initialize a cluster; it serves 503 until added), add it as a
  learner, wait until it is within reach of the leader's log (it may take a
  snapshot and exit 75 on the way), promote it. A removed voter is dropped
  entirely, and comes back under its id only with its disk wiped. Every step
  is recorded with the membership its answer carried."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [random :as rand]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.queen [db :as qdb]
                          [http :as qh]]))

(def path "/api/v1/system/raft/membership")

(def change-timeout-ms
  "?timeoutMs= of every change; the HTTP timeout is 5 s more."
  20000)

(def ^:private http
  (delay (qh/client)))

(defn id->node [test id] (nth (vec (:nodes test)) (dec (long id))))
(defn node->id [test node] (qdb/node-id test node))

(defn- call!
  "One admin request: {:status s :body b}, or {:error [type msg]} when it got
  no answer."
  [node method suffix body timeout-ms]
  (try (qh/request! @http method (str (qh/base-url node) path suffix) body timeout-ms)
       (catch clojure.lang.ExceptionInfo e
         (let [{:keys [type msg]} (ex-data e)]
           (if (#{::qh/refused ::qh/timeout ::qh/io} type)
             {:error [(keyword (name type)) msg]}
             (throw e))))))

(defn summary
  "What to record of a membership."
  [m]
  (when m
    (cond-> {:leader   (:leader m)
             :term     (:term m)
             :voters   (vec (sort (:voters m)))
             :learners (vec (sort (:learners m)))}
      (:joint m)          (assoc :joint (:joint m))
      (:changeInFlight m) (assoc :in-flight true))))

(defn member
  "The member entry of node id `id`, or nil."
  [m id]
  (some #(when (= id (:nodeId %)) %) (:members m)))

(defn fetch
  "The freshest membership the nodes report: a leader's own view when one
  answers (the highest term), else the view with the highest term and
  membership index. nil when no node answers."
  [test]
  (let [views (->> (:nodes test)
                   (util/real-pmap
                     (fn [n]
                       (let [r (call! n :get "" nil 3000)]
                         (when (and (= 200 (:status r)) (map? (:membership (:body r))))
                           (assoc (:membership (:body r)) :via n)))))
                   (remove nil?))
        rank  (juxt #(or (:term %) -1) #(or (:membershipIndex %) -1))
        leads (filter #(= "leader" (:source %)) views)]
    (when (seq views)
      (last (sort-by rank (if (seq leads) leads views))))))

(defn- record
  "The record of one change: where it went, its answer, the membership the
  answer carried."
  [step via r t0]
  (let [b (:body r)]
    (cond-> {:step step, :via via, :ms (- (System/currentTimeMillis) t0)}
      (:status r)      (assoc :status (:status r))
      (:error r)       (assoc :error (:error r))
      (map? b)         (cond->
                         (:code b)       (assoc :code (:code b))
                         (and (:error b) (not (:ok b))) (assoc :message (str (:error b)))
                         (:membership b) (assoc :membership (summary (:membership b)))))))

(def transient-codes
  "409 codes a later attempt can pass: another change still running, or the
  view the change was checked against moved on."
  #{"in_flight" "membership_changed" "leader_changed"})

(defn change!
  "Sends one change through the candidate nodes in turn (a node that does not
  answer, or answers 503, is skipped for the next), retrying transient
  refusals until deadline-ms. The record of the last answer, with :tries."
  [test step candidates method suffix body deadline-ms]
  (let [t0 (System/currentTimeMillis)]
    (loop [cands (cycle (rand/shuffle (vec candidates))), tries 1]
      (let [via (first cands)
            r   (call! via method (str suffix "?timeoutMs=" change-timeout-ms) body
                       (+ 5000 change-timeout-ms))
            rec (record step via r t0)
            again? (or (:error r)
                       (= 503 (:status r))
                       (and (= 409 (:status r)) (transient-codes (:code rec))))]
        (if (and again? (< (- (System/currentTimeMillis) t0) deadline-ms))
          (do (Thread/sleep 1000) (recur (rest cands) (inc tries)))
          (assoc rec :tries tries))))))

(defn await-answering!
  "Until `node` answers /health with any status (a joining node serves 503)."
  [node timeout-ms]
  (let [t0 (System/currentTimeMillis)]
    (loop []
      (let [r (try (qh/request! @http :get (str (qh/base-url node) "/health") nil 1000)
                   (catch Exception _ nil))]
        (cond r                                               true
              (< timeout-ms (- (System/currentTimeMillis) t0)) false
              :else (do (Thread/sleep 250) (recur)))))))

(defn await-caught-up!
  "Until the leader reports member `id` live and at most max-lag entries
  behind its last one."
  [test id max-lag timeout-ms]
  (let [t0 (System/currentTimeMillis)]
    (loop []
      (let [m   (fetch test)
            mem (when m (member m id))
            el  (- (System/currentTimeMillis) t0)]
        (cond (and mem (:live mem) (some? (:lag mem)) (<= (:lag mem) max-lag))
              {:step :catch-up, :ms el, :lag (:lag mem), :matched (:matched mem)}

              (< timeout-ms el)
              {:step :catch-up, :ms el, :timeout true
               :member (select-keys mem [:voter :matched :lag :lastAckMs :live])}

              :else (do (Thread/sleep 1000) (recur)))))))

(defn remove!
  "DELETE member `node`, through a node not in `exclude`. After a 504 the
  change may still complete: wait up to 30 s to see. :removed says whether
  it is out."
  [test node exclude]
  (let [id  (node->id test node)
        via (vec (remove (set exclude) (:nodes test)))
        rec (change! test :remove via :delete (str "/members/" id) nil 60000)]
    (if (= 200 (:status rec))
      (assoc rec :node node, :removed true)
      (let [m (loop [i 0]
                (let [m (fetch test)]
                  (if (or (<= 30 i) (not= 504 (:status rec)) (and m (not (member m id))))
                    m
                    (do (Thread/sleep 1000) (recur (inc i))))))]
        (assoc rec :node node, :removed (boolean (and m (not (member m id))))
               :after (summary m))))))

(defn restart-empty!
  "kill `node`, wipe its data, start it with QUEEN_RAFT_JOIN=true."
  [test node]
  (let [t0 (System/currentTimeMillis)]
    (c/on-nodes test [node]
                (fn [test node]
                  (qdb/kill-node! (dissoc test :lazyfs) node)
                  (qdb/wipe-data!)
                  (qdb/write-run-sh! test node {"QUEEN_RAFT_JOIN" "true"})
                  (qdb/start-node! test node)))
    {:step      :wipe-start
     :node      node
     :answering (await-answering! node 30000)
     :ms        (- (System/currentTimeMillis) t0)}))

(defn add-and-promote!
  "Add the (empty, started) `node` as a learner, wait until it has caught up,
  promote it. The step records."
  [test node]
  (let [id  (node->id test node)
        ip  (cn/ip node)
        via (vec (remove #{node} (:nodes test)))
        add (change! test :add-learner via :post "/learners"
                     {:id id, :raft (str ip ":" qdb/raft-port), :http (str ip ":" qh/http-port)}
                     60000)]
    (if (not= 200 (:status add))
      [add]
      (let [cu (await-caught-up! test id 100 180000)
            pr (loop [i 0]
                 (let [p (change! test :promote via :post "/promote" {:ids [id]} 60000)]
                   (if (and (= "learner_behind" (:code p)) (< i 30))
                     (do (Thread/sleep 2000) (recur (inc i)))
                     p)))]
        [add cu pr]))))

(defn rejoined?
  [steps]
  (let [p (peek (vec steps))]
    (and (= :promote (:step p)) (= 200 (:status p)))))

(defn replace!
  "Remove `targets` one after another (stopping at the first refusal: a
  no_quorum refusal is the API doing its job), then wipe and rejoin every
  node that is out."
  [test targets]
  (let [before   (fetch test)
        removals (reduce (fn [acc node]
                           (let [r (remove! test node targets)]
                             (if (:removed r) (conj acc r) (reduced (conj acc r)))))
                         [] targets)
        out      (->> removals (filter :removed) (map :node))
        rejoins  (mapv (fn [node]
                         (let [steps (into [(restart-empty! test node)]
                                           (add-and-promote! test node))]
                           {:node node, :rejoined (rejoined? steps), :steps steps}))
                       out)]
    {:targets  (vec targets)
     :before   (summary before)
     :removals removals
     :rejoins  rejoins
     :after    (summary (fetch test))}))

(defn cycle!
  "One voter out and back: a follower on odd cycles, the leader on even ones."
  [test n]
  (let [m (fetch test)]
    (cond
      (nil? m)              {:skipped :no-membership}
      (:changeInFlight m)   {:skipped :in-flight, :before (summary m)}
      :else
      (let [voters (set (:voters m))
            leader (:leader m)
            pick   (if (and (even? n) (voters leader))
                     leader
                     (let [fs (vec (sort (disj voters leader)))]
                       (when (seq fs) (rand/nth fs))))]
        (if-not pick
          {:skipped :no-target, :before (summary m)}
          (assoc (replace! test [(id->node test pick)])
                 :leader-target (= pick leader)))))))

(defn double-cycle!
  "Two voters out (the cluster runs on three), then both back. Only from a
  full five-voter membership."
  [test]
  (let [m (fetch test)]
    (cond
      (nil? m)                      {:skipped :no-membership}
      (:changeInFlight m)           {:skipped :in-flight, :before (summary m)}
      (< (count (:voters m)) 5)     {:skipped :not-full, :before (summary m)}
      :else
      (replace! test (mapv #(id->node test %)
                           (take 2 (rand/shuffle (vec (:voters m)))))))))

(defn heal!
  "Every node back as a voter: a learner is caught up and promoted, a node
  that is not a member is wiped and rejoins."
  [test]
  (let [m (fetch test)]
    (if (nil? m)
      {:skipped :no-membership}
      (let [voters   (set (:voters m))
            learners (set (:learners m))
            missing  (remove #(voters (node->id test %)) (:nodes test))
            healed   (mapv (fn [node]
                             (let [id    (node->id test node)
                                   steps (if (learners id)
                                           (let [cu (await-caught-up! test id 100 180000)
                                                 pr (change! test :promote
                                                             (vec (remove #{node} (:nodes test)))
                                                             :post "/promote" {:ids [id]} 60000)]
                                             [cu pr])
                                           (into [(restart-empty! test node)]
                                                 (add-and-promote! test node)))]
                               {:node node, :rejoined (rejoined? steps), :steps steps}))
                           missing)]
        {:before (summary m)
         :healed healed
         :after  (summary (fetch test))}))))
