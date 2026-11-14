(ns jepsen.queen.workload.seek
  "W26 --workload seek: moving a consumer group (POST
  /api/v1/consumer-groups/:group/queues/:queue/seek, writing-applications/
  pop.mdx): to the end, or to a point in time. W2's client pushes and
  consumes (group g1); twice in a test a client seeks:

    at 30% of the time limit  {toEnd: true}: the group skips everything the
                              queue holds
    at 60%                    {timestamp: 5 s before the seek}: the group
                              reads again from there

  The checker rejects, judged only without clock faults (1 s of margin for
  the broker's clock, which stamps the messages):
    - after the seek to the end answered, a pop delivering a message whose
      push was acknowledged before that seek was sent (it should have been
      skipped), until the second seek was sent;
    - a message pushed at least 1 s after the second seek's point in time,
      and acknowledged before that seek was sent, that no pop sent after the
      seek answered delivered (the group must read it again);
    - a message pushed after the seek to the end answered that no pop
      delivered by the end.
  W2's rules are reported, not judged: a seek skips messages for good,
  delivers acked ones again, and clears the group's leases."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]]
            [jepsen.queen.http :as qh]
            [jepsen.queen.workload.queue :as queue])
  (:import (java.net URLEncoder)
           (java.time Instant)))

(def s 1000000000)
(def back-s 5)

(defn- enc [x] (URLEncoder/encode (str x) "UTF-8"))

(defrecord Client [inner]
  client/Client
  (open! [this test node] (assoc this :inner (client/open! inner test node)))
  (setup! [this test])
  (invoke! [this test op]
    (if (= :seek (:f op))
      (let [c    (:inner this)
            back (= :time (:value op))
            body (if back
                   {:timestamp (str (Instant/ofEpochMilli (- (System/currentTimeMillis) (* 1000 back-s))))}
                   {:toEnd true})
            r    (try (qh/request! (:http c) :post
                                   (str (:base c) "/api/v1/consumer-groups/g1/queues/"
                                        (enc (queue/queue-name test)) "/seek")
                                   body (:client-timeout-ms test))
                      (catch clojure.lang.ExceptionInfo e {:exception (ex-data e)}))]
        (assoc op
               :type (cond (= 200 (:status r)) :ok
                           (= ::qh/refused (:type (:exception r))) :fail
                           (:exception r) :info
                           (<= 400 (:status r) 499) :fail
                           :else :info)
               ; The seek's point in the history's own time base.
               :point (when back (- (:time op) (* back-s s)))
               :answer (or (:body r) (:type (:exception r)))))
      (client/invoke! (:inner this) test op)))
  (teardown! [this test])
  (close! [this test] (client/close! (:inner this) test))
  client/Reusable
  (reusable? [this test] true))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [judged? (not (some #{:clock} (:faults test)))
            hist    (h/client-ops history)
            seek-of (fn [v] (->> hist (h/filter #(and (= :seek (:f %)) (= v (:value %)) (= :ok (:type %))))
                                 first))
            end     (seek-of :end)
            back    (seek-of :time)
            inv-of  (fn [op] (:time (h/invocation hist op)))
            pushes  (->> hist (h/filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                         (map (fn [op] [(:value op) {:inv (inv-of op), :comp (:time op)}]))
                         (into {}))
            pops    (->> hist (h/filter #(and (= :dequeue (:f %)) (#{:ok :info} (:type %)))) vec)
            ; Seek to the end: nothing older than it delivered after it,
            ; until the seek back was sent.
            until   (if back (inv-of back) Long/MAX_VALUE)
            skipped (if-not end
                      []
                      (vec (for [op pops
                                 :let [pinv (inv-of op)]
                                 :when (< (:time end) pinv until)
                                 m (:msgs op)
                                 :let [p (pushes (:v m))]
                                 :when (and p (< (:comp p) (inv-of end)))]
                             {:v (:v m), :pop (:index op)})))
            ; Seek back: every message after its point, there before it,
            ; delivered again after it: by a pop sent after the seek
            ; answered, or by a pop in flight while it applied that brought
            ; back a message acked before that pop was sent, which only the
            ; seek can do (seek-pause 2026-11-14: a 1 s long-poll sent 4 ms
            ; before the seek answered re-read 12267 and acked it).
            acked-before (fn [v t] (some (fn [op] (and (< (:time op) t)
                                                       (some #(and (= v (:v %)) (= :acked (:ack %))) (:msgs op))))
                                         pops))
            again   (set (concat
                           (for [op pops :when (and back (< (:time back) (inv-of op)))
                                 m (:msgs op)]
                             (:v m))
                           (for [op pops :when (and back (< (inv-of back) (:time op))
                                                    (<= (inv-of op) (:time back)))
                                 m (:msgs op)
                                 :when (acked-before (:v m) (inv-of op))]
                             (:v m))))
            missed  (if-not back
                      []
                      (->> pushes
                           (filter (fn [[v p]] (and (<= (+ (:point back) s) (:inv p))
                                                    (< (:comp p) (inv-of back)))))
                           (map key)
                           (remove again)
                           sort vec))
            delivered (set (for [op pops, m (:msgs op)] (:v m)))
            lost    (if-not end
                      []
                      (->> pushes (filter (fn [[v p]] (< (:time end) (:inv p))))
                           (map key) (remove delivered) sort vec))
            ; W2's rules, reported only.
            qc      (checker/check (queue/queue-checker) test hist opts)]
        {:valid?        (and (or (not judged?) (and (empty? skipped) (empty? missed)))
                             (empty? lost))
         :lost-after-seek-to-end (take 16 lost)
         :judged?       judged?
         :seek-to-end   (some? end)
         :seek-back     (some? back)
         :not-skipped   (take 16 skipped)
         :not-skipped-count (count skipped)
         :not-read-again (take 16 missed)
         :not-read-again-count (count missed)
         :leases        (select-keys qc [:lease-overlap-count :acked-twice-count :deliveries :acked])}))))

(defn workload
  [opts]
  (let [inner (queue/workload opts)
        tl    (long (:time-limit opts 300))]
    {:client          (->Client (:client inner))
     :checker         (checker)
     :generator       (gen/any
                        (gen/phases (gen/sleep (long (* 0.3 tl)))
                                    (gen/once {:f :seek, :value :end})
                                    (gen/sleep (long (* 0.3 tl)))
                                    (gen/once {:f :seek, :value :time}))
                        (:generator inner))
     :final-generator (:final-generator inner)}))
