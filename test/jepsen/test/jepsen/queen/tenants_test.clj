(ns jepsen.queen.tenants-test
  "W21 (tenants, raft groups): the checker on crafted histories, and the
  node env it asks for. No cluster."
  (:require [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.queen.workload.tenants :as w]))

(def ts (w/tenant-ids 2))
(def A (ts 0))
(def B (ts 1))

(def test-map {:name "tenants-synthetic", :start-time (util/local-time), :nodes ["n1"], :tenants 2})

(defn hist [ops] (h/history (map-indexed (fn [i op] (assoc op :time (* 1000000 i))) ops)))

(defn enq [t v p]
  [{:process 0, :type :invoke, :f :enqueue, :value v, :tenant t}
   {:process 0, :type :ok, :f :enqueue, :value v, :tenant t, :partition p, :offset 0}])

(defn deq [t ds]
  [{:process 1, :type :invoke, :f :dequeue, :value nil, :tenant t}
   {:process 1, :type :ok, :f :dequeue, :value (mapv :v ds), :tenant t, :lease-s 3
    :msgs (mapv #(merge {:txn (str "w" (:v %)), :pid (str "p" (:partition %)), :lease (str "l" (:v %))
                         :attempt 1, :ack :acked} %) ds)}])

(defn incr [t k type]
  [{:process 2, :type :invoke, :f :incr, :value nil, :tenant t, :key k}
   {:process 2, :type type, :f :incr, :value nil, :tenant t, :key k}])

(defn read-op [value]
  [{:process 3, :type :invoke, :f :read, :value nil}
   {:process 3, :type :ok, :f :read, :value value}])

(def clean
  (concat (enq A 0 "w0") (enq B 1 "w0")
          (deq A [{:v 0, :partition "w0", :offset 0}])
          (deq B [{:v 1, :partition "w0", :offset 0}])
          (incr A 0 :ok) (incr A 0 :info) (incr B 0 :ok)
          (read-op {A {"c:0" 2}, B {"c:0" 1}})))

(defn run [ops] (checker/check (w/checker) test-map (hist ops) {}))

(deftest w21-tenants
  (testing "clean"
    (is (:valid? (run clean))))
  (testing "a value delivered in a tenant it was not pushed in"
    (let [res (run (concat clean (deq B [{:v 0, :partition "w0", :offset 1}])))]
      (is (= 1 (:leak-count res)))))
  (testing "a tenant's counter above its acknowledged plus unknown incrs"
    (let [res (run (concat (butlast (butlast clean)) (read-op {A {"c:0" 3}, B {"c:0" 1}})))]
      (is (= [A] (map :tenant (:counters res))))))
  (testing "a tenant's counter below its acknowledged incrs (another tenant's
            write landed elsewhere)"
    (let [res (run (concat (butlast (butlast clean)) (read-op {A {"c:0" 2}, B {"c:0" 0}})))]
      (is (= [B] (map :tenant (:counters res))))))
  (testing "a tenant's acknowledged push never dequeued"
    (let [res (run (concat (enq A 5 "w1") clean))]
      (is (not (:valid? res)))
      (is (not (get-in res [:per-tenant A :valid?]))))))

(deftest w21-env
  (testing "tenants alone: the header, a trusted proxy declared, grants open"
    (is (= "1" (get (w/db-env {:tenants 2}) "QUEEN_KV_TRUSTED_PROXY")))
    (is (nil? (get (w/db-env {:tenants 2}) "QUEEN_RAFT_GROUPS"))))
  (testing "tenants over groups: pinned round robin"
    (is (= (str A "=0," B "=1")
           (get (w/db-env {:tenants 2, :raft-groups 3}) "QUEEN_TENANT_GROUPS")))))
