; Runs the fixed panic checker over stored tests, without writing to them.
(require '[jepsen.store :as store]
         '[jepsen.checker :as checker]
         '[jepsen.queen.core :as core]
         '[clojure.pprint :refer [pprint]])

(doseq [dir *command-line-args*]
  (let [t (store/test dir)
        h (:history t)
        c (core/panic-checker)]
    (println "==" dir)
    (pprint {:fixed (checker/check c t h {})
             ; Control: without the nemesis's records, the same line fails.
             :without-corrupt-ops (checker/check c t (remove #(= :corrupt-file (:f %)) h) {})})))
(shutdown-agents)
(System/exit 0)
