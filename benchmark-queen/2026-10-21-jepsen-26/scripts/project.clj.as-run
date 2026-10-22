(defproject jepsen.queen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Queen's raft storage (openraft, queue logs as the WAL)."
  :url "https://github.com/queen-mq/queen"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.14"]
                 [org.clojure/data.json "2.5.1"]]
  :main jepsen.queen.core
  :jvm-opts ["-Xmx12g"
             "-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.queen.core})
