(defproject jepsen.queen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Queen's raft storage (openraft, queue logs as the WAL)."
  :url "https://github.com/queen-mq/queen"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.14"]
                 [org.clojure/data.json "2.5.1"]
                 ; W14 kafka: the Kafka facade through the Java client
                 ; (the facade is tested with 3.6.2, 3.9.1 and 4.3.1).
                 [org.apache.kafka/kafka-clients "3.9.1" :exclusions [org.slf4j/slf4j-api]]
                 ; The connector workloads read and write PostgreSQL.
                 [org.postgresql/postgresql "42.7.4"]
                 [com.github.seancorfield/next.jdbc "1.3.955"]]
  :main jepsen.queen.core
  :jvm-opts ["-Xmx12g"
             "-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.queen.core})
