(ns jepsen.queen.db
  "A Queen raft cluster on the test nodes: one openraft voter per node, with
  the queue logs as the WAL and LMDB checkpoints under /opt/queen/data.

  Each node runs /opt/queen/run.sh, a wrapper that restarts the binary ONLY on
  exit 75 (a node exits 75 to load a snapshot it received; see qc.sh in the
  benchmark archive). Any other death, kill -9 included, leaves the node down
  until the nemesis starts it again, so a crash is never hidden by a restart."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.queen.http :as qh]
            [jepsen.queen.lazyfs :as qlazyfs]
            [jepsen.queen.runners :as runners])
  (:import (java.security MessageDigest)
           (java.io FileInputStream)))

(def dir              "/opt/queen")
(def bin              (str dir "/queen"))
(def run-sh           (str dir "/run.sh"))
(def data-dir         (str dir "/data"))
(def buf-dir          (str dir "/buf"))
(def log-file         (str dir "/queen.log"))
(def ls-file          (str dir "/data-ls.txt"))
(def state-file       (str dir "/raft-state.json"))
(def state-loop-sh    (str dir "/state-loop.sh"))
(def state-loop-pid   (str dir "/state-loop.pid"))
(def pid-file         (str dir "/queen.pid"))
(def wrapper-pid-file (str dir "/wrapper.pid"))
(def raft-port        7400)
(def proxy-port       6711)
(def proxy-spool      (str dir "/proxy-spool"))

;; ---------------------------------------------------------------------------
;; Two clusters in one test (--standby-nodes N): the last N nodes of the node
;; list are a cluster of their own, a STANDBY of the cluster the other nodes
;; form (the SOURCE). Each has its own raft group, node ids from 1 and raft
;; token. Without the option every node is in the one cluster, as before.

(defn standby-nodes
  "The nodes of the standby cluster, or none."
  [test]
  (vec (take-last (long (:standby-nodes test 0)) (:nodes test))))

(defn source-nodes
  "The nodes of the source: every node, without --standby-nodes."
  [test]
  (vec (drop-last (long (:standby-nodes test 0)) (:nodes test))))

(defn standby?
  [test node]
  (boolean (some #{node} (standby-nodes test))))

(defn cluster-nodes
  "The nodes of `node`'s own cluster."
  [test node]
  (if (standby? test node) (standby-nodes test) (source-nodes test)))

(defn node-id
  "Raft node ids are 1-based positions in the node's own cluster."
  [test node]
  (inc (.indexOf ^java.util.List (cluster-nodes test node) node)))

(defn peers
  "QUEEN_RAFT_PEERS: id=raft_addr/http_addr for every node of `node`'s
  cluster, private IPs."
  [test node]
  (->> (cluster-nodes test node)
       (map (fn [n]
              (let [ip (cn/ip n)]
                (str (node-id test n) "=" ip ":" raft-port "/" ip ":"
                     qh/http-port))))
       (str/join ",")))

(defn raft-token
  "QUEEN_RAFT_TOKEN of `node`'s cluster: the two clusters do not share one."
  [test node]
  (cond-> (:raft-token test)
    (standby? test node) (str "-standby")))

(defn link-env
  "What the cluster link adds (rsm/link): the source's nodes serve a standby
  that presents the link token; the standby's nodes name every raft address
  of the source and become a standby at their first election.

  --standby-seed: the standby is SEEDED instead (QUEEN_LINK_SEED=1, in place
  of QUEEN_LINK_STANDBY): its node 1 takes the source's snapshot as its first
  state, and its other nodes, fresh, wait to be added to it. Its source has a
  history by then: it keeps 16 entries behind its slowest live follower, and
  a follower that makes no progress for 5 s no longer holds its log, so the
  start of the log is gone and only the snapshot can start the standby."
  [test node]
  (cond
    (empty? (standby-nodes test)) {}
    (standby? test node)
    (merge {"QUEEN_LINK_SOURCE"       (->> (source-nodes test)
                                           (map #(str (cn/ip %) ":" raft-port))
                                           (str/join ","))
            "QUEEN_LINK_SOURCE_TOKEN" (:link-token test)}
           (if (:standby-seed test)
             {"QUEEN_LINK_SEED" "1"}
             {"QUEEN_LINK_STANDBY" "true"}))
    :else
    (merge {"QUEEN_LINK_TOKEN" (:link-token test)}
           (when (:standby-seed test)
             {"QUEEN_RAFT_LOG_KEEP"      "16"
              "QUEEN_RAFT_PURGE_HOLD_S"  "5"}))))

(defn env
  "The node's environment: the benchmark harness's (qc.sh) plus test knobs."
  [test node]
  (let [ip (cn/ip node)]
    (merge
      (sorted-map
        "QUEEN_STORAGE"                 "raft"
        "QUEEN_RAFT_REPLICATOR"         "openraft"
        "QUEEN_RAFT_NODE_ID"            (node-id test node)
        "QUEEN_RAFT_PEERS"              (peers test node)
        "QUEEN_RAFT_LISTEN"             (str ip ":" raft-port)
        "QUEEN_RAFT_TOKEN"              (raft-token test node)
        "QUEEN_RAFT_DIR"                data-dir
        "QUEEN_RAFT_DEDUP_INDEX"        (:dedup-index test)
        "QUEEN_RAFT_CLIENT_OFFLOAD"     (if (:offload test) "1" "0")
        "QUEEN_BIND_ADDR"               ip
        "PORT"                          qh/http-port
        "JWT_ENABLED"                   "false"
        "QUEEN_TENANCY_HEADER"          "false"
        "QUEEN_LANES"                   (:lanes test)
        "FILE_BUFFER_DIR"               buf-dir
        "LOG_LEVEL"                     "info"
        ; The push deadline (handlers/raft.rs deadline_for); the client's
        ; socket timeout is longer, so most outcomes are known.
        "POP_DEFAULT_TIMEOUT_MS"        (:server-timeout-ms test))
      (link-env test node)
      ; --tenants / --raft-groups (jepsen.queen.workload.tenants/db-env).
      (when (or (pos? (long (:tenants test 0))) (< 1 (long (:raft-groups test 1))))
        ((requiring-resolve 'jepsen.queen.workload.tenants/db-env) test))
      ; --via-proxy: the embedded proxy on its own port; QUEEN_PROXY_ENFORCE
      ; off (the dev plans cap requests far below a test's rate).
      (when (:via-proxy test)
        {"QUEEN_PROXY_EMBEDDED"          "true"
         "QUEEN_PROXY_PORT"              (str proxy-port)
         "QUEEN_PROXY_JWT_SECRET"        (:proxy-secret test)
         "QUEEN_PROXY_CP_TOKEN"          (:proxy-cp-token test)
         "QUEEN_PROXY_SPOOL_DIR"         proxy-spool
         "QUEEN_PROXY_BOOTSTRAP_TENANT"  "jepsen"
         "QUEEN_PROXY_BOOTSTRAP_PLAN"    "pro"
         "QUEEN_PROXY_BOOTSTRAP_API_KEY" (:proxy-key test)
         "QUEEN_PROXY_DEFAULT_CLUSTER"   "jepsen"
         "QUEEN_PROXY_ENFORCE"           "false"})
      ; W28-W30: the connectors (jepsen.queen.workload.connectors/db-env).
      (when (:connectors? test)
        ((requiring-resolve 'jepsen.queen.workload.connectors/db-env) test))
      ; --encrypt: at-rest encryption, one key for the whole cluster.
      (when (:encrypt test)
        {"QUEEN_ENCRYPTION_KEY" (:encryption-key test)})
      ; W14: the Kafka facade on every node, advertising its own address.
      (when (:kafka-facade? test)
        {"QUEEN_KAFKA_EMBEDDED"        "true"
         "QUEEN_KAFKA_ADDR"            (str ip ":9092")
         "QUEEN_KAFKA_ADVERTISED_ADDR" (str ip ":9092")})
      (:extra-env test)
      ; --slow-fsync-nodes: a slow disk on these nodes only (a test knob of
      ; the queue-log syncer), so they apply entries well before their fsync.
      (when (some #{node} (:slow-fsync-nodes test))
        {"QUEEN_TEST_FSYNC_DELAY_MS" (:slow-fsync-ms test)}))))

(defn- sh-quote
  [x]
  (str "'" (str/replace (str x) "'" "'\\''") "'"))

(defn run-sh-script
  [test node]
  (str "#!/usr/bin/env bash\n"
       "# Written by jepsen.queen.db. One Queen raft node; restarts the binary\n"
       "# ONLY on exit 75 (load a received snapshot). kill -9 (137) stays down.\n"
       "ulimit -n 1048576\n"
       (->> (env test node)
            (map (fn [[k v]] (str "export " k "=" (sh-quote v) "\n")))
            (apply str))
       "echo $$ > " wrapper-pid-file "\n"
       "while true; do\n"
       "  echo \"run.sh: starting queen $(date -u +%Y-%m-%dT%H:%M:%S.%NZ)\"\n"
       "  " bin " &\n"
       "  echo $! > " pid-file "\n"
       "  wait $!\n"
       "  rc=$?\n"
       "  echo \"run.sh: queen exited rc=$rc $(date -u +%Y-%m-%dT%H:%M:%S.%NZ)\"\n"
       "  [ $rc -eq 75 ] || break\n"
       "  echo \"run.sh: exit 75 = load a received snapshot; restarting\"\n"
       "done\n"))

(defn write-run-sh!
  "Writes this node's run.sh; `extra` env entries go on top of the test's
  (a node that rejoins empty gets QUEEN_RAFT_JOIN=true, and keeps it: the
  flag only matters to a node with an empty data directory)."
  ([test node]
   (write-run-sh! test node nil))
  ([test node extra]
   (c/su
     (cu/write-file! (run-sh-script (update test :extra-env merge extra) node) run-sh)
     (c/exec :chmod :+x run-sh))))

(def ^:private local-md5
  (memoize
    (fn [path]
      (let [md (MessageDigest/getInstance "MD5")
            buf (byte-array 1048576)]
        (with-open [in (FileInputStream. ^String path)]
          (loop []
            (let [n (.read in buf)]
              (when (pos? n)
                (.update md buf 0 n)
                (recur)))))
        (apply str (map #(format "%02x" %) (.digest md)))))))

(def bin-new
  "--nemesis upgrade: the release a node is upgraded to (--bin)."
  (str dir "/queen-new"))

(def bin-old
  "--nemesis upgrade: the release every node starts on (--bin-old)."
  (str dir "/queen-old"))

(defn upgrade-test?
  "Does this test upgrade its nodes while it runs (--nemesis upgrade)?"
  [test]
  (boolean (some #{:upgrade} (:nemesis test))))

(defn- upload-if-changed!
  "Uploads `local` to `remote` unless the node already has the same bytes;
  returns the md5."
  [local remote]
  (let [want (local-md5 local)
        have (try (first (str/split (c/exec :md5sum remote) #"\s+"))
                  (catch Exception _ nil))]
    (when-not (= want have)
      (info "uploading" local "to" remote "md5" want)
      (c/upload local remote)
      (c/exec :chmod :+x remote))
    want))

(defn install-binary!
  "Uploads --bin unless the node already has the same bytes. With --nemesis
  upgrade the node keeps both releases (queen-old from --bin-old, queen-new
  from --bin) and starts on the OLD one: the nemesis upgrades the nodes while
  the clients run."
  [test]
  (if (upgrade-test? test)
    (let [old (upload-if-changed! (or (:bin-old test)
                                      (throw (ex-info "--nemesis upgrade needs --bin-old" {})))
                                  bin-old)
          new (upload-if-changed! (:bin test) bin-new)]
      (c/exec :cp :-f bin-old bin)
      (c/exec :chmod :+x bin)
      (info "upgrade test: starts on" old "upgrades to" new)
      old)
    (upload-if-changed! (:bin test) bin)))

(defn running-release
  "Which release the bound node's binary is: :old or :new (by md5), or nil."
  []
  (let [md5 (fn [f] (try (first (str/split (c/su (c/exec :md5sum f)) #"\s+"))
                         (catch Exception _ nil)))
        cur (md5 bin)]
    (cond (nil? cur)          nil
          (= cur (md5 bin-new)) :new
          (= cur (md5 bin-old)) :old)))

(defn running?
  "Is a queen process alive on this node?"
  []
  (try (c/exec :pgrep :-x :queen) true
       (catch Exception _ false)))

(def seed-marker
  "--standby-seed: written on the standby's nodes when the failover nemesis
  seeds the standby; until then nothing starts them."
  (str dir "/seed-started"))

(defn mark-seed!
  []
  (c/su (c/exec :touch seed-marker)))

(defn- seed-started?
  []
  (try (c/exec :test :-e seed-marker) true
       (catch Exception _ false)))

(defn start-node!
  "Starts the wrapper unless queen already runs here. --standby-seed: a node
  of the standby stays down until the failover nemesis seeds the standby,
  whichever fault would start it before (a start of the kill nemesis, a
  restart, an upgrade)."
  [test node]
  (c/su
    (cond
      (running?)
      :already-running

      (and (:standby-seed test) (standby? test node) (not (seed-started?)))
      :waits-for-the-seed

      :else
      (do (c/exec :bash :-c (str "setsid nohup " run-sh " >> " log-file
                                 " 2>&1 < /dev/null &"))
          :started))))

(defn wipe-data!
  "Deletes everything under the data and buffer directories. The directories
  stay (a lazyfs data directory stays mounted)."
  []
  (c/su
    (c/exec :mkdir :-p data-dir buf-dir)
    (c/exec :find data-dir :-mindepth 1 :-delete)
    (c/exec :find buf-dir :-mindepth 1 :-delete)))

(defn lazyfs
  "The lazyfs map for this test's data directory, or nil without --lazyfs."
  [test]
  (when (:lazyfs test)
    (qlazyfs/lazyfs-map test data-dir)))

(defn kill-node!
  "kill -9 the wrapper, then every queen process. With --lazyfs the kill is a
  power loss: lazyfs then forgets every write the node had not fsynced."
  [test node]
  (c/su
    (meh (c/exec :bash :-c (str "test -f " wrapper-pid-file
                                " && kill -9 $(cat " wrapper-pid-file
                                ") 2>/dev/null; true")))
    (meh (c/exec :pkill :-9 :-x :queen))
    ; Wait until the processes are really gone (they may be stopped).
    (loop [i 0]
      (when (and (running?) (< i 50))
        (meh (c/exec :pkill :-9 :-x :queen))
        (Thread/sleep 100)
        (recur (inc i)))))
  (if-let [lfs (lazyfs test)]
    (if (qlazyfs/mounted? lfs)
      {:killed true, :power-loss (qlazyfs/power-loss! lfs)}
      :killed)
    :killed))

(def hog-file     "/opt/queen-hog")
(def hog-pid-file "/opt/queen-hog.pid")

(defn start-disk-hog!
  "--disk-hog: a background loop of synchronous writes to the same disk as
  the data directory, so every fsync of the node waits behind them: written
  queue-log groups wait longer for their fsync, widening the window in which
  a node has applied entries it has not fsynced yet."
  [test]
  (c/su
    (c/exec :bash :-c
            (str "setsid nohup bash -c 'while true; do dd if=/dev/zero of="
                 hog-file " bs=" (:disk-hog-bs test "256k") " count=256 oflag=dsync"
                 " 2>/dev/null; done' > /dev/null 2>&1 < /dev/null & echo $! > "
                 hog-pid-file))))

(defn stop-disk-hog!
  []
  (c/su
    (meh (c/exec :bash :-c (str "test -f " hog-pid-file " && kill -9 $(cat "
                                hog-pid-file ") 2>/dev/null; pkill -9 -x dd; true")))
    (c/exec :rm :-f hog-file hog-pid-file)))

(defn start-state-loop!
  "Every 2 s, this node's own raft view (POST /raft/v1/state: its voters) into
  raft-state.json, kept only when the node answered. Jepsen kills the nodes
  BEFORE it collects their logs, so the view has to be taken while they run:
  the file then holds the last one, for the membership agreement checker."
  [test node]
  (c/su
    (cu/write-file!
      (str "#!/usr/bin/env bash\n"
           "# Written by jepsen.queen.db: this node's raft view, while it answers.\n"
           "while true; do\n"
           "  if curl -s -m 2 -X POST -H 'x-queen-raft-token: " (raft-token test node) "'"
           " -o " state-file ".tmp -w '%{http_code}'"
           " http://" (cn/ip node) ":" raft-port "/raft/v1/state | grep -q '^200$'; then\n"
           "    mv -f " state-file ".tmp " state-file "\n"
           "  fi\n"
           "  sleep 2\n"
           "done\n")
      state-loop-sh)
    (c/exec :bash :-c (str "setsid nohup bash " state-loop-sh
                           " > /dev/null 2>&1 < /dev/null & echo $! > " state-loop-pid))))

(defn stop-state-loop!
  []
  (c/su
    (meh (c/exec :bash :-c (str "test -f " state-loop-pid " && kill -9 $(cat "
                                state-loop-pid ") 2>/dev/null; true")))
    (c/exec :rm :-f state-loop-pid state-loop-sh (str state-file ".tmp"))))

(declare await-healthy!)

(defn graceful-restart!
  "SIGTERM the queen binary (the wrapper then exits: it restarts only on 75),
  wait up to timeout-s for it to exit, start the node again and wait for
  /health. Returns what happened: the exit code the wrapper logged, how long
  the exit took, whether it had to be killed, how long until healthy."
  ([test node timeout-s]
   (graceful-restart! test node timeout-s nil))
  ([test node timeout-s before-start]
  (let [t0      (System/currentTimeMillis)
        lines   (try (parse-long (str/trim (c/su (c/exec :bash :-c (str "wc -l < " log-file)))))
                     (catch Exception _ 0))
        pid     (try (str/trim (c/su (c/exec :pgrep :-x :queen))) (catch Exception _ nil))]
    (if-not pid
      (do (when before-start (before-start))
          {:node node, :was-running false, :start (start-node! test node)})
      (do
        (c/su (c/exec :kill :-TERM (first (str/split-lines pid))))
        (let [exited? (loop []
                        (cond (not (running?)) true
                              (< (* 1000 timeout-s) (- (System/currentTimeMillis) t0)) false
                              :else (do (Thread/sleep 100) (recur))))
              exit-ms (- (System/currentTimeMillis) t0)
              _       (when-not exited? (kill-node! (dissoc test :lazyfs) node))
              rc      (try (->> (c/su (c/exec :bash :-c (str "tail -n +" (inc lines) " " log-file
                                                             " | grep -o 'queen exited rc=[0-9]*' | tail -1")))
                                (re-find #"rc=(\d+)") second parse-long)
                           (catch Exception _ nil))
              _       (when before-start (before-start))
              _       (start-node! test node)
              up?     (try (await-healthy! node 60000) true
                           (catch Exception _ false))]
          {:node node, :exit-ms exit-ms, :forced (not exited?), :rc rc
           :healthy-after-ms (when up? (- (System/currentTimeMillis) t0))}))))))

(defn swap-release!
  "--nemesis upgrade: restart the bound node on the other release, the way an
  operator rolls a cluster: SIGTERM (a leader hands leadership off first),
  wait for the exit, put `to` (:new or :old) in place, start, wait for
  /health. Returns graceful-restart!'s map with :from and :to."
  [test node to]
  (let [from (running-release)
        src  (case to :new bin-new :old bin-old)]
    (assoc (graceful-restart! test node 30
                              #(c/su (c/exec :cp :-f src bin)
                                     (c/exec :chmod :+x bin)))
           :from from
           :to   to)))

(defn await-healthy!
  "Waits until this node's /health answers 200 (a leader is known)."
  [node timeout-ms]
  (let [http (qh/client)]
    (util/await-fn
      (fn []
        (let [r (qh/request! http :get (str (qh/broker-url node) "/health")
                             nil 2000)]
          (when-not (= 200 (:status r))
            (throw (ex-info "not healthy" r)))
          (:body r)))
      {:timeout        timeout-ms
       :retry-interval 500
       :log-interval   10000
       :log-message    (str "waiting for " node " /health")})))

(def queue-options
  "Every test queue: no ttl, no retention, no DLQ, retries effectively
  unbounded, dedup longer than any test (--dedup-window shortens it), a
  short lease."
  {:leaseTime          5
   :retryLimit         1000000
   :retryDelay         0
   :ttl                0
   :deadLetterQueue    false
   :dlqAfterMaxRetries false
   :retentionEnabled   false
   :retentionSeconds   0
   :completedRetentionSeconds 0
   :dedupWindowSeconds 86400})

(defn queue-config
  "The options every test queue is configured with."
  [test]
  (cond-> (assoc queue-options :dedupWindowSeconds (:dedup-window test 86400))
    (:encrypt test) (assoc :encryptionEnabled true)))

(defn configure-queues!
  "POST /api/v1/configure for each test queue, via `node`; retried until 200."
  [test node]
  (let [http (qh/client)]
    (doseq [q (:queue-names test)]
      (util/await-fn
        (fn []
          (let [r (qh/request! http :post
                               (str (qh/base-url node) "/api/v1/configure")
                               {:queue q, :options (queue-config test)}
                               10000)]
            (when-not (= 200 (:status r))
              (throw (ex-info "configure failed" r)))
            (info "configured" q (:body r))
            r))
        {:timeout 60000, :retry-interval 1000, :log-interval 10000,
         :log-message (str "configuring " q)}))))

(defn link-status
  "GET /api/v1/system/link of a node: the parsed body on 200, else nil."
  [http node timeout-ms]
  (try (let [r (qh/request! http :get (str (qh/broker-url node) "/api/v1/system/link")
                            nil timeout-ms)]
         (when (and (= 200 (:status r)) (map? (:body r)))
           (:body r)))
       (catch clojure.lang.ExceptionInfo _ nil)))

(defn link-parts
  "Each raft group's part of a link status, with its :group: a node of
  several groups answers {role, groups: [{group, ...} ...]} (rsm/facade/
  groups.rs), a node of one group its one part, which is group 0's."
  [st]
  (cond (not (map? st))            []
        (sequential? (:groups st)) (vec (:groups st))
        :else                      [(assoc st :group 0)]))

(defn standby-leaders
  "{group [node part]}: for each raft group of the standby, the node that
  says it leads that group and the group's part of its link status. Each
  group has a leader of its own, and only that node's follower reads the
  source's group."
  [http test]
  (into (sorted-map)
        (for [[n st] (util/real-pmap (fn [n] [n (link-status http n 2000)])
                                     (standby-nodes test))
              part   (link-parts st)
              :when  (true? (:leader part))]
          [(:group part) [n part]])))

(defn await-following!
  "Waits until every raft group's leader in the standby reads the source and
  has nothing left to read. Returns {group [node part]}; throws after
  timeout-ms, having logged what every node of the standby says of the link
  and the lines of its log that say why (the test's teardown removes the
  logs before they are collected)."
  [test timeout-ms]
  (let [http   (qh/client)
        groups (range (long (:raft-groups test 1)))]
    (try
      (util/await-fn
        (fn []
          (let [leaders (standby-leaders http test)
                behind  (remove (fn [g]
                                  (let [part (second (get leaders g))
                                        f    (:follower part)]
                                    (and (= "standby" (:role part))
                                         (= "following" (:state f))
                                         (= 0 (:lagEntries f)))))
                                groups)]
            (when (seq behind)
              (throw (ex-info "the standby does not follow yet"
                              {:groups (vec behind), :leaders leaders})))
            (info "the standby follows its source:"
                  (into (sorted-map)
                        (map (fn [[g [n part]]]
                               [g [n (select-keys (:follower part) [:source :scanned :sourceApplied])]]))
                        leaders))
            leaders))
        {:timeout        timeout-ms
         :retry-interval 500
         :log-interval   10000
         :log-message    "waiting for the standby to follow its source"})
      (catch Exception e
        (doseq [n (standby-nodes test)]
          (warn "the standby does not follow:" n
                (pr-str (select-keys (link-status http n 2000)
                                     [:role :leader :position :follower]))
                (pr-str (select-keys (:raft (qh/health http n 2000))
                                     [:role :leader :term :applied :commit]))))
        (doseq [[n lines] (c/on-nodes
                            test (standby-nodes test)
                            (fn [_ _]
                              (try (c/su (c/exec :bash :-c
                                                 (str "grep -a 'rsm link\\|ERROR\\| WARN rsm\\|becomes leader\\|steps down'"
                                                      " " log-file " | cut -c1-400 | tail -25")))
                                   (catch Exception e (str "no log: " (.getMessage e))))))]
          (warn "the standby does not follow, log of" n "\n" lines))
        (throw e)))))

(defn disable-ntp!
  "The clock nemesis needs the node's clock left alone: on Ubuntu 24.04
  jepsen's maybe-disable-ntp! probes the wrong unit, so do it explicitly."
  []
  (c/su
    (meh (c/exec :timedatectl :set-ntp :false))
    (meh (c/exec :systemctl :disable :--now :systemd-timesyncd))))

(defn set-clock!
  "Steps this node's clock to true time, before its broker starts. With the
  time daemon off a node's clock runs free from one clock test, which resets
  it, to the next: 40 minutes after one, five nodes were 124 ms apart
  (-93 ms to +31 ms of true time), and three hours after one, two nodes of
  another set were 0.55 s apart. A lease survives a leader change only while
  the nodes' clocks agree within QUEEN_RAFT_MAX_CLOCK_SKEW_MS (500 ms), which
  is also the margin the lock checker allows: on that second set a permit
  went to its next owner 543 ms early when leadership moved to the node that
  was ahead, and W11b was judged invalid (x-sem-leader-deaf, 2026-10-09).

  ntpdate is what jepsen's clock nemesis resets the clocks with; a node that
  does not have it keeps its clock."
  []
  (c/su (meh (c/exec :ntpdate :-b "time.google.com"))))

(def ^:private shared-http
  (delay (qh/client)))

(def ^:private primaries-cache
  (atom {}))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (c/su
      (disable-ntp!)
      (set-clock!)
      (c/exec :mkdir :-p dir data-dir buf-dir proxy-spool)
      (let [md5 (install-binary! test)]
        (info node "queen md5" md5)))
    (write-run-sh! test node)
    ; QUEEN_RAFT_DIR on lazyfs: the queue logs, the LMDB store (lock.mdb is a
    ; shared writable mmap; it works on lazyfs), the raft state files.
    (when-let [lfs (lazyfs test)]
      (qlazyfs/install!)
      (qlazyfs/mount! lfs))
    ; Start every node together, on empty directories: each initializes the
    ; cluster with the same member list and they elect a leader.
    ; --standby-seed: not the standby's nodes, which the failover nemesis
    ; seeds once the source has a history.
    (jepsen/synchronize test)
    (when-not (and (:standby-seed test) (standby? test node))
      (start-node! test node)
      (await-healthy! node 120000))
    (start-state-loop! test node)
    (jepsen/synchronize test)
    ; The first node is always a node of the source: a standby takes no
    ; write, and gets the queues from the source's log.
    (when (= node (jepsen/primary test))
      (configure-queues! test node))
    (jepsen/synchronize test)
    ; --standby-nodes: the test begins on a standby that reads its source.
    ; 40 s: the others wait at the next barrier, which breaks after a minute.
    (when (and (= node (first (standby-nodes test))) (not (:standby-seed test)))
      (await-following! test 40000))
    (jepsen/synchronize test)
    (when (:disk-hog test)
      (start-disk-hog! test)))

  (teardown! [this test node]
    ; A test that was killed in the middle of a partition leaves its packet
    ; rules behind, and the next test's nodes then cannot form their clusters
    ; (the partition nemesis heals at ITS setup, which comes after the DB's).
    (c/su (meh (c/exec :iptables :-F :-w))
          (meh (c/exec :iptables :-X :-w)))
    (stop-disk-hog!)
    (stop-state-loop!)
    ; Always unmount a lazyfs left over from an earlier test, whatever this
    ; test's options: the data directory may be its mount point.
    (kill-node! (dissoc test :lazyfs) node)
    (qlazyfs/umount! (qlazyfs/lazyfs-map test data-dir))
    (c/su (c/exec :rm :-rf data-dir buf-dir log-file ls-file state-file
                  pid-file wrapper-pid-file proxy-spool seed-marker)))

  db/LogFiles
  (log-files [this test node]
    ; The listing waits up to 30 s for snapshot transfers to finish: a
    ; `snapshots/send-*` or `recv-*` directory still there after that is left
    ; over (the leftovers checker reads the listing).
    (meh (c/su (c/exec :bash :-c
                       (str "for i in 1 2 3 4 5 6; do "
                            "find " data-dir " -maxdepth 4 -path '*/snapshots/*' -type d "
                            "\\( -name 'send-*' -o -name 'recv-*' \\) | grep -q . || break; "
                            "sleep 5; done; "
                            "ls -laR " data-dir " > " ls-file " 2>&1; true"))))
    ; The final cache usage goes into lazyfs.log, for the lazyfs checker.
    (when-let [lfs (lazyfs test)]
      (when (qlazyfs/mounted? lfs)
        (meh (qlazyfs/usage! lfs))))
    (cond-> {log-file   "queen.log"
             ls-file    "data-ls.txt"
             state-file "raft-state.json"
             run-sh     "run.sh"}
      (lazyfs test) (assoc (:log-file (lazyfs test)) "lazyfs.log")
      ; W19: the SDK runners' JSON lines and logs.
      (= :feedstream (:workload test)) (merge (runners/log-files))))

  db/Process
  (start! [this test node]
    (start-node! test node))

  (kill! [this test node]
    (kill-node! test node))

  db/Pause
  (pause! [this test node]
    (c/su (meh (c/exec :pkill :-STOP :-x :queen)))
    :paused)

  (resume! [this test node]
    (c/su (meh (c/exec :pkill :-CONT :-x :queen)))
    :resumed)

  db/Primary
  (setup-primary! [this test node])

  (primaries [this test]
    ; The clock package resolves its node spec INSIDE its generator, which
    ; runs on the interpreter thread on every scheduling pass: answer from a
    ; one-second cache, over one shared client, or the whole test stalls.
    (let [{:keys [at nodes]} @primaries-cache
          now (System/nanoTime)]
      (if (and at (< (- now at) 1000000000))
        nodes
        (let [nodes (->> (:nodes test)
                         (util/real-pmap
                           (fn [n]
                             (when (= "leader"
                                      (get-in (qh/health @shared-http n 500)
                                              [:raft :role]))
                               n)))
                         (remove nil?)
                         vec)]
          (reset! primaries-cache {:at (System/nanoTime), :nodes nodes})
          nodes)))))

(defn db
  []
  (DB.))
