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
            [jepsen.queen.lazyfs :as qlazyfs])
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
  of the source and become a standby at their first election."
  [test node]
  (cond
    (empty? (standby-nodes test)) {}
    (standby? test node)
    {"QUEEN_LINK_SOURCE"       (->> (source-nodes test)
                                    (map #(str (cn/ip %) ":" raft-port))
                                    (str/join ","))
     "QUEEN_LINK_SOURCE_TOKEN" (:link-token test)
     "QUEEN_LINK_STANDBY"      "true"}
    :else
    {"QUEEN_LINK_TOKEN" (:link-token test)}))

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

(defn install-binary!
  "Uploads --bin unless the node already has the same bytes."
  [test]
  (let [local (:bin test)
        want  (local-md5 local)
        have  (try (first (str/split (c/exec :md5sum bin) #"\s+"))
                   (catch Exception _ nil))]
    (when-not (= want have)
      (info "uploading" local "md5" want)
      (c/upload local bin)
      (c/exec :chmod :+x bin))
    want))

(defn running?
  "Is a queen process alive on this node?"
  []
  (try (c/exec :pgrep :-x :queen) true
       (catch Exception _ false)))

(defn start-node!
  "Starts the wrapper unless queen already runs here."
  [test node]
  (c/su
    (if (running?)
      :already-running
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
  [test node timeout-s]
  (let [t0      (System/currentTimeMillis)
        lines   (try (parse-long (str/trim (c/su (c/exec :bash :-c (str "wc -l < " log-file)))))
                     (catch Exception _ 0))
        pid     (try (str/trim (c/su (c/exec :pgrep :-x :queen))) (catch Exception _ nil))]
    (if-not pid
      {:node node, :was-running false, :start (start-node! test node)}
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
              _       (start-node! test node)
              up?     (try (await-healthy! node 60000) true
                           (catch Exception _ false))]
          {:node node, :exit-ms exit-ms, :forced (not exited?), :rc rc
           :healthy-after-ms (when up? (- (System/currentTimeMillis) t0))})))))

(defn await-healthy!
  "Waits until this node's /health answers 200 (a leader is known)."
  [node timeout-ms]
  (let [http (qh/client)]
    (util/await-fn
      (fn []
        (let [r (qh/request! http :get (str (qh/base-url node) "/health")
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
  unbounded, dedup longer than any test, a short lease."
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

(defn configure-queues!
  "POST /api/v1/configure for each test queue, via `node`; retried until 200."
  [test node]
  (let [http (qh/client)]
    (doseq [q (:queue-names test)]
      (util/await-fn
        (fn []
          (let [r (qh/request! http :post
                               (str (qh/base-url node) "/api/v1/configure")
                               {:queue q, :options queue-options} 10000)]
            (when-not (= 200 (:status r))
              (throw (ex-info "configure failed" r)))
            (info "configured" q (:body r))
            r))
        {:timeout 60000, :retry-interval 1000, :log-interval 10000,
         :log-message (str "configuring " q)}))))

(defn link-status
  "GET /api/v1/system/link of a node: the parsed body on 200, else nil."
  [http node timeout-ms]
  (try (let [r (qh/request! http :get (str (qh/base-url node) "/api/v1/system/link")
                            nil timeout-ms)]
         (when (and (= 200 (:status r)) (map? (:body r)))
           (:body r)))
       (catch clojure.lang.ExceptionInfo _ nil)))

(defn standby-leader-status
  "The link status of the standby's leader, as [node status], or nil when no
  node of the standby says it leads."
  [http test]
  (->> (standby-nodes test)
       (util/real-pmap (fn [n] [n (link-status http n 2000)]))
       (filter (fn [[_ st]] (true? (:leader st))))
       first))

(defn await-following!
  "Waits until the standby's leader reads the source and has nothing left to
  read. Returns its status; throws after timeout-ms, having logged what every
  node of the standby says of the link and the lines of its log that say why
  (the test's teardown removes the logs before they are collected)."
  [test timeout-ms]
  (let [http (qh/client)]
    (try
      (util/await-fn
        (fn []
          (let [[node st] (standby-leader-status http test)
                f         (:follower st)]
            (when-not (and (= "standby" (:role st))
                           (= "following" (:state f))
                           (= 0 (:lagEntries f)))
              (throw (ex-info "the standby does not follow yet" {:node node, :status st})))
            (info "the standby follows its source:" node (select-keys f [:source :scanned :sourceApplied]))
            st))
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
      (c/exec :mkdir :-p dir data-dir buf-dir)
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
    (jepsen/synchronize test)
    (start-node! test node)
    (await-healthy! node 120000)
    (start-state-loop! test node)
    (jepsen/synchronize test)
    ; The first node is always a node of the source: a standby takes no
    ; write, and gets the queues from the source's log.
    (when (= node (jepsen/primary test))
      (configure-queues! test node))
    (jepsen/synchronize test)
    ; --standby-nodes: the test begins on a standby that reads its source.
    ; 40 s: the others wait at the next barrier, which breaks after a minute.
    (when (= node (first (standby-nodes test)))
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
                  pid-file wrapper-pid-file)))

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
      (lazyfs test) (assoc (:log-file (lazyfs test)) "lazyfs.log")))

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
