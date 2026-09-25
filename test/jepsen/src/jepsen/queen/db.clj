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
(def pid-file         (str dir "/queen.pid"))
(def wrapper-pid-file (str dir "/wrapper.pid"))
(def raft-port        7400)

(defn node-id
  "Raft node ids are 1-based positions in the test's node list."
  [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn peers
  "QUEEN_RAFT_PEERS: id=raft_addr/http_addr for every node, private IPs."
  [test]
  (->> (:nodes test)
       (map (fn [n]
              (let [ip (cn/ip n)]
                (str (node-id test n) "=" ip ":" raft-port "/" ip ":"
                     qh/http-port))))
       (str/join ",")))

(defn env
  "The node's environment: the benchmark harness's (qc.sh) plus test knobs."
  [test node]
  (let [ip (cn/ip node)]
    (merge
      (sorted-map
        "QUEEN_STORAGE"                 "raft"
        "QUEEN_RAFT_REPLICATOR"         "openraft"
        "QUEEN_RAFT_NODE_ID"            (node-id test node)
        "QUEEN_RAFT_PEERS"              (peers test)
        "QUEEN_RAFT_LISTEN"             (str ip ":" raft-port)
        "QUEEN_RAFT_TOKEN"              (:raft-token test)
        "QUEEN_RAFT_DIR"                data-dir
        "QUEEN_RAFT_DEDUP_INDEX"        (:dedup-index test)
        "QUEEN_RAFT_POP_FASTPATH_EMPTY" "1"
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
      (:extra-env test))))

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

(defn disable-ntp!
  "The clock nemesis needs the node's clock left alone: on Ubuntu 24.04
  jepsen's maybe-disable-ntp! probes the wrong unit, so do it explicitly."
  []
  (c/su
    (meh (c/exec :timedatectl :set-ntp :false))
    (meh (c/exec :systemctl :disable :--now :systemd-timesyncd))))

(def ^:private shared-http
  (delay (qh/client)))

(def ^:private primaries-cache
  (atom {}))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (c/su
      (disable-ntp!)
      (c/exec :mkdir :-p dir data-dir buf-dir)
      (let [md5 (install-binary! test)]
        (info node "queen md5" md5))
      (cu/write-file! (run-sh-script test node) run-sh)
      (c/exec :chmod :+x run-sh))
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
    (jepsen/synchronize test)
    (when (= node (jepsen/primary test))
      (configure-queues! test node))
    (jepsen/synchronize test)
    (when (:disk-hog test)
      (start-disk-hog! test)))

  (teardown! [this test node]
    (stop-disk-hog!)
    ; Always unmount a lazyfs left over from an earlier test, whatever this
    ; test's options: the data directory may be its mount point.
    (kill-node! (dissoc test :lazyfs) node)
    (qlazyfs/umount! (qlazyfs/lazyfs-map test data-dir))
    (c/su (c/exec :rm :-rf data-dir buf-dir log-file ls-file pid-file
                  wrapper-pid-file)))

  db/LogFiles
  (log-files [this test node]
    (meh (c/su (c/exec :bash :-c (str "ls -laR " data-dir " > " ls-file
                                      " 2>&1; true"))))
    ; The final cache usage goes into lazyfs.log, for the lazyfs checker.
    (when-let [lfs (lazyfs test)]
      (when (qlazyfs/mounted? lfs)
        (meh (qlazyfs/usage! lfs))))
    (cond-> {log-file "queen.log"
             ls-file  "data-ls.txt"
             run-sh   "run.sh"}
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
