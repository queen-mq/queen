(ns jepsen.queen.runners
  "Feed-stream runners (the SDKs' Stream.fromFeed, through the cross-SDK
  conformance runners in clients/conformance/feed) as processes on the Jepsen
  nodes, so a test can kill and pause them like the brokers.

  A bundle built on the control node by test/jepsen/rig/build-runners.sh
  (--runner-bundle, default /root/runners.tgz) holds node, the JS SDK and its
  runner, the Python SDK with its dependencies, and the Go and Rust runners
  built for linux. It is unpacked to /opt/queen-runners/bundle on each node.

  A runner instance is one SDK running one case (a query) on one node, talking
  to that node's broker. Its stdin is a FIFO held open by the runner's own
  shell (`exec 3<>fifo`), so `echo stop > fifo` stops it gracefully and
  nothing else ever closes it; its stdout (the runner's JSON lines) and stderr
  go to files the test collects."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [util :as util :refer [meh]]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.queen.http :as qh])
  (:import (java.io FileInputStream)
           (java.security MessageDigest)))

(defn- md5
  [^String file]
  (let [md  (MessageDigest/getInstance "MD5")
        buf (byte-array 1048576)]
    (with-open [in (FileInputStream. file)]
      (loop []
        (let [n (.read in buf)]
          (when (pos? n) (.update md buf 0 n) (recur)))))
    (apply str (map #(format "%02x" %) (.digest md)))))

(def dir     "/opt/queen-runners")
(def bundle  (str dir "/bundle"))
(def run-dir (str dir "/run"))
(def cases   (str dir "/cases"))

(def sdks #{:js :python :go :rust})

(defn command
  "The runner command line of an SDK, from the unpacked bundle."
  [sdk]
  (case sdk
    :js     [(str bundle "/node/bin/node")
             (str bundle "/clients/conformance/feed/runners/js.mjs")]
    :python ["env" (str "PYTHONPATH=" bundle "/py-deps:" bundle "/clients/client-py")
             "python3" (str bundle "/clients/client-py/tests/feed_conformance/runner.py")]
    :go     [(str bundle "/go-runner")]
    :rust   [(str bundle "/rust-runner")]))

(defn id
  "An instance's name: <query>-<sdk>."
  [query sdk]
  (str query "-" (name sdk)))

(defn- path [id ext] (str run-dir "/" id "." ext))

(defn install!
  "On the bound node: unpacks the bundle (uploaded unless the node has the
  same bytes), makes the run and case directories."
  [test]
  (c/su
    (c/exec :mkdir :-p dir run-dir cases)
    (let [local (:runner-bundle test "/root/runners.tgz")
          want  (md5 local)
          have  (try (str/trim (c/exec :cat (str dir "/bundle.md5"))) (catch Exception _ nil))]
      (when-not (= want have)
        (info "uploading runner bundle" local)
        (c/upload local (str dir "/bundle.tgz"))
        (c/exec :rm :-rf bundle)
        (c/exec :mkdir :-p bundle)
        (c/exec :tar :-xzf (str dir "/bundle.tgz") :-C bundle)
        (cu/write-file! want (str dir "/bundle.md5"))))))

(defn write-case!
  "On the bound node: the case file of a query (README.md section 2:
  source, chain, run)."
  [query case-map]
  (c/su (cu/write-file! (json/write-str case-map) (str cases "/" query ".json"))))

(defn running?
  "Is this instance's process alive on the bound node?"
  [iid]
  (try (c/su (c/exec :bash :-c (str "kill -0 $(cat " (path iid "pid") ") 2>/dev/null")))
       true
       (catch Exception _ false)))

(defn start!
  "On the bound node: starts `sdk` running `query` against this node's
  broker, unless it already runs."
  [test node query sdk]
  (let [iid (id query sdk)
        url (str "http://" (cn/ip node) ":" qh/http-port)
        cmd (concat (command sdk)
                    ["run" "--url" url "--case" (str cases "/" query ".json")
                     "--runner" (str (name sdk) "@" node)])]
    (if (running? iid)
      :already-running
      (c/su
        (c/exec :bash :-c (str "test -p " (path iid "in") " || mkfifo " (path iid "in")))
        (c/exec :bash :-c
                (str "setsid nohup bash -c '"
                     "echo $$ > " (path iid "pid") "; "
                     "exec 3<>" (path iid "in") "; "
                     "exec " (str/join " " (map #(str "\"" % "\"") cmd))
                     " <&3 >> " (path iid "out") " 2>> " (path iid "err")
                     "' > /dev/null 2>&1 < /dev/null &"))
        :started))))

(defn signal!
  "Sends a signal (:KILL, :STOP, :CONT) to an instance on the bound node."
  [iid sig]
  (c/su (meh (c/exec :bash :-c (str "kill -" (name sig) " $(cat " (path iid "pid") ") 2>/dev/null; true")))))

(defn- tell!
  "Writes one command line to an instance's stdin, only while it runs: a
  write to a FIFO nobody reads would block for ever. `timeout 2` bounds it
  should the process die between the check and the write."
  [iid line]
  (when (running? iid)
    (c/su (meh (c/exec :bash :-c (str "timeout 2 bash -c 'echo " line " > " (path iid "in") "'"))))
    true))

(defn stop!
  "A graceful stop: `stop` on its stdin."
  [iid]
  (tell! iid "stop"))

(defn role
  "Asks an instance for its metrics and answers its role (\"active\",
  \"standby\", ...) from the last metrics line, or nil."
  [iid]
  (when (tell! iid "metrics")
    (try
      (c/su
        (Thread/sleep 400)
        (let [lines (c/exec :bash :-c (str "tail -n 50 " (path iid "out") " | grep '\"event\":\"metrics\"' | tail -n 1"))]
          (when-not (str/blank? lines)
            (get-in (json/read-str lines :key-fn keyword) [:metrics :role]))))
      (catch Exception _ nil))))

(defn kill-all!
  "On the bound node: every runner instance, kill -9."
  []
  (c/su (meh (c/exec :bash :-c (str "for f in " run-dir "/*.pid; do [ -f \"$f\" ] && kill -9 $(cat \"$f\") 2>/dev/null; done; true")))))

(defn wipe!
  "On the bound node: the run directory (pids, FIFOs, outputs)."
  []
  (c/su (meh (c/exec :rm :-rf run-dir))
        (c/exec :mkdir :-p run-dir)))

(defn log-files
  "{remote local} of the runner outputs on the bound node, for the store."
  []
  (let [fs (try (str/split-lines (c/su (c/exec :bash :-c (str "ls " run-dir "/*.out " run-dir "/*.err 2>/dev/null; true"))))
                (catch Exception _ []))]
    (into {} (for [f fs :when (not (str/blank? f))]
               [f (str "runners/" (last (str/split f #"/")))]))))
