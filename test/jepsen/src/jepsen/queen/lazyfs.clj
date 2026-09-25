(ns jepsen.queen.lazyfs
  "lazyfs on Ubuntu 24.04. jepsen.lazyfs/install! wants libfuse3-4 (a Debian 13
  package); noble ships fuse3 3.14 with libfuse3-3, which libfuse3-dev pulls
  in. So this installs and builds lazyfs itself, once per node, and reuses
  jepsen.lazyfs for the lazyfs map, mount! and the fifo commands.

  A power loss here is: the process is already dead (kill -9), then lazyfs
  forgets every write not yet fsynced. Before the forget it logs the cache
  usage and which files hold un-fsynced bytes, so a run can show the fault had
  teeth (a FULL cache writes straight through and makes it toothless)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def built-marker
  (str lazyfs/dir "/.queen-built"))

(defn install!
  "Installs and builds lazyfs `lazyfs/commit` on the bound node, unless this
  commit is already built there."
  []
  (c/su
    (let [built (try (str/trim (c/exec :cat built-marker))
                     (catch Exception _ nil))]
      (when-not (and (= built lazyfs/commit) (cu/exists? lazyfs/bin))
        (info "Installing lazyfs" lazyfs/commit)
        (debian/install [:g++ :cmake :libfuse3-dev :fuse3 :git])
        (when-not (cu/exists? lazyfs/dir)
          (c/exec :mkdir :-p (str/replace lazyfs/dir #"/[^/]+$" ""))
          (c/exec :git :clone :-q lazyfs/repo-url lazyfs/dir))
        (c/cd lazyfs/dir
              (c/exec :git :fetch :-q)
              (c/exec :git :checkout :-q lazyfs/commit)
              (c/exec :git :clean :-fxq)
              (c/cd "libs/libpcache" (c/exec "./build.sh"))
              (c/cd "lazyfs" (c/exec "./build.sh")))
        (c/exec :echo lazyfs/commit :> built-marker)))
    (c/exec :sed :-i "/\\s*user_allow_other/s/^#//g" "/etc/fuse.conf")))

(defn lazyfs-map
  "The lazyfs map for Queen's data directory."
  [test dir]
  (lazyfs/lazyfs {:dir dir, :cache-size (:lazyfs-cache test "1GB")}))

(defn mounted?
  [lfs]
  (try (c/su (c/exec :mountpoint :-q (:dir lfs))) true
       (catch Exception _ false)))

(defn mount!
  [lfs]
  (lazyfs/mount! lfs))

(defn umount!
  "Unmounts (if mounted) and removes the lazyfs directories."
  [lfs]
  (c/su
    (when (mounted? lfs)
      (meh (c/exec :fusermount :-uz (:dir lfs))))
    (c/exec :rm :-rf (:lazyfs-dir lfs))))

(defn- log-count
  [lfs pattern]
  (try (parse-long (str/trim (c/su (c/exec :grep :-c pattern (:log-file lfs)))))
       (catch Exception _ 0)))

(defn- log-since-last
  "The lazyfs log from the last line matching pattern to its end."
  [lfs pattern]
  (try (str/split-lines
         (c/su (c/exec :awk (str "/" pattern "/{n=NR} {l[NR]=$0} "
                                 "END{if (n) for(i=n;i<=NR;i++) print l[i]}")
                       (:log-file lfs))))
       (catch Exception _ [])))

(defn power-loss!
  "Forgets the un-fsynced writes of the bound node's lazyfs, and waits until
  lazyfs says the cache is cleared. Call it with the process already dead.
  Returns what it logged: the cache usage, the un-fsynced byte total and the
  files that held un-fsynced bytes."
  [lfs]
  (let [cleared  (log-count lfs "cache is cleared")
        reported (log-count lfs "report generated")]
    (lazyfs/fifo! lfs "lazyfs::display-cache-usage")
    (lazyfs/fifo! lfs "lazyfs::unsynced-data-report")
    (util/await-fn (fn [] (or (< reported (log-count lfs "report generated"))
                              (throw (ex-info "no report yet" {}))))
                   {:timeout 10000, :retry-interval 100, :log-interval 5000
                    :log-message "waiting for the lazyfs unsynced report"})
    (let [lines (log-since-last lfs "cache usage")
          usage (some->> lines
                         (some #(re-find #"cache usage .* is ([0-9.]+)%" %))
                         second
                         parse-double)
          ; lazyfs prints a running total after each inode: the last one is
          ; the report's total.
          total (some->> lines
                         (keep #(re-find #"un-fsynced: (\d+) bytes" %))
                         last
                         second
                         parse-long)
          files (->> lines
                     (keep #(second (re-find #"=> file: '([^']+)'" %)))
                     distinct
                     vec)]
      (lazyfs/fifo! lfs "lazyfs::clear-cache")
      (util/await-fn (fn [] (or (< cleared (log-count lfs "cache is cleared"))
                                (throw (ex-info "not cleared yet" {}))))
                     {:timeout 10000, :retry-interval 100, :log-interval 5000
                      :log-message "waiting for lazyfs to clear its cache"})
      {:cache-usage-pct usage
       :unsynced-bytes  (or total 0)
       :unsynced-files  files})))

(defn usage!
  "Logs and returns the bound node's lazyfs cache usage (percent of pages)."
  [lfs]
  (let [n (log-count lfs "cache usage")]
    (lazyfs/fifo! lfs "lazyfs::display-cache-usage")
    (util/await-fn (fn [] (or (< n (log-count lfs "cache usage"))
                              (throw (ex-info "no usage yet" {}))))
                   {:timeout 10000, :retry-interval 100, :log-interval 5000
                    :log-message "waiting for the lazyfs cache usage"})
    (some->> (log-since-last lfs "cache usage")
             (some #(re-find #"cache usage .* is ([0-9.]+)%" %))
             second
             parse-double)))
