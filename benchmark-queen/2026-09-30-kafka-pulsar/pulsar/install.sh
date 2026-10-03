#!/usr/bin/env bash
# install.sh (node) — JDK 21 headless (apt, only when java 21+ or jstack is missing) + the Apache Pulsar 4.2.4 binary
# distribution, sha512-verified, unpacked to $REMOTE_ROOT/pulsar/dist (= PULSAR_HOME for pc.sh / mkconf.sh).
# Tarball from $CACHE if present there, else downloads.apache.org (archive.apache.org once 4.2.4 is superseded).
# The .sha512 format is "<hash>  ./apache-pulsar-4.2.4-bin.tar.gz"; the published value is pinned below as the fallback
# (and must agree when a .sha512 is reachable). Idempotent: a dist that already holds the verified version is left alone.
# Env (normally from $REMOTE_ROOT/hosts.env via common/deploy.sh): CACHE, REMOTE_ROOT. PULSAR_VERSION (4.2.4).
# Niced + ionice'd: it may run on a box that is still busy with something else.
set -u
main() {
  local D; D=$(cd "$(dirname "$0")/.." && pwd)
  [ -f "$D/hosts.env" ] && . "$D/hosts.env"
  local V=${PULSAR_VERSION:-4.2.4}
  local T=apache-pulsar-$V-bin.tar.gz P=$D/pulsar
  local DIST=$P/dist DL=$P/download
  # sha512 of apache-pulsar-4.2.4-bin.tar.gz as published on downloads.apache.org (the cache's .sha512, fetched 09-30)
  local PINNED=8c0a63cc6421c8eb3c4a55b8895ca08a38a1c6771758251fc7a82edbc4c6a42bb8ba5c956066b637ec7e049149d467c034e854b571a4cb000f42a47ae6105971
  log() { echo "[$(date -u +%FT%TZ)] install $(hostname): $*"; }
  export DEBIAN_FRONTEND=noninteractive
  mkdir -p "$P" "$DL"

  # 1. JDK 21 (Pulsar 4.2 needs 17+; the SPEC runs every JVM on 21, where Pulsar's default GC is generational ZGC).
  #    jstack (pc.sh threads) comes with the -jdk- package; python3 runs pbalance.py and the stats parser.
  local jv; jv=$(java -version 2>&1 | awk -F'"' '/version/{split($2, a, "."); print a[1]; exit}')
  if [ -z "$jv" ] || [ "$jv" -lt 21 ] 2>/dev/null || ! command -v jstack >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
    log "java ${jv:-missing} (jstack $(command -v jstack >/dev/null && echo ok || echo missing)): apt install openjdk-21-jdk-headless"
    nice -n 19 ionice -c3 apt-get -o DPkg::Lock::Timeout=600 update -qq > "$P/apt-install.log" 2>&1
    nice -n 19 ionice -c3 apt-get -o DPkg::Lock::Timeout=600 install -y -qq --no-install-recommends \
      openjdk-21-jdk-headless python3 curl >> "$P/apt-install.log" 2>&1 || { log "apt failed:"; tail -5 "$P/apt-install.log"; return 1; }
  fi
  local J21; J21=$(ls -d /usr/lib/jvm/java-21-openjdk-* 2>/dev/null | head -1)
  log "$(java -version 2>&1 | head -1)${J21:+ (JDK 21 at $J21)}"

  # 2. already installed?
  if [ -f "$DIST/.installed" ] && grep -qx "version=$V" "$DIST/.installed" && [ -x "$DIST/bin/pulsar" ]; then
    log "Pulsar $V already in $DIST ($(grep '^sha512=' "$DIST/.installed" | cut -c8-23)...), nothing to do"; return 0
  fi

  # 3. tarball: cache first, then the mirrors
  local TGZ="" base
  if [ -n "${CACHE:-}" ] && [ -s "$CACHE/$T" ]; then TGZ=$CACHE/$T; log "tarball from cache $TGZ"
  else
    TGZ=$DL/$T
    if [ ! -s "$TGZ" ]; then
      for base in https://downloads.apache.org/pulsar/pulsar-$V https://archive.apache.org/dist/pulsar/pulsar-$V; do
        log "downloading $base/$T"
        nice -n 19 ionice -c3 curl -fsS --retry 3 --connect-timeout 20 -o "$TGZ.part" "$base/$T" && mv "$TGZ.part" "$TGZ" && break
        rm -f "$TGZ.part"
      done
    fi
    [ -s "$TGZ" ] || { log "no tarball (cache '${CACHE:-}' empty, mirrors unreachable)"; return 1; }
  fi

  # 4. sha512: the published value (cache or mirrors, "<hash>  ./<file>"), else the pinned one; both must agree
  local PUB="" f
  for f in "${CACHE:-/nonexistent}/$T.sha512" "$DL/$T.sha512"; do
    [ -s "$f" ] && { PUB=$(awk 'NF{print tolower($1); exit}' "$f"); log "published sha512 from $f"; break; }
  done
  if [ -z "$PUB" ]; then
    for base in https://downloads.apache.org/pulsar/pulsar-$V https://archive.apache.org/dist/pulsar/pulsar-$V; do
      curl -fsS --retry 2 --connect-timeout 10 -o "$DL/$T.sha512" "$base/$T.sha512" 2>/dev/null \
        && { PUB=$(awk 'NF{print tolower($1); exit}' "$DL/$T.sha512"); log "published sha512 from $base"; break; }
    done
  fi
  if [ -n "$PUB" ] && [ "$V" = 4.2.4 ] && [ "$PUB" != "$PINNED" ]; then log "published sha512 differs from the pinned one: refusing"; return 1; fi
  [ -n "$PUB" ] || { [ "$V" = 4.2.4 ] && PUB=$PINNED && log "no published .sha512 reachable: using the pinned value"; }
  [ -n "$PUB" ] || { log "no sha512 to verify $T against"; return 1; }
  local GOT; GOT=$(nice -n 19 ionice -c3 sha512sum "$TGZ" | cut -d' ' -f1)
  log "published sha512: $PUB"
  log "computed  sha512: $GOT"
  [ "$PUB" = "$GOT" ] || { log "SHA512 MISMATCH on $TGZ"; return 1; }
  log "SHA512 OK ($(stat -c %s "$TGZ") bytes)"

  # 5. unpack into a fresh dir, then swap it in (never half a dist in place)
  rm -rf "$DIST.new" && mkdir -p "$DIST.new"
  nice -n 19 ionice -c3 tar -xzf "$TGZ" -C "$DIST.new" --strip-components=1 --no-same-owner || { log "untar failed"; return 1; }
  printf 'version=%s\nsha512=%s\ninstalled=%s\n' "$V" "$GOT" "$(date -u +%FT%TZ)" > "$DIST.new/.installed"
  rm -rf "$DIST.old"; [ -d "$DIST" ] && mv "$DIST" "$DIST.old"
  mv "$DIST.new" "$DIST" && rm -rf "$DIST.old"
  log "Pulsar $V installed in $DIST ($(du -sm "$DIST" | cut -f1) MB): $(ls "$DIST/lib" | grep -E '^org\.apache\.(pulsar-pulsar-broker|bookkeeper-bookkeeper-server|zookeeper-zookeeper-jute)-[0-9]' | tr '\n' ' ')"
}
main "$@"; exit $?
