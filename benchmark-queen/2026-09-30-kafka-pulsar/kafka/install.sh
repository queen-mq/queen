#!/usr/bin/env bash
# install.sh (node) — JDK 21 headless (apt, only when java 21+ is missing) + Kafka 4.3.1, sha512-verified, unpacked to
# $REMOTE_ROOT/kafka/dist. Tarball from $CACHE if present there, else downloads.apache.org (archive.apache.org as a
# fallback once 4.3.1 is superseded). Idempotent: a dist that already holds the verified version is left alone.
# Env (normally from $REMOTE_ROOT/hosts.env via common/deploy.sh): CACHE, REMOTE_ROOT. KAFKA_VERSION (4.3.1), SCALA (2.13).
# Niced + ionice'd: it may run on a box that is still busy with something else.
set -u
main() {
  local D; D=$(cd "$(dirname "$0")/.." && pwd)
  [ -f "$D/hosts.env" ] && . "$D/hosts.env"
  local V=${KAFKA_VERSION:-4.3.1} S=${SCALA:-2.13}
  local T=kafka_$S-$V.tgz K=$D/kafka
  local DIST=$K/dist DL=$K/download
  # The published sha512 of kafka_2.13-4.3.1.tgz (downloads.apache.org, checked on 09-24 against three droplets). It is
  # the fallback when neither $CACHE nor the network has the .sha512; when both exist they must agree.
  local PINNED=c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c
  log() { echo "[$(date -u +%FT%TZ)] install $(hostname): $*"; }
  export DEBIAN_FRONTEND=noninteractive
  mkdir -p "$K" "$DL"

  # 1. JDK 21 (Kafka 4.x needs 17+ for the broker; the SPEC runs every JVM on 21). jstack comes with the -jdk- package.
  local jv; jv=$(java -version 2>&1 | awk -F'"' '/version/{split($2, a, "."); print a[1]; exit}')
  if [ -z "$jv" ] || [ "$jv" -lt 21 ] 2>/dev/null || ! command -v jstack >/dev/null 2>&1; then
    log "java ${jv:-missing} (jstack $(command -v jstack >/dev/null && echo ok || echo missing)): apt install openjdk-21-jdk-headless"
    nice -n 19 ionice -c3 apt-get -o DPkg::Lock::Timeout=600 update -qq > "$K/apt-install.log" 2>&1
    nice -n 19 ionice -c3 apt-get -o DPkg::Lock::Timeout=600 install -y -qq --no-install-recommends \
      openjdk-21-jdk-headless python3 >> "$K/apt-install.log" 2>&1 || { log "apt failed:"; tail -5 "$K/apt-install.log"; return 1; }
  fi
  log "$(java -version 2>&1 | head -1)"

  # 2. already installed?
  if [ -f "$DIST/.installed" ] && grep -qx "version=$V" "$DIST/.installed" && [ -x "$DIST/bin/kafka-server-start.sh" ]; then
    log "Kafka $V already in $DIST ($(grep '^sha512=' "$DIST/.installed" | cut -c8-23)...), nothing to do"; return 0
  fi

  # 3. tarball: cache first, then the mirrors
  local TGZ="" base
  if [ -n "${CACHE:-}" ] && [ -s "$CACHE/$T" ]; then TGZ=$CACHE/$T; log "tarball from cache $TGZ"
  else
    TGZ=$DL/$T
    if [ ! -s "$TGZ" ]; then
      for base in https://downloads.apache.org/kafka/$V https://archive.apache.org/dist/kafka/$V; do
        log "downloading $base/$T"
        nice -n 19 ionice -c3 curl -fsS --retry 3 --connect-timeout 20 -o "$TGZ.part" "$base/$T" && mv "$TGZ.part" "$TGZ" && break
        rm -f "$TGZ.part"
      done
    fi
    [ -s "$TGZ" ] || { log "no tarball (cache '${CACHE:-}' empty, mirrors unreachable)"; return 1; }
  fi

  # 4. sha512: published value from the cache or the mirrors (format "file: C7D7B231 8CB51AA0 ..."), else the pinned one
  local PUB="" f
  norm() { cut -d: -f2- | tr -d ' \n\r\t' | tr 'A-F' 'a-f'; }
  for f in "${CACHE:-/nonexistent}/$T.sha512" "$DL/$T.sha512"; do [ -s "$f" ] && { PUB=$(norm < "$f"); break; }; done
  if [ -z "$PUB" ]; then
    for base in https://downloads.apache.org/kafka/$V https://archive.apache.org/dist/kafka/$V; do
      curl -fsS --retry 2 --connect-timeout 10 -o "$DL/$T.sha512" "$base/$T.sha512" 2>/dev/null && { PUB=$(norm < "$DL/$T.sha512"); break; }
    done
  fi
  if [ -n "$PUB" ] && [ "$V" = 4.3.1 ] && [ "$PUB" != "$PINNED" ]; then log "published sha512 differs from the pinned one: refusing"; return 1; fi
  [ -n "$PUB" ] || { [ "$V" = 4.3.1 ] && PUB=$PINNED && log "no published .sha512 reachable: using the pinned value"; }
  [ -n "$PUB" ] || { log "no sha512 to verify $T against"; return 1; }
  local GOT; GOT=$(nice -n 19 ionice -c3 sha512sum "$TGZ" | cut -d' ' -f1)
  log "published sha512: $PUB"
  log "computed  sha512: $GOT"
  [ "$PUB" = "$GOT" ] || { log "SHA512 MISMATCH on $TGZ"; return 1; }
  log "SHA512 OK ($(stat -c %s "$TGZ") bytes)"

  # 5. unpack into a fresh dir, then swap it in (never half a dist in place)
  rm -rf "$DIST.new" && mkdir -p "$DIST.new"
  nice -n 19 ionice -c3 tar -xzf "$TGZ" -C "$DIST.new" --strip-components=1 --no-same-owner || { log "untar failed"; return 1; }
  printf 'version=%s\nscala=%s\nsha512=%s\ninstalled=%s\n' "$V" "$S" "$GOT" "$(date -u +%FT%TZ)" > "$DIST.new/.installed"
  rm -rf "$DIST.old"; [ -d "$DIST" ] && mv "$DIST" "$DIST.old"
  mv "$DIST.new" "$DIST" && rm -rf "$DIST.old"
  log "Kafka $V installed in $DIST: $(ls "$DIST/libs" | grep -E "^kafka_$S-$V\.jar$")"
}
main "$@"; exit $?
