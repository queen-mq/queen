#!/usr/bin/env bash
#
# queen-kafka compat: Spring Boot 3.x + spring-kafka
#
# Assumes a stack is ALREADY running (rig.sh, or a hand-rolled broker+facade).
# Starts and stops nothing.
#
# Env:
#   QUEEN_KAFKA_BOOTSTRAP        plaintext facade, default 127.0.0.1:19092
#                                (KAFKA_BOOTSTRAP is accepted as an alias)
#   RUN_ID                       suffix on every topic and group, default a timestamp
#
#   QUEEN_KAFKA_TLS_BOOTSTRAP    SASL_SSL facade; the TLS phase is skipped if unset
#   QUEEN_KAFKA_SASL_TOKEN       the Queen bearer token = the SASL PLAIN password
#   QUEEN_KAFKA_TLS_CERT         PEM the facade serves; this script imports it into a
#                                PKCS12 truststore with the JDK's own keytool
#   QUEEN_KAFKA_TRUSTSTORE       pre-built PKCS12 truststore (skips the keytool step)
#   QUEEN_KAFKA_TRUSTSTORE_PASSWORD   default "changeit"
#
#   QUEEN_COMPAT_CACHE           where to park a downloaded Maven, default
#                                ${XDG_CACHE_HOME:-$HOME/.cache}/queen-kafka-compat
#   SUITE_TIMEOUT_S              hard watchdog on the JVM, default 900
#
# Java is the only hard prerequisite (17+; Boot 3.5 is happy on 17 through 25).
# Maven is used if present on PATH, otherwise a pinned distribution is downloaded
# into the cache directory - never into this repository.
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

BOOTSTRAP="${QUEEN_KAFKA_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-127.0.0.1:19092}}"
RUN_ID="${RUN_ID:-$(date +%s)}"
CACHE="${QUEEN_COMPAT_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/queen-kafka-compat}"
MAVEN_VERSION="${MAVEN_VERSION:-3.9.16}"
SUITE_TIMEOUT_S="${SUITE_TIMEOUT_S:-900}"

command -v java >/dev/null 2>&1 || { echo "run.sh: no java on PATH (need 17+)" >&2; exit 2; }

# ---------------------------------------------------------------- maven ----
if command -v mvn >/dev/null 2>&1; then
  MVN="$(command -v mvn)"
elif [ -x "${MAVEN_HOME:-/nonexistent}/bin/mvn" ]; then
  MVN="$MAVEN_HOME/bin/mvn"
elif [ -x "$CACHE/apache-maven-$MAVEN_VERSION/bin/mvn" ]; then
  MVN="$CACHE/apache-maven-$MAVEN_VERSION/bin/mvn"
else
  echo "run.sh: no mvn on PATH; fetching Apache Maven $MAVEN_VERSION into $CACHE"
  mkdir -p "$CACHE"
  curl -fsSL -o "$CACHE/maven.tar.gz" \
    "https://repo1.maven.org/maven2/org/apache/maven/apache-maven/$MAVEN_VERSION/apache-maven-$MAVEN_VERSION-bin.tar.gz"
  tar xzf "$CACHE/maven.tar.gz" -C "$CACHE"
  rm -f "$CACHE/maven.tar.gz"
  MVN="$CACHE/apache-maven-$MAVEN_VERSION/bin/mvn"
fi

JAR="$HERE/target/queen-kafka-spring-compat.jar"
if [ ! -f "$JAR" ] || [ -n "$(find "$HERE/src" "$HERE/pom.xml" -newer "$JAR" 2>/dev/null | head -1)" ]; then
  echo "=== building (mvn package)"
  "$MVN" -q -B -DskipTests package
fi
[ -f "$JAR" ] || { echo "run.sh: build produced no $JAR" >&2; exit 2; }

# ------------------------------------------------------------ truststore ----
if [ -n "${QUEEN_KAFKA_TLS_BOOTSTRAP:-}" ] && [ -z "${QUEEN_KAFKA_TRUSTSTORE:-}" ] && [ -n "${QUEEN_KAFKA_TLS_CERT:-}" ]; then
  TS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/qk-spring-ts.XXXXXX")"
  trap 'rm -rf "$TS_DIR"' EXIT
  QUEEN_KAFKA_TRUSTSTORE="$TS_DIR/truststore.p12"
  QUEEN_KAFKA_TRUSTSTORE_PASSWORD="${QUEEN_KAFKA_TRUSTSTORE_PASSWORD:-changeit}"
  keytool -importcert -noprompt -alias queen-kafka \
    -file "$QUEEN_KAFKA_TLS_CERT" \
    -keystore "$QUEEN_KAFKA_TRUSTSTORE" -storetype PKCS12 \
    -storepass "$QUEEN_KAFKA_TRUSTSTORE_PASSWORD" >/dev/null
  export QUEEN_KAFKA_TRUSTSTORE QUEEN_KAFKA_TRUSTSTORE_PASSWORD
  echo "=== built PKCS12 truststore from $QUEEN_KAFKA_TLS_CERT"
fi

# ------------------------------------------------------------------ run ----
# macOS has no timeout(1): watchdog by hand so a hung JVM is a result, not a stall.
#
# The watchdog subshell has ALL of its stdio redirected away from the caller's.  If it
# inherited them, its `sleep` would keep the write end of a caller-side pipe open and
# `run.sh | tee` would appear to hang for the whole SUITE_TIMEOUT_S even after the JVM
# had exited and printed RESULT.  Killing the subshell is not enough: the sleep is a
# separate process holding the same descriptors, so it is killed explicitly too.
java -jar "$JAR" "$BOOTSTRAP" "$RUN_ID" &
JAVA_PID=$!
(
  sleep "$SUITE_TIMEOUT_S"
  kill -9 "$JAVA_PID" 2>/dev/null || true
) >/dev/null 2>&1 </dev/null &
WATCHDOG=$!

set +e
wait "$JAVA_PID"
RC=$?
set -e
pkill -P "$WATCHDOG" >/dev/null 2>&1 || true
kill "$WATCHDOG" >/dev/null 2>&1 || true
if [ "$RC" -ge 128 ]; then
  echo "run.sh: the suite JVM died on a signal (exit $RC); the watchdog is ${SUITE_TIMEOUT_S}s" >&2
fi
exit "$RC"
