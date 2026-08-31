#!/usr/bin/env bash
# compat/java-matrix — the org.apache.kafka:kafka-clients VERSION MATRIX.
#
# compat/java already answers "does the official Java client work" for one
# version. This directory answers a different question: does the facade's
# advertised window (protocols/queen-kafka/src/versions.rs) still satisfy kafka-clients as
# kafka-clients moves? Kafka 4.0 shipped KIP-896, which RAISED the minimum
# protocol versions a client will speak, and the facade deliberately caps Fetch
# at v6, Metadata at v9 and the group APIs one below group_instance_id. Those two
# facts point at each other and only a real run settles it.
#
# It assumes a stack is ALREADY RUNNING — it starts and stops nothing. Point it
# at one with the environment:
#
#   KAFKA_BOOTSTRAP   host:port of a plaintext facade      (default 127.0.0.1:19092)
#   RUN_ID            suffix on every topic and group name (default: a timestamp)
#   KAFKA_VERSIONS    space-separated kafka-clients versions to drive
#                     (default "4.3.1 3.6.2")
#   JARS_CACHE        where the jars live/land (default $TMPDIR/queen-kafka-clients)
#   SUITES            which programs to run: "matrix", "edges", or "matrix edges"
#                     (default "matrix edges")
#
# The SASL_SSL lane, all optional and all passed through to the Java programs:
#   QK_SECURITY_PROTOCOL=SASL_SSL
#   QK_SASL_MECHANISM=PLAIN
#   QK_SASL_USERNAME=<free label>   QK_SASL_PASSWORD=<the Queen bearer token>
#   QK_TRUSTSTORE=<path.p12>        QK_TRUSTSTORE_PASSWORD=changeit
#   QK_DISABLE_HOSTNAME_VERIFICATION=1   (needed when the advertised host is not
#                                         a SAN on the facade's certificate)
#
# THE JARS. There is no pom and no gradle, matching compat/java: the programs run
# in java's single-file source mode against a directory of jars. This script
# fetches them from Maven Central on first use and caches them OUTSIDE the repo.
# Set JARS_CACHE to a pre-populated directory to run with no network at all.
#
# Requires a JDK 17 or newer on PATH (kafka-clients 4.x needs 11+; single-file
# source mode needs 11+; this was validated on 24).
#
# ONE HOST TRAP, and it is not the facade's: kafka-clients BEFORE 3.9 cannot do
# SASL AT ALL on a JDK 24 or newer. Their SaslClientCallbackHandler calls
# Subject.getSubject(AccessControlContext), which JDK 24 finalised the removal of
# (JEP 486) — it throws UnsupportedOperationException, the channel is never
# built, and the client retries the connection forever while the facade logs
# "TLS handshake eof" as the half-open socket closes. There is no flag that
# rescues it: -Djava.security.manager=allow, the documented escape on 17-23, is
# itself rejected by JDK 24. The plaintext lane of those same versions is
# unaffected. The check below warns rather than failing, because the right fix is
# a JDK 21 for that row (compat/README's differential image, apache/kafka:3.9.1,
# carries a Temurin 21 that runs it clean).

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
RUN_ID="${RUN_ID:-$(date +%s)}"
VERSIONS="${KAFKA_VERSIONS:-4.3.1 3.6.2}"
SUITES="${SUITES:-matrix edges}"
CACHE="${JARS_CACHE:-${TMPDIR:-/tmp}/queen-kafka-clients}"
CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"

# The runtime deps of kafka-clients, per version. Keep these pinned to what the
# release's own pom declares; a mismatched compression jar is a confusing
# NoClassDefFoundError inside a codec, not an obvious dependency error.
deps_for() {
  case "$1" in
    4.*) echo "com/github/luben/zstd-jni/1.5.6-10/zstd-jni-1.5.6-10.jar
at/yawk/lz4/lz4-java/1.10.2/lz4-java-1.10.2.jar
org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar" ;;
    3.*) echo "com/github/luben/zstd-jni/1.5.5-1/zstd-jni-1.5.5-1.jar
org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar
org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" ;;
    *)   echo "" ;;
  esac
}
SLF4J="org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar"

fetch() { # fetch <maven/path/to.jar> <destdir>
  local path="$1" dest="$2" file
  file="$dest/$(basename "$path")"
  [ -s "$file" ] && return 0
  echo "     fetching $(basename "$path")"
  curl -sS -f -m 180 -o "$file" "$CENTRAL/$path" || { rm -f "$file"; return 1; }
}

jars_for() { # jars_for <version> -> prints the classpath dir
  local v="$1" dir="$CACHE/$v"
  mkdir -p "$dir" || return 1
  fetch "org/apache/kafka/kafka-clients/$v/kafka-clients-$v.jar" "$dir" || return 1
  local d
  for d in $(deps_for "$v") $SLF4J; do fetch "$d" "$dir" || return 1; done
  echo "$dir"
}

command -v java >/dev/null 2>&1 || { echo "no java on PATH"; exit 2; }

echo "compat/java-matrix"
echo "  bootstrap  $BOOTSTRAP"
echo "  runId      $RUN_ID"
echo "  versions   $VERSIONS"
echo "  suites     $SUITES"
echo "  security   ${QK_SECURITY_PROTOCOL:-PLAINTEXT}"
echo "  jars cache $CACHE"
echo "  java       $(java -version 2>&1 | head -1)"

overall=0
summary=""
for v in $VERSIONS; do
  echo
  echo "############################################################"
  echo "# kafka-clients $v"
  echo "############################################################"
  # The JDK 24 / pre-3.9 SASL trap, called out BEFORE the run rather than left
  # to look like a facade timeout.
  if [ -n "${QK_SECURITY_PROTOCOL:-}" ] && [ "${QK_SECURITY_PROTOCOL#*SASL}" != "$QK_SECURITY_PROTOCOL" ]; then
    jdk_major="$(java -version 2>&1 | sed -n '1s/.*version "\([0-9]*\).*/\1/p')"
    case "$v" in
      3.[0-8].*)
        if [ -n "$jdk_major" ] && [ "$jdk_major" -ge 24 ] 2>/dev/null; then
          echo "  !! kafka-clients $v cannot do SASL on JDK $jdk_major (JEP 486 removed"
          echo "     Subject.getSubject; fixed upstream in kafka-clients 3.9). Expect every"
          echo "     connection to fail before the TLS handshake completes. Run this row on a"
          echo "     JDK 21 instead — this is a client/JDK limit, not a facade one."
        fi ;;
    esac
  fi

  if ! dir="$(jars_for "$v")"; then
    echo "  FAIL could not assemble the classpath for $v"
    summary="$summary\n  kafka-clients $v  JARS-MISSING"
    overall=1
    continue
  fi

  for suite in $SUITES; do
    case "$suite" in
      matrix) file="$HERE/QueenKafkaMatrix.java";  scored=1 ;;
      edges)  file="$HERE/QueenKafkaEdges4x.java"; scored=0 ;;
      *) echo "  unknown suite '$suite'"; continue ;;
    esac
    echo
    echo "--- $suite ($(basename "$file")) ---"
    # The only JVM flags here exist to keep a modern JDK quiet about jars older
    # than its module rules: snappy-java calls System.load, which JDK 24 warns
    # about unless native access is granted, and the warning lands in the middle
    # of the compression section looking like a failure.
    java -XX:+IgnoreUnrecognizedVMOptions \
         --enable-native-access=ALL-UNNAMED \
         --add-opens=java.base/java.nio=ALL-UNNAMED \
         -cp "$dir/*" "$file" "$BOOTSTRAP" "${RUN_ID}-${v//./}"
    rc=$?
    if [ "$scored" = 1 ]; then
      if [ $rc -ne 0 ]; then overall=1; summary="$summary\n  kafka-clients $v  $suite  FAIL(rc=$rc)"
      else summary="$summary\n  kafka-clients $v  $suite  PASS"; fi
    else
      summary="$summary\n  kafka-clients $v  $suite  (unscored, rc=$rc)"
    fi
  done
done

echo
echo "============================================================"
echo -e "SUMMARY$summary"
echo "RESULT: $([ $overall -eq 0 ] && echo PASS || echo FAIL)"
exit $overall
