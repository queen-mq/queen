#!/usr/bin/env bash
#
# The Confluent.Kafka (.NET) row of the M6 client matrix.
#
#   queen-kafka/compat/confluent-dotnet/run.sh [scenario]
#
# It assumes a stack is ALREADY RUNNING and starts nothing: the stack is
# rig.sh's job, or yours. Everything it needs comes from the environment.
#
#   KAFKA_BOOTSTRAP          plaintext listener          (default 127.0.0.1:19092)
#   RUN_ID                   topic/group suffix          (default: a timestamp)
#   QUEEN_KAFKA_PARTITIONS   what the facade was booted with (default 8)
#   KAFKA_TLS_BOOTSTRAP      SASL_SSL listener, for the M5 lane
#   QUEEN_KAFKA_SASL_TOKEN   the bearer token = the SASL password
#   QUEEN_KAFKA_TLS_CA       PEM for that listener
#   CONFLUENT_KAFKA_VERSION  NuGet version to test      (default 2.15.0)
#   QK_DOTNET_MODE           docker | host              (default: host if the
#                            SDK is installed, else docker)
#   QK_DOTNET_IMAGE          (default mcr.microsoft.com/dotnet/sdk:8.0)
#   QK_BUDGET_S              suite watchdog             (default 900)
#   QK_VERBOSE=1             echo every librdkafka debug line
#
# Scenarios: core | edges | sasl | all   (default: core + edges)
#
# WHY DOCKER BY DEFAULT ON A DEV MAC. There is no .NET SDK on this machine and
# there is no reason for the compat matrix to require one; the official SDK
# image is multi-arch and already arm64-native here. The consequence is the one
# thing to know about running this suite:
#
#   A CONTAINER CANNOT DIAL 127.0.0.1 ON YOUR MAC, AND MORE TO THE POINT IT
#   CANNOT DIAL AN ADVERTISED 127.0.0.1 EITHER.
#
# queen-kafka hands every client an advertised address after Metadata and after
# FindCoordinator, and the client re-dials THAT. So a facade booted with
# QUEEN_KAFKA_ADVERTISED_ADDR=127.0.0.1:PORT is unusable from a container no
# matter what bootstrap you pass it: the bootstrap connection succeeds and the
# next one resolves 127.0.0.1 inside the container's own namespace. Boot the
# facade with QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:PORT for this
# suite (a second facade on a second port can keep advertising 127.0.0.1 for
# host-side clients; one broker carries both, which is what rig.sh --m5 does).
#
# As a convenience this script rewrites a 127.0.0.1/localhost BOOTSTRAP to
# $QK_DOCKER_HOST_ALIAS (default host.docker.internal) in docker mode and says
# so — that fixes the bootstrap, not the advertised address.
#
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-default}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
TLS_BOOTSTRAP="${KAFKA_TLS_BOOTSTRAP:-}"
RUN="${RUN_ID:-$(date +%s)}"
PARTS="${QUEEN_KAFKA_PARTITIONS:-8}"
CKV="${CONFLUENT_KAFKA_VERSION:-2.15.0}"
IMAGE="${QK_DOTNET_IMAGE:-mcr.microsoft.com/dotnet/sdk:8.0}"
ALIAS="${QK_DOCKER_HOST_ALIAS:-host.docker.internal}"
BUDGET="${QK_BUDGET_S:-900}"

MODE="${QK_DOTNET_MODE:-}"
if [ -z "$MODE" ]; then
  if command -v dotnet >/dev/null 2>&1; then MODE=host; else MODE=docker; fi
fi

say() { printf '\n===== %s\n' "$*"; }

# macOS has no timeout(1) and this suite must never be the thing that hangs a
# rig run. The C# side has its own watchdog; this is the belt to that braces.
limit() {
  local secs="$1"; shift
  "$@" &
  local pid=$! i=0
  while [ $i -lt $((secs * 2)) ]; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid"; return $?; }
    sleep 0.5; i=$((i + 1))
  done
  echo "  !!   TIMED OUT after ${secs}s: $*" >&2
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  return 124
}

say "Confluent.Kafka $CKV, scenario '$SCENARIO', mode $MODE"

if [ "$MODE" = docker ]; then
  case "$BOOTSTRAP" in
    127.0.0.1:*|localhost:*)
      NEW="$ALIAS:${BOOTSTRAP##*:}"
      echo "  --   rewriting bootstrap $BOOTSTRAP -> $NEW (a container cannot dial your loopback)"
      echo "  --   the facade must ALSO advertise $ALIAS or the client will die re-dialling"
      BOOTSTRAP="$NEW" ;;
  esac
  if [ -n "$TLS_BOOTSTRAP" ]; then
    case "$TLS_BOOTSTRAP" in
      127.0.0.1:*|localhost:*) TLS_BOOTSTRAP="$ALIAS:${TLS_BOOTSTRAP##*:}" ;;
    esac
  fi

  NUGET="$HERE/.nuget"; mkdir -p "$NUGET"
  ARGS=(
    run --rm -i
    --name "qkcompat-dotnet-run-$RUN"
    --user "$(id -u):$(id -g)"
    -v "$HERE":/src -w /src
    -v "$NUGET":/nuget
    -e NUGET_PACKAGES=/nuget
    -e HOME=/tmp -e DOTNET_CLI_HOME=/tmp
    -e DOTNET_NOLOGO=1 -e DOTNET_CLI_TELEMETRY_OPTOUT=1
    -e CONFLUENT_KAFKA_VERSION="$CKV"
    -e QUEEN_KAFKA_PARTITIONS="$PARTS"
    -e QK_BUDGET_S="$BUDGET"
    -e QK_VERBOSE="${QK_VERBOSE:-0}"
  )
  [ -n "${QK_EDGE_PROBES:-}" ] && ARGS+=(-e QK_EDGE_PROBES="$QK_EDGE_PROBES")
  [ -n "${QK_BROKER_ID:-}" ] && ARGS+=(-e QK_BROKER_ID="$QK_BROKER_ID")
  # See Edges.cs: this one aborts the process on Confluent.Kafka 2.x.
  [ -n "${QK_PROBE_LISTGROUPS:-}" ] && ARGS+=(-e QK_PROBE_LISTGROUPS="$QK_PROBE_LISTGROUPS")
  [ -n "$TLS_BOOTSTRAP" ] && ARGS+=(-e KAFKA_TLS_BOOTSTRAP="$TLS_BOOTSTRAP")
  [ -n "${QUEEN_KAFKA_SASL_TOKEN:-}" ] && ARGS+=(-e QUEEN_KAFKA_SASL_TOKEN="$QUEEN_KAFKA_SASL_TOKEN")
  [ -n "${QK_SSL_INSECURE:-}" ] && ARGS+=(-e QK_SSL_INSECURE="$QK_SSL_INSECURE")
  if [ -n "${QUEEN_KAFKA_TLS_CA:-}" ] && [ -f "${QUEEN_KAFKA_TLS_CA}" ]; then
    ARGS+=(-v "$(cd "$(dirname "$QUEEN_KAFKA_TLS_CA")" && pwd)/$(basename "$QUEEN_KAFKA_TLS_CA")":/certs/ca.pem:ro
           -e QUEEN_KAFKA_TLS_CA=/certs/ca.pem)
  fi
  ARGS+=("$IMAGE" dotnet run --project /src/QueenKafkaCompat.csproj -c Release --
         "$BOOTSTRAP" "$RUN" "$SCENARIO")

  limit $((BUDGET + 300)) docker "${ARGS[@]}"
  RC=$?
else
  export CONFLUENT_KAFKA_VERSION="$CKV"
  export QUEEN_KAFKA_PARTITIONS="$PARTS"
  export QK_BUDGET_S="$BUDGET"
  [ -n "$TLS_BOOTSTRAP" ] && export KAFKA_TLS_BOOTSTRAP="$TLS_BOOTSTRAP"
  limit $((BUDGET + 300)) env DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    dotnet run --project "$HERE/QueenKafkaCompat.csproj" -c Release -- \
      "$BOOTSTRAP" "$RUN" "$SCENARIO"
  RC=$?
fi

say "confluent-dotnet: exit $RC"
exit $RC
