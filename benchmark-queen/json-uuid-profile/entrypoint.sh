#!/usr/bin/env bash
#
# Entrypoint for the profiling queen-server.
#
# LD_PRELOADs gperftools' libprofiler and execs the broker. Profiling is armed
# in signal-toggle mode (CPUPROFILESIGNAL): it does NOT run at startup. Send
# SIGUSR2 once to start sampling, again to stop and flush $CPUPROFILE. This lets
# run.sh bracket only the steady-state measurement window.
set -euo pipefail

CPUPROFILE="${CPUPROFILE:-/profiles/queen.prof}"

# Locate libprofiler.so regardless of architecture (amd64 / arm64).
LIBPROF="$(ldconfig -p 2>/dev/null | awk '/libprofiler\.so /{print $NF; exit}')"
if [[ -z "${LIBPROF:-}" || ! -e "$LIBPROF" ]]; then
  LIBPROF="/usr/lib/$(uname -m)-linux-gnu/libprofiler.so"
fi
if [[ ! -e "$LIBPROF" ]]; then
  echo "[entrypoint] ERROR: libprofiler.so not found" >&2
  exit 1
fi

mkdir -p "$(dirname "$CPUPROFILE")"

echo "[entrypoint] LD_PRELOAD=$LIBPROF"
echo "[entrypoint] CPUPROFILE=$CPUPROFILE CPUPROFILESIGNAL=${CPUPROFILESIGNAL:-<unset>}"
echo "[entrypoint] profiling is OFF; send SIGUSR2 to toggle on/off"

export LD_PRELOAD="$LIBPROF"
export CPUPROFILE
exec server/bin/queen-server
