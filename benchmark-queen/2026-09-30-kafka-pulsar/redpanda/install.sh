#!/usr/bin/env bash
# install.sh (node) — Redpanda 26.2.3 (redpanda + redpanda-rpk + redpanda-tuner) from Redpanda's official apt repository,
# pinned version, idempotent. The repository is configured exactly as the official
# https://linux.pkg.redpanda.com/setup-redpanda.deb.sh does it (signing key -> /usr/share/keyrings/artifact-registry.gpg,
# one source line "deb [signed-by=...] https://linux.pkg.redpanda.com/apt redpanda-apt main"), written out here instead of
# `curl | sudo bash` so every step is visible, and the key's fingerprint is checked against a pinned value (the repository is
# a Google Artifact Registry: the key is GCP AR's "Artifact Registry Repository Signer").
# The redpanda postinst ENABLES redpanda.service and redpanda-tuner.service (it does not start them): they are disabled
# again here, so a droplet reboot never brings Redpanda (on :9092) or its tuners up under another system's run. The harness
# starts Redpanda itself (rc.sh start, as root, with the unit's limits) and tunes the OS on purpose (rc.sh tune/untune).
# Env: REDPANDA_VERSION (apt version, default 26.2.3-1). apt runs niced + ionice'd.
set -u
main() {
  local D; D=$(cd "$(dirname "$0")/.." && pwd)
  [ -f "$D/hosts.env" ] && . "$D/hosts.env"
  local V=${REDPANDA_VERSION:-26.2.3-1} RP=$D/redpanda
  local KEYRING=/usr/share/keyrings/artifact-registry.gpg SRC=/etc/apt/sources.list.d/redpanda.list
  local KEYURL=https://linux.pkg.redpanda.com/redpanda-deb-signing-public.gpg
  local FPR=35BAA0B33E9EB396F59CA838C0BA5CE6DC6315A3   # checked 2026-10-01 (rsa2048, 2021-05-04)
  local LINE="deb [signed-by=$KEYRING] https://linux.pkg.redpanda.com/apt redpanda-apt main"
  log() { echo "[$(date -u +%FT%TZ)] install $(hostname): $*"; }
  inst() { dpkg-query -W -f='${Version}' "$1" 2>/dev/null; }
  export DEBIAN_FRONTEND=noninteractive
  mkdir -p "$RP/state"
  local APT="nice -n 19 ionice -c3 apt-get -o DPkg::Lock::Timeout=600"

  if [ "$(inst redpanda)" = "$V" ] && [ "$(inst redpanda-rpk)" = "$V" ] && [ "$(inst redpanda-tuner)" = "$V" ]; then
    log "redpanda $V already installed"
  else
    # 1. repository (official setup script's steps; prerequisites are on the droplet image already, installed if not)
    command -v gpg > /dev/null && command -v curl > /dev/null || $APT install -y -qq --no-install-recommends curl gnupg ca-certificates > "$RP/state/apt.log" 2>&1
    local tk; tk=$(mktemp)
    curl -fsSL --tlsv1.2 --retry 7 --retry-delay 2 "$KEYURL" -o "$tk" || { log "cannot fetch $KEYURL"; rm -f "$tk"; return 1; }
    local got; got=$(gpg --batch --show-keys --with-colons "$tk" 2>/dev/null | awk -F: '$1 == "fpr" {print $10; exit}')
    [ "$got" = "$FPR" ] || { log "signing key fingerprint '$got' != pinned $FPR: refusing"; rm -f "$tk"; return 1; }
    gpg --dearmor --batch < "$tk" > "$KEYRING.tmp" && mv "$KEYRING.tmp" "$KEYRING" && chmod 644 "$KEYRING"; rm -f "$tk"
    log "signing key $FPR -> $KEYRING"
    [ "$(cat "$SRC" 2>/dev/null)" = "$LINE" ] || { echo "$LINE" > "$SRC"; chmod 644 "$SRC"; log "wrote $SRC"; }
    # refresh only this source (the Ubuntu lists stay as they are)
    $APT update -qq -o Dir::Etc::sourcelist="sources.list.d/redpanda.list" -o Dir::Etc::sourceparts="-" -o APT::Get::List-Cleanup="0" >> "$RP/state/apt.log" 2>&1 \
      || { log "apt-get update of $SRC failed:"; tail -5 "$RP/state/apt.log"; return 1; }
    apt-cache madison redpanda 2>/dev/null | head -3 | sed 's/^/  available: /'
    # 2. the pinned version of all three packages (the newest stable on 2026-10-01: 26.2.3-1)
    $APT install -y -qq --no-install-recommends --allow-downgrades "redpanda=$V" "redpanda-rpk=$V" "redpanda-tuner=$V" >> "$RP/state/apt.log" 2>&1 \
      || { log "apt-get install failed:"; tail -8 "$RP/state/apt.log"; return 1; }
    log "installed redpanda $(inst redpanda), redpanda-rpk $(inst redpanda-rpk), redpanda-tuner $(inst redpanda-tuner)"
  fi

  # 3. the package's units stay off: the harness starts Redpanda (rc.sh) and tunes on purpose (rc.sh tune / untune)
  local u
  for u in redpanda.service redpanda-tuner.service; do
    systemctl is-active -q "$u" && { systemctl stop "$u"; log "stopped $u (was running)"; }
    systemctl is-enabled -q "$u" 2>/dev/null && { systemctl disable -q "$u" 2>/dev/null; log "disabled $u (the postinst enables it)"; }
  done

  # 4. record what is installed
  local rv bv
  rv=$(rpk --version 2>/dev/null | head -1)
  bv=$(/opt/redpanda/bin/redpanda --version 2>/dev/null | head -1)
  { echo "version=$V"; echo "redpanda=$bv"; echo "rpk=$rv"; echo "release_date=$(cat /opt/redpanda/RELEASE-DATE.txt 2>/dev/null)"
    echo "units=redpanda:$(systemctl is-enabled redpanda.service 2>/dev/null)/$(systemctl is-active redpanda.service 2>/dev/null) redpanda-tuner:$(systemctl is-enabled redpanda-tuner.service 2>/dev/null)/$(systemctl is-active redpanda-tuner.service 2>/dev/null)"
    echo "installed=$(date -u +%FT%TZ)"; } > "$RP/state/installed.tmp" && mv "$RP/state/installed.tmp" "$RP/state/installed"
  log "redpanda: $bv | rpk: $rv | $(grep '^units=' "$RP/state/installed")"
  [ "$(inst redpanda)" = "$V" ]
}
main "$@"; exit $?
