#!/usr/bin/env bash
# build.sh — static kload + pload + qload binaries:
#   bin/linux-amd64/{kload,pload,qload}   the droplets (deploy.sh copies bin/$BIN_ARCH/* to /root/bench/bin; txn/deploy.sh
#                                         copies them to /root/bench/bin/txn for the transactional benchmark)
#   bin/linux-arm64/{kload,pload,qload}   the local rehearsal containers (benchvm:24.04 on Apple silicon)
#   bin/darwin-arm64/{kload,pload,qload}  the Mac (optional: TARGETS="linux/amd64 linux/arm64" skips it)
# CGO_ENABLED=0 and -trimpath; each binary is written to a temp name and mv'ed, so a running binary is never
# overwritten in place. Tests run first unless SKIP_TESTS=1.
# go.mod asks for go 1.26 (franz-go v1.22.x requires it): with Go 1.25 installed, GOTOOLCHAIN=auto (the default)
# fetches/uses go1.26.8 from the module cache. GOWORK=off: /Users/alice/Work/queen/go.work must not capture this module.
set -eu
cd "$(dirname "$0")"
export GOWORK=off CGO_ENABLED=0
TARGETS=${TARGETS:-"linux/amd64 linux/arm64 darwin/arm64"}
CMDS=${CMDS:-"kload pload qload"}   # e.g. CMDS=pload rebuilds pload only (never touch a kload that is running on the VMs)
echo "[$(date -u +%T)] $(go version)"
go vet ./...
if [ "${SKIP_TESTS:-0}" != 1 ]; then
  go test -count=1 ./...
fi
for t in $TARGETS; do
  os=${t%/*}; arch=${t#*/}; out=bin/$os-$arch
  mkdir -p "$out"
  for cmd in $CMDS; do
    GOOS=$os GOARCH=$arch go build -trimpath -o "$out/.$cmd.tmp" "./cmd/$cmd"
    mv -f "$out/.$cmd.tmp" "$out/$cmd"
    printf '[%s] %-14s %-5s %.1f MB sha256 %s\n' "$(date -u +%T)" "$out" "$cmd" "$(( $(wc -c < "$out/$cmd") ))e-6" "$(shasum -a 256 "$out/$cmd" 2>/dev/null | cut -c1-16 || sha256sum "$out/$cmd" | cut -c1-16)"
  done
done
