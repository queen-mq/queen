#!/usr/bin/env bash
# dexec.sh — ssh stand-in for the local rehearsal: `dexec.sh [ssh options] [user@]<ip> [command...]` runs the command
# with bash -lc in the docker container that owns <ip> (stdin passed through, so `tar c | $SSH host 'tar x'` works).
# hosts.env for a rehearsal sets SSH="$HARNESS/common/dexec.sh"; run.sh / cluster.sh / deploy.sh stay unchanged.
set -u
while [ $# -gt 0 ]; do
  case $1 in
    -o|-i|-p|-l|-F|-J|-c|-m|-E|-S|-W|-w|-b|-L|-R|-D|-O|-Q) shift 2 ;;   # options that take a value
    -*) shift ;;
    *) break ;;
  esac
done
[ $# -ge 1 ] || { echo "usage: dexec.sh [opts] <ip> [cmd...]" >&2; exit 2; }
ip=${1#*@}; shift
cache=${TMPDIR:-/tmp}/dexec.$(id -u).map
lookup() { awk -v ip="$ip" '$1==ip{print $2; exit}' "$cache" 2>/dev/null; }
c=$(lookup)
if [ -z "$c" ] || ! docker inspect "$c" >/dev/null 2>&1; then
  docker ps -q | xargs -r docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}{{.Name}}' \
    | awk '{n=$NF; sub(/^\//,"",n); for(i=1;i<NF;i++) print $i, n}' > "$cache.$$" && mv "$cache.$$" "$cache"
  c=$(lookup)
fi
[ -n "$c" ] || { echo "dexec: no running container has IP $ip" >&2; exit 255; }
if [ $# -eq 0 ]; then exec docker exec -i "$c" bash -l; fi
if [ -t 0 ]; then exec docker exec "$c" bash -lc "$*"; else exec docker exec -i "$c" bash -lc "$*"; fi
