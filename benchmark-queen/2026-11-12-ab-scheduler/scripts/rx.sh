#!/usr/bin/env bash
# rx.sh <n1|n2|n3|load|brokers|all> <command...> (Mac): run a command as root on rig machines, in parallel, each
# output line prefixed with the machine.
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
t=$1; shift
case $t in
  n1) hs=("${BP[0]}"); names=(n1) ;;
  n2) hs=("${BP[1]}"); names=(n2) ;;
  n3) hs=("${BP[2]}"); names=(n3) ;;
  load) hs=("${LP[0]}"); names=(load) ;;
  brokers) hs=("${BP[@]}"); names=(n1 n2 n3) ;;
  all) hs=("${BP[@]}" "${LP[0]}"); names=(n1 n2 n3 load) ;;
  *) echo "rx.sh <n1|n2|n3|load|brokers|all> <command>" >&2; exit 2 ;;
esac
for i in "${!hs[@]}"; do
  ( $SSH root@"${hs[$i]}" "$*" 2>&1 | sed "s/^/[${names[$i]}] /" ) &
done
wait
