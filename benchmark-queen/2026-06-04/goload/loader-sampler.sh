#!/usr/bin/env bash
# loader-sampler.sh <outfile> <duration_s>
#
# 1 Hz sampler for the QueenMQ loader host. Every second appends one CSV line:
#   epoch_ms,goload_cpu_pct,goload_mem_mb,total_cpu_idle_pct,
#   net_rx_mbps,net_tx_mbps,tcp_established,tcp_timewait
#
# goload CPU is computed from /proc/<pid>/stat utime+stime DELTAS (ps %cpu is a
# lifetime average and is NOT used). Idle % and net rates are /proc deltas.
# Cost-control choices (keep it under a couple % of one core):
#   * full /proc scan to (re)discover goload pids only every GL_REFRESH s;
#     in between, stat only the cached pids (pgrep measured ~5x costlier);
#   * /proc/stat and /proc/net/dev parsed by helpers that set globals (no
#     command-substitution fork), plus ONE awk per line for all float math,
#     and ss parsed with bash regex (no sed).
# Robust to missing values: any unreadable field is emitted empty, never crashes.
#
# CPU% is "percent of one core" (100% = 1 full core; 48 cores => ceiling 4800%).
set -u

OUT="${1:?usage: loader-sampler.sh <outfile> <duration_s>}"
DUR="${2:?usage: loader-sampler.sh <outfile> <duration_s>}"

IFACE="eth1"                                  # private interface
CLK=$(getconf CLK_TCK 2>/dev/null); [ -n "$CLK" ] || CLK=100
PAGE=$(getconf PAGE_SIZE 2>/dev/null); [ -n "$PAGE" ] || PAGE=4096
GL_REFRESH=3                                  # full /proc goload re-scan interval (s)

now_ms() { date +%s%3N; }

# read /proc/net/dev -> sets G_RX / G_TX (bytes) for $IFACE; empty if missing
read_net() {
  G_RX=""; G_TX=""
  local l
  while read -r l; do
    case "$l" in
      *${IFACE}:*)
        l=${l#*${IFACE}:}; set -- $l   # after label: 1=rx_bytes ... 9=tx_bytes
        G_RX="${1:-}"; G_TX="${9:-}"; return ;;
    esac
  done < /proc/net/dev 2>/dev/null
}

# read aggregate /proc/stat cpu line -> sets G_IDLE / G_TOT jiffies
read_cpu() {
  G_IDLE=""; G_TOT=""
  local line; read -r line < /proc/stat 2>/dev/null || return
  set -- $line; shift              # user nice system idle iowait irq softirq ...
  local tot=0 f
  for f in "$@"; do tot=$(( tot + f )); done
  G_IDLE="${4:-}"; G_TOT="$tot"
}

# header only when creating a fresh file
if [ ! -s "$OUT" ]; then
  printf 'epoch_ms,goload_cpu_pct,goload_mem_mb,total_cpu_idle_pct,net_rx_mbps,net_tx_mbps,tcp_established,tcp_timewait\n' >> "$OUT"
fi

declare -A prev_ticks
GL_PIDS=""
read_net; prev_rx="$G_RX"; prev_tx="$G_TX"
read_cpu; cpu_idle_prev="$G_IDLE"; cpu_tot_prev="$G_TOT"
prev_ms=$(now_ms)
start_ms=$prev_ms

i=0
while [ "$i" -lt "$DUR" ]; do
  i=$((i+1))
  target=$(( start_ms + i*1000 ))
  cur=$(now_ms)
  slp=$(( target - cur ))
  if [ "$slp" -gt 0 ]; then
    sleep "$((slp/1000)).$(printf '%03d' $((slp%1000)))"
  fi

  ts=$(now_ms); dt=$(( ts - prev_ms )); [ "$dt" -le 0 ] && dt=1

  # ---- goload cpu (utime+stime deltas) + rss over all goload* pids ----------
  if [ $(( (i-1) % GL_REFRESH )) -eq 0 ]; then    # periodic full re-discovery
    GL_PIDS=""
    for d in /proc/[0-9]*; do
      [ -r "$d/comm" ] || continue
      read -r comm < "$d/comm" 2>/dev/null || continue
      case "$comm" in goload*) GL_PIDS="$GL_PIDS ${d#/proc/}" ;; esac
    done
  fi
  unset cur_ticks; declare -A cur_ticks
  delta_ticks=0; rss_pages=0; found=0; live=""
  for pid in $GL_PIDS; do
    read -r line < "/proc/$pid/stat" 2>/dev/null || continue   # pid gone -> drop
    cm=${line#*\(}; cm=${cm%%\)*}          # verify identity (pid recycle-safe)
    case "$cm" in goload*) ;; *) continue ;; esac
    rest=${line##*) }                      # fields start at state
    set -- $rest                           # 12=utime 13=stime 22=rss(pages)
    tk=$(( ${12:-0} + ${13:-0} ))
    cur_ticks[$pid]=$tk
    rss_pages=$(( rss_pages + ${22:-0} ))
    found=1; live="$live $pid"
    if [ -n "${prev_ticks[$pid]:-}" ]; then
      dtk=$(( tk - prev_ticks[$pid] )); [ "$dtk" -lt 0 ] && dtk=0
      delta_ticks=$(( delta_ticks + dtk ))
    fi
  done
  GL_PIDS="$live"                          # prune dead pids from the cache
  unset prev_ticks; declare -A prev_ticks
  for k in "${!cur_ticks[@]}"; do prev_ticks[$k]=${cur_ticks[$k]}; done

  # ---- cpu idle + net deltas -----------------------------------------------
  read_cpu; cpu_idle="$G_IDLE"; cpu_tot="$G_TOT"
  didle="x"; dtot="x"
  if [ -n "$cpu_idle" ] && [ -n "$cpu_idle_prev" ]; then
    dtot=$(( cpu_tot - cpu_tot_prev )); didle=$(( cpu_idle - cpu_idle_prev ))
  fi
  cpu_idle_prev="$cpu_idle"; cpu_tot_prev="$cpu_tot"

  read_net; drx="x"; dtx="x"
  [ -n "$G_RX" ] && [ -n "$prev_rx" ] && drx=$(( G_RX - prev_rx ))
  [ -n "$G_TX" ] && [ -n "$prev_tx" ] && dtx=$(( G_TX - prev_tx ))
  prev_rx="$G_RX"; prev_tx="$G_TX"

  # ---- one awk: goload cpu%, goload mem MB, idle%, rx Mbps, tx Mbps ---------
  # "-" sentinel from awk = missing/blank -> emitted as an empty CSV field.
  read goload_cpu goload_mem idle_pct rx_mbps tx_mbps < <(awk \
      -v found="$found" -v ticks="$delta_ticks" -v rss="$rss_pages" \
      -v didle="$didle" -v dtot="$dtot" -v drx="$drx" -v dtx="$dtx" \
      -v k="$CLK" -v pg="$PAGE" -v dt="$dt" 'BEGIN{
    if(found=="1"){ gc=sprintf("%.1f",(ticks/k)/(dt/1000)*100); gm=sprintf("%.1f",rss*pg/1048576) } else { gc="-"; gm="-" }
    ip=(didle=="x"||dtot=="x"||dtot<=0)?"-":sprintf("%.1f",didle/dtot*100)
    rx=(drx=="x")?"-":sprintf("%.2f",(drx<0?0:drx)*8/(dt/1000)/1000000)
    tx=(dtx=="x")?"-":sprintf("%.2f",(dtx<0?0:dtx)*8/(dt/1000)/1000000)
    print gc, gm, ip, rx, tx
  }')
  [ "$goload_cpu" = "-" ] && goload_cpu=""
  [ "$goload_mem" = "-" ] && goload_mem=""
  [ "$idle_pct"  = "-" ] && idle_pct=""
  [ "$rx_mbps"   = "-" ] && rx_mbps=""
  [ "$tx_mbps"   = "-" ] && tx_mbps=""

  # ---- tcp socket summary (one ss call, parsed with bash regex) -------------
  sss=$(ss -s 2>/dev/null)
  estab=""; tw=""
  [[ $sss =~ estab\ ([0-9]+) ]]    && estab=${BASH_REMATCH[1]}
  [[ $sss =~ timewait\ ([0-9]+) ]] && tw=${BASH_REMATCH[1]}

  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$ts" "$goload_cpu" "$goload_mem" "$idle_pct" "$rx_mbps" "$tx_mbps" "$estab" "$tw" >> "$OUT"

  prev_ms=$ts
done

# ---- self overhead report (this process + all reaped children) -----------
# NB: read /proc/$$/stat (the script PID) not /proc/self/stat -- inside $(...)
# "self" would resolve to the sed subprocess, reporting ~0.
rest=$(sed 's/.*) //' "/proc/$$/stat" 2>/dev/null)
set -- $rest
clk=$(getconf CLK_TCK 2>/dev/null); [ -n "$clk" ] || clk=100
awk -v u="${12:-0}" -v s="${13:-0}" -v cu="${14:-0}" -v cs="${15:-0}" -v k="$clk" -v d="$DUR" \
  'BEGIN{sec=(u+s+cu+cs)/k; printf "[loader-sampler] overhead: %.2fs CPU over %ss => %.2f%% of one core\n", sec, d, (d>0?sec/d*100:0)}' >&2
