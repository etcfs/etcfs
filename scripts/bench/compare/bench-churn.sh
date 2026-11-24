#!/bin/bash
# bench-churn.sh — repeated membership changes at a fixed rate, measured on the
# nodes that stay. One node leaves cleanly and rejoins, CYCLES times, with
# INTERVAL seconds between the start of one change and the start of the next,
# while the survivors write and a survivor runs the 4 KiB probe throughout.
#
# Two questions, one script:
#   - Repeated leave/rejoin on every backend (bench-rejoin-load.sh is
#     etcfs-only): per cycle, the survivors' worst stall across that leave and
#     that join.
#   - The churn rate sweep: run it at INTERVAL = 60, 30, 10, 5, 2. When a change
#     lands inside the previous one's synchronisation, Corosync aborts and
#     restarts it, so GFS2's stall is expected to grow faster than the rate;
#     etcfs has no lockspace to suspend and should stay flat.
#
# A join that takes longer than INTERVAL is not cut short: the next change
# starts as soon as it finishes, and achieved_interval_s reports the rate the
# cluster actually sustained, which is itself a result at the low end.
#
# On gfs2 the kernel's dlm_recover lines are collected from the survivors
# (compare_gfs2_recoveries): finished recoveries against started ones shows how
# many a following change interrupted.
#
# ETCFS_CHURN_INTERVAL (default 30), ETCFS_CHURN_CYCLES (default 6),
# ETCFS_ELASTIC_RUNTIME (baseline load, 30), ETCFS_ELASTIC_NODES (3).
#
# Usage:
#   COMPARE_BACKEND=gfs2 ETCFS_CHURN_INTERVAL=5 ./bench-churn.sh
#   for i in 60 30 10 5 2; do ETCFS_CHURN_INTERVAL=$i ./bench-churn.sh; done
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

INTERVAL="${ETCFS_CHURN_INTERVAL:-30}"
CYCLES="${ETCFS_CHURN_CYCLES:-6}"
RUNTIME="${ETCFS_ELASTIC_RUNTIME:-30}"
SCENARIO="churn-${INTERVAL}s"

compare_client_nodes "${ETCFS_ELASTIC_NODES:-3}"
compare_begin
compare_mount
rm -f "$RESULTS_DIR"/churn-*.json
[[ "${#BENCH_NODES[@]}" -ge 3 ]] || die "churn needs a node to cycle and two to keep writing"

CHURNER="$(compare_elastic_joiner)"
SURVIVORS=()
for ip in "${BENCH_NODES[@]}"; do
    [[ "$ip" == "$CHURNER" ]] || SURVIVORS+=("$ip")
done
PROBE_NODE="${SURVIVORS[0]}"
PROBE_PATH="$MOUNT_PATH/churn-probe.dat"
log "churner=$CHURNER survivors=${SURVIVORS[*]} interval=${INTERVAL}s cycles=$CYCLES"

load_job() {
    cat <<EOF
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/churn-load-@NODE@.dat
size=1G
runtime=$1
time_based=1
group_reporting=1
[seqwrite]
rw=write
EOF
}

baseline=$(compare_parallel_fio "churn-baseline" "$RUNTIME" "$(load_job "$RUNTIME")" "${SURVIVORS[@]}")

# The churner holds the probe inode's lock when it first leaves, so the first
# leave has a lock to hand over rather than none.
$SSH_CMD "ec2-user@$CHURNER" "sudo dd if=/dev/zero of=$PROBE_PATH bs=4k count=64 conv=notrunc oflag=direct >/dev/null 2>&1; true"

# Load covers every cycle plus a tail, so the last join is measured under load.
LOAD_SECS=$((CYCLES * 2 * INTERVAL + 60))
compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
compare_parallel_fio "churn-load" "$LOAD_SECS" "$(load_job "$LOAD_SECS")" "${SURVIVORS[@]}" >/dev/null &
load_pid=$!
sleep 5

# events: "<epoch> leave|join" in order, the windows the per-cycle stall is cut by.
EVENTS="$RESULTS_DIR/churn-events.log"
: > "$EVENTS"
pace() {  # sleep out the rest of INTERVAL since $1, if any is left
    awk -v s="$1" -v i="$INTERVAL" -v n="$(compare_epoch)" 'BEGIN{d=s+i-n; printf "%.3f", (d > 0 ? d : 0)}'
}
[[ "$COMPARE_BACKEND_BASE" == gfs2 ]] && compare_gfs2_dmesg_mark "${SURVIVORS[@]}"
T_START=$(compare_epoch)
total_join=0
for c in $(seq 1 "$CYCLES"); do
    t=$(compare_epoch); echo "$t leave" >> "$EVENTS"
    compare_leave_fs "$CHURNER"
    sleep "$(pace "$t")"
    t=$(compare_epoch); echo "$t join" >> "$EVENTS"
    js=$(compare_join_fs "$CHURNER")
    total_join=$(awk -v a="$total_join" -v b="$js" 'BEGIN{printf "%.3f", a+b}')
    log "cycle $c: rejoin ${js}s"
    sleep "$(pace "$t")"
done
T_END=$(compare_epoch)
sleep 10
echo "$(compare_epoch) end" >> "$EVENTS"

compare_probe_stop "$PROBE_NODE"
wait "$load_pid"
read -r _ worst_stall errs < <(compare_probe_recovery "$PROBE_NODE" "$T_START")
during=$(jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' \
    "$RESULTS_DIR"/churn-load-*.json)

# Per cycle: the longest gap between probe successes from that cycle's leave
# to the next cycle's leave (the last cycle runs to "end").
awk 'NR==FNR { if ($2 == "leave" || $2 == "end") b[++n] = $1; next }
     $2 == "ok" { ok[++m] = $1 }
     END {
        for (c = 1; c < n; c++) {
            g = 0; prev = ""
            for (i = 1; i <= m; i++) {
                if (ok[i] < b[c]) { prev = ok[i]; continue }
                if (ok[i] > b[c+1]) break
                if (prev != "" && ok[i] - prev > g) g = ok[i] - prev
                prev = ok[i]
            }
            printf "%d %.3f\n", c, g
        }
     }' "$EVENTS" "$RESULTS_DIR/probe-$PROBE_NODE.log" > "$RESULTS_DIR/churn-per-cycle.txt"
while read -r c g; do
    compare_headline "$SCENARIO" "cycle${c}_max_stall_s" "$g" s
done < "$RESULTS_DIR/churn-per-cycle.txt"


[[ "$COMPARE_BACKEND_BASE" == gfs2 ]] && compare_gfs2_recoveries "$SCENARIO" "${SURVIVORS[@]}"
[[ "$COMPARE_BACKEND_BASE" == gfs2 ]] && compare_headline "$SCENARIO" active_locks "$(compare_gfs2_lock_count "$PROBE_NODE")" glocks
compare_headline "$SCENARIO" interval_s "$INTERVAL" s
compare_headline "$SCENARIO" achieved_interval_s \
    "$(awk -v s="$T_START" -v e="$T_END" -v n="$((CYCLES * 2))" 'BEGIN{printf "%.2f", (e-s)/n}')" s
compare_headline "$SCENARIO" mean_join_s "$(compare_div "$total_join" "$CYCLES" 3)" s
compare_headline "$SCENARIO" worst_stall_s "$worst_stall" s
compare_headline "$SCENARIO" failed_ops "$errs" ops
compare_headline "$SCENARIO" survivor_baseline_mibps "$baseline" MiB/s
compare_headline "$SCENARIO" survivor_during_mibps "$during" MiB/s
compare_headline "$SCENARIO" bandwidth_lost_pct \
    "$(awk -v b="$baseline" -v d="$during" 'BEGIN{ if (b+0 == 0) print "0"; else printf "%.2f", (b-d)/b*100 }')" %
