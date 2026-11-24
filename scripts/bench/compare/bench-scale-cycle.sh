#!/bin/bash
# bench-scale-cycle.sh — what an autoscaler does to a cluster, without an
# autoscaler: a brand-new instance is launched and joined to the running
# filesystem, then taken out and terminated, CYCLES times, while the original
# three nodes write throughout and one of them runs the 4 KiB probe.
#
# The join and leave are compare-scale.sh's, per backend — the same steps an
# autoscaler's launch and termination hooks would have to run, each one timed
# against a readiness poll rather than a fixed sleep:
#
#   etcfs  launch, ssh, attach, install (copy the running build), member_add,
#          etcd_start, meta_start, fuse_start, first_write (arena claim is lazy,
#          so it is in here); out: leave, member_remove, detach, terminate
#   gfs2   launch, ssh, attach, install, conf_write (corosync.conf on every
#          node), corosync_reload, corosync_join, dlm_start, jadd (first cycle
#          only — a removed node's journal is reused), mount, first_write; out:
#          leave, conf_write, corosync_reload, detach, terminate
#
# Every step is a headline out_<step>_s / in_<step>_s, plus out_total_s and
# in_total_s, and the survivors' worst stall over each (survivor_max_stall_out_s,
# _in_s). etcfs also reports membership_gone_s and arenas_released_s: from the
# start of the leave until etcd no longer lists the node, and until it owns no
# arena (-1 if either outlasts ETCFS_SCALE_WAIT).
#
# ETCFS_SCALE_CYCLES (default 3), ETCFS_SCALE_WAIT (default 120).
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-scale-cycle.sh
#   COMPARE_BACKEND=gfs2  ./bench-scale-cycle.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
source "$SCRIPT_DIR/compare-scale.sh"
case "$COMPARE_BACKEND" in
    etcfs|gfs2) ;;
    *) die "bench-scale-cycle.sh runs etcfs or gfs2 (got $COMPARE_BACKEND)" ;;
esac

CYCLES="${ETCFS_SCALE_CYCLES:-3}"
WAIT="${ETCFS_SCALE_WAIT:-120}"

compare_client_nodes 3
compare_begin
compare_mount
rm -f "$RESULTS_DIR"/scale-*.json "$RESULTS_DIR"/scale-marks-*.log
BASE=("${BENCH_NODES[@]}")
PROBE_NODE="${BASE[0]}"
"scale_${COMPARE_BACKEND}_prepare"

# max_stall <from> <to> — longest gap between probe successes in the window.
max_stall() {
    $SSH_CMD "ec2-user@$PROBE_NODE" "sudo cat /tmp/probe.log" > "$RESULTS_DIR/probe-$PROBE_NODE.log"
    awk -v a="$1" -v b="$2" '$2 == "ok" && $1 >= a && $1 <= b { if (p != "" && $1 - p > g) g = $1 - p; p = $1 }
        END { printf "%.3f", g + 0 }' "$RESULTS_DIR/probe-$PROBE_NODE.log"
}

LOAD_SECS=$(( CYCLES * 900 ))
LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/scale-load-@NODE@.dat
size=1G
runtime=$LOAD_SECS
time_based=1
[seqwrite]
rw=write
"
compare_probe_start "$PROBE_NODE" "$MOUNT_PATH/scale-probe.dat"
compare_parallel_fio "scale-load" "$LOAD_SECS" "$LOAD_JOB" "${BASE[@]}" >/dev/null &
load_pid=$!
sleep 5

for c in $(seq 1 "$CYCLES"); do
    S="scale-cycle$c"

    SCALE_MARKS="$RESULTS_DIR/scale-marks-$c-out.log"; : > "$SCALE_MARKS"
    t0=$(compare_epoch)
    "scale_add_${COMPARE_BACKEND}"
    t1=$(compare_epoch)
    scale_report "$S" out_
    compare_headline "$S" survivor_max_stall_out_s "$(max_stall "$t0" "$t1")" s

    SCALE_MARKS="$RESULTS_DIR/scale-marks-$c-in.log"; : > "$SCALE_MARKS"
    t0=$(compare_epoch)
    if [[ "$COMPARE_BACKEND" == etcfs ]]; then
        # Watched in the background: the key and the arenas go while the
        # removal is still detaching and terminating.
        (
            gone=-1 released=-1 deadline=$((SECONDS + WAIT))
            while [[ $SECONDS -lt $deadline && ( "$gone" == -1 || "$released" == -1 ) ]]; do
                now=$(compare_epoch)
                [[ "$gone" != -1 ]] || ! scale_etcfs_member_gone || gone=$(awk -v s="$t0" -v e="$now" 'BEGIN{printf "%.3f", e-s}')
                [[ "$released" != -1 ]] || ! scale_etcfs_arenas_gone || released=$(awk -v s="$t0" -v e="$now" 'BEGIN{printf "%.3f", e-s}')
                sleep 0.5
            done
            echo "$gone $released"
        ) > "$RESULTS_DIR/scale-gone-$c.txt" &
        watch_pid=$!
    fi
    "scale_remove_${COMPARE_BACKEND}"
    t1=$(compare_epoch)
    scale_report "$S" in_
    compare_headline "$S" survivor_max_stall_in_s "$(max_stall "$t0" "$t1")" s
    if [[ "$COMPARE_BACKEND" == etcfs ]]; then
        wait "$watch_pid"
        read -r gone released < "$RESULTS_DIR/scale-gone-$c.txt"
        compare_headline "$S" membership_gone_s "$gone" s
        compare_headline "$S" arenas_released_s "$released" s
    fi
done

compare_probe_stop "$PROBE_NODE"
for ip in "${BASE[@]}"; do $SSH_CMD "ec2-user@$ip" "sudo pkill fio" >/dev/null 2>&1 || true; done
wait "$load_pid" || true
