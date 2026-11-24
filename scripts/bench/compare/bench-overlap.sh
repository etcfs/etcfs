#!/bin/bash
# bench-overlap.sh — two membership changes landing in the same second,
# measured on the nodes that stay.
#
#   double-join: two nodes leave one after the other and settle, then both
#                rejoin at once.
#   join-leave:  one node is out; it rejoins at the same instant another leaves.
#
# On GFS2 the second change arrives inside the first one's Corosync
# synchronisation, which aborts and restarts it; on etcfs each change is its own
# etcd key and arena claim, so the two should not see each other. The survivors
# write throughout and a survivor runs the 4 KiB probe; the number is their
# worst stall from the overlapping instant on, plus each join's own time.
#
# Needs four nodes: two to churn, two to keep writing.
#
# ETCFS_ELASTIC_WAIT (default 60) — seconds of probe after the event, and the
# settle time between the setup leaves.
#
# Usage:
#   COMPARE_BACKEND=gfs2 ./bench-overlap.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

WAIT_AFTER="${ETCFS_ELASTIC_WAIT:-60}"
compare_client_nodes "${ETCFS_ELASTIC_NODES:-4}"
compare_begin
compare_mount
rm -f "$RESULTS_DIR"/overlap-*.json
[[ "${#BENCH_NODES[@]}" -ge 4 ]] || die "overlap needs two nodes to churn and two to keep writing"
[[ "$COMPARE_BACKEND_BASE" != "nfs" ]] || die "overlap churns two clients; the nfs server is not one"

A="${BENCH_NODES[-1]}"
B="${BENCH_NODES[-2]}"
SURVIVORS=("${BENCH_NODES[@]:0:$((${#BENCH_NODES[@]} - 2))}")
PROBE_NODE="${SURVIVORS[0]}"
PROBE_PATH="$MOUNT_PATH/overlap-probe.dat"
log "churners=$A,$B survivors=${SURVIVORS[*]}"

LOAD_SECS=$((WAIT_AFTER + 20))
LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/overlap-load-@NODE@.dat
size=1G
runtime=$LOAD_SECS
time_based=1
[seqwrite]
rw=write
"

# measure <name> <command...> — run the overlapping event under load and probe.
measure() {
    local name="$1"; shift
    compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
    compare_parallel_fio "overlap-$name" "$LOAD_SECS" "$LOAD_JOB" "${SURVIVORS[@]}" >/dev/null &
    local load_pid=$! t0
    sleep 5
    [[ "$COMPARE_BACKEND_BASE" == gfs2 ]] && compare_gfs2_dmesg_mark "${SURVIVORS[@]}"
    t0=$(compare_epoch)
    log "overlap $name at $t0"
    "$@"
    sleep "$WAIT_AFTER"
    compare_probe_stop "$PROBE_NODE"
    wait "$load_pid"
    local stall errs
    read -r _ stall errs < <(compare_probe_recovery "$PROBE_NODE" "$t0")
    if [[ "$COMPARE_BACKEND_BASE" == gfs2 ]]; then compare_gfs2_recoveries "overlap-$name" "${SURVIVORS[@]}"; fi
    compare_headline "overlap-$name" max_stall_s "$stall" s
    compare_headline "overlap-$name" failed_ops "$errs" ops
    compare_headline "overlap-$name" survivor_mibps \
        "$(jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' \
            "$RESULTS_DIR"/overlap-"$name"-*.json)" MiB/s
}

# Both joins in parallel; each one's time lands in its own file, because a
# backgrounded function's output cannot come back as a variable.
double_join() {
    compare_join_fs "$A" > "$RESULTS_DIR/overlap-join-A.s" &
    local pa=$!
    compare_join_fs "$B" > "$RESULTS_DIR/overlap-join-B.s" &
    wait "$pa" "$!"
}

join_while_leave() {
    compare_join_fs "$A" > "$RESULTS_DIR/overlap-join-A.s" &
    local pa=$!
    compare_leave_fs "$B"
    wait "$pa"
}

# The leaver holds the probe inode's lock when it goes.
$SSH_CMD "ec2-user@$A" "sudo dd if=/dev/zero of=$PROBE_PATH bs=4k count=64 conv=notrunc oflag=direct >/dev/null 2>&1; true"
compare_leave_fs "$A"; sleep "$WAIT_AFTER"
compare_leave_fs "$B"; sleep "$WAIT_AFTER"
measure double-join double_join
compare_headline overlap-double-join join_a_s "$(cat "$RESULTS_DIR/overlap-join-A.s")" s
compare_headline overlap-double-join join_b_s "$(cat "$RESULTS_DIR/overlap-join-B.s")" s

compare_leave_fs "$A"; sleep "$WAIT_AFTER"
measure join-leave join_while_leave
compare_headline overlap-join-leave join_s "$(cat "$RESULTS_DIR/overlap-join-A.s")" s
