#!/bin/bash
# bench-gfs2-journal-add.sh — what it costs GFS2 to admit a node that has no
# journal, and what the nodes already running feel while it happens.
#
# GFS2 needs one journal per mounting node. The filesystem is formatted with one
# journal fewer than there are nodes, so the last node is refused at mount. The
# admission is then the operator's sequence: gfs2_jadd on a mounted survivor,
# then the mount. The survivors write and run the 4 KiB probe throughout, and
# the numbers are their stall and bandwidth loss across that sequence, plus the
# time each step takes.
#
# The joiner is already a corosync member, so this measures the journal step
# only; adding a node to corosync's nodelist is a further configuration change
# on every node, which this does not time.
#
# Usage:
#   COMPARE_BACKEND=gfs2 ./bench-gfs2-journal-add.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-gfs2}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND" == "gfs2" ]] || die "bench-gfs2-journal-add.sh is gfs2-only (see header)"

RUNTIME="${ETCFS_ELASTIC_RUNTIME:-30}"
WAIT_AFTER="${ETCFS_ELASTIC_WAIT:-60}"
NODES="${ETCFS_ELASTIC_NODES:-3}"

compare_client_nodes "$NODES"
export COMPARE_GFS2_JOURNALS="$((NODES - 1))"

compare_begin
compare_mount
# The aggregate below globs this scenario's fio JSONs, so a previous run's
# files left in the results directory would be summed into this one.
rm -f "$RESULTS_DIR"/jadd-*.json
JOINER="${BENCH_NODES[-1]}"
SURVIVORS=("${BENCH_NODES[@]:0:$((${#BENCH_NODES[@]} - 1))}")
PROBE_NODE="${SURVIVORS[0]}"
PROBE_PATH="$MOUNT_PATH/jadd-probe.dat"
log "journals=$COMPARE_GFS2_JOURNALS joiner=$JOINER survivors=${SURVIVORS[*]}"

# The refusal itself, recorded rather than assumed.
refused=0
if ! $SSH_CMD "ec2-user@$JOINER" "sudo mkdir -p $MOUNT_PATH && timeout 60 $COMPARE_REMOTE_MOUNT_CMD" \
        > "$RESULTS_DIR/jadd-refused-mount.log" 2>&1; then
    refused=1
fi
log "mount without a free journal refused=$refused ($(tr '\n' ' ' < "$RESULTS_DIR/jadd-refused-mount.log"))"
[[ "$refused" == 1 ]] || die "joiner mounted without a free journal; the scenario does not apply"

LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/jadd-load-@NODE@.dat
size=1G
runtime=$RUNTIME
time_based=1
group_reporting=1
[seqwrite]
rw=write
"
baseline=$(compare_parallel_fio "jadd-baseline" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}")

compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
compare_parallel_fio "jadd-during" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}" >/dev/null &
load_pid=$!
sleep 3

T0=$(compare_epoch)
log "gfs2_jadd on $PROBE_NODE at $T0"
jadd_s=$($SSH_CMD "ec2-user@$PROBE_NODE" "
    s=\$(date +%s.%N)
    sudo gfs2_jadd -j 1 $MOUNT_PATH >&2 || exit 1
    e=\$(date +%s.%N)
    awk -v s=\$s -v e=\$e 'BEGIN{printf \"%.3f\", e-s}'
" 2> "$RESULTS_DIR/jadd.log") || die "gfs2_jadd failed: $(cat "$RESULTS_DIR/jadd.log")"
mount_s=$(compare_join_fs "$JOINER")
log "jadd ${jadd_s}s, mount ${mount_s}s"

sleep "$WAIT_AFTER"
compare_probe_stop "$PROBE_NODE"
wait "$load_pid"
during=$(jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' \
    "$RESULTS_DIR"/jadd-during-*.json)
read -r _ stall errs < <(compare_probe_recovery "$PROBE_NODE" "$T0")
impact=$(awk -v b="$baseline" -v d="$during" 'BEGIN{ if (b+0 == 0) print "0"; else printf "%.2f", (b-d)/b*100 }')

compare_headline journal-add jadd_s "$jadd_s" s
compare_headline journal-add mount_after_jadd_s "$mount_s" s
compare_headline journal-add total_admit_s "$(awk -v a="$jadd_s" -v b="$mount_s" 'BEGIN{printf "%.3f", a+b}')" s
compare_headline journal-add max_stall_s "$stall" s
compare_headline journal-add failed_ops "$errs" ops
compare_headline journal-add survivor_baseline_mibps "$baseline" MiB/s
compare_headline journal-add survivor_during_mibps "$during" MiB/s
compare_headline journal-add impact_pct "$impact" %
