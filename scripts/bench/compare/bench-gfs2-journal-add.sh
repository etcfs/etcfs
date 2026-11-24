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
# By default the joiner is already a corosync member, so this measures the
# journal step only. ETCFS_JADD_FULL=1 leaves the joiner out of corosync as well
# and times the whole sequence an operator runs for a brand-new node: rewrite
# corosync.conf on every node, `corosync-cfgtool -R` on a member, start corosync
# and dlm on the joiner and wait for it to be counted, then gfs2_jadd and mount.
# The survivors' stall is measured from the first of those steps, so a stall
# caused by the reload shows up in it. conf_write_s is wall time from this
# machine (one ssh per node, so it is mostly ssh); every other step is timed on
# the node.
#
# Usage:
#   COMPARE_BACKEND=gfs2 ./bench-gfs2-journal-add.sh
#   COMPARE_BACKEND=gfs2 ETCFS_JADD_FULL=1 ./bench-gfs2-journal-add.sh
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
FULL="${ETCFS_JADD_FULL:-0}"
[[ "$FULL" == 1 ]] && export COMPARE_GFS2_MEMBERS="$((NODES - 1))"
SCENARIO=journal-add
[[ "$FULL" == 1 ]] && SCENARIO=journal-add-full

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

# The joiner must have the shared device before its refusal means anything:
# an attachment that did not land at provisioning fails the mount for that
# reason instead, which once passed this check and failed the admission later.
compare_reattach_volume_if_missing "$JOINER"

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

compare_gfs2_dmesg_mark "${SURVIVORS[@]}"
T0=$(compare_epoch)
if [[ "$FULL" == 1 ]]; then
    log "adding $JOINER to corosync at $T0"
    conf=$(compare_gfs2_corosync_conf "${COMPARE_PRIV_IPS[@]}")
    for ip in "${BENCH_NODES[@]}"; do
        echo "$conf" | $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/corosync && sudo tee /etc/corosync/corosync.conf >/dev/null"
    done
    conf_s=$(awk -v s="$T0" -v e="$(compare_epoch)" 'BEGIN{printf "%.3f", e-s}')
    reload_s=$(compare_remote_time "$PROBE_NODE" "sudo corosync-cfgtool -R")
    member_s=$($SSH_CMD "ec2-user@$JOINER" "
        s=\$(date +%s.%N)
        sudo systemctl enable --now corosync >/dev/null 2>&1 || exit 1
        deadline=\$((\$(date +%s) + 120))
        until [ \"\$(sudo corosync-quorumtool -s 2>/dev/null | awk '/^Nodes:/{print \$2}')\" = $NODES ]; do
            [ \$(date +%s) -lt \$deadline ] || exit 1
            sleep 0.05
        done
        sudo systemctl enable --now dlm >/dev/null 2>&1 || exit 1
        e=\$(date +%s.%N)
        awk -v s=\$s -v e=\$e 'BEGIN{printf \"%.3f\", e-s}'
    ") || die "$JOINER never became a corosync member"
    log "conf ${conf_s}s, reload ${reload_s}s, member+dlm ${member_s}s"
fi
log "gfs2_jadd on $PROBE_NODE at $(compare_epoch)"
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

compare_gfs2_recoveries "$SCENARIO" "${SURVIVORS[@]}"
compare_headline "$SCENARIO" jadd_s "$jadd_s" s
compare_headline "$SCENARIO" mount_after_jadd_s "$mount_s" s
if [[ "$FULL" == 1 ]]; then
    compare_headline "$SCENARIO" conf_write_s "$conf_s" s
    compare_headline "$SCENARIO" corosync_reload_s "$reload_s" s
    compare_headline "$SCENARIO" joiner_member_s "$member_s" s
    total=$(awk -v a="$conf_s" -v b="$reload_s" -v c="$member_s" -v d="$jadd_s" -v e="$mount_s" 'BEGIN{printf "%.3f", a+b+c+d+e}')
else
    total=$(awk -v a="$jadd_s" -v b="$mount_s" 'BEGIN{printf "%.3f", a+b}')
fi
compare_headline "$SCENARIO" total_admit_s "$total" s
compare_headline "$SCENARIO" max_stall_s "$stall" s
compare_headline "$SCENARIO" failed_ops "$errs" ops
compare_headline "$SCENARIO" survivor_baseline_mibps "$baseline" MiB/s
compare_headline "$SCENARIO" survivor_during_mibps "$during" MiB/s
compare_headline "$SCENARIO" impact_pct "$impact" %
