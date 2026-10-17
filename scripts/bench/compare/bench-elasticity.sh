#!/bin/bash
# bench-elasticity.sh — what a membership change costs the nodes that are
# already running. One node leaves the filesystem cleanly and later rejoins it,
# while every other node is writing the whole time, and the numbers are taken on
# those *other* nodes: how long their I/O stalled, and how much bandwidth they
# lost across the event.
#
# This is the "stop the world" question, made measurable. GFS2 (and OCFS2)
# suspend the DLM lockspace on every membership change: each surviving node
# stops granting locks until the new membership is agreed, so a node arriving or
# departing is felt by everyone. etcfs has no lockspace to suspend — membership
# is an etcd key, a node claims its own arena, and a clean leave hands back its
# locks in the same transaction that removes it — so the expected cost to the
# survivors is nothing. The three server-mediated or replicated backends are run
# for the same event because a client mount is genuinely cheap for them, which
# is worth showing rather than assuming.
#
# What "leave" and "join" mean per backend is compare_leave_fs/compare_join_fs
# (compare-backends.sh); the header there lists them. Nothing here is a kill —
# a crash is bench-node-kill.sh's scenario, and mixing the two would measure
# fencing rather than elasticity.
#
# The probe file is one the leaver itself was writing right up to its departure,
# so its locks on that inode really do have to move; a probe on an untouched
# file would let a backend look elastic by never having to transfer anything.
#
# ETCFS_ELASTIC_RUNTIME (default 30) is the seconds of load per phase.
# ETCFS_ELASTIC_WAIT (default 60) is how long the probe keeps running after the
# event, and therefore the largest stall this can report.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-elasticity.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_ELASTIC_RUNTIME:-30}"
WAIT_AFTER="${ETCFS_ELASTIC_WAIT:-60}"

# Three nodes with the filesystem mounted: one to cycle, two to keep writing.
compare_client_nodes "${ETCFS_ELASTIC_NODES:-3}"

compare_begin
compare_mount
# aggregate_mibps globs these JSONs, so a previous run's files would be summed in.
rm -f "$RESULTS_DIR"/elastic-*.json
[[ "${#BENCH_NODES[@]}" -ge 3 ]] || die "elasticity needs a node to cycle and two to keep writing"

JOINER="$(compare_elastic_joiner)"
SURVIVORS=()
for ip in "${BENCH_NODES[@]}"; do
    [[ "$ip" == "$JOINER" ]] || SURVIVORS+=("$ip")
done
PROBE_NODE="${SURVIVORS[0]}"
PROBE_PATH="$MOUNT_PATH/elastic-probe.dat"

log "joiner=$JOINER survivors=${SURVIVORS[*]} probe=$PROBE_PATH"

LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/elastic-load-@NODE@.dat
size=1G
runtime=$RUNTIME
time_based=1
group_reporting=1
[seqwrite]
rw=write
"

baseline=$(compare_parallel_fio "elastic-baseline" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}")

# aggregate_mibps <label> — the same sum compare_parallel_fio prints, read back
# off the JSONs a backgrounded run left behind (a subshell's variable would not
# survive, and the load has to overlap the event to mean anything).
aggregate_mibps() {
    jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' \
        "$RESULTS_DIR/$1"-*.json
}

# ---- phase 1: the leave ----
#
# The leaver writes the probe file right up to the moment it goes, so it holds
# that inode's lock when it leaves.
$SSH_CMD "ec2-user@$JOINER" "sudo touch $PROBE_PATH && sudo dd if=/dev/zero of=$PROBE_PATH bs=4k count=64 conv=notrunc oflag=direct >/dev/null 2>&1; true"
compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
compare_parallel_fio "elastic-leave" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}" >/dev/null &
load_pid=$!
sleep 3

T_LEAVE=$(compare_epoch)
log "removing $JOINER from the filesystem at $T_LEAVE"
compare_leave_fs "$JOINER"
sleep "$WAIT_AFTER"
compare_probe_stop "$PROBE_NODE"
wait "$load_pid"
during_leave=$(aggregate_mibps elastic-leave)
read -r _ leave_stall leave_errs < <(compare_probe_recovery "$PROBE_NODE" "$T_LEAVE")

# ---- phase 2: the join ----
compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
compare_parallel_fio "elastic-join" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}" >/dev/null &
load_pid=$!
sleep 3

T_JOIN=$(compare_epoch)
log "putting $JOINER back into the filesystem at $T_JOIN"
join_s=$(compare_join_fs "$JOINER")
sleep "$WAIT_AFTER"
compare_probe_stop "$PROBE_NODE"
wait "$load_pid"
during_join=$(aggregate_mibps elastic-join)
read -r _ join_stall join_errs < <(compare_probe_recovery "$PROBE_NODE" "$T_JOIN")

pct() { awk -v b="$1" -v d="$2" 'BEGIN{ if (b+0 == 0) print "0"; else printf "%.2f", (b-d)/b*100 }'; }

compare_headline elasticity survivor_baseline_mibps "$baseline" MiB/s
compare_headline elasticity leave_max_stall_s "$leave_stall" s
compare_headline elasticity leave_failed_ops "$leave_errs" ops
compare_headline elasticity survivor_during_leave_mibps "$during_leave" MiB/s
compare_headline elasticity leave_impact_pct "$(pct "$baseline" "$during_leave")" %
compare_headline elasticity join_s "$join_s" s
compare_headline elasticity join_max_stall_s "$join_stall" s
compare_headline elasticity join_failed_ops "$join_errs" ops
compare_headline elasticity survivor_during_join_mibps "$during_join" MiB/s
compare_headline elasticity join_impact_pct "$(pct "$baseline" "$during_join")" %
