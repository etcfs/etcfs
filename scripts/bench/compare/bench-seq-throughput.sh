#!/bin/bash
# bench-seq-throughput.sh — large sequential I/O, the workload where a direct
# path to the shared device should beat a network file service if it beats it
# anywhere. Every backend runs the same jobs: psync, O_DIRECT, 1 MiB blocks,
# written and then read back, one job and four jobs on one node, then one job
# on every node at once. psync because EtcFS is a FUSE filesystem driven one
# request deep per job; giving the others a deeper queue would measure the
# queue, not the path.
#
# For etcfs the raw device is measured first, with the same jobs, as the
# ceiling the volume and instance allow — before the filesystem touches it.
#
# The instance's EBS and network allowances are a burst over a much lower
# baseline, so a short run measures the burst. ETCFS_BENCH_RUNTIME=1200 runs the
# one-job legs long enough to cross into the baseline; the per-10 s bandwidth
# log of each leg is kept so the drop is visible rather than averaged away.
#
# The volume must hold the 8 GiB one-job file and leave the allocator room, and
# 1000 IOPS would cap io2 at ~256 MiB/s (0.256 MiB/s per IOPS) below the
# instance's burst: run with 8000 IOPS and 64 GiB.
#
# Usage:
#   ETCFS_VOLUME_IOPS=8000 ETCFS_VOLUME_SIZE=64 COMPARE_BACKEND=etcfs ./bench-seq-throughput.sh
#   COMPARE_BACKEND=efs ./bench-seq-throughput.sh
#   ETCFS_VOLUME_IOPS=8000 ETCFS_VOLUME_SIZE=64 ETCFS_BENCH_RUNTIME=1200 COMPARE_BACKEND=etcfs ./bench-seq-throughput.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-60}"
# ETCFS_SEQ_LEGS=agg runs only the multi-node aggregate leg.
LEGS="${ETCFS_SEQ_LEGS:-all}"
LONG=0
[[ "$RUNTIME" -ge 600 ]] && LONG=1

# seq_job <rw> <filename-or-directory line> <numjobs> <size> <label>
seq_job() {
    local rw="$1" target="$2" jobs="$3" size="$4" label="$5"
    printf '[global]\nioengine=psync\ndirect=1\nbs=1M\n%s\nsize=%s\nruntime=%s\ntime_based=1\nnumjobs=%s\ngroup_reporting=1\nwrite_bw_log=/tmp/%s\nlog_avg_msec=10000\n[%s]\nrw=%s\n' \
        "$target" "$size" "$RUNTIME" "$jobs" "$label" "$label" "$rw"
}

# run <label> <ip> <job> <direction> — one fio leg, its headline, and its
# bandwidth log next to the JSON.
run() {
    local label="$1" ip="$2" job="$3" dir="$4" json
    json=$(compare_run_job "$label" "$ip" "$RUNTIME" "$job")
    scp $SSH_OPTS "ec2-user@$ip:/tmp/${label}_bw.*.log" "$RESULTS_DIR/" >/dev/null 2>&1 || true
    compare_headline seq-throughput "${label}_mibps" "$(compare_fio_bw_mibps "$json" "$dir")" MiB/s
}

# agg_write — one writer per node on its own file, every node starting at the
# same second of the first node's clock (EC2 instances sync to the Amazon Time
# Sync Service), so the summed bandwidth covers one shared window. Launching
# the nodes one SSH call after another staggers them by seconds, and summing
# averages of runs that only partly overlap overstates the aggregate.
agg_write() {
    local start i ip pids=() jsons=() label
    for i in "${!BENCH_NODES[@]}"; do
        seq_job write "filename=$MOUNT_PATH/agg-$i.dat" 1 4G "agg-write-$i" \
            | $SSH_CMD "ec2-user@${BENCH_NODES[$i]}" "cat > /tmp/agg-write-$i.fio"
    done
    start=$(( $($SSH_CMD "ec2-user@${BENCH_NODES[0]}" date +%s) + 20 ))
    for i in "${!BENCH_NODES[@]}"; do
        ip="${BENCH_NODES[$i]}" label="agg-write-$i"
        (
            timeout "$((RUNTIME * 4 + 120))" $SSH_CMD "ec2-user@$ip" \
                "sleep \$(( $start - \$(date +%s) )); sudo fio /tmp/$label.fio --output-format=json --output=/tmp/$label.json" >/dev/null \
                && scp $SSH_OPTS "ec2-user@$ip:/tmp/$label.json" "$RESULTS_DIR/$label.json" >/dev/null
        ) &
        pids+=($!)
        jsons+=("$RESULTS_DIR/$label.json")
    done
    for i in "${!pids[@]}"; do wait "${pids[$i]}" || die "agg_write: node $i failed"; done
    jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' "${jsons[@]}"
}

compare_begin
N0="${COMPARE_PUB_IPS[0]}"

if [[ "$COMPARE_BACKEND_BASE" == "etcfs" && "$LEGS" == all ]]; then
    compare_install_fio "$N0"
    RAW_DEV=$(compare_shared_device "$N0")
    log "Raw-device ceiling on $RAW_DEV (before EtcFS touches it)..."
    run raw-write-j1 "$N0" "$(seq_job write "filename=$RAW_DEV" 1 8G raw-write-j1)" write
    run raw-read-j1 "$N0" "$(seq_job read "filename=$RAW_DEV" 1 8G raw-read-j1)" read
fi

compare_mount 1
N0="${BENCH_NODES[0]}"

if [[ "$LEGS" == agg ]]; then
    compare_headline seq-throughput agg_write_mibps "$(agg_write)" MiB/s
    exit 0
fi

run fs-write-j1 "$N0" "$(seq_job write "filename=$MOUNT_PATH/seq-j1.dat" 1 8G fs-write-j1)" write
run fs-read-j1 "$N0" "$(seq_job read "filename=$MOUNT_PATH/seq-j1.dat" 1 8G fs-read-j1)" read

# Each leg's files are removed before the next, so no leg runs on a volume the
# previous one filled.
$SSH_CMD "ec2-user@$N0" "sudo rm -f $MOUNT_PATH/seq-j1.dat"

if [[ "$LONG" == 0 ]]; then
    run fs-write-j4 "$N0" "$(seq_job write "directory=$MOUNT_PATH" 4 2G fs-write-j4)" write
    run fs-read-j4 "$N0" "$(seq_job read "directory=$MOUNT_PATH" 4 2G fs-read-j4)" read
    $SSH_CMD "ec2-user@$N0" "sudo rm -f $MOUNT_PATH/fs-write-j4.*"
    # One writer per node on its own file: the aggregate the shared volume (or
    # the file service) delivers to the whole cluster at once.
    compare_headline seq-throughput agg_write_mibps "$(agg_write)" MiB/s
fi
