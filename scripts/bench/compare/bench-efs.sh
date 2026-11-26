#!/bin/bash
# bench-efs.sh — Amazon EFS (elastic throughput by default) on a 3-node
# cluster, with the matched-load job the etcfs and GFS2 tail figures use:
# psync, four jobs at depth 1, one 8 MiB file per job, direct=1, 60 s per
# stanza. Four requests in flight on every backend, so the p99 columns compare
# like with like.
#
# Unlike the other bench-<backend>.sh scripts it also writes headline.json, so
# bench-repeat.sh can run it as a series.
#
# Usage:
#   ./bench-efs.sh
#   ETCFS_EFS_PROVISIONED_MIBPS=256 ./bench-efs.sh
set -euo pipefail

export COMPARE_BACKEND=efs
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
compare_mount

run_fio efs "directory=$MOUNT_PATH
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync 4 1 "${ETCFS_BENCH_RUNTIME:-60}" 1
json="$RESULTS_DIR/efs.json"
compare_headline matched-load write_iops "$(jq '.jobs[0].write.iops | round' "$json")" ops/s
compare_headline matched-load write_p99_us "$(compare_p99_us "$json" 0 write)" us
compare_headline matched-load read_iops "$(jq '.jobs[1].read.iops | round' "$json")" ops/s
compare_headline matched-load read_p99_us "$(compare_p99_us "$json" 1 read)" us
compare_finish efs
