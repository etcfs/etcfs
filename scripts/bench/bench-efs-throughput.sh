#!/bin/bash
# bench-efs-throughput.sh — EFS bursting (the default managed alternative)
# vs. EFS provisioned throughput (a fixed, stated budget, the closest EFS
# comes to io2's --iops), plus etcfs at its own baseline for reference.
#
# Bursting scales with stored size and a burst-credit balance a fresh
# filesystem starts with, so a bursting number is not comparable to a
# provisioned ceiling. Provisioned mode gives a stated MiB/s budget, the same
# idea as choosing --iops for io2.
#
# Requires: create-infra.sh + setup-compute.sh already run.
#
# Usage:
#   ./bench-efs-throughput.sh                        # 100 MiB/s provisioned
#   ETCFS_BENCH_PROVISIONED_MIBPS=256 ./bench-efs-throughput.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"
source "$SCRIPT_DIR/bench-lib.sh"

RESULTS_DIR="$PROJECT_ROOT/benchmark-results/efs-throughput"
mkdir -p "$RESULTS_DIR"
RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
MIBPS="${ETCFS_BENCH_PROVISIONED_MIBPS:-100}"

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
[[ "${#PUB_IPS[@]}" -ge 1 ]] || die "No compute nodes in state. Run create-infra.sh and setup-compute.sh first."
N0="${PUB_IPS[0]}"
SG_ID=$(state_get sg_id | tr -d '"')
SUBNET_ID=$(state_get subnet_id | tr -d '"')

$SSH_CMD "ec2-user@$N0" "sudo dnf install -y fio nfs-utils >/dev/null 2>&1 || sudo yum install -y fio nfs-utils >/dev/null 2>&1"

BURST_EFS_ID=$(state_get bench_efs_id 2>/dev/null | tr -d '"' || echo "")
if [[ -z "$BURST_EFS_ID" || "$BURST_EFS_ID" == "null" ]]; then
    log "Creating bursting EFS filesystem..."
    BURST_EFS_ID=$(efs_create bursting "" "$SG_ID" "$SUBNET_ID" "${CLUSTER_NAME}-bench-bursting")
    state_put bench_efs_id "\"$BURST_EFS_ID\""
fi

log "Creating provisioned EFS filesystem (${MIBPS} MiB/s)..."
PROV_EFS_ID=$(efs_create provisioned "$MIBPS" "$SG_ID" "$SUBNET_ID" "${CLUSTER_NAME}-bench-provisioned")

efs_mount "$N0" "$BURST_EFS_ID" /mnt/bench-efs-burst
efs_mount "$N0" "$PROV_EFS_ID" /mnt/bench-efs-prov

run_fio "efs-bursting" "filename=/mnt/bench-efs-burst/fio.dat" 1G libaio 4 32 "$RUNTIME"
run_fio "efs-provisioned-${MIBPS}mibps" "filename=/mnt/bench-efs-prov/fio.dat" 1G libaio 4 32 "$RUNTIME"
run_fio "etcfs-baseline" "directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync "${ETCFS_BENCH_JOBS:-4}" 1 "$RUNTIME"

{
    echo "| Target | randwrite IOPS | randwrite p99 (us) | randread IOPS | randread p99 (us) |"
    echo "|---|---|---|---|---|"
    fio_row "EFS bursting" "$RESULTS_DIR/efs-bursting.json"
    fio_row "EFS provisioned (${MIBPS} MiB/s)" "$RESULTS_DIR/efs-provisioned-${MIBPS}mibps.json"
    fio_row "etcfs" "$RESULTS_DIR/etcfs-baseline.json"
} | tee "$RESULTS_DIR/summary.md"

$SSH_CMD "ec2-user@$N0" "sudo umount /mnt/bench-efs-prov 2>/dev/null || true"
log "Deleting provisioned EFS filesystem (bursting one stays, tracked in state)..."
efs_destroy "$PROV_EFS_ID"

log "EFS throughput comparison complete. Results in $RESULTS_DIR"
