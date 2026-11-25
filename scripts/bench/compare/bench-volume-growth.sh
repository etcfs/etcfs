#!/bin/bash
# bench-volume-growth.sh — fill the filesystem until writes fail for space,
# grow the shared EBS volume underneath the running cluster, and time how long
# until the new space is actually allocatable — with no daemon restart or
# remount anywhere, which is the claim being tested. A second node runs the
# 4 KiB probe throughout; survivor_max_stall_s is its worst gap from the
# modify-volume call to the first successful post-growth write.
#
# The daemon re-reads the device size when an arena acquisition fails for space
# and retries the allocation once if the volume grew (Device.RefreshSize,
# internal/ipc/service.go), so the writer that hit ENOSPC is itself what
# notices the growth: the poll loop below is the measurement, not a workaround.
#
# gfs2 has no such path: the new size is visible to the kernel once EC2 has
# resized the device, but GFS2 allocates nothing from it until gfs2_grow runs
# on a mounted node, which writes the new resource groups and makes every
# mount re-read the rindex. The script waits for the device to grow on N0 and
# runs gfs2_grow there itself, as an operator's automation would, so
# allocatable_after_s is partly the speed of this script; device_grown_s and
# gfs2_grow_s split it into EC2's part and GFS2's part, and the stall is the
# number that is GFS2's alone.
#
# Usage:
#   ./bench-volume-growth.sh
#   COMPARE_BACKEND=gfs2 ./bench-volume-growth.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
case "$COMPARE_BACKEND_BASE" in
    etcfs|gfs2) ;;
    *) die "bench-volume-growth.sh runs etcfs or gfs2 (got $COMPARE_BACKEND)" ;;
esac

GROW_BY="${ETCFS_GROW_BY_GIB:-10}"
POLL_TIMEOUT="${ETCFS_GROW_TIMEOUT:-600}"

compare_client_nodes "${ETCFS_ELASTIC_NODES:-3}"
compare_begin
compare_mount
N0="${BENCH_NODES[0]}"
PROBE_NODE="${BENCH_NODES[1]}"
# Started before the fill so the probe's block is already allocated: its
# overwrites keep working on a full filesystem, and any gap is a stall.
compare_probe_start "$PROBE_NODE" "$MOUNT_PATH/growth-probe.dat"

log "Filling $MOUNT_PATH until a write fails for space..."
$SSH_CMD "ec2-user@$N0" "
    i=0
    while sudo dd if=/dev/zero of=$MOUNT_PATH/fill-\$i.dat bs=1M count=512 oflag=direct >/dev/null 2>&1; do
        i=\$((i + 1))
    done
    echo \"filled with \$i x 512MiB files\"
"

NEW_SIZE=$((ETCFS_VOLUME_SIZE + GROW_BY))
log "Growing $COMPARE_VOL_ID to ${NEW_SIZE} GiB"
DEV="$(compare_shared_dev_path)"
OLD_BYTES=$($SSH_CMD "ec2-user@$N0" "sudo blockdev --getsize64 $DEV")
T0=$(compare_epoch)
aws ec2 modify-volume --volume-id "$COMPARE_VOL_ID" --size "$NEW_SIZE" >/dev/null

if [[ "$COMPARE_BACKEND_BASE" == gfs2 ]]; then
    # Timed on the node: the ssh round trip is the order of gfs2_grow itself.
    read -r grown_s grow_s < <($SSH_CMD "ec2-user@$N0" "
        t0=\$(date +%s.%N); deadline=\$(( \$(date +%s) + $POLL_TIMEOUT ))
        until [[ \$(sudo blockdev --getsize64 $DEV) -gt $OLD_BYTES ]]; do
            [[ \$(date +%s) -lt \$deadline ]] || exit 1
            sleep 0.2
        done
        t1=\$(date +%s.%N)
        sudo gfs2_grow $MOUNT_PATH >/dev/null || exit 1
        t2=\$(date +%s.%N)
        awk -v a=\$t0 -v b=\$t1 -v c=\$t2 'BEGIN{printf \"%.3f %.3f\\n\", b-a, c-b}'
    ") || die "gfs2: the device never grew or gfs2_grow failed within ${POLL_TIMEOUT}s"
    compare_headline volume-growth device_grown_s "$grown_s" s
    compare_headline volume-growth gfs2_grow_s "$grow_s" s
fi

# The write that succeeds is the definition of "allocatable": statfs numbers
# can move for reasons other than the new space being usable.
$SSH_CMD "ec2-user@$N0" "
    deadline=\$(( \$(date +%s) + $POLL_TIMEOUT ))
    until sudo dd if=/dev/zero of=$MOUNT_PATH/post-growth.dat bs=1M count=64 oflag=direct >/dev/null 2>&1; do
        [[ \$(date +%s) -lt \$deadline ]] || exit 1
        sleep 1
    done
" || die "no write succeeded within ${POLL_TIMEOUT}s of the volume growing"
T1=$(compare_epoch)

compare_probe_stop "$PROBE_NODE"
read -r _ stall errs < <(compare_probe_recovery "$PROBE_NODE" "$T0")
compare_headline volume-growth survivor_max_stall_s "$stall" s
compare_headline volume-growth probe_failed_ops "$errs" ops
compare_headline volume-growth allocatable_after_s \
    "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.3f", b-a}')" s
compare_headline volume-growth grown_by_gib "$GROW_BY" GiB
