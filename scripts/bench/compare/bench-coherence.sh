#!/bin/bash
# bench-coherence.sh — two cross-node semantics, shown by running them rather
# than citing a specification. Meant for COMPARE_BACKEND=efs and =etcfs side by
# side.
#
# 1. Staleness without reopening. Node B opens a file and keeps it open,
#    re-reading the same bytes every 50 ms through the one descriptor. Node A,
#    also holding it open, overwrites those bytes and fsyncs. NFS promises
#    close-to-open consistency only (RFC 7530 §10.3.1): B is entitled to cached
#    data until it reopens, bounded in practice by the attribute cache timeout.
#    etcfs hands the inode's lock to the writer and invalidates the reader's
#    pages, so B should see the write on its next read. Reported:
#    stale_read_s, from A's fsync returning to B's first read of the new bytes
#    (-1 if B never saw them within the window), once with B reading through the
#    page cache and once with O_DIRECT.
#
# 2. Cross-node fcntl exclusion. A takes an exclusive POSIX record lock and
#    holds it; B asks for the same lock without blocking, then the same with
#    flock(2). Reported: fcntl_excludes / flock_excludes, 1 if B was refused and
#    0 if it was granted the lock A holds. etcfs does not implement cross-node
#    locks today and is expected to report 0 for both.
#
# ETCFS_COHERENCE_WINDOW (default 90) — seconds B keeps reading after the write.
#
# Usage:
#   COMPARE_BACKEND=efs ./bench-coherence.sh
#   COMPARE_BACKEND=etcfs ./bench-coherence.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

WINDOW="${ETCFS_COHERENCE_WINDOW:-90}"
compare_client_nodes 2
compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 2 ]] || die "coherence needs the filesystem mounted on two nodes"
A="${BENCH_NODES[0]}"
B="${BENCH_NODES[1]}"

# stale_read <mode> — mode is "cached" or "direct".
stale_read() {
    local mode="$1" f="$MOUNT_PATH/coherence-$1.dat" flags="os.O_RDWR"
    [[ "$mode" == direct ]] && flags="os.O_RDWR | os.O_DIRECT"
    # 4 KiB of "old" written and closed by A first, so both nodes start from a
    # file that exists everywhere with known contents.
    $SSH_CMD "ec2-user@$A" "sudo python3 -c \"
import os
fd = os.open('$f', os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o666)
os.write(fd, b'old.' * 1024); os.fsync(fd); os.close(fd)\""
    sleep 2

    # B: open once, then read the first block through that descriptor until the
    # window closes. An aligned buffer, so the same code serves O_DIRECT.
    $SSH_CMD -n -f "ec2-user@$B" "sudo nohup python3 -c \"
import mmap, os, time
fd = os.open('$f', $flags)
buf = mmap.mmap(-1, 4096)
end = time.time() + $WINDOW + 10
with open('/tmp/coh-$mode.log', 'w') as log:
    while time.time() < end:
        os.preadv(fd, [buf], 0)
        log.write('%.6f %s\\\\n' % (time.time(), bytes(buf[:4]).decode(errors='replace')))
        time.sleep(0.05)
\" >/dev/null 2>&1 &"
    sleep 5

    # A: open, overwrite in place, fsync, and keep the descriptor open — the
    # write is never followed by a close, which is the case close-to-open
    # consistency does not cover.
    local t_write
    t_write=$($SSH_CMD "ec2-user@$A" "sudo python3 -c \"
import os, time
fd = os.open('$f', os.O_RDWR)
os.pwrite(fd, b'new.' * 1024, 0); os.fsync(fd)
print('%.6f' % time.time())
time.sleep($WINDOW)
os.close(fd)\"")
    sleep 12
    $SSH_CMD "ec2-user@$B" "cat /tmp/coh-$mode.log" > "$RESULTS_DIR/coherence-$mode-$B.log"
    local seen
    seen=$(awk -v t="$t_write" '$1 >= t && $2 == "new." { printf "%.3f", $1 - t; f=1; exit }
                                END { if (!f) print "-1" }' "$RESULTS_DIR/coherence-$mode-$B.log")
    compare_headline coherence "stale_read_${mode}_s" "$seen" s
}

# lock_excludes <fcntl|flock> — 1 if B is refused the lock A holds.
lock_excludes() {
    local kind="$1" f="$MOUNT_PATH/coherence-lock.dat" take
    case "$kind" in
        fcntl) take="fcntl.lockf(fd, fcntl.LOCK_EX" ;;
        flock) take="fcntl.flock(fd, fcntl.LOCK_EX" ;;
    esac
    $SSH_CMD -n -f "ec2-user@$A" "sudo nohup python3 -c \"
import fcntl, os, time
fd = os.open('$f', os.O_RDWR | os.O_CREAT, 0o666)
$take)
open('/tmp/lock-held', 'w').close()
time.sleep(30)\" >/dev/null 2>&1 &"
    $SSH_CMD "ec2-user@$A" "for i in \$(seq 100); do [ -f /tmp/lock-held ] && exit 0; sleep 0.1; done; exit 1" \
        || die "lock_excludes: A never took the $kind lock"
    local verdict
    verdict=$($SSH_CMD "ec2-user@$B" "sudo python3 -c \"
import fcntl, os
fd = os.open('$f', os.O_RDWR)
try:
    $take | fcntl.LOCK_NB)
    print(0)
except OSError:
    print(1)\"")
    $SSH_CMD "ec2-user@$A" "sudo pkill -f 'coherence-lock.dat' ; sudo rm -f /tmp/lock-held" || true
    sleep 2
    compare_headline coherence "${kind}_excludes" "$verdict" bool
}

stale_read cached
stale_read direct
lock_excludes fcntl
lock_excludes flock
