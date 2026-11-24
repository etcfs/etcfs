#!/bin/bash
# run-level2.sh — the thesis-defence benchmark set ("level 2"), one run at a
# time, every run on a fresh cluster. Each series goes through bench-repeat.sh,
# so every run's headline.json is kept and a spread is written per series.
# Short scenarios first, the cluster-size sweep last so it can be cut.
#
# Usage:
#   ETCFS_KEY_NAME=etcfs-bench-key ./run-level2.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
R="$SCRIPT_DIR/bench-repeat.sh"
export ETCFS_KEY_NAME="${ETCFS_KEY_NAME:-etcfs-bench-key}"

# series <runs> <script> [VAR=value...] — one series; a failure is logged and
# the set moves on.
series() {
    local runs="$1" script="$2"
    shift 2
    echo "=== $(date -Is) START $script x$runs $*"
    if env "$@" "$R" "$runs" "$script"; then
        echo "=== $(date -Is) DONE  $script $*"
    else
        echo "=== $(date -Is) FAIL  $script $*"
    fi
}

# Single runs.
series 1 bench-coherence.sh COMPARE_BACKEND=efs ETCFS_COHERENCE_WINDOW=90
series 1 bench-coherence.sh COMPARE_BACKEND=etcfs ETCFS_COHERENCE_WINDOW=90
series 1 bench-gfs2-journal-add.sh COMPARE_BACKEND=gfs2 ETCFS_JADD_FULL=1 ETCFS_ELASTIC_WAIT=30
for b in etcfs gfs2; do
    series 1 bench-scale-cycle.sh COMPARE_BACKEND=$b
done
series 1 bench-handoff.sh COMPARE_BACKEND=gfs2 ETCFS_VOLUME_IOPS=8000 ETCFS_VOLUME_SIZE=64
series 1 bench-seq-throughput.sh COMPARE_BACKEND=gfs2 ETCFS_VOLUME_IOPS=8000 ETCFS_VOLUME_SIZE=64

# Overlap and churn, 2 runs each.
for b in etcfs gfs2; do
    series 2 bench-overlap.sh COMPARE_BACKEND=$b ETCFS_ELASTIC_WAIT=30
    for i in 60 30 10 5 2; do
        series 2 bench-churn.sh COMPARE_BACKEND=$b ETCFS_CHURN_INTERVAL=$i ETCFS_REPEAT_TAG=i$i
    done
done

# Headline numbers, 5 runs each. Node-kill keeps its 120 s wait: fenced GFS2
# has needed up to 21.7 s to recover.
for b in etcfs gfs2-fenced; do
    series 5 bench-node-kill.sh COMPARE_BACKEND=$b
done
for b in etcfs gfs2; do
    series 5 bench-elasticity.sh COMPARE_BACKEND=$b ETCFS_ELASTIC_WAIT=30
done

# Cluster-size sweep, 2 runs per point.
for n in 6 8 10; do
    for locks in 100 100000; do
        for b in etcfs gfs2; do
            series 2 bench-elasticity.sh COMPARE_BACKEND=$b ETCFS_ELASTIC_NODES=$n \
                ETCFS_ELASTIC_LOCKS=$locks ETCFS_ELASTIC_WAIT=30 ETCFS_REPEAT_TAG=l$locks
        done
    done
done
echo "=== $(date -Is) ALL DONE"
