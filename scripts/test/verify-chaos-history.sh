#!/bin/bash
# Runs the Porcupine consistency checks (cmd/verify-history) against the
# operation histories a docker-based chaos run just recorded.
#
# Usage:
#   scripts/test/verify-chaos-history.sh
#   CRASHED=n1,n2 scripts/test/verify-chaos-history.sh
#
# CRASHED names the nodes the scenario SIGKILLed rather than shut down. Writes
# are acknowledged out of the daemon's own memory and published by a later
# flush, so a killed node legitimately loses whatever it had not yet flushed —
# and the extent model refuses to excuse that loss for any node not named here,
# because a write vanishing under a healthy cluster is exactly the failure it
# exists to catch. Naming a node that did not die only weakens the check; not
# naming one that did makes it report a violation that is not one.
#
# Every node in deploy/docker/docker-compose.yml is started with
# --history-log writing into the shared history_data volume; this script
# copies the per-node files out of that volume into a temp directory and
# checks them. Run it after a chaos scenario, while the cluster from that
# scenario is still up (or at least the history_data volume still exists) —
# chaos-lib.sh's teardown removes it along with everything else.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

VOLUME="${1:-docker_history_data}"
# KEEP_HISTORY_DIR preserves the copied histories instead of discarding them
# with the temp directory. A violation reported against a history that no
# longer exists cannot be investigated, and the volume it came from is removed
# by the chaos suite's teardown moments later.
if [[ -n "${KEEP_HISTORY_DIR:-}" ]]; then
    OUT="$KEEP_HISTORY_DIR"
    mkdir -p "$OUT"
else
    OUT="$(mktemp -d)"
    trap 'rm -rf "$OUT"' EXIT
fi

if ! docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    echo "[verify-history] volume $VOLUME does not exist — was a docker chaos scenario run?" >&2
    exit 1
fi

echo "[verify-history] copying histories out of $VOLUME"
# The daemon writes its history file 0600 as the container's root, so the copy
# has to run as root too; chown the copies to us afterward or nothing outside
# the container can read them back.
docker run --rm -v "$VOLUME:/history" -v "$OUT:/out" alpine:3.20 \
    sh -c "cp /history/history-*.jsonl /out/ 2>/dev/null; chown $(id -u):$(id -g) /out/*.jsonl 2>/dev/null || true"

files=("$OUT"/history-*.jsonl)
if [[ ! -e "${files[0]}" ]]; then
    echo "[verify-history] no history files found in $VOLUME — was --history-log recording?" >&2
    exit 1
fi

joined="$(IFS=,; echo "${files[*]}")"

echo "[verify-history] building cmd/verify-history"
cd "$PROJECT_ROOT"
go build -o "$OUT/verify-history" ./cmd/verify-history

echo "[verify-history] checking: $joined"
"$OUT/verify-history" --files="$joined" ${CRASHED:+--crashed="$CRASHED"}
