#!/bin/bash
# bench-repeat.sh — run one scenario script RUNS times and report each headline
# metric as n, min, median and max across the runs, so a number the thesis
# leans on comes with its range instead of from a single run.
#
# Each run provisions and tears down its own cluster (the scenario's own
# compare_begin does that), so the runs are independent. Every run's
# headline.json is kept as headline-run<k>.json; the spread lands in
# headline-spread.json and is printed as a table.
#
# Usage:
#   COMPARE_BACKEND=gfs2 ./bench-repeat.sh 5 bench-node-kill.sh
#   ETCFS_ELASTIC_NODES=6 ./bench-repeat.sh 5 bench-elasticity.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

[[ $# -eq 2 ]] || die "usage: $0 <runs> <bench-script.sh>"
RUNS="$1"
SCRIPT="$SCRIPT_DIR/$(basename "$2")"
[[ -x "$SCRIPT" ]] || die "$SCRIPT is not an executable scenario"
TAG="$(basename "$SCRIPT" .sh)-n${ETCFS_ELASTIC_NODES:-default}"
OUT="$RESULTS_DIR/repeat-$TAG"
mkdir -p "$OUT"

for k in $(seq 1 "$RUNS"); do
    log "=== $TAG run $k/$RUNS ==="
    rm -f "$RESULTS_DIR/headline.json"
    # A failed run is recorded and skipped rather than ending the series: the
    # spread over the runs that finished is still worth having, and the count
    # says how many those were.
    if "$SCRIPT"; then
        cp "$RESULTS_DIR/headline.json" "$OUT/headline-run$k.json"
    else
        log "WARNING: run $k failed; excluded from the spread"
    fi
done

shopt -s nullglob
runs=("$OUT"/headline-run*.json)
[[ "${#runs[@]}" -gt 0 ]] || die "no run finished"
jq -s 'add | group_by([.scenario, .metric]) | map({
        scenario: .[0].scenario, metric: .[0].metric, unit: .[0].unit,
        n: length,
        min: (map(.value) | min),
        median: (map(.value) | sort | if length % 2 == 1 then .[length/2|floor]
                 else (.[length/2-1] + .[length/2]) / 2 end),
        max: (map(.value) | max)
    })' "${runs[@]}" > "$OUT/headline-spread.json"
jq -r '["scenario","metric","n","min","median","max","unit"], (.[] | [.scenario, .metric, .n, .min, .median, .max, .unit]) | @tsv' \
    "$OUT/headline-spread.json" | column -t
