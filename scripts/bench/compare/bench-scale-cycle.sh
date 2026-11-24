#!/bin/bash
# bench-scale-cycle.sh — what an autoscaler does to an etcfs cluster, timed
# step by step: a brand-new instance is launched and added, then removed and
# terminated, CYCLES times, while the original nodes write throughout.
#
# Scale-out is scripts/infra/add-compute-node.sh, unchanged: launch, attach the
# shared volume, install and build, `etcdctl member add`, start etcd, start the
# daemons, mount. Each of its log lines is stamped here with a sub-second clock,
# and the step times are the gaps between them:
#   launch_s         run-instances until running (includes the wait for it)
#   attach_s         attach-volume plus the script's fixed 8 s settle
#   install_build_s  packages, etcd download, building both binaries on-node —
#                    an artifact of this harness, which deploys the working
#                    tree; an autoscaler would bake these into the image
#   member_add_s     `etcdctl member add` against the running cluster
#   etcd_start_s     start etcd, including a fixed 8 s settle
#   daemon_start_s   start both daemons to the mount answering, including
#                    fixed sleeps of 4 + 7 s
#   first_write_s    mount up -> first 4 KiB O_DIRECT write that succeeds,
#                    timed on the node (arena claim is lazy, so it is in here)
# and etcd_ready_s, from member add to the new member's own "ready to serve"
# log line, which is the part of etcd_start_s that is not a fixed sleep.
#
# Scale-in follows chaos-lib.sh's AWS remove_node: unmount and SIGTERM (a
# clean leave), `etcdctl member remove`, detach, terminate. Reported from the
# start of the leave: membership_gone_s (membership:<node> deleted),
# arenas_released_s (no arena:<node>/ key left; -1 if still owned after
# ETCFS_SCALE_WAIT), member_remove_s, terminate_s.
#
# survivor_max_stall_out_s / _in_s: the longest gap between successes of the
# 4 KiB probe on an original node, over each scale-out and each scale-in.
#
# etcfs only: GFS2's equivalent of adding a brand-new node is
# bench-gfs2-journal-add.sh with ETCFS_JADD_FULL=1.
#
# ETCFS_SCALE_CYCLES (default 3), ETCFS_SCALE_WAIT (default 120).
#
# Usage:
#   ./bench-scale-cycle.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-scale-cycle.sh is etcfs-only (see header)"

CYCLES="${ETCFS_SCALE_CYCLES:-3}"
WAIT="${ETCFS_SCALE_WAIT:-120}"
ETCDCTL="sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379"

compare_client_nodes 3
compare_begin
compare_mount
rm -f "$RESULTS_DIR"/scale-*.json
BASE=("${BENCH_NODES[@]}")
PROBE_NODE="${BASE[0]}"
PROBE_PATH="$MOUNT_PATH/scale-probe.dat"

since() { awk -v s="$1" -v e="$2" 'BEGIN{ if (s < 0 || e < 0) print "-1"; else printf "%.3f", e-s }'; }
# step_at <log> <pattern> — the stamp of the first line matching, or -1.
step_at() { awk -v p="$2" 'index($0, p) { print $1; f=1; exit } END { if (!f) print "-1" }' "$1"; }
# max_stall <from> <to> — longest gap between probe successes in the window.
max_stall() {
    $SSH_CMD "ec2-user@$PROBE_NODE" "sudo cat /tmp/probe.log" > "$RESULTS_DIR/probe-$PROBE_NODE.log"
    awk -v a="$1" -v b="$2" '$2 == "ok" && $1 >= a && $1 <= b { if (p != "" && $1 - p > g) g = $1 - p; p = $1 }
        END { printf "%.3f", g + 0 }' "$RESULTS_DIR/probe-$PROBE_NODE.log"
}
key_gone() {  # <key-or-prefix> [--prefix] — true once no key matches
    [[ -z $($SSH_CMD "ec2-user@$PROBE_NODE" "$ETCDCTL get $2 '$1' --keys-only") ]]
}

LOAD_SECS=$(( CYCLES * (WAIT + 600) ))
LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/scale-load-@NODE@.dat
size=1G
runtime=$LOAD_SECS
time_based=1
[seqwrite]
rw=write
"
compare_probe_start "$PROBE_NODE" "$PROBE_PATH"
compare_parallel_fio "scale-load" "$LOAD_SECS" "$LOAD_JOB" "${BASE[@]}" >/dev/null &
load_pid=$!
sleep 5

for c in $(seq 1 "$CYCLES"); do
    S="scale-cycle$c"
    addlog="$RESULTS_DIR/scale-add-$c.log"

    # ---- scale out ----
    t0=$(compare_epoch)
    bash "$INFRA_DIR/add-compute-node.sh" 2>&1 \
        | while IFS= read -r line; do printf '%s %s\n' "$(date +%s.%N)" "$line"; done > "$addlog"
    grep -q "=== Node added:" "$addlog" || die "add-compute-node.sh failed, see $addlog"
    read -r NEW_PUB ENAME NODE_ID < <(sed -nE 's/.*Node added: ([0-9.]+) \((\S+) \/ (\S+)\).*/\1 \2 \3/p' "$addlog")
    NEW_INST=$(state_get compute_instance_ids | jq -r '.[-1]')

    first=$($SSH_CMD "ec2-user@$NEW_PUB" "until sudo dd if=/dev/zero of=$MOUNT_PATH/scale-first-\$\$ bs=4k count=1 oflag=direct >/dev/null 2>&1; do sleep 0.05; done; date +%s.%N")
    t_out=$first
    # etcd's own JSON log carries an RFC 3339 timestamp with sub-second digits.
    ready=$($SSH_CMD "ec2-user@$NEW_PUB" "grep -m1 'ready to serve client requests' /tmp/etcd.log" \
        | jq -r '.ts // empty' | xargs -r -I{} date -d {} +%s.%N || true)
    owns=$($SSH_CMD "ec2-user@$PROBE_NODE" "$ETCDCTL get --prefix 'arena:$NODE_ID/' --keys-only | grep -c . || true")

    s_launch=$(step_at "$addlog" "Launching instance")
    s_attach=$(step_at "$addlog" "Attaching EBS volume")
    s_install=$(step_at "$addlog" "Installing packages")
    s_member=$(step_at "$addlog" "Adding etcd member")
    s_etcd=$(step_at "$addlog" "Starting etcd on new node")
    s_daemon=$(step_at "$addlog" "Starting EtcFS daemons")
    s_done=$(step_at "$addlog" "=== Node added:")
    compare_headline "$S" launch_s "$(since "$s_launch" "$s_attach")" s
    compare_headline "$S" attach_s "$(since "$s_attach" "$s_install")" s
    compare_headline "$S" install_build_s "$(since "$s_install" "$s_member")" s
    compare_headline "$S" member_add_s "$(since "$s_member" "$s_etcd")" s
    compare_headline "$S" etcd_start_s "$(since "$s_etcd" "$s_daemon")" s
    compare_headline "$S" etcd_ready_s "$(since "$s_etcd" "${ready:--1}")" s
    compare_headline "$S" daemon_start_s "$(since "$s_daemon" "$s_done")" s
    compare_headline "$S" first_write_s "$(since "$s_done" "$first")" s
    compare_headline "$S" scale_out_total_s "$(since "$t0" "$t_out")" s
    compare_headline "$S" arena_claimed "$([[ "$owns" -gt 0 ]] && echo 1 || echo 0)" bool
    compare_headline "$S" survivor_max_stall_out_s "$(max_stall "$t0" "$t_out")" s

    # ---- scale in: chaos-lib.sh's remove_node, with each step timed ----
    t0=$(compare_epoch)
    $SSH_CMD "ec2-user@$NEW_PUB" "sudo fusermount -u $MOUNT_PATH 2>/dev/null; sleep 1; sudo pkill -TERM etcfuse-meta 2>/dev/null; sleep 2; true"
    t_left=$(compare_epoch)
    memberid=$($SSH_CMD "ec2-user@$PROBE_NODE" "$ETCDCTL member list" | grep ", $ENAME," | cut -d, -f1 || true)
    [[ -n "$memberid" ]] || log "WARNING: no etcd member id for $ENAME"
    [[ -z "$memberid" ]] || $SSH_CMD "ec2-user@$PROBE_NODE" "$ETCDCTL member remove $memberid" >/dev/null
    t_removed=$(compare_epoch)
    aws ec2 detach-volume --volume-id "$(state_get volume_id | tr -d '"')" --instance-id "$NEW_INST" >/dev/null 2>&1 || true
    sleep 5
    aws ec2 terminate-instances --instance-ids "$NEW_INST" >/dev/null
    gone=-1 released=-1
    deadline=$((SECONDS + WAIT))
    while [[ $SECONDS -lt $deadline && ( "$gone" == -1 || "$released" == -1 ) ]]; do
        now=$(compare_epoch)
        [[ "$gone" != -1 ]] || ! key_gone "membership:$NODE_ID" || gone=$(since "$t0" "$now")
        [[ "$released" != -1 ]] || ! key_gone "arena:$NODE_ID/" --prefix || released=$(since "$t0" "$now")
        sleep 0.5
    done
    aws ec2 wait instance-terminated --instance-ids "$NEW_INST"
    t_in=$(compare_epoch)
    compare_headline "$S" leave_s "$(since "$t0" "$t_left")" s
    compare_headline "$S" member_remove_s "$(since "$t_left" "$t_removed")" s
    compare_headline "$S" membership_gone_s "$gone" s
    compare_headline "$S" arenas_released_s "$released" s
    compare_headline "$S" terminate_s "$(since "$t_removed" "$t_in")" s
    compare_headline "$S" survivor_max_stall_in_s "$(max_stall "$t0" "$t_in")" s
done

compare_probe_stop "$PROBE_NODE"
for ip in "${BASE[@]}"; do $SSH_CMD "ec2-user@$ip" "sudo pkill fio" >/dev/null 2>&1 || true; done
wait "$load_pid" || true
