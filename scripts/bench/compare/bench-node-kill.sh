#!/bin/bash
# bench-node-kill.sh — kill a node that is holding locks under load and measure
# how long until a surviving node's I/O to those same inodes resumes. The
# flagship number of this suite.
#
# Both nodes hammer one shared file so the victim genuinely owns its lock at
# the moment it dies; the survivor's probe log then gives the interval from
# the kill to its next successful write.
#
# All five backends are killed identically — the victim machine is powered off
# through sysrq, so nothing runs an exit path, nothing unmounts, nothing
# releases a lock and no daemon gets to notice it is dying — so what is compared
# is one fault, not five:
#   etcfs    lease expiry plus a generation guard — no reboot, no replay
#   gfs2     the survivors fence the dead node and replay its journal
#   gluster  the replica set drops to two copies
#   nfs      the server IS the filesystem: every client is out until it returns
#   juicefs  same shape — redis + minio on node0 are the single point
#
# The last two lose their single server rather than one of several peers, so
# their numbers are an outage length rather than a recovery time. That is the
# point of running them, not a flaw in the comparison.
#
# Two timestamps matter and they are not the same: when the kill was issued, and
# when the victim actually stopped answering. A machine powering off through
# sysrq has taken 45 s to go quiet on this harness, and charging that to
# recovery would flatter every backend equally and mean nothing. The victim's
# port is therefore watched at 5 Hz from the survivor for the whole run, and the
# headline pair is reported from both origins.
#
# ETCFS_KILL_SETTLE (default 20) is how long the load runs before the kill.
# ETCFS_KILL_WAIT (default 120) is how long the probe runs after it, and so the
# largest stall this can report; it has to exceed the victim's own shutdown.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-node-kill.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

SETTLE="${ETCFS_KILL_SETTLE:-20}"
WAIT_AFTER="${ETCFS_KILL_WAIT:-120}"

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 2 ]] || die "node-kill needs two nodes with the filesystem mounted"
SURVIVOR="${BENCH_NODES[0]}"
VICTIM="$(compare_failure_target)"
# An HA NFS pair serves from whichever node Pacemaker placed the group on, which
# is not always the preferred one; killing the standby measures nothing. The
# victim is the node serving at the moment of the kill, and the cluster's view
# is kept from before and after it.
if [[ "$COMPARE_BACKEND" == "nfs-ha" ]]; then
    active=$($SSH_CMD "ec2-user@$SURVIVOR" "sudo crm_resource --resource nfsgroup --locate 2>/dev/null" \
        | grep -oE 'cmp-n[0-9]+' | head -1)
    [[ -n "$active" ]] || die "nfs-ha: could not locate the NFS group before the kill"
    VICTIM="${COMPARE_PUB_IPS[$(( ${active#cmp-n} - 1 ))]}"
    [[ "$VICTIM" != "$SURVIVOR" ]] || die "nfs-ha: the probing client is the active server"
    $SSH_CMD "ec2-user@$SURVIVOR" "sudo pcs status --full 2>&1" > "$RESULTS_DIR/nfs-ha-before-kill.txt" 2>&1 || true
    log "nfs-ha: NFS group active on $active ($VICTIM)"
fi
SHARED="$MOUNT_PATH/kill-target.dat"
OWNED="$MOUNT_PATH/kill-victim-owned.dat"

# The victim's own port, watched from the survivor at 5 Hz for the whole run.
# Which port depends on what is being taken away: ssh for a machine that is
# being powered off, the service's own port for the two backends whose server is
# blocked rather than killed.
victim_priv=""
for i in "${!COMPARE_PUB_IPS[@]}"; do
    [[ "${COMPARE_PUB_IPS[$i]}" == "$VICTIM" ]] && victim_priv="${COMPARE_PRIV_IPS[$i]}"
done
DEATH_PORT=22

log "victim=$VICTIM survivor=$SURVIVOR shared inode=$SHARED"
compare_death_watch_start "$SURVIVOR" "$victim_priv" "$DEATH_PORT"
compare_probe_start "$VICTIM" "$SHARED"
compare_probe_start "$SURVIVOR" "$SHARED"
# A second file, written only by the victim, so the lock on it is unambiguously
# the dead node's at the moment it dies.
compare_probe_start "$VICTIM" "$OWNED" owned
sleep "$SETTLE"

T0=$(compare_epoch)
log "killing $VICTIM at $T0"
compare_kill_node "$VICTIM"

# The survivor now reaches for the dead node's file for the first time. This is
# the measurement the scenario is really about, and the shared-file probe above
# does not make it: a survivor that already holds the contended lock keeps
# writing straight through the failure, because it needs nothing from the dead
# node to do so. Only a lock it does not yet hold has to be recovered — for
# gfs2 that means waiting for the lockspace to be fenced and the journal
# replayed, and for etcfs for the dead node's lease to expire.
compare_probe_start "$SURVIVOR" "$OWNED" takeover

# Long enough for the slowest recovery in the field, plus the victim's own
# shutdown: a lease expiry is seconds, a journal replay after fencing is tens of
# seconds, and a machine powering off through sysrq has itself taken 45s to stop
# answering on this harness.
sleep "$WAIT_AFTER"
compare_probe_stop "$SURVIVOR"
compare_probe_stop "$SURVIVOR" takeover
compare_death_watch_stop "$SURVIVOR"

# When the fault actually landed. A survivor that never stalls is the result
# this scenario is looking for and also exactly what a *failed* kill produces,
# and the two are indistinguishable from the I/O probe alone — so the death is
# measured rather than assumed. -1 means the victim never stopped answering,
# which invalidates the run rather than making the backend look fast.
T_DOWN=$(compare_death_watch_time "$SURVIVOR" "$T0")
if [[ "$T_DOWN" == "-1" ]]; then
    log "WARNING the victim never stopped answering on port $DEATH_PORT — the fault did not land"
    victim_down_s=-1
else
    victim_down_s=$(awk -v a="$T0" -v b="$T_DOWN" 'BEGIN{printf "%.3f", b-a}')
fi

# What the survivor's own cluster stack made of the loss, kept next to the
# numbers: a GFS2 resume with no stall at all is only interpretable if the DLM
# lockspace can be seen to have gone through recovery (or not).
case "$COMPARE_BACKEND_BASE" in
    gfs2)
        $SSH_CMD "ec2-user@$SURVIVOR" \
            "sudo dlm_tool ls 2>&1; echo ---; sudo corosync-quorumtool -s 2>&1; echo ---; sudo dmesg | tail -40" \
            > "$RESULTS_DIR/survivor-cluster-state.txt" 2>&1 || true
        # Under pacemaker the recovery time only means anything if the fence
        # actually completed: a lockspace that recovered without one has not
        # been made safe, and a fence that failed leaves a number that is an
        # outage rather than a recovery. The history says which happened.
        if [[ "$COMPARE_BACKEND" == "gfs2-fenced" ]]; then
            $SSH_CMD "ec2-user@$SURVIVOR" \
                "sudo pcs status 2>&1; echo ---; sudo pcs stonith history 2>&1 || sudo stonith_admin --history '*' 2>&1" \
                > "$RESULTS_DIR/survivor-fencing-state.txt" 2>&1 || true
            # Why a lockspace stays in "wait fencing" after pacemaker's fence:
            # dlm_controld only leaves it once its own fence helper,
            # dlm_stonith, finds a fence of that nodeid newer than the failure.
            # The dump shows each attempt and its result, and the nodelist shows
            # whether that nodeid resolves to the name pacemaker fenced.
            $SSH_CMD "ec2-user@$SURVIVOR" \
                "sudo dlm_tool status 2>&1; echo ---; sudo dlm_tool dump 2>&1 | tail -150; echo ---;
                 sudo corosync-cmapctl nodelist 2>&1; echo ---;
                 sudo journalctl -u pacemaker --no-pager 2>&1 | grep -iE 'dlm|stonith|fence' | tail -60" \
                > "$RESULTS_DIR/survivor-dlm-fence-debug.txt" 2>&1 || true
            # Pacemaker's own phrasing, not a word this script invented:
            # `pcs stonith history` reports a completed fence as "<node> was
            # able to turn off node <victim>", and stonith_admin as
            # "Node <victim> last fenced at". Grepping for "successful" found
            # neither, and reported a fence that had plainly worked as one that
            # had not.
            if grep -qiE 'was able to turn (off|on) node|last fenced at' \
                   "$RESULTS_DIR/survivor-fencing-state.txt" 2>/dev/null; then
                compare_headline node-kill fence_confirmed 1 bool
            else
                compare_headline node-kill fence_confirmed 0 bool
                log "WARNING no successful fence is recorded on the survivor — the recovery"
                log "        number below is not a fenced recovery, whatever it says"
            fi
        fi
        ;;
    nfs)
        if [[ "$COMPARE_BACKEND" == "nfs-ha" ]]; then
            $SSH_CMD "ec2-user@$SURVIVOR" \
                "sudo pcs status --full 2>&1; echo ---; sudo pcs stonith history 2>&1" \
                > "$RESULTS_DIR/nfs-ha-after-kill.txt" 2>&1 || true
            if grep -qiE 'turning off of cmp-n[0-9]+ successful|was able to turn off' \
                   "$RESULTS_DIR/nfs-ha-after-kill.txt" 2>/dev/null; then
                compare_headline node-kill fence_confirmed 1 bool
            else
                compare_headline node-kill fence_confirmed 0 bool
                log "WARNING no successful fence is recorded on the survivor"
            fi
        fi
        ;;
    etcfs)
        $SSH_CMD "ec2-user@$SURVIVOR" "sudo tail -60 /tmp/meta.log 2>&1" \
            > "$RESULTS_DIR/survivor-cluster-state.txt" 2>&1 || true
        ;;
esac

read -r resume stall errs < <(compare_probe_recovery "$SURVIVOR" "$T0")
[[ "$resume" != "-1" ]] || die "survivor $SURVIVOR never completed a write after the kill (>${WAIT_AFTER}s)"

compare_headline node-kill victim_down_s "$victim_down_s" s
compare_headline node-kill resume_s "$resume" s
compare_headline node-kill max_stall_s "$stall" s
compare_headline node-kill failed_ops_after_kill "$errs" ops

# The same two numbers taken from the moment the victim actually died rather
# than from the moment the kill was issued. These are the ones worth comparing
# across backends: everything before the death is the victim's own shutdown,
# which no backend can be credited or charged for.
if [[ "$T_DOWN" != "-1" ]]; then
    read -r resume_d stall_d errs_d < <(compare_probe_recovery "$SURVIVOR" "$T_DOWN")
    compare_headline node-kill resume_after_death_s "$resume_d" s
    compare_headline node-kill max_stall_after_death_s "$stall_d" s
    compare_headline node-kill failed_ops_after_death "$errs_d" ops
fi

# Taking over the dead node's own file: -1 means the survivor never managed it
# inside the window, which for a lockspace waiting on a fence agent that this
# harness does not configure is the honest answer rather than a missing number.
T_ORIGIN="$T_DOWN"
[[ "$T_ORIGIN" == "-1" ]] && T_ORIGIN="$T0"
# The takeover probe's own log, kept whatever it reported: -1 means the survivor
# never wrote the dead node's file inside the window, and a probe that failed to
# start looks exactly the same from the outside.
# Wrapped in a timeout, and it does not touch the file itself: a stat on an
# inode whose lock holder is gone blocks for as long as the recovery being
# measured — which, on a GFS2 cluster whose lockspace has not released that
# inode, is indefinitely. One unguarded `ls` here hung a whole run.
timeout 30 $SSH_CMD "ec2-user@$SURVIVOR" "sudo tail -40 /tmp/takeover.log 2>&1" \
    > "$RESULTS_DIR/takeover-probe.txt" 2>&1 || true

read -r takeover stall_t errs_t < <(compare_probe_recovery "$SURVIVOR" "$T_ORIGIN" takeover)
compare_headline node-kill takeover_s "$takeover" s
compare_headline node-kill takeover_max_stall_s "$stall_t" s
compare_headline node-kill takeover_failed_ops "$errs_t" ops

# How long each probe had been silent when the run ended. A backend that never
# stalled and one that stopped answering for good are indistinguishable in the
# stall figures above — see compare_probe_silence — and the difference between
# them is the whole result.
T_END=$(compare_epoch)
compare_headline node-kill probe_silent_at_end_s "$(compare_probe_silence "$SURVIVOR" "$T_END")" s
compare_headline node-kill takeover_silent_at_end_s "$(compare_probe_silence "$SURVIVOR" "$T_END" takeover)" s
