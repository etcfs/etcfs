#!/bin/bash
# chaos-test-single-cluster.sh — run all chaos scenarios against ONE cluster,
# back to back, to check the cluster recovers from repeated/sequential faults
# instead of each scenario getting a fresh cluster (see chaos-test.sh).
#
# Usage:
#   ./chaos-test-single-cluster.sh docker [scenario|all]
#   ./chaos-test-single-cluster.sh aws    [scenario|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-single-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [scenario|all]"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

# S9/S10 need etcfsctl (fsck/scrub) against the cluster's etcd, which neither
# transport exposes to this host: docker publishes no port for it, and the AWS
# members listen on private addresses. So the binary is built statically — the
# same CGO_ENABLED=0 build the meta image uses, which also makes it runnable on
# Amazon Linux — and shipped to a node that is already on the cluster's network,
# rather than teaching the host how to reach either.
ETCFSCTL_BIN="$REPORT_DIR/etcfsctl-static"
build_etcfsctl() {
    [[ -x "$ETCFSCTL_BIN" ]] && return 0
    ( cd "$PROJECT_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$ETCFSCTL_BIN" ./cmd/etcfsctl ) 2>&1 | tee -a "$REPORT_DIR/chaos.log"
    [[ -x "$ETCFSCTL_BIN" ]]
}
run_etcfsctl() {
    build_etcfsctl || { echo "etcfsctl build failed"; return 1; }
    if [[ "$MODE" == "docker" ]]; then
        docker cp "$ETCFSCTL_BIN" "$M1:/tmp/etcfsctl" >/dev/null 2>&1
        docker exec "$M1" chmod +x /tmp/etcfsctl >/dev/null 2>&1
        docker exec "$M1" /tmp/etcfsctl --etcd-endpoints=http://etcfs-etcd1:2379,http://etcfs-etcd2:2379,http://etcfs-etcd3:2379 "$@"
        return
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q \
        "$ETCFSCTL_BIN" "ec2-user@$N1:/tmp/etcfsctl" 2>/dev/null
    runcmd60 "$N1" "chmod +x /tmp/etcfsctl && sudo /tmp/etcfsctl --etcd-endpoints=$(etcd_endpoints) $*"
}

# assert_storage_clean <label> [fsck] — assert the on-disk state has no leaks,
# once the cluster has had the chance to tidy up.
#
# Superseded and orphaned extents are reclaimed by the owning node's background
# scrubber on a 30s interval (cmd/etcfuse-meta/main.go); the pass etcfsctl runs
# only reports. Any scenario that overwrites a file therefore leaves a dead
# extent behind for up to one interval, and asserting straight after the write
# measures that interval rather than a leak. Waiting one full pass is what R3 in
# chaos-arena-reclaim.sh does, for exactly this reason.
assert_storage_clean() {
    local label="$1" want_fsck="${2:-}"
    # Reclaim is asynchronous (the scrubber runs every 30s on each node), and
    # the node that owns the superseded extents may have just been restarted,
    # so its first pass is up to a full interval away. The invariant is that
    # storage settles clean, not that it is clean at one arbitrary instant —
    # so poll for it rather than sleeping a fixed guess and asserting once.
    local ok=0 out fsck_out="" scrub_out="" k
    log "  polling fsck/scrub until storage settles (up to 100s)..."
    for ((k = 0; k < 100; k += 20)); do
        sleep 20
        ok=1
        if [[ -n "$want_fsck" ]]; then
            fsck_out=$(run_etcfsctl fsck 2>&1)
            echo "$fsck_out" | grep -q "^fsck: 0 errors" || ok=0
        fi
        scrub_out=$(run_etcfsctl scrub 2>&1)
        echo "$scrub_out" | grep -q "^scrub: 0 anomalies" || ok=0
        [[ "$ok" -eq 1 ]] && break
    done
    for out in "$fsck_out" "$scrub_out"; do
        [[ -z "$out" ]] && continue
        log "  $(echo "$out" | head -1)"
        [[ "$ok" -eq 1 ]] || echo "$out" | tail -10 | while IFS= read -r l; do log "    $l"; done
    done
    # shellcheck disable=SC2015
    [[ "$ok" -eq 1 ]] && { PASS=$((PASS+1)); log "  PASS: $label"; } || { FAIL=$((FAIL+1)); log "  FAIL: $label"; }
}

# ============================================================
# Scenarios — same assertions as chaos-test.sh S1/S2/S3/S5/S6/S7,
# run back to back against the ONE cluster provisioned above.
# ============================================================

run_s1() {
    log "======== S1: C daemon SIGKILL ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "s1-data" "s1-hello.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; return; fi
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_fuse "$N1")
    else
        runcmd "$N1" "sudo pkill -9 -x etcfuse 2>/dev/null; sleep 1; sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
        local R=$(runcmd30 "$N1" "
          sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
          for i in \$(seq 1 20); do sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0; sleep 1; done
          echo FAIL
        ")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V=$(readf "$N1" "s1-hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
}

run_s2() {
    log "======== S2: Go daemon SIGKILL ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "go-data" "s2-hello.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; return; fi
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo pkill -9 etcfuse-meta 2>/dev/null; sleep 1; sudo rm -f /run/etcfuse/etcfuse.sock; true"
        runcmd "$N1" "sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V=$(readf "$N1" "s2-hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
}

run_s3() {
    log "======== S3: Network partition ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "pre-part" "s3-p1.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-partition write did not land"; dump_logs "$N1"; return; fi
    writef "$N2" "survivor-write" "s3-p2.txt" || log "  WARN: N2 pre-partition write failed"
    readf "$N3" "s3-p1.txt" > /dev/null

    if [[ "$MODE" == "docker" ]]; then
        log "  Disconnecting N1 (fuse+meta) from network..."
        partition_node "$N1" "$M1"
        sleep 15
        writef "$N2" "during-part" "s3-p3.txt"
        local V2=$(readf "$N3" "s3-p3.txt")
        # shellcheck disable=SC2015
        [[ -n "$V2" ]] && { PASS=$((PASS+1)); log "  PASS: Survivors work: $V2"; } || { FAIL=$((FAIL+1)); log "  FAIL: survivors"; }
        log "  Reconnecting N1..."
        heal_node "$N1" "$M1"
        sleep 20
        log "  Restarting N1 daemons after self-fence..."
        local R=$(restart_pair "$M1" "$N1")
    else
        local SG=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        local VPC=$(jq -r '.vpc_id' "$PROJECT_ROOT/$STATE_FILE")
        local N1_INST=$(jq -r '.compute_instance_ids[0]' "$PROJECT_ROOT/$STATE_FILE")
        local N1_ENI=$(aws ec2 describe-instances --instance-ids $N1_INST \
            --query 'Reservations[0].Instances[0].NetworkInterfaces[0].NetworkInterfaceId' --output text 2>/dev/null)
        local MY_IP=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "0.0.0.0")
        local TEMP_SG=$(aws ec2 create-security-group --group-name "chaos-temp-$$" --description "Temp partition SG" --vpc-id "$VPC" --query 'GroupId' --output text 2>/dev/null)
        aws ec2 authorize-security-group-ingress --group-id "$TEMP_SG" --protocol tcp --port 22 --cidr "${MY_IP}/32" 2>/dev/null || true
        log "  Swapping N1 to TEMP_SG=$TEMP_SG (no etcd ports)..."
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$TEMP_SG" 2>/dev/null
        sleep 15
        writef "$N2" "during-part" "s3-p3.txt"
        local V2=$(readf "$N3" "s3-p3.txt")
        # shellcheck disable=SC2015
        [[ -n "$V2" ]] && { PASS=$((PASS+1)); log "  PASS: Survivors work: $V2"; } || { FAIL=$((FAIL+1)); log "  FAIL: survivors"; }
        log "  Restoring N1 to original SG..."
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$SG" "$TEMP_SG" 2>/dev/null || true
        sleep 5
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$SG" 2>/dev/null || true
        aws ec2 delete-security-group --group-id "$TEMP_SG" 2>/dev/null || true
        sleep 20
        log "  Restarting N1 daemons after self-fence..."
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V3=$(readf "$N1" "s3-p3.txt")
    # shellcheck disable=SC2015
    [[ -n "$V3" ]] && { PASS=$((PASS+1)); log "  PASS: N1 reads survivor: $V3"; } || { FAIL=$((FAIL+1)); log "  FAIL: N1 restore"; }
}

run_s5() {
    log "======== S5: Generation bump ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    local GEN
    if [[ "$MODE" == "docker" ]]; then
        GEN=$(etcdctl_on get gen:n1 --print-value-only 2>/dev/null || echo "1")
    else
        GEN=$(runcmd "$N1" 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n1 --print-value-only' 2>/dev/null || echo "1")
    fi
    [[ "$GEN" =~ ^[0-9]+$ ]] || GEN=1
    local NEWGEN=$((GEN + 1))
    log "  bumping gen:n1 $GEN -> $NEWGEN"
    if [[ "$MODE" == "docker" ]]; then
        etcdctl_on put gen:n1 "$NEWGEN" 2>/dev/null
    else
        runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $NEWGEN" 2>/dev/null
    fi
    writef "$N1" "post-fence" "s5-fence.txt"
    local V=$(readf "$N1" "s5-fence.txt")
    # shellcheck disable=SC2015
    [[ -z "$V" ]] && { PASS=$((PASS+1)); log "  PASS: write blocked"; } || { FAIL=$((FAIL+1)); log "  FAIL: write succeeded"; }
    # Restore gen so later scenarios on this SAME cluster aren't fenced forever.
    if [[ "$MODE" == "docker" ]]; then
        etcdctl_on put gen:n1 "$GEN" 2>/dev/null
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $GEN" 2>/dev/null
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly after un-fencing ($R)"
}

run_s6() {
    log "======== S6: All 3 crash ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    for i in 1 2 3; do
        eval "ip=\$N$i"
        # shellcheck disable=SC2154
        if ! writef "$ip" "data-n$i" "s6-ac$i.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write on n$i did not land"; dump_logs "$ip"; return; fi
    done
    if [[ "$MODE" == "docker" ]]; then
        docker kill -s KILL "$N1" "$N2" "$N3" "$M1" "$M2" "$M3" >/dev/null 2>&1
        sleep 3
        for pair in "$M1 $N1" "$M2 $N2" "$M3 $N3"; do
            docker start ${pair% *} >/dev/null 2>&1
        done
        sleep 3
        for pair in "$M1 $N1" "$M2 $N2" "$M3 $N3"; do
            docker start ${pair#* } >/dev/null 2>&1
        done
    else
        for i in 1 2 3; do
            eval "ip=\$N$i"
            runcmd "$ip" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; true"
        done
        sleep 3
        for i in 1 2 3; do
            eval "ip=\$N$i"
            local R=$(restart_daemons "$ip" "n$i")
            [[ "$R" == "OK" ]] || log "  WARN: n$i did not remount cleanly ($R)"
        done
    fi
    local V ALL=0
    for i in 1 2 3; do
        eval "ip=\$N$i"
        for k in $(seq 1 20); do check_mount "$ip" && break; sleep 1; done
        V=$(readf "$ip" "s6-ac$i.txt")
        [[ -n "$V" ]] && ALL=$((ALL+1))
    done
    # shellcheck disable=SC2015
    [[ "$ALL" -ge 3 ]] && { PASS=$((PASS+1)); log "  PASS: $ALL/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: $ALL/3"; }
}

run_s7() {
    log "======== S7: Mid-write crash ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    for f in a b c; do
        if ! writef "$N1" "wal-$f" "s7-w$f.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write w$f.txt did not land"; dump_logs "$N1"; return; fi
    done
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 2; true"
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local S=0
    for f in wa wb wc; do local V=$(readf "$N1" "s7-$f.txt"); [[ -n "$V" ]] && S=$((S+1)); done
    # shellcheck disable=SC2015
    [[ "$S" -ge 1 ]] && { PASS=$((PASS+1)); log "  PASS: $S/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: 0/3"; }
}

run_s8() {
    log "======== S8: Cross-node contention on one inode ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "seed" "s8-shared.txt"; then FAIL=$((FAIL+1)); log "  FAIL: seed write did not land"; dump_logs "$N1"; return; fi

    local A B
    A=$(printf 'node1-%.0s' $(seq 1 500))
    B=$(printf 'node2-%.0s' $(seq 1 500))

    writef "$N1" "$A" "s8-shared.txt" &
    local P1=$!
    writef "$N2" "$B" "s8-shared.txt" &
    local P2=$!
    local WFAIL=0
    wait "$P1" || WFAIL=$((WFAIL+1))
    wait "$P2" || WFAIL=$((WFAIL+1))
    # Both writers must complete: contention is a reason to wait for the other
    # node's recall, never a reason to hand the caller an error.
    # shellcheck disable=SC2015
    [[ "$WFAIL" -eq 0 ]] && { PASS=$((PASS+1)); log "  PASS: both contending writers completed"; } || { FAIL=$((FAIL+1)); log "  FAIL: $WFAIL/2 contending writers errored"; dump_logs "$(meta_of 1)"; }

    local V
    V=$(readf "$N3" "s8-shared.txt")
    # shellcheck disable=SC2015
    [[ "$V" == "$A" || "$V" == "$B" ]] && { PASS=$((PASS+1)); log "  PASS: final content matches one writer, no interleave/corruption"; } || { FAIL=$((FAIL+1)); log "  FAIL: content corrupted or interleaved"; }

    # The recall is logged at debug ("yielded a cached inode lock to a peer",
    # internal/ipc/lockcache.go), which is why the metadata daemons run at
    # --log-level=2 in both transports: at the default level this assertion
    # could never match, whether or not a recall happened.
    local RECALL_SEEN=0 i
    for i in 1 2; do
        meta_log "$i" | grep -q "yielded a cached inode lock" && RECALL_SEEN=1
    done
    # shellcheck disable=SC2015
    [[ "$RECALL_SEEN" -eq 1 ]] && { PASS=$((PASS+1)); log "  PASS: recall observed — a node yielded its cached lock to a peer"; } || { FAIL=$((FAIL+1)); log "  FAIL: no recall observed"; dump_logs "$(meta_of 1)"; dump_logs "$(meta_of 2)"; }
}

run_s9() {
    log "======== S9: Crash with a full buffer ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "synced-data" "s9-synced.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; return; fi
    runcmd "$N1" "dd if=/dev/null of=/mnt/etcfuse/s9-synced.txt bs=1 count=0 conv=notrunc,fsync 2>/dev/null" >/dev/null 2>&1

    # Unflushed: written with no explicit fsync, right before the kill —
    # whether it lands is a genuine race with the flush interval, so this is
    # informational only, not an assertion.
    writef "$N1" "unflushed-data" "s9-unflushed.txt" || true

    local R=$(restart_node 1)
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"

    local VS
    VS=$(readf "$N1" "s9-synced.txt")
    # shellcheck disable=SC2015
    [[ "$VS" == "synced-data" ]] && { PASS=$((PASS+1)); log "  PASS: fsynced write survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: fsynced write lost: '$VS'"; }

    local VU
    VU=$(readf "$N1" "s9-unflushed.txt")
    log "  info: unflushed write survived=$( [[ -n "$VU" ]] && echo yes || echo no )"

    log "  Running fsck + scrub against the surviving cluster..."
    assert_storage_clean "fsck/scrub clean, no extent references a freed block" fsck
}

run_s10() {
    log "======== S10: Lease loss under sustained write load ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "seed" "s10-file.txt"; then FAIL=$((FAIL+1)); log "  FAIL: seed write did not land"; dump_logs "$N1"; return; fi

    # Disconnect only n1's meta container — its etcd session dies, but its
    # FUSE peer keeps talking to it over the local unix socket and keeps
    # issuing writes, unlike S3's whole-node partition.
    log "  Cutting n1's metadata daemon off etcd (its session dies, the mount stays)..."
    if ! isolate_etcd 1; then
        FAIL=$((FAIL+1)); log "  FAIL: could not cut n1 off etcd — the fault was never injected"
        rejoin_etcd 1; return
    fi

    writef "$N1" "n1-during-session-loss" "s10-file.txt" || true

    writef "$N2" "n2-took-it" "s10-file.txt"
    local V2
    V2=$(readf "$N3" "s10-file.txt")
    # shellcheck disable=SC2015
    [[ "$V2" == "n2-took-it" ]] && { PASS=$((PASS+1)); log "  PASS: peer took the inode while n1's session was dead"; } || { FAIL=$((FAIL+1)); log "  FAIL: peer write did not land: '$V2'"; }

    # Past the self-fence window (2-3x CHAOS_LEASE_TTL) so n1's watchdog has
    # actually fired and its flush is rejected, not just delayed.
    log "  Waiting past n1's self-fence window..."
    sleep 35
    rejoin_etcd 1
    local R=$(restart_node 1)
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    wait_writable 1 || log "  WARN: n1 is not serving writes again yet"

    log "  Confirming n1's blocks were reclaimed, not double-referenced..."
    assert_storage_clean "no leaked/double-referenced blocks"
}

run_s11() {
    log "======== S11: Flush failure injection (etcd unavailable) ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "seed" "s11-file.txt"; then FAIL=$((FAIL+1)); log "  FAIL: seed write did not land"; dump_logs "$N1"; return; fi

    log "  Cutting n1's metadata daemon off etcd..."
    if ! isolate_etcd 1; then
        FAIL=$((FAIL+1)); log "  FAIL: could not cut n1 off etcd — the fault was never injected"
        rejoin_etcd 1; return
    fi

    # The buffer must still accept the write locally...
    writef "$N1" "buffered-during-outage" "s11-file.txt" || true

    # ...but an explicit fsync must surface EIO while etcd stays unreachable,
    # not silently succeed or hang, and keep failing on repeat.
    #
    # runcmd30, not runcmd: a FUSE request is bounded by config.RequestTimeout
    # (10s), and under a black-holed network — which is how the AWS transport
    # injects this, via iptables DROP, unlike docker's detach where etcd is
    # refused outright — that whole bound is spent waiting before the error can
    # be returned. A 10s harness budget therefore races the very bound it is
    # checking and kills dd just as EIO arrives (rc=124, reported as "no EIO").
    # Two failure shapes are both correct here, and which one comes back depends
    # only on how far into the outage the attempt lands: EIO while the node is
    # merely unable to publish, and ECONNABORTED once its self-fence watchdog
    # (2-3x the lease TTL) has torn the FUSE session down. Demanding EIO every
    # time made the watchdog doing its job look like a failure. What must never
    # happen is a *successful* fsync while the flush cannot commit.
    local FAILED_COUNT=0 EIO_COUNT=0 k out
    for k in 1 2 3; do
        out=$(runcmd30 "$N1" "dd if=/dev/zero of=/mnt/etcfuse/s11-file.txt bs=1 count=1 conv=notrunc,fsync 2>&1")
        if echo "$out" | grep -qi "input/output error\|i/o error"; then
            EIO_COUNT=$((EIO_COUNT+1)); FAILED_COUNT=$((FAILED_COUNT+1))
        elif echo "$out" | grep -qi "connection abort\|transport endpoint is not connected"; then
            FAILED_COUNT=$((FAILED_COUNT+1))
            log "    fsync attempt $k hit the self-fenced mount (expected past the fence window)"
        else
            log "    fsync attempt $k neither failed nor fenced: $(echo "$out" | tr '\n' ' ' | cut -c1-120)"
        fi
        sleep 1
    done
    # shellcheck disable=SC2015
    [[ "$FAILED_COUNT" -eq 3 && "$EIO_COUNT" -ge 1 ]] && { PASS=$((PASS+1)); log "  PASS: fsync never succeeded while etcd was unreachable ($EIO_COUNT EIO, $((FAILED_COUNT-EIO_COUNT)) fenced)"; } || { FAIL=$((FAIL+1)); log "  FAIL: fsync did not consistently fail ($FAILED_COUNT/3 failed, $EIO_COUNT EIO)"; }

    # No partial publication: a peer must never see a value that never
    # actually committed.
    local V2
    V2=$(readf "$N2" "s11-file.txt")
    # shellcheck disable=SC2015
    [[ "$V2" == "seed" ]] && { PASS=$((PASS+1)); log "  PASS: no partial publication visible to a peer"; } || { FAIL=$((FAIL+1)); log "  FAIL: peer saw unpublished data: '$V2'"; }

    log "  Restoring n1's etcd connectivity..."
    rejoin_etcd 1
    sleep 5
    local R=$(restart_node 1)
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly after etcd recovery ($R)"
    wait_writable 1 || log "  WARN: n1 is not serving writes again yet"
}

run_s12() {
    log "======== S12: Recall storm ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi

    # Pre-create the files, so the storm is contention on existing inodes rather
    # than 15 racing O_CREAT|O_EXCL-shaped creates of the same name reporting
    # "File exists" — that race is S8's business, not this one's.
    local i
    for i in 1 2 3 4 5; do
        writef "$N1" "seed-$i" "s12-f$i.txt" || { FAIL=$((FAIL+1)); log "  FAIL: seed write s12-f$i.txt did not land"; dump_logs "$N1"; return; }
    done

    local START END DUR
    local -a PIDS=()
    START=$(date +%s)
    for i in 1 2 3 4 5; do
        writef "$N1" "n1-$i" "s12-f$i.txt" & PIDS+=($!)
        writef "$N2" "n2-$i" "s12-f$i.txt" & PIDS+=($!)
        writef "$N3" "n3-$i" "s12-f$i.txt" & PIDS+=($!)
    done
    local FAILED_WRITES=0 p
    for p in "${PIDS[@]}"; do wait "$p" || FAILED_WRITES=$((FAILED_WRITES+1)); done
    END=$(date +%s)
    DUR=$((END-START))
    log "  15 concurrent writes across 5 contended inodes took ${DUR}s, $FAILED_WRITES failed"
    # shellcheck disable=SC2015
    [[ "$DUR" -le 30 ]] && { PASS=$((PASS+1)); log "  PASS: bounded latency (<=30s), no deadlock"; } || { FAIL=$((FAIL+1)); log "  FAIL: exceeded 30s budget, possible deadlock"; }

    # Contention must make a write wait, not fail: an EIO here is the recall
    # queue outrunning the acquisition budget, which is what lockKeyAttempts
    # exists to cover.
    # shellcheck disable=SC2015
    [[ "$FAILED_WRITES" -eq 0 ]] && { PASS=$((PASS+1)); log "  PASS: every contended write completed, none returned an error"; } || { FAIL=$((FAIL+1)); log "  FAIL: $FAILED_WRITES/15 contended writes failed"; dump_logs "$(meta_of 1)"; }

    # "No lock left behind" is NOT "no lock keys in etcd": a cached lock is
    # deliberately held past the operation that took it (internal/ipc/lockcache.go),
    # so keys outliving the storm are the feature working. What must not survive
    # is a lock nobody can take back — so the check is that every contended inode
    # is still acquirable, from a node other than the one that wrote it last.
    sleep 2
    local KEYS STUCK=0
    KEYS=$(etcdctl_on get lock: --prefix --keys-only 2>/dev/null | grep -c '^lock:')
    log "  $KEYS cached lock keys still held after the storm (expected: cached, not leaked)"
    for i in 1 2 3 4 5; do
        writef "$N2" "post-storm-$i" "s12-f$i.txt" || STUCK=$((STUCK+1))
    done
    # shellcheck disable=SC2015
    [[ "$STUCK" -eq 0 ]] && { PASS=$((PASS+1)); log "  PASS: every contended inode still acquirable, no lock stranded"; } || { FAIL=$((FAIL+1)); log "  FAIL: $STUCK/5 inodes left locked against the cluster"; dump_logs "$(meta_of 1)"; }
}

run_s13() {
    log "======== S13: Read-after-recall across nodes (page cache) ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "version-1" "s13-file.txt"; then FAIL=$((FAIL+1)); log "  FAIL: initial write did not land"; dump_logs "$N1"; return; fi

    # Node A (N2) reads and warms its kernel page cache.
    local RA1
    RA1=$(readf "$N2" "s13-file.txt")
    [[ "$RA1" == "version-1" ]] || log "  WARN: node A's first read was unexpected: '$RA1'"

    # Node B (N3) writes new data, which must recall N2's cache.
    writef "$N3" "version-2" "s13-file.txt"

    # Node A reads again — must see the fresh write, not a stale kernel page.
    local RA2
    RA2=$(readf "$N2" "s13-file.txt")
    # shellcheck disable=SC2015
    [[ "$RA2" == "version-2" ]] && { PASS=$((PASS+1)); log "  PASS: node A saw the fresh write, no stale page"; } || { FAIL=$((FAIL+1)); log "  FAIL: node A read stale data: '$RA2'"; }
}

# ============================================================
# MAIN — provision ONCE, run scenarios in sequence, teardown ONCE.
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

case "$SCENARIO" in
    1) run_s1 ;; 2) run_s2 ;; 3|4) run_s3 ;; 5) run_s5 ;; 6) run_s6 ;; 7) run_s7 ;;
    8) run_s8 ;; 9) run_s9 ;; 10) run_s10 ;; 11) run_s11 ;; 12) run_s12 ;; 13) run_s13 ;;
    all) run_s1; run_s2; run_s3; run_s5; run_s6; run_s7; run_s8; run_s9; run_s10; run_s11; run_s12; run_s13 ;;
    *) log "unknown scenario: $SCENARIO" ;;
esac

# On by default, docker only (histories are recorded to a docker volume): a
# violation here means an invariant the rest of this suite depends on broke,
# which is worth failing the run over rather than logging past. Set
# VERIFY_HISTORY=0 to skip it.
if [[ "$MODE" == "docker" && "${VERIFY_HISTORY:-1}" == "1" ]]; then
    log "Checking recorded operation histories..."
    # Every node is SIGKILLed by this suite (n1 in S1/S2/S7/S9, all three in
    # S6), so all three are named as crashed: a killed node legitimately loses
    # writes it had buffered but not yet flushed, and the extent model needs to
    # be told which nodes may excuse that rather than report it as loss under a
    # healthy cluster.
    if ! CRASHED="n1,n2,n3" KEEP_HISTORY_DIR="$REPORT_DIR/histories" \
        "$SCRIPT_DIR/verify-chaos-history.sh" | tee -a "$REPORT_DIR/chaos.log"; then
        FAIL=$((FAIL+1))
        log "FAIL: history verification found a violation or could not run"
    fi
fi

teardown_cluster

{
    echo "=== Single-Cluster Chaos Test Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
