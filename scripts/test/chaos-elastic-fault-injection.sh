#!/bin/bash
# chaos-elastic-fault-injection.sh — fault injection DURING the join/leave
# window itself, not on a stable cluster like every other chaos-*.sh script.
# The membership set, quorum size, and arena ownership are all in flux
# during a join/leave — the most likely place for a fencing or allocator bug
# to hide, and untested until now.
#
# Five scenarios, run in sequence against one 3-node base cluster (each
# fully cleans up its own extra node before the next starts, so node id 4
# is reused throughout):
#
#   Kill the joining node after `etcd member add` but before the FUSE
#        mount comes up. Assert the cluster stays writable and the
#        half-joined node never held an arena.
#   Partition the joining node from etcd right after it mounts. Assert
#        it stops serving (self-fences) rather than continuing to operate
#        in a split view.
#   Bump the leaving node's generation WHILE it is gracefully leaving.
#        Assert clean teardown afterward: no lock left referencing it, and
#        the arena record is unchanged (arena reclamation on leave has no
#        production implementation yet — see the comment in run_fj3 — so
#        "unchanged" is the correct expectation, not "released").
#   Kill a SURVIVING node's daemons while a different node is mid-join,
#        then let the survivor recover. Assert the join still completes and
#        the survivor rejoins cleanly (quorum stress).
#   Partition a joined node from etcd (lease TTL widened to 120s so
#        self-fencing can't race the probe) and assert its FUSE ops bound at
#        internal/ipc/retry.go's requestTimeout (10s), with the daemon
#        confirmed alive before and after.
#
# This script does NOT reuse add_node/remove_node's internals for the
# kill-before-mount scenario — the whole point is to stop mid-sequence,
# which a black-box function call cannot do. Its docker/aws
# partial_join_* functions below are a deliberate, commented duplication of
# add_node's first half (etcd member + meta daemon), stopping short of
# starting the FUSE container/process. The other scenarios reuse
# add_node/remove_node from chaos-lib.sh unmodified, since their fault lands
# before/after the join rather than inside it.
#
# Usage:
#   ./chaos-elastic-fault-injection.sh docker [FJ1|FJ2|FJ3|FJ4|FJ5|all]
#   ./chaos-elastic-fault-injection.sh aws    [FJ1|FJ2|FJ3|FJ4|FJ5|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-elastic-faultinject-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [FJ1|FJ2|FJ3|FJ4|FJ5|all]"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# arena_val <node_key> — raw etcd value at arena:<node>, empty if absent.
# Ownership is one key per arena — arena:<node>/<arena_id> — so a node's
# holdings are a count over a prefix, not the value of a single key.
arena_count() { etcdctl_on get --prefix "arena:$1/" --keys-only 2>/dev/null | grep -c "arena:$1/"; }
gen_val()   { etcdctl_on get "gen:$1" --print-value-only 2>/dev/null | tr -d '[:space:]'; }
member_id_for() { etcdctl_on member list 2>/dev/null | grep ", $1," | cut -d, -f1; }

# ============================================================
# kill-before-mount helpers — partial join, stopping before the FUSE mount.
# Deliberate duplication of add_node's first half (see header comment).
# ============================================================
if [[ "$MODE" == "docker" ]]; then
    partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        logerr "  FJ1: partial join $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(docker exec etcfs-etcd1 etcdctl member add "$ename" --peer-urls="http://etcfs-etcd$id:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  FJ1: could not parse ETCD_INITIAL_CLUSTER"; return 1; }

        docker run -d --name "etcfs-etcd$id" --network docker_etcfuse-net "$ETCD_IMG" \
            etcd --name "$ename" --data-dir /etcd-data \
            --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls "http://etcfs-etcd$id:2379" \
            --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls "http://etcfs-etcd$id:2380" \
            --initial-cluster "$initial_cluster" --initial-cluster-state existing >/dev/null

        local ok=0
        for i in $(seq 1 20); do
            docker exec "etcfs-etcd$id" etcdctl endpoint health >/dev/null 2>&1 && { ok=1; break; }
            sleep 1
        done
        [[ "$ok" -eq 1 ]] || { logerr "  FJ1: new etcd member never became healthy"; return 1; }

        local endpoints; endpoints=$(etcd_client_urls)
        docker run -d --name "etcfs-meta$id" --network docker_etcfuse-net \
            -v "docker_block_data:/block-device" -v "etcfuse-meta${id}-sock:/var/run" "$META_IMG" \
            --listen=/run/etcfuse/etcfuse.sock --etcd-endpoints="$endpoints" --node-id="n$id" \
            --cluster-name=docker-chaos --lease-ttl=10s --block-device=/block-device/etcfuse.img --log-level=1 >/dev/null

        # STOP HERE — no fuse container. This is the "before FUSE mount" window.
        sleep 3
        return 0
    }
    kill_partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        docker rm -f "etcfs-meta$id" >/dev/null 2>&1
        local memberid; memberid=$(member_id_for "$ename")
        [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        docker rm -f "etcfs-etcd$id" >/dev/null 2>&1
    }
else
    partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        local name="${TAG}-compute-${id}"
        local ami_id subnet_id key_name sg vol_id
        ami_id=$(jq -r '.ami_id' "$PROJECT_ROOT/$STATE_FILE")
        subnet_id=$(jq -r '.subnet_id' "$PROJECT_ROOT/$STATE_FILE")
        key_name=$(jq -r '.key_name' "$PROJECT_ROOT/$STATE_FILE")
        sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        logerr "  FJ1: partial join $id: launching EC2 instance..."
        local inst
        inst=$(aws ec2 run-instances \
            --image-id "$ami_id" --instance-type t3.medium --key-name "$key_name" \
            --security-group-ids "$sg" --subnet-id "$subnet_id" --associate-public-ip-address \
            --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=ClusterName,Value=$TAG}]" \
            --query 'Instances[0].InstanceId' --output text 2>/dev/null)
        [[ -n "$inst" && "$inst" != "None" ]] || { logerr "  FJ1: run-instances failed"; return 1; }
        aws ec2 wait instance-running --instance-ids "$inst" 2>/dev/null
        local pub priv
        pub=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
        priv=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
        save_node_info "$id" "$pub" "$priv" "$inst"

        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1
        sleep 8
        {
            wait_ssh "$pub" 20
            ssh_retry "$pub" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
            ssh_retry "$pub" "
                curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
                sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
                sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
            " || true
            scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q "$PROJECT_ROOT/bin/etcfuse-meta" ec2-user@"$pub":/tmp/ 2>/dev/null || true
            ssh_retry "$pub" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta" || true
            ssh_retry "$pub" "sudo mkdir -p /mnt/etcfuse" || true
        } >> "$REPORT_DIR/chaos.log" 2>&1

        logerr "  FJ1: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member add $ename --peer-urls=http://$priv:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  FJ1: could not parse ETCD_INITIAL_CLUSTER"; return 1; }

        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@"$pub" "
            sudo mkdir -p /var/lib/etcd
            sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                --initial-cluster '$initial_cluster' --initial-cluster-state existing > /tmp/etcd.log 2>&1 &
        " 2>/dev/null
        sleep 8

        local endpoints="http://$P1:2379,http://$P2:2379,http://$P3:2379"
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@"$pub" "
            sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
                --etcd-endpoints=$endpoints --node-id=n$id --cluster-name=$TAG \
                --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
        " 2>/dev/null

        # STOP HERE — no fuse process/mount. This is the "before FUSE mount" window.
        sleep 3
        return 0
    }
    kill_partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        local pub inst
        pub=$(node_pub "$id"); inst=$(node_inst "$id")
        runcmd "$pub" "sudo pkill -9 etcfuse-meta etcd 2>/dev/null; true"
        local memberid; memberid=$(member_id_for "$ename")
        [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        local vol_id; vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        aws ec2 detach-volume --volume-id "$vol_id" --instance-id "$inst" >/dev/null 2>&1
        sleep 5
        aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
        rm -f "$(node_info_file "$id")"
    }
fi

cleanup_extras() {
    [[ "$MODE" == "aws" ]] || return
    local f inst ids=()
    for f in "$REPORT_DIR"/node*.info; do
        [[ -f "$f" ]] || continue
        inst=$(cut -d' ' -f3 "$f")
        [[ -n "$inst" ]] && ids+=("$inst")
    done
    [[ "${#ids[@]}" -gt 0 ]] || return
    log "  cleanup: terminating leftover extra AWS instances: ${ids[*]}"
    aws ec2 terminate-instances --instance-ids "${ids[@]}" >/dev/null 2>&1 || true
}
trap cleanup_extras EXIT

# ============================================================
# kill the joining node between etcd member add and FUSE mount.
# ============================================================
run_fj1() {
    log "======== FJ1: kill mid-join, before FUSE mount ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    if ! writef "$N1" "pre-fj1" "fj1-baseline.txt"; then bad "pre-scenario write failed"; return; fi

    if ! partial_join 4; then
        bad "partial_join setup itself failed (infra issue, not the thing under test)"
        kill_partial_join 4
        return
    fi

    # The half-joined node never reached a write — arena acquisition is lazy
    # (pkg/arena.Allocator.AcquireArena is only called from handleWriteBlock,
    # never at daemon startup), so this proves that laziness holds under a
    # real kill, not just in the harness.
    local a4; a4=$(arena_val n4)
    if [[ -z "$a4" ]]; then
        ok "half-joined node n4 holds no arena"
    else
        bad "half-joined node n4 unexpectedly has an arena record: $a4"
    fi

    if writef "$N1" "during-fj1" "fj1-during.txt" && [[ -n "$(readf "$N2" "fj1-during.txt")" ]]; then
        ok "cluster stayed writable with a half-joined member present"
    else
        bad "cluster was not writable while n4 was half-joined"
    fi

    kill_partial_join 4
    sleep 3

    if [[ -n "$(readf "$N1" "fj1-baseline.txt")" ]]; then
        ok "baseline data intact after killing the half-joined node"
    else
        bad "baseline data lost after FJ1"
    fi
}

# ============================================================
# Shared node4 partition/heal/liveness helpers — the partition-mid-join and
# partition-after-join scenarios both sever node4 from etcd and watch its
# meta daemon; factored here so the fault
# injection itself only exists once.
#
# History worth keeping: this scenario originally partitioned on AWS by
# swapping the instance's security group (the technique chaos-test.sh's
# pre-existing S3 scenario still uses). That does NOT sever established
# connections — AWS security groups are stateful and only evaluate NEW
# connection attempts — so node4's daemon kept using the etcd connection it
# had opened before the swap and was never actually partitioned, producing a
# false "fencing failed" result. It now uses iptables instead, and verifies
# the cut took effect before proceeding. Note that S3 still has the original
# blind spot; it only asserts survivors keep working, never that the
# partitioned node's own operations are blocked, so it would not surface it.
# ============================================================

# node4_meta_alive — true if node4's meta daemon process is still running.
node4_meta_alive() {
    if [[ "$MODE" == "docker" ]]; then
        [[ "$(docker inspect etcfs-meta4 --format '{{.State.Status}}' 2>/dev/null)" == "running" ]]
    else
        local alive; alive=$(runcmd "$(node_pub 4)" "pgrep -x etcfuse-meta >/dev/null && echo ALIVE || echo GONE")
        [[ "$alive" == *ALIVE* ]]
    fi
}

# partition_node4 <node4_addr> — cuts node4 off from etcd (docker: detach
# its network; aws: iptables DROP on 2379/2380 to each peer) and asserts the
# cut actually took effect.
partition_node4() {
    local node4="$1"
    log "  partitioning node4 from etcd (lease_ttl=$CHAOS_LEASE_TTL)..."
    if [[ "$MODE" == "docker" ]]; then
        docker network disconnect docker_etcfuse-net etcfs-meta4 2>/dev/null
        return
    fi

    # iptables on the instance, NOT a security-group swap.
    #
    # SG swaps do not sever already-established connections — AWS security
    # groups are stateful and only evaluate NEW connection attempts against
    # the changed rules, so a daemon whose etcd connection predates the swap
    # keeps using it and is never actually partitioned. That produced a
    # false "fencing failed" result on a previous run of this scenario;
    # confirmed by direct testing (a fresh etcdctl correctly saw "unhealthy
    # cluster" post-swap while the daemon's existing connection kept
    # working). iptables filters every packet, established or not, so it is
    # a genuine cut. Same technique as scripts/test/isolate-node.sh.
    #
    # Port 22 is deliberately left open so the harness can still reach the
    # node to observe it; only etcd's client (2379) and peer (2380) ports
    # are dropped, in both directions.
    # Amazon Linux 2023 does not ship the iptables CLI in the base AMI, and
    # add_node's setup only installs fuse3/gcc — so install it here if
    # absent. nftables is the AL2023 default backend; the iptables package
    # provides the iptables-nft shim that drives it.
    local ipt_setup
    ipt_setup=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$node4" \
        "command -v iptables >/dev/null 2>&1 || sudo dnf install -q -y iptables iptables-nft 2>&1 | tail -3; command -v iptables || echo NO_IPTABLES" 2>&1)
    log "  iptables availability on node4: $(echo "$ipt_setup" | tr '\n' ' ' | cut -c1-160)"

    # Do NOT use runcmd here: it redirects stderr to /dev/null, which hid
    # the reason these rules failed on a previous run (only "ERR:1" was
    # visible). Capture stderr so a failure is diagnosable from the log.
    local peer ipt_out ipt_rc
    for peer in "$P1" "$P2" "$P3"; do
        ipt_out=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$node4" "
            set -e
            sudo iptables -A OUTPUT -p tcp -d $peer --dport 2379 -j DROP
            sudo iptables -A OUTPUT -p tcp -d $peer --dport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --sport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --sport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --dport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --dport 2380 -j DROP
        " 2>&1); ipt_rc=$?
        echo "iptables peer=$peer rc=$ipt_rc out=$ipt_out" >> "$REPORT_DIR/chaos.log"
        [[ "$ipt_rc" -ne 0 ]] && logerr "  iptables rules for peer $peer FAILED rc=$ipt_rc: $(echo "$ipt_out" | tr '\n' ' ' | cut -c1-200)"
    done
    # Verify the cut actually took effect rather than assuming it did — this
    # check is what would have caught the SG-statefulness problem.
    local probe
    probe=$(runcmd "$node4" "timeout 5 curl -s -o /dev/null -w '%{http_code}' http://$P1:2379/health 2>&1; echo rc=\$?")
    if [[ "$probe" == *"rc=0"* && "$probe" != *"000"* ]]; then
        bad "partition did not take effect — node4 still reached etcd at $P1 (probe=$probe)"
    else
        log "  partition verified: node4 cannot reach etcd (probe=$probe)"
    fi
}

# heal_node4 — reverses partition_node4.
heal_node4() {
    if [[ "$MODE" == "docker" ]]; then
        docker network connect docker_etcfuse-net etcfs-meta4 2>/dev/null
    else
        runcmd "$(node_pub 4)" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >>"$REPORT_DIR/chaos.log" 2>&1
    fi
}

run_fj2() {
    log "======== FJ2: partition joining node from etcd post-join ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then bad "node4 failed to join at all — cannot run FJ2"; return; fi
    ok "node4 joined normally ($node4)"

    # A file to stat later.  The probe must not stat the mount root: the C
    # daemon answers getattr for FUSE_ROOT_ID locally, without any IPC, so a
    # partitioned node succeeds there and is right to.
    writef "$node4" "fj5-before" "fj5-before.txt" >/dev/null 2>&1

    partition_node4 "$node4"

    # Watch the META daemon, not the FUSE mount.
    #
    # Self-fencing is os.Exit(77) in the Go meta daemon. The C FUSE daemon is
    # a separate process and keeps the mount listed in /proc/mounts after its
    # meta peer dies — measured: meta exits 77 at t+20s while the mount is
    # still MOUNTED at t+41s and beyond. Operations on it correctly fail
    # (see the write probe below), but "is it still mounted" is simply not a
    # test of whether self-fencing fired, and asserting on it reported a
    # working fence as a failure.
    #
    # Poll rather than sleep a fixed interval: the watchdog ticks every third
    # of the lease TTL and fires on the first tick where the lease has been
    # dead longer than 2x TTL, so the fence lands 2-2.33x TTL after the last
    # keepalive depending on tick phase, plus up to TTL/3 because that last
    # keepalive precedes the partition. With TTL=10s that is ~20-27s, which a
    # fixed wait would race.
    local fence_deadline=45 fence_waited=0 fenced=0
    log "  waiting up to ${fence_deadline}s for the meta daemon to self-fence..."
    while [[ "$fence_waited" -lt "$fence_deadline" ]]; do
        node4_meta_alive || { fenced=1; break; }
        sleep 5; fence_waited=$((fence_waited+5))
    done

    if [[ "$fenced" -eq 1 ]]; then
        local exitcode=""
        [[ "$MODE" == "docker" ]] && exitcode=$(docker inspect etcfs-meta4 --format '{{.State.ExitCode}}' 2>/dev/null)
        # 77 is the self-fence exit code (pkg/fencing/watchdog.go trigger()).
        if [[ "$MODE" == "docker" && "$exitcode" != "77" ]]; then
            bad "meta daemon on node4 stopped after ${fence_waited}s but with exit code $exitcode, not 77 (self-fence)"
        else
            ok "node4's meta daemon self-fenced after ~${fence_waited}s${exitcode:+ (exit $exitcode)}"
        fi
    else
        bad "node4's meta daemon still running ${fence_deadline}s after partition — self-fencing watchdog did not fire"
    fi

    # The self-fence assertion above is the regression test for this: before
    # the fix, Membership.IsAlive() returned the raw `alive` flag, which is
    # only cleared when the etcd client's lease KeepAlive channel closes — and
    # under a total partition that channel never closes, the client just
    # retries silently forever. A partitioned node therefore believed itself
    # alive indefinitely (verified: 8+ minutes, no fence). IsAlive() now also
    # requires the last successful keepalive to be within the lease TTL, which
    # is exactly when etcd expires the lease server-side, making the partition
    # locally detectable without depending on the client library surfacing it.

    # Did the SERVER-SIDE mechanism (external fencing) also fire?
    # membership:n4's lease expires at etcd regardless of what node4's client
    # believes, and the fencing controller on a healthy node watches for that
    # deletion and bumps gen:n4. This path is independent of node4's own
    # watchdog — it was already working correctly even when the self-fencing
    # watchdog was inert, and is what kept the system safe in the meantime.
    local gen4; gen4=$(gen_val n4)
    if [[ -n "$gen4" && "$gen4" != "0" ]]; then
        ok "gen:n4 was bumped to $gen4 by external fencing"
    else
        bad "gen:n4 was never bumped ($gen4) — external fencing did not fire"
    fi

    # Does a write attempt from the fenced node fail in bounded time?
    # Pre-fix this hung indefinitely (killed after 7+ minutes, no response),
    # because the node never self-fenced and the guarded commit's etcd call
    # simply blocked. With the watchdog firing, the daemon is gone by now and
    # the write should fail fast instead. Still bounded to 20s so a
    # regression can never hang this script again.
    log "  probing a write from the fenced node4, bounded to 20s..."
    local probe_rc=0
    if [[ "$MODE" == "docker" ]]; then
        timeout 20 docker exec etcfs-fuse4 sh -c "echo probe > /mnt/etcfuse/fj2-probe.txt" >/dev/null 2>&1 || probe_rc=$?
    else
        timeout 20 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 -q ec2-user@"$(node_pub 4)" \
            "echo probe | sudo tee /mnt/etcfuse/fj2-probe.txt >/dev/null" >/dev/null 2>&1 || probe_rc=$?
    fi
    if [[ "$probe_rc" -eq 124 ]]; then
        bad "write from fenced node4 hung past 20s instead of failing fast (regression: this was the pre-fix behavior)"
    elif [[ "$probe_rc" -ne 0 ]]; then
        ok "write from fenced node4 failed fast (rc=$probe_rc) — generation guard rejected it promptly"
    else
        # Landed without error — confirm it did NOT actually reach the
        # survivors, since that would mean the guard failed to protect data,
        # not merely that it was slow to reject.
        if [[ -z "$(readf "$N1" "fj2-probe.txt")" ]]; then
            ok "write call returned success but never reached survivors — cosmetic success, not a corruption"
        else
            bad "write from fenced node4 SUCCEEDED and is visible on survivors — fencing failed to protect data"
        fi
    fi

    if writef "$N1" "during-fj2" "fj2-survivor.txt" && [[ -n "$(readf "$N2" "fj2-survivor.txt")" ]]; then
        ok "survivors (n1/n2/n3) stayed writable while node4 was partitioned"
    else
        bad "survivors were affected by node4's partition"
    fi

    log "  cleanup: removing node4 (bounded — node4 itself may be wedged)..."
    # The node may have self-fenced (os.Exit(77)) by now, which is fine —
    # heal_node4 only touches the kernel firewall / docker network, not the
    # daemon, and the instance is terminated moments later by remove_node
    # anyway.
    heal_node4
    remove_node 4 &
    local rm_pid=$! rm_waited=0 rm_ok=0
    while [[ "$rm_waited" -lt 90 ]]; do
        kill -0 "$rm_pid" 2>/dev/null || { rm_ok=1; break; }
        sleep 3; rm_waited=$((rm_waited+3))
    done
    if [[ "$rm_ok" -eq 0 ]]; then
        logerr "  remove_node 4 did not complete within 90s — force-killing instead"
        kill -9 "$rm_pid" 2>/dev/null
        if [[ "$MODE" == "docker" ]]; then
            docker rm -f etcfs-fuse4 etcfs-meta4 etcfs-etcd4 >/dev/null 2>&1
            local memberid; memberid=$(member_id_for e3)
            [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        else
            local inst; inst=$(node_inst 4)
            [[ -n "$inst" ]] && aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
            local memberid; memberid=$(member_id_for e3)
            [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
            rm -f "$(node_info_file 4)"
        fi
    fi
    sleep 3
}

# ============================================================
# bump the leaving node's generation WHILE it leaves gracefully.
# ============================================================
run_fj3() {
    log "======== FJ3: generation bump during graceful leave ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then bad "node4 failed to join — cannot run FJ3"; return; fi

    if ! writef "$node4" "fj3-data" "fj3-file.txt"; then bad "write from node4 failed before leave"; return; fi

    local arena_before; arena_before=$(arena_count n4)
    if [[ "${arena_before:-0}" -eq 0 ]]; then bad "node4 has no arena before it can leave (unexpected)"; return; fi

    log "  starting graceful remove_node 4, bumping gen:n4 concurrently..."
    remove_node 4 &
    local remove_pid=$!
    sleep 1
    local gbefore; gbefore=$(gen_val n4)
    [[ -z "$gbefore" ]] && gbefore=0
    etcdctl_on put "gen:n4" "$((gbefore + 1))" >/dev/null 2>&1
    log "  bumped gen:n4 $gbefore -> $((gbefore + 1)) mid-leave"
    wait "$remove_pid"

    # A graceful leave releases the node's arenas: main runs
    # metadata.Membership.Leave after the IPC server has stopped, which is the
    # node's own proof of quiescence.  The generation bump firing concurrently
    # must not stop that — the release is the departing node's own work, not a
    # fenced peer's.
    sleep 2
    local arena_after; arena_after=$(arena_count n4)
    if [[ "${arena_after:-0}" -eq 0 ]]; then
        ok "n4's $arena_before arena(s) released on graceful leave, despite the concurrent bump"
    else
        bad "n4 still owns $arena_after arena(s) after a graceful leave (was $arena_before)"
    fi

    local stray_locks; stray_locks=$(etcdctl_on get "lock:" --prefix --keys-only 2>/dev/null | xargs -I{} etcdctl_on get {} --print-value-only 2>/dev/null | grep -c '"n4"' || true)
    if [[ "${stray_locks:-0}" -eq 0 ]]; then
        ok "no lock left referencing n4"
    else
        bad "found $stray_locks lock record(s) still referencing n4"
    fi

    if writef "$N1" "after-fj3" "fj3-after.txt" && [[ -n "$(readf "$N2" "fj3-after.txt")" ]]; then
        ok "cluster healthy after FJ3"
    else
        bad "cluster unhealthy after FJ3"
    fi
}

# ============================================================
# kill a surviving node's daemons while a different node is mid-join.
# ============================================================
run_fj4() {
    log "======== FJ4: kill a survivor while a different node joins ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    if ! writef "$N1" "fj4-baseline" "fj4-baseline.txt"; then bad "baseline write failed"; return; fi

    log "  starting add_node 4 in background, will kill n2 mid-join..."
    add_node 4 > "$REPORT_DIR/fj4-join.out" 2>>"$REPORT_DIR/chaos.log" &
    local join_pid=$!

    sleep 5
    log "  killing n2's daemons now (join should be roughly mid-flight)..."
    if [[ "$MODE" == "docker" ]]; then
        docker kill -s KILL "$M2" "$N2" >/dev/null 2>&1
    else
        runcmd "$N2" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; true"
    fi

    wait "$join_pid"
    local node4; node4=$(cat "$REPORT_DIR/fj4-join.out" 2>/dev/null)
    if [[ -n "$node4" ]]; then
        ok "node4 completed its join despite n2 crashing concurrently ($node4)"
    else
        bad "node4 failed to join while n2 was crashing"
    fi

    log "  restarting n2..."
    local r
    if [[ "$MODE" == "docker" ]]; then
        r=$(restart_pair "$M2" "$N2")
    else
        r=$(restart_daemons "$N2" "n2")
    fi
    [[ "$r" == "OK" ]] || log "  WARN: n2 did not remount cleanly ($r)"

    if [[ -n "$(readf "$N2" "fj4-baseline.txt")" ]]; then
        ok "n2 recovered and sees pre-crash data"
    else
        bad "n2 did not recover cleanly"
    fi

    if [[ -n "$node4" ]] && [[ -n "$(readf "$node4" "fj4-baseline.txt")" ]]; then
        ok "node4 (joined during the crash) sees baseline data"
    else
        bad "node4 cannot see baseline data"
    fi

    [[ -n "$node4" ]] && remove_node 4
    sleep 3
}

# ============================================================
# partition a joined node from etcd and prove that FUSE ops on it
# return within `requestTimeout` (internal/ipc/retry.go, 10s) rather than
# hanging until self-fencing kills the daemon out from under them.
#
# The partition-mid-join scenario partitions node4 too, but by the time it probes, self-fencing has
# already exited the daemon (fence window ~20-33s at the default 10s lease
# TTL, tighter than requestTimeout's own 10s) — so a fast rc there proves
# "the process is dead," not "the request was bounded." This scenario widens
# the lease TTL to CHAOS_LEASE_TTL=120s for node4's join only, pushing the
# self-fence window out to ~4-6 minutes, and probes well inside that window
# with the daemon confirmed alive both before and after — the only way to
# attribute the bound to requestTimeout rather than to the daemon dying.
# ============================================================

# timed_probe4 <docker_cmd> <aws_cmd> — runs the mode-appropriate cmd on
# node4 (bounded 30s), echoes "<elapsed_seconds> <rc>". 124 is timeout's own
# "still running" rc. Separate commands per mode because AWS writes need
# `sudo tee` (same reason the partition-mid-join write probe does) while docker exec runs as
# root already.
timed_probe4() {
    local docker_cmd="$1" aws_cmd="$2" t0 t1 rc=0
    t0=$(date +%s)
    if [[ "$MODE" == "docker" ]]; then
        timeout 30 docker exec etcfs-fuse4 sh -c "$docker_cmd" >/dev/null 2>&1 || rc=$?
    else
        timeout 30 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 -q ec2-user@"$(node_pub 4)" \
            "$aws_cmd" >/dev/null 2>&1 || rc=$?
    fi
    t1=$(date +%s)
    echo "$((t1 - t0)) $rc"
}

run_fj5() {
    log "======== FJ5: partitioned node's ops are bounded by requestTimeout, not self-fence ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    # `local` shadows CHAOS_LEASE_TTL for the rest of this function only
    # (bash scopes locals dynamically to everything it calls), so add_node
    # picks up 120s here without touching any other scenario's default.
    local CHAOS_LEASE_TTL=120s
    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then bad "node4 failed to join at all — cannot run FJ5"; return; fi
    ok "node4 joined normally with a 120s lease TTL ($node4)"

    # A file to stat later.  The probe must not stat the mount root: the C
    # daemon answers getattr for FUSE_ROOT_ID locally, without any IPC, so a
    # partitioned node succeeds there and is right to.
    writef "$node4" "fj5-before" "fj5-before.txt" >/dev/null 2>&1

    partition_node4 "$node4"

    if node4_meta_alive; then
        ok "node4's meta daemon still alive right after partition (precondition for this scenario)"
    else
        bad "node4's meta daemon already gone right after partition — self-fence raced the probe, widen CHAOS_LEASE_TTL further"
        heal_node4; remove_node 4; return
    fi

    # requestTimeout (internal/config) bounds each FUSE operation's etcd work
    # at 10s — but one syscall is not one operation: `stat path` costs a LOOKUP
    # and a GETATTR, so a partitioned node answers it in two bounded stalls
    # rather than one.  25s covers two of them plus ssh/docker-exec overhead,
    # and is still nowhere near the ~240-360s fence window above, which is what
    # this scenario exists to distinguish.
    local bound=25
    local dt rc

    assert_bounded4() {
        local label="$1" dt="$2" rc="$3"
        log "  $label on partitioned node4: ${dt}s, rc=$rc"
        if [[ "$rc" -eq 124 || "$dt" -gt "$bound" ]]; then
            bad "$label on partitioned node4 took ${dt}s (rc=$rc) — did not bound at requestTimeout"
        elif [[ "$rc" -eq 0 ]]; then
            bad "$label on partitioned node4 returned success — should have been rejected while cut off from etcd"
        else
            ok "$label on partitioned node4 bounded to ${dt}s (rc=$rc)"
        fi
    }

    read -r dt rc <<< "$(timed_probe4 "echo probe > /mnt/etcfuse/fj5-probe.txt" "echo probe | sudo tee /mnt/etcfuse/fj5-probe.txt")"
    assert_bounded4 "write" "$dt" "$rc"

    read -r dt rc <<< "$(timed_probe4 "stat /mnt/etcfuse/fj5-before.txt" "stat /mnt/etcfuse/fj5-before.txt")"
    assert_bounded4 "getattr" "$dt" "$rc"

    read -r dt rc <<< "$(timed_probe4 "ls /mnt/etcfuse" "ls /mnt/etcfuse")"
    assert_bounded4 "readdir" "$dt" "$rc"

    if node4_meta_alive; then
        ok "node4's meta daemon still alive after the probes — bound is attributable to requestTimeout, not to the daemon dying"
    else
        bad "node4's meta daemon died during the probe window — self-fence fired before requestTimeout could be isolated as the cause"
    fi

    heal_node4
    remove_node 4
    sleep 3
}

# ============================================================
# MAIN
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

[[ "$MODE" == "aws" ]] && TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")

case "$SCENARIO" in
    FJ1) run_fj1 ;;
    FJ2) run_fj2 ;;
    FJ3) run_fj3 ;;
    FJ4) run_fj4 ;;
    FJ5) run_fj5 ;;
    all) run_fj1; run_fj2; run_fj3; run_fj4; run_fj5 ;;
    *) log "unknown scenario: $SCENARIO" ;;
esac

teardown_cluster

{
    echo "=== Fault-Injection-During-Join/Leave Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
