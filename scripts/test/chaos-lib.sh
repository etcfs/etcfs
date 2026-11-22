#!/bin/bash
# chaos-lib.sh — shared docker/aws transport for the single-cluster chaos
# scripts. Source this after setting MODE=docker|aws, PROJECT_ROOT, REPORT_DIR,
# and defining log()/logerr(). Provides: runcmd/runcmd30/runcmd60, check_mount,
# readf, writef, dump_logs, restart_fuse, restart_pair, partition_node,
# heal_node, etcdctl_on, provision_cluster, teardown_cluster.
#
# Every ssh/scp here carries StrictHostKeyChecking=no, UserKnownHostsFile=
# /dev/null and LogLevel=ERROR together. The second of those is the load-bearing
# one and is easy to mistake for redundant — see the comment on SSH_OPTS in
# scripts/infra/state.sh for why a recycled EC2 IP breaks ssh outright without
# it. Keep the three together when adding a new call site.
COMPOSE="docker compose -f $PROJECT_ROOT/deploy/docker/docker-compose.yml"

# CHAOS_LEASE_TTL overrides the --lease-ttl every daemon launches with (default
# 10s everywhere below). Self-fence lands 2-3x this value (pkg/fencing/
# watchdog.go), so a scenario that needs to observe behavior *before* self-fence
# widens this instead of racing the default window.
CHAOS_LEASE_TTL="${CHAOS_LEASE_TTL:-10s}"

# Burstable classes (t2, t3, t3a, t4g) are refused: once a run spends its CPU
# credits it is throttled to the baseline rate, and the scenario's timings
# measure the throttle instead of the filesystem.
if [[ "${ETCFS_INSTANCE_TYPE:-}" == t[0-9]* ]]; then
    echo "ERROR: burstable instance type $ETCFS_INSTANCE_TYPE is not allowed; use m7i.large or larger" >&2
    exit 1
fi

if [[ "$MODE" == "docker" ]]; then
    N1=etcfs-fuse1; N2=etcfs-fuse2; N3=etcfs-fuse3
    M1=etcfs-meta1; M2=etcfs-meta2; M3=etcfs-meta3

    runcmd()   { docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }
    runcmd30() { timeout 30 docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }
    runcmd60() { timeout 60 docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }

    check_mount() { docker exec "$1" sh -c "grep -q ' /mnt/etcfuse ' /proc/mounts" 2>/dev/null; }

    readf() {
        local out rc
        out=$(docker exec "$1" cat "/mnt/etcfuse/$2" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    readf($1,$2) failed rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        printf '%s' "$out"
    }
    writef() {
        local out rc
        out=$(printf '%s' "$2" | docker exec -i "$1" sh -c "cat > /mnt/etcfuse/$3" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    writef($1,$3) FAILED rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        return 0
    }
    rmf()  { docker exec "$1" rm -f "/mnt/etcfuse/$2" 2>&1; }
    mvf()  { docker exec "$1" mv "/mnt/etcfuse/$2" "/mnt/etcfuse/$3" 2>&1; }
    mkdirf() { docker exec "$1" mkdir -p "/mnt/etcfuse/$2" 2>&1; }
    lsf()  { docker exec "$1" sh -c "ls /mnt/etcfuse/ 2>/dev/null"; }

    dump_logs() {
        log "  ---- daemon logs from $1 ----"
        docker logs --tail 40 "$1" 2>&1 | while IFS= read -r line; do log "    $line"; done
        log "  ---- end daemon logs ----"
    }
    # restart_fuse <fuse_container> — SIGKILL + restart just the FUSE
    # container, paired meta container keeps running (mirrors S1).
    restart_fuse() {
        docker kill -s KILL "$1" >/dev/null 2>&1
        docker start "$1" >/dev/null 2>&1
        for i in $(seq 1 20); do
            check_mount "$1" && { echo OK; return; }
            sleep 1
        done
        echo FAIL
    }
    # restart_pair <meta_container> <fuse_container> — kill+restart both
    # (mirrors S2/S7).
    restart_pair() {
        docker kill -s KILL "$1" "$2" >/dev/null 2>&1
        docker start "$1" >/dev/null 2>&1
        sleep 3
        docker start "$2" >/dev/null 2>&1
        for i in $(seq 1 20); do
            check_mount "$2" && { echo OK; return; }
            sleep 1
        done
        echo FAIL
    }
    partition_node() { docker network disconnect docker_etcfuse-net "$1" 2>/dev/null; docker network disconnect docker_etcfuse-net "$2" 2>/dev/null; }
    heal_node()      { docker network connect docker_etcfuse-net "$1" 2>/dev/null; docker network connect docker_etcfuse-net "$2" 2>/dev/null; }
    etcdctl_on()     { docker exec etcfs-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 "$@"; }

    # ---- Node-indexed operations (1|2|3), identical in meaning across modes.
    # S8-S13 are written against these rather than against container names or
    # IPs, because the two transports disagree about what a "node" even is:
    # docker splits the FUSE and metadata daemons into a container each, AWS
    # runs both on one instance. A scenario that spelled either out could only
    # ever run on one of them.
    node_ip()   { eval "echo \$N$1"; }
    meta_of()   { eval "echo \$M$1"; }
    restart_node() { restart_pair "$(meta_of "$1")" "$(node_ip "$1")"; }

    # isolate_etcd <idx> — cut this node's metadata daemon off etcd while
    # leaving the FUSE daemon talking to it over their shared socket, so the
    # node keeps serving I/O with no way to commit it. Detaching the meta
    # container from the network is exactly that separation.
    isolate_etcd() { docker network disconnect docker_etcfuse-net "$(meta_of "$1")" 2>/dev/null; }
    rejoin_etcd()  { docker network connect docker_etcfuse-net "$(meta_of "$1")" 2>/dev/null; }

    # meta_log <idx> — the metadata daemon's log, for scenarios asserting on
    # something only it reports (S8's recall).
    meta_log() { docker logs "$(meta_of "$1")" 2>&1; }

    provision_cluster() {
        log "  Building + starting docker-compose cluster..."
        $COMPOSE down -v >/dev/null 2>&1 || true

        # The build output is captured rather than discarded.  A failed build
        # used to be invisible: `up` exited non-zero into a suppressed stream,
        # the wait loop below then spun for a minute against containers that
        # had never been created, and the run reported "never mounted" — which
        # reads as a product failure and is not one.
        local buildlog
        buildlog=$(mktemp)
        if ! $COMPOSE up -d --build >"$buildlog" 2>&1; then
            log "  ERROR: docker compose up --build failed"
            tail -30 "$buildlog" | while IFS= read -r line; do log "    $line"; done
            rm -f "$buildlog"
            return 1
        fi
        rm -f "$buildlog"

        log "  Waiting for mounts..."
        for c in "$N1" "$N2" "$N3"; do
            local ok=0
            # 60 x 2s.  The mount itself takes seconds; the budget is this
            # large because on a cold cache the images are still settling and
            # the daemons wait on etcd's own health check.
            for i in $(seq 1 60); do check_mount "$c" && { ok=1; break; }; sleep 2; done
            if [[ "$ok" -ne 1 ]]; then
                # Distinguish "the container is gone" from "it is up but has
                # not mounted": they have completely different causes, and
                # dump_logs cannot say anything about a container that does
                # not exist.
                if ! docker inspect "$c" >/dev/null 2>&1; then
                    log "  ERROR: $c does not exist — compose never created it, or something removed it"
                else
                    log "  ERROR: $c is running but never mounted /mnt/etcfuse"
                    dump_logs "$c"
                fi
                return 1
            fi
        done
        # One listing per node, so every recorded history actually contains
        # readdir traffic.  The workload is otherwise all writes and reads,
        # which never list a directory, and the namespace model's readdir
        # checks would then be exercised by nothing at chaos scale.
        for c in "$N1" "$N2" "$N3"; do
            docker exec "$c" ls -la /mnt/etcfuse >/dev/null 2>&1 || true
        done
        log "  Cluster up: $N1 $N2 $N3"
    }
    teardown_cluster() {
        log "  Tearing down docker-compose cluster..."
        # Nodes added during the run are not part of the compose project, so
        # `compose down` leaves them behind — and a leftover etcd member makes
        # the *next* run's `member add` fail with "Peer URLs already exists",
        # which reads as a product failure and is not one.
        for id in 4 5 6 7 8; do
            docker rm -f "etcfs-fuse$id" "etcfs-meta$id" "etcfs-etcd$id" >/dev/null 2>&1
            docker volume rm "etcfuse-meta${id}-sock" >/dev/null 2>&1
        done
        # Synchronous, deliberately.  Backgrounded, this outlived the script
        # that started it: the next run's provision would create its
        # containers and this stale teardown would then delete them, which
        # surfaced as "No such container" a minute later in the mount wait.
        # A few seconds at the end of a run is worth not racing the next one.
        $COMPOSE down -v >/dev/null 2>&1 || true
    }

    # add_node <id> / remove_node <id> — join/remove a node (id 4, 5, ...)
    # against the running cluster. Shared by chaos-elastic.sh (sequential
    # add) and chaos-elastic-concurrent.sh (parallel add) so the join/leave
    # logic only exists once.
    ETCD_IMG="quay.io/coreos/etcd:v3.5.18"
    META_IMG="docker-etcfuse-meta1:latest"
    FUSE_IMG="docker-etcfuse1:latest"

    etcd_client_urls() {
        local out=""
        for c in etcfs-etcd1 etcfs-etcd2 etcfs-etcd3 etcfs-etcd4 etcfs-etcd5; do
            docker inspect "$c" >/dev/null 2>&1 && out="${out:+$out,}http://$c:2379"
        done
        echo "$out"
    }

    add_node() {
        local id="$1"
        local ename="e$((id-1))"
        # etcd rejects `member add` with "unhealthy cluster" during the
        # brief internal stabilization window right after a previous
        # member join, even when endpoint health already reports OK —
        # so retry the add itself rather than trust a health pre-check.
        logerr "  add_node $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(docker exec etcfs-etcd1 etcdctl member add "$ename" --peer-urls="http://etcfs-etcd$id:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            # An add that succeeded but could not be read back — the cluster
            # reported unhealthy on a later retry, say — leaves the member in
            # place, and every retry then fails with "Peer URLs already
            # exists".  The member list has everything the joiner needs, so
            # rebuild the initial cluster from it rather than giving up.
            if echo "$addout" | grep -q "Peer URLs already exists"; then
                initial_cluster=$(docker exec etcfs-etcd1 etcdctl member list 2>/dev/null |
                    awk -F', ' '$3 != "" && $4 != "" {print $3 "=" $4}' | paste -sd, -)
                [[ -n "$initial_cluster" ]] && break
            fi
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  add_node $id: could not parse ETCD_INITIAL_CLUSTER from member add output after $attempt attempts"; return 1; }

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
        [[ "$ok" -eq 1 ]] || { logerr "  add_node $id: new etcd member never became healthy"; return 1; }

        local endpoints
        endpoints=$(etcd_client_urls)
        docker run -d --name "etcfs-meta$id" --network docker_etcfuse-net \
            -v "docker_block_data:/block-device" -v "etcfuse-meta${id}-sock:/var/run" \
            -v "docker_history_data:/var/log/etcfuse" "$META_IMG" \
            --listen=/run/etcfuse/etcfuse.sock --etcd-endpoints="$endpoints" --node-id="n$id" \
            --cluster-name=docker-chaos --lease-ttl=$CHAOS_LEASE_TTL --block-device=/block-device/etcfuse.img \
            --log-level=1 --history-log=/var/log/etcfuse/history-n$id.jsonl >/dev/null

        sleep 2
        docker run -d --name "etcfs-fuse$id" --network docker_etcfuse-net --privileged --device /dev/fuse \
            -v "etcfuse-meta${id}-sock:/var/run" --entrypoint /bin/sh "$FUSE_IMG" \
            -c "mkdir -p /mnt/etcfuse && exec /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n$id --log-level=1 /mnt/etcfuse" >/dev/null

        for i in $(seq 1 20); do
            check_mount "etcfs-fuse$id" && { echo "etcfs-fuse$id"; return 0; }
            sleep 1
        done
        logerr "  add_node $id: FUSE never mounted"
        dump_logs "etcfs-fuse$id" 1>&2
        return 1
    }

    remove_node() {
        local id="$1"
        local ename="e$((id-1))"
        log "  remove_node $id: unmounting FUSE gracefully..."
        docker exec "etcfs-fuse$id" fusermount -u /mnt/etcfuse >/dev/null 2>&1
        sleep 1
        docker stop "etcfs-fuse$id" >/dev/null 2>&1
        docker stop "etcfs-meta$id" >/dev/null 2>&1

        local memberid
        memberid=$(docker exec etcfs-etcd1 etcdctl member list 2>/dev/null | grep ", $ename," | cut -d, -f1)
        if [[ -n "$memberid" ]]; then
            log "  remove_node $id: etcd member remove $memberid ($ename)..."
            docker exec etcfs-etcd1 etcdctl member remove "$memberid" >/dev/null 2>&1 || logerr "  member remove failed for $ename"
        else
            logerr "  remove_node $id: could not find etcd member id for $ename"
        fi
        docker stop "etcfs-etcd$id" >/dev/null 2>&1
        docker rm -f "etcfs-fuse$id" "etcfs-meta$id" "etcfs-etcd$id" >/dev/null 2>&1
    }
else
    # aws mode: single persistent EC2 cluster, ssh transport.
    STATE_FILE="infra-state-single-$$.json"
    wait_ssh() {
        local ip=$1; local max=$2
        for i in $(seq 1 $max); do
            timeout 5 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 ec2-user@$ip "echo ok" 2>/dev/null && return 0
            sleep 5
        done
        return 1
    }
    ssh_retry() {
        local ip=$1; shift
        wait_ssh "$ip" 12
        timeout 120 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 ec2-user@$ip "$@" 2>&1
    }
    runcmd()   { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    runcmd30() { timeout 30 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    runcmd60() { timeout 60 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    check_mount() { local M=$(runcmd "$1" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL"); [[ "$M" == "OK" ]]; }
    readf() {
        local out rc
        out=$(timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo cat /mnt/etcfuse/$2" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    readf($1,$2) failed rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        printf '%s' "$out"
    }
    writef() {
        local out rc
        out=$(printf '%s' "$2" | timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo tee /mnt/etcfuse/$3 > /dev/null" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    writef($1,$3) FAILED rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        return 0
    }
    rmf()  { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo rm -f /mnt/etcfuse/$2" 2>&1; }
    mvf()  { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo mv /mnt/etcfuse/$2 /mnt/etcfuse/$3" 2>&1; }
    mkdirf() { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo mkdir -p /mnt/etcfuse/$2" 2>&1; }
    lsf()  { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo ls /mnt/etcfuse/ 2>/dev/null"; }

    dump_logs() {
        local out
        out=$(timeout 20 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" \
            "echo '--- meta.log ---'; sudo tail -40 /tmp/meta.log 2>/dev/null; echo '--- fuse.log ---'; sudo tail -20 /tmp/fuse.log 2>/dev/null" 2>&1)
        log "  ---- daemon logs from $1 ----"
        while IFS= read -r line; do log "    $line"; done <<< "$out"
        log "  ---- end daemon logs ----"
    }
    etcd_endpoints() {
        echo "http://$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[2]' "$PROJECT_ROOT/$STATE_FILE"):2379"
    }
    # etcdctl_on — run etcdctl against the cluster, over SSH via N1.
    # Docker mode's equivalent execs inside a container; there is no local
    # etcd client here, so this shells out through N1 instead. Runs against
    # the full endpoint list (not just N1's own node) so a query still works
    # even if N1 itself is the node currently being restarted/partitioned.
    etcdctl_on() {
        timeout 15 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$N1" \
            "/usr/local/bin/etcdctl --endpoints=$(etcd_endpoints) $*" 2>/dev/null
    }
    # resolve_block_device <ip> — the shared volume's current path on this
    # node, matched by EBS serial (stable across attach cycles) rather than
    # assumed as /dev/nvme1n1 (not stable — Nitro's guest-side NVMe
    # enumeration is reassigned on each attach, so a node whose volume was
    # detached and reattached, or whose daemon simply restarts after some
    # other node's attach churned the numbering, can find the shared volume
    # at nvme2n1 or elsewhere). Falls back to the old fixed-path guesses
    # only if serial matching finds nothing, e.g. lsblk/python3 unavailable.
    #
    # This is the one existing implementation of this match
    # (scripts/infra/state.sh's detect_ebs_dev) inlined rather than sourced:
    # state.sh sets its own `set -euo pipefail` and redefines log()/SCRIPT_DIR,
    # sourcing it into chaos-lib.sh would fight this file's own set/log.
    resolve_block_device() {
        local ip="$1"
        local vol_id; vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        local serial="${vol_id//-/}"
        timeout 15 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$ip" "
            dev=\$(lsblk -J -o NAME,SERIAL 2>/dev/null | python3 -c \"
import sys, json
for d in json.load(sys.stdin).get('blockdevices', []):
    if d.get('serial','') == '$serial':
        print('/dev/' + d['name']); break
\" 2>/dev/null)
            if [[ -z \"\$dev\" ]]; then
                for cand in /dev/nvme1n1 /dev/sdf /dev/xvdf; do
                    [[ -b \"\$cand\" ]] && dev=\$cand && break
                done
            fi
            echo \"\$dev\"
        " 2>/dev/null
    }

    # ensure_volume_attached <public_ip> — re-attach the shared volume to this
    # node if fencing took it away.
    #
    # Control-plane fencing works by detaching the EBS volume from the fenced
    # node, so once a node has self-fenced its block device is simply gone and
    # the daemon cannot start: "open /dev/nvme1n1 with O_DIRECT: no such file or
    # directory". Restarting a fenced node therefore has to undo the fence
    # first, exactly as scripts/infra/bootstrap-cluster.sh does on re-bootstrap.
    ensure_volume_attached() {
        local pub="$1" vol_id inst attached
        vol_id=$(jq -r '.volume_id // empty' "$PROJECT_ROOT/$STATE_FILE")
        inst=$(jq -r --arg ip "$pub" '.compute_instance_ids[(.compute_public_ips | index($ip))] // empty' "$PROJECT_ROOT/$STATE_FILE" 2>/dev/null)
        [[ -n "$vol_id" && -n "$inst" ]] || return 0
        attached=$(aws ec2 describe-volumes --volume-ids "$vol_id" \
            --query "Volumes[0].Attachments[?InstanceId=='$inst'] | length(@)" --output text 2>/dev/null)
        [[ "$attached" == "0" ]] || return 0
        logerr "    $inst has no attachment to $vol_id (fenced) — re-attaching"
        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1 \
            || logerr "    re-attach failed for $inst"
        sleep 10
    }

    # restart_daemons <ip> <node_id> [ebs_volume_id] [ec2_instance_id]
    # The last two args are optional and opt this node's daemon into
    # dual-confirmed external fencing (see provision_cluster's daemon-start
    # block); omitted, restart preserves prior single-signal behavior.
    restart_daemons() {
        local etcd=$(etcd_endpoints); local tag=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")
        ensure_volume_attached "$1"
        local fence_flags=""
        [[ -n "${3:-}" && -n "${4:-}" ]] && fence_flags="--ebs-volume-id=$3 --ec2-instance-id=$4"
        # ETCFS_FENCE_MODE=nvme replaces control-plane fencing with
        # device-enforced NVMe reservations (see chaos-nvme-fencing.sh).
        [[ "${ETCFS_FENCE_MODE:-ebs}" == "nvme" ]] && fence_flags="--nvme-reservations"
        # nvme mode: let the daemon resolve --volume-id itself (see the
        # matching change in provision_cluster) instead of this harness
        # pre-resolving the path — this is the code path under test.
        local dev_flag
        if [[ "${ETCFS_FENCE_MODE:-ebs}" == "nvme" ]]; then
            local vol_id; vol_id=$(jq -r '.volume_id // empty' "$PROJECT_ROOT/$STATE_FILE")
            dev_flag="--volume-id=$vol_id"
        else
            local dev; dev=$(resolve_block_device "$1")
            [[ -n "$dev" ]] || dev=/dev/nvme1n1
            dev_flag="--block-device=$dev"
        fi
        # Not runcmd60: this needs more than 60s of budget (see below) and
        # timeout killing the remote command mid-script would also swallow
        # the diagnostic tail this prints on failure.
        timeout 100 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "
          sudo killall -9 etcfuse-meta etcfuse 2>/dev/null
          sleep 1
          # Plain umount, not -l: the server holding /dev/fuse is already
          # dead (killed above), so the mount is no longer busy and a normal
          # unmount clears it immediately. -l detaches from the namespace but
          # defers the actual teardown, which raced the fresh mount attempt
          # right after it and produced an FS session the kernel considered
          # aborted (ECONNABORTED on the first I/O through the new daemon).
          # Retried because the kernel can take a moment past the kill to let
          # go of the fd.
          for k in \$(seq 1 5); do sudo umount /mnt/etcfuse 2>/dev/null && break; sleep 1; done
          sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
          sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock --etcd-endpoints=$etcd --node-id=$2 --cluster-name=$tag --lease-ttl=$CHAOS_LEASE_TTL $dev_flag --log-level=2 $fence_flags > /tmp/meta.log 2>&1 &
          # A restart right after a partition heals can legitimately need
          # this long: the node's own local etcd
          # has to rejoin raft and the client has to reconnect and ride out a
          # leader election before it opens the socket — observed taking
          # ~15s in a real run (DeadlineExceeded/leader-changed retries in
          # meta.log). etcfuse has no retry of its own on a failed connect
          # (production relies on systemd's Restart=on-failure for that, see
          # setup-compute.sh's unit — this raw nohup harness has no
          # supervisor, so it must not start the client before the socket
          # exists), so starting it early was a guaranteed, permanent
          # failure rather than a race that sometimes wins.
          SOCK_OK=0
          for k in \$(seq 1 40); do sudo [ -S /run/etcfuse/etcfuse.sock ] && SOCK_OK=1 && break; sleep 1; done
          if [ \"\$SOCK_OK\" -ne 1 ]; then
            echo 'FAIL (socket never appeared)'; echo '--- meta.log tail ---'; sudo tail -20 /tmp/meta.log 2>/dev/null
            exit 1
          fi
          sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=$2 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
          for k in \$(seq 1 30); do sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0; sleep 1; done
          echo 'FAIL'; echo '--- meta.log tail ---'; sudo tail -15 /tmp/meta.log 2>/dev/null; echo '--- fuse.log tail ---'; sudo tail -15 /tmp/fuse.log 2>/dev/null
        " 2>/dev/null || echo "ERR:$?"
    }

    provision_cluster() {
        local TAG="chaos-single-$$"
        log "  Provisioning cluster=$TAG (state=$STATE_FILE)..."
        cd "$PROJECT_ROOT" || return 1
        rm -f "$STATE_FILE"
        ETCFS_STATE="$STATE_FILE" ETCFS_CLUSTER="$TAG" ETCFS_COMPUTE_NODES=3 \
          bash scripts/infra/create-infra.sh 2>&1 | tail -3

        N1=$(jq -r '.compute_public_ips[0]' "$STATE_FILE")
        N2=$(jq -r '.compute_public_ips[1]' "$STATE_FILE")
        N3=$(jq -r '.compute_public_ips[2]' "$STATE_FILE")
        P1=$(jq -r '.compute_ips[0]' "$STATE_FILE")
        P2=$(jq -r '.compute_ips[1]' "$STATE_FILE")
        P3=$(jq -r '.compute_ips[2]' "$STATE_FILE")
        SG=$(jq -r '.sg_id' "$STATE_FILE")
        log "  Nodes: $N1 $N2 $N3  SG: $SG"

        # The actual node bootstrap (packages, etcd, both EtcFS binaries, a
        # fresh etcd cluster, the daemons) is bootstrap-cluster.sh — the same
        # script scripts/infra/setup-compute.sh calls for a persistent
        # cluster. This IS the logic that used to be inlined here; it moved
        # so the two callers share one implementation instead of two that
        # drift apart.
        local ok=0
        ETCFS_LEASE_TTL="$CHAOS_LEASE_TTL" bash "$PROJECT_ROOT/scripts/infra/bootstrap-cluster.sh" "$STATE_FILE" || ok=1

        # aws has no separate meta "container" — alias M1..M3 to the same
        # host as N1..N3 so callers written for docker's meta/fuse split
        # (chaos-fuzz.sh's inject_fault) don't hit unbound variables.
        M1="$N1"; M2="$N2"; M3="$N3"
        return "$ok"
    }
    teardown_cluster() {
        log "  Teardown ($STATE_FILE)..."
        # nohup + disown: this runs async so the caller doesn't block on
        # instance termination, but a bare `&` dies with the parent shell —
        # if the script exits (or is killed) before termination finishes,
        # the volume and security group are orphaned and keep billing.
        ETCFS_STATE="$STATE_FILE" nohup bash "$PROJECT_ROOT/scripts/infra/destroy-infra.sh" --force \
            > "$PROJECT_ROOT/teardown-$STATE_FILE.log" 2>&1 &
        disown
        log "  Teardown launched (async, log: teardown-$STATE_FILE.log)"
    }

    # add_node <id> / remove_node <id> — join/remove a node (id 4, 5, ...)
    # against the running cluster. Shared by chaos-elastic.sh (sequential
    # add) and chaos-elastic-concurrent.sh (parallel add) so the join/leave
    # logic only exists once.
    # aws mode: launch/terminate real EC2 instances joining the same etcd
    # cluster + shared Multi-Attach EBS volume as the base 3-node cluster.
    #
    # add_node/remove_node are invoked as `X=$(add_node N)` so the caller
    # can capture the winning node's IP — command substitution runs the
    # function in a subshell, so any in-memory var/array it sets (pub, priv,
    # instance id) is gone the instant it returns. Persist that per-node
    # state to files instead, so it survives across the subshell boundary.
    node_info_file() { echo "$REPORT_DIR/node$1.info"; }
    save_node_info() { echo "$2 $3 $4" > "$(node_info_file "$1")"; }
    node_pub()  { cut -d' ' -f1 "$(node_info_file "$1")" 2>/dev/null; }
    node_priv() { cut -d' ' -f2 "$(node_info_file "$1")" 2>/dev/null; }
    node_inst() { cut -d' ' -f3 "$(node_info_file "$1")" 2>/dev/null; }

    add_node() {
        local id="$1"
        local ename="e$((id-1))"
        # SG/AMI_ID/SUBNET_ID/KEY_NAME/VOL_ID/TAG are locals scoped inside
        # chaos-lib.sh's aws provision_cluster(); read fresh from the state
        # file it wrote instead of depending on that scope, which is gone
        # the instant provision_cluster returns — a bare $TAG here is an
        # unbound-variable error under `set -u` for any caller that adds a
        # node after provisioning finished, which is every caller.
        local ami_id subnet_id key_name sg vol_id TAG
        ami_id=$(jq -r '.ami_id' "$PROJECT_ROOT/$STATE_FILE")
        subnet_id=$(jq -r '.subnet_id' "$PROJECT_ROOT/$STATE_FILE")
        key_name=$(jq -r '.key_name' "$PROJECT_ROOT/$STATE_FILE")
        sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")
        local name="${TAG}-compute-${id}"
        logerr "  add_node $id: launching EC2 instance..."
        local inst
        local iam_profile; iam_profile=$("$PROJECT_ROOT/scripts/infra/fencing-iam.sh" profile-name)
        inst=$(aws ec2 run-instances \
            --image-id "$ami_id" --instance-type "${ETCFS_INSTANCE_TYPE:-m7i.large}" --key-name "$key_name" \
            --security-group-ids "$sg" --subnet-id "$subnet_id" --associate-public-ip-address \
            --iam-instance-profile "Name=$iam_profile" \
            --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=ClusterName,Value=$TAG}]" \
            --query 'Instances[0].InstanceId' --output text 2>/dev/null)
        [[ -n "$inst" && "$inst" != "None" ]] || { logerr "  add_node $id: run-instances failed"; return 1; }
        aws ec2 wait instance-running --instance-ids "$inst" 2>/dev/null
        local pub priv
        pub=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
        priv=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
        save_node_info "$id" "$pub" "$priv" "$inst"
        logerr "  add_node $id: $inst -> $pub / $priv"

        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1
        sleep 8

        # add_node's own stdout is captured via $(add_node N) by the caller
        # to get the winning node's public IP, so none of this setup noise
        # (raw ssh/scp output) may reach stdout — redirect it all to the log.
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
            scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q /tmp/chaos.tar.gz ec2-user@"$pub":/tmp/ 2>/dev/null || true
            ssh_retry "$pub" "
                rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/chaos.tar.gz && \
                gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
                    cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c \
                    -o /tmp/etcfuse -lfuse3 -lpthread 2>&1 && \
                sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
            " || true
            ssh_retry "$pub" "sudo mkdir -p /mnt/etcfuse" || true
        } >> "$REPORT_DIR/chaos.log" 2>&1

        # See the docker branch's add_node for why this retries the add
        # itself instead of trusting a health pre-check.
        logerr "  add_node $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member add $ename --peer-urls=http://$priv:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  add_node $id: could not parse ETCD_INITIAL_CLUSTER after $attempt attempts"; return 1; }

        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@"$pub" "
            sudo mkdir -p /var/lib/etcd
            sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                --initial-cluster '$initial_cluster' --initial-cluster-state existing > /tmp/etcd.log 2>&1 &
        " 2>/dev/null
        sleep 8

        local endpoints="http://$P1:2379,http://$P2:2379,http://$P3:2379"
        local j jpriv
        for j in 4 5; do
            [[ "$j" -eq "$id" ]] && continue
            jpriv=$(node_priv "$j")
            [[ -n "$jpriv" ]] && endpoints="$endpoints,http://$jpriv:2379"
        done

        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@"$pub" "
            sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
                --etcd-endpoints=$endpoints --node-id=n$id --cluster-name=$TAG \
                --lease-ttl=$CHAOS_LEASE_TTL --block-device=/dev/nvme1n1 --log-level=2 \
                --ebs-volume-id=$vol_id --ec2-instance-id=$inst > /tmp/meta.log 2>&1 &
            sleep 4
            sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
                --node-id=n$id --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
            sleep 7
        " 2>/dev/null

        for i in $(seq 1 20); do
            check_mount "$pub" && { echo "$pub"; return 0; }
            sleep 1
        done
        logerr "  add_node $id: FUSE never mounted on $pub"
        dump_logs "$pub" 1>&2
        return 1
    }

    remove_node() {
        local id="$1"
        local ename="e$((id-1))"
        local pub inst
        pub=$(node_pub "$id")
        inst=$(node_inst "$id")
        log "  remove_node $id ($pub): unmounting FUSE gracefully..."
        runcmd "$pub" "sudo fusermount -u /mnt/etcfuse 2>/dev/null; sleep 1; sudo pkill -TERM etcfuse-meta 2>/dev/null; sleep 2; true"

        local memberid
        memberid=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member list" 2>/dev/null | grep ", $ename," | cut -d, -f1)
        if [[ -n "$memberid" ]]; then
            log "  remove_node $id: etcd member remove $memberid ($ename)..."
            runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member remove $memberid" >/dev/null 2>&1 || logerr "  member remove failed for $ename"
        else
            logerr "  remove_node $id: could not find etcd member id for $ename"
        fi

        local vol_id
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        aws ec2 detach-volume --volume-id "$vol_id" --instance-id "$inst" >/dev/null 2>&1
        sleep 5
        aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
        rm -f "$(node_info_file "$id")"
    }

    # ---- Node-indexed operations (1|2|3), identical in meaning across modes.
    # See the docker branch's copy for why S8-S13 are written against these
    # rather than against a container name or an IP.
    #
    # On AWS both daemons live on one instance, so there is no meta "host"
    # distinct from the node: meta_of and node_ip are the same address, and
    # what separates the two daemons is which of them the fault is aimed at.
    node_ip()   { eval "echo \$N$1"; }
    meta_of()   { eval "echo \$N$1"; }
    restart_node() { restart_daemons "$(node_ip "$1")" "n$1"; }

    # isolate_etcd <idx> — cut this node's metadata daemon off etcd while the
    # FUSE daemon keeps talking to it over their shared unix socket.
    #
    # Docker detaches the meta container from the network; here both daemons
    # share a host, so the separation has to be made by port instead: DROP the
    # etcd client and peer ports to every peer, which takes the metadata
    # daemon's session and this node's own etcd member (which loses quorum)
    # while leaving the socket, the mount and the block device untouched. This
    # is what scripts/test/isolate-node.sh does, inlined for the same reason
    # resolve_block_device is.
    # Returns non-zero if the node can still reach a peer's etcd afterwards.
    # A fault injection that quietly does nothing is worse than one that fails:
    # the scenario goes on to assert partitioned behaviour against a healthy
    # cluster and reports the product broken. That is exactly what happened
    # when iptables turned out not to be installed.
    isolate_etcd() {
        local idx="$1"
        local ip self rules="" p peer=""
        ip=$(node_ip "$idx")
        self=$(jq -r ".compute_ips[$((idx - 1))]" "$PROJECT_ROOT/$STATE_FILE")

        # Amazon Linux 2023 ships without iptables, so the tool this depends on
        # has to be put there first. It is done here rather than at provision
        # time so that the one helper that needs it is the one that guarantees
        # it — a provisioning step whose failure was swallowed by `|| true` is
        # how the rules came to be applied by a binary that did not exist.
        # runcmd* swallow the exit status (they append `|| echo ERR:$?`), so
        # this asks for the answer in the output rather than in $?.
        local have
        have=$(runcmd30 "$ip" "sudo test -x /usr/sbin/iptables && echo yes || echo no")
        if [[ "$have" != *yes* ]]; then
            have=$(runcmd60 "$ip" "sudo dnf install -y iptables-nft >/dev/null 2>&1; sudo test -x /usr/sbin/iptables && echo yes || echo no")
        fi
        if [[ "$have" != *yes* ]]; then
            logerr "    isolate_etcd($idx): iptables is not available and could not be installed"
            return 1
        fi
        while read -r p; do
            [[ -z "$p" || "$p" == "$self" ]] && continue
            peer="$p"
            rules+=" sudo iptables -A OUTPUT -d $p -p tcp --dport 2379 -j DROP;"
            rules+=" sudo iptables -A OUTPUT -d $p -p tcp --dport 2380 -j DROP;"
            rules+=" sudo iptables -A INPUT -s $p -p tcp -j DROP;"
        done < <(jq -r '.compute_ips[]' "$PROJECT_ROOT/$STATE_FILE")
        runcmd30 "$ip" "$rules true" >/dev/null

        local probe
        probe=$(runcmd30 "$ip" "timeout 3 bash -c '</dev/tcp/$peer/2379' 2>/dev/null && echo REACHABLE || echo BLOCKED")
        if [[ "$probe" != *BLOCKED* ]]; then
            logerr "    isolate_etcd($idx): etcd on $peer is still reachable — fault not injected"
            return 1
        fi
        return 0
    }
    rejoin_etcd() {
        local ip; ip=$(node_ip "$1")
        # -F rather than deleting the rules one by one: the harness is the only
        # thing adding any, and a partially-restored node is a worse state to
        # leave a later scenario in than a fully flushed one.
        runcmd30 "$ip" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null
    }

    # meta_log <idx> — the metadata daemon's log (restart_daemons writes it to
    # /tmp/meta.log).
    meta_log() { runcmd30 "$(node_ip "$1")" "sudo cat /tmp/meta.log 2>/dev/null"; }
fi

# wait_writable <idx> [timeout_s] — block until the node can actually serve a
# write, not merely until its mount exists.
#
# restart_node returns as soon as /mnt/etcfuse is mounted, which is earlier than
# the point at which a write commits: after a partition heals the node's own etcd
# member still has to rejoin raft, and the first requests through it come back
# "etcdserver: leader changed" until an election settles. Proceeding at mount
# time made the *next* scenario's seed write fail with EIO and report the
# filesystem broken for a fault the previous scenario had injected.
wait_writable() {
    local idx="$1" limit="${2:-60}" ip k
    ip=$(node_ip "$idx")
    for ((k = 0; k < limit; k += 3)); do
        if check_mount "$ip" && writef "$ip" "ready" ".ready-n$idx.txt" 2>/dev/null; then
            return 0
        fi
        sleep 3
    done
    logerr "    wait_writable($idx): node still could not serve a write after ${limit}s"
    return 1
}
