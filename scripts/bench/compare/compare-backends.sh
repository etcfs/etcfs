#!/bin/bash
# compare-backends.sh — "bring backend $COMPARE_BACKEND up on the cluster
# compare_provision just created, and hand back a path".
#
# The IOPS suite has one script per backend, each of which sets its backend up
# and immediately benchmarks it. The scenario suite inverts that: one script
# per *scenario*, run against whichever backend COMPARE_BACKEND names, so the
# setup half has to be callable on its own. It lives here rather than in
# compare-lib.sh only for size — compare-lib.sh sources this at the end, so
# every caller already has it.
#
# compare_mount sets, for the caller:
#   MOUNT_PATH   path of the filesystem under test, identical on every node
#                that has it
#   BENCH_NODES  public IPs that have it mounted, in cluster order
#   N0           BENCH_NODES[0] (bench-lib.sh's run_fio drives this one)
#
# Every backend's own quirks — which AMI, which ports, why gluster gets local
# volumes and juicefs does not — are documented in the matching bench-*.sh
# header, which is where the reasoning was worked out.

# compare_mount [mount_all]
# mount_all=1 (default) mounts on every node; the IOPS runs only ever needed
# one client and some backends charge real setup time per client.
compare_mount() {
    local mount_all="${1:-1}"
    case "$COMPARE_BACKEND_BASE" in
        etcfs)   compare_mount_etcfs ;;
        nfs)     compare_mount_nfs ;;
        gfs2)    compare_mount_gfs2 ;;
        gluster) compare_mount_gluster "$mount_all" ;;
        juicefs) compare_mount_juicefs "$mount_all" ;;
        *) die "compare_mount: unknown backend $COMPARE_BACKEND" ;;
    esac
    N0="${BENCH_NODES[0]}"
    compare_install_fio "${BENCH_NODES[@]}"
    log "[$COMPARE_BACKEND] mounted at $MOUNT_PATH on ${#BENCH_NODES[@]} node(s)"
}

compare_mount_etcfs() {
    bash "$INFRA_DIR/bootstrap-cluster.sh" "$ETCFS_STATE" || die "bootstrap-cluster.sh failed"
    local ip
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        wait_for_fuse_mount "$ip" 60 2 || die "etcfs never mounted on $ip"
    done
    MOUNT_PATH="$FUSE_MOUNTPOINT"
    BENCH_NODES=("${COMPARE_PUB_IPS[@]}")

    # Whether the kernel is allowed to cache this mount's data pages, reported
    # rather than assumed. The daemon only answers an open as cacheable while a
    # cache-invalidation client is connected to take those pages back again, so
    # a mount without one leaves --page-cache inert and every read measurement
    # device-bound, which is indistinguishable from the coordination layer
    # simply being slow. The C side retries the connection and both daemons now
    # log the outage, but a run whose numbers are only interpretable when the
    # cache was available should still check rather than trust it.
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        case "$(compare_etcfs_page_cache "$ip")" in
            up) log "  $ip: cache-invalidation client connected (page cache available)" ;;
            down)
                log "  $ip: WARNING the cache-invalidation client connected and was then lost —"
                log "      the kernel is no longer allowed to cache this mount's pages"
                ;;
            *)
                log "  $ip: WARNING no cache-invalidation client has ever connected — the kernel"
                log "      will not cache this mount's pages, so every read is device-bound"
                ;;
        esac
    done
}

compare_mount_nfs() {
    compare_export_backing "${COMPARE_PUB_IPS[0]}" "${COMPARE_PRIV_IPS[0]}" "${COMPARE_PUB_IPS[@]:1}"
    MOUNT_PATH="$BACKING_PATH"
    COMPARE_REMOTE_MOUNT_CMD="sudo mount -t nfs4 ${COMPARE_PRIV_IPS[0]}:$BACKING_PATH $BACKING_PATH"
    BENCH_NODES=("${COMPARE_PUB_IPS[@]:1}" "${COMPARE_PUB_IPS[0]}")
}

# compare_mount_gfs2 brings up the bare corosync + dlm stack every GFS2 run
# has used: no cluster resource manager, and so no fencing. That is honest for
# throughput scenarios and wrong for the node-kill one, where a survivor's
# lockspace waits for a fence that never comes and there is no recovery time to
# quote. COMPARE_BACKEND=gfs2-fenced selects the pacemaker-managed variant
# below instead, which is the like-for-like comparison against EtcFS's own
# recovery.
compare_mount_gfs2() {
    if [[ "$COMPARE_BACKEND" == "gfs2-fenced" ]]; then
        compare_mount_gfs2_fenced
        return
    fi
    local ip i dev
    compare_open_port_udp 5404 5405  # corosync totem
    compare_open_port 21064          # dlm_controld

    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "set -e; sudo yum install -y corosync dlm gfs2-utils"
    done

    local conf="
totem {
    version: 2
    cluster_name: comparegfs2
    transport: udpu
    crypto_hash: none
    crypto_cipher: none
}
nodelist {
$(for i in "${!COMPARE_PRIV_IPS[@]}"; do printf '    node {\n        ring0_addr: %s\n        nodeid: %d\n    }\n' "${COMPARE_PRIV_IPS[$i]}" "$((i+1))"; done)
}
quorum {
    provider: corosync_votequorum
}
logging {
    to_syslog: yes
}
"
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/corosync"
        echo "$conf" | $SSH_CMD "ec2-user@$ip" "sudo tee /etc/corosync/corosync.conf >/dev/null"
        $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now corosync"
    done

    log "Waiting for corosync quorum..."
    for _ in $(seq 1 30); do
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" \
            "sudo corosync-quorumtool -s 2>/dev/null | grep -q 'Quorate:.*Yes'" && break
        sleep 2
    done
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now dlm"
    done
    sleep 5

    dev=$(compare_shared_device "${COMPARE_PUB_IPS[0]}")
    # One journal per node by default. COMPARE_GFS2_JOURNALS formats fewer, so
    # the nodes past that count stay unmounted for bench-gfs2-journal-add.sh.
    local journals="${COMPARE_GFS2_JOURNALS:-${#COMPARE_PUB_IPS[@]}}"
    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" \
        "sudo mkfs.gfs2 -O -p lock_dlm -t comparegfs2:vol1 -j $journals $dev"
    for ip in "${COMPARE_PUB_IPS[@]:0:$journals}"; do
        $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /mnt/compare-gfs2 && sudo mount -t gfs2 -o noatime $dev /mnt/compare-gfs2"
    done
    MOUNT_PATH=/mnt/compare-gfs2
    COMPARE_REMOTE_MOUNT_CMD="sudo mount -t gfs2 -o noatime $dev /mnt/compare-gfs2"
    BENCH_NODES=("${COMPARE_PUB_IPS[@]}")
}

# compare_mount_gfs2_fenced — GFS2 under pacemaker with real STONITH, so that a
# node-kill run measures recovery rather than a lockspace that stopped for good.
#
# The difference from the bare stack is the whole point of the variant. There,
# corosync and dlm are started by hand and nothing is configured to fence, so
# when a node dies DLM moves its lockspace to "wait fencing" and stays there:
# survivors keep the locks they already hold and can never take the dead node's.
# Here pacemaker owns DLM and the mount as cloned resources, and a fence_aws
# STONITH device stops the dead instance through the EC2 API. DLM releases the
# lockspace only once pacemaker reports that stop confirmed, which is the
# sequence GFS2 was designed around and the one EtcFS's 2.19 s should be read
# against.
#
# Needs the STONITH policy from scripts/infra/fencing-iam.sh on the instance
# role (ec2:StopInstances on this harness's own instances) and fence_aws, which
# is a boto3 script — without either, the device fails to probe and pacemaker
# refuses to start the filesystem clone rather than running unfenced.
compare_mount_gfs2_fenced() {
    local ip i dev cluster=comparegfs2 pw=etcfsbench
    # Checked before a cluster is provisioned rather than after: without the
    # STONITH policy the fence device cannot start, the filesystem clone never
    # comes up, and the run costs a full provision to tell you so.
    aws iam get-role-policy --role-name etcfs-nodes \
        --policy-name gfs2-stonith-permissions >/dev/null 2>&1 \
        || die "gfs2-fenced needs the STONITH policy on the etcfs-nodes role: run scripts/infra/fencing-iam.sh create"

    compare_open_port_udp 5404 5405  # corosync totem
    compare_open_port 21064          # dlm_controld
    compare_open_port 2224           # pcsd
    compare_open_port 3121           # pacemaker crmd

    # Cluster node names, resolvable everywhere. Pacemaker identifies a node by
    # the name corosync was given, and the fence map has to use the same
    # strings, so the names are assigned here rather than inherited from
    # whatever hostname EC2 handed out.
    local names=() hosts=""
    for i in "${!COMPARE_PRIV_IPS[@]}"; do
        names+=("cmp-n$((i+1))")
        hosts+="${COMPARE_PRIV_IPS[$i]} cmp-n$((i+1))"$'\n'
    done

    for i in "${!COMPARE_PUB_IPS[@]}"; do
        ip="${COMPARE_PUB_IPS[$i]}"
        $SSH_CMD "ec2-user@$ip" "set -e
            sudo yum install -y pcs pacemaker dlm gfs2-utils >/dev/null
            sudo hostnamectl set-hostname ${names[$i]}
            echo '$hosts' | sudo tee -a /etc/hosts >/dev/null
            echo 'hacluster:$pw' | sudo chpasswd
            sudo systemctl enable --now pcsd
            sudo systemctl enable corosync pacemaker >/dev/null 2>&1 || true"
    done

    # fence_aws on AL2 ships with a python2 shebang and imports boto3, which
    # python2 no longer has a supported build of — the agent then answers a
    # metadata query with an ImportError, and pacemaker reports it as an agent
    # that "does not provide valid metadata", which points at the wrong thing
    # entirely. Python 3.8 comes from amazon-linux-extras, boto3 is installed
    # for that interpreter specifically, and the agent is pointed at it.
    #
    # The second patch fixes the agent itself: 4.2's fence_aws builds its EC2
    # resource with no region, so every call fails on an instance whose region
    # is not in the environment pacemaker runs it under.
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "set -e
            sudo amazon-linux-extras install python3.8 -y >/dev/null
            # python38-devel, libcurl-devel and openssl-devel are pycurl's
            # build dependencies: the agent's own fencing.py imports pycurl
            # before it imports anything AWS, so a missing one fails the agent
            # in a way that says nothing about AWS at all.
            sudo yum install -y fence-agents-aws python38-devel libcurl-devel \
                openssl-devel gcc >/dev/null
            sudo /usr/bin/python3.8 -m ensurepip --upgrade >/dev/null
            sudo /usr/bin/python3.8 -m pip install --quiet 'urllib3<2.0' \
                boto3 botocore requests pexpect pycurl certifi --upgrade
            sudo sed -i 's|^#!/usr/bin/python.*|#!/usr/bin/python3.8|' /usr/sbin/fence_aws
            sudo sed -i \"s/conn = boto3.resource('ec2')/conn = boto3.resource('ec2', region_name=options.get('--region'))/\" /usr/sbin/fence_aws"
    done

    # The agent is run, not merely looked for: an installed fence_aws whose
    # boto3 import fails answers a metadata query with an error, and pacemaker
    # reports that as "not installed or does not provide valid metadata", which
    # sends the reader looking for a package that is already there.
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        local probe
        probe=$($SSH_CMD "ec2-user@$ip" "sudo /usr/sbin/fence_aws -o metadata 2>&1 | head -20" 2>&1)
        case "$probe" in
            *"<resource-agent"*) ;;
            *) die "gfs2-fenced: fence_aws cannot run on $ip, so this run would measure an unfenced cluster again. The agent said:
$probe" ;;
        esac
    done

    local n0="${COMPARE_PUB_IPS[0]}"
    # pcs 0.9 (AL2) and 0.10 spell authentication and setup differently, and
    # the harness spans both. The older form is tried first here because AL2 is
    # what this backend is pinned to.
    $SSH_CMD "ec2-user@$n0" "
        sudo pcs cluster auth ${names[*]} -u hacluster -p $pw --force 2>/dev/null ||
        sudo pcs host auth ${names[*]} -u hacluster -p $pw" \
        || die "gfs2-fenced: pcs authentication failed across the cluster"
    $SSH_CMD "ec2-user@$n0" "
        sudo pcs cluster setup --force --name $cluster ${names[*]} 2>/dev/null ||
        sudo pcs cluster setup --force $cluster ${names[*]}" \
        || die "gfs2-fenced: pcs cluster setup failed"
    $SSH_CMD "ec2-user@$n0" "sudo pcs cluster start --all" || die "gfs2-fenced: cluster did not start"

    log "Waiting for pacemaker quorum..."
    for _ in $(seq 1 40); do
        $SSH_CMD "ec2-user@$n0" "sudo pcs status 2>/dev/null | grep -q 'partition with quorum'" && break
        sleep 3
    done

    # startup-fencing and a long stonith-timeout matter here: stopping an EC2
    # instance through the API is slow enough that pacemaker's default gives up
    # while the fence is still succeeding, and a fence that is reported failed
    # leaves the lockspace stopped exactly as an unfenced cluster does.
    $SSH_CMD "ec2-user@$n0" "
        sudo pcs property set stonith-enabled=true
        sudo pcs property set stonith-action=off
        sudo pcs property set startup-fencing=true
        sudo pcs property set no-quorum-policy=stop
        sudo pcs property set stonith-timeout=600s"

    local map="" inst
    for i in "${!names[@]}"; do
        inst=$(state_get compute_instance_ids | jq -r ".[$i]")
        map+="${names[$i]}:$inst;"
    done
    $SSH_CMD "ec2-user@$n0" "sudo pcs stonith create clusterfence fence_aws \
        region=$AWS_DEFAULT_REGION pcmk_host_map='$map' \
        power_timeout=600 pcmk_off_timeout=600 pcmk_reboot_timeout=480 \
        pcmk_reboot_retries=4 power_wait=5" \
        || die "gfs2-fenced: STONITH device not created"

    log "Waiting for the STONITH device to start..."
    for _ in $(seq 1 30); do
        $SSH_CMD "ec2-user@$n0" "sudo pcs status 2>/dev/null | grep -q 'clusterfence.*Started'" && break
        sleep 4
    done
    if ! $SSH_CMD "ec2-user@$n0" "sudo pcs status 2>/dev/null | grep -q 'clusterfence.*Started'"; then
        $SSH_CMD "ec2-user@$n0" "sudo pcs status 2>&1 | tail -30; sudo journalctl -u pacemaker --no-pager 2>&1 | grep -i fence | tail -20" \
            > "$RESULTS_DIR/stonith-start-failure.txt" 2>&1 || true
        die "gfs2-fenced: the fence device never started — see $RESULTS_DIR/stonith-start-failure.txt"
    fi

    dev=$(compare_shared_device "$n0")
    # GFS2 straight on the shared device, not on a clustered LVM volume: that
    # is what every other GFS2 run in this suite measures, and the fenced
    # variant exists to change one thing — whether a dead node is fenced — not
    # the storage stack underneath it.
    $SSH_CMD "ec2-user@$n0" \
        "sudo mkfs.gfs2 -O -p lock_dlm -t $cluster:vol1 -j ${#COMPARE_PUB_IPS[@]} $dev" \
        || die "gfs2-fenced: mkfs.gfs2 failed"

    $SSH_CMD "ec2-user@$n0" "
        sudo pcs resource create dlm ocf:pacemaker:controld op monitor interval=30s on-fail=fence clone interleave=true ordered=true
        sudo pcs resource create clusterfs Filesystem device=$dev directory=/mnt/compare-gfs2 fstype=gfs2 options=noatime op monitor interval=10s on-fail=fence clone interleave=true
        sudo pcs constraint order start dlm-clone then clusterfs-clone
        sudo pcs constraint colocation add clusterfs-clone with dlm-clone" \
        || die "gfs2-fenced: cluster resources not created"

    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /mnt/compare-gfs2"
    done

    log "Waiting for the filesystem clone on every node..."
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        for _ in $(seq 1 45); do
            $SSH_CMD "ec2-user@$ip" "mountpoint -q /mnt/compare-gfs2" && break
            sleep 4
        done
        if ! $SSH_CMD "ec2-user@$ip" "mountpoint -q /mnt/compare-gfs2"; then
            $SSH_CMD "ec2-user@$n0" "sudo pcs status 2>&1 | tail -40" \
                > "$RESULTS_DIR/clone-start-failure.txt" 2>&1 || true
            die "gfs2-fenced: $ip never mounted the clustered filesystem — see $RESULTS_DIR/clone-start-failure.txt"
        fi
    done

    MOUNT_PATH=/mnt/compare-gfs2
    # Pacemaker owns this mount: a bare mount command here would fight it.
    COMPARE_REMOTE_MOUNT_CMD=""
    BENCH_NODES=("${COMPARE_PUB_IPS[@]}")
}

compare_mount_gluster() {
    local mount_all="$1" i ip entries=() bricks=""
    compare_open_port 24007
    compare_open_port 49152 49251

    for i in "${!COMPARE_PUB_IPS[@]}"; do
        entries+=("${COMPARE_PUB_IPS[$i]}:${COMPARE_PRIV_IPS[$i]}:$(state_get compute_instance_ids | jq -r ".[$i]")")
    done
    local devs=()
    mapfile -t devs < <(compare_create_local_volumes "$ETCFS_VOLUME_IOPS" "${entries[@]}")

    for i in "${!COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[$i]}" "set -e
            sudo amazon-linux-extras install -y epel
            printf '%s\n' '[gluster9]' 'name=Gluster 9' \
                'baseurl=https://vault.centos.org/centos/7/storage/\$basearch/gluster-9/' \
                'gpgcheck=0' 'enabled=1' | sudo tee /etc/yum.repos.d/gluster9.repo >/dev/null
            sudo yum install -y --disableplugin=priorities userspace-rcu
            sudo yum install -y xfsprogs ncurses-compat-libs
            sudo yum install -y --disablerepo=amzn2-core --disableplugin=priorities glusterfs-server
            sudo mkfs.xfs -f ${devs[$i]}
            sudo mkdir -p /mnt/gluster-brick
            sudo mount ${devs[$i]} /mnt/gluster-brick
            sudo mkdir -p /mnt/gluster-brick/brick
            sudo systemctl enable --now glusterd
        "
        bricks+=" ${COMPARE_PRIV_IPS[$i]}:/mnt/gluster-brick/brick"
    done

    for i in "${!COMPARE_PRIV_IPS[@]}"; do
        [[ "$i" -eq 0 ]] && continue
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "sudo gluster peer probe ${COMPARE_PRIV_IPS[$i]}"
    done
    sleep 5
    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "set -e
        sudo gluster volume create compare-vol replica ${#COMPARE_PRIV_IPS[@]} $bricks force
        sudo gluster volume start compare-vol
    "
    sleep 5

    BENCH_NODES=()
    for ip in "${COMPARE_PUB_IPS[@]:1}"; do
        $SSH_CMD "ec2-user@$ip" "set -e
            sudo yum install -y glusterfs-fuse >/dev/null 2>&1
            sudo mkdir -p /mnt/compare-gluster
            sudo mount -t glusterfs ${COMPARE_PRIV_IPS[0]}:/compare-vol /mnt/compare-gluster
        "
        BENCH_NODES+=("$ip")
        [[ "$mount_all" == "1" ]] || break
    done
    MOUNT_PATH=/mnt/compare-gluster
    COMPARE_REMOTE_MOUNT_CMD="sudo mount -t glusterfs ${COMPARE_PRIV_IPS[0]}:/compare-vol /mnt/compare-gluster"
}

compare_mount_juicefs() {
    local mount_all="$1" ip dev
    compare_open_port 6379  # redis (metadata)
    compare_open_port 9000  # minio (object storage)
    dev=$(compare_shared_device "${COMPARE_PUB_IPS[0]}")

    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "set -e
        sudo mkfs.ext4 -q -F $dev
        sudo mkdir -p /mnt/juicefs-minio-data
        sudo mount $dev /mnt/juicefs-minio-data

        sudo dnf install -y redis6 fio || sudo dnf install -y redis fio || sudo yum install -y redis fio
        REDIS_BIN=\$(command -v redis-server || command -v redis6-server)
        sudo nohup \"\$REDIS_BIN\" --bind 0.0.0.0 --protected-mode no > /tmp/redis.log 2>&1 &
        sleep 2

        curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /tmp/minio
        sudo install -m 755 /tmp/minio /usr/local/bin/minio
        sudo useradd -r -m -d /home/minio-user minio-user 2>/dev/null || true
        sudo chown -R minio-user:minio-user /mnt/juicefs-minio-data
        sudo -u minio-user MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
            nohup /usr/local/bin/minio server /mnt/juicefs-minio-data --address :9000 \
            > /tmp/minio.log 2>&1 &
        sleep 3

        curl -fsSL https://d.juicefs.com/install | sudo sh -
        sudo juicefs format --storage minio \
            --bucket http://${COMPARE_PRIV_IPS[0]}:9000/juicefs-bucket \
            --access-key minioadmin --secret-key minioadmin \
            redis://127.0.0.1:6379/1 compare-vol
    "

    BENCH_NODES=()
    for ip in "${COMPARE_PUB_IPS[@]:1}"; do
        $SSH_CMD "ec2-user@$ip" "set -e
            curl -fsSL https://d.juicefs.com/install | sudo sh -
            sudo mkdir -p /mnt/compare-juicefs
            sudo juicefs mount -d redis://${COMPARE_PRIV_IPS[0]}:6379/1 /mnt/compare-juicefs
        "
        BENCH_NODES+=("$ip")
        [[ "$mount_all" == "1" ]] || break
    done
    sleep 3
    MOUNT_PATH=/mnt/compare-juicefs
    COMPARE_REMOTE_MOUNT_CMD="sudo juicefs mount -d redis://${COMPARE_PRIV_IPS[0]}:6379/1 /mnt/compare-juicefs"
}

# ---- membership churn: one node leaves the filesystem and comes back ----
#
# The elasticity scenario needs "take this node out of the filesystem, then put
# it back" for every backend, which is a different event in each:
#
#   etcfs    both daemons stop and restart — a node claims its own arena and
#            announces its departure, so no peer is expected to pause
#   gfs2     the mount joins and leaves the DLM lockspace, and every surviving
#            node's lockspace is suspended while the membership changes; the
#            leave also makes the survivors recover the leaver's journal
#   gluster  a client mount attaches to and detaches from the volume
#   nfs      a client mount, which the server absorbs on its own
#   juicefs  a client mount against the shared redis + object store
#
# Only the first two have any reason to disturb the nodes already running,
# which is precisely the number the scenario exists to take: the same event,
# timed on the *other* nodes, across five backends.
#
# compare_elastic_joiner — the node whose churn is worth timing. Never the
# server for a server-mediated backend: taking that out is bench-node-kill.sh's
# scenario, and it stops the filesystem for everyone rather than testing
# membership at all.
compare_elastic_joiner() {
    case "$COMPARE_BACKEND_BASE" in
        nfs) echo "${BENCH_NODES[1]}" ;;   # BENCH_NODES ends with the server
        *)   echo "${BENCH_NODES[-1]}" ;;
    esac
}

# compare_leave_fs <pub_ip> — an orderly departure, not a kill. Prints nothing.
compare_leave_fs() {
    local ip="$1"
    case "$COMPARE_BACKEND_BASE" in
        etcfs)
            # SIGTERM plus an explicit unmount, the same clean-leave path
            # bench-join-latency.sh uses: a killed daemon is a crash to the
            # fencing controller, which detaches the node's EBS attachment for
            # real and turns the rejoin below into a fencing-recovery test.
            compare_etcfs_snapshot_cmdline "$ip"
            $SSH_CMD "ec2-user@$ip" \
                "sudo killall etcfuse-meta etcfuse 2>/dev/null; sudo umount -l $MOUNT_PATH 2>/dev/null; true"
            ;;
        juicefs)
            $SSH_CMD "ec2-user@$ip" "sudo umount $MOUNT_PATH 2>/dev/null || sudo umount -l $MOUNT_PATH 2>/dev/null; true"
            ;;
        *)
            $SSH_CMD "ec2-user@$ip" "sudo umount $MOUNT_PATH 2>/dev/null || sudo umount -l $MOUNT_PATH 2>/dev/null; true"
            ;;
    esac
}

# compare_join_fs <pub_ip> — put that node back into the filesystem, printing
# the seconds it took to reach a mount that answers a write. Timed on the node
# for the same reason compare_remote_time is: an ssh round trip is the same
# order as the number being taken.
compare_join_fs() {
    local ip="$1"
    if [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]]; then
        compare_etcfs_start "$ip"
        return
    fi
    [[ -n "${COMPARE_REMOTE_MOUNT_CMD:-}" ]] || die "compare_join_fs: no mount command recorded for $COMPARE_BACKEND"
    $SSH_CMD "ec2-user@$ip" "
        s=\$(date +%s.%N)
        $COMPARE_REMOTE_MOUNT_CMD >/dev/null 2>&1
        deadline=\$((\$(date +%s) + 120))
        until sudo mountpoint -q $MOUNT_PATH && \
              sudo dd if=/dev/zero of=$MOUNT_PATH/join-probe.\$\$ bs=4k count=1 >/dev/null 2>&1; do
            [[ \$(date +%s) -lt \$deadline ]] || { echo 'compare_join_fs: mount never came up within 120s' >&2; exit 1; }
            sleep 0.05
        done
        sudo rm -f $MOUNT_PATH/join-probe.* 2>/dev/null
        e=\$(date +%s.%N)
        awk -v s=\$s -v e=\$e 'BEGIN{printf \"%.3f\", e-s}'
    " || die "compare_join_fs: $ip never produced a working mount"
}

# ---- node failure ----
#
# compare_kill_node <pub_ip> — take that node out the way a real failure would.
#
# The three shared-device backends are killed identically, and that is the whole
# point: the machine is powered off through sysrq, so nothing runs an exit path,
# nothing unmounts, nothing releases a lock, and no daemon gets to notice it is
# dying. Killing hand-picked daemons instead measured a different failure per
# backend — `killall -9 corosync dlm_controld` in particular is an orderly
# membership change, which is most of what "fence plus journal replay" was
# supposed to cost, so gfs2 was being asked to recover from something far milder
# than etcfs was. A comparison is only worth publishing if the fault is the same.
#
# The two server-mediated backends are killed exactly the same way, and for the
# same reason. They have no partial failure to inject — the server IS the
# filesystem — so the equivalent fault is losing the machine that runs it, which
# is what the other three lose too. An earlier version blocked their ports and
# killed their service processes instead, to make the outage begin at a known
# instant; measurement showed that fault did not reliably land at all (`iptables
# -I INPUT --dport 2049` left the NFS server answering on 2049 for the whole of
# a 180 s run, and `systemctl kill nfs-server` does not stop the kernel's nfsd
# threads), so the backend was being credited with surviving a failure it never
# had. The known instant now comes from the death watch
# (compare_death_watch_start), not from the shape of the fault, which frees the
# fault to be the same one everywhere.
#
# Powering the victim off is deliberately not undone: the survivor is what is
# being measured, and a victim that reboots and rejoins mid-run changes what the
# survivor is recovering from. Teardown destroys the cluster regardless.
compare_kill_node() {
    local ip="$1"
    # The connection dies with the machine, so a failed ssh here is the expected
    # outcome rather than an error.
    $SSH_CMD -o ConnectTimeout=5 "ec2-user@$ip" \
        "sudo sh -c 'echo 1 > /proc/sys/kernel/sysrq; echo o > /proc/sysrq-trigger' &" \
        >/dev/null 2>&1 || true
}

# compare_failure_target — the node whose loss is worth timing. For the
# shared-device backends that is any node holding locks under load; for the
# server-mediated ones it is the server, and killing anything else measures
# nothing.
compare_failure_target() {
    case "$COMPARE_BACKEND_BASE" in
        nfs|juicefs) echo "${COMPARE_PUB_IPS[0]}" ;;
        *) echo "${BENCH_NODES[-1]}" ;;
    esac
}

# ---- etcfs node lifecycle (join / leave / rejoin) ----
#
# Restarting a node means re-running the two daemons with the arguments
# bootstrap-cluster.sh gave them — endpoints, node ID, device, fencing flags,
# all derived from state this library does not have. Rather than duplicate that
# derivation and drift from it, snapshot each daemon's own command line off the
# running process before killing it, and replay that.
compare_etcfs_snapshot_cmdline() {
    $SSH_CMD "ec2-user@$1" "sudo sh -c '
        ps -o args= -C etcfuse-meta | head -1 > /tmp/meta.cmd
        ps -o args= -C etcfuse | head -1 > /tmp/fuse.cmd
        test -s /tmp/meta.cmd && test -s /tmp/fuse.cmd'" \
        || die "compare_etcfs_snapshot_cmdline: no running daemons on $1"
}

# compare_reattach_volume_if_missing <pub_ip> — re-attaches COMPARE_VOL_ID to
# that node's instance if the shared device isn't there, the same recovery
# scripts/infra/bootstrap-cluster.sh already does for a full cluster rebuild.
# External fencing detaches on purpose and never puts a volume back by itself
# (see bootstrap-cluster.sh's own comment) — this is a benchmark script
# standing in for the operator action that fencing expects to follow it.
compare_reattach_volume_if_missing() {
    local ip="$1" i inst
    $SSH_CMD "ec2-user@$ip" "[[ -e /dev/nvme1n1 ]]" 2>/dev/null && return 0
    for i in "${!COMPARE_PUB_IPS[@]}"; do
        [[ "${COMPARE_PUB_IPS[$i]}" == "$ip" ]] && break
    done
    inst=$(state_get compute_instance_ids | jq -r ".[$i]")
    [[ -n "$inst" && "$inst" != "null" ]] || die "compare_reattach_volume_if_missing: no instance id for $ip"
    log "  $ip ($inst) has lost its device (fenced), reattaching $COMPARE_VOL_ID..."
    aws ec2 attach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1 || true
    aws ec2 wait volume-in-use --volume-ids "$COMPARE_VOL_ID" 2>/dev/null || true
    local n
    for n in 1 2 3 4 5 6 7 8 9 10; do
        $SSH_CMD "ec2-user@$ip" "[[ -e /dev/nvme1n1 ]]" 2>/dev/null && return 0
        sleep 2
    done
    die "compare_reattach_volume_if_missing: device still missing on $ip after reattach"
}

# compare_etcfs_start <pub_ip> — prints seconds from process start to a mount
# that answers a write, which is the join-latency number.
#
# Reattaches the shared volume first if it is missing, which a clean leave no
# longer causes: a node that shuts down gracefully gives back its locks and
# arenas and then announces the departure in the same transaction that removes
# it from membership, so its peers can tell an intentional leave from a crash
# and do not detach it. The reattach stays because a node that was killed, or
# one whose arena release failed, is still fenced for real — and fencing never
# puts a volume back by itself (see bootstrap-cluster.sh's own comment).
compare_etcfs_start() {
    local ip="$1" start_s
    start_s=$(compare_epoch)
    compare_reattach_volume_if_missing "$ip"
    # 120s, not 60: under concurrent write load on the survivors (the caller's
    # own fio job, running the whole time this restarts), the joiner's first
    # etcd writes (membership register, arena claim) contend with that load
    # for commit throughput, and 60s was tight enough to fail a run that was
    # still making progress, not stuck.
    $SSH_CMD "ec2-user@$ip" "
        sudo umount -l $FUSE_MOUNTPOINT 2>/dev/null
        sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
        sudo sh -c 'nohup \$(cat /tmp/meta.cmd) >> /tmp/meta.log 2>&1 &'
        # etcfuse (the FUSE client) does not retry a missing socket — it exits
        # immediately if /run/etcfuse/etcfuse.sock isn't there yet, which a bare
        # back-to-back launch races against etcfuse-meta creating it.
        # bootstrap-cluster.sh's own first-boot sequence sidesteps this with a
        # flat sleep between the two; polling for the socket is the same fix
        # without waiting longer than the daemon actually needs.
        sock_deadline=\$((\$(date +%s) + 30))
        until sudo test -S /run/etcfuse/etcfuse.sock; do
            [[ \$(date +%s) -lt \$sock_deadline ]] || { echo 'compare_etcfs_start: etcfuse-meta never created its socket' >&2; exit 1; }
            sleep 0.1
        done
        sudo sh -c 'nohup \$(cat /tmp/fuse.cmd) >> /tmp/fuse.log 2>&1 &'
        deadline=\$((\$(date +%s) + 120))
        until sudo mountpoint -q $FUSE_MOUNTPOINT && \
              sudo dd if=/dev/zero of=$FUSE_MOUNTPOINT/join-probe.\$\$ bs=4k count=1 >/dev/null 2>&1; do
            [[ \$(date +%s) -lt \$deadline ]] || { echo 'compare_etcfs_start: mount never came up within 120s' >&2; exit 1; }
            sleep 0.05
        done
        sudo rm -f $FUSE_MOUNTPOINT/join-probe.* 2>/dev/null
    " || die "compare_etcfs_start: daemon on $ip never produced a working mount"
    local end_s
    end_s=$(compare_epoch)
    awk -v s="$start_s" -v e="$end_s" 'BEGIN{printf "%.3f", e-s}'
}
