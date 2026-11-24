#!/bin/bash
# compare-scale.sh — add a brand-new node to a running cluster, and take one
# out again, the way an autoscaler's launch and termination hooks would: launch
# an instance, give it the shared volume, run the backend's own join steps.
# Sourced by bench-scale-cycle.sh after compare-lib.sh.
#
# Every step calls scale_mark <step> as it starts, appending "<epoch> <step>" to
# $SCALE_MARKS, so a step's duration is the gap to the next mark. Each wait is
# a poll on the condition that step exists to reach — the device appearing,
# etcd answering /health, the daemon's socket, the mount — never a fixed
# sleep, so the gaps are the time the system took rather than the time a
# script chose to wait.
#
# etcfs follows scripts/infra/add-compute-node.sh (etcd `member add`, start
# etcd as an existing member, start both daemons) and chaos-lib.sh's AWS
# remove_node. The binaries are copied from a running node rather than built
# per node, so the add measures deploying a release, not compiling one.
#
# gfs2 runs the operator's sequence for a node corosync has never seen:
# install the stack, rewrite corosync.conf on every node with the new address,
# `corosync-cfgtool -R` on a member, start corosync and dlm on the new node,
# gfs2_jadd if no journal is free, mount. Removal is the same in reverse:
# unmount, stop dlm and corosync on the leaver, drop it from every survivor's
# corosync.conf and reload. A removed node's journal stays, so the next add
# reuses it instead of adding another.

SCALE_MARKS=""
scale_mark() { printf '%s %s\n' "$(date +%s.%N)" "$1" >> "$SCALE_MARKS"; }

# scale_poll <pub_ip> <seconds> <remote condition> — until the condition holds.
scale_poll() {
    local ip="$1" secs="$2" cond="$3"
    $SSH_CMD "ec2-user@$ip" "deadline=\$((\$(date +%s) + $secs));
        until $cond; do [ \$(date +%s) -lt \$deadline ] || exit 1; sleep 0.1; done" \
        || die "scale: '$cond' never held on $ip within ${secs}s"
}

# scale_report <scenario> <prefix> — one headline per step from $SCALE_MARKS.
scale_report() {
    local scenario="$1" prefix="$2" t step next_t
    local -a ts steps
    while read -r t step; do ts+=("$t"); steps+=("$step"); done < "$SCALE_MARKS"
    for i in "${!steps[@]}"; do
        [[ "${steps[$i]}" == end ]] && continue
        next_t="${ts[$((i + 1))]}"
        compare_headline "$scenario" "${prefix}${steps[$i]}_s" \
            "$(awk -v s="${ts[$i]}" -v e="$next_t" 'BEGIN{printf "%.3f", e-s}')" s
    done
    compare_headline "$scenario" "${prefix}total_s" \
        "$(awk -v s="${ts[0]}" -v e="${ts[-1]}" 'BEGIN{printf "%.3f", e-s}')" s
}

# ---- shared: launch an instance and give it the volume ----

SCALE_INST="" SCALE_PUB="" SCALE_PRIV=""
scale_launch() {
    scale_mark launch
    local n; n=$(( $(state_get compute_ips | jq 'length') + 1 ))
    SCALE_INST=$(aws ec2 run-instances \
        --image-id "$(state_get ami_id | tr -d '"')" --instance-type "$INSTANCE_TYPE" \
        --key-name "$(state_get key_name | tr -d '"')" \
        --security-group-ids "$(state_get sg_id | tr -d '"')" \
        --subnet-id "$(state_get subnet_id | tr -d '"')" --associate-public-ip-address \
        --iam-instance-profile "Name=$("$INFRA_DIR/fencing-iam.sh" profile-name)" \
        --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${ETCFS_CLUSTER}-scale-$n},{Key=ClusterName,Value=$ETCFS_CLUSTER}]" \
        --query 'Instances[0].InstanceId' --output text)
    # Recorded at once, so teardown reaches it even if a later step dies.
    state_append compute_instance_ids "\"$SCALE_INST\""
    aws ec2 wait instance-running --instance-ids "$SCALE_INST"
    SCALE_PRIV=$(get_instance_private_ip "$SCALE_INST")
    SCALE_PUB=$(get_instance_public_ip "$SCALE_INST")
    state_append compute_ips "\"$SCALE_PRIV\""
    state_append compute_public_ips "\"$SCALE_PUB\""

    scale_mark ssh
    wait_for_ssh "$SCALE_PUB" 60 2 || die "scale: $SCALE_PUB never answered SSH"

    scale_mark attach
    aws ec2 attach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$SCALE_INST" --device /dev/sdf >/dev/null
    scale_poll "$SCALE_PUB" 120 "[ -e /dev/nvme1n1 ] || [ -e /dev/xvdf ]"
}

# scale_detach_terminate <instance_id>
scale_detach_terminate() {
    scale_mark detach
    aws ec2 detach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$1" >/dev/null 2>&1 || true
    # Multi-Attach: the volume stays in use by the others, so wait for this
    # instance's attachment to disappear rather than for the volume to free.
    local deadline=$((SECONDS + 120))
    while aws ec2 describe-volumes --volume-ids "$COMPARE_VOL_ID" \
            --query "Volumes[0].Attachments[?InstanceId=='$1'] | length(@)" --output text | grep -qv '^0$'; do
        [[ $SECONDS -lt $deadline ]] || { log "WARNING: $1 still attached after 120s"; break; }
        sleep 1
    done
    scale_mark terminate
    aws ec2 terminate-instances --instance-ids "$1" >/dev/null
    aws ec2 wait instance-terminated --instance-ids "$1"
    scale_mark end
}

# ---- etcfs ----

ETCDCTL="sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379"
SCALE_BIN_DIR=""
SCALE_NODE_ID="" SCALE_ENAME=""
SCALE_ETCFS_N=0

# Once per run: the binaries the base nodes are running, so every added node
# runs the same build and no add pays for a compile.
scale_etcfs_prepare() {
    SCALE_BIN_DIR="$RESULTS_DIR/scale-bin"
    mkdir -p "$SCALE_BIN_DIR"
    scp $SSH_OPTS -q "ec2-user@${COMPARE_PUB_IPS[0]}:/usr/local/bin/{etcfuse,etcfuse-meta,etcd,etcdctl}" "$SCALE_BIN_DIR/" \
        || die "scale: could not copy binaries off ${COMPARE_PUB_IPS[0]}"
}

scale_add_etcfs() {
    local seed="${COMPARE_PUB_IPS[0]}" cluster endpoints p initial out
    cluster=$(state_get cluster_name | tr -d '"')
    SCALE_ETCFS_N=$((SCALE_ETCFS_N + 1))
    SCALE_ENAME="scale$SCALE_ETCFS_N"
    SCALE_NODE_ID="${cluster}-scale-${SCALE_ETCFS_N}"

    scale_launch

    scale_mark install
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo dnf install -y fuse3-libs >/dev/null 2>&1; sudo mkdir -p /mnt/etcfuse /run/etcfuse /var/lib/etcd /var/lib/etcd-wal"
    scp $SSH_OPTS -q "$SCALE_BIN_DIR"/* "ec2-user@$SCALE_PUB:/tmp/"
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo install -m 0755 /tmp/etcfuse /tmp/etcfuse-meta /tmp/etcd /tmp/etcdctl /usr/local/bin/"

    scale_mark member_add
    out=$($SSH_CMD "ec2-user@$seed" "$ETCDCTL member add $SCALE_ENAME --peer-urls=http://$SCALE_PRIV:2380") \
        || die "scale: member add failed: $out"
    initial=$(echo "$out" | sed -nE 's/^ETCD_INITIAL_CLUSTER="?([^"]*)"?$/\1/p')
    [[ -n "$initial" ]] || die "scale: no ETCD_INITIAL_CLUSTER in: $out"

    scale_mark etcd_start
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo nohup /usr/local/bin/etcd --name $SCALE_ENAME --data-dir /var/lib/etcd \
        --wal-dir /var/lib/etcd-wal \
        --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$SCALE_PRIV:2379 \
        --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$SCALE_PRIV:2380 \
        --initial-cluster '$initial' --initial-cluster-state existing \
        --auto-compaction-mode revision --auto-compaction-retention 100000 \
        --quota-backend-bytes 8589934592 > /tmp/etcd.log 2>&1 &"
    scale_poll "$SCALE_PUB" 120 "curl -sf -m 1 http://127.0.0.1:2379/health >/dev/null"

    scale_mark meta_start
    endpoints=""
    while read -r p; do endpoints+="${endpoints:+,}http://$p:2379"; done \
        < <($SSH_CMD "ec2-user@$seed" "$ETCDCTL member list" | awk -F', ' '{print $5}' | sed -E 's#http://([0-9.]+):2379#\1#')
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
        --etcd-endpoints=$endpoints --node-id=$SCALE_NODE_ID --cluster-name=$cluster \
        --lease-ttl=${ETCFS_LEASE_TTL:-10s} --block-device=/dev/nvme1n1 --log-level=1 \
        --ebs-volume-id=$COMPARE_VOL_ID --ec2-instance-id=$SCALE_INST > /tmp/meta.log 2>&1 &"
    scale_poll "$SCALE_PUB" 60 "sudo test -S /run/etcfuse/etcfuse.sock"

    scale_mark fuse_start
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
        --node-id=$SCALE_NODE_ID --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &"
    scale_poll "$SCALE_PUB" 60 "sudo mountpoint -q /mnt/etcfuse"

    # Arena claim is lazy, so the first write is where this node takes one.
    scale_mark first_write
    scale_poll "$SCALE_PUB" 60 "sudo dd if=/dev/zero of=/mnt/etcfuse/scale-first-$SCALE_ETCFS_N bs=4k count=1 oflag=direct >/dev/null 2>&1"
    scale_mark end
}

scale_remove_etcfs() {
    local seed="${COMPARE_PUB_IPS[0]}" memberid
    scale_mark leave
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo fusermount -u /mnt/etcfuse 2>/dev/null; sudo pkill -TERM etcfuse-meta 2>/dev/null; true"
    scale_poll "$SCALE_PUB" 60 "! pgrep -x etcfuse-meta >/dev/null"

    scale_mark member_remove
    memberid=$($SSH_CMD "ec2-user@$seed" "$ETCDCTL member list" | awk -F', ' -v n="$SCALE_ENAME" '$3 == n {print $1}')
    [[ -z "$memberid" ]] || $SSH_CMD "ec2-user@$seed" "$ETCDCTL member remove $memberid" >/dev/null

    scale_detach_terminate "$SCALE_INST"
}

# Whether etcd still records this node, read from a survivor.
scale_etcfs_member_gone() { [[ -z $($SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "$ETCDCTL get 'membership:$SCALE_NODE_ID' --keys-only") ]]; }
scale_etcfs_arenas_gone() { [[ -z $($SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "$ETCDCTL get --prefix 'arena:$SCALE_NODE_ID/' --keys-only") ]]; }

# ---- gfs2 ----

SCALE_GFS2_MEMBERS=()   # private IPs corosync currently lists
SCALE_GFS2_JOURNALS=0

scale_gfs2_prepare() {
    SCALE_GFS2_MEMBERS=("${COMPARE_PRIV_IPS[@]}")
    SCALE_GFS2_JOURNALS="${#COMPARE_PRIV_IPS[@]}"
}

# scale_gfs2_reconfigure — write corosync.conf for SCALE_GFS2_MEMBERS on each
# named node, then reload from the first.
scale_gfs2_reconfigure() {
    local conf ip
    conf=$(compare_gfs2_corosync_conf "${SCALE_GFS2_MEMBERS[@]}")
    for ip in "$@"; do
        echo "$conf" | $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/corosync && sudo tee /etc/corosync/corosync.conf >/dev/null"
    done
}

scale_add_gfs2() {
    local member="${COMPARE_PUB_IPS[0]}" n
    scale_launch

    scale_mark install
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo yum install -y corosync dlm gfs2-utils >/dev/null 2>&1 && sudo mkdir -p $MOUNT_PATH" \
        || die "scale: gfs2 packages failed to install on $SCALE_PUB"

    scale_mark conf_write
    SCALE_GFS2_MEMBERS+=("$SCALE_PRIV")
    scale_gfs2_reconfigure "${COMPARE_PUB_IPS[@]}" "$SCALE_PUB"

    scale_mark corosync_reload
    $SSH_CMD "ec2-user@$member" "sudo corosync-cfgtool -R >/dev/null"

    scale_mark corosync_join
    n=${#SCALE_GFS2_MEMBERS[@]}
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo systemctl start corosync"
    scale_poll "$member" 120 "[ \"\$(sudo corosync-quorumtool -s 2>/dev/null | awk '/^Nodes:/{print \$2}')\" = $n ]"

    scale_mark dlm_start
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo systemctl start dlm"
    scale_poll "$SCALE_PUB" 60 "sudo dlm_tool status >/dev/null 2>&1"

    if [[ "$n" -gt "$SCALE_GFS2_JOURNALS" ]]; then
        scale_mark jadd
        $SSH_CMD "ec2-user@$member" "sudo gfs2_jadd -j 1 $MOUNT_PATH >/dev/null" || die "scale: gfs2_jadd failed"
        SCALE_GFS2_JOURNALS=$((SCALE_GFS2_JOURNALS + 1))
    fi

    scale_mark mount
    $SSH_CMD "ec2-user@$SCALE_PUB" "$COMPARE_REMOTE_MOUNT_CMD" || die "scale: gfs2 mount failed on $SCALE_PUB"

    scale_mark first_write
    scale_poll "$SCALE_PUB" 60 "sudo dd if=/dev/zero of=$MOUNT_PATH/scale-first-\$\$ bs=4k count=1 oflag=direct >/dev/null 2>&1"
    scale_mark end
}

scale_remove_gfs2() {
    local member="${COMPARE_PUB_IPS[0]}" ip keep=()
    scale_mark leave
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo umount $MOUNT_PATH && sudo systemctl stop dlm && sudo systemctl stop corosync" \
        || die "scale: clean gfs2 leave failed on $SCALE_PUB"

    scale_mark conf_write
    for ip in "${SCALE_GFS2_MEMBERS[@]}"; do [[ "$ip" == "$SCALE_PRIV" ]] || keep+=("$ip"); done
    SCALE_GFS2_MEMBERS=("${keep[@]}")
    scale_gfs2_reconfigure "${COMPARE_PUB_IPS[@]}"

    scale_mark corosync_reload
    $SSH_CMD "ec2-user@$member" "sudo corosync-cfgtool -R >/dev/null"
    scale_poll "$member" 60 "[ \"\$(sudo corosync-quorumtool -s 2>/dev/null | awk '/^Nodes:/{print \$2}')\" = ${#SCALE_GFS2_MEMBERS[@]} ]"

    scale_detach_terminate "$SCALE_INST"
}
