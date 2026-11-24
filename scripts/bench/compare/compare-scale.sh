#!/bin/bash
# compare-scale.sh — add a brand-new node to a running cluster, and take one
# out again, the way an autoscaler's launch and termination hooks would: launch
# an instance, give it the shared volume, run the backend's own join steps.
# Sourced by bench-scale-cycle.sh after compare-lib.sh.
#
# Every step appends "<step> <seconds>" to $SCALE_STEPS. A step that runs on a
# node is timed on that node, inside the one SSH call that performs it: an SSH
# round trip from here costs about a second, the same order as most of these
# steps, and timing around it reported a 0.02 s corosync reload as 2.3 s. Only
# the AWS API steps and the fan-outs that touch several nodes (conf_write,
# install) are timed from here. Each wait is a poll on the condition the step
# exists to reach — the device appearing, etcd answering /health, the daemon's
# socket, the mount — never a fixed sleep.
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

SCALE_STEPS=""
scale_record() { printf '%s %s\n' "$1" "$2" >> "$SCALE_STEPS"; }

# scale_local <step> <command...> — time a step run from this machine.
scale_local() {
    local step="$1" s
    shift
    s=$(date +%s.%N)
    "$@"
    scale_record "$step" "$(awk -v s="$s" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f", e-s}')"
}

# scale_remote <step> <pub_ip> <command> [<until-condition> [<seconds>]] — run
# the command on the node, then poll the condition there, timing both on the
# node's own clock. The command's stdout comes back on this function's stdout.
scale_remote() {
    local step="$1" ip="$2" cmd="$3" cond="${4:-true}" secs="${5:-120}" out
    out=$($SSH_CMD "ec2-user@$ip" "s=\$(date +%s.%N)
        { $cmd
        } || exit 1
        deadline=\$((\$(date +%s) + $secs))
        until $cond; do [ \$(date +%s) -lt \$deadline ] || exit 2; sleep 0.05; done
        echo \"@step \$(awk -v s=\$s -v e=\$(date +%s.%N) 'BEGIN{printf \"%.3f\", e-s}')\"") \
        || die "scale: step $step failed on $ip (command: $cmd; until: $cond)"
    scale_record "$step" "$(echo "$out" | awk '$1 == "@step" {print $2}')"
    echo "$out" | grep -v '^@step '
}

# scale_report <scenario> <prefix> <total_seconds> — one headline per step.
scale_report() {
    local scenario="$1" prefix="$2" step secs
    while read -r step secs; do
        compare_headline "$scenario" "${prefix}${step}_s" "$secs" s
    done < "$SCALE_STEPS"
    compare_headline "$scenario" "${prefix}total_s" "$3" s
}

# ---- shared: launch an instance and give it the volume ----

SCALE_INST="" SCALE_PUB="" SCALE_PRIV=""
scale_launch() {
    scale_local launch scale_run_instance
    scale_local ssh wait_for_ssh "$SCALE_PUB" 60 2
    # From the API call until the device shows up, so timed from here: the
    # attachment starts before any SSH call could.
    scale_local attach scale_attach
}

scale_attach() {
    aws ec2 attach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$SCALE_INST" --device /dev/sdf >/dev/null
    $SSH_CMD "ec2-user@$SCALE_PUB" "deadline=\$((\$(date +%s) + 120));
        until [ -e /dev/nvme1n1 ] || [ -e /dev/xvdf ]; do [ \$(date +%s) -lt \$deadline ] || exit 1; sleep 0.05; done" \
        || die "scale: the shared volume never appeared on $SCALE_PUB"
}

scale_run_instance() {
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
}

# scale_detach_terminate <instance_id>
scale_detach_terminate() {
    scale_local detach scale_detach "$1"
    scale_local terminate scale_terminate "$1"
}

scale_detach() {
    aws ec2 detach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$1" >/dev/null 2>&1 || true
    # Multi-Attach: the volume stays in use by the others, so wait for this
    # instance's attachment to disappear rather than for the volume to free.
    local deadline=$((SECONDS + 120))
    while aws ec2 describe-volumes --volume-ids "$COMPARE_VOL_ID" \
            --query "Volumes[0].Attachments[?InstanceId=='$1'] | length(@)" --output text | grep -qv '^0$'; do
        [[ $SECONDS -lt $deadline ]] || { log "WARNING: $1 still attached after 120s"; break; }
        sleep 1
    done
}

scale_terminate() {
    aws ec2 terminate-instances --instance-ids "$1" >/dev/null
    aws ec2 wait instance-terminated --instance-ids "$1"
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
    local b
    # One file per scp: the SFTP-based scp does not expand {a,b} in a remote path.
    for b in etcfuse etcfuse-meta etcd etcdctl; do
        scp $SSH_OPTS -q "ec2-user@${COMPARE_PUB_IPS[0]}:/usr/local/bin/$b" "$SCALE_BIN_DIR/" \
            || die "scale: could not copy $b off ${COMPARE_PUB_IPS[0]}"
    done
}

scale_add_etcfs() {
    local seed="${COMPARE_PUB_IPS[0]}" cluster endpoints p initial out
    cluster=$(state_get cluster_name | tr -d '"')
    SCALE_ETCFS_N=$((SCALE_ETCFS_N + 1))
    SCALE_ENAME="scale$SCALE_ETCFS_N"
    SCALE_NODE_ID="${cluster}-scale-${SCALE_ETCFS_N}"

    scale_launch

    scale_local install scale_etcfs_install

    # Through a file, not $(...): a subshell would drop the step's timing record.
    scale_remote member_add "$seed" "$ETCDCTL member add $SCALE_ENAME --peer-urls=http://$SCALE_PRIV:2380" \
        > "$RESULTS_DIR/scale-member-add.out"
    out=$(cat "$RESULTS_DIR/scale-member-add.out")
    initial=$(echo "$out" | sed -nE 's/^ETCD_INITIAL_CLUSTER="?([^"]*)"?$/\1/p')
    [[ -n "$initial" ]] || die "scale: no ETCD_INITIAL_CLUSTER in: $out"

    scale_remote etcd_start "$SCALE_PUB" "sudo nohup /usr/local/bin/etcd --name $SCALE_ENAME --data-dir /var/lib/etcd \
        --wal-dir /var/lib/etcd-wal \
        --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$SCALE_PRIV:2379 \
        --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$SCALE_PRIV:2380 \
        --initial-cluster '$initial' --initial-cluster-state existing \
        --auto-compaction-mode revision --auto-compaction-retention 100000 \
        --quota-backend-bytes 8589934592 > /tmp/etcd.log 2>&1 &" \
        "curl -sf -m 1 http://127.0.0.1:2379/health >/dev/null" >/dev/null

    endpoints=""
    while read -r p; do endpoints+="${endpoints:+,}http://$p:2379"; done \
        < <($SSH_CMD "ec2-user@$seed" "$ETCDCTL member list" | awk -F', ' '{print $5}' | sed -E 's#http://([0-9.]+):2379#\1#')
    scale_remote meta_start "$SCALE_PUB" "sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
        --etcd-endpoints=$endpoints --node-id=$SCALE_NODE_ID --cluster-name=$cluster \
        --lease-ttl=${ETCFS_LEASE_TTL:-10s} --block-device=/dev/nvme1n1 --log-level=1 \
        --ebs-volume-id=$COMPARE_VOL_ID --ec2-instance-id=$SCALE_INST > /tmp/meta.log 2>&1 &" \
        "sudo test -S /run/etcfuse/etcfuse.sock" 60 >/dev/null

    scale_remote fuse_start "$SCALE_PUB" "sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
        --node-id=$SCALE_NODE_ID --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &" \
        "sudo mountpoint -q /mnt/etcfuse" 60 >/dev/null

    # Arena claim is lazy, so the first write is where this node takes one.
    scale_remote first_write "$SCALE_PUB" true \
        "sudo dd if=/dev/zero of=/mnt/etcfuse/scale-first-$SCALE_ETCFS_N bs=4k count=1 oflag=direct >/dev/null 2>&1" 60 >/dev/null
}

scale_etcfs_install() {
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo dnf install -y fuse3-libs 2>&1 | tail -2; sudo mkdir -p /mnt/etcfuse /run/etcfuse /var/lib/etcd /var/lib/etcd-wal" \
        || die "scale: package install failed on $SCALE_PUB"
    scp $SSH_OPTS "$SCALE_BIN_DIR"/* "ec2-user@$SCALE_PUB:/tmp/" || die "scale: copying binaries to $SCALE_PUB failed"
    $SSH_CMD "ec2-user@$SCALE_PUB" "sudo install -m 0755 /tmp/etcfuse /tmp/etcfuse-meta /tmp/etcd /tmp/etcdctl /usr/local/bin/" \
        || die "scale: installing binaries on $SCALE_PUB failed"
}

scale_remove_etcfs() {
    local seed="${COMPARE_PUB_IPS[0]}" memberid
    scale_remote leave "$SCALE_PUB" "sudo fusermount -u /mnt/etcfuse 2>/dev/null; sudo pkill -TERM etcfuse-meta 2>/dev/null; true" \
        "! pgrep -x etcfuse-meta >/dev/null" 60 >/dev/null

    memberid=$($SSH_CMD "ec2-user@$seed" "$ETCDCTL member list" | awk -F', ' -v n="$SCALE_ENAME" '$3 == n {print $1}')
    [[ -n "$memberid" ]] || die "scale: no etcd member named $SCALE_ENAME"
    scale_remote member_remove "$seed" "$ETCDCTL member remove $memberid >/dev/null"

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

    scale_local install $SSH_CMD "ec2-user@$SCALE_PUB" "sudo yum install -y corosync dlm gfs2-utils >/dev/null 2>&1 && sudo mkdir -p $MOUNT_PATH"

    SCALE_GFS2_MEMBERS+=("$SCALE_PRIV")
    n=${#SCALE_GFS2_MEMBERS[@]}
    scale_local conf_write scale_gfs2_reconfigure "${COMPARE_PUB_IPS[@]}" "$SCALE_PUB"
    scale_remote corosync_reload "$member" "sudo corosync-cfgtool -R >/dev/null" >/dev/null
    # Counted from the new node's own view of the membership.
    scale_remote corosync_join "$SCALE_PUB" "sudo systemctl start corosync" \
        "[ \"\$(sudo corosync-quorumtool -s 2>/dev/null | awk '/^Nodes:/{print \$2}')\" = $n ]" >/dev/null
    scale_remote dlm_start "$SCALE_PUB" "sudo systemctl start dlm" "sudo dlm_tool status >/dev/null 2>&1" 60 >/dev/null
    if [[ "$n" -gt "$SCALE_GFS2_JOURNALS" ]]; then
        scale_remote jadd "$member" "sudo gfs2_jadd -j 1 $MOUNT_PATH >/dev/null" >/dev/null
        SCALE_GFS2_JOURNALS=$((SCALE_GFS2_JOURNALS + 1))
    fi
    scale_remote mount "$SCALE_PUB" "$COMPARE_REMOTE_MOUNT_CMD" >/dev/null
    scale_remote first_write "$SCALE_PUB" true \
        "sudo dd if=/dev/zero of=$MOUNT_PATH/scale-first-\$\$ bs=4k count=1 oflag=direct >/dev/null 2>&1" 60 >/dev/null
}

scale_remove_gfs2() {
    local member="${COMPARE_PUB_IPS[0]}" ip keep=()
    scale_remote leave "$SCALE_PUB" "sudo umount $MOUNT_PATH && sudo systemctl stop dlm && sudo systemctl stop corosync" >/dev/null

    for ip in "${SCALE_GFS2_MEMBERS[@]}"; do [[ "$ip" == "$SCALE_PRIV" ]] || keep+=("$ip"); done
    SCALE_GFS2_MEMBERS=("${keep[@]}")
    scale_local conf_write scale_gfs2_reconfigure "${COMPARE_PUB_IPS[@]}"
    scale_remote corosync_reload "$member" "sudo corosync-cfgtool -R >/dev/null" \
        "[ \"\$(sudo corosync-quorumtool -s 2>/dev/null | awk '/^Nodes:/{print \$2}')\" = ${#SCALE_GFS2_MEMBERS[@]} ]" 60 >/dev/null

    scale_detach_terminate "$SCALE_INST"
}
