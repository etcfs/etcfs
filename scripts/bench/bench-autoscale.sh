#!/bin/bash
# bench-autoscale.sh — the Auto Scaling path, end to end, on a real ASG built
# with the AWS CLI alone.
#
# Scale-out: from the desired-capacity change to the new instance's first
# successful write, split into
#   asg_to_launch_s   change -> the instance's EC2 LaunchTime
#   launch_to_boot_s  LaunchTime -> the bootstrap script's first log line
#   install_s         bootstrap start -> packages installed, volume attached
#   etcd_join_s       -> etcd active (seed election, member add, start)
#   daemon_start_s    -> etcfuse-meta and etcfuse active, mount up
#   first_write_s     mount up -> first 4 KiB O_DIRECT write that succeeds
#                     (arena claim is lazy, so it is inside this step)
# Bootstrap and systemd timestamps have one-second resolution; the totals are
# taken from sub-second clocks on both ends.
#
# Scale-in: the instance is terminated through the ASG with a TERMINATING
# lifecycle hook. This script stands in for the Terraform module's
# graceful-leave Lambda and runs the same step — `etcdctl member remove` from a
# survivor over SSM — then completes the hook. Reported, from the terminate
# call: hook_wait_s, member_remove_s, membership_gone_s (the node's
# membership:<id> key deleted) and arenas_released_s (no arena:<id>/ key left;
# -1 if they were still owned after ETCFS_AS_WAIT seconds).
#
# Throughout, the original nodes write sequentially and one of them runs the
# 4 KiB probe; survivor_max_stall_s is the longest gap between its successes
# over each event.
#
# No GFS2 equivalent: GFS2 admits a node only after its journal is added and
# its address is in every member's corosync.conf, so a launch hook would have to
# rewrite and reload the configuration of every running node. That is the
# sequence bench-gfs2-journal-add.sh ETCFS_JADD_FULL=1 times by hand; it is
# not reproduced as an ASG here.
#
# The node bootstrap is etcfs-terraform-modules' node-bootstrap.sh, used
# verbatim, and installs the last *released* RPMs, not this working tree.
#
# ETCFS_AS_CYCLES (default 3), ETCFS_AS_NODES (initial size, 3),
# ETCFS_AS_WAIT (seconds to wait for each step, 300),
# ETCFS_AS_BOOTSTRAP (path to node-bootstrap.sh), ETCFS_VERSION (release, latest).
#
# Usage:
#   ./bench-autoscale.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

REGION="${AWS_REGION:-eu-west-1}"
AZ="${ETCFS_AZ:-eu-west-1a}"
CYCLES="${ETCFS_AS_CYCLES:-3}"
NODES="${ETCFS_AS_NODES:-3}"
WAIT="${ETCFS_AS_WAIT:-300}"
INSTANCE_TYPE="${ETCFS_INSTANCE_TYPE:-m7i.large}"
KEY_NAME="${ETCFS_KEY_NAME:-etcfs-bench-key}"
PEM="${PEM_PATH:-$HOME/.ssh/id_ed25519}"
BOOTSTRAP="${ETCFS_AS_BOOTSTRAP:-$PROJECT_ROOT/../etcfs-terraform-modules/terraform/modules/etcfs-asg/scripts/node-bootstrap.sh}"
CLUSTER="autoscale-$(date +%Y%m%d-%H%M%S)"
RESULTS_DIR="$PROJECT_ROOT/benchmark-results/autoscale/$CLUSTER"
MNT=/mnt/etcfuse
SSH="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o BatchMode=yes -i $PEM"
export AWS_REGION="$REGION"

log() { echo "[$(date +%T)] $*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }
epoch() { date +%s.%N; }
since() { awk -v s="$1" -v e="${2:-$(epoch)}" 'BEGIN{printf "%.3f", e-s}'; }
[[ -f "$BOOTSTRAP" ]] || die "node bootstrap not found at $BOOTSTRAP (set ETCFS_AS_BOOTSTRAP)"
[[ -f "$PEM" ]] || die "no SSH key at $PEM"
mkdir -p "$RESULTS_DIR"
HEADLINE="$RESULTS_DIR/headline.json"
echo "[]" > "$HEADLINE"
headline() {
    jq --arg s "$1" --arg m "$2" --arg v "$3" --arg u "${4:-}" \
        '. + [{scenario: $s, backend: "etcfs", metric: $m, value: ($v | tonumber), unit: $u}]' \
        "$HEADLINE" > "$HEADLINE.tmp" && mv "$HEADLINE.tmp" "$HEADLINE"
    log "HEADLINE [$1] $2 = $3 $4"
}

# ---- teardown: everything this run made, and only that ----
ASG="$CLUSTER-asg"
SG_ID="" VOL_ID="" LT_ID="" TABLE="$CLUSTER-etcfs-seed"
teardown() {
    set +e
    log "tearing down $CLUSTER"
    aws autoscaling delete-auto-scaling-group --auto-scaling-group-name "$ASG" --force-delete >/dev/null 2>&1
    local ids
    ids=$(aws ec2 describe-instances --filters "Name=tag:ClusterName,Values=$CLUSTER" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" \
        --query 'Reservations[].Instances[].InstanceId' --output text)
    if [[ -n "$ids" ]]; then
        # shellcheck disable=SC2086
        aws ec2 terminate-instances --instance-ids $ids >/dev/null
        # shellcheck disable=SC2086
        aws ec2 wait instance-terminated --instance-ids $ids
    fi
    [[ -n "$LT_ID" ]] && aws ec2 delete-launch-template --launch-template-id "$LT_ID" >/dev/null
    if [[ -n "$VOL_ID" ]]; then
        aws ec2 wait volume-available --volume-ids "$VOL_ID"
        aws ec2 delete-volume --volume-id "$VOL_ID"
    fi
    aws dynamodb delete-table --table-name "$TABLE" >/dev/null 2>&1
    if [[ -n "$SG_ID" ]]; then
        for _ in $(seq 1 30); do
            aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null && break
            sleep 10
        done
    fi
    log "teardown done"
}
trap teardown EXIT

# ---- provision ----
TAGS="Key=ClusterName,Value=$CLUSTER"
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC" "Name=availability-zone,Values=$AZ" \
    --query 'Subnets[0].SubnetId' --output text)
MY_IP=$(curl -fsS https://checkip.amazonaws.com)

SG_ID=$(aws ec2 create-security-group --group-name "$CLUSTER-sg" --description "EtcFS autoscale bench $CLUSTER" \
    --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{$TAGS}]" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 --cidr "$MY_IP/32" >/dev/null
for p in 2379 2380 9090 9100; do
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port "$p" --source-group "$SG_ID" >/dev/null
done

VOL_ID=$(aws ec2 create-volume --availability-zone "$AZ" --volume-type io2 --size 20 --iops 1000 \
    --multi-attach-enabled --tag-specifications "ResourceType=volume,Tags=[{$TAGS},{Key=Name,Value=$CLUSTER-data}]" \
    --query VolumeId --output text)
aws dynamodb create-table --table-name "$TABLE" --billing-mode PAY_PER_REQUEST \
    --attribute-definitions AttributeName=cluster_name,AttributeType=S \
    --key-schema AttributeName=cluster_name,KeyType=HASH --tags "Key=ClusterName,Value=$CLUSTER" >/dev/null
aws ec2 wait volume-available --volume-ids "$VOL_ID"
aws dynamodb wait table-exists --table-name "$TABLE"

AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --query Parameter.Value --output text)
# The same user-data the Terraform module builds: an environment preamble and
# the bootstrap verbatim, gzipped to stay under EC2's 16 KiB limit.
USERDATA=$( {
    printf '#!/bin/bash\n'
    printf 'export ETCFS_CLUSTER_NAME=%s\nexport ETCFS_REGION=%s\nexport ETCFS_VOLUME_ID=%s\n' "$CLUSTER" "$REGION" "$VOL_ID"
    printf 'export ETCFS_SEED_TABLE=%s\nexport ETCFS_VERSION=%s\n' "$TABLE" "${ETCFS_VERSION:-}"
    cat "$BOOTSTRAP"
} | gzip -9 | base64 -w0)
[[ "${#USERDATA}" -le 16384 ]] || die "user-data is ${#USERDATA} bytes encoded, over EC2's 16384"
LT_DATA=$(jq -n --arg ami "$AMI" --arg it "$INSTANCE_TYPE" --arg key "$KEY_NAME" --arg sg "$SG_ID" \
    --arg ud "$USERDATA" --arg c "$CLUSTER" '{
    ImageId: $ami, InstanceType: $it, KeyName: $key,
    IamInstanceProfile: {Name: "etcfs-nodes"},
    SecurityGroupIds: [$sg], UserData: $ud,
    MetadataOptions: {HttpTokens: "required", HttpEndpoint: "enabled"},
    BlockDeviceMappings: [{DeviceName: "/dev/xvda", Ebs: {VolumeSize: 20, VolumeType: "gp3"}}],
    TagSpecifications: [{ResourceType: "instance", Tags: [{Key: "ClusterName", Value: $c}, {Key: "Role", Value: "etcfs-node"}]}]
}')
LT_ID=$(aws ec2 create-launch-template --launch-template-name "$CLUSTER" --launch-template-data "$LT_DATA" \
    --tag-specifications "ResourceType=launch-template,Tags=[{$TAGS}]" --query LaunchTemplate.LaunchTemplateId --output text)

aws autoscaling create-auto-scaling-group --auto-scaling-group-name "$ASG" \
    --launch-template "LaunchTemplateId=$LT_ID,Version=\$Latest" \
    --min-size 0 --max-size $((NODES + 1)) --desired-capacity "$NODES" \
    --vpc-zone-identifier "$SUBNET" --health-check-type EC2 \
    --tags "ResourceId=$ASG,ResourceType=auto-scaling-group,Key=ClusterName,Value=$CLUSTER,PropagateAtLaunch=true" \
           "ResourceId=$ASG,ResourceType=auto-scaling-group,Key=Role,Value=etcfs-node,PropagateAtLaunch=true"
aws autoscaling put-lifecycle-hook --auto-scaling-group-name "$ASG" --lifecycle-hook-name graceful-leave \
    --lifecycle-transition autoscaling:EC2_INSTANCE_TERMINATING --default-result CONTINUE --heartbeat-timeout 300

# ---- helpers ----
asg_instances() {
    aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$ASG" \
        --query 'AutoScalingGroups[0].Instances[].[InstanceId,LifecycleState]' --output text
}
pub_ip()  { aws ec2 describe-instances --instance-ids "$1" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text; }
priv_ip() { aws ec2 describe-instances --instance-ids "$1" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text; }
node_id() { echo "$CLUSTER-$(echo -n "$1" | tail -c 8)"; }
on() { local ip="$1"; shift; $SSH "ec2-user@$ip" "$@"; }
on_bg() { local ip="$1"; shift; $SSH -n -f "ec2-user@$ip" "$@"; }

wait_mounted() {  # <pub_ip> — until the bootstrap reports the mount up
    local deadline=$((SECONDS + WAIT))
    until on "$1" "sudo grep -q 'up and mounted' /var/log/etcfs-bootstrap.log" 2>/dev/null; do
        [[ $SECONDS -lt $deadline ]] || die "$1 never mounted (see /var/log/etcfs-bootstrap.log there)"
        on "$1" "sudo grep -q FATAL /var/log/etcfs-bootstrap.log" 2>/dev/null && die "$1 bootstrap failed"
        sleep 3
    done
}

log "waiting for the initial $NODES nodes of $CLUSTER"
deadline=$((SECONDS + WAIT * 2))
until [[ $(asg_instances | awk '$2 == "InService"' | wc -l) -ge "$NODES" ]]; do
    [[ $SECONDS -lt $deadline ]] || die "ASG never reached $NODES InService instances"
    sleep 5
done
mapfile -t BASE_IDS < <(asg_instances | awk '$2 == "InService" {print $1}')
BASE_PUB=()
for id in "${BASE_IDS[@]}"; do BASE_PUB+=("$(pub_ip "$id")"); done
for ip in "${BASE_PUB[@]}"; do wait_mounted "$ip"; done
PROBE="${BASE_PUB[0]}"
ETCDCTL="etcdctl --endpoints=http://127.0.0.1:2379"
log "base nodes up: ${BASE_PUB[*]}"

# ---- survivor load and probe ----
for ip in "${BASE_PUB[@]}"; do on "$ip" "sudo dnf install -y fio >/dev/null 2>&1"; done
load_start() {
    local ip
    for ip in "${BASE_PUB[@]}"; do
        on_bg "$ip" "sudo nohup fio --name=load --filename=$MNT/as-load-$ip.dat --rw=write --bs=1M --size=1G \
            --direct=1 --ioengine=psync --time_based --runtime=$1 --output-format=json --output=/tmp/as-load.json >/dev/null 2>&1 &"
    done
}
probe_start() {
    on_bg "$PROBE" "sudo sh -c 'rm -f /tmp/probe.stop; while [ ! -f /tmp/probe.stop ]; do
        t=\$(date +%s.%N)
        if dd if=/dev/zero of=$MNT/as-probe.dat bs=4k count=1 conv=notrunc oflag=direct >/dev/null 2>&1;
        then echo \"\$t ok\"; else echo \"\$t err\"; fi
    done > /tmp/probe.log' >/dev/null 2>&1"
}
# max_stall <from> <to> — longest gap between probe successes in the window.
max_stall() {
    on "$PROBE" "sudo cat /tmp/probe.log" > "$RESULTS_DIR/probe.log"
    awk -v a="$1" -v b="$2" '$2 == "ok" && $1 >= a && $1 <= b { if (p != "" && $1 - p > g) g = $1 - p; p = $1 }
        END { printf "%.3f", g + 0 }' "$RESULTS_DIR/probe.log"
}
# boot_step <pub_ip> <grep pattern> — epoch of the first bootstrap log line matching.
# The log stamps UTC HH:MM:SS; the date comes from the node's clock.
boot_step() {
    on "$1" "d=\$(date -u +%F); t=\$(sudo grep -m1 -- '$2' /var/log/etcfs-bootstrap.log | sed -E 's/^\[([0-9:]+)\].*/\1/');
             [ -n \"\$t\" ] && date -u -d \"\$d \$t\" +%s || echo -1"
}
unit_up() {  # <pub_ip> <unit> — epoch the unit last became active
    on "$1" "date -d \"\$(systemctl show -p ActiveEnterTimestamp --value $2)\" +%s"
}

load_start $(( CYCLES * (WAIT + 120) + 120 ))
probe_start
sleep 5

# ---- cycles ----
for c in $(seq 1 "$CYCLES"); do
    S="autoscale-cycle$c"
    # Scale out.
    t0=$(epoch)
    aws autoscaling set-desired-capacity --auto-scaling-group-name "$ASG" --desired-capacity $((NODES + 1))
    new=""
    deadline=$((SECONDS + WAIT))
    while [[ -z "$new" ]]; do
        [[ $SECONDS -lt $deadline ]] || die "scale-out launched nothing"
        sleep 2
        new=$(asg_instances | awk '{print $1}' | grep -vxF -f <(printf '%s\n' "${BASE_IDS[@]}") | head -1 || true)
    done
    aws ec2 wait instance-running --instance-ids "$new"
    launch=$(date -d "$(aws ec2 describe-instances --instance-ids "$new" \
        --query 'Reservations[0].Instances[0].LaunchTime' --output text)" +%s.%N)
    nip=$(pub_ip "$new")
    until on "$nip" true 2>/dev/null; do sleep 2; done
    wait_mounted "$nip"
    # First write timed on the node, from the moment the mount is up.
    first=$(on "$nip" "until sudo dd if=/dev/zero of=$MNT/as-first-\$\$ bs=4k count=1 oflag=direct >/dev/null 2>&1; do sleep 0.05; done; date +%s.%N")
    t_end=$first
    b_start=$(boot_step "$nip" 'instance=')
    b_attach=$(boot_step "$nip" 'attaching volume')
    u_etcd=$(unit_up "$nip" etcd)
    u_fuse=$(unit_up "$nip" etcfuse)
    b_mounted=$(boot_step "$nip" 'up and mounted')
    owns=$(on "${BASE_PUB[0]}" "$ETCDCTL get --prefix 'arena:$(node_id "$new")/' --keys-only | grep -c . || true")

    headline "$S" asg_to_launch_s "$(since "$t0" "$launch")" s
    headline "$S" launch_to_boot_s "$(since "$launch" "$b_start")" s
    headline "$S" install_s "$(since "$b_start" "$b_attach")" s
    headline "$S" etcd_join_s "$(since "$b_attach" "$u_etcd")" s
    headline "$S" daemon_start_s "$(since "$u_etcd" "$b_mounted")" s
    headline "$S" etcfuse_active_after_etcd_s "$(since "$u_etcd" "$u_fuse")" s
    headline "$S" first_write_s "$(since "$b_mounted" "$first")" s
    headline "$S" scale_out_total_s "$(since "$t0" "$t_end")" s
    headline "$S" arena_claimed "$([[ "$owns" -gt 0 ]] && echo 1 || echo 0)" bool
    headline "$S" survivor_max_stall_out_s "$(max_stall "$t0" "$t_end")" s

    # Scale in: terminate the new node through the ASG, playing the Lambda.
    nid=$(node_id "$new")
    npriv=$(priv_ip "$new")
    survivor="${BASE_IDS[0]}"
    t0=$(epoch)
    aws autoscaling terminate-instance-in-auto-scaling-group --instance-id "$new" \
        --should-decrement-desired-capacity >/dev/null
    deadline=$((SECONDS + WAIT))
    until asg_instances | grep -q "^${new}[[:space:]]Terminating:Wait"; do
        [[ $SECONDS -lt $deadline ]] || die "$new never reached Terminating:Wait"
        sleep 1
    done
    t_hook=$(epoch)
    remove="M=\$($ETCDCTL member list | grep '$npriv:2380' | cut -d, -f1); [ -z \"\$M\" ] || $ETCDCTL member remove \$M"
    cmd=$(aws ssm send-command --instance-ids "$survivor" --document-name AWS-RunShellScript \
        --parameters "$(jq -n --arg c "$remove" '{commands: [$c]}')" \
        --query Command.CommandId --output text)
    until st=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$survivor" \
                --query Status --output text 2>/dev/null) && [[ "$st" != Pending && "$st" != InProgress && "$st" != Delayed ]]; do
        sleep 1
    done
    [[ "$st" == Success ]] || log "WARNING: member remove over SSM ended $st"
    t_removed=$(epoch)
    aws autoscaling complete-lifecycle-action --auto-scaling-group-name "$ASG" \
        --lifecycle-hook-name graceful-leave --instance-id "$new" --lifecycle-action-result CONTINUE
    gone=-1 released=-1
    deadline=$((SECONDS + WAIT))
    while [[ $SECONDS -lt $deadline && ( "$gone" == -1 || "$released" == -1 ) ]]; do
        now=$(epoch)
        if [[ "$gone" == -1 ]] && [[ -z $(on "${BASE_PUB[0]}" "$ETCDCTL get 'membership:$nid' --keys-only") ]]; then
            gone=$(since "$t0" "$now")
        fi
        if [[ "$released" == -1 ]] && [[ -z $(on "${BASE_PUB[0]}" "$ETCDCTL get --prefix 'arena:$nid/' --keys-only") ]]; then
            released=$(since "$t0" "$now")
        fi
        sleep 1
    done
    t_end=$(epoch)
    headline "$S" hook_wait_s "$(since "$t0" "$t_hook")" s
    headline "$S" member_remove_s "$(since "$t_hook" "$t_removed")" s
    headline "$S" membership_gone_s "$gone" s
    headline "$S" arenas_released_s "$released" s
    headline "$S" survivor_max_stall_in_s "$(max_stall "$t0" "$t_end")" s
    aws ec2 wait instance-terminated --instance-ids "$new"
done

on "$PROBE" "sudo touch /tmp/probe.stop" || true
log "results in $RESULTS_DIR"
