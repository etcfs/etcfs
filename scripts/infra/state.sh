#!/bin/bash
# state.sh — EtcFS infrastructure state management.
#
# Sources:
#   $ETCFS_STATE   — path to state JSON file (default: infra-state.json)
#
# Provides helpers: state_get, state_put, state_save, state_init,
#   discover_*, wait_for_*, SSH_CMD, log, die.
#
# Adapted from QAttach infra scripts. Replaces cluster-agent references
# with etcfuse daemon references. Same state-file + etcd colocation pattern.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default state file
ETCFS_STATE="${ETCFS_STATE:-$PROJECT_ROOT/infra-state.json}"

# ---- Defaults (overridable via env) ----
REGION="${AWS_DEFAULT_REGION:-eu-west-1}"
AZ="${ETCFS_AZ:-eu-west-1a}"
CLUSTER_NAME="${ETCFS_CLUSTER:-etcfuse}"
KEY_NAME="${ETCFS_KEY_NAME:-etcfuse-keypair}"
PEM_PATH="${ETCFS_PEM_PATH:-$HOME/.ssh/id_ed25519}"
COMPUTE_NODES="${ETCFS_COMPUTE_NODES:-3}"
INSTANCE_TYPE="${ETCFS_INSTANCE_TYPE:-m7i.large}"
VOLUME_SIZE_GB="${ETCFS_VOLUME_SIZE:-10}"
VOLUME_IOPS="${ETCFS_VOLUME_IOPS:-100}"
SUBNET_ID="${ETCFS_SUBNET:-}"
SECURITY_GROUP_ID="${ETCFS_SG:-}"
ETCD_VER="${ETCFS_ETCD_VERSION:-v3.5.18}"
LEASH_TTL="${ETCFS_LEASE_TTL:-10}"
HEARTBEAT_INTERVAL="${ETCFS_HEARTBEAT:-3}"
FUSE_MOUNTPOINT="${ETCFS_MOUNTPOINT:-/mnt/etcfuse}"

export AWS_DEFAULT_REGION="$REGION"

# Expand ~ in PEM_PATH
PEM="${PEM_PATH/#\~/$HOME}"

# UserKnownHostsFile=/dev/null is not redundant with StrictHostKeyChecking=no.
# StrictHostKeyChecking=no only auto-accepts an *unknown* host key; it does not
# override a *conflicting* one. AWS recycles public IPs between runs, so an IP
# from a torn-down cluster is routinely reissued to a new instance with a new
# host key, and ssh then refuses the connection outright with "REMOTE HOST
# IDENTIFICATION HAS CHANGED" — which stalls provisioning until someone runs
# ssh-keygen -R by hand. Observed on 2026-08-06. Discarding the known-hosts
# file means no entry is ever recorded, so no conflict can arise, and the
# harness stops accumulating dozens of dead IPs in the operator's
# ~/.ssh/known_hosts. The host-key check this gives up was already disabled by
# StrictHostKeyChecking=no; these are ephemeral, self-provisioned test
# instances, so its only practical effect here was the false positive above.
# LogLevel=ERROR suppresses the "Permanently added ... to the list of known
# hosts" line that /dev/null would otherwise print on every single connection.
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o BatchMode=yes -i $PEM"
SSH_CMD="ssh $SSH_OPTS"

# stderr, not stdout: several callers capture a function's real return value
# via $(...) (e.g. compare_run_job's result path) and would otherwise pick up
# every log() line printed during that call as part of the value.
log()  { echo "[$(date +%T)] $*" >&2; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# Burstable classes (t2, t3, t3a, t4g) are refused: once a run spends its CPU
# credits it is throttled to the baseline rate, and its wall-clock numbers
# measure the throttle instead of the filesystem.
[[ "$INSTANCE_TYPE" == t[0-9]* ]] && die "burstable instance type $INSTANCE_TYPE is not allowed; use m7i.large or larger"

# ---- State file helpers ----

_state_file() {
    # Reload state when the JSON file changes on disk
    cat "$ETCFS_STATE" 2>/dev/null || echo "{}"
}

state_init() {
    if [[ ! -f "$ETCFS_STATE" ]]; then
        cat > "$ETCFS_STATE" <<'JSON'
{
  "cluster_name": "",
  "region": "",
  "az": "",
  "key_name": "",
  "vpc_id": "",
  "sg_id": "",
  "subnet_id": "",
  "ami_id": "",
  "volume_id": "",
  "compute_ips": [],
  "compute_public_ips": [],
  "compute_instance_ids": [],
  "etcd_endpoints": "",
  "created_at": ""
}
JSON
    fi
}

state_get() {
    local key="$1"
    jq -r ".$key" "$ETCFS_STATE"
}

state_put() {
    local key="$1" value="$2"
    local tmp
    tmp="$(mktemp)"
    jq ".$key = $value" "$ETCFS_STATE" > "$tmp" && mv "$tmp" "$ETCFS_STATE"
}

state_append() {
    local key="$1" value="$2"
    local tmp
    tmp="$(mktemp)"
    jq ".$key += [$value]" "$ETCFS_STATE" > "$tmp" && mv "$tmp" "$ETCFS_STATE"
}

state_save_array() {
    local key="$1"; shift
    local json
    json=$(printf '%s\n' "$@" | jq -R . | jq -s .)
    state_put "$key" "$json"
}

# ---- AWS discovery ----

discover_vpc() {
    if [[ -n "$SUBNET_ID" ]]; then
        aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" \
            --query 'Subnets[0].VpcId' --output text
    else
        aws ec2 describe-vpcs \
            --filters Name=isDefault,Values=true \
            --query 'Vpcs[0].VpcId' --output text
    fi
}

discover_subnet() {
    if [[ -n "$SUBNET_ID" ]]; then
        echo "$SUBNET_ID"
    else
        aws ec2 describe-subnets \
            --filters "Name=vpc-id,Values=$(discover_vpc)" "Name=availability-zone,Values=$AZ" \
            --query 'Subnets[0].SubnetId' --output text
    fi
}

# ETCFS_AMI_NAME_FILTER overrides the AMI family every other backend uses
# (al2023-ami-2023*) — e.g. GFS2's gfs2-utils/kmod-gfs2 packages exist for
# Amazon Linux 2 (amzn2-ami-hvm-*) but not AL2023, which dropped them.
discover_ami() {
    aws ec2 describe-images \
        --owners amazon \
        --filters \
            "Name=name,Values=${ETCFS_AMI_NAME_FILTER:-al2023-ami-2023*}" \
            "Name=architecture,Values=x86_64" \
            "Name=state,Values=available" \
        --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text
}

# ---- Instance helpers ----

wait_for_instance() {
    local id="$1"
    log "waiting for instance $id to be running..."
    aws ec2 wait instance-running --instance-ids "$id"
}

get_instance_private_ip() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text
}

get_instance_public_ip() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}

get_instance_state() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].State.Name' --output text
}

# ---- Daemon build ----

# build_etcfuse_binary — build cmd/etcfuse for the AWS target and print the
# path to the resulting binary.
#
# Built via deploy/docker/Dockerfile.etcfuse (amazonlinux:2023, same base as
# the AMI resolved by discover_ami), not the caller's local toolchain. A
# binary built on the operator's machine links whatever libfuse3/glibc happen
# to be installed there, which routinely does not match the target AMI's
# package versions — observed 2026-08-09 as `error while loading shared
# libraries: libfuse3.so.4`, the AMI's fuse3-libs providing only .so.3. The
# Docker build already targets the right base image; this reuses it instead
# of re-deriving the compatible toolchain by hand.
#
# Output path is separate from bin/etcfuse (the Makefile's local-build
# target) so this never clobbers a binary someone built for local/Docker-
# compose use.
build_etcfuse_binary() {
    local out="$PROJECT_ROOT/bin/etcfuse.aws-linux-amd64"
    docker build -q -f "$PROJECT_ROOT/deploy/docker/Dockerfile.etcfuse" \
        -t etcfuse-aws-build "$PROJECT_ROOT" >&2 || return 1
    local cid
    cid=$(docker create etcfuse-aws-build) || return 1
    docker cp "$cid:/usr/local/bin/etcfuse" "$out" >&2 || { docker rm "$cid" >/dev/null; return 1; }
    docker rm "$cid" >/dev/null
    echo "$out"
}

# build_etcfuse_meta_binary — build cmd/etcfuse-meta for the AWS target and
# print the path to the resulting binary.
#
# Unlike etcfuse, this is CGO_ENABLED=0 pure Go (see
# deploy/docker/Dockerfile.etcfuse-meta) — a linux/amd64 cross-build is
# statically linked and portable to any glibc/musl target as-is, so no
# Docker step is needed to match the AMI the way build_etcfuse_binary's is.
build_etcfuse_meta_binary() {
    local out="$PROJECT_ROOT/bin/etcfuse-meta.aws-linux-amd64"
    (cd "$PROJECT_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -ldflags="-s -w" -o "$out" ./cmd/etcfuse-meta) >&2 || return 1
    echo "$out"
}

# ---- SSH / health-check helpers ----

wait_for_ssh() {
    local ip="$1"
    local max_retries="${2:-60}"
    local delay="${3:-5}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" "echo ok" &>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

wait_for_inactive() {
    local ip="$1" svc="$2"
    local max_retries="${3:-30}"
    local delay="${4:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        local state
        state=$(ssh $SSH_OPTS "ec2-user@${ip}" "systemctl is-active $svc 2>/dev/null || true" 2>/dev/null)
        if [[ "$state" != "active" ]]; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

wait_for_active() {
    local ip="$1" svc="$2"
    local max_retries="${3:-30}"
    local delay="${4:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        local state
        state=$(ssh $SSH_OPTS "ec2-user@${ip}" "systemctl is-active $svc 2>/dev/null || true" 2>/dev/null)
        if [[ "$state" == "active" ]]; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# wait_for_etcd <ip> [max_retries]
# Waits for local etcd endpoint to respond healthy.
wait_for_etcd() {
    local ip="$1"
    local max_retries="${2:-30}"
    local delay=2
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" \
            "sudo ETCDCTL_API=3 etcdctl \
              --endpoints=https://localhost:2379 \
              --cacert=/etc/etcfuse/ca.crt \
              --cert=/etc/etcfuse/client.crt \
              --key=/etc/etcfuse/client.key \
              endpoint health 2>&1" | grep -q "is healthy"; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# wait_for_fuse_mount <ip> [max_retries]
# Waits for the EtcFS FUSE mount to appear.
wait_for_fuse_mount() {
    local ip="$1"
    local max_retries="${2:-60}"
    local delay="${3:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# detect_ebs_dev <ip> — find the shared EBS block device on a node
detect_ebs_dev() {
    local ip="$1"
    local vol_id
    vol_id=$(state_get volume_id)
    $SSH_CMD "ec2-user@${ip}" "
VOL_SERIAL=\${vol_id//-/}
EBS_DEV=\$(lsblk -J -o NAME,SERIAL 2>/dev/null | python3 -c \"
import sys, json
data = json.load(sys.stdin)
for dev in data.get('blockdevices', []):
    if dev.get('serial','') == '\$VOL_SERIAL':
        print('/dev/' + dev['name'])
        break
\" 2>/dev/null || true)
if [[ -z \"\$EBS_DEV\" ]]; then
    for dev in /dev/nvme1n1 /dev/sdf /dev/xvdf; do
        [[ -b \$dev ]] && EBS_DEV=\$dev && break
    done
fi
echo \"\$EBS_DEV\"
"
}

# etcdctl_on <ip> <args...> — run etcdctl on a node.
#
# Plain http, no client cert: every cluster this repo's scripts create is
# TLS-less now (see bootstrap-cluster.sh's header for why) and reachable
# only inside the VPC's own security group.
etcdctl_cmd() {
    local ip="$1"; shift
    ssh $SSH_OPTS "ec2-user@${ip}" -- \
        "sudo ETCDCTL_API=3 etcdctl \
          --endpoints=http://localhost:2379 $*" 2>/dev/null
}

# remote_cmd <ip> <cmd...> — run command on a node
remote_cmd() {
    local ip="$1"; shift
    ssh $SSH_OPTS "ec2-user@${ip}" -- "$*"
}

# ---- Init ----
state_init
