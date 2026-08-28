#!/bin/bash
# fencing-iam.sh — create the permanent IAM role/instance profile EtcFS nodes
# run under. Not per-cluster, not torn down: create-infra.sh attaches this
# same role to every instance it launches, every run.
#
# External fencing is only dual-confirmed when the daemon can call
# ec2:DetachVolume and ec2:DescribeVolumes. Without an instance profile the
# controller degrades to single-signal fencing (generation bump on lease
# expiry, no detach) — stops a fenced node publishing metadata, does not stop
# it writing bytes to the device.
#
# Idempotent: safe to run when the role already exists (create-infra.sh calls
# it on every provision).
#
# Usage:
#   ./fencing-iam.sh create
#   ./fencing-iam.sh profile-name
set -uo pipefail

ROLE="etcfs-nodes"
PROFILE="etcfs-nodes"

ACTION="${1:-}"
[[ "$ACTION" == "create" || "$ACTION" == "profile-name" ]] || {
    echo "usage: $0 create|profile-name"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1"; }

[[ "$ACTION" == "profile-name" ]] && { echo "$PROFILE"; exit 0; }

TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# What an EtcFS node needs, current + near-term. EC2-only, no IAM, no S3
# (etcd binaries come from GitHub releases, not S3), no CloudWatch (metrics
# are self-hosted Prometheus) — added when something actually needs them,
# not speculatively.
#
# DescribeVolumes/DescribeVolumeStatus/DescribeInstances/DescribeTags cannot
# be resource-scoped: AWS does not support resource-level permissions for
# these Describe* actions, so "*" here is an API limitation, not a choice.
# They are read-only regardless.
read -r -d '' POLICY <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FencingDetachReattach",
      "Effect": "Allow",
      "Action": ["ec2:DetachVolume", "ec2:AttachVolume"],
      "Resource": ["arn:aws:ec2:*:*:volume/*", "arn:aws:ec2:*:*:instance/*"]
    },
    {
      "Sid": "AsgClusterFormationElection",
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:GetItem"],
      "Resource": "arn:aws:dynamodb:*:*:table/*-etcfs-seed"
    },
    {
      "Sid": "VolumeAndPeerVisibility",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVolumes",
        "ec2:DescribeVolumeStatus",
        "ec2:DescribeInstances",
        "ec2:DescribeTags"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# What a pacemaker-managed GFS2 cluster needs to fence a peer for real.
# fence_aws stops the dead node's instance through the EC2 API, which is the
# confirmation DLM waits for before it releases that node's lockspace and
# replays its journal — without it the survivors' lockspace stops for good and
# there is no recovery time to measure. Only the comparison suite's GFS2 runs
# use this; an EtcFS node fences by detaching the volume and never calls it.
#
# Deliberately narrower than the fence agent would accept: StopInstances and
# StartInstances only, and only on instances carrying this harness's own
# ClusterName tag, so a misconfigured pcmk_host_map cannot reach an instance
# the benchmark did not create. RebootInstances is not granted — the STONITH
# resource is configured with the "off" action, which is the correct one for
# fencing anyway, since a rebooted node may come back before its journal has
# been replayed.
read -r -d '' STONITH_POLICY <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Gfs2StonithPowerControl",
      "Effect": "Allow",
      "Action": ["ec2:StopInstances", "ec2:StartInstances"],
      "Resource": "arn:aws:ec2:*:*:instance/*",
      "Condition": {
        "StringLike": {"aws:ResourceTag/ClusterName": "compare-*"}
      }
    },
    {
      "Sid": "Gfs2StonithStatus",
      "Effect": "Allow",
      "Action": ["ec2:DescribeInstanceStatus"],
      "Resource": "*"
    }
  ]
}
EOF

if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    log "role $ROLE already exists"
else
    log "creating role $ROLE..."
    aws iam create-role --role-name "$ROLE" \
        --assume-role-policy-document "$TRUST" \
        --description "Permanent role for EtcFS EC2 nodes (fencing detach/reattach, peer visibility)" \
        --query 'Role.Arn' --output text || exit 1
fi

log "putting inline policy on $ROLE (idempotent, overwrites to latest)..."
aws iam put-role-policy --role-name "$ROLE" \
    --policy-name etcfs-node-permissions --policy-document "$POLICY" || exit 1

# SSM registration, not fencing: lets the graceful-leave Lambda (ASG scale-in
# path, etcfs-terraform-modules' terraform/modules/etcfs-asg) reach a
# *surviving* node via SSM
# Run Command to issue `etcd member remove` for the node being terminated.
# AWS-managed policy, not hand-rolled, since it also covers the SSM agent's
# own connectivity requirements (ssmmessages:*, ec2messages:*) which change
# across agent versions.
log "putting the GFS2 STONITH policy on $ROLE (idempotent)..."
aws iam put-role-policy --role-name "$ROLE" \
    --policy-name gfs2-stonith-permissions --policy-document "$STONITH_POLICY" || exit 1

log "attaching AmazonSSMManagedInstanceCore to $ROLE..."
aws iam attach-role-policy --role-name "$ROLE" \
    --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore || exit 1

if aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1; then
    log "instance profile $PROFILE already exists"
else
    log "creating instance profile $PROFILE..."
    aws iam create-instance-profile --instance-profile-name "$PROFILE" \
        --query 'InstanceProfile.Arn' --output text || exit 1
    log "waiting for IAM propagation before attaching role..."
    sleep 10
fi

if aws iam get-instance-profile --instance-profile-name "$PROFILE" \
     --query 'InstanceProfile.Roles[].RoleName' --output text 2>/dev/null | grep -qw "$ROLE"; then
    log "role already attached to profile"
else
    log "attaching role to instance profile..."
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$PROFILE" --role-name "$ROLE" || exit 1
    # IAM is eventually consistent: RunInstances can reject a profile whose
    # role attachment just landed. Wait rather than let provisioning fail
    # with a confusing "Invalid IAM Instance Profile name".
    log "waiting for IAM propagation..."
    sleep 10
fi

log "done — instance profile: $PROFILE"
