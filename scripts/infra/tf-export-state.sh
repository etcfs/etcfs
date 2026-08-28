#!/bin/bash
# tf-export-state.sh — write infra-state.json from the Terraform module's
# outputs, so everything downstream of create-infra.sh works unchanged against
# a Terraform-provisioned cluster.
#
# The Terraform path replaces create-infra.sh, destroy-infra.sh and
# fencing-iam.sh — not bootstrap-cluster.sh. Installing etcd, compiling the
# binaries on-node, starting one fresh etcd cluster on every node at once and
# `etcd member add` for a later join are ordered, imperative steps; expressing
# them as Terraform provisioners would re-create exactly the "re-run against a
# partially-up cluster" bug class bootstrap-cluster.sh's header documents
# having already been fixed once.
#
# So: Terraform owns the infrastructure, this hands its outputs to the bash
# tooling in the shape scripts/infra/state.sh already reads.
#
# Usage:
#   ./tf-export-state.sh                 # -> $ETCFS_STATE (default infra-state.json)
#   ETCFS_STATE=/tmp/x.json ./tf-export-state.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TF_DIR="${ETCFS_TF_DIR:-$PROJECT_ROOT/../etcfs-terraform-modules/terraform}"
STATE_FILE="${ETCFS_STATE:-$PROJECT_ROOT/infra-state.json}"

[[ -d "$TF_DIR" ]] || { echo "ERROR: terraform dir not found: $TF_DIR" >&2; exit 1; }

# terraform output exits non-zero on an empty state, which is the common
# "you have not applied yet" mistake — say so rather than leaking its message.
OUT=$(terraform -chdir="$TF_DIR" output -raw infra_state 2>/dev/null) || {
    echo "ERROR: no infra_state output in $TF_DIR — run 'terraform -chdir=$TF_DIR apply' first" >&2
    exit 1
}

# Round-trip through jq: a truncated or partially-rendered output must fail
# here rather than land on disk as an unparseable state file that every
# downstream script then reports as "no compute nodes".
echo "$OUT" | jq . > "$STATE_FILE"

echo "Wrote $STATE_FILE"
jq -r '"  cluster: \(.cluster_name)  nodes: \(.compute_public_ips | length)  volume: \(.volume_id)"' "$STATE_FILE"
