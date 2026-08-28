#!/bin/bash
# tf-up.sh — provision an EtcFS cluster with Terraform and bring the software
# up on it: the Terraform equivalent of `create-infra.sh && setup-compute.sh`.
#
# The Terraform itself lives in a sibling checkout of
# https://github.com/etcfs/etcfs-terraform-modules — set ETCFS_TF_DIR if
# yours is not at ../etcfs-terraform-modules/terraform.
#
# Three steps, each usable on its own:
#   1. terraform init + apply        ($TF_DIR)
#   2. tf-export-state.sh            (outputs -> infra-state.json)
#   3. bootstrap-cluster.sh          (etcd + both daemons on every node)
#
# Teardown is `terraform -chdir=$TF_DIR destroy` — destroy-infra.sh is
# for the bash-provisioned path and reads a state file Terraform owns here.
#
# Usage:
#   ./tf-up.sh                   # apply, export state, bootstrap
#   ./tf-up.sh --no-bootstrap    # infra only
#   ./tf-up.sh -- -var node_count=5    # anything after -- goes to terraform apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TF_DIR="${ETCFS_TF_DIR:-$PROJECT_ROOT/../etcfs-terraform-modules/terraform}"
STATE_FILE="${ETCFS_STATE:-$PROJECT_ROOT/infra-state.json}"

BOOTSTRAP=true
TF_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-bootstrap) BOOTSTRAP=false; shift ;;
        --) shift; TF_ARGS=("$@"); break ;;
        *) echo "unknown argument: $1 (use -- to pass terraform args)" >&2; exit 1 ;;
    esac
done

log() { echo "[$(date +%T)] $*"; }

log "=== terraform init ($TF_DIR) ==="
terraform -chdir="$TF_DIR" init -input=false

log "=== terraform apply ==="
terraform -chdir="$TF_DIR" apply -input=false -auto-approve "${TF_ARGS[@]}"

log "=== exporting state to $STATE_FILE ==="
ETCFS_STATE="$STATE_FILE" ETCFS_TF_DIR="$TF_DIR" "$SCRIPT_DIR/tf-export-state.sh"

if [[ "$BOOTSTRAP" != "true" ]]; then
    log "=== infra up, bootstrap skipped ==="
    log "Next: ./scripts/infra/bootstrap-cluster.sh $STATE_FILE"
    exit 0
fi

log "=== bootstrapping cluster software ==="
bash "$SCRIPT_DIR/bootstrap-cluster.sh" "$STATE_FILE"

log ""
log "=== EtcFS cluster up ==="
log "State file: $STATE_FILE"
log "Next:       ./scripts/infra/run-full-test.sh"
log "Teardown:   terraform -chdir=$TF_DIR destroy"
