#!/bin/bash
# eks-build-push.sh — builds and pushes the two images this repo owns
# (etcfuse-meta, etcfuse) to an ECR repository, and prints the -var flags to
# pass straight to `terraform apply` in etcfs-terraform-modules. The CSI
# driver image is built and published (to ghcr.io/etcfs/etcfs-csi, which is
# public) by etcfs-csi-driver's own CI — pass its tag as csi_image_tag
# directly, or mirror it into your own ECR the same way this script does.
#
# The Terraform module takes image references as required variables rather
# than defaulting to a public registry: a registry is account-specific, and a
# default that does not exist in the caller's account would fail opaquely at
# pod scheduling time instead of at `terraform plan`.
#
# Usage:
#   ./scripts/infra/eks-build-push.sh [ecr-repo-prefix] [region]
#   # defaults: etcfs, us-east-1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PREFIX="${1:-etcfs}"
REGION="${2:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
ECR="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
VERSION="$(git -C "$PROJECT_ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)"

log() { echo "[$(date +%T)] $*"; }

for repo in "$PREFIX/etcfuse-meta" "$PREFIX/etcfuse"; do
    aws ecr describe-repositories --region "$REGION" --repository-names "$repo" >/dev/null 2>&1 \
        || aws ecr create-repository --region "$REGION" --repository-name "$repo" >/dev/null
done

log "=== docker login: $ECR ==="
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR"

log "=== building etcfuse-meta ==="
docker build --network=host -f "$PROJECT_ROOT/deploy/docker/Dockerfile.etcfuse-meta" \
    -t "$ECR/$PREFIX/etcfuse-meta:$VERSION" --build-arg "VERSION=$VERSION" "$PROJECT_ROOT"
docker push "$ECR/$PREFIX/etcfuse-meta:$VERSION"

log "=== building etcfuse ==="
docker build --network=host -f "$PROJECT_ROOT/deploy/docker/Dockerfile.etcfuse" \
    -t "$ECR/$PREFIX/etcfuse:$VERSION" "$PROJECT_ROOT"
docker push "$ECR/$PREFIX/etcfuse:$VERSION"

log ""
log "=== done ==="
log "terraform -chdir=terraform-eks apply \\"
log "  -var etcfuse_meta_image=$ECR/$PREFIX/etcfuse-meta:$VERSION \\"
log "  -var etcfuse_image=$ECR/$PREFIX/etcfuse:$VERSION \\"
log "  -var csi_image_repository=ghcr.io/etcfs/etcfs-csi \\"
log "  -var csi_image_tag=<version from etcfs-csi-driver's own releases> \\"
log "  -var csi_chart_version=<same version>"
