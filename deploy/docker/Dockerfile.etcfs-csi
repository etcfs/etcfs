# EtcFS CSI driver (nested Go module: csi/)
#
# Build:
#   docker build -f deploy/docker/Dockerfile.etcfs-csi -t etcfs-csi \
#     --build-arg VERSION=$(git describe --tags --always) .
#
# The build context is the repository root, not csi/: the nested module
# replaces github.com/etcfs/etcfs with ../, so the root module has to be
# present for the build to resolve it.

FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY . .

ARG VERSION=dev
RUN cd csi && CGO_ENABLED=0 go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /usr/local/bin/etcfs-csi ./cmd/etcfs-csi

FROM alpine:3.20

# The node plugin issues bind mounts itself, so it needs no mount helper; only
# TLS roots for the controller's etcd client.
RUN apk add --no-cache ca-certificates

COPY --from=builder /usr/local/bin/etcfs-csi /usr/local/bin/etcfs-csi

ENTRYPOINT ["/usr/local/bin/etcfs-csi"]
