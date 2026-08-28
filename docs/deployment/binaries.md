# Binaries and containers

Every push to `main` that semantic-release decides is a release runs a
`release-assets` CI job against the tag it just created
(`.github/workflows/ci.yml`). It builds all three binaries, packages them,
and publishes:

- `etcfuse-<version>-linux-amd64.tar.gz`, `etcfuse-meta-<version>-linux-amd64.tar.gz`,
  `etcfsctl-<version>-linux-amd64.tar.gz`
- `etcfuse_<version>_amd64.deb`, `etcfuse-meta_<version>_amd64.deb`, `etcfsctl_<version>_amd64.deb`
- `etcfuse-<version>-1.x86_64.rpm`, `etcfuse-meta-<version>-1.x86_64.rpm`, `etcfsctl-<version>-1.x86_64.rpm`
- `checksums.txt` (sha256, one line per artifact above)

as assets on the GitHub release, and pushes three container images to
`ghcr.io/etcfs/etcfuse`, `ghcr.io/etcfs/etcfuse-meta` and
`ghcr.io/etcfs/etcfs-csi` (the [CSI driver](kubernetes-csi.md))
(tagged `<version>` and `latest`). `etcfsctl` has no image — it's a client
tool, not something you run as a service.

## Install from a release

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/etcfs/etcfs/releases/latest | jq -r .tag_name | tr -d v)

curl -fsSLO "https://github.com/etcfs/etcfs/releases/download/v${VERSION}/checksums.txt"
curl -fsSLO "https://github.com/etcfs/etcfs/releases/download/v${VERSION}/etcfuse-meta_${VERSION}_amd64.deb"
sha256sum --ignore-missing -c checksums.txt

sudo apt install ./etcfuse-meta_${VERSION}_amd64.deb
```

`.deb`/`.rpm` for `etcfuse` and `etcfuse-meta` install the binary to
`/usr/local/bin/` and the matching unit from `deploy/systemd/` to
`/usr/lib/systemd/system/`, then `systemctl daemon-reload`. Neither package
enables or starts the service — both need cluster-specific flags filled in
first. See [Configuration](configuration.md).

`etcfsctl`'s package is the binary only, no unit — it's invoked directly
against a running cluster's etcd endpoints.

RPM-based distros: `sudo dnf install ./etcfuse-meta-${VERSION}-1.x86_64.rpm`.

## Containers

```bash
docker pull ghcr.io/etcfs/etcfuse-meta:latest
docker pull ghcr.io/etcfs/etcfuse:latest
```

Built from `deploy/docker/Dockerfile.etcfuse-meta` and
`deploy/docker/Dockerfile.etcfuse` — see those files for the multi-stage
build (Go image / Amazon Linux 2023 + `fuse3-devel`, each copied into a
minimal runtime base). `docker compose` (`deploy/docker/docker-compose.yml`)
builds these locally rather than pulling — point it at the published tags
instead by replacing each service's `build:` block with `image:` if you want
compose to use a released version.

## Package specs and local testing

`packaging/nfpm/{etcfuse,etcfuse-meta,etcfsctl}.yaml` — one [nfpm](https://nfpm.goreleaser.com)
spec per binary; `packaging/nfpm/scripts/postinstall.sh` (shared by the two
daemon packages) just reloads the systemd unit cache.

Build a package locally without CI:

```bash
mkdir -p dist
CGO_ENABLED=0 go build -o dist/etcfuse-meta ./cmd/etcfuse-meta
VERSION=0.0.0-dev nfpm package -f packaging/nfpm/etcfuse-meta.yaml -p deb -t dist/
```

## Building from source

```bash
make all      # bin/etcfuse, bin/etcfuse-meta, bin/etcfsctl
make check    # lint + test — CI's entry point
```

Needs `libfuse3-dev`/`fuse3-devel` for the C daemon; the Go binaries have no
external build dependency (`CGO_ENABLED=0`, statically linked). See
`Makefile` for the full target list.
