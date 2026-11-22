<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/headline-dark.png">
    <img src="assets/headline-light.png" alt="EtcFS" width="50%">
  </picture>
</p>

<h1 align="center">EtcFS</h1>

<p align="center">
  <a href="https://github.com/etcfs/etcfs/actions/workflows/ci.yml"><img src="https://github.com/etcfs/etcfs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.codefactor.io/repository/github/etcfs/etcfs"><img src="https://www.codefactor.io/repository/github/etcfs/etcfs/badge" alt="CodeFactor"></a>
  <a href="https://codecov.io/gh/etcfs/etcfs"><img src="https://codecov.io/gh/etcfs/etcfs/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="https://etcfs.github.io/etcfs-docs/"><img src="https://img.shields.io/badge/docs-mkdocs-blue" alt="Docs"></a>
</p>

<p align="center"><b>A cluster filesystem for shared raw block devices, with all metadata in etcd.</b></p>

AWS EBS Multi-Attach and Kubernetes `ReadWriteMany` block volumes let several
machines write to one disk, but nothing coordinates them. Mount ext4 on two
nodes at once and the volume is corrupted.

EtcFS is that coordination layer. A FUSE daemon on each node keeps all
metadata (namespace, inodes, locks, allocation) in etcd and writes file bytes
straight to the shared device. There is no on-disk format, no kernel module
and no separate lock manager; I/O fencing has
[three independent layers](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fencing/self-fencing-watchdog.md).

> **Status:** implemented and under hardening. Cross-node `fcntl`/`flock` locks
> are not yet enforced between nodes. Read
> [Consistency and durability](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/consistency/consistency-and-durability-model.md)
> before trusting it with real data.

## Highlights

Measured against GFS2, GlusterFS, NFS and JuiceFS, each on its own AWS cluster
([benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md)).

- **Node loss:** a survivor takes over a dead node's file in **3.0–3.5 s**; GFS2, NFS and JuiceFS never recover it within 180 s.
- **Membership changes:** a node joining or leaving stalls the others for **~0.1 s**.
- **Scaling:** shared-directory metadata throughput rises **79%** from two to six nodes, while GFS2's falls 44%.
- **Online growth:** new space is allocatable in **3.9 s**, with no remount.
- **Verified:** 8,789/8,789 pjdfstest assertions, linearizable histories under chaos (Porcupine), and TLA+-checked fencing.

## Quick start

Requires Go 1.24+, a C11 compiler, `libfuse3-dev`.

```bash
make all      # bin/etcfuse-meta (Go), bin/etcfuse (C), bin/etcfsctl
make check    # lint + test — also wired as a pre-push git hook (make hooks)
```

A full 3-node cluster on one machine, no cloud account needed:

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build
# FUSE mount at /mnt/etcfuse inside each etcfuse<N> container
docker compose -f deploy/docker/docker-compose.yml down -v
```

Or install a released binary/package/container and provision real
infrastructure with the Terraform module — see
**[Deployment](https://github.com/etcfs/etcfs-docs/blob/main/docs/deployment/index.md)**.

```bash
etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=http://127.0.0.1:2379 \
  --node-id=n1 --cluster-name=my-cluster --lease-ttl=10s --block-device=/dev/nvme1n1
etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 /mnt/etcfuse

etcfsctl --etcd-endpoints=http://127.0.0.1:2379 status
```

Full flag reference and every config knob:
[Configuration](https://github.com/etcfs/etcfs-docs/blob/main/docs/deployment/configuration.md).

## Comparison

Each filesystem runs on its own AWS cluster with a 1000-IOPS io2 Multi-Attach
volume. Method and caveats are in the
[benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md).

**Losing a node** (machine powered off mid-write, holding locks):

| Backend | takes over the dead node's file | survivor's own I/O |
| --- | --- | --- |
| **EtcFS** | **3.0–3.5 s** (8 runs) | kept writing (≤1.5 s worst gap) |
| gluster | 22.3–22.7 s (3 runs) | kept writing, 77–78 s worst gap |
| gfs2 + `fence_aws` | 15.9–21.7 s (4 runs) | paused 12–17 s until the fence was confirmed |
| nfs | never | client hung on the hard mount |
| juicefs | never (`t3.medium`) | 3,356 errors, no recovery |

**Where EtcFS is slower:** anything that costs a Raft commit or a FUSE round trip.

| Case | EtcFS | Best competitor | Ratio |
| --- | --- | --- | --- |
| 80k-file untar | 1452.5 s | 18.9 s (gfs2) | **77x slower** |
| Shared-directory metadata, 3 nodes | 405 ops/s | 2063 ops/s (gfs2) | **5.1x slower** |
| 4 KiB random write p99, matched load | 12.4 ms | 8.0 ms (gfs2) | **1.5x higher** |
| O_DSYNC 4 KiB writes | 155 IOPS | 989 (gfs2) | **6.4x slower** |
| `du -s` over 80k files | 128 s | 0.41 s (nfs) | **312x slower** |

The [small-file storm report](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/smallfile-storm.md)
breaks down where the untar's time goes.

## Documentation

The [documentation site](https://etcfs.github.io/etcfs-docs/) covers everything
beyond this quick start:

| | |
| --- | --- |
| **[Deployment](https://github.com/etcfs/etcfs-docs/blob/main/docs/deployment/index.md)** | Terraform module, binaries/containers, configuration, `etcfsctl`, Prometheus + Grafana |
| **[Architecture](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fuse/fuse-architecture.md)** | FUSE layer, metadata model, storage substrate, consistency, fencing, reliability, cluster ops — one doc per subsystem |
| **[Reports](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/chaos-reports/fresh-cluster-per-scenario.md)** | Chaos-testing and benchmark results, by date |
| **[Background](https://github.com/etcfs/etcfs-docs/blob/main/docs/background/etcd_raft_research.md)** | Research behind the design decisions: etcd/Raft internals, cluster-FS survey, VFS/FUSE, userspace FS patterns |

## Related repositories

- [etcfs-csi-driver](https://github.com/etcfs/etcfs-csi-driver) — Kubernetes CSI driver
- [etcfs-terraform-modules](https://github.com/etcfs/etcfs-terraform-modules) — Terraform for provisioning clusters on AWS
- [etcfs-tla-specs](https://github.com/etcfs/etcfs-tla-specs) — TLA+ models for the fencing and cached-lock protocols
- [etcfs-docs](https://github.com/etcfs/etcfs-docs) — this documentation site's source

## Contributing

- [`CONTRIBUTING.md`](CONTRIBUTING.md): build and test setup, commit convention
- [`SECURITY.md`](SECURITY.md): reporting a vulnerability privately
- [`AGENTS.md`](AGENTS.md): conventions for AI agents working in this repo
