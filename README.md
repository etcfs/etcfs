# EtcFS

[![CI](https://github.com/etcfs/etcfs/actions/workflows/ci.yml/badge.svg)](https://github.com/etcfs/etcfs/actions/workflows/ci.yml)
[![CodeFactor](https://www.codefactor.io/repository/github/etcfs/etcfs/badge)](https://www.codefactor.io/repository/github/etcfs/etcfs)
[![codecov](https://codecov.io/gh/etcfs/etcfs/branch/main/graph/badge.svg)](https://codecov.io/gh/etcfs/etcfs)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://etcfs.github.io/etcfs-docs/)

**A cluster filesystem for shared raw block devices, with all metadata in etcd.**

AWS EBS Multi-Attach attaches one io2 volume to up to sixteen instances, and Kubernetes exposes it as a `ReadWriteMany`, `volumeMode: Block` volume. Neither coordinates the writers. [The EBS CSI driver's documentation](https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/multi-attach.md) says that using such a volume safely "requires application-level coordination (e.g. via I/O fencing)", and that without it the result "can result in data loss and silent data corruption". An ordinary filesystem such as ext4, mounted on two instances at once, corrupts the volume.

EtcFS provides that coordination. etcd's Raft log holds every durable structural fact (namespace, inode metadata, locks, allocation), and the shared device holds only file bytes. There is no on-disk filesystem format, no kernel module and no separate distributed lock manager: a FUSE daemon on each node presents POSIX semantics, reads and writes metadata in etcd, and reads and writes file content directly on the block device. I/O fencing has three independent layers, one of them enforced by the device itself; see [Fencing](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fencing/self-fencing-watchdog.md).

GFS2 and OCFS2 keep durable state on the disk (inodes, bitmaps, a journal) and use a distributed lock manager to arbitrate access to it. EtcFS keeps that state in etcd instead, and the disk is an unformatted array of bytes addressed by extents `(logical_offset, disk_offset, length)` recorded in etcd. Atomicity, consistency and metadata recovery come from etcd's replicated log rather than from a filesystem-specific recovery protocol. The cost is that every structural operation is an etcd round trip. Client-side caching reduces it, and reads and writes to already-allocated extents go to the device without contacting etcd.

Status: implemented and under hardening. Cross-node `fcntl`/`flock` advisory
locks are accepted but not enforced between nodes; read
[Consistency and durability](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/consistency/consistency-and-durability-model.md)
before relying on EtcFS for real data.

## Measured results

Measured against GFS2, GlusterFS, self-hosted NFS and JuiceFS, each on its own
isolated AWS cluster. The
[benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md)
lists every scenario, including those where EtcFS is slower.

- **Recovery without a fence device or journal replay.** A node is powered off
  mid-write while holding locks; a survivor takes over its file in **3.0–3.5 s**
  (median 3.11 s over eight runs) and keeps serving its own I/O. GlusterFS took
  22.3–22.7 s; GFS2, NFS and JuiceFS did not recover the file inside 180 s, and
  GFS2's survivors stopped even when `fence_aws` confirmed the fence.
- **Membership changes under load.** A node leaving or joining stalls the
  others for **0.08–0.11 s** and costs them 6.8% and 11.5% of bandwidth, against
  19.2% and 23.8% for GFS2, which suspends its DLM lockspace on every membership
  change. A GFS2 node with no free journal needs `gfs2_jadd` first; an EtcFS
  node needs no equivalent step.
- **Metadata throughput grows with node count.** From two nodes to six,
  shared-directory metadata throughput rises **79%** while GFS2's falls **44%**
  (GFS2 is still 1.7x faster at six nodes); disjoint write bandwidth rises from
  265 to 290 MiB/s.
- **Cross-node handoff.** A file written on one node reads back on another at
  **253.8 MiB/s** for 8 GiB, with time-to-first-byte between 58 and 81 ms from
  1 MiB to 8 GiB, because only the extent map crosses the network.
- **Coherent page cache.** 600,852 IOPS on a RAM-resident working set with
  **no reads reaching the daemon**; pages are cached while the node holds the
  inode's lock and dropped before the lock is released.
- **Growing the volume under a running cluster.** New space is allocatable in
  **3.90 s**, with no restart and no remount.
- **Correctness checked with external tools.** **8,789/8,789** pjdfstest POSIX
  assertions pass; Porcupine finds every recorded history linearizable against
  four models (**20/20** chaos assertions, 7/7 model runs); the fencing protocol
  is TLA+ model-checked to **11.7 M states**, four deliberately broken variants
  produce counterexamples, and the 2-node models run in CI.

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

Five filesystems, each on its own isolated AWS cluster with a dedicated
1000-IOPS io2 Multi-Attach volume (`scripts/bench/compare/`). The node-kill and
cost tables below are from `m5.large` runs on 2026-10-16/17; method, caveats and
the full list of scenarios are in the
[benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md).

**Losing a node.** Every backend takes the same fault: the victim machine is
powered off, so nothing releases a lock and nothing runs an exit path. The
table shows how long a survivor takes to write a file the dead node held the
lock on.

| Backend | takes over the dead node's file | survivor's own I/O |
|---|---|---|
| **EtcFS** | **3.0–3.5 s** (8 runs) | kept writing (≤1.5 s worst gap) |
| gluster | 22.3–22.7 s (3 runs) | kept writing, 77–78 s worst gap |
| gfs2 | never, inside 180 s | **stopped for the rest of the run** |
| nfs | never | client hung on the hard mount |
| juicefs | never (`t3.medium`) | 3,356 errors, no recovery |

GFS2's DLM lockspace goes to `kern_stop` / "wait fencing" and stops granting
locks on the surviving node. With a `fence_aws` STONITH device configured, the
fence was confirmed and the lockspace still stayed stopped; whether that comes
from GFS2 or from the harness's DLM–Pacemaker integration is not established
([report](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/node-kill-recovery.md)).

**Where EtcFS is slower.** Every case that costs a Raft commit or a FUSE round
trip:

| Case | EtcFS | Best competitor | Ratio |
|---|---|---|---|
| 80k-file untar | 1452.5 s | 18.9 s (gfs2) | **77x slower** |
| Shared-directory metadata, 3 nodes | 405 ops/s | 2063 ops/s (gfs2) | **5.1x slower** |
| 4 KiB random write p99, matched load | 12.4 ms | 8.0 ms (gfs2) | **1.5x higher** |
| O_DSYNC 4 KiB writes | 155 IOPS | 989 (gfs2) | **6.4x slower** |
| `du -s` over 80k files | 128 s | 0.41 s (nfs) | **312x slower** |

A created and written file costs a little over two commits, before any
`chmod` or `chown` the application adds (`tar` adds two, for 4.18 in all): the create
transaction carries the name, the inode and the node's lock key, inode numbers
are reserved in blocks, the parent directory's timestamp is published in
batches, and evicted lock keys are released a batch at a time. The transaction
that publishes a name still commits before `create()` returns, because
[deferring it would answer `create()` before its exclusivity check has run](https://github.com/etcfs/etcfs-docs/blob/main/docs/design-decisions.md#creates-are-not-deferred-into-a-batch).
The [small-file storm report](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/smallfile-storm.md)
breaks down where the untar's time goes.

**Where the backends tie.** On a warm page cache all five reach 530–620k IOPS
on a RAM-resident working set, the speed of memory. EtcFS gets there while
caching pages only under the inode's lock, with no reads reaching the daemon
during the warm pass.

**Negative lookups.** A missing name costs 110 µs cold and 2.21 µs warm,
second and first of the five. EtcFS answers misses from the directory's
prefetched name set and keeps cached entries for a minute, invalidated by a
cluster-wide watch; gluster and juicefs do not cache absences.

## Documentation

The [documentation site](https://etcfs.github.io/etcfs-docs/) covers everything
beyond this quick start:

| | |
|---|---|
| **[Deployment](https://github.com/etcfs/etcfs-docs/blob/main/docs/deployment/index.md)** | Terraform module, binaries/containers, configuration, `etcfsctl`, Prometheus + Grafana |
| **[Architecture](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fuse/fuse-architecture.md)** | FUSE layer, metadata model, storage substrate, consistency, fencing, reliability, cluster ops — one doc per subsystem |
| **[Reports](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/chaos-reports/fresh-cluster-per-scenario.md)** | Chaos-testing and benchmark results, by date |
| **[Background](https://github.com/etcfs/etcfs-docs/blob/main/docs/background/etcd_raft_research.md)** | Research behind the design decisions: etcd/Raft internals, cluster-FS survey, VFS/FUSE, userspace FS patterns |

## Related repositories

- [etcfs-csi-driver](https://github.com/etcfs/etcfs-csi-driver) — Kubernetes CSI driver
- [etcfs-terraform-modules](https://github.com/etcfs/etcfs-terraform-modules) — Terraform for provisioning clusters on AWS
- [etcfs-tla-specs](https://github.com/etcfs/etcfs-tla-specs) — TLA+ models for the fencing and cached-lock protocols
- [etcfs-docs](https://github.com/etcfs/etcfs-docs) — this documentation site's source

Read the relevant subsystem doc before making a design decision that touches
fencing, the write path, or the metadata schema.

## Contributing

[`CONTRIBUTING.md`](CONTRIBUTING.md) — build/test setup, the commit
convention the release automation reads, what a change to a safety-critical
path needs. [`SECURITY.md`](SECURITY.md) — how to report a vulnerability
privately. [`AGENTS.md`](AGENTS.md) — conventions for AI agents working in
this repo.
