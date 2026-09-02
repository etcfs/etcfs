# EtcFS

[![CI](https://github.com/etcfs/etcfs/actions/workflows/ci.yml/badge.svg)](https://github.com/etcfs/etcfs/actions/workflows/ci.yml)
[![CodeFactor](https://www.codefactor.io/repository/github/etcfs/etcfs/badge)](https://www.codefactor.io/repository/github/etcfs/etcfs)
[![codecov](https://codecov.io/gh/etcfs/etcfs/branch/main/graph/badge.svg)](https://codecov.io/gh/etcfs/etcfs)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://etcfs.github.io/etcfs-docs/)

**A cluster-aware filesystem for shared raw block devices — the piece AWS and Kubernetes tell you to bring yourself.**

AWS EBS Multi-Attach will attach one io2 volume to sixteen instances at once. Kubernetes will hand it to you as a `ReadWriteMany` `volumeMode: Block` volume. Both then stop, and [the EBS CSI driver's documentation says why](https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/multi-attach.md): using it safely "requires application-level coordination (e.g. via I/O fencing)", and failure to do so "can result in data loss and silent data corruption". Using ext4 on a Multi-Attach volume and mount it twice and it will get corrupted. The platform gives you the shared device and declines to make it safe.

EtcFS is what goes on top. **etcd/Raft is the only source of durable truth**, and the shared device holds nothing but file bytes. No on-disk filesystem format, no kernel module, no bespoke distributed lock manager — a userspace FUSE daemon on each node presents POSIX semantics, backed by etcd for everything structural (namespace, inode metadata, locks, allocation) and direct block I/O for file content. The I/O fencing that AWS says you need is three independent layers, one of them enforced by the drive itself — see [Fencing](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fencing/self-fencing-watchdog.md).

Traditional cluster filesystems (GFS2, OCFS2) keep durable truth *on disk* — inodes, bitmaps, a journal — and bolt a distributed lock manager on top to arbitrate access to it. EtcFS inverts that: etcd's replicated Raft log *is* the durable truth for every structural fact, and the disk is demoted to a flat, unformatted array of bytes addressed by extents `(logical_offset, disk_offset, length)` recorded in etcd. Atomicity, consistency, and metadata recovery come from etcd's existing quorum-replicated log almost for free, instead of a bespoke recovery protocol — at the cost of every structural operation being an etcd round trip, mitigated by client-side caching and keeping the hot data path (reads/writes to already-allocated extents) on direct block I/O with no etcd round trip at all.

Status: implemented and under hardening. Cross-node `fcntl`/`flock` advisory
locks are the one accepted-but-unenforced POSIX surface — see
[Consistency and durability](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/consistency/consistency-and-durability-model.md)
before relying on this for real data.

## What that inversion buys

Measured against GFS2, GlusterFS, self-hosted NFS and JuiceFS, each on its own
isolated AWS cluster, all in one session. Full ledger — including every scenario
EtcFS loses — in the
[benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md).

- **Recovery with no fence device and no journal replay.** A node is powered off
  mid-write holding locks; a survivor takes over its file in **2.19 s** and never
  stops serving its own I/O. **10.3x faster than GlusterFS** — and GFS2, NFS and
  JuiceFS never recovered inside 180 s, GFS2's survivors stopping entirely until
  an external STONITH device confirms the kill.
- **Elastic membership, no stop-the-world.** A node leaving or joining under load
  stalls the others **0.09–0.11 s** and costs them 7.2% / 11.3% of bandwidth —
  about **half GFS2's cost**, which suspends its DLM lockspace on every
  membership change. Three leave/rejoin cycles under load: 3.02%.
- **A far tighter tail.** At the same random-write throughput as the kernel
  filesystems, p99 write latency is **24x better than GFS2**, 16x better than
  GlusterFS, 13.6x better than NFS; read p99 **21.9x better than GFS2**.
- **Scales where the directory lock stops others.** Over a 2 → 6 node sweep
  shared-directory metadata throughput climbs **+34%** while GFS2 **loses 47%**
  of its own; disjoint-workload bandwidth climbs 253 → 282 MiB/s.
- **Handoff at device speed.** A file written on one node reads back on another
  at **255.95 MiB/s** against a 254.14 MiB/s raw-device ceiling, with
  time-to-first-byte flat at 69–112 ms from 1 MiB to 8 GiB — only the extent map
  crosses the network.
- **A coherent page cache that is free to read through.** 600,852 IOPS on a
  RAM-resident set with **zero reads reaching the daemon**, while the pages stay
  under the inode's lock and are invalidated before it is yielded.
- **Grow the volume under a running cluster.** New space allocatable in
  **3.90 s**, no restart and no remount anywhere.
- **Correctness checked by tools EtcFS did not write.** **8,787/8,787**
  pjdfstest POSIX assertions pass; Porcupine finds every recorded history
  linearizable against four models (**20/20** chaos assertions, 7/7 model runs);
  the fencing protocol is TLA+ model-checked to **11.7 M states**, with four
  deliberately broken variants producing counterexamples, and runs in CI.

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

## How it measures up

Five filesystems, each on its own isolated AWS cluster with a dedicated
1000-IOPS io2 Multi-Attach volume — `scripts/bench/compare/`. Every comparison
below was measured in one session on 2026-08-24/25. Full method, caveats and the
scenario-by-scenario ledger of wins and losses:
[Benchmark overview](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/overview.md).

**Where EtcFS wins — losing a node.** All five backends take one identical
fault: the victim machine is powered off, nothing releases a lock, nothing runs
an exit path. The number that matters is how long a survivor takes to write a
file the dead node held the lock on.

| Backend | takes over the dead node's file | survivor's own I/O |
|---|---|---|
| **EtcFS** | **2.19 s** | never stopped (0.11 s worst gap) |
| gluster | 22.64 s | continued, 78 s worst gap |
| gfs2 | never, inside 180 s | **stopped and stayed stopped** |
| nfs | never | client hung indefinitely |
| juicefs | never | 3,356 errors, no recovery |

GFS2's DLM lockspace goes to `kern_stop` / "wait fencing" and stops granting
locks to *anyone* on the surviving node until a fence device confirms the kill —
this harness configures none, which is a caveat the
[report](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/node-kill-recovery.md)
states plainly.

**Where EtcFS wins — the latency tail.** Per competitor, at effectively the same
random-write throughput (934 IOPS against gluster's 1041 and gfs2's 973):

| vs. | their p99 write | EtcFS advantage |
|---|---|---|
| gfs2 | 432.1 ms | **24x better** |
| gluster | 288.8 ms | **16x better** |
| nfs | 244.3 ms | **13.6x better** |
| juicefs | 61.1 ms | 3.4x better, at 2.4x its throughput |

**Where EtcFS loses.** Everything that costs a Raft commit:

| Case | EtcFS | Best competitor | Deficit |
|---|---|---|---|
| 80k-file untar | 2244 s | 29.8 s (gfs2) | **75x slower** |
| Shared-directory metadata, 3 nodes | 327 ops/s | 1515 ops/s (gfs2) | **4.6x slower** |
| O_DSYNC 4 KiB writes | 155 IOPS | 989 (gfs2) | **6.4x slower** |
| `du -s` over 80k files | 128 s | 0.41 s (nfs) | **312x slower** |

One Raft commit per structural mutation is the standing cost of putting durable
truth in etcd, and it is not a rounding error. It does come down: taking the
inode-number reservation and the parent directory's timestamp off the per-file
path moved the first two rows from 112x and 8.4x. Two more have since gone — the
inode's lock key rides the create transaction, and evicted keys are released a
batch at a time — so a created-and-written file costs two commits and a fraction
where it cost six. The untar row does not move with them: that scenario is no
longer commit-bound, and the remaining effect is smaller than the benchmark's
own spread ([why](https://github.com/etcfs/etcfs-docs/blob/main/docs/reports/benchmark-reports/smallfile-storm.md)).

It does not come down to nothing — the transaction that publishes a name commits
before `create()` returns, and
[deferring that is a wrong answer rather than a durability trade](https://github.com/etcfs/etcfs-docs/blob/main/docs/design-decisions.md#creates-are-not-deferred-into-a-batch).

**Where it makes no difference.** A warm page cache: all five converge on
530–620k IOPS on a RAM-resident working set, which is RAM, not a filesystem.
EtcFS reaches it while holding data pages only under the inode's lock (zero
reads reached the daemon across the warm pass), so the coherence obligation is
free here — but it is not an advantage.

**Where the caches earn their keep.** A missing name costs 110 us cold and
2.21 us warm — second and first of the five respectively, against 1474 us and
8.81 us before the directory name-set prefetch and a one-minute entry timeout
backed by a cluster-wide watch. gluster and juicefs do not cache absences at
all.

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
