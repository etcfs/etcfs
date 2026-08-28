# EtcFS — Agent Instructions

## State

**Implemented and under hardening.** Go + C codebase, `make` build, Go test suite, chaos/stress harness against real AWS infra.

Layout: `cmd/etcfuse` (C FUSE daemon) · `cmd/etcfuse-meta` (Go metadata daemon) · `pkg/{metadata,fencing,blockio,arena,scrub,fsck,fuse,block}` · `internal/{ipc,config}` · `test/harness` (Go, mock-store + simulator) · `test/etcdtest` (per-test etcd namespaces) · `scripts/{infra,test}` (AWS provisioning + chaos).

The two daemons talk over a hand-rolled length-prefixed binary protocol on a Unix socket (`internal/ipc/socket.go`). There is no gRPC/protobuf anywhere in the wire path — an earlier `proto/ipc.proto` sketch was removed as dead weight.

Every `docs/...` path referenced below lives in the sibling [etcfs-docs](https://github.com/etcfs/etcfs-docs) repo, not in this checkout — the Kubernetes CSI driver, Terraform modules, and TLA+ specs are likewise split into [etcfs-csi-driver](https://github.com/etcfs/etcfs-csi-driver), [etcfs-terraform-modules](https://github.com/etcfs/etcfs-terraform-modules), and [etcfs-tla-specs](https://github.com/etcfs/etcfs-tla-specs).

### Chaos suite status

**7 of 7 assertions pass**, most recently on the docker cluster (2026-08-10) alongside arena reclamation (6/6) and a 240 s fuzz run; and earlier on real AWS infra (`chaos-report-20260730-180644`, commit `660a14a`): S1, S2, S3 (both assertions), S5, S6, S7. Full scenario descriptions, the two product bugs that were fixed to get here (an inode-numbering collision with the FUSE root directory, and a `readdirplus` parser desync), the harness bugs found along the way, and known gaps not covered by this suite are all in `docs/reports/chaos-reports/fresh-cluster-per-scenario.md` — read that before touching `scripts/test/chaos-test.sh` or the write path. Single-cluster sequential and randomized-fuzz results are in `docs/reports/chaos-reports/single-cluster-and-fuzz.md`.

Every mutation is generation-guarded: the guard is installed on the store itself (`Store.SetGuard`), so namespace transactions carry it as well as extent commits. `docs/design-decisions.md` records the choices behind the non-obvious ones.

## Measurement traps

Written down because each of these cost real time and produced a wrong
conclusion at least once. Read before benchmarking anything.

- **Measure commits, not seconds, on the small-file storm.** Each run
  provisions its own cluster, so wall clock carries host-to-host variance:
  five runs spanned 1159–1440 s with *four of them the same build*. Anything
  below ~20% is invisible at n=1. `etcfuse_etcd_txn_origin_total` barely
  varies and attributes every commit to the operation that asked for it. If a
  wall-clock number is genuinely needed, run both builds on one cluster,
  alternating, rather than averaging separate clusters.
- **Never compare across dates or instance classes.** A 1.24x speed-up was
  published and had to be retracted because the two runs came from different
  days; the competitor tables are `t3.medium` and today's runs are
  `m7i.large`, and mixing them proves nothing.
- **Burstable instances lie.** `t3.medium` exhausts CPU credits minutes into a
  sustained untar, which once manufactured a 1.54x "regression". Use
  `m7i.large` (or `m5.large`) for long runs, one at a time, and check
  `CPUCreditBalance` afterwards. The comparison harness now enforces both
  halves: a second run refuses to start while one is in flight
  (`COMPARE_ALLOW_CONCURRENT=1` overrides), and a run on a `t*` instance
  reports the lowest credit balance CloudWatch saw before it tears the
  cluster down.
- **Instrument before predicting.** Estimating a change's saving from the
  design was wrong four times in a row on the same workload — 2.05, 4.0, 7.2
  and "batching removes one commit" all failed, while the counter got it right
  on the first run each time. Add the counter, then predict.
- **A deferral only pays if nothing else drains the queue.** Deferring
  timestamps was worth exactly nothing until two unwatched paths stopped
  publishing them one inode at a time: the synchronous `setattr` that follows
  (which flushed before writing its own change — fold it into that
  transaction instead) and `close()` (which published for durability nobody
  asked for). Check what else touches the queue before measuring.
- **Only enforcement-free fields can be deferred.** Timestamps can; mode and
  ownership cannot, because a peer checks access against what etcd holds, and
  a deferred tightening leaves that peer granting permission already taken
  away. The same rule blocks answering `getattr` from the lock's snapshot:
  `setattr` takes no lock, so a peer can rewrite those fields of an inode this
  node holds exclusively.
- **Run pjdfstest before believing a metadata change is safe.** It caught two
  regressions in one afternoon that reasoning and the unit suites did not: a
  `utimensat` reply built from etcd rather than from this node's queue, and
  `chown` returning `EAGAIN` once the timestamp sweep became a second writer of
  the inode record. The documented baseline is 0 failures.

## Document map

| Document | Purpose |
|----------|---------|
| `docs/architecture/*.md` | Per-subsystem design docs — fencing, write ordering, schema, coherence, scrubber |
| `docs/design-decisions.md` | Why the non-obvious choices were made |
| `docs/background/etcd_raft_research.md` | etcd/Raft internals research — transaction model, leases, watches, scaling limits (~1M files per cluster), watch amplification, pagination |
| `docs/background/cluster-fs-survey.md` | Cluster/distributed filesystem survey — GFS2, OCFS2, CephFS, GlusterFS, Lustre, EBS Multi-Attach failure modes, design lessons |
| `docs/background/vfs_fuse_block_research.md` | Linux VFS, FUSE protocol/operations/capabilities, O_DIRECT alignment, io_uring, design implications |
| `docs/background/userspace_filesystem_research.md` | FUSE daemon design patterns, metadata caching, language choice (Rust vs Go), test harnesses (Jepsen, FoundationDB simulation, xfstests) |
| `scripts/infra/` | AWS EC2 + EBS Multi-Attach provisioning, etcd deployment, FUSE daemon bootstrap (template), node add/remove |
| `scripts/test/` | Chaos engineering, fencing validation, epoch tests, network isolation (adapted from QAttach) |

## Architecture

Four subsystems per node (FUSE userspace daemon):
1. **FUSE frontend** — VFS ops → metadata + data calls
2. **Metadata client** — etcd as single source of truth for inodes, dirents, locks, allocation
3. **Data engine** — O_DIRECT/io_uring pread/pwrite against shared raw block device
4. **Membership/fencing agent** — etcd lease heartbeat + external fencing controller

Key design invariants:
- Data-then-metadata ordering for writes (crash consistency without disk journal)
- Metadata-then-data ordering for truncates
- Fencing: self-fencing watchdog → dual-confirmed external fencing → generation-stamped extent scrubber
- No directory-level locking — namespace mutations via atomic etcd Txn
- Block device carries only raw extents of file content — no filesystem format

## Conventions

- `docs/architecture/*.md` is the authoritative reference — consult the relevant subsystem doc before making any design decision.
- Language/build decisions are made: C (FUSE frontend, C11, `-Wall -Wextra -Werror`) + Go (metadata backend). Build with `make`; `make check` runs lint + tests.
- The fencing/generation invariants (see `docs/architecture/fencing-generation-protocol.md`) are the most safety-critical part of the design. Any change touching `pkg/fencing`, `pkg/metadata/gen.go`, or the extent-commit path must preserve them.
- etcd value encodings are **not uniform**: `inode_alloc_counter` and dirent values are 8-byte big-endian; `gen:<node>` is decimal ASCII; `extent:<ino>/<chunk>` is five comma-separated decimals followed by the writer's node ID. Match the existing encoding when seeding keys by hand or via `etcdctl`.
- Chaos scenarios run against docker (`chaos-test.sh docker <scenario>`) or AWS; the AWS mode costs real resources and takes ~4 min per scenario to provision. Iterate on docker.
- Integration tests need an etcd and the `integration` tag: `docker run -d --rm -p 2379:2379 quay.io/coreos/etcd:v3.5.18 etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379`, then `ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration ./...`. Every test gets its own etcd key space, so `./...` is safe.
- The chaos harness swallows errors (`readf`/`writef`/`runcmd` return `""` on any failure), so an SSH timeout looks identical to data loss. Confirm a failure's cause in `/tmp/meta.log` and `/tmp/fuse.log` on the node before treating it as a product bug.
