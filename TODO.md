# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Next steps, ranked

**1. Put security tooling in CI.** There is none: no `govulncheck`, `gosec`,
CodeQL, Trivy, SBOM, Dependabot or Scorecard anywhere in `.github/`. Ranked by
value for a root-running FUSE daemon: ASan/UBSan builds of the existing C tests;
a libFuzzer target for the C request path, which the system-level chaos fuzzers
do not cover for memory safety; `govulncheck`; pinning Actions to SHAs rather
than the mutable tags the release pipeline currently trusts to publish binaries,
packages, containers and the Helm chart; signing releases and attaching
provenance.

## Deferred

Measured, understood, and parked — not because they do not matter, but because
what is left to gain is small or is held back by something worth more than the
gain.

**The cost of `open`.** Was ranked on a 208 us `open`+`read` of six bytes
against ext4's 14.8, on the theory that the cost was per-open work in the daemon
or the lock path. Measured on AWS (`m7i.large`, io2 Multi-Attach, one node,
warm — the lock already cached and the read served from the kernel's page
cache): 54.2 us total, of which the Go `open` handler is **3.6 us**. So neither
the daemon nor the lock path explains it, and the gap to ext4 is roughly 4x, not
the 14x the old number implied. The remaining ~50 us is the FUSE upcall plus the
C-to-Go IPC hop, and that split has not been measured — it is what decides
whether anything here is actionable at all, since only the IPC half could ever
be removed. Both ways of removing it are blocked: answering `open` in the C
daemon breaks the open-descriptor count that POSIX's unlink-while-open rule
rests on, which `openFiles.heldOpen` consults inside the unlink transaction
under the mutex a close takes; and the kernel's zero-message open
(`FUSE_CAP_NO_OPEN_SUPPORT`) suppresses `release` along with `open`, taking the
count away entirely and with it the per-open page-cache decision. Folding
`open`+`read` into one message buys nothing in this case, because the warm read
never reaches the daemon. Worth reviving only if an open-heavy workload shows
the cost mattering; the Gollum benchmark's read-path parity suggests it does
not. Timing `ipc_sync` inside the C daemon is the ~20-line measurement that
would settle the split.

## Pending benchmark work

- [ ] **Multi-hour fuzzing beyond two hours.** A two-hour run (319,465
      operations, 317 injected faults) is clean on memory, file descriptors and
      store size. That earns "stable for two hours", not "no slow-leak class of
      bug exists".
- [ ] **The arena soak's residual drift.** Allocatable space still trends down
      1.50 GiB/h over half an hour after the reclaim fix, against 25.55 before
      it. Too small to reach ENOSPC and too large to call zero; separating a
      smaller leak from the scrubber's cadence needs allocator-level free-block
      accounting rather than `df`.
- [ ] **Real-application benchmarks.** Every report today is synthetic (fio,
      untar, `du`) or head-to-head against another filesystem; none answers what
      happens when an ordinary application runs on the mount. A Gollum wiki
      backed by a git repository on the shared volume, measured against the same
      stack on local ext4 on the same node, gave read-path parity across the
      board (125 vs 102 ms to render an 8 KB page; 4041 vs 3979 ms for a
      whole-repo log; a 38 KB page marginally faster on EtcFS) and 412 vs 25 ms
      to commit an edit. Read parity under a real application is a stronger
      claim than any fio number and is currently made nowhere; the write row
      belongs beside it, since git turns a 2 KB edit into ~25 namespace
      mutations and is close to worst-case for this architecture. Caveats to fix
      in a real harness: single-run medians, one instance type, and an ext4
      control on the gp3 root volume against io2 for EtcFS. Candidates beyond
      git: SQLite, a build cache, a Prometheus TSDB.
- [ ] **GFS2's takeover under fencing.** With `fence_aws` confirmed at ~10 s the
      survivors keep serving, but the dead node's inode was still not recovered
      inside 180 s. Needs `dlm_tool ls` and the recovery journal inspected while
      it is happening to say whether a step is missing from the setup or that is
      GFS2's own behaviour.


# Big Extensions or Impactful changes

Changes that reach past one handler — a cluster-wide semantic, a formal
artifact, or a headline benchmark property.

**Mode and ownership under the inode lock.** `getattr` costs ~1.5 ms per file
reading etcd for a record the lock's own snapshot already holds, and it cannot
use that snapshot today: `setattr` changes mode and ownership under a bare
compare-and-set and takes no lock (`internal/ipc/handlers.go:871`), so a peer
can rewrite those fields of an inode this node holds exclusively. Bringing them
under the lock makes the snapshot authoritative and makes a `chmod` on a file
another node holds force a handover — a change to how `chmod` behaves
cluster-wide, not an optimisation. `getxattr` (~1.5 ms more) is a separate
problem, not the same one twice: `xattr:` is its own prefix and is watched by
nothing, so caching it needs a new watch as well as lock coverage of the
unlocked `setxattr`.

Three findings from reading the paths, none of them reproduced yet:

- *There is a correctness hole underneath the performance question.*
  `inodeChanged` (`internal/ipc/notify.go:277`) skips the kernel attribute
  invalidation for any inode this node holds a lock key on, on the grounds that
  a held inode "cannot have been written by a peer". True for size and extents;
  false for mode, ownership and nlink. With `default_permissions`
  (`pkg/fuse/fuse.c:572`) and a 60 s attribute timeout, a peer's `chmod` can go
  unseen on a holding node for up to that timeout — `getattr` reading etcd does
  not save it, because the kernel does not call `getattr` while its cached
  attributes are valid. Bringing mode under the lock closes this as a side
  effect, which makes the item a correctness fix that unlocks an optimisation
  rather than an optimisation with a semantic cost. Needs a two-node test
  before anyone acts on it.
- *Scope is wider than mode and ownership.* `nlink` is written unlocked too, by
  `link`/`unlink`/`rename` (`pkg/metadata/dirent.go:255,351,444,738`). A
  snapshot authoritative for mode but stale on nlink still cannot answer
  `getattr`, and locking the namespace path aims straight at the lock-free
  namespace property the 2→6 node metadata sweep is built on.
- *The formal artifacts point opposite ways.* `CachedLock.tla`'s `Write` action
  already requires `Holds(n)` and `NoPublishWithoutLock` asserts no non-holder
  publishes — so the spec already models the world this item would create, and
  the code is the divergence. What it needs is a stated field mapping. The
  alternative design (holders drop their snapshot on a peer's write, keeping
  `chmod` lock-free) is cheaper at runtime but needs a new spec action and
  weakens `ViewMatchesTruth` to an eventual property. Either way Porcupine
  covers nothing here: no model touches mode, ownership or nlink, and
  `namespace.go:46` accepts EACCES without constraining state.

**`FUSE_CAP_WRITEBACK_CACHE`.** `fuse.c:486-495` takes `READDIRPLUS`,
`ASYNC_READ` and `AUTO_INVAL_DATA` and deliberately leaves writeback out, so the
kernel cannot coalesce small buffered writes and each one is its own round trip.
Measured writing 64 MiB: 2 MiB/s at 4 KiB, 3 at 8 KiB, 12 at 32 KiB, 45 at
128 KiB, 126 at 1 MiB, against ext4's 891 MiB/s at 4 KiB. Any application
writing in small buffered chunks — git, SQLite, log writers, `tar` — lands at
the bottom of that curve without knowing block size was a performance decision.
`ops.c:653` states the write-through property is intentional; this is a question
about that decision, not a bug report against it.

Findings from reading the paths, none of them reproduced yet:

- *Write-through is load-bearing, not incidental.* It is what makes the kernel
  hold only clean pages, so yielding an inode is `invalidatePages`
  (`internal/ipc/notify.go:344`) — a drop. Under writeback the kernel holds
  dirty pages the daemon has never seen, and dropping those on a handover is a
  lost acknowledged write. The recall path would have to write them back before
  invalidating, lengthening a path that already waits out `minHoldTime` and a
  flush.
- *It puts an unpublished buffer outside the daemon's control.* Today an ack
  means the daemon holds the bytes (`pending`, `delegate.go`), and a self-fence
  loses only what that buffer records. Under writeback the app is acked by the
  kernel and the bytes may never reach the daemon at all, so a fenced node loses
  writes the whole design currently accounts for. The kernel also takes over
  `st_size` and mtime for an open file, against `withPending`/`pendingSize`
  (`handlers.go:107`) patching size from the daemon's own buffer — two owners of
  one field, with peers reading a third copy from etcd. `direct_io` is a third
  collision: `ops.c:655` sets it whenever the backend refuses page caching, and
  writeback does not combine with it.
- *Both formal artifacts need work, and the checker fails silently without it.*
  `CachedLock.tla` models the kernel's pages as a boolean `pages[n]` — clean
  only — so writeback needs a dirty-page variable representing a second
  unpublished buffer beside `buf`, `NoLostAckedWrite` extended to cover it, and
  a broken variant in the shape of the existing `RecallFlushes FALSE`. On the
  Porcupine side `test/verify/pagecache.go` asserts every yield was preceded by
  an invalidation; under writeback the obligation becomes "write back, then
  invalidate", and until the checker learns the new event it keeps passing while
  data is lost. No chaos scenario writes small buffered chunks and then kills
  the node.

# Future Extensions

Sized by effort and by how far the change reaches.

**Backup and restore. Large; allocator, metadata, new tool.** Nothing today could
restore this filesystem's data if the shared device were lost. The path is clean
— two etcd revisions diff to exactly the changed extents — but a backup that
reads a block already reused reads another file's bytes into itself, so it needs
the same pinning machinery snapshots do. That pinning is the work.

**Snapshots. Large; same pinning, plus a namespace clone.** Shares all of its
hard part with backup, so the two are one project rather than two.

**Cross-node `fcntl`/`flock`. Medium; a key namespace and two handlers.** Today
`SETLK` always succeeds and `GETLK` always reports the range free, so an
application coordinating through file locks silently gets nothing. `SETLKW`
needs blocking semantics against a lease, which is the design cost. Unrelated to
the per-inode lease lock the data path uses, which works. Worth promoting on
severity rather than effort: the showcase wiki is safe across nodes only because
git coordinates through `O_CREAT|O_EXCL` and `rename`, both of which are
enforced; an application using `flock` for the same job gets no exclusion at all
and no indication of it. Until the handlers exist, a loud log line on first
`SETLK`, or a mount option returning `ENOTSUP` instead of success, would stop
that failing silently.

**A production caller for arena rebalancing. Small; contained.** The mechanism
exists and nothing invokes it, so an imbalanced cluster has no remedy.

**Shard the inode counter. Small to medium; `inodealloc.go` and one key.**
Contention grows with node count by design; named as the structure most likely
to need reworking first if metadata-creation throughput becomes a target.

**Cross-file / cross-directory atomicity. Structural; not planned.**
Cross-file / cross-directory atomicity does not exist. Each inode is independently consistent; there is no multi-inode transaction, no snapshot spanning several files. An application that needs "these three files change together, atomically, cluster-wide" gets nothing from the filesystem for that — it has to build it itself.
