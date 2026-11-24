# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Next steps, ranked

**1. Decide the Terraform misconfiguration findings.** Scanning is in place and
reports 21, none of them silenced. They need answers rather than patches:
encryption at rest on the shared volume and the root devices is the one that
matters, and adding it forces resource replacement — data loss on the next
apply against a live cluster — so it needs a migration story, not a commit. The
EKS module's public endpoint and the open egress rules are deliberate for a
module meant to stand a cluster up in an empty account, but that is currently a
decision nobody has written down. Two are cheap and safe: requiring IMDS session
tokens, which the node bootstrap already uses, and descriptions on the security
group rules the EKS module leaves bare.

**2. Get an OpenSSF Best Practices badge.** The cheapest remaining Scorecard
points and the only ones left that are not structural — a self-assessment form,
no code. Maintained, Contributors and Code-Review cannot move without repo age
or a second reviewer, which caps a solo project near 8/10; there is no value in
chasing those.

**3. Free blocks held after an ambiguous extent commit without a restart.**
When a commit's outcome is unknown and a re-read does not show it landed, the
write returns EIO and keeps its blocks allocated until the daemon restarts and
rebuilds the bitmap. A background pass that re-reads later and frees the blocks
once the extent is confirmed absent would return the space online. Small to
medium; `commitWriteOp` and the allocator.

**4. Run the lagging-member stale-read test in CI.**
`TestIntegration_LockedReadIsNotStaleOnALaggingMember` skips unless
`ETCD_LAGGING_ENDPOINT` points at a member cut off from its peers, so the normal
integration run never exercises it. A job with a three-member compose cluster
and one member isolated would. Small.

**5. Bound the startup rebuild's read.** `Reconstruct` reads every extent in the
cluster in one unpaginated range read and filters by arena in memory, so each
node's startup costs O(cluster extents) in time and memory. Paginate first; a
per-arena index key would make it O(own data) at one extra key per extent
commit. Small (pagination) to medium (index).

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
not.

That split has since been measured — `ETCFS_IPC_TRACE=1` times each `ipc_sync`
round trip — and it lands on the removable half: `OPEN` and `RELEASE` cost
**42–48 us** per hop against a Go handler of 3.6 us, so the hop is nearly all of
it and the FUSE upcall is the smaller remainder. The conclusion is unchanged
even so, because what is expensive is not what is removable: both ways of taking
that hop away are the ones blocked above. Reviving this means solving the
open-descriptor count first, not measuring again.

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

## For the thesis defence

Gaps an examiner can point at. The elasticity argument currently rests on
mechanism (journals, the node list, DLM recovery) more than on measurement:
a single planned join or leave costs GFS2 only 0.02–0.22 s, and everything
beyond that is untested.

What the DLM does on a membership change, from the sources in the thesis's
`references/` (`dlm-4.3.0-dlm_controld-cpg.c`, `linux-dlm-6.18.54/`):

- `dlm_controld` stops the kernel lockspace on every CPG configuration change
  (`confchg_cb` → `stop_kernel`), waits for ring id, quorum, fencing of failed
  nodes and fs-done, then for a start message from every member, and only then
  restarts it. Changes queued while it waits are folded into the one that
  starts (`cleanup_changes`). This is the DLM's own protocol on CPG, not
  Corosync's `sync_*` barrier.
- The kernel then runs `ls_recover`. The directory node of a resource is
  `hash % total_weight` over the member array (`dlm_hash2nodeid`), so every
  join or leave remaps it, and every node rebuilds its share by asking every
  other member for master names (`dlm_recover_directory`): about N×(N−1)
  request rounds, with data proportional to active resources. If any node
  left, clean leaves included (`node->gone` counts as `neg`), it also purges,
  remasters and resends locks. Barriers separate each step.
- A change that arrives during kernel recovery interrupts it
  (`dlm_recovery_stopped` → `-EINTR`) and the work is redone for the new
  change.
- The kernel logs each recovery as `dlm_recover <seq> generation <n> done:
  <ms> ms`. Collect it from `dmesg` on every node in every churn benchmark
  below, together with the active-lock count, alongside the survivors' stall.

### Membership churn: new benchmarks

- [ ] **Repeated leave/rejoin on GFS2.** Script ready, not yet run:
      `COMPARE_BACKEND=gfs2 bench-churn.sh` (per-cycle stall, bandwidth lost,
      `dlm_tool ls` every 100 ms). The `new change` parsing of `dlm_tool ls` is
      unverified until the first run's `dlm-*.log` is read.
- [ ] **Churn rate sweep.** Script ready, not yet run: `bench-churn.sh` with
      `ETCFS_CHURN_INTERVAL` at 60, 30, 10, 5 and 2 s. Two effects pull in
      opposite directions: `dlm_controld` folds queued changes into one
      restart, which bounds the number of stalls, while a change landing
      inside kernel recovery aborts it with `-EINTR`, which repeats the
      directory rebuild. Which one dominates is what the sweep measures: per
      change, the survivors' stall and the `dlm_recover ... done` times,
      including interrupted recoveries. EtcFS should stay flat. This is the
      measurement that turns the autoscaling argument from mechanism into
      evidence.
- [ ] **Overlapping changes.** Script ready, not yet run: `bench-overlap.sh`
      (four nodes) on both backends.
- [ ] **Stall versus cluster size.** Run `ETCFS_ELASTIC_NODES=N
      bench-repeat.sh 5 bench-elasticity.sh` per N. The elasticity table is
      three nodes only.
      Repeat the planned join/leave at 3, 6, 10 and 16 nodes (16 is the
      Multi-Attach limit and Red Hat's supported maximum for GFS2) to see
      whether the lockspace suspension grows with N. The directory rebuild is
      about N×(N−1) request rounds, so record `dlm_recover ... done` times and
      run each N with a small and a large number of active locks (for example
      100 and 100,000 open files), since rebuild data scales with resources,
      not with what the departing node held.
- [ ] **Time the full GFS2 add path.** Script ready, not yet run:
      `ETCFS_JADD_FULL=1 bench-gfs2-journal-add.sh`. `sec:eval:jadd` notes that
      adding a node not yet in Corosync's node list was not timed. Measure the whole sequence
      an operator runs: edit `corosync.conf` on every member,
      `corosync-cfgtool -R`, `gfs2_jadd`, mount, first write. Report the
      survivors' stall during the reload as well.
- [ ] **Autoscaling end to end.** Script ready, not yet run:
      `bench-scale-cycle.sh` (add-compute-node.sh out, remove_node's steps in,
      etcfs only). Nothing measures the scale path yet. For
      scale-out: time from the ASG desired-count change to the instance's first
      successful write, split into instance launch, etcd member add, daemon
      start, arena claim. For scale-in: time to `member remove` and arena
      reclaim, and the survivors' stall throughout. A GFS2 equivalent needs the
      journal and node-list steps scripted into a launch hook; if that is
      impractical, say so rather than leave the row empty.

### Missing measurements behind slide claims

- [ ] **More runs on the headline numbers.** `bench-repeat.sh <runs>
      <script>` gives min/median/max per metric. The kill timeline (8 EtcFS runs,
      4 GFS2) and the elasticity table (single 30 s aggregates) are thin for
      the two results the talk leans on. Aim for at least 5 runs each with
      ranges.
- [ ] **EFS close-to-open staleness.** Script ready, not yet run:
      `bench-coherence.sh` on `efs` and `etcfs`, which covers the `fcntl` item
      below too. The "why not EFS" slide relies on RFC
      7530 §10.3.1 only. A two-node test that keeps a file open on both, writes
      on one and reads on the other without reopening, run on EFS and on
      EtcFS, would show the difference directly. Read the EFS documentation on
      consistency first, so the result is compared with what AWS promises.
- [ ] **Cross-node `fcntl` on EFS against EtcFS.** One script showing that a
      lock taken on node A blocks node B on EFS and does not on EtcFS. It backs
      the out-of-scope line with a measurement instead of a statement.
- [ ] **GFS2 large-file reads.** Both scripts already accept
      `COMPARE_BACKEND=gfs2`; this needs only running. The 8 GiB handoff and the concurrent-write
      rows of the alternatives table say "not measured here" for GFS2. Run
      `bench-handoff.sh` and `bench-seq-throughput.sh` against GFS2 on the same
      `m7i.large`/8000-IOPS setup so the direct-path comparison includes the
      incumbent.


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

**Reattach the volume automatically after an EBS fence. Small to medium;
bootstrap plus a fence-completion check.** A node fenced by detach restarts
without its volume until someone reattaches it. Reattaching is safe only once
the old incarnation's fence is complete — generation raised and the controller
done with its arenas — and with the old process gone; the new process must read
its generation after the reattach. Reattaching earlier lets the new process
rebuild and allocate in arenas the controller is about to release as
device-fenced, putting two writers in one range.

**Several shared volumes. Medium to large; extent addressing plus fencing.**
Capacity and IOPS are bounded by one io2 volume. Attaching several, each to
every node, keeps the shared-device property: an extent becomes (volume,
offset), arenas are numbered per volume, and allocation, ownership and the
bitmap rebuild work per volume unchanged. The hard part is fencing: an arena may
be released only after the device cut its old owner off, so a fence must cover
every volume, and a fence that succeeds on some volumes and fails on others may
release only the fenced volumes' arenas. The fencing controller and the TLA+
model both assume one device. Multi-Attach still caps each volume at 16
instances in one availability zone, so this grows capacity, not cluster size.
One private disk per node is not the same extension: other nodes could no
longer read a file's bytes directly, and a dead node would make its files
unreadable rather than only unwritable.

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

**Owner-initiated arena donation. Medium; allocator plus a file relocation.**
Free space inside a live node's partly used arenas cannot move to another node,
so a node can get `ENOSPC` on a half-empty device. The safe remedy is for the
owner to empty a mostly free arena by relocating its few live files into its
other arenas (copy, then switch the extents in one transaction under the file's
lock) and release it through `ReleaseEmptyArenas`. Taking an arena from a live
node is never safe: both nodes are unfenced, so no guard rejects either writer.

**Shard the inode counter. Small to medium; `inodealloc.go` and one key.**
Contention grows with node count by design; named as the structure most likely
to need reworking first if metadata-creation throughput becomes a target.

**Cross-file / cross-directory atomicity. Structural; not planned.**
Cross-file / cross-directory atomicity does not exist. Each inode is independently consistent; there is no multi-inode transaction, no snapshot spanning several files. An application that needs "these three files change together, atomically, cluster-wide" gets nothing from the filesystem for that — it has to build it itself.
