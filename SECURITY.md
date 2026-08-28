# Security Policy

EtcFS runs a privileged mount over a shared raw block device, and its metadata
lives in an etcd cluster that every node can write to. A vulnerability here can
mean data loss or cross-node data disclosure, so please report one privately
rather than in a public issue.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting on this repository
(**Security → Report a vulnerability**), which opens a channel visible only to
the maintainers.

Please include:

- The version (`etcfuse-meta --version`) and commit.
- The deployment shape: fencing mode (`--ebs-volume-id`, `--nvme-reservations`,
  or single-signal), whether etcd runs with TLS client certificates, and
  whether the mount uses `allow_other`.
- What an attacker gains, and what access they need to start with.
- A reproduction, ideally as a test or a chaos scenario.

You can expect an acknowledgement within a few days, and an assessment with a
fix or mitigation plan after that. Please give us a chance to ship a fix before
disclosing publicly.

## Supported versions

EtcFS is pre-1.0 and under active development. Only the latest release on
`main` receives fixes; there are no maintained release branches.

## Scope

In scope, and treated as vulnerabilities:

- Anything that lets a node write to a device range it does not own, or that
  defeats the fencing generation guard.
- Metadata that can be crafted to make a daemon read or write outside its
  arenas, or to escape the bounds checks on the IPC frames.
- Local privilege escalation through the daemon, the IPC socket, or the mount —
  including anything that lets a user other than the socket's owner issue
  metadata operations.
- Denial of service that outlives the attacker's own session, such as a request
  that wedges the daemon for every mount it serves.

Out of scope, because they are documented properties of the design rather than
defects:

- **POSIX locks are node-local.** `fcntl`/`flock` are enforced within a node and
  exclude nothing between nodes; the daemon warns about this at startup. See
  [`posix-lock-operations.md`](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/metadata/posix-lock-operations.md).
- **Shared writable `mmap` across nodes** is not supported.
- **An untrusted etcd cluster.** etcd is the source of truth for all structural
  state; anyone able to write to the keyspace can do anything the filesystem
  can. Run etcd with TLS client certificates and restrict access to it.
- **An untrusted operator on the same host.** Anyone who can read the block
  device directly bypasses the filesystem entirely.
- **`--allow-buffered-io`.** It disables `O_DIRECT` and is documented as safe
  only on an unshared device; the daemon warns loudly when it is used.

## Hardening the deployment

- Give etcd TLS client certificates (`--etcd-cert`, `--etcd-key`, `--etcd-ca`)
  and restrict its keyspace to the EtcFS nodes.
- Keep the IPC socket's directory `0700` and owned by the daemon's user; the
  daemon creates it that way, and a looser permission is enough to let another
  local user issue filesystem operations.
- Use a real fencer (`--ebs-volume-id` or `--nvme-reservations`). Single-signal
  mode stops a fenced node's metadata writes but not its in-flight device
  writes, which is a correctness exposure on a genuinely shared volume.
- Expose `--metrics-addr` on an interface only your monitoring can reach: the
  metrics disclose node identities, arena occupancy, and fencing state.
