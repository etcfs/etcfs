# Contributing to EtcFS

EtcFS is a cluster filesystem that takes a shared raw block device and runs a
privileged mount on it. That shapes what contributions look like: a change to
the fencing, allocation, or write path can corrupt a user's data silently, so
those areas are reviewed harder and need evidence rather than argument.

Everything else — documentation, tests, tooling, packaging, the missing POSIX
surface — is ordinary work and very welcome.

## Getting set up

```bash
# Build both binaries: the Go metadata backend and the C FUSE daemon.
make all          # needs libfuse3-dev and a Go toolchain matching go.mod

make test         # Go unit tests (-race) plus the C wire tests
make lint         # golangci-lint, clang-format, shellcheck
make check        # lint + test — the same thing CI runs
make fmt          # goimports and clang-format, in place
make hooks        # install the pre-push hook that runs the checks above
```

The pre-push hook runs the lint and test checks above. Documentation lives
in [etcfs-docs](https://github.com/etcfs/etcfs-docs), which runs its own
`mkdocs build --strict` and `lychee` link check in CI — send docs changes
there instead.

A separate `Security` workflow runs on every push and pull request, and again
weekly so that an advisory published against unchanged code is still reported.
It runs CodeQL over both languages, `govulncheck` against the Go module, and the
C wire tests under AddressSanitizer and UndefinedBehaviorSanitizer. The last two
are worth running locally before sending a change that touches the C daemon or
adds a dependency:

```bash
GOTOOLCHAIN=auto go run golang.org/x/vuln/cmd/govulncheck@latest ./...

mkdir -p bin
cc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O1 -g \
  -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all \
  test/c/test_ops.c pkg/fuse/fuse.c pkg/block/block.c \
  -o bin/test-c-asan -lfuse3 -lpthread && ./bin/test-c-asan
```

`golangci-lint` is pinned rather than optional: the version lives in
`.golangci-version`, CI installs exactly that, and the hook refuses to pass with
anything else. A different version is not a stricter or looser opinion, it is a
different set of findings — and one built with an older Go than `go.mod`
targets refuses to run at all, which is how a green hook turns into a red CI.

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(cat .golangci-version)
```

Upgrading it means editing `.golangci-version`; both sides follow.

Integration tests need a running etcd and the `integration` build tag:

```bash
docker run -d --rm -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
  etcd --advertise-client-urls http://0.0.0.0:2379 \
       --listen-client-urls http://0.0.0.0:2379

ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration ./...
```

Every test uses its own etcd key space, so running the whole tree at once is
safe. `make dev` brings up the three-node Docker environment.

## Commit messages

The release automation reads the commit history, so the format is not
cosmetic: `semantic-release` derives the version and the release notes from
it, and a misclassified commit produces a wrong version number.

The convention is **header-only Conventional Commits, in the past tense, at
most 100 characters**:

```
type(scope): subject
```

- **No body.** The subject line is the whole message. If the reasoning does not
  fit, it belongs in a code comment or a doc page, where it stays next to the
  thing it explains.
- **Past tense**, describing what the commit did: `fixed`, `added`, `removed` —
  not `fix`, `add`, `remove`.
- **Types**: `feat` (minor release), `fix` (patch release), and the
  non-releasing `docs`, `test`, `refactor`, `chore`, `build`, `ci`, `perf`,
  `style`. A breaking change gets a `!` after the type — `feat(ipc)!:` — and
  cuts a major release.
- **Scope** is the package or subsystem: `fencing`, `arena`, `ipc`, `fuse`,
  `metrics`, `chaos`, `infra`.

Examples from the history:

```
fix(fuse): restored allow_other so non-root sessions can reach the mount
test(fencing): added diagnostics to R4's failure path
refactor(watch): deleted the unused watch multiplexer and kept its reasoning in the docs
```

Bundle related fixes, chores, and documentation into one commit; keep separate
features and refactors in separate commits.

## What a change should come with

- **Tests.** Non-trivial logic gets a test that fails if the logic breaks. Go
  code uses the standard library plus `testify`; the C daemon's wire handling
  is tested in `test/c/`. Tests needing a real etcd go behind the `integration`
  build tag.
- **Documentation, in a companion PR.** `docs/architecture/` in
  [etcfs-docs](https://github.com/etcfs/etcfs-docs) has one page per subsystem
  and it is the authoritative reference. If a change alters behaviour,
  configuration, or setup, revise the page that already covers it there
  rather than adding a new one.
- **Comments that explain why.** The codebase's comments record the reasoning
  behind non-obvious choices and the failure that motivated them. A comment
  restating the line below it is noise; a comment explaining why the order of
  two operations matters is the point.

## Changes to the safety-critical paths

`pkg/fencing`, `pkg/metadata/gen.go`, `pkg/arena`, and the extent-commit path
in `internal/ipc` carry the invariants that keep two nodes from writing the
same blocks. Before changing them, read
[`fencing-generation-protocol.md`](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/fencing/fencing-generation-protocol.md)
and
[`kleppmann-stale-write-analysis.md`](https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/storage/kleppmann-stale-write-analysis.md).

A change there should say which invariant it preserves and how that was
checked. The chaos suite is how it is checked in practice:

```bash
scripts/test/chaos-test.sh docker <scenario>    # iterate here
scripts/test/chaos-test.sh aws <scenario>       # costs real resources, ~4 min/scenario
```

Note that the chaos harness swallows errors — an SSH timeout looks exactly like
data loss — so confirm a failure's cause in `/tmp/meta.log` and `/tmp/fuse.log`
on the node before reporting it as a bug.

## Reporting bugs

Include the output of `etcfuse-meta --version`, the fencing mode the cluster
runs in (`--ebs-volume-id`, `--nvme-reservations`, or neither), the etcd
version, and the relevant part of `/tmp/meta.log` and `/tmp/fuse.log`. For
anything touching data integrity, the output of `etcfuse-meta --fsck` is worth
more than a description of the symptom.

Security issues go to the process in [`SECURITY.md`](SECURITY.md), not to the
issue tracker.
