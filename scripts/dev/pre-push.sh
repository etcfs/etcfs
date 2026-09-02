#!/bin/bash
# pre-push.sh — run linters and tests before pushing.
#
# Install:
#   ln -sf ../../scripts/dev/pre-push.sh .git/hooks/pre-push
#
# Bypass (emergency only):
#   git push --no-verify

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

failures=0

run_check() {
    local name="$1"; shift
    printf "  %-30s " "$name"
    if "$@" &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAIL${NC}"
        failures=$((failures + 1))
        "$@" 2>&1 | head -20 || true
        echo ""
    fi
}

echo ""
echo "=== pre-push checks ==="

run_check "go build"  go build ./...
run_check "go test"   go test -race -count=1 ./...
run_check "go vet"    go vet ./...
# Tagged tests are invisible to the untagged build, so a signature change that
# breaks one compiles clean here and fails in CI.  Vetting with the tag is the
# cheapest way to compile them without needing the etcd a run would want.
run_check "go vet (integration)" go vet -tags=integration ./...
# The linter must be the version CI installs.  A different one is not a
# stricter or looser opinion, it is a different set of findings — and one built
# with an older Go refuses go.mod's target outright, which is a green hook and
# a red CI.
pinned_lint="$(cat .golangci-version)"
# `|| true`, because this whole assignment runs under `set -e` with `pipefail`:
# with golangci-lint absent the grep matches nothing and exits 1, the pipeline
# inherits that, and the hook dies here — before reaching the branch below that
# exists precisely to report a missing linter. git surfaces that as a bare
# "error: failed to push some refs", with every check above it printed OK, which
# looks like the remote rejecting the push rather than the hook aborting it.
# The `v` is optional because v2 dropped it: v1 printed "has version v1.64.8",
# v2 prints "has version 2.13.2". Matching only the prefixed form read a present
# v2 linter as absent, and the hook then refused a push over a linter that was
# installed and correct.
installed_lint="v$(golangci-lint --version 2>/dev/null | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || true)"
if [[ "$installed_lint" != "$pinned_lint" ]]; then
    printf "  %-30s " "golangci-lint"
    echo -e "${RED}FAIL${NC}"
    failures=$((failures + 1))
    echo "  installed ${installed_lint:-none}, CI uses ${pinned_lint} (.golangci-version)"
    echo "  install it with:"
    echo "    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${pinned_lint}"
    echo ""
else
    # And it must run under the Go go.mod targets, for the same reason: the
    # linter typechecks against the export data of whichever toolchain is on
    # PATH, and a release built for an older Go cannot read a newer one's, so a
    # newer Go here reports every package as broken while CI is clean.
    # The toolchain line when there is one, exactly as CI's setup-go resolves
    # it. Falling back to the `go` line alone is not enough: that line may name
    # a language version like "1.26", and GOTOOLCHAIN rejects "go1.26" as "a
    # language version but not a toolchain version".
    pinned_go="$(awk '/^toolchain go[0-9]/{print $2; exit}' go.mod)"
    if [[ -z "$pinned_go" ]]; then
        pinned_go="go$(awk '/^go [0-9]/{print $2; exit}' go.mod)"
    fi
    run_check "golangci-lint ${pinned_lint}" \
        env GOTOOLCHAIN="$pinned_go" golangci-lint run --timeout=5m ./...
fi
run_check "C build"   make -C cmd/etcfuse
run_check "C test"    make test-c
mapfile -d '' c_files < <(find cmd/etcfuse pkg/fuse pkg/block test/c \( -name '*.c' -o -name '*.h' \) -print0)
run_check "clang-format" clang-format --dry-run --Werror "${c_files[@]}"
run_check "shellcheck" shellcheck scripts/infra/*.sh scripts/test/*.sh

if [[ "$failures" -gt 0 ]]; then
    echo ""
    echo -e "${RED}${failures} check(s) failed. Push aborted.${NC}"
    echo "Fix with: make fmt && make check"
    exit 1
fi

echo ""
echo -e "${GREEN}All checks passed.${NC}"
