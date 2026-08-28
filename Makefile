# EtcFS — top-level build orchestration.
#
# Targets:
#   make all         build both binaries (etcfuse, etcfuse-meta)
#   make test        run unit tests (Go + C)
#   make test-integration  run the etcd-backed suites (needs a running etcd)
#   make lint        run linters (Go: golangci-lint, C: clang-format check, bash: shellcheck)
#   make fmt         auto-format all code
#   make clean       remove build artifacts
#   make dev         start docker-compose development environment
#   make check       lint + test (CI entry point)

.PHONY: all test lint fmt clean dev check test-conformance test-integration test-e2e

GO_ENTRY   := ./cmd/etcfuse-meta
GO_OUT     := bin/etcfuse-meta
CTL_ENTRY  := ./cmd/etcfsctl
CTL_OUT    := bin/etcfsctl
C_ENTRY    := cmd/etcfuse
C_OUT      := bin/etcfuse

# Stamped into the binary so `etcfuse-meta --version` reports the tag a bug
# report was filed against, not the placeholder in the source.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GO_LDFLAGS := -X github.com/etcfs/etcfs/internal/config.Version=$(VERSION)
all: $(GO_OUT) $(CTL_OUT) $(C_OUT)

# ---- Go build ----

$(GO_OUT): $(shell find . -name '*.go' -not -path './vendor/*' -not -path './test/*')
	go build -ldflags "$(GO_LDFLAGS)" -o $(GO_OUT) $(GO_ENTRY)

$(CTL_OUT): $(shell find . -name '*.go' -not -path './vendor/*' -not -path './test/*')
	go build -ldflags "$(GO_LDFLAGS)" -o $(CTL_OUT) $(CTL_ENTRY)

# ---- C build ----

C_SRCS := $(shell find cmd/etcfuse pkg/fuse pkg/block -name '*.c')
C_HDRS := $(shell find cmd/etcfuse pkg/fuse pkg/block -name '*.h')
C_CFLAGS := -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g
C_LIBS   := -lfuse3 -lpthread

$(C_OUT): $(C_SRCS) $(C_HDRS)
	$(CC) $(C_CFLAGS) $(C_SRCS) -o $(C_OUT) $(C_LIBS)

# ---- Testing ----

test: test-go test-c

test-go:
	go test -race -count=1 ./...

# The test binary includes ops.c, so it needs the same flags and libraries the
# daemon is built with, minus the daemon's own main.
C_TEST_SRC  := test/c/test_ops.c

test-c: bin/test-c
	./bin/test-c

bin/test-c: $(C_TEST_SRC) $(C_SRCS) $(C_HDRS)
	@mkdir -p bin
	$(CC) $(C_CFLAGS) $(C_TEST_SRC) pkg/fuse/fuse.c pkg/block/block.c -o $@ $(C_LIBS)

# The suites behind the `integration` build tag: the handlers, the lock cache,
# the flusher and the recall path against a real etcd.  Needs one running —
# ETCD_ENDPOINTS points at it, default http://localhost:2379:
#
#   docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
#     /usr/local/bin/etcd --data-dir=/etcd-data \
#     --listen-client-urls=http://0.0.0.0:2379 \
#     --advertise-client-urls=http://0.0.0.0:2379
#
# -race here and not only in test-go: this is the only place those subsystems
# run against a real etcd and each other, which is where a data race shows up.
test-integration:
	go test -race -tags=integration -count=1 -timeout 900s ./...

# The whole stack in one process tree: etcd, the Go backend, and the C daemon
# on a real mountpoint.  Needs FUSE and an etcd; see the script's header.
test-e2e:
	bash test/e2e/run-phase2.sh

# POSIX conformance: upstream pjdfstest against a live mount in Docker.
# Results in deploy/docker/pjdfstest-results/.
test-conformance:
	bash scripts/test/pjdfstest.sh

# ---- Linting & formatting ----

lint: lint-go lint-c lint-sh

# Pinning the version of golangci-lint is not enough on its own: it also has to
# run under the Go go.mod targets.  The linter typechecks against the export
# data of whichever toolchain is on PATH, and a release built for an older Go
# cannot read a newer one's — so a machine with a newer Go installed reports
# every package as broken ("undefined: yaml", "l.Error undefined") while the
# build, the tests and CI are all clean.  GOTOOLCHAIN fetches the pinned
# version on demand and leaves the rest of the machine alone.
GO_TOOLCHAIN := go$(shell awk '/^go [0-9]/{print $$2; exit}' go.mod)

lint-go:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) golangci-lint run ./...

lint-c:
	clang-format --dry-run --Werror $(C_SRCS) $(C_HDRS) $(C_TEST_SRC)

lint-sh:
	shellcheck scripts/infra/*.sh scripts/test/*.sh scripts/bench/*.sh scripts/bench/compare/*.sh

fmt: fmt-go fmt-c

fmt-go:
	goimports -w .

fmt-c:
	clang-format -i $(C_SRCS) $(C_HDRS) $(C_TEST_SRC)

# ---- Docker dev environment ----

dev:
	cd deploy/docker && docker compose up -d
	@echo "EtcFS dev environment started."
	@echo "  etcd endpoints: http://localhost:2379, http://localhost:2380, http://localhost:2381"
	@echo "  Logs: docker compose -f deploy/docker/docker-compose.yml logs -f"

dev-down:
	cd deploy/docker && docker compose down -v

# ---- Clean ----

clean:
	rm -rf bin/
	go clean -cache

# ---- CI check (lint + test) ----

check: lint test

# ---- Git hooks ----

hooks:
	ln -sf ../../scripts/dev/pre-push.sh .git/hooks/pre-push
	@echo "Git hooks installed"

# ---- Help ----

help:
	@echo "EtcFS build targets:"
	@echo "  make all              build everything"
	@echo "  make test             run unit tests (Go, C)"
	@echo "  make lint             run all linters"
	@echo "  make fmt              auto-format code"
	@echo "  make hooks            install git pre-push hook"
	@echo "  make dev              start dev environment"
	@echo "  make clean            remove artifacts"
	@echo "  make check            CI pipeline (lint + test)"
