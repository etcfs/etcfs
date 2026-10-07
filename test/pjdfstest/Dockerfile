# POSIX conformance harness: upstream pjdfstest run against a live EtcFS mount.
#
# One container holds the metadata daemon, the FUSE daemon and the test suite,
# because pjdfstest has to run inside the mount namespace that owns the mount:
# a FUSE mount made in one container is not visible in another.
#
# Build:
#   docker compose -f deploy/docker/docker-compose.pjdfstest.yml build

FROM golang:1.27 AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/etcfuse-meta ./cmd/etcfuse-meta

FROM amazonlinux:2023 AS c-builder
RUN dnf install -y gcc make fuse3-devel fuse3-libs && dnf clean all
WORKDIR /build
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN make -C cmd/etcfuse

# pjdfstest is built from source: no distribution packages it, and the suite
# compiles a single helper binary the test scripts drive.
FROM amazonlinux:2023 AS pjd-builder
ARG PJDFSTEST_REF=master
RUN dnf install -y gcc make autoconf automake libtool git && dnf clean all
WORKDIR /build
RUN git clone --depth 1 --branch "$PJDFSTEST_REF" \
        https://github.com/pjd/pjdfstest.git pjdfstest
WORKDIR /build/pjdfstest
RUN autoreconf -ifs && ./configure && make pjdfstest

FROM amazonlinux:2023
# util-linux supplies /bin/mount, which libfuse shells out to.  openssl and
# gawk are what the suite's namegen() shells out to for unique scratch names:
# without them every test reuses the literal name "pjdfstest_" and the run
# reports collisions that are artefacts of the harness, not of the filesystem.
RUN dnf install -y fuse3-libs util-linux openssl gawk findutils diffutils \
    && dnf clean all

COPY --from=c-builder  /build/bin/etcfuse   /usr/local/bin/etcfuse
COPY --from=go-builder /out/etcfuse-meta    /usr/local/bin/etcfuse-meta
COPY --from=pjd-builder /build/pjdfstest    /opt/pjdfstest
COPY test/pjdfstest/run.sh /usr/local/bin/run-pjdfstest

ENTRYPOINT ["/usr/local/bin/run-pjdfstest"]
