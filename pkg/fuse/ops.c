/*
 * ops.c — FUSE low-level operation handlers (synchronous IPC).
 *
 * Each handler builds a binary request payload, performs a synchronous
 * IPC exchange with the Go metadata backend over the Unix socket,
 * parses the response, and calls the appropriate fuse_reply_* function.
 *
 * IPC wire format (C ↔ Go):
 *   Request:  [u16:be opcode] [u32:be payload_len] [payload]
 *   Response: [u32:be payload_len] [payload]
 */

#include "ops.h"
#include "fuse.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

/* ---- opcodes (match Go side) ---- */
#define IPC_OP_LOOKUP      1
#define IPC_OP_GETATTR     2
#define IPC_OP_READDIR     3
#define IPC_OP_READLINK    4
#define IPC_OP_CREATE      5
#define IPC_OP_MKDIR       6
#define IPC_OP_UNLINK      7
#define IPC_OP_RMDIR       8
#define IPC_OP_RENAME      9
#define IPC_OP_SYMLINK     10
#define IPC_OP_LINK        11
#define IPC_OP_SETATTR     12
#define IPC_OP_OPEN        13
#define IPC_OP_RELEASE     14
#define IPC_OP_OPENDIR     15
#define IPC_OP_RELEASEDIR  16
#define IPC_OP_STATFS      17
#define IPC_OP_READ        22
#define IPC_OP_WRITE       23
#define IPC_OP_FSYNC       24
#define IPC_OP_MKNOD       25
#define IPC_OP_FLUSH       26
#define IPC_OP_READDIRPLUS 29
#define IPC_OP_SETXATTR    30
#define IPC_OP_GETXATTR    31
#define IPC_OP_LISTXATTR   32
#define IPC_OP_REMOVEXATTR 33
#define IPC_OP_LSEEK       34
#define IPC_OP_FALLOCATE   35

/* Must match maxFrameLen in internal/ipc/socket.go: 1 MiB write payload plus
 * its 28-byte fixed header (ino + offset + size + uid + flags, see
 * ec_write's payload below). */
#define IPC_MAX_FRAME_LEN ((1u << 20) + 28)

/* MAX_NAME_LEN comes from fuse.h; the notification reader bounds its buffer by
 * the same constant. */
/* A symlink target is a path, not a name: bounded by PATH_MAX, not NAME_MAX. */
#define MAX_TARGET_LEN 4095

/* Extended attributes: the kernel's own XATTR_NAME_MAX and XATTR_SIZE_MAX.
 * The Go side enforces the same two numbers, because it is etcd -- not this
 * process -- that an oversized value would actually hurt. */
#define MAX_XATTR_NAME_LEN  255
#define MAX_XATTR_VALUE_LEN 65536

static int send_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        len -= (size_t) n;
        p += n;
    }
    return 0;
}

static int recv_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) {
            if (n == 0)
                return -1;
            if (errno == EINTR)
                continue;
            return -1;
        }
        len -= (size_t) n;
        p += n;
    }
    return 0;
}

/* ---- synchronous IPC ---- */

/*
 * Times the round trip to the metadata daemon when ETCFS_IPC_TRACE is set in
 * the environment, and costs nothing when it is not.
 *
 * It exists to split a number the daemon's own metrics cannot reach. An
 * operation's latency is the FUSE upcall from the kernel, plus this hop to the
 * Go daemon and back, plus the handler at the far end — and only the handler is
 * measured, by etcfuse_fuse_op_duration_seconds. Subtracting both from the
 * end-to-end time is what says whether the cost of an operation is the upcall,
 * which nothing in this process can remove, or this hop, which folding requests
 * together could.
 *
 * Per call rather than aggregated: a mount serving a benchmark writes a lot of
 * these, and the distribution is the interesting part. Aggregate them from the
 * log.
 */
static int ipc_trace_enabled(void)
{
    /* Read once. The race between two threads doing this at the same time is
     * benign: they compute the same answer from the same environment. */
    static int enabled = -1;
    if (enabled < 0)
        enabled = getenv("ETCFS_IPC_TRACE") != NULL;
    return enabled;
}

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}

static int ipc_sync(int fd, uint16_t op, const uint8_t *payload, uint32_t plen, uint8_t **resp,
                    uint32_t *rlen)
{
    int traced = ipc_trace_enabled();
    uint64_t started = traced ? monotonic_ns() : 0;
    uint8_t hdr[6];
    hdr[0] = (uint8_t) (op >> 8);
    hdr[1] = (uint8_t) op;
    hdr[2] = (uint8_t) (plen >> 24);
    hdr[3] = (uint8_t) (plen >> 16);
    hdr[4] = (uint8_t) (plen >> 8);
    hdr[5] = (uint8_t) plen;
    if (fd < 0)
        return -1;
    if (send_full(fd, hdr, 6) < 0)
        goto broken;
    if (plen > 0 && send_full(fd, payload, plen) < 0)
        goto broken;

    uint8_t rhdr[4];
    if (recv_full(fd, rhdr, 4) < 0)
        goto broken;
    uint32_t rl = ((uint32_t) rhdr[0] << 24) | ((uint32_t) rhdr[1] << 16) |
                  ((uint32_t) rhdr[2] << 8) | (uint32_t) rhdr[3];
    /*
     * The length is whatever arrived on the wire, and it is about to become a
     * malloc size.  A desynchronised stream would otherwise ask for up to 4
     * GiB.  The Go side refuses to send or accept a frame past the same cap.
     */
    if (rl > IPC_MAX_FRAME_LEN)
        goto broken;
    uint8_t *rb = malloc(rl > 0 ? rl : 1);
    if (!rb)
        return -1;
    if (rl > 0 && recv_full(fd, rb, rl) < 0) {
        free(rb);
        goto broken;
    }
    *resp = rb;
    *rlen = rl;
    if (traced)
        etcfs_log(ETCFS_LOG_DEBUG, "ipc op=%u took %llu ns", (unsigned) op,
                  (unsigned long long) (monotonic_ns() - started));
    return 0;

    /*
     * Any failure here leaves the socket at an unknown offset -- a short read
     * mid-frame would otherwise make the next request read this reply's tail --
     * so the connection is dropped rather than reused.  The caller still sees
     * one EIO: the request may or may not have reached the daemon, and the ops
     * are not all idempotent, so retrying it transparently is not safe.  The
     * next request from this thread reconnects.
     */
broken:
    etcfs_ipc_drop();
    return -1;
}

/* ---- binary readers on response buffers ---- */

/*
 * A cursor over a response, so a short one cannot be read past.  The readers
 * used to advance a bare offset with no reference to rlen: the Go side always
 * sends fixed-width blocks, which is why that held, but it is the same
 * assumption the readdirplus desync broke.  Once a read runs out of buffer, ok
 * stays 0 and every later read yields zero, so a handler tests ok once before
 * replying.
 */
struct rbuf {
    const uint8_t *p;
    uint32_t len;
    uint32_t off;
    int ok;
};

static struct rbuf rb_new(const uint8_t *p, uint32_t len)
{
    struct rbuf r = {p, len, 0, 1};
    return r;
}

static int rb_take(struct rbuf *r, uint32_t n)
{
    if (!r->ok || n > r->len - r->off) {
        r->ok = 0;
        return 0;
    }
    return 1;
}

static uint64_t rb_u64(struct rbuf *r)
{
    if (!rb_take(r, 8))
        return 0;
    const uint8_t *p = r->p + r->off;
    r->off += 8;
    return ((uint64_t) p[0] << 56) | ((uint64_t) p[1] << 48) | ((uint64_t) p[2] << 40) |
           ((uint64_t) p[3] << 32) | ((uint64_t) p[4] << 24) | ((uint64_t) p[5] << 16) |
           ((uint64_t) p[6] << 8) | (uint64_t) p[7];
}

static uint32_t rb_u32(struct rbuf *r)
{
    if (!rb_take(r, 4))
        return 0;
    const uint8_t *p = r->p + r->off;
    r->off += 4;
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) | ((uint32_t) p[2] << 8) |
           (uint32_t) p[3];
}

static int32_t rb_i32(struct rbuf *r)
{
    return (int32_t) rb_u32(r);
}

/* rb_bytes returns a pointer to n bytes and advances past them, or NULL. */
static const uint8_t *rb_bytes(struct rbuf *r, uint32_t n)
{
    if (!rb_take(r, n))
        return NULL;
    const uint8_t *p = r->p + r->off;
    r->off += n;
    return p;
}

static void rb_attr(struct rbuf *r, struct etcfs_attr *a)
{
    a->ino = rb_u64(r);
    a->size = rb_u64(r);
    a->blocks = rb_u64(r);
    a->atime = rb_u64(r);
    a->mtime = rb_u64(r);
    a->ctime = rb_u64(r);
    a->atime_nsec = rb_u32(r);
    a->mtime_nsec = rb_u32(r);
    a->ctime_nsec = rb_u32(r);
    a->mode = rb_u32(r);
    a->nlink = rb_u32(r);
    a->uid = rb_u32(r);
    a->gid = rb_u32(r);
    a->rdev = rb_u32(r);
    a->blksize = rb_u32(r);
}

/* ---- binary writers for building request payloads ---- */

static uint32_t wb_u64(uint8_t *buf, uint64_t v)
{
    buf[0] = (uint8_t) (v >> 56);
    buf[1] = (uint8_t) (v >> 48);
    buf[2] = (uint8_t) (v >> 40);
    buf[3] = (uint8_t) (v >> 32);
    buf[4] = (uint8_t) (v >> 24);
    buf[5] = (uint8_t) (v >> 16);
    buf[6] = (uint8_t) (v >> 8);
    buf[7] = (uint8_t) v;
    return 8;
}
static uint32_t wb_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (uint8_t) (v >> 24);
    buf[1] = (uint8_t) (v >> 16);
    buf[2] = (uint8_t) (v >> 8);
    buf[3] = (uint8_t) v;
    return 4;
}

/* wb_creds appends the caller's identity, which the backend stores as the
 * owner of whatever it is about to create.  Every creating operation carries
 * it; without it every file in the filesystem ends up owned by one hardcoded
 * uid regardless of who made it. */
static uint32_t wb_creds(uint8_t *buf, fuse_req_t req)
{
    const struct fuse_ctx *c = fuse_req_ctx(req);
    uint32_t off = 0;
    off += wb_u32(buf + off, (uint32_t) c->uid);
    off += wb_u32(buf + off, (uint32_t) c->gid);
    return off;
}

static void fill_stat(struct stat *st, const struct etcfs_attr *a)
{
    memset(st, 0, sizeof(*st));
    st->st_ino = a->ino;
    st->st_mode = a->mode;
    st->st_nlink = a->nlink;
    st->st_uid = a->uid;
    st->st_gid = a->gid;
    st->st_size = (off_t) a->size;
    st->st_blksize = a->blksize;
    st->st_blocks = (blkcnt_t) a->blocks;
    st->st_rdev = a->rdev;
    st->st_atime = (time_t) a->atime;
    st->st_mtime = (time_t) a->mtime;
    st->st_ctime = (time_t) a->ctime;
    /* The backend keeps timestamps to the nanosecond, and st_atime and
     * st_atim.tv_sec are the same field: writing only the seconds leaves the
     * sub-second half at the zero memset put there, which is what made every
     * utimensat with a fractional time read back rounded down. */
    st->st_atim.tv_nsec = (long) a->atime_nsec;
    st->st_mtim.tv_nsec = (long) a->mtime_nsec;
    st->st_ctim.tv_nsec = (long) a->ctime_nsec;
}

/* ---- helper: this thread's IPC connection ---- */

/* The handlers run on FUSE worker threads, each with its own connection to the
 * backend (see etcfs_ipc_fd).  The context's ipc_fd belongs to the thread that
 * started the daemon and is not used to serve requests. */
#define FD(ctx) etcfs_ipc_fd()

/*
 * ipc_call — one request/response exchange plus the errno check every handler
 * opens with.  Returns 1 when the reply is the caller's to read from rb (and
 * to free through resp), 0 when it has already answered the request.
 *
 * The three blocks it replaces — send, "did the socket fail", "did the daemon
 * return an errno" — were written out at every one of the twenty-odd
 * operations in this file, which is twenty chances to leak resp on one path or
 * to reply twice on another.
 */
static int ipc_call(fuse_req_t req, uint16_t op, const uint8_t *payload, uint32_t plen,
                    uint8_t **resp, struct rbuf *rb)
{
    uint32_t rlen = 0;
    *resp = NULL;
    if (ipc_sync(etcfs_ipc_fd(), op, payload, plen, resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return 0;
    }
    *rb = rb_new(*resp, rlen);
    int32_t err = rb_i32(rb);
    if (err != 0) {
        free(*resp);
        *resp = NULL;
        fuse_reply_err(req, -err);
        return 0;
    }
    return 1;
}

/*
 * ipc_reply_status — the whole of an operation whose reply is nothing but its
 * errno: unlink, rmdir, rename, fsync and the rest.  Answers the request in
 * every case, so the caller has nothing left to do.
 */
static void ipc_reply_status(fuse_req_t req, uint16_t op, const uint8_t *payload, uint32_t plen)
{
    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, op, payload, plen, &resp, &rb))
        return;
    int short_reply = !rb.ok;
    free(resp);
    /* A reply too short to hold its own errno word is a broken daemon, not a
     * success: the errno-only handlers used to read that as zero and tell the
     * application the operation had worked. */
    fuse_reply_err(req, short_reply ? EIO : 0);
}

/* File handles are only ever handed back to the kernel, but they are still
 * allocated from one counter by every worker thread at once. */
static uint64_t next_file_handle(struct etcfs_context *ctx)
{
    return __atomic_add_fetch(&ctx->next_fh, 1, __ATOMIC_RELAXED);
}

/* ---- FUSE operation handlers ---- */

static void ec_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    if (parent == FUSE_ROOT_ID &&
        (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strcmp(name, "/") == 0)) {
        struct fuse_entry_param e;
        memset(&e, 0, sizeof(e));
        e.ino = FUSE_ROOT_ID;
        e.attr_timeout = ETCFS_ROOT_ATTR_TIMEOUT;
        e.entry_timeout = ETCFS_ROOT_ENTRY_TIMEOUT;
        e.attr.st_ino = FUSE_ROOT_ID;
        e.attr.st_mode = S_IFDIR | 0755;
        e.attr.st_nlink = 2;
        e.attr.st_size = 4096;
        e.attr.st_blksize = 4096;
        fuse_reply_entry(req, &e);
        return;
    }

    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_LOOKUP, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    /* ep.ino == 0 is a negative entry, not a failure: the daemon answers a
     * name that is not there that way so the kernel can cache the absence for
     * entry_timeout instead of asking again on every probe.  fuse_reply_entry
     * turns it back into ENOENT for the caller. */
    fuse_reply_entry(req, &ep);
}

static void ec_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) fi;
    if (ino == FUSE_ROOT_ID || ino == 0) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = FUSE_ROOT_ID;
        st.st_mode = S_IFDIR | 0755;
        st.st_nlink = 2;
        st.st_uid = getuid();
        st.st_gid = getgid();
        st.st_size = 4096;
        st.st_blksize = 4096;
        fuse_reply_attr(req, &st, ETCFS_ROOT_ATTR_TIMEOUT);
        return;
    }
    uint8_t payload[8];
    wb_u64(payload, ino);
    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_GETATTR, payload, 8, &resp, &rb))
        return;
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    uint32_t to = rb_u32(&rb);
    free(resp);
    struct stat st;
    fill_stat(&st, &a);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_attr(req, &st, (double) to);
}

static void ec_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[20];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_READDIR, payload, p, &resp, &rb))
        return;

    uint32_t count = rb_u32(&rb);
    size_t bufsz = size > 0 ? size : 4096;
    char *dbuf = malloc(bufsz + 512);
    size_t used = 0;
    uint64_t off_cookie = (uint64_t) off;
    for (uint32_t i = 0; i < count; i++) {
        uint64_t di = rb_u64(&rb);
        uint32_t nl = rb_u32(&rb);
        const uint8_t *dn = rb_bytes(&rb, nl);
        if (!dn)
            break; /* short response: stop rather than read past it */
        uint32_t dt = rb_u32(&rb);
        uint64_t d_off = rb_u64(&rb);
        if (d_off <= off_cookie)
            continue; /* skip already-returned entries */
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = di;
        st.st_mode = (dt == 4) ? (S_IFDIR | 0755) : (S_IFREG | 0644);
        size_t sz = fuse_add_direntry(req, dbuf + used, bufsz - used, (const char *) dn, &st,
                                      (off_t) d_off);
        if (sz > bufsz - used)
            break;
        used += sz;
    }
    free(resp);
    fuse_reply_buf(req, dbuf, used);
    free(dbuf);
}

static void ec_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                           struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[20];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_READDIRPLUS, payload, p, &resp, &rb))
        return;

    uint32_t count = rb_u32(&rb);
    size_t bufsz = size > 0 ? size : 4096;
    char *dbuf = malloc(bufsz + 512);
    size_t used = 0;
    uint64_t off_cookie = (uint64_t) off;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t di = rb_u64(&rb);
        uint32_t nl = rb_u32(&rb);
        const uint8_t *dn = rb_bytes(&rb, nl);
        if (!dn)
            break; /* short response: stop rather than read past it */
        uint32_t dt __attribute__((unused)) = rb_u32(&rb);
        uint64_t d_off = rb_u64(&rb);

        /* Consume the whole entry before deciding whether to skip it.  Each
         * readdirplus entry carries an attr block and two timeouts after the
         * offset cookie; skipping straight to the next iteration would leave
         * those bytes unread and desynchronise the parser, turning every
         * following entry into garbage. */
        struct fuse_entry_param ep;
        memset(&ep, 0, sizeof(ep));
        ep.ino = di;
        struct etcfs_attr a;
        rb_attr(&rb, &a);
        ep.entry_timeout = (double) rb_u32(&rb);
        ep.attr_timeout = (double) rb_u32(&rb);
        fill_stat(&ep.attr, &a);

        if (d_off <= off_cookie)
            continue; /* already returned in an earlier call */

        size_t sz =
            fuse_add_direntry_plus(req, dbuf + used, bufsz - used, (const char *) dn, &ep, d_off);
        if (sz > bufsz - used)
            break;
        used += sz;
    }
    free(resp);
    fuse_reply_buf(req, dbuf, used);
    free(dbuf);
}

static void ec_readlink(fuse_req_t req, fuse_ino_t ino)
{
    uint8_t payload[8];
    wb_u64(payload, ino);
    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_READLINK, payload, 8, &resp, &rb))
        return;
    uint32_t tl = rb_u32(&rb);
    const uint8_t *target = rb_bytes(&rb, tl);
    if (!target) {
        fuse_reply_err(req, EIO);
        free(resp);
        return;
    }
    /* fuse_reply_readlink wants a C string, and the wire form is not one. */
    char *path = malloc((size_t) tl + 1);
    if (!path) {
        fuse_reply_err(req, ENOMEM);
        free(resp);
        return;
    }
    memcpy(path, target, tl);
    path[tl] = '\0';
    fuse_reply_readlink(req, path);
    free(path);
    free(resp);
}

static void ec_statfs(fuse_req_t req, fuse_ino_t ino)
{
    (void) ino;
    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_STATFS, NULL, 0, &resp, &rb))
        return;
    struct statvfs sv;
    memset(&sv, 0, sizeof(sv));
    sv.f_blocks = rb_u64(&rb);
    sv.f_bfree = rb_u64(&rb);
    sv.f_bavail = rb_u64(&rb);
    sv.f_files = rb_u64(&rb);
    sv.f_ffree = rb_u64(&rb);
    sv.f_bsize = rb_u32(&rb);
    sv.f_namemax = rb_u32(&rb);
    sv.f_frsize = rb_u32(&rb);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_statfs(req, &sv);
}

static void ec_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    struct etcfs_context *ctx = fuse_req_userdata(req);

    /* The backend has to see every open: O_TRUNC empties the file, and the
     * descriptor is counted there so that unlinking the file's last name can
     * keep the record alive until the last close. */
    uint8_t payload[8 + 4];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u32(payload + off, (uint32_t) fi->flags);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_OPEN, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    struct rbuf rb = rb_new(resp, rlen);
    int32_t e = rb_i32(&rb);
    uint32_t keep_cache = rb_u32(&rb);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    if (e != 0) {
        fuse_reply_err(req, -e);
        return;
    }

    fi->fh = next_file_handle(ctx);
    /* The backend decides, because only it knows whether it can take the pages
     * back: it invalidates them before it yields the inode's lock, and it says
     * no when there is nothing connected to carry that invalidation.  Writes
     * stay write-through either way — FUSE_WRITEBACK_CACHE is not negotiated —
     * so the kernel caches what it reads and nothing it has yet to send. */
    fi->direct_io = keep_cache ? 0 : 1;
    fi->keep_cache = keep_cache ? 1 : 0;
    fuse_reply_open(req, fi);
}

/*
 * ec_opendir — hand back a handle, and let the kernel keep the listing it is
 * about to read.
 *
 * cache_readdir (FOPEN_CACHE_DIR) is what stops a repeated walk of a tree from
 * costing one etcd prefix scan per directory per pass, which is why a warm
 * `find` used to cost exactly what a cold one did.  The kernel drops a cached
 * listing when the directory's i_version moves or its mtime changes, and both
 * happen here:
 *
 *   - every change to a dirent anywhere in the cluster reaches this node as an
 *     INVAL_ENTRY, and the kernel's handling of one bumps the parent's
 *     i_version;
 *   - every create and unlink also moves the parent directory's mtime in etcd,
 *     so a node that missed the notification still drops the listing as soon
 *     as the parent's attributes expire.
 *
 * The second is the reason this does not depend on the notification path being
 * healthy -- but only where the kernel actually re-reads that mtime, which it
 * does when a listing is asked for from the start and FUSE_AUTO_INVAL_DATA was
 * negotiated, and not otherwise.  So the cache is offered only when it was, and
 * never for the root directory: root has no inode record, its attributes are
 * synthesised below with a fixed mtime, and a cached listing of it would have
 * only the notification to invalidate it.
 */
static void ec_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    struct etcfs_context *ctx = fuse_req_userdata(req);
    fi->fh = next_file_handle(ctx);
    fi->cache_readdir = (ino != FUSE_ROOT_ID && etcfs_dir_cache_allowed()) ? 1 : 0;
    fuse_reply_open(req, fi);
}

static void ec_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
                      struct fuse_file_info *fi)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    /* parent + name_len + name + mode + flags + umask + uid + gid */
    uint8_t payload[8 + 4 + MAX_NAME_LEN + 4 * 5];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, (uint32_t) (fi ? fi->flags : 0));
    off += wb_u32(payload + off, (uint32_t) fuse_req_ctx(req)->umask);
    off += wb_creds(payload + off, req);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_CREATE, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    uint32_t keep_cache = rb_u32(&rb);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    struct etcfs_context *ctx = fuse_req_userdata(req);
    fi->fh = next_file_handle(ctx);
    /* Decided by the backend for the same reason an open's is: only it knows
     * whether it can take the pages back before the inode's lock is yielded. */
    fi->direct_io = keep_cache ? 0 : 1;
    fi->keep_cache = keep_cache ? 1 : 0;
    fuse_reply_create(req, &ep, fi);
}

static void ec_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    /* parent + name_len + name + mode + umask + uid + gid */
    uint8_t payload[8 + 4 + MAX_NAME_LEN + 4 * 4];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, (uint32_t) fuse_req_ctx(req)->umask);
    off += wb_creds(payload + off, req);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_MKDIR, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_entry(req, &ep);
}

static void ec_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    uint8_t payload[8 + 4 + MAX_NAME_LEN];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    ipc_reply_status(req, IPC_OP_UNLINK, payload, off);
}

static void ec_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    uint8_t payload[8 + 4 + MAX_NAME_LEN];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    ipc_reply_status(req, IPC_OP_RMDIR, payload, off);
}

static void ec_rename(fuse_req_t req, fuse_ino_t old_parent, const char *old_name,
                      fuse_ino_t new_parent, const char *new_name, unsigned int flags)
{
    size_t olen = strlen(old_name), nlen = strlen(new_name);
    if (olen > MAX_NAME_LEN || nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    /* old_parent + old_len + old + new_parent + new_len + new + flags */
    uint8_t payload[8 + 4 + MAX_NAME_LEN + 8 + 4 + MAX_NAME_LEN + 4];
    uint32_t off = 0;
    off += wb_u64(payload + off, old_parent);
    off += wb_u32(payload + off, (uint32_t) olen);
    memcpy(payload + off, old_name, olen);
    off += (uint32_t) olen;
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, new_name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, flags);

    ipc_reply_status(req, IPC_OP_RENAME, payload, off);
}

static void ec_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off,
                     struct fuse_file_info *fi)
{
    uint8_t *payload = malloc(28 + size);
    uint32_t pos = 0;
    pos += wb_u64(payload + pos, ino);
    pos += wb_u64(payload + pos, (uint64_t) off);
    pos += wb_u32(payload + pos, (uint32_t) size);
    memcpy(payload + pos, buf, size);
    pos += (uint32_t) size;
    /* The backend owns the mode, so it is the only place that can drop the
     * set-user-ID bits this write costs the file. */
    pos += wb_u32(payload + pos, fuse_req_ctx(req)->uid);
    /* The open flags as the kernel attached them to *this write*, which is
     * where O_SYNC and O_DSYNC arrive on a direct-IO mount: the kernel sets
     * them from fuse_write_flags() on every write request, and never sends a
     * FUSE_FSYNC for a synchronous open, because fuse_direct_write_iter does
     * not call generic_write_sync().  The backend decides per write whether it
     * may defer publishing the extent. */
    pos += wb_u32(payload + pos, (uint32_t) (fi ? fi->flags : 0));

    uint8_t *resp;
    uint32_t rlen;
    int ret = ipc_sync(FD(ctx), IPC_OP_WRITE, payload, pos, &resp, &rlen);
    free(payload);
    if (ret < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    struct rbuf rb = rb_new(resp, rlen);
    int32_t e = rb_i32(&rb);
    uint32_t written = rb_u32(&rb);
    free(resp);
    if (e != 0) {
        fuse_reply_err(req, -e);
        return;
    }
    fuse_reply_write(req, (size_t) written);
}

/* Every field SETATTR can change goes over the wire; to_set says which of them
 * the kernel actually means.  Sending only st_size is what made chmod, chown
 * and utimensat succeed while changing nothing. */
static void ec_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set,
                       struct fuse_file_info *fi)
{
    uint8_t payload[76];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, (fi ? fi->fh : 0));
    off += wb_u32(payload + off, (uint32_t) to_set);
    off += wb_u64(payload + off, (uint64_t) attr->st_size);
    off += wb_u32(payload + off, (uint32_t) attr->st_mode);
    off += wb_u32(payload + off, (uint32_t) attr->st_uid);
    off += wb_u32(payload + off, (uint32_t) attr->st_gid);
    off += wb_u64(payload + off, (uint64_t) attr->st_atime);
    off += wb_u64(payload + off, (uint64_t) attr->st_mtime);
    off += wb_u64(payload + off, (uint64_t) attr->st_ctime);
    off += wb_u32(payload + off, (uint32_t) attr->st_atim.tv_nsec);
    off += wb_u32(payload + off, (uint32_t) attr->st_mtim.tv_nsec);
    off += wb_u32(payload + off, (uint32_t) attr->st_ctim.tv_nsec);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_SETATTR, payload, off, &resp, &rb))
        return;
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    uint32_t to = rb_u32(&rb);
    free(resp);
    struct stat st;
    fill_stat(&st, &a);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_attr(req, &st, (double) to);
}

static void ec_symlink(fuse_req_t req, const char *target, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name), tlen = strlen(target);
    if (nlen > MAX_NAME_LEN || tlen > MAX_TARGET_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    /* parent + name_len + name + target_len + target + uid + gid */
    uint8_t payload[8 + 4 + MAX_NAME_LEN + 4 + MAX_TARGET_LEN + 4 * 2];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) tlen);
    memcpy(payload + off, target, tlen);
    off += (uint32_t) tlen;
    off += wb_creds(payload + off, req);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_SYMLINK, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_entry(req, &ep);
}

static void ec_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t new_parent, const char *new_name)
{
    size_t nlen = strlen(new_name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    uint8_t payload[8 + 8 + 4 + MAX_NAME_LEN];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, new_name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_LINK, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_entry(req, &ep);
}

static void ec_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    /* parent + name_len + name + mode + rdev + umask + uid + gid */
    uint8_t payload[8 + 4 + MAX_NAME_LEN + 4 * 5];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, (uint32_t) rdev);
    off += wb_u32(payload + off, (uint32_t) fuse_req_ctx(req)->umask);
    off += wb_creds(payload + off, req);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_MKNOD, payload, off, &resp, &rb))
        return;
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(&rb);
    struct etcfs_attr a;
    rb_attr(&rb, &a);
    ep.entry_timeout = (double) rb_u32(&rb);
    ep.attr_timeout = (double) rb_u32(&rb);
    fill_stat(&ep.attr, &a);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_entry(req, &ep);
}

/* ---- no-op handlers ---- */

/* Release is where an unlinked-but-open file finally goes: the backend counts
 * this node's descriptors, so it is the only place that knows the last one has
 * closed.  A failure is not reported — the descriptor is gone either way, and
 * the leftover record is reclaimed at the next startup. */
static void ec_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[8];
    uint32_t off = wb_u64(payload, ino);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_RELEASE, payload, off, &resp, &rlen) == 0)
        free(resp);

    fuse_reply_err(req, 0);
}
static void ec_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}
/* Ask the backend to publish everything it is holding for this inode, and wait
 * for it.  fsync, fsyncdir and flush go through here: a write is acknowledged before
 * its extent reaches etcd, so neither can be answered locally any more, and a
 * failure has to reach the caller rather than be swallowed. */
static void ec_sync_inode(fuse_req_t req, fuse_ino_t ino)
{
    uint8_t payload[8];
    uint32_t off = wb_u64(payload, ino);

    ipc_reply_status(req, IPC_OP_FSYNC, payload, off);
}

/* close() sends this, so it is where a program that never calls fsync still
 * gets its writes published before the descriptor goes away.
 *
 * That commit is not the durability POSIX declines to promise at close(); it is
 * what makes a file closed on one node readable in full on another.  A peer's
 * stat does not take the inode's lock, so nothing recalls this node's on its
 * behalf, and a peer that reads a size etcd has not been told about stops at
 * the old one.  Answering close() without publishing was measured against the
 * chaos suite and broke exactly that. */
static void ec_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) fi;
    ec_sync_inode(req, ino);
}
/* datasync is ignored: this filesystem has no attribute state that outlives a
 * write independently of the extent publishing it, so both halves of the
 * distinction flush the same thing. */
static void ec_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    (void) datasync;
    (void) fi;
    ec_sync_inode(req, ino);
}
/* Namespace operations commit before they are acknowledged, so the entries
 * themselves need no waiting for.  The directory's own timestamp does: the
 * backend queues it rather than paying a second commit per entry, and a caller
 * that asked for the directory to be durable is asking for that to be
 * published. */
static void ec_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    (void) datasync;
    (void) fi;
    ec_sync_inode(req, ino);
}
static void ec_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                    struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[24];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_READ, payload, p, &resp, &rb))
        return;
    uint32_t dataLen = rb_u32(&rb);
    const uint8_t *data = rb_bytes(&rb, dataLen);
    if (!data) {
        fuse_reply_err(req, EIO);
        free(resp);
        return;
    }
    fuse_reply_buf(req, (const char *) data, dataLen);
    free(resp);
}
static void ec_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length,
                         struct fuse_file_info *fi)
{
    (void) fi;
    if (offset < 0 || length <= 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    uint8_t payload[8 + 4 + 8 + 8];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u64(payload + off, (uint64_t) offset);
    off += wb_u64(payload + off, (uint64_t) length);

    ipc_reply_status(req, IPC_OP_FALLOCATE, payload, off);
}

/* SEEK_DATA and SEEK_HOLE only. The kernel resolves SEEK_SET, SEEK_CUR and
 * SEEK_END itself and never calls this for them. */
static void ec_lseek(fuse_req_t req, fuse_ino_t ino, off_t off, int whence,
                     struct fuse_file_info *fi)
{
    (void) fi;
    if (off < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    uint8_t payload[8 + 8 + 4];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) whence);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_LSEEK, payload, p, &resp, &rb))
        return;
    uint64_t found = rb_u64(&rb);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_lseek(req, (off_t) found);
}

/* ---- extended attributes ----
 *
 * getxattr and listxattr are each called twice by the kernel: once with
 * size 0, which asks only how many bytes the answer needs, and again with a
 * buffer of that size.  The backend does not know which call this is and
 * always returns the whole answer; deciding between fuse_reply_xattr (the
 * size) and fuse_reply_buf (the bytes) is this side's job, and so is the
 * ERANGE that a buffer too small for the answer earns.
 */

static void reply_xattr_buf(fuse_req_t req, const uint8_t *data, uint32_t len, size_t size)
{
    if (size == 0) {
        fuse_reply_xattr(req, len);
        return;
    }
    if (size < len) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    fuse_reply_buf(req, (const char *) data, len);
}

static void ec_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value,
                        size_t size, int flags)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_XATTR_NAME_LEN) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    if (size > MAX_XATTR_VALUE_LEN) {
        fuse_reply_err(req, E2BIG);
        return;
    }

    /* Heap rather than stack: a value may be 64 KiB, which is far past what
     * this thread's stack should be carrying per request. */
    uint32_t plen = 8 + 4 + (uint32_t) nlen + 4 + (uint32_t) size + 4 + 4 * 2;
    uint8_t *payload = malloc(plen);
    if (!payload) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) size);
    memcpy(payload + off, value, size);
    off += (uint32_t) size;
    off += wb_u32(payload + off, (uint32_t) flags);
    off += wb_creds(payload + off, req);

    uint8_t *resp;
    uint32_t rlen;
    int rc = ipc_sync(FD(ctx), IPC_OP_SETXATTR, payload, off, &resp, &rlen);
    free(payload);
    if (rc < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    struct rbuf rb = rb_new(resp, rlen);
    int32_t e = rb_i32(&rb);
    free(resp);
    if (!rb.ok) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_err(req, -e);
}

static void ec_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_XATTR_NAME_LEN) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    uint8_t payload[8 + 4 + MAX_XATTR_NAME_LEN];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_GETXATTR, payload, off, &resp, &rb))
        return;
    uint32_t vlen = rb_u32(&rb);
    const uint8_t *value = rb_bytes(&rb, vlen);
    if (!value) {
        fuse_reply_err(req, EIO);
        free(resp);
        return;
    }
    reply_xattr_buf(req, value, vlen, size);
    free(resp);
}

static void ec_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
    uint8_t payload[8];
    wb_u64(payload, ino);

    uint8_t *resp;
    struct rbuf rb;
    if (!ipc_call(req, IPC_OP_LISTXATTR, payload, 8, &resp, &rb))
        return;
    uint32_t nlen = rb_u32(&rb);
    /* An inode with no attributes answers zero bytes, which rb_bytes reports
     * as NULL without it being a decoding failure -- the empty list is a
     * perfectly ordinary answer and must not become EIO. */
    const uint8_t *names = nlen > 0 ? rb_bytes(&rb, nlen) : (const uint8_t *) "";
    if (!names) {
        fuse_reply_err(req, EIO);
        free(resp);
        return;
    }
    reply_xattr_buf(req, names, nlen, size);
    free(resp);
}

static void ec_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name)
{
    size_t nlen = strlen(name);
    if (nlen > MAX_XATTR_NAME_LEN) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    uint8_t payload[8 + 4 + MAX_XATTR_NAME_LEN + 4 * 2];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_creds(payload + off, req);

    ipc_reply_status(req, IPC_OP_REMOVEXATTR, payload, off);
}

/* ---- op table ---- */

struct fuse_lowlevel_ops *etcfs_fuse_ops(void)
{
    static struct fuse_lowlevel_ops ops;
    memset(&ops, 0, sizeof(ops));
    ops.lookup = ec_lookup;
    ops.getattr = ec_getattr;
    ops.readdir = ec_readdir;
    ops.readdirplus = ec_readdirplus;
    ops.readlink = ec_readlink;
    ops.statfs = ec_statfs;
    ops.open = ec_open;
    ops.opendir = ec_opendir;
    ops.create = ec_create;
    ops.mkdir = ec_mkdir;
    ops.unlink = ec_unlink;
    ops.rmdir = ec_rmdir;
    ops.rename = ec_rename;
    ops.symlink = ec_symlink;
    ops.link = ec_link;
    ops.setattr = ec_setattr;
    ops.write = ec_write;
    ops.mknod = ec_mknod;
    ops.release = ec_release;
    ops.releasedir = ec_releasedir;
    ops.flush = ec_flush;
    ops.fsync = ec_fsync;
    ops.fsyncdir = ec_fsyncdir;
    ops.read = ec_read;
    ops.fallocate = ec_fallocate;
    ops.setxattr = ec_setxattr;
    ops.getxattr = ec_getxattr;
    ops.listxattr = ec_listxattr;
    ops.removexattr = ec_removexattr;
    ops.lseek = ec_lseek;
    /* getlk/setlk are deliberately left unset. libfuse: "if the locking
     * methods are not implemented, the kernel will still allow file locking
     * to work locally." Implementing them takes that job away from the
     * kernel, and the daemon granted every request, so fcntl() locks
     * excluded nothing -- not even two processes on the same node. Unset,
     * fcntl() gets the node-local enforcement flock() already had. */
    return &ops;
}
