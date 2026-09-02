/*
 * fuse.c — FUSE daemon core: session lifecycle, signal handling, IPC setup.
 *
 * Initialises the FUSE session, creates the Unix socket connection to the Go
 * metadata backend, and enters the libfuse event loop.  Actual FUSE operation
 * handlers live in ops.c.
 */

#include "fuse.h"
#include "ops.h"
#include "../block/block.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>

#include <fuse3/fuse_lowlevel.h>

/*
 * Cache invalidation client.
 *
 * Connects to the metadata daemon's notification socket and calls the kernel on
 * its behalf: only this process holds the FUSE session handle.  The wire format
 * is one fixed header, then a name of the length it declares — see the comment
 * at the top of internal/ipc/notify.go for why the length is there.
 *
 * This connection is what makes the kernel's page cache available at all.  The
 * daemon answers an open as cacheable only while a client is connected to take
 * those pages back before it yields the inode's lock, so a mount whose notify
 * client is not up serves every read from the daemon, and says nothing about
 * why.  Hence the retry, and hence the logging on both edges.
 */

#define NOTIFY_HDR_LEN     16
#define NOTIFY_INVAL_ENTRY 1
#define NOTIFY_INVAL_INODE 2
#define NOTIFY_INVAL_ATTR  3

/* Reconnection backoff, in milliseconds: fast enough that losing the race with
 * a daemon still binding its socket costs nothing, slow enough that a daemon
 * that is simply not there does not spin. */
#define NOTIFY_RETRY_MIN_MS 100
#define NOTIFY_RETRY_MAX_MS 5000

/* Set at teardown so the thread stops reconnecting and can be joined.  The
 * session it calls into is destroyed once this returns, so it has to be gone by
 * then rather than merely asked to stop. */
static volatile sig_atomic_t notify_stop;

/* The live connection, so teardown can unblock a thread parked in read().
 * Guarded because the two ends run on different threads. */
static pthread_mutex_t notify_fd_mu = PTHREAD_MUTEX_INITIALIZER;
static int notify_fd = -1;

static void notify_fd_set(int fd)
{
    pthread_mutex_lock(&notify_fd_mu);
    notify_fd = fd;
    pthread_mutex_unlock(&notify_fd_mu);
}

static void notify_fd_shutdown(void)
{
    pthread_mutex_lock(&notify_fd_mu);
    if (notify_fd >= 0)
        shutdown(notify_fd, SHUT_RDWR);
    pthread_mutex_unlock(&notify_fd_mu);
}

/* Reads exactly len bytes, or reports failure.  A stream socket is free to
 * return a message in as many pieces as it likes, and a reader that treats a
 * short read as a framing error drops connections that were perfectly fine. */
static int read_full(int fd, void *buf, size_t len)
{
    uint8_t *p = (uint8_t *) buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n > 0) {
            p += n;
            len -= (size_t) n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return -1;
    }
    return 0;
}

static uint32_t notify_u32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) | ((uint32_t) p[2] << 8) |
           (uint32_t) p[3];
}

static uint64_t notify_u64(const uint8_t *p)
{
    return ((uint64_t) notify_u32(p) << 32) | (uint64_t) notify_u32(p + 4);
}

/* Sleeps in slices so teardown does not have to wait out a full backoff. */
static void notify_sleep_ms(unsigned ms)
{
    while (ms > 0 && !notify_stop) {
        unsigned slice = ms < 50 ? ms : 50;
        struct timespec ts = {.tv_sec = 0, .tv_nsec = (long) slice * 1000000L};
        nanosleep(&ts, NULL);
        ms -= slice;
    }
}

/* ---- the fire-and-forget invalidation queue ---- */

void etcfs_inval_queue_init(struct etcfs_inval_queue *q)
{
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->cv, NULL);
    q->head = 0;
    q->len = 0;
    q->closed = 0;
}

int etcfs_inval_queue_push(struct etcfs_inval_queue *q, const struct etcfs_inval_msg *msg)
{
    int dropped = 0;

    pthread_mutex_lock(&q->mu);
    if (q->closed) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    if (q->len == ETCFS_INVAL_QUEUE_CAP) {
        q->head = (q->head + 1) % ETCFS_INVAL_QUEUE_CAP;
        q->len--;
        dropped = 1;
    }
    q->slot[(q->head + q->len) % ETCFS_INVAL_QUEUE_CAP] = *msg;
    q->len++;
    pthread_cond_signal(&q->cv);
    pthread_mutex_unlock(&q->mu);
    return dropped;
}

int etcfs_inval_queue_pop(struct etcfs_inval_queue *q, struct etcfs_inval_msg *out)
{
    pthread_mutex_lock(&q->mu);
    while (q->len == 0 && !q->closed)
        pthread_cond_wait(&q->cv, &q->mu);
    if (q->len == 0) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    *out = q->slot[q->head];
    q->head = (q->head + 1) % ETCFS_INVAL_QUEUE_CAP;
    q->len--;
    pthread_mutex_unlock(&q->mu);
    return 1;
}

void etcfs_inval_queue_close(struct etcfs_inval_queue *q)
{
    pthread_mutex_lock(&q->mu);
    q->closed = 1;
    pthread_cond_broadcast(&q->cv);
    pthread_mutex_unlock(&q->mu);
}

static struct etcfs_inval_queue inval_queue;

/* Makes the kernel calls for the queued invalidations.  Its own thread so that
 * a slow one delays nothing the backend is waiting on; see the queue's comment
 * in fuse.h.  Not a request thread either, which is what keeps
 * fuse_lowlevel_notify_inval_inode clear of the kernel's writeback of the inode
 * it is invalidating. */
static void *inval_thread(void *arg)
{
    struct etcfs_context *ctx = (struct etcfs_context *) arg;
    struct etcfs_inval_msg msg;

    while (etcfs_inval_queue_pop(&inval_queue, &msg)) {
        if (msg.type == NOTIFY_INVAL_ENTRY)
            fuse_lowlevel_notify_inval_entry(ctx->notify_se, msg.ino, msg.name, msg.nlen);
        else
            /* A negative offset asks for the attributes only: the data pages
             * belong to whoever holds the inode's lock and are dropped
             * separately, before that lock is yielded. */
            fuse_lowlevel_notify_inval_inode(ctx->notify_se, msg.ino, -1, 0);
    }
    return NULL;
}

static int notify_connect(const char *path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    return fd;
}

/* Serves one connection until it closes, fails, or stops making sense. */
static void notify_serve(struct etcfs_context *ctx, int fd)
{
    uint8_t hdr[NOTIFY_HDR_LEN];
    int complained = 0;

    while (!notify_stop && read_full(fd, hdr, sizeof(hdr)) == 0) {
        uint32_t typ = notify_u32(hdr);
        uint64_t ino = notify_u64(hdr + 4);
        uint32_t nlen = notify_u32(hdr + 12);

        /* Nothing sends a longer name, so this header did not begin where a
         * message begins and every later one would be read from the middle of
         * a message.  There is no resynchronising a stream with no delimiters:
         * the connection is dropped and the loop below builds a fresh one. */
        if (nlen > MAX_NAME_LEN) {
            etcfs_log(ETCFS_LOG_ERROR,
                      "cache-invalidation stream is out of step (name length %u); reconnecting",
                      nlen);
            return;
        }

        char name[MAX_NAME_LEN + 1];
        if (nlen > 0 && read_full(fd, name, nlen) < 0)
            return;
        name[nlen] = '\0';

        if (typ == NOTIFY_INVAL_ENTRY || typ == NOTIFY_INVAL_ATTR) {
            /* Queued rather than carried out here.  Nothing waits on either,
             * and the acknowledged message below must not queue behind them —
             * that is the whole reason the queue exists. */
            struct etcfs_inval_msg msg = {.type = typ, .ino = ino, .nlen = nlen};
            memcpy(msg.name, name, nlen + 1);
            if (etcfs_inval_queue_push(&inval_queue, &msg) && !complained) {
                etcfs_log(ETCFS_LOG_WARN,
                          "cache invalidations are arriving faster than the kernel accepts "
                          "them; the oldest are being dropped and the names or attributes "
                          "they covered stay cached on this node until they time out");
                complained = 1;
            }
        } else if (typ == NOTIFY_INVAL_INODE) {
            /* Drop the kernel's data pages for one inode, then acknowledge.
             * The backend is holding that inode's lock open until this reply
             * arrives: a peer that took the inode while pages were still
             * cached here would have its writes hidden behind them, and a page
             * cache has no timeout to fall back on.
             *
             * This runs on the notify thread and must keep doing so.  A
             * request thread calling it can deadlock against the kernel's own
             * writeback of the inode being invalidated. */
            int rc = fuse_lowlevel_notify_inval_inode(ctx->notify_se, ino, 0, 0);
            /* ENOENT means the kernel has no such inode cached, which is the
             * outcome the caller wanted. */
            uint8_t ack = (rc == 0 || rc == -ENOENT) ? 0 : 1;
            if (write(fd, &ack, 1) != 1)
                return;
        } else {
            etcfs_log(ETCFS_LOG_ERROR, "unknown cache-invalidation message type %u; reconnecting",
                      typ);
            return;
        }
    }
}

static void *notify_thread(void *arg)
{
    struct etcfs_context *ctx = (struct etcfs_context *) arg;
    const char *path = getenv("ETCFS_NOTIFY_SOCKET");
    if (!path)
        path = "/run/etcfuse/etcfuse-notify.sock";

    unsigned backoff_ms = NOTIFY_RETRY_MIN_MS;
    int complained = 0;

    while (!notify_stop) {
        int fd = notify_connect(path);
        if (fd < 0) {
            /* Said once per outage rather than once per attempt, at a level the
             * default log setting shows: an unreachable socket here is the
             * difference between a mount that caches data pages and one that
             * does not, and the old code failed at it in silence. */
            if (!complained) {
                etcfs_log(ETCFS_LOG_WARN,
                          "cannot reach the cache-invalidation socket %s (%s); retrying. "
                          "Until this connects the kernel caches none of this mount's file "
                          "data and every read goes to the metadata daemon",
                          path, strerror(errno));
                complained = 1;
            }
            notify_sleep_ms(backoff_ms);
            backoff_ms =
                backoff_ms >= NOTIFY_RETRY_MAX_MS / 2 ? NOTIFY_RETRY_MAX_MS : backoff_ms * 2;
            continue;
        }

        etcfs_log(ETCFS_LOG_INFO, "cache-invalidation client connected to %s", path);
        backoff_ms = NOTIFY_RETRY_MIN_MS;
        complained = 0;

        notify_fd_set(fd);
        notify_serve(ctx, fd);
        notify_fd_set(-1);
        close(fd);

        if (!notify_stop)
            etcfs_log(ETCFS_LOG_WARN,
                      "the cache-invalidation connection dropped; reconnecting. Data pages "
                      "stay uncached until it is back");
    }
    return NULL;
}

/* ---- logging ---- */

static int etcfs_log_level = ETCFS_LOG_INFO;

void etcfs_set_log_level(int level);
void etcfs_set_log_level(int level)
{
    etcfs_log_level = level;
}

void etcfs_log(int level, const char *fmt, ...)
{
    if (level > etcfs_log_level)
        return;

    const char *prefix;
    switch (level) {
    case ETCFS_LOG_ERROR:
        prefix = "ERROR";
        break;
    case ETCFS_LOG_WARN:
        prefix = "WARN";
        break;
    case ETCFS_LOG_INFO:
        prefix = "INFO";
        break;
    case ETCFS_LOG_DEBUG:
        prefix = "DEBUG";
        break;
    default:
        prefix = "???";
        break;
    }

    fprintf(stderr, "[etcfuse] %s: ", prefix);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}

/* ---- IPC connection to Go backend ---- */

static int connect_to_meta(const char *socket_path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        etcfs_log(ETCFS_LOG_ERROR, "socket: %s", strerror(errno));
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        etcfs_log(ETCFS_LOG_ERROR, "connect to %s: %s", socket_path, strerror(errno));
        close(fd);
        return -1;
    }

    etcfs_log(ETCFS_LOG_INFO, "connected to metadata backend at %s fd=%d", socket_path, fd);
    return fd;
}

/* ---- per-thread IPC connections ---- */

/*
 * Every FUSE worker thread gets its own connection to the metadata backend.
 * The wire protocol carries no request identifiers — a reply is whatever
 * arrives next on the socket — so a shared fd would let two threads interleave
 * their frames and take each other's replies.  One fd per thread keeps every
 * exchange in ops.c exactly as synchronous as it was when the daemon ran one
 * request at a time; the Go side already serves a goroutine per connection.
 */
static pthread_key_t ipc_fd_key;
static pthread_once_t ipc_fd_once = PTHREAD_ONCE_INIT;
static char ipc_socket_path[108];

/* The path every connection is made to: what etcfs_run resolved, falling back
 * to the environment so a worker can connect before (or without) the daemon
 * having run. */
static const char *ipc_socket(void)
{
    if (ipc_socket_path[0] != '\0')
        return ipc_socket_path;
    const char *env = getenv("ETCFS_IPC_SOCKET");
    return env ? env : "/tmp/etcfuse.sock";
}

static void ipc_fd_destroy(void *value)
{
    int fd = (int) (intptr_t) value;
    if (fd >= 0)
        close(fd);
}

static void ipc_fd_key_create(void)
{
    if (pthread_key_create(&ipc_fd_key, ipc_fd_destroy) != 0)
        etcfs_log(ETCFS_LOG_ERROR, "pthread_key_create for IPC connections failed");
}

int etcfs_ipc_fd(void)
{
    pthread_once(&ipc_fd_once, ipc_fd_key_create);

    /* A stored fd is offset by one so that a thread that has not connected yet
     * (NULL) stays distinguishable from one holding fd 0. */
    intptr_t stored = (intptr_t) pthread_getspecific(ipc_fd_key);
    if (stored > 0)
        return (int) (stored - 1);

    int fd = connect_to_meta(ipc_socket());
    if (fd < 0)
        return -1;
    if (pthread_setspecific(ipc_fd_key, (void *) (intptr_t) (fd + 1)) != 0) {
        etcfs_log(ETCFS_LOG_ERROR, "cannot store this thread's IPC connection");
        close(fd);
        return -1;
    }
    return fd;
}

void etcfs_ipc_drop(void)
{
    pthread_once(&ipc_fd_once, ipc_fd_key_create);

    intptr_t stored = (intptr_t) pthread_getspecific(ipc_fd_key);
    if (stored <= 0)
        return;
    pthread_setspecific(ipc_fd_key, NULL);
    close((int) (stored - 1));
    etcfs_log(ETCFS_LOG_WARN, "dropped this thread's IPC connection; will reconnect");
}

/* ---- FUSE init callback ---- */

/* Whether the kernel agreed to FUSE_AUTO_INVAL_DATA, which is the whole reason
 * a cached directory listing is safe to hand out.  Without it the kernel never
 * re-reads a directory's mtime when a listing it has cached is asked for again,
 * so that listing is invalidated only by an INVAL_ENTRY notification — and
 * nothing then bounds how long a notification that never arrived leaves it
 * stale.  Read by ec_opendir through etcfs_dir_cache_allowed. */
static int dir_cache_allowed;

int etcfs_dir_cache_allowed(void)
{
    return dir_cache_allowed;
}

static void etcfs_init(void *userdata, struct fuse_conn_info *conn)
{
    (void) userdata;
    conn->want |= FUSE_CAP_READDIRPLUS;
    conn->want |= FUSE_CAP_ASYNC_READ;

    /* Asked for explicitly even though libfuse enables it by default: the
     * directory-listing cache's only bound on a missed invalidation depends on
     * it, and a safety argument should not rest on a default nothing in this
     * source mentions.  conn->capable is what the kernel offered, so a kernel
     * too old to offer it turns the listing cache off rather than leaving it
     * running with nothing to bound it. */
    conn->want |= FUSE_CAP_AUTO_INVAL_DATA;
    dir_cache_allowed = (conn->capable & FUSE_CAP_AUTO_INVAL_DATA) != 0;
    if (!dir_cache_allowed)
        etcfs_log(ETCFS_LOG_WARN,
                  "this kernel does not offer FUSE_AUTO_INVAL_DATA, so a cached directory "
                  "listing could outlive a lost invalidation without bound; "
                  "directory listings will not be cached");
}

/* Runs one command and waits for it, without a shell.
 *
 * The mountpoint reaches this from argv, and this daemon runs as root: built
 * into a string for system(), a mountpoint carrying shell metacharacters would
 * be interpreted rather than passed along, and one longer than the buffer would
 * be truncated into a different path to unmount. execvp takes the argument as
 * an argument, so neither is possible.
 *
 * argv[0] names the program and the vector is NULL-terminated. Failures are
 * deliberately not reported: both callers are best-effort cleanup of a stale
 * mount, and whether it worked is decided by the mount retry that follows. */
static void run_quiet(const char *argv[])
{
    pid_t pid = fork();
    if (pid < 0)
        return;

    if (pid == 0) {
        /* stderr to /dev/null, as the shell redirect these calls replaced did:
         * unmounting a mountpoint that turns out not to be stale is an
         * expected outcome, not something to print. */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }
        execvp(argv[0], (char *const *) argv);
        _exit(127);
    }

    int status;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR)
        ;
}

/* ---- main entry ---- */

int etcfs_run(struct etcfs_context *ctx)
{
    struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
    struct fuse_session *se;
    char *mountpoint;
    int ipc_fd;
    int ret;

    /* get the FUSE op table (populated in ops.c) */
    struct fuse_lowlevel_ops *ops = etcfs_fuse_ops();

    /* A daemon that dies makes the next write to its socket raise SIGPIPE,
     * whose default action would kill the mount outright.  Ignored so that the
     * write fails with EPIPE instead and the connection can be re-established. */
    signal(SIGPIPE, SIG_IGN);

    /* connect to Go metadata backend */
    const char *socket_path = getenv("ETCFS_IPC_SOCKET");
    if (!socket_path)
        socket_path = "/tmp/etcfuse.sock";

    /* Kept for every worker thread to connect with; the one opened here only
     * proves the backend is reachable before the mountpoint is taken over. */
    snprintf(ipc_socket_path, sizeof(ipc_socket_path), "%s", socket_path);

    ipc_fd = connect_to_meta(socket_path);
    if (ipc_fd < 0)
        return -1;

    ctx->ipc_fd = ipc_fd;
    ctx->ipc_sync = 1; /* synchronous IPC mode */

    mountpoint = ctx->mountpoint;
    if (!mountpoint) {
        etcfs_log(ETCFS_LOG_ERROR, "mountpoint not set");
        return -1;
    }

    /* open block device if specified */
    if (ctx->volume_id) {
        ctx->block_fd = -1;
        struct etcfs_block_dev *block_dev = etcfs_block_open(ctx->volume_id);
        (void) block_dev; /* access through ctx->block_fd when I/O is implemented */
        etcfs_log(ETCFS_LOG_WARN, "block device not available (read-only mode)");
    }

    /* build FUSE args — mountpoint passed to fuse_session_mount, not in args */
    if (fuse_opt_add_arg(&args, "etcfuse") < 0)
        return -1;

    /* default_permissions hands permission checking to the kernel, which
     * evaluates the mode, uid and gid this daemon reports against the calling
     * process before the request is ever sent.  EtcFS deliberately implements
     * no access checks of its own: duplicating them in the daemon would mean a
     * second, divergent copy of rules the kernel already applies correctly.
     *
     * allow_other is required alongside it: the daemon runs as root, and
     * without allow_other FUSE restricts the mount to the mounting user only
     * — every other user, including one connecting over SSH to use the mount,
     * gets EACCES on the mountpoint itself regardless of default_permissions.
     * It is safe together with default_permissions because the kernel still
     * enforces per-file mode/uid/gid on every access; allow_other only lifts
     * the FUSE-level single-user restriction, it does not bypass Unix
     * permissions. */
    if (fuse_opt_add_arg(&args, "-o") < 0)
        return -1;
    if (fuse_opt_add_arg(&args, "default_permissions,allow_other") < 0)
        return -1;

    /* register init callback */
    ops->init = etcfs_init;

    /* create session */
    se = fuse_session_new(&args, ops, sizeof(*ops), ctx);
    if (!se) {
        etcfs_log(ETCFS_LOG_ERROR, "fuse_session_new failed");
        fuse_opt_free_args(&args);
        return -1;
    }

    fuse_opt_free_args(&args);

    /* check for a stale FUSE mount from a previous daemon instance.
     * if the mountpoint is already occupied by a dead FUSE mount,
     * the kernel will reject or corrupt the new session.
     * scan /proc/mounts to detect this and force-unmount if needed. */
    {
        FILE *fp = fopen("/proc/mounts", "r");
        if (fp) {
            char line[1024];
            while (fgets(line, sizeof(line), fp)) {
                char dev[256], mp[256], fst[256];
                if (sscanf(line, "%255s %255s %255s", dev, mp, fst) == 3) {
                    if (strcmp(mp, mountpoint) == 0 &&
                        (strcmp(fst, "fuse") == 0 || strcmp(fst, "fuseblk") == 0)) {
                        etcfs_log(ETCFS_LOG_WARN,
                                  "stale FUSE mount at %s (previous daemon crash?), cleaning up",
                                  mountpoint);
                        fclose(fp);
                        /* force-unmount the stale mount */
                        run_quiet((const char *[]){"fusermount", "-uz", mountpoint, NULL});
                        run_quiet((const char *[]){"umount", "-l", mountpoint, NULL});
                        goto after_cleanup;
                    }
                }
            }
            fclose(fp);
        }
    }
after_cleanup:

    /* mount — retry up to 5 times if the kernel hasn't released a stale
     * FUSE superblock from a just-cleaned-up previous daemon instance. */
    {
        int mounted = -1;
        for (int attempt = 0; attempt < 5; attempt++) {
            if (fuse_session_mount(se, mountpoint) == 0) {
                mounted = 0;
                break;
            }
            etcfs_log(ETCFS_LOG_WARN, "fuse_session_mount attempt %d failed, retrying (2s)",
                      attempt + 1);
            sleep(2);
        }
        if (mounted != 0) {
            etcfs_log(ETCFS_LOG_ERROR, "fuse_session_mount failed after 5 attempts");
            fuse_remove_signal_handlers(se);
            fuse_session_destroy(se);
            return -1;
        }
    }

    etcfs_log(ETCFS_LOG_INFO, "EtcFS mounted at %s", mountpoint);

    ctx->notify_se = se;
    etcfs_inval_queue_init(&inval_queue);
    pthread_t ntid, itid;
    pthread_create(&ntid, NULL, notify_thread, ctx);
    pthread_create(&itid, NULL, inval_thread, ctx);

    /*
     * Multi-threaded: each worker takes its own IPC connection from
     * etcfs_ipc_fd(), so the synchronous exchanges in ops.c never share a
     * socket.  One slow etcd operation now blocks the request that issued it
     * rather than the whole mount.
     *
     * clone_fd gives every worker its own /dev/fuse descriptor, which is what
     * stops the kernel-side read of the request queue from becoming the next
     * serialisation point once the IPC one is gone.
     */
    {
        struct fuse_loop_config loop_config;
        memset(&loop_config, 0, sizeof(loop_config));
        loop_config.clone_fd = 1;
        loop_config.max_idle_threads = ETCFS_MAX_THREADS;
        ret = fuse_session_loop_mt(se, &loop_config);
    }

    /* cleanup — the notify thread first.  It calls into the session on every
     * message, and it now reconnects rather than exiting at the first failure,
     * so leaving it running would let it enter a session that is being
     * destroyed underneath it.  The shutdown is what unblocks it: it spends
     * almost all of its life parked in a read that nothing else will end. */
    notify_stop = 1;
    notify_fd_shutdown();
    pthread_join(ntid, NULL);
    /* The reader is gone, so nothing can enqueue past this point; what is still
     * queued is for caches the session about to be destroyed owns anyway. */
    etcfs_inval_queue_close(&inval_queue);
    pthread_join(itid, NULL);

    fuse_session_unmount(se);
    fuse_session_destroy(se);
    close(ipc_fd);
    etcfs_log(ETCFS_LOG_INFO, "EtcFS unmounted");
    return ret;
}
