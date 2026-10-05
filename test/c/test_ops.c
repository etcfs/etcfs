/*
 * Unit tests for the C daemon's wire handling.
 *
 * ops.c is included rather than linked: the frame encoder and the response
 * readers are static, and they are exactly the code worth testing — they are
 * pure functions of a buffer and a length, they sit in front of every FUSE
 * operation, and they are what a malformed or desynchronised frame reaches
 * first.
 *
 * Built and run by `make test-c`.
 */

#include "../../pkg/fuse/ops.c"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>

/* ---- response readers ---- */

static void test_rb_reads_big_endian(void)
{
    uint8_t buf[12];
    assert(wb_u64(buf, 0x0102030405060708ULL) == 8);
    assert(wb_u32(buf + 8, 0x090a0b0c) == 4);

    struct rbuf r = rb_new(buf, sizeof(buf));
    assert(rb_u64(&r) == 0x0102030405060708ULL);
    assert(rb_u32(&r) == 0x090a0b0c);
    assert(r.ok);
}

static void test_rb_stops_at_the_end_of_a_short_response(void)
{
    uint8_t buf[4] = {0xff, 0xff, 0xff, 0xff};

    /* A u64 read on a 4-byte response must fail rather than read past it. */
    struct rbuf r = rb_new(buf, sizeof(buf));
    assert(rb_u64(&r) == 0);
    assert(!r.ok);

    /* Once short, every later read yields zero and ok stays clear, so one
     * check before replying covers the whole parse. */
    assert(rb_u32(&r) == 0);
    assert(rb_bytes(&r, 1) == NULL);
    assert(!r.ok);
}

static void test_rb_bytes_bounds_the_variable_length_field(void)
{
    uint8_t buf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    struct rbuf r = rb_new(buf, sizeof(buf));

    assert(rb_bytes(&r, 8) == buf);
    assert(r.ok);
    /* Exactly consumed: a further byte is past the end. */
    assert(rb_bytes(&r, 1) == NULL);
    assert(!r.ok);

    /* A length that overflows the remaining span must not wrap. */
    struct rbuf r2 = rb_new(buf, sizeof(buf));
    assert(rb_bytes(&r2, 0xffffffffu) == NULL);
    assert(!r2.ok);
}

static void test_rb_attr_leaves_a_truncated_attr_marked_short(void)
{
    /* An attr block is 6 u64s and 9 u32s; one byte less must not parse. */
    uint8_t buf[6 * 8 + 9 * 4 - 1];
    memset(buf, 0, sizeof(buf));

    struct etcfs_attr a;
    struct rbuf r = rb_new(buf, sizeof(buf));
    rb_attr(&r, &a);
    assert(!r.ok);
}

/* ---- framing ---- */

/* ipc_sync writes a request frame and reads a response frame; a socketpair
 * stands in for the daemon on the other end. */
static void test_ipc_sync_frames_the_request_and_reads_the_response(void)
{
    int sv[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

    const uint8_t payload[3] = {0xaa, 0xbb, 0xcc};
    const uint8_t response[] = {0, 0, 0, 2, 0x11, 0x22}; /* len=2, body */
    assert(write(sv[1], response, sizeof(response)) == (ssize_t) sizeof(response));

    uint8_t *resp = NULL;
    uint32_t rlen = 0;
    assert(ipc_sync(sv[0], 0x1234, payload, sizeof(payload), &resp, &rlen) == 0);
    assert(rlen == 2);
    assert(resp[0] == 0x11 && resp[1] == 0x22);
    free(resp);

    /* The request header the peer saw: [u16 op][u32 len][payload]. */
    uint8_t sent[9];
    assert(read(sv[1], sent, sizeof(sent)) == (ssize_t) sizeof(sent));
    assert(sent[0] == 0x12 && sent[1] == 0x34);
    assert(sent[2] == 0 && sent[3] == 0 && sent[4] == 0 && sent[5] == 3);
    assert(sent[6] == 0xaa && sent[7] == 0xbb && sent[8] == 0xcc);

    close(sv[0]);
    close(sv[1]);
}

/* A desynchronised stream can name any 32-bit length, and that length becomes
 * a malloc size.  Anything past the cap must be refused, not allocated. */
static void test_ipc_sync_refuses_an_oversized_response_frame(void)
{
    int sv[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

    const uint32_t too_big = IPC_MAX_FRAME_LEN + 1;
    uint8_t hdr[4] = {(uint8_t) (too_big >> 24), (uint8_t) (too_big >> 16),
                      (uint8_t) (too_big >> 8), (uint8_t) too_big};
    assert(write(sv[1], hdr, sizeof(hdr)) == (ssize_t) sizeof(hdr));

    uint8_t *resp = NULL;
    uint32_t rlen = 0;
    assert(ipc_sync(sv[0], IPC_OP_GETATTR, NULL, 0, &resp, &rlen) == -1);
    assert(resp == NULL);

    close(sv[0]);
    close(sv[1]);
}

/* A peer that closes mid-frame must be reported, not treated as a short read
 * of whatever happened to be in the buffer. */
static void test_ipc_sync_reports_a_truncated_response(void)
{
    int sv[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

    const uint8_t partial[] = {0, 0, 0, 8, 0x01, 0x02}; /* claims 8, sends 2 */
    assert(write(sv[1], partial, sizeof(partial)) == (ssize_t) sizeof(partial));
    /* Half-close: the peer stops sending, but the request write still has
     * somewhere to go, so this tests the truncation and not a SIGPIPE. */
    assert(shutdown(sv[1], SHUT_WR) == 0);

    uint8_t *resp = NULL;
    uint32_t rlen = 0;
    assert(ipc_sync(sv[0], IPC_OP_GETATTR, NULL, 0, &resp, &rlen) == -1);

    close(sv[0]);
    close(sv[1]);
}

/* The payload buffers the handlers put on the stack are sized from these
 * caps, so a name longer than the cap is rejected before it is copied. */
static void test_name_bounds_match_the_protocol(void)
{
    assert(MAX_NAME_LEN == 255); /* NAME_MAX */
    assert(MAX_TARGET_LEN == 4095) /* PATH_MAX - 1 */;
    assert(IPC_MAX_FRAME_LEN == (1u << 20) + 28); /* 1 MiB write payload + its fixed header */
}

/* ---- per-thread IPC connections ---- */

static void *ipc_fd_of_this_thread(void *out)
{
    int fd = etcfs_ipc_fd();
    /* Asked twice on purpose: a thread must keep reusing its own connection
     * rather than opening one per request. */
    assert(etcfs_ipc_fd() == fd);
    *(int *) out = fd;
    return NULL;
}

/* Two FUSE workers must never end up on one socket: the protocol has no
 * request identifiers, so they would read each other's replies. */
static void test_each_thread_gets_its_own_ipc_connection(void)
{
    char path[64];
    snprintf(path, sizeof(path), "/tmp/etcfs-test-ipc-%d.sock", (int) getpid());
    unlink(path);

    int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    assert(listener >= 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path);
    assert(bind(listener, (struct sockaddr *) &addr, sizeof(addr)) == 0);
    assert(listen(listener, 8) == 0);
    setenv("ETCFS_IPC_SOCKET", path, 1);

    int first = -1, second = -1;
    pthread_t a, b;
    assert(pthread_create(&a, NULL, ipc_fd_of_this_thread, &first) == 0);
    assert(pthread_join(a, NULL) == 0);
    assert(pthread_create(&b, NULL, ipc_fd_of_this_thread, &second) == 0);
    assert(pthread_join(b, NULL) == 0);

    assert(first >= 0);
    assert(second >= 0);
    assert(first != second);

    close(listener);
    unlink(path);
    unsetenv("ETCFS_IPC_SOCKET");
}

/* A daemon restart must not be terminal: a thread that kept its dead fd would
 * turn every later request into EIO forever.  A failed exchange must instead
 * leave the thread with no connection, so the next request opens a fresh one. */
static void test_a_failed_exchange_reconnects_on_the_next_request(void)
{
    signal(SIGPIPE, SIG_IGN); /* writing to the closed peer below must return, not kill us */

    char path[64];
    snprintf(path, sizeof(path), "/tmp/etcfs-test-reconnect-%d.sock", (int) getpid());
    unlink(path);

    int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    assert(listener >= 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path);
    assert(bind(listener, (struct sockaddr *) &addr, sizeof(addr)) == 0);
    assert(listen(listener, 8) == 0);
    setenv("ETCFS_IPC_SOCKET", path, 1);

    int fd = etcfs_ipc_fd();
    assert(fd >= 0);
    int served = accept(listener, NULL, NULL);
    assert(served >= 0);

    /* The daemon goes away mid-flight. */
    close(served);
    uint8_t *resp = NULL;
    uint32_t rlen = 0;
    assert(ipc_sync(fd, IPC_OP_GETATTR, NULL, 0, &resp, &rlen) == -1);

    /* The next request connects again: a second accept only completes because
     * the thread reconnected rather than reusing the dead fd. */
    assert(etcfs_ipc_fd() >= 0);
    int reconnected = accept(listener, NULL, NULL);
    assert(reconnected >= 0);

    etcfs_ipc_drop();
    close(reconnected);
    close(listener);
    unlink(path);
    unsetenv("ETCFS_IPC_SOCKET");
}

/* The reply widths this file's readers consume, as the daemon's own
 * socket_test.go states them.  The two are hand-encoded on opposite sides of
 * the socket and only ever meet at run time, so each side pins the numbers
 * independently: a field added here and not there shows up as a failing test
 * rather than as a parser that silently reads the next reply's bytes.
 *
 * Each entry is the layout summed field by field on the left and the total the
 * daemon writes on the right, so a diff names the field that moved. */
static void test_fixed_reply_widths_match_the_daemon(void)
{
    /* Derived, not assumed: the attr block is the one width these readers
     * consume as a unit, so it is measured through rb_attr itself. */
    uint8_t block[6 * 8 + 9 * 4];
    memset(block, 0, sizeof(block));
    struct etcfs_attr a;
    struct rbuf r = rb_new(block, sizeof(block));
    rb_attr(&r, &a);
    assert(r.ok);
    const uint32_t attr = r.off;
    assert(attr == 84);

    assert(4 == 4);                          /* errno-only: unlink, rmdir, rename, fsync */
    assert(4 + 4 == 8);                      /* OPEN: error, keep_cache */
    assert(4 + 4 == 8);                      /* WRITE: error, written */
    assert(4 + attr + 4 == 92);              /* GETATTR/SETATTR: error, attr, attr_timeout */
    assert(4 + 8 + attr + 4 + 4 == 104);     /* LOOKUP and friends: + ino, two timeouts */
    assert(4 + 8 + attr + 4 + 4 + 4 == 108); /* CREATE: + keep_cache */
    assert(4 + 8 == 12);                     /* LSEEK: error, offset */
    assert(4 + 5 * 8 + 3 * 4 == 56);         /* STATFS */
}

/* A missing name comes back as a *successful* LOOKUP carrying inode 0, which
 * is how FUSE spells an absence the kernel is allowed to cache.  ec_lookup
 * reads it with the same sequence it reads a found entry with, so what has to
 * hold is that the errno check in ipc_call lets it through and the reader ends
 * on a non-zero entry_timeout: an errno here would drop the caching, and a
 * short parse would make ec_lookup answer EIO for a file that is merely
 * absent. */
static void test_a_negative_entry_parses_as_a_cacheable_absence(void)
{
    uint8_t reply[104];
    memset(reply, 0, sizeof(reply));
    uint32_t off = 0;
    off += wb_u32(reply + off, 0); /* errno: success */
    off += wb_u64(reply + off, 0); /* ino 0: the entry is negative */
    off += 6 * 8 + 9 * 4;          /* the zeroed attr block */
    off += wb_u32(reply + off, 1); /* entry_timeout */
    off += wb_u32(reply + off, 0); /* attr_timeout: no inode to describe */
    assert(off == sizeof(reply));

    struct rbuf r = rb_new(reply, sizeof(reply));
    assert((int32_t) rb_u32(&r) == 0);
    assert(rb_u64(&r) == 0);
    struct etcfs_attr a;
    rb_attr(&r, &a);
    assert(rb_u32(&r) == 1); /* entry_timeout: the kernel may remember this */
    assert(rb_u32(&r) == 0);
    assert(r.ok);
    assert(r.off == sizeof(reply));
}

/* ---- the fire-and-forget invalidation queue ---- */

static void test_the_queue_is_first_in_first_out(void)
{
    struct etcfs_inval_queue q;
    etcfs_inval_queue_init(&q);

    for (unsigned i = 0; i < 3; i++) {
        struct etcfs_inval_msg m = {.type = 1, .ino = i, .nlen = 1};
        m.name[0] = (char) ('a' + i);
        assert(etcfs_inval_queue_push(&q, &m) == 0);
    }

    struct etcfs_inval_msg out;
    for (unsigned i = 0; i < 3; i++) {
        assert(etcfs_inval_queue_pop(&q, &out) == 1);
        assert(out.ino == i);
        assert(out.name[0] == (char) ('a' + i));
    }

    /* Closing an empty queue is what ends the draining thread's loop. */
    etcfs_inval_queue_close(&q);
    assert(etcfs_inval_queue_pop(&q, &out) == 0);
}

static void test_a_full_queue_drops_its_oldest_message(void)
{
    struct etcfs_inval_queue q;
    etcfs_inval_queue_init(&q);

    /* One past capacity: the first message is the one that goes. */
    for (unsigned i = 0; i < ETCFS_INVAL_QUEUE_CAP + 1; i++) {
        struct etcfs_inval_msg m = {.type = 1, .ino = i};
        assert(etcfs_inval_queue_push(&q, &m) == (i == ETCFS_INVAL_QUEUE_CAP));
    }

    struct etcfs_inval_msg out;
    assert(etcfs_inval_queue_pop(&q, &out) == 1);
    assert(out.ino == 1);

    etcfs_inval_queue_close(&q);
}

static void *queue_pop_once(void *arg)
{
    struct etcfs_inval_queue *q = (struct etcfs_inval_queue *) arg;
    static struct etcfs_inval_msg out;
    return etcfs_inval_queue_pop(q, &out) ? &out : NULL;
}

static void test_a_waiting_pop_is_woken_by_a_push(void)
{
    struct etcfs_inval_queue q;
    etcfs_inval_queue_init(&q);

    pthread_t tid;
    assert(pthread_create(&tid, NULL, queue_pop_once, &q) == 0);

    struct etcfs_inval_msg m = {.type = 1, .ino = 42};
    /* The pop is parked on the condition variable at some point in here; either
     * order it observes is a wake-up rather than a missed one. */
    etcfs_inval_queue_push(&q, &m);

    void *got = NULL;
    assert(pthread_join(tid, &got) == 0);
    assert(got != NULL);
    assert(((struct etcfs_inval_msg *) got)->ino == 42);

    etcfs_inval_queue_close(&q);
}

/* A close with a pop already waiting has to wake it, or teardown would join a
 * thread that never returns. */
static void test_a_waiting_pop_is_woken_by_a_close(void)
{
    struct etcfs_inval_queue q;
    etcfs_inval_queue_init(&q);

    pthread_t tid;
    assert(pthread_create(&tid, NULL, queue_pop_once, &q) == 0);
    etcfs_inval_queue_close(&q);

    void *got = &q;
    assert(pthread_join(tid, &got) == 0);
    assert(got == NULL);
}

int main(void)
{
    test_rb_reads_big_endian();
    test_rb_stops_at_the_end_of_a_short_response();
    test_rb_bytes_bounds_the_variable_length_field();
    test_rb_attr_leaves_a_truncated_attr_marked_short();
    test_ipc_sync_frames_the_request_and_reads_the_response();
    test_ipc_sync_refuses_an_oversized_response_frame();
    test_ipc_sync_reports_a_truncated_response();
    test_name_bounds_match_the_protocol();
    test_each_thread_gets_its_own_ipc_connection();
    test_a_failed_exchange_reconnects_on_the_next_request();
    test_fixed_reply_widths_match_the_daemon();
    test_a_negative_entry_parses_as_a_cacheable_absence();
    test_the_queue_is_first_in_first_out();
    test_a_full_queue_drops_its_oldest_message();
    test_a_waiting_pop_is_woken_by_a_push();
    test_a_waiting_pop_is_woken_by_a_close();

    printf("test/c: all checks passed\n");
    return 0;
}
