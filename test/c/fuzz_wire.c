/*
 * libFuzzer target for the C daemon's response reader.
 *
 * ops.c is included rather than linked, for the same reason test_ops.c includes
 * it: the readers are static, and they are the code a malformed or truncated
 * frame reaches first. Every FUSE operation in this daemon is a write of a
 * request followed by a fixed sequence of these reads over whatever the backend
 * sent back, so a reader that walks off the end of a short response is a
 * memory-safety bug on the path every operation takes.
 *
 * The fuzzer drives the reads rather than the bytes alone: the first part of
 * the input is a script naming which reader to call next, and the rest is the
 * buffer they read from. That is what makes it cover desynchronisation — the
 * failure the readdirplus bug actually was — instead of only covering a single
 * hard-coded parse of arbitrary bytes. A reader that returns without consuming
 * its bytes leaves the next one reading from the middle of a field, and this
 * explores those sequences directly.
 *
 * Correctness is asserted by the sanitizers rather than by this file: any read
 * past the end of the buffer is an ASan report, and rb_take's own bookkeeping
 * is checked below. Build and run it with `make fuzz-c`.
 */

#include "../../pkg/fuse/ops.c"

#include <assert.h>
#include <stdint.h>
#include <stddef.h>

/* How much of the input steers the reads. The rest is what they read. */
#define SCRIPT_BYTES 16

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < SCRIPT_BYTES + 1)
        return 0;

    const uint8_t *script = data;
    const uint8_t *buf = data + SCRIPT_BYTES;
    uint32_t buflen = (uint32_t) (size - SCRIPT_BYTES);

    struct rbuf r = rb_new(buf, buflen);

    for (size_t i = 0; i < SCRIPT_BYTES; i++) {
        /* Once a read has run off the end the reader latches ok to 0 and every
         * later read must be a no-op; carrying on past that point is what this
         * checks, since a caller does not test ok after every field. */
        int was_ok = r.ok;
        uint32_t before = r.off;

        switch (script[i] % 6) {
        case 0:
            (void) rb_u32(&r);
            break;
        case 1:
            (void) rb_u64(&r);
            break;
        case 2:
            (void) rb_i32(&r);
            break;
        case 3: {
            /* A length taken from the buffer itself, which is how a name or a
             * blob is actually read: the length is data the backend sent, so a
             * hostile one has to be survivable. */
            uint32_t n = rb_u32(&r);
            const uint8_t *p = rb_bytes(&r, n);
            /* A zero-length read is legal and hands back a pointer to the
             * cursor, which there is nothing behind — so only a non-empty
             * range is touched. */
            if (p && n > 0) {
                /* Touch both ends, so ASan sees the range the caller was
                 * handed rather than only the pointer it was given. */
                volatile uint8_t sink = p[0];
                sink ^= p[n - 1];
                (void) sink;
            }
            break;
        }
        case 4: {
            struct etcfs_attr a;
            rb_attr(&r, &a);
            break;
        }
        default:
            (void) rb_take(&r, script[i]);
            break;
        }

        assert(r.off <= r.len);
        if (!was_ok)
            assert(r.off == before && !r.ok);
    }

    return 0;
}
