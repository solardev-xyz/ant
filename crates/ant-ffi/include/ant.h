/*
 * ant.h — hand-written C interface for the iOS download smoke test.
 *
 * Companion to `crates/ant-ffi/src/lib.rs`. Kept hand-written (not
 * cbindgen-generated) for the PLAN.md § 9 smoke test so that the
 * surface area is obvious from reading the header. The proper mobile
 * artefact replaces this with a UniFFI-generated `.udl` (§ 7).
 *
 * Memory model
 * ------------
 *   * `AntHandle*`          owned by ant, freed by `ant_shutdown`.
 *   * `unsigned char*` body owned by ant, freed by `ant_free_buffer`.
 *   * `char*`          msg  owned by ant, freed by `ant_free_string`.
 *
 * None of the pointers may be passed to `free(3)` directly.
 */

#ifndef ANT_FFI_H
#define ANT_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct AntHandle AntHandle;

/*
 * POD mirror of the Rust `AntProgress` struct. Field order must match
 * `crates/ant-ffi/src/lib.rs`; Swift treats this as a fixed-layout C
 * struct.
 *
 *   in_progress     1 while ant_download is active; 0 otherwise.
 *   bytes_done      bytes joined so far (chunk wire bytes).
 *   total_bytes     expected file body size, 0 until the data root
 *                   chunk has been fetched.
 *   chunks_done     chunks delivered to the joiner (cache + network).
 *   total_chunks    estimated chunk count, 0 until the root span is
 *                   known.
 *   elapsed_ms      ms since the node accepted the request.
 *   peers_used      distinct peers that served a chunk so far.
 *   in_flight       chunks currently dispatched (proxy for load).
 *   cache_hits      chunk deliveries served from the local cache.
 */
typedef struct AntProgress {
  uint8_t  in_progress;
  uint64_t bytes_done;
  uint64_t total_bytes;
  uint64_t chunks_done;
  uint64_t total_chunks;
  uint64_t elapsed_ms;
  uint32_t peers_used;
  uint32_t in_flight;
  uint64_t cache_hits;
} AntProgress;

/*
 * Spin up an embedded light-node against Swarm mainnet. `data_dir`
 * points at a writable directory (the iOS Application Support sandbox
 * works). Identity + peer snapshot + in-memory chunk cache live there.
 *
 * On success returns a non-NULL handle. On failure returns NULL and
 * writes an allocated error string to *out_err (caller frees with
 * ant_free_string). Pass NULL for out_err to opt out of error
 * reporting.
 */
AntHandle *ant_init(const char *data_dir, char **out_err);

/*
 * Download a Swarm reference. Accepted forms:
 *
 *   64-hex          single-chunk or multi-chunk /bytes tree
 *   bytes://<hex>   explicit bytes-tree form
 *   bzz://<hex>     mantaray manifest, resolves `website-index-document`
 *   bzz://<hex>/p   mantaray manifest with explicit path
 *
 * On success returns a heap-allocated body buffer and writes its
 * length into *out_len; free the buffer with ant_free_buffer. On
 * failure returns NULL, writes an allocated NUL-terminated error
 * string into *out_err, and sets *out_len to 0 if out_len is non-NULL.
 * Thread-safe: multiple threads may call ant_download concurrently
 * against the same handle; the embedded node serialises dispatch
 * through its control-command channel.
 */
unsigned char *ant_download(AntHandle *handle,
                            const char *reference,
                            size_t *out_len,
                            char **out_err);

/*
 * Number of BZZ peers currently connected (post-handshake). Cheap —
 * reads the last-published status snapshot without blocking. Returns
 * -1 if `handle` is NULL.
 */
int ant_peer_count(const AntHandle *handle);

/*
 * Sample the current download progress. Fills *out and returns 0 on
 * success; returns -1 if `handle` or `out` is NULL. Safe to call at
 * any cadence (the implementation only takes a short mutex).
 *
 * When no download has started yet, `out->in_progress == 0` and every
 * other field is 0. The `in_progress` flag drops back to 0 once
 * ant_download returns (success or failure).
 */
int ant_download_progress(const AntHandle *handle, AntProgress *out);

/*
 * Request cancellation of the in-flight ant_download on `handle`.
 * Checked cooperatively on the next ack loop iteration (typically
 * within ~150 ms). The racing ant_download call returns NULL with
 * the error string "download canceled". No-op when no download is
 * running. Returns 0 on success, -1 if `handle` is NULL.
 */
int ant_cancel_download(const AntHandle *handle);

/*
 * Free a buffer returned by ant_download. `len` must be the exact
 * length the call wrote into *out_len. Null pointer / zero length is
 * a no-op.
 */
void ant_free_buffer(unsigned char *ptr, size_t len);

/*
 * Free a NUL-terminated string returned via an `out_err` slot. Null is
 * a no-op.
 */
void ant_free_string(char *ptr);

/*
 * Shut the embedded node down and free the handle. After this
 * returns, `handle` must not be used again.
 */
void ant_shutdown(AntHandle *handle);

#ifdef __cplusplus
}
#endif

#endif /* ANT_FFI_H */
