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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct AntHandle AntHandle;
typedef struct AntStream AntStream;

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
 * Snapshot the running node's `agent` string from the live status
 * channel (currently `ant-ffi/<crate-version>`). Useful for displaying
 * the version of the embedded library actually running, independent
 * of the host bundle's version metadata.
 *
 * Caller takes ownership of the returned UTF-8 C string and must free
 * it with `ant_free_string`. Returns NULL if `handle` is NULL.
 */
char *ant_agent_string(const AntHandle *handle);

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
 * List the entries of a mantaray manifest. `reference` is a
 * `bzz://<64-hex>` (or bare hex) string. Returns a heap-allocated
 * NUL-terminated JSON document of the form
 *   {"entries":[{"path":"...","reference":"...","size":N,
 *               "content_type":"..."},...]}
 * (size and content_type are omitted when not known). Free with
 * ant_free_string. On failure returns NULL with an allocated error in
 * *out_err.
 */
char *ant_list_bzz(AntHandle *handle,
                   const char *reference,
                   char **out_err);

/*
 * Open a streaming session for a single file inside a bzz manifest.
 * Sends a head-only request to learn `total_bytes` + Content-Type
 * (written into *out_total_bytes / *out_content_type — pass NULL to
 * skip). Returns an opaque AntStream* on success; null + *out_err on
 * failure. The caller frees *out_content_type with ant_free_string
 * (when it was non-NULL on entry and non-NULL on return).
 *
 * The stream borrows the AntHandle; closing the handle while a stream
 * is open is undefined behaviour. Always call ant_stream_close before
 * ant_shutdown.
 */
AntStream *ant_stream_open(AntHandle *handle,
                           const char *reference,
                           uint64_t *out_total_bytes,
                           char **out_content_type,
                           char **out_err);

/*
 * Read `len` bytes starting at `offset` into `dst`. Blocks until the
 * range is delivered (or the per-read timeout fires). Returns the
 * number of bytes written on success (<= len; truncated near EOF), or
 * -1 on failure with an allocated error in *out_err.
 *
 * Safe to call from any thread, but callers should serialise reads on
 * the same stream - concurrent reads issue independent range requests
 * to the node and the pattern stops being efficient.
 */
ssize_t ant_stream_read(AntStream *stream,
                        uint64_t offset,
                        size_t len,
                        unsigned char *dst,
                        char **out_err);

/*
 * Pull a contiguous byte range and deliver each joiner-produced chunk
 * to `on_chunk` as soon as it arrives. Unlike ant_stream_read, which
 * blocks until the entire `len` window has buffered, this drives one
 * StreamBzz request through to its terminal StreamDone and invokes
 * the callback for every BytesChunk along the way - matching the
 * shape of AVAssetResourceLoaderDelegate's dataRequest.
 *
 * The callback returns 0 to keep pulling, non-zero to ask the FFI to
 * stop early (e.g. AVPlayer canceled the dataRequest). The FFI then
 * drains the remaining acks for clean shutdown.
 *
 * Returns the total bytes delivered, or -1 on error (with *out_err
 * set if non-null).
 */
typedef int (*AntStreamChunkCb)(void *ctx, const unsigned char *data, size_t len);

int64_t ant_stream_pull(AntStream *stream,
                        uint64_t offset,
                        uint64_t len,
                        AntStreamChunkCb on_chunk,
                        void *ctx,
                        char **out_err);

/*
 * Sample stream-level progress. `bytes_done` is the cumulative bytes
 * AVPlayer has consumed so far for this file; `total_bytes` is what
 * ant_stream_open reported. The chunk / peer / in-flight counters
 * reflect the most recent range request's `Progress` ack. Returns 0 on
 * success, -1 if either pointer is null.
 */
int ant_stream_progress(const AntStream *stream, AntProgress *out);

/*
 * Close a streaming session and free its handle. Null is a no-op.
 */
void ant_stream_close(AntStream *stream);

/* =========================================================================
 * AntDrive: uploads, storage plan, account
 *
 * Each function returns a heap-allocated NUL-terminated string (JSON for
 * the structured calls, a plain id / hex string otherwise), owned by ant
 * and freed with ant_free_string. On failure they return NULL and write
 * an allocated error string into *out_err (also freed with
 * ant_free_string). Pass NULL for out_err to opt out of error reporting.
 * =========================================================================
 */

/*
 * Start an upload job for the file at `path`. `batch_id` selects the
 * storage plan to stamp against (NULL = node default); `name` /
 * `content_type` are optional manifest metadata (NULL to let the daemon
 * infer). Returns the new job id (16 hex chars).
 */
char *ant_upload_start(const AntHandle *handle,
                       const char *path,
                       const char *batch_id,
                       const char *name,
                       const char *content_type,
                       char **out_err);

/*
 * Snapshot every upload job as a JSON document:
 *   {"jobs":[{"job_id","source_path","source_size","name","content_type",
 *             "raw","status","bytes_pushed","chunks_pushed","chunks_total",
 *             "created_at_unix","last_update_unix","last_error",
 *             "reference"},...]}
 * (optional fields are omitted when unknown).
 */
char *ant_upload_list(const AntHandle *handle, char **out_err);

/*
 * Snapshot one upload job by id (or unique 8-hex-char prefix) as a JSON
 * object with the same shape as a `jobs[]` element above.
 */
char *ant_upload_status(const AntHandle *handle,
                        const char *job_id,
                        char **out_err);

/*
 * Pause / resume / cancel an upload job. Each returns the updated job
 * JSON object.
 */
char *ant_upload_pause(const AntHandle *handle, const char *job_id, char **out_err);
char *ant_upload_resume(const AntHandle *handle, const char *job_id, char **out_err);
char *ant_upload_cancel(const AntHandle *handle, const char *job_id, char **out_err);

/*
 * Snapshot the local storage plan (postage issuer) as a JSON object:
 *   {"enabled":bool,"batch_id","batch_depth","bucket_depth","immutable",
 *    "bucket_count","bucket_capacity","total_capacity_chunks",
 *    "issued_chunks","bucket_fill_min","bucket_fill_max",
 *    "remaining_total_chunks","worst_case_remaining_chunks"}
 * `enabled=false` means no plan is connected yet.
 */
char *ant_storage_status(const AntHandle *handle, char **out_err);

/*
 * Outbound-settlement status as a JSON object:
 *   {"enabled":bool,"chequebook":"0x…"|null}
 * `enabled=true` once a chequebook is deployed, which is what lets
 * uploads actually propagate (bee charges the uploader per pushed chunk
 * and freezes out a node that can't pay). Builds without the `chain`
 * feature always report {"enabled":false,"chequebook":null}.
 */
char *ant_storage_settlement_status(const AntHandle *handle, char **out_err);

/*
 * Deep read-back propagation check for an uploaded reference. Resolves
 * the manifest to its data root, enumerates the file's chunk tree
 * (fetching every interior node network-only, which proves the skeleton
 * is retrievable), then checks the real data leaves network-only. All
 * fetches bypass local caches so our own store-then-push copy can't mask
 * a failed push. Returns JSON:
 *   {"reference","retrievable":bool,"total_chunks":int,"leaf_chunks":int,
 *    "intermediate_chunks":int,"checked_chunks":int,
 *    "retrievable_chunks":int,"sampled_leaves":int,"sources":int,
 *    "error"?:string}
 * `retrievable` is true iff the root and every interior node were
 * fetched and every checked leaf came back; `sources` is the minimum
 * distinct-route count across the probed subset (a replication floor).
 * `reference` accepts a bare/0x 64-hex address, a `bytes://` ref, or a
 * `bzz://<ref>/<path>` URL (path ignored). `samples == 0` checks *every*
 * leaf (a full verification, internally bounded for very large files);
 * a non-zero value spot-checks that many evenly-spread leaves. `probes`
 * is clamped to 1..=8; pass 0 to use a default.
 */
char *ant_storage_verify_propagation(const AntHandle *handle,
                                     const char *reference,
                                     uint8_t samples,
                                     uint8_t probes,
                                     char **out_err);

/*
 * Account identity as a JSON object:
 *   {"eth_address","overlay","peer_id","agent"}
 */
char *ant_account_info(const AntHandle *handle, char **out_err);

/*
 * Export the account's raw secp256k1 signing key as 64 hex chars (no
 * 0x). Secret material — handle accordingly.
 */
char *ant_account_export_key(const AntHandle *handle, char **out_err);

/*
 * Connect a storage plan this account already owns on Gnosis, by id.
 * Reads the plan parameters from `gnosis_rpc`, verifies ownership,
 * registers it for stamping, and returns the refreshed
 * ant_storage_status JSON. Only functional when ant-ffi is built with
 * the `chain` cargo feature; otherwise returns NULL + an error.
 */
char *ant_storage_connect_batch(const AntHandle *handle,
                                const char *gnosis_rpc,
                                const char *batch_id,
                                char **out_err);

/*
 * Auto-discover and connect every funded storage plan this account owns
 * on Gnosis (an on-chain log scan — can take a while). Returns
 *   {"registered":[...ids],"status":<ant_storage_status object>}
 * Requires the `chain` cargo feature.
 */
char *ant_storage_discover(const AntHandle *handle,
                           const char *gnosis_rpc,
                           char **out_err);

/*
 * Deploy (or return the already-persisted) node-owned chequebook so the
 * publish-setup checklist's "chequebook deployed" step can complete.
 * Idempotent: if this device already deployed a chequebook it's returned
 * as-is (no redeploy); otherwise this signs an on-chain
 * factory.deploySimpleSwap (issuer = node EOA), persists the association,
 * and returns the new address. SUBMITS A REAL ON-CHAIN TRANSACTION:
 * spends gas (xDAI) plus the default xBZZ deposit, and BLOCKS until the
 * tx confirms. Light-mode only (requires the `chain` cargo feature;
 * otherwise returns NULL + an error). Returns
 *   {"chequebookAddress":"0x<40hex>"}
 * (free with ant_free_string), or NULL with an error in *out_err. The
 * caller should restart the gateway (ant_stop_gateway + ant_start_gateway)
 * afterwards so /chequebook/address reflects the deployed chequebook.
 */
char *ant_deploy_chequebook(const AntHandle *handle,
                            const char *gnosis_rpc,
                            char **out_err);

/*
 * Price a storage plan (no transaction). `depth` sets capacity
 * (2^depth chunks × 4 KiB); `days` sets how long it should last.
 * Returns a JSON object:
 *   {"depth","days","amount_per_chunk","total_cost_plur",
 *    "total_cost_bzz","capacity_bytes","account_bzz",
 *    "account_bzz_display","account_xdai","account_xdai_display",
 *    "needed_bzz","needed_bzz_display","xdai_required",
 *    "xdai_required_display","xdai_to_send","xdai_to_send_display",
 *    "sufficient_funds"}
 * For the xDAI-only flow, `xdai_to_send_display` is exactly how much more
 * xDAI to send; the node swaps it to xBZZ and buys the plan itself.
 * Requires the `chain` cargo feature.
 */
char *ant_storage_quote(const AntHandle *handle,
                        const char *gnosis_rpc,
                        uint8_t depth,
                        uint64_t days,
                        char **out_err);

/*
 * Buy and activate a storage plan on Gnosis (approve + createBatch, then
 * register it for stamping). `amount_per_chunk` is the value from
 * ant_storage_quote so the charge matches the approved quote; `immutable`
 * is 0 or 1. Returns the refreshed ant_storage_status JSON. SUBMITS REAL
 * TRANSACTIONS AND SPENDS REAL FUNDS. Requires the `chain` cargo feature.
 */
char *ant_storage_buy(const AntHandle *handle,
                      const char *gnosis_rpc,
                      uint8_t depth,
                      const char *amount_per_chunk,
                      int immutable,
                      char **out_err);

/*
 * Buy and activate a storage plan funding ONLY with xDAI: the node swaps
 * the xBZZ shortfall on-chain (deploying a tiny stateless swap helper the
 * first time) before approve + createBatch. `amount_per_chunk` is the
 * value from ant_storage_quote; `immutable` is 0 or 1. Returns the
 * refreshed ant_storage_status JSON. SUBMITS REAL TRANSACTIONS AND SPENDS
 * REAL FUNDS. Requires the `chain` cargo feature.
 */
char *ant_storage_buy_xdai(const AntHandle *handle,
                           const char *gnosis_rpc,
                           uint8_t depth,
                           const char *amount_per_chunk,
                           int immutable,
                           char **out_err);

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
 * Start the in-process bee-shaped HTTP gateway on `api_addr` (pass NULL
 * or "" for the default 127.0.0.1:1633), serving the node this handle
 * owns. Lets an iOS app expose the same HTTP API `antd` serves
 * (/bzz, /bytes, /chunks, /feeds, /soc, /tags, status, plus bee-stubbed
 * /wallet, /stamps, /chequebook) without spawning a separate daemon
 * process.
 *
 * `light_mode` drives GET /node.beeMode: nonzero -> "light" (publish /
 * feed / SOC writes allowed), zero -> "ultra-light" (read-only).
 *
 * `gnosis_rpc` is the Gnosis JSON-RPC endpoint backing the on-chain
 * /wallet, /stamps, and /chequebook surfaces. Pass NULL or "" to disable
 * on-chain reads (those endpoints fall back to the bee zero-stub / 501).
 * When set together with light_mode, it enables real /wallet balances
 * and /stamps postage state (desktop `antd` parity). Honoured only when
 * the library is built with the `chain` feature; ignored otherwise.
 *
 * Returns true on success (or if a gateway is already running on this
 * handle). On failure returns false and writes an allocated message to
 * *out_err (free with ant_free_string). Idempotent: a second call while
 * one is live is a no-op success. Run off the main thread.
 */
bool ant_start_gateway(const AntHandle *handle,
                       const char *api_addr,
                       bool light_mode,
                       const char *gnosis_rpc,
                       char **out_err);

/*
 * Stop the in-process HTTP gateway started by ant_start_gateway.
 * Returns true if a gateway was running and was stopped, false if none
 * was running (or `handle` is NULL). Safe to call repeatedly.
 */
bool ant_stop_gateway(const AntHandle *handle);

/*
 * Shut the embedded node down and free the handle. After this
 * returns, `handle` must not be used again.
 */
void ant_shutdown(AntHandle *handle);

#ifdef __cplusplus
}
#endif

#endif /* ANT_FFI_H */
