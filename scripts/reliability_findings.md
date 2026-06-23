# AntDrive upload reliability — investigation & fix (2026-06-21)

## Goal
Make AntDrive photo uploads (3–4 MB) 100% reliable: every chunk (data,
intermediate, and the Mantaray manifest root) retrievable from **all corners of
Swarm the moment the upload call returns** — eliminate the "I had to push
again" symptom.

## Test rig
- **Uploader**: an isolated `antd` (own data-dir/port/sock) using the same path
  AntDrive uses — `antctl upload start` → `UploadManager::run_job` → single-file
  Mantaray manifest + pushsync. Funded by a fresh depth-22 batch
  `0x5f5b167c…` (the only pre-existing live batch, `0x69fb…`, is owned by a key
  not present in `.env`, so it could not be stamped against — a new batch was
  bought with the `.env` wallet `0x0afc…`, ~0.5 BZZ).
- **Verifiers**: 4 fresh `antd` nodes in distinct neighbourhoods (overlays
  `0x2a / 0xf1 / 0x83 / 0xa7`) + the public gateway `vibing.at/ant` — the
  "corners".
- Harness: `scripts/reliability_nodes.sh` (rig) + `scripts/reliability_probe.py`
  (upload each size, then probe whole-file `GET /bzz/<root>/` + root-chunk from
  every corner at t = 0,3,8,20,45,90 s; SHA-256 verified).
- bee reference updated to `v2.8.1-rc3` for protocol comparison.

## Measurements
**Baseline (current 0.5.28, well-connected host): 100% available at offset 0**
for every size 1 K → 8 MB, across all 4 verifiers + public gateway
(`scripts/reliability_report_before.md`). The recent deep-push + heal work
already makes uploads reliable on a *good* network.

**But the mechanism that guarantees this is fragile on weak networks:**
- ~5% of chunk pushes get a **shallow receipt** even here (po=1–8 vs storage
  radius 8–9): the chunk is stored, but on a peer shallower than its true
  neighbourhood. bee behaves identically — after `DefaultRetryCount=6` shallow
  retries it *accepts* the shallow receipt and reports `ChunkSynced`; bee has
  **no** post-upload read-back. Until pull-sync propagates a shallow chunk into
  its neighbourhood, a fresh node on the far side can't retrieve it.
- Ant's post-upload **self-heal** (read every chunk back via deep neighbourhood
  probes, re-push the missing/shallow ones) is what closes this gap — it is
  *stronger* than bee. In the evidence run it re-confirmed all chunks
  deep-reachable and set `heal_verified`.

## Root cause of "push again"
1. `run_job` marks the job **`completed`** (and returns the manifest reference)
   **before** the background self-heal runs. Heal only sets `heal_verified`
   minutes later (observed: ~3.5 min under 3 concurrent jobs; ~60 s for one
   4 MB photo).
2. **`heal_verified` was never surfaced to clients** — `UploadJobView` (what
   `antctl upload status` and the AntDrive FFI expose) had no such field. So the
   app's only "done" signal is `completed`, which means *pushed once*, not
   *deeply reachable*.

On a good network pull-sync hides the gap (100% at offset 0). On a constrained
mobile node — few peers, NAT, slower pull-sync, more/deeper shallow placements —
the shallow chunks stay unreachable for a while, the app already said "done",
and the user re-pushes.

## Fix
Surface durability and let the upload optionally report "done" only when the
file is actually retrievable everywhere:

- `UploadJobView` gains `heal_verified` and `heal_finished` (serde-default →
  backward compatible). The AntDrive FFI `upload status` JSON now carries them
  automatically, so the app can show "uploaded" at `completed` and "✓ safe
  everywhere" when `heal_verified` arrives — no further FFI work needed.
- `UploadJobInfo` gains `heal_finished`; `verify_and_heal` sets it on every exit
  path (verified, degraded, or inconclusive) so a waiter never blocks forever.
- `antctl upload start --await-sync` / `follow --await-sync`: block until the
  heal verdict, then report `durable: verified` (or a clear degraded message).
  `antctl upload status` renders a `durable:` line.

Files: `crates/ant-control/src/protocol.rs`, `crates/ant-node/src/uploads/state.rs`,
`crates/ant-node/src/uploads/mod.rs`, `crates/antctl/src/main.rs`.

## Verification
- `cargo test -p ant-control -p ant-node -p antctl` ✅, `cargo clippy` ✅ (clean).
- `antctl upload start --await-sync` on a 4 MB photo: the normal follow returned
  at `completed` with `heal_verified=false`; `--await-sync` then blocked ~64 s
  until `heal_verified=true, durable=true`. `upload status` shows
  `durable: verified (every chunk deep-reachable from its neighbourhood)`.

## Recommendation for AntDrive
- Treat `completed` as "uploaded", **`heal_verified` as "backed up / safe"**.
  Poll `upload status` (or use a `--await-sync`-style wait) and only tell the
  user the photo is safe once `heal_verified` is true. This makes "push again"
  unnecessary by construction.
- The deeper the network constraints, the more heal matters — consider keeping
  the node alive a little longer after `completed` so the background heal (and
  the startup heal on next launch) can finish re-pushing shallow chunks.

## Follow-ups (not done here)
- Bump workspace crate versions (0.5.28 → 0.5.29) as the usual release chore.
- Optional: an FFI convenience that blocks until `heal_verified` (the app can
  already do this by polling the now-exposed field).
- Heal latency: deep-verify of ~1000 chunks takes tens of seconds and is
  serialised behind `verify_gate`; could be parallelised/bounded if it becomes a
  UX issue for the await path.
