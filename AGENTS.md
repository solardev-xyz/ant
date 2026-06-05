# Agent Notes

- The implementation plan lives in `PLAN.md` at the repo root.
- **Keep CI green — it is a hard requirement, not a nicety.** Before you
 push or merge anything, the full CI gate must pass locally. CI
 (`.github/workflows/ci.yml`) runs these steps in order and stops at the
 first failure, so run all of them yourself first:
   1. `cargo fmt --all -- --check`
   2. `cargo clippy --workspace --all-targets -- -D warnings`
   3. `cargo clippy -p ant-ffi --features jni --all-targets -- -D warnings`
   4. `cargo test --workspace --lib`
 Never merge to `main` (or push a branch you expect to be merged) with a
 known-red gate. Toolchain upgrades can introduce new `clippy`/`rustfmt`
 findings on untouched code — fix those too (the gate is whole-repo, not
 just your diff) rather than leaving `main` red. If a finding is a genuine
 false positive, prefer a narrowly-scoped `#[allow(...)]` with a comment
 over disabling the lint workspace-wide.
- Bump the patch version (`x.y.Z` → `x.y.Z+1`) of every workspace
 crate whose Cargo.toml declares one — currently `antd`, `antctl`,
 `antop`, `ant-chain`, `ant-control`, `ant-crypto`, `ant-ffi`,
 `ant-gateway`, `ant-node`, `ant-p2p`, `ant-postage`, `ant-retrieval`
 — on **each** deployment to `vibing.at/ant` (or any other production
 host). Do this *before* building the release binary so `antd
 --version` reflects what's actually deployed and the systemd journal
 lets us correlate behaviour to a specific build. Use the same patch
 number across all crates; we ship them as one unit. Bump minor or
 major only when the user asks for it explicitly.
