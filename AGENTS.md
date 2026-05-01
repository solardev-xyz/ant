# Agent Notes

- The implementation plan lives in `PLAN.md` at the repo root.
- Bump the patch version (`x.y.Z` → `x.y.Z+1`) of every workspace
  crate whose Cargo.toml declares one — currently `antd`, `antctl`,
  `ant-control`, `ant-crypto`, `ant-gateway`, `ant-node`, `ant-p2p`,
  `ant-retrieval` — on **each** deployment to `vibing.at/ant` (or any
  other production host). Do this *before* building the release
  binary so `antd --version` reflects what's actually deployed and
  the systemd journal lets us correlate behaviour to a specific
  build. Use the same patch number across all crates; we ship them
  as one unit. Bump minor or major only when the user asks for it
  explicitly.
