# Contributing to `socketio-rs`

Thank you for contributing to `socketio-rs`! We maintain strict engineering, honesty, and safety standards across all crates and docs.

## Branching & Release Workflow (GitFlow)

| Branch | Purpose |
| :--- | :--- |
| `main` | Production-ready releases (`v0.1.0`, `v1.0.0`, ...). Protected; fast-forward or squash-merged from `develop` / `hotfix/*`. |
| `develop` | Integration branch for verified features and fixes. |
| `feat/<scope>` | New features (branched from `develop`). |
| `fix/<scope>` | Bug fixes (branched from `develop`). |
| `hotfix/<scope>` | Urgent security or production fixes (branched from `main`, merged back to both `main` and `develop`). |

## Pull Request Checklist

Before opening a PR against `develop`:
1. Keep `#![forbid(unsafe_code)]` intact in all protocol and server crates.
2. Run Clippy with warnings denied:
   ```bash
   cargo clippy --workspace --all-targets -- -D warnings
   ```
3. Run the full workspace test suite:
   ```bash
   cargo test --workspace
   ```
4. Update [`docs/06-Gap-Audit-Progress.md`](./docs/06-Gap-Audit-Progress.md) if your PR changes protocol parity or adapter coverage.
5. Never claim unmeasured benchmarks or unexecuted external tests—always report exact hardware, OS, and test counts.
