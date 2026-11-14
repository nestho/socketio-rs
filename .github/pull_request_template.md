## Description

Briefly describe the change, motivations, and related issue (if applicable).

Fixes #(issue)

## Type of Change

- [ ] 🐛 Bug fix (non-breaking change fixing an issue)
- [ ] 🚀 New feature (non-breaking change adding functionality)
- [ ] ⚡ Performance improvement
- [ ] 🔒 Security hardening / CVE mitigation
- [ ] 📚 Documentation update / examples
- [ ] 🧹 Maintenance / CI / refactoring

## Checklist

- [ ] `#![forbid(unsafe_code)]` remains strictly enforced across protocol and parser crates.
- [ ] Zero `.unwrap()` or `.expect()` calls added on untrusted network inputs or packet payloads.
- [ ] `cargo check --workspace --all-targets` passes.
- [ ] `cargo test --workspace` passes with zero regressions.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` reports 0 warnings.
- [ ] `cargo fmt --all -- --check` reports no diffs.
- [ ] Documentation updated where relevant.
