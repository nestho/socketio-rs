# `socketio-rs` Documentation Hub

Welcome to the `socketio-rs` documentation directory. Start with the **Developer Guide** if you are deploying or embedding `socketio-rs`, or explore the architecture and comparison reports below.

## Start Here

| Document | Audience | Summary |
| :--- | :--- | :--- |
| [**Developer Guide**](./09-Developer-Guide.md) | App & Rust Developers | `gateway.toml` schema, Control API (`/_broadcast`, `/_rooms/*`), webhooks, Redis/NATS/Postgres clustering, HTTP/3 WebTransport, native Rust embedding, and troubleshooting. |
| [**Motivation & `uWebSockets.js` Comparison**](./08-Motivation-and-uWS-Comparison.md) | Tech Leads & Architects | Detailed engineering rationale and verified upstream issue analysis for `socketio/socket.io` and `uWebSockets.js`. |
| [**Interactive Architecture Demo**](./interactive-comparison.html) | Everyone | Self-contained browser animation comparing Engine.IO v4 handshake upgrades, zero-copy sharded fan-out, and CSWSH/backpressure protection. |

## Architecture, Strategy & Audit Reports

| Document | Summary |
| :--- | :--- |
| [**01 — Deep Analysis & Comparison Report**](./01-Deep-Analysis-and-Comparison-Report.md) | Protocol-level analysis of `socketio/socket.io` vs. Rust design (`pdf` also included). |
| [**02 — High-Level Design (HLD)**](./02-High-Level-Design-HLD.md) | Crate boundaries, memory model, sharding, and transport state machines (`pdf` also included). |
| [**03 — Visionary Development & Transformation Plan**](./03-Visionary-Development-and-Transformation-Plan.md) | Long-term roadmap, polyglot sidecar vision, and `v1.0.0` GA gates (`pdf` also included). |
| [**04 — Developer Migration Guide & Assistant Spec**](./04-Developer-Migration-Guide-and-Assistant-Spec.md) | Design and specification for `sio-migrate` and Node.js-to-Rust migration workflows (`pdf` also included). |
| [**05 — Sprint Plan & Task Breakdown**](./05-Sprint-Plan-and-Task-Breakdown.md) | Sprint-by-sprint implementation checklist and verification status. |
| [**06 — Gap Audit & Progress**](./06-Gap-Audit-Progress.md) | Honest audit of completed vs. remaining external/Linux GA verification items. |
| [**07 — Upstream Weaknesses & Sprint 7**](./07-Upstream-Weaknesses-and-Sprint-7.md) | Security, memory leak, and backpressure mitigations mapped to tests. |
