# `socketio-rs` — Next-Generation Rust Real-Time Engine & Polyglot Gateway

Wire-compatible with [Socket.IO v4/v5](https://socket.io/docs/v4/how-it-works/) and [Engine.IO v4](https://socket.io/docs/v4/engine-io-protocol/), engineered in memory-safe Rust (`#![forbid(unsafe_code)]` in protocol crates) for ultra-low tail latency, high connection density (~2–6 KB/conn), native HTTP/3 WebTransport readiness, durable Connection State Recovery (CSR), and zero-friction migration from Node.js [`socketio/socket.io`](https://github.com/socketio/socket.io).

## Architecture & Engineering Documentation Suite

All foundational research, high-level design, roadmap, and developer migration specifications are located in [`./docs/`](./docs/):

1. **Deep Analysis & Standard Comparison Report**:
   - [Markdown (`docs/01-Deep-Analysis-and-Comparison-Report.md`)](./docs/01-Deep-Analysis-and-Comparison-Report.md) | [PDF (`docs/01-Deep-Analysis-and-Comparison-Report.pdf`)](./docs/01-Deep-Analysis-and-Comparison-Report.pdf)
2. **High-Level Design (HLD) Specification**:
   - [Markdown (`docs/02-High-Level-Design-HLD.md`)](./docs/02-High-Level-Design-HLD.md) | [PDF (`docs/02-High-Level-Design-HLD.pdf`)](./docs/02-High-Level-Design-HLD.pdf)
3. **Visionary, Development & Transformation Plan**:
   - [Markdown (`docs/03-Visionary-Development-and-Transformation-Plan.md`)](./docs/03-Visionary-Development-and-Transformation-Plan.md) | [PDF (`docs/03-Visionary-Development-and-Transformation-Plan.pdf`)](./docs/03-Visionary-Development-and-Transformation-Plan.pdf)
4. **Developer Migration Guide & Automated Migration Assistant Spec**:
   - [Markdown (`docs/04-Developer-Migration-Guide-and-Assistant-Spec.md`)](./docs/04-Developer-Migration-Guide-and-Assistant-Spec.md) | [PDF (`docs/04-Developer-Migration-Guide-and-Assistant-Spec.pdf`)](./docs/04-Developer-Migration-Guide-and-Assistant-Spec.pdf)

For developer setup, build commands, kernel tuning, and phase-by-phase workflow instructions, see [`INSTRUCTIONS.md`](./INSTRUCTIONS.md).

## Workspace Crates

| Crate | Path | Description |
| :--- | :--- | :--- |
| `engineio-core` | [`crates/engineio-core`](./crates/engineio-core) | Adaptive `IoDriver` (`io_uring` bare-metal probe + `epoll` container `seccomp` fallback), Engine.IO v4 session FSM, heartbeat timers, streaming `maxPayload` guard, and write watermarks (128 KiB / 16 KiB). |
| `socketio-parser` | [`crates/socketio-parser`](./crates/socketio-parser) | Zero-copy `bytes::Bytes` framing for Engine.IO v4 and Socket.IO v5 packets (JSON + MsgPack + binary attachments) with decompression-bomb ratio enforcement. |
| `socketio-server` | [`crates/socketio-server`](./crates/socketio-server) | Multiplexed namespaces, Discord-style sharded lock-free room router, `#[repr(align(64))]` cache-line padded counters, CSWSH `Origin` guard, and ergonomic handler API. |
| `socketio-adapters` | [`crates/socketio-adapters`](./crates/socketio-adapters) | Pluggable horizontal scaling & durable CSR (`pid` + monotonic offset replay) trait with In-Memory ring-buffer and distributed adapter stubs (Redis/Valkey Streams, NATS JetStream, PostgreSQL). |
| `socketio-gateway` | [`crates/socketio-gateway`](./crates/socketio-gateway) | Standalone polyglot edge gateway binary (`tikv-jemallocator` ready) allowing Node.js/Python/Go backends to offload 1M+ connections without rewriting business logic. |
| `sio-migrate-cli` | [`crates/sio-migrate-cli`](./crates/sio-migrate-cli) | Automated Node.js/TypeScript Socket.IO repository scanner and migration assistant CLI (`sio-migrate`) for readiness scoring, ROI estimation, and sidecar/Rust handler scaffolding. |

## Quick Start

```bash
# 1. Verify workspace compilation and run all unit tests
cargo test --workspace

# 2. Run the standalone polyglot gateway binary
cargo run -p socketio-gateway -- --config gateway.toml

# 3. Run the automated migration analyzer against a project directory
cargo run -p sio-migrate-cli -- scan --path .
```

## License

Dual-licensed under MIT or Apache-2.0.
