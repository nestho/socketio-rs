![socketio-rs](./assets/logo.svg)

**Wire-compatible [Socket.IO v4/v5](https://socket.io/docs/v4/how-it-works/) & [Engine.IO v4](https://socket.io/docs/v4/engine-io-protocol/) engine in pure safe Rust (`#![forbid(unsafe_code)]`)** — built for predictable tail latency, native HTTP/3 WebTransport, durable Connection State Recovery (CSR), and zero-rewrite adoption from any backend language.

> **Release Status (`v0.1.0` Preview):** `53/53` workspace tests and `cargo clippy --workspace --all-targets -- -D warnings` pass with `0` warnings. External Node.js `socket.io-client@4.x` compliance runs and 72-hour Linux soak benchmarks are scheduled before `v1.0.0` GA.

[Why `socketio-rs`?](#why-socketio-rs) • [Comparison](#technical-comparison) • [Architecture](#architecture) • [Quick Start](#quick-start) • [Developer Guide](./docs/09-Developer-Guide.md) • [Benchmarks](#verified-benchmarks--status)

---

## Why `socketio-rs`?

Teams operating high-concurrency chat, notifications, and live telemetry hit structural bottlenecks in both **Node.js `socket.io`** and **C++ `uWebSockets.js` (`uWS`)**:

| Runtime | Production Bottlenecks Solved by `socketio-rs` |
| :--- | :--- |
| **[`socketio/socket.io`](https://github.com/socketio/socket.io)** (Node.js) | Single-threaded V8 event loop (~52 KB idle RAM/conn); silent ~50% event drops across multi-process deployments without an external adapter ([Ably](https://ably.com/topic/socketio)); empty room map memory leaks ([Issue #1736](https://github.com/socketio/socket.io/issues/1736)); `EIO=3/4` reconnect leaks ([Issue #3477](https://github.com/socketio/socket.io/issues/3477)); unbounded `writeBuffer` OOM risk on slow clients ([Issue #3582](https://github.com/socketio/socket.io/issues/3582)); no built-in Prometheus metrics. |
| **[`uWebSockets.js`](https://github.com/uNetworking/uWebSockets.js)** (C++ / Node) | Fast raw I/O, but invokes hard C++ `std::terminate()` crashes if any async handler misses `onAborted()`—bypassing Node `uncaughtException` ([uWS #1296](https://github.com/uNetworking/uWebSockets.js/issues/1296)); recurring Socket.IO `uWS.HttpResponse` lifecycle and room-on-upgrade bugs ([Issue #5066](https://github.com/socketio/socket.io/issues/5066), [Issue #4443](https://github.com/socketio/socket.io/issues/4443)); no built-in rooms, CSR, HTTP/3 WebTransport, or polyglot sidecar mode. |
| **[`Totodore/socketioxide`](https://github.com/Totodore/socketioxide)** (Rust) | Solid native Rust library, but Rust-only (no standalone sidecar gateway or JS/TS migration CLI) and HTTP/3 WebTransport remains an open issue ([Issue #261](https://github.com/Totodore/socketioxide/issues/261)). |

Read the full researched breakdown in [`docs/08-Motivation-and-uWS-Comparison.md`](./docs/08-Motivation-and-uWS-Comparison.md) and explore the interactive flow in [`docs/interactive-comparison.html`](./docs/interactive-comparison.html).

---

## Technical Comparison

| Capability | `socketio/socket.io` (Node.js) | `uWebSockets.js` (C++ / Node) | `Totodore/socketioxide` (Rust) | **`socketio-rs`** |
| :--- | :--- | :--- | :--- | :--- |
| **Memory & Crash Safety** | V8 Garbage Collector | Manual C++ (`std::terminate` risk) | Safe Rust | **Safe Rust (`#![forbid(unsafe_code)]`)** |
| **Threading & Scaling** | Single-threaded event loop | Single-threaded per worker | Multi-threaded Tokio | **Sharded `SO_REUSEPORT` + `#[repr(align(64))]`** |
| **Engine.IO v4 + Upgrade** | Native | Wrapper (`#5066`, `#4443`) | Native | **Native (`2probe`->`3probe`->`5`, `EIO=3/4`, CORS)** |
| **HTTP/3 WebTransport** | Experimental | Not supported | Open issue ([#261](https://github.com/Totodore/socketioxide/issues/261)) | **Built-in (`wtransport`/`quinn` + stream guard)** |
| **Polyglot Sidecar Mode** | No (Node.js only) | No (Node.js only) | No (Rust library only) | **Built-in (`socketio-gateway` + `webhook_url`)** |
| **JSON & Binary Codec** | `JSON.parse` + MsgPack | Raw buffers (DIY) | `serde_json` + `rmp-serde` | **`sonic-rs` SIMD JSON + `rmp-serde` + `SharedBytes`** |
| **Slow-Client Protection** | Unbounded `writeBuffer` ([#3582](https://github.com/socketio/socket.io/issues/3582)) | Manual `getBufferedAmount` | Bounded channels | **`128 KiB` / `16 KiB` watermarks + `TCP_NODELAY`** |
| **Security Mitigations** | Manual config | Manual C++ lifetimes | Rust safety | **CSWSH `403`, `100:1` zip-bomb guard, `#1736` prune** |
| **State Recovery (CSR)** | In-memory / adapter | None (DIY) | In-memory | **Ring buffer + durable Redis `CsrSnapshotStore`** |
| **Distributed Adapters** | Redis / Mongo / Postgres | None (DIY) | Redis / Postgres | **Redis Streams, NATS JetStream, Postgres `LISTEN`** |
| **Presence & Delta** | None | None | None | **Built-in `PresenceRegistry` & `zstd` delta** |
| **Migration Tooling** | N/A | N/A | N/A | **`sio-migrate` (JS/TS lexer & scaffolder)** |

---

## Architecture

Adopt `socketio-rs` without rewriting your existing backend, or embed it directly in Rust:

```text
Clients (Browser / Mobile / IoT)
   │  HTTP Long-Polling • WebSocket • HTTP/3 WebTransport
   ▼
┌────────────────────────────────────────────────────────────────┐
│                  socketio-rs Edge Engine                       │
│  CSWSH Origin Guard • Zip-Bomb 100:1 Cap • Sharded Room Index  │
│  Durable CSR (pid + offset) • Redis / NATS / Postgres Adapters │
└──────────────┬─────────────────────────────────┬───────────────┘
               │                                 │
     Path A: Polyglot Sidecar          Path B: Native Rust
      (Python/Go/Node/PHP/Java)        (`socketio-server` crate)
               │                                 │
   POST webhook_url  ◄─── Events                 ▼
   POST /_broadcast  ───► Rooms        Axum-style Extractors
   POST /_rooms/*                      Data<T>, AckSender, State<S>
```

---

## Quick Start

### 1. Verify Tests & Lints

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 2. Run the Polyglot Sidecar Gateway (`Path A`)

```bash
cargo build --release -p socketio-gateway -p sio-migrate-cli
./target/release/socketio-gateway --help
./target/release/socketio-gateway --serve --bind 0.0.0.0:3000 --webtransport-bind 0.0.0.0:3443
```

### 3. Run Native Rust Examples (`Path B`)

```bash
cargo run -p socketio-server --example echo_server
cargo run -p socketio-server --example chat_rooms
cargo run -p socketio-server --example recovery_and_presence
```

### 4. Analyze & Scaffold an Existing Node.js Repo (`sio-migrate`)

```bash
./target/release/sio-migrate --help
./target/release/sio-migrate --scan /path/to/node-app --scaffold ./migration-out
```

---

## Control API (`127.0.0.1:3001`)

| Endpoint | Method | Payload / Query | Purpose |
| :--- | :--- | :--- | :--- |
| `/_broadcast` | `POST` | `{"namespace":"/","room":"chat:1","event":"msg","data":{...}}` | Zero-copy room broadcast (local + cluster) |
| `/_rooms/join` | `POST` | `{"sid":"<sid>","room":"chat:1"}` | Join a connected socket to a room |
| `/_rooms/leave` | `POST` | `{"sid":"<sid>","room":"chat:1"}` | Remove socket from room (prunes empty rooms) |
| `/_disconnect` | `POST` | `{"sid":"<sid>"}` | Server-initiated disconnect (`41` + cleanup) |
| `/_presence` | `GET` | `?room=chat:1` | List active `PresenceMember` records |
| `/_stats` | `GET` | — | Live session, room, CSR, and broadcast counters |
| `/healthz` | `GET` | — | Readiness probe (`{"status":"ok","shards":N}`) |
| `/metrics` | `GET` | — | Prometheus text exposition (`0.0.4`) |

---

## Verified Benchmarks & Status

| Check | Result | Environment / Scope |
| :--- | :--- | :--- |
| **Workspace Test Suite** | **`53/53` passing** (`0` failed) | `cargo test --workspace` (unit, adversarial, E2E `hyper` + `tungstenite`) |
| **Static Safety & Lints** | **`0` warnings**, `#![forbid(unsafe_code)]` | `cargo clippy --workspace --all-targets -- -D warnings` across all 6 crates |
| **10K Zero-Copy Fan-Out** | **`4.38 ms`** (`--release`) | Apple Silicon macOS, 8 shards, `sonic-rs` + `SharedBytes` (`load_and_jitter`) |
| **CSR WiFi-Jitter Replay** | **`100%` missed-frame delivery** | Simulated 2-second disconnect via `RecoveryManager` (`pid` + `offset`) |
| **Pre-`v1.0.0` GA Gates** | **Scheduled** | External Node.js `socket.io-client@4.x` compliance run, 72h Linux soak, `io_uring` loop |

---

## Workspace Crates & Documentation

| Crate | Path | Responsibility |
| :--- | :--- | :--- |
| `engineio-core` | [`crates/engineio-core`](./crates/engineio-core) | `IoDriver`, Engine.IO v4 polling/WS/WebTransport, streaming `maxPayload`, watermarks |
| `socketio-parser` | [`crates/socketio-parser`](./crates/socketio-parser) | `SharedBytes` v5 codec, `sonic-rs` SIMD JSON, `rmp-serde`, `flate2`, `zstd` delta |
| `socketio-server` | [`crates/socketio-server`](./crates/socketio-server) | Namespaces, `io.use_ns(...)`, `ShardedRoomIndex`, `AckTable`, `PresenceRegistry`, CSWSH |
| `socketio-adapters` | [`crates/socketio-adapters`](./crates/socketio-adapters) | `RecoveryManager`, `CsrSnapshotStore`, Redis Streams, NATS JetStream, Postgres `LISTEN` |
| `socketio-gateway` | [`crates/socketio-gateway`](./crates/socketio-gateway) | Standalone polyglot edge binary (`tikv-jemallocator` on Linux), Control API, webhooks |
| `sio-migrate-cli` | [`crates/sio-migrate-cli`](./crates/sio-migrate-cli) | Comment/string-aware JS/TS scanner and `gateway.toml` / Rust handler scaffolder |

### Documentation Index

- **[Developer Guide](./docs/09-Developer-Guide.md)** — `gateway.toml` schema, Control API, webhooks, clustering, and troubleshooting
- **[Motivation & uWS Comparison](./docs/08-Motivation-and-uWS-Comparison.md)** — Detailed engineering rationale and upstream issue analysis
- **[Interactive Comparison](./docs/interactive-comparison.html)** — Visual handshake, fan-out, and backpressure animation
- **[Architecture & Plans](./docs/)** — [Deep Analysis](./docs/01-Deep-Analysis-and-Comparison-Report.md) • [HLD](./docs/02-High-Level-Design-HLD.md) • [Visionary Plan](./docs/03-Visionary-Development-and-Transformation-Plan.md) • [Migration Spec](./docs/04-Developer-Migration-Guide-and-Assistant-Spec.md) • [Sprint Backlog](./docs/05-Sprint-Plan-and-Task-Breakdown.md)
- **Governance** — [SECURITY.md](./SECURITY.md) • [CONTRIBUTING.md](./CONTRIBUTING.md) • [CHANGELOG.md](./CHANGELOG.md) • [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)

## License

Dual-licensed under **MIT** or **Apache-2.0**.
