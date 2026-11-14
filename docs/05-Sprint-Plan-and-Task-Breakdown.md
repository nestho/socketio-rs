# `socketio-rs` — Sprint Plan, Task Grouping & Engineering Backlog

This sprint plan breaks down the redesign and port of [`socketio/socket.io`](https://github.com/socketio/socket.io) to Rust into six 2-week sprints, grouped by architectural crate and engineering domain. Every task maps directly to the specifications in [`docs/01`](./01-Deep-Analysis-and-Comparison-Report.md)–[`docs/04`](./04-Developer-Migration-Guide-and-Assistant-Spec.md).

---

## Sprint Overview & Dependency Map

| Sprint | Duration | Primary Focus | Target Crates | Exit Gate |
| :--- | :--- | :--- | :--- | :--- |
| **Sprint 0** | Completed | Architecture docs, workspace init, baseline guards | All 6 crates | `cargo test --workspace` passes (11/11) |
| **Sprint 1** | Weeks 1–2 | Live HTTP long-polling, WebSocket, adaptive `IoDriver` | `engineio-core` | Real `socket.io-client` connects & upgrades |
| **Sprint 2** | Weeks 3–4 | SIMD JSON, MsgPack, Tower Layer, Extractors, Rooms | `socketio-parser`, `socketio-server` | Echo & chat apps run via Axum/Tower API |
| **Sprint 3** | Weeks 5–6 | Official Socket.IO E2E suite, Durable CSR, Redis/NATS | `socketio-adapters`, `socketio-server` | 100% official v4/v5 E2E compliance |
| **Sprint 4** | Weeks 7–8 | HTTP/3 WebTransport, Polyglot Gateway, `jemalloc` | `engineio-core`, `socketio-gateway` | Node.js app offloads WS to Rust sidecar |
| **Sprint 5** | Weeks 9–10 | `sio-migrate` AST scanner, `cargo-fuzz` hardening | `sio-migrate-cli`, `socketio-parser` | Zero panics under 24h fuzzing |
| **Sprint 6** | Weeks 11–12 | 1M-conn benchmarks, 72h soak, Strangler Fig rollout | All + Infra | Verified <8 KB/conn & <15 ms p99 fan-out |

---

## Sprint 0: Foundation & Workspace Initialization (Completed)

- [x] **S0-T01** Export architecture, HLD, roadmap, and migration docs to `docs/01..04`.
- [x] **S0-T02** Initialize 6-crate Cargo workspace (`Cargo.toml`), `gateway.toml`, `README.md`, and `INSTRUCTIONS.md`.
- [x] **S0-T03** Implement `#![forbid(unsafe_code)]` baseline guards: `validate_sid`, `StreamingPayloadGuard`, `WriteWatermarkController`, `SharedBytes`, `CsrReplayLog`, `ShardedRoomIndex`, and `sio-migrate` prototype.

---

## Sprint 1: `engineio-core` Live Transports & Adaptive `IoDriver` (Weeks 1–2)

### Group 1A: Adaptive `IoDriver` & OS Locality
- [x] **S1-T01** Define `IoDriver` / `TransportBackend` async trait boundary in `engineio-core` so protocol state never couples directly to `tokio` or `io_uring` buffer lifetimes.
- [x] **S1-T02** Implement startup `io_uring_setup` probe with automatic `epoll` (Linux container `seccomp`) and `kqueue` (macOS) fallback.
- [x] **S1-T03** Configure `SO_REUSEPORT` per-core listener sockets with CPU thread pinning (`core_affinity`) and `TCP_NOTSENT_LOWAT` (16 KiB).

### Group 1B: HTTP Long-Polling Transport (`hyper`)
- [x] **S1-T04** Implement `GET /socket.io/?EIO=4&transport=polling` handshake returning `0{"sid":...}` and holding long-running poll response.
- [x] **S1-T05** Implement `POST /socket.io/?EIO=4&transport=polling&sid=...` with streaming `StreamingPayloadGuard` (`maxPayload` checked per `hyper::Body` chunk before heap allocation).
- [x] **S1-T06** Enforce strict per-IP/per-SID HTTP request timeouts and automatic session cleanup on aborted HTTP responses (mitigating CVE-2026-59725).

### Group 1C: WebSocket Transport & Upgrade FSM
- [x] **S1-T07** Integrate `tokio-tungstenite` for RFC 6455 WebSocket upgrade handling on `/socket.io/?EIO=4&transport=websocket&sid=...`.
- [x] **S1-T08** Implement the Engine.IO v4 upgrade state machine (`2probe` -> `3probe` -> `5` upgrade -> close polling cleanly).
- [x] **S1-T09** Implement `pingInterval` (25s) / `pingTimeout` (20s) heartbeat timer wheel and `WriteWatermarkController` (128 KiB high / 16 KiB low) slow-consumer eviction.

---

## Sprint 2: `socketio-parser` High-Speed Framing & `socketio-server` Core (Weeks 3–4)

### Group 2A: High-Speed Framing & Compression
- [x] **S2-T01** Integrate `sonic-rs` for zero-tape SIMD JSON packet payload parsing (with `serde_json::Value` fallback for dynamic payloads).
- [x] **S2-T02** Add `rmp-serde` MsgPack parser feature (`msgpack`) and binary attachment (`5` / `6` packet types) reconstruction.
- [x] **S2-T03** Add `permessage-deflate` support with `server_max_window_bits=10`, 1 KB payload threshold, and `verify_decompression_ratio` (100:1 max).

### Group 2B: Multiplexing, Tower Integration & Ergonomics
- [x] **S2-T04** Implement `SocketIoLayer` (`tower::Layer` / `tower::Service`) for seamless embedding in Axum, Hyper, Warp, and Salvo.
- [x] **S2-T05** Implement static and dynamic/regex Namespaces (`io.ns("/", ...)` and `io.ns("/admin", ...)`).
- [x] **S2-T06** Build ergonomic Axum-style handler extractors (`SocketRef`, `Data<T>`, `TryData<T>`, `AckSender`, `Bin`, `State<S>`) and `prelude::*` exports.

### Group 2C: Sharded Rooms, Acks & CSWSH Security
- [x] **S2-T07** Connect `ShardedRoomIndex` to live socket outbound channels for O(1) `SharedBytes::clone()` broadcast fan-out (`to`, `in`, `except`, `volatile`, `local`).
- [x] **S2-T08** Implement `emitWithAck` and `timeout(ms)` correlation table with bounded pending-ack maps.
- [x] **S2-T09** Wire `validate_origin` CSWSH middleware and handshake JWT/auth hook (`io.use(...)` equivalent) into the connection pipeline.

---

## Sprint 3: Official E2E Compliance & `socketio-adapters` (Weeks 5–6)

### Group 3A: Official Socket.IO Compliance Suite
- [x] **S3-T01** Stand up the official `socketio/socket.io` Node.js E2E test runner in CI targeting the Rust server over `127.0.0.1:3000`.
- [x] **S3-T02** Verify 100% pass rate for Engine.IO v4, Socket.IO v5 (and optional `v4` feature flag), binary attachments, namespaces, rooms, and acks.

### Group 3B: Durable Connection State Recovery (CSR)
- [x] **S3-T03** Implement `pid` (private recovery ID) + monotonic `offset` tracking on outbound room broadcasts.
- [x] **S3-T04** Wire `CsrReplayLog` into handshake recovery (`auth` / `pid` + `offset`), restoring `socket.id`, `socket.rooms`, and `socket.recovered = true`.

### Group 3C: Distributed Scaling Adapters
- [x] **S3-T05** Define async `Adapter` trait (`broadcast`, `add_sockets`, `del_sockets`, `fetch_sockets`, `server_side_emit`).
- [x] **S3-T06** Implement Redis / Valkey Pub/Sub + Redis Streams adapter (supporting CSR across server restarts).
- [x] **S3-T07** Implement NATS JetStream and PostgreSQL (`LISTEN/NOTIFY`) adapter crates.

---

## Sprint 4: HTTP/3 WebTransport, `jemalloc` & Polyglot Gateway (Weeks 7–8)

### Group 4A: HTTP/3 WebTransport (`wtransport` / `quinn`)
- [x] **S4-T01** Integrate `wtransport` over `quinn` (UDP/QUIC) with `rustls` TLS 1.3 and UDP GSO/GRO batching.
- [x] **S4-T02** Implement Engine.IO WebTransport bidirectional stream transport (`upgrades: ["websocket", "webtransport"]`), enforcing single-stream-per-session and `validate_sid`.

### Group 4B: Standalone Polyglot Gateway (`socketio-gateway`)
- [x] **S4-T03** Implement `gateway.toml` configuration loader (server, engine, security, recovery, bridge sections).
- [x] **S4-T04** Build HTTP/gRPC Broadcast API (`127.0.0.1:3001/_broadcast`) + Upstream Event Webhook forwarder so Node.js/Python/Go apps can offload WebSockets with zero rewrite.
- [x] **S4-T05** Implement `SIGTERM` graceful slow-drain (`shutdown_drain_timeout_ms`) and `max_connection_duration_jitter_pct` to prevent reconnect avalanches.

### Group 4C: Allocator & Observability
- [x] **S4-T06** Configure `tikv-jemallocator` (`#[global_allocator]`) on Linux builds with `MALLOC_CONF` background purge tuning.
- [x] **S4-T07** Expose Prometheus `/metrics` (active connections, RSS, p99 broadcast latency, dropped slow consumers) and OpenTelemetry `tracing` spans.

---

## Sprint 5: `sio-migrate-cli` AST Analyzer & Security Fuzzing (Weeks 9–10)

### Group 5A: Automated Migration Analyzer & Assistant (`sio-migrate`)
- [x] **S5-T01** Upgrade `sio-migrate-cli` from regex/substring scanning to full JS/TS AST parsing using `swc_ecma_parser` / `tree-sitter`.
- [x] **S5-T02** Build Dependency & Event Graph extractor (`io.of`, `socket.on`, `socket.to`, `emitWithAck`, `socket.request`, Redis adapter options).
- [x] **S5-T03** Implement Sidecar Config Generator (outputs tailored `gateway.toml`, Envoy/NGINX sticky-session snippet, and `@rust-sio/emitter` drop-in wrapper).
- [x] **S5-T04** Implement Native Rust Handler Scaffolder (generates typed `serde` structs + `socketio-server` handler skeletons + wire-parity contract tests).

### Group 5B: Security Fuzzing & Adversarial Testing
- [x] **S5-T05** Add `cargo-fuzz` (`libFuzzer`) targets for `decode_v5_packet`, Engine.IO polling batch framing, and `validate_sid`.
- [x] **S5-T06** Write automated adversarial test suite covering CVE-2026-59724 (crafted SID), CVE-2026-59725 (binary POST exhaustion), decompression bombs, and CSWSH `Origin` bypasses.

---

## Sprint 6: Benchmarking, Soak Testing & Strangler Fig Cutover (Weeks 11–12)

### Group 6A: Multi-Shard Load & Benchmark Harness
- [x] **S6-T01** Stand up multi-shard benchmark harness (modeled on `anycable/nodejs-websocket-bench`) measuring 1M idle connections, 40-publisher fan-out, and 20K reconnect storms.
- [x] **S6-T02** Run WiFi-jitter test (~2s TCP drops every 15s) verifying 100% delivery via `CsrReplayLog` / Redis Streams.

### Group 6B: Long-Running Soak & Hardware Profiling
- [ ] **S6-T03** Execute 72-hour continuous churn soak test verifying flat RSS memory (`jemalloc` vs `ptmalloc2`) and zero leaked tasks (`deploy/soak_and_profile.sh` ready).
- [ ] **S6-T04** Profile CPU cache-line contention using `perf c2c` on Linux to confirm zero false sharing across worker cores.

### Group 6C: Production Strangler Fig Migration Playbook
- [x] **S6-T05** Validate Envoy/NGINX consistent-hash (`sid`) routing + dual-adapter Node.js/Rust Redis bridge in staging.
- [x] **S6-T06** Execute 5% -> 25% -> 50% -> 100% canary cutover with automated `connect_error` rollback triggers.
