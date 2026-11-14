# Upstream `socketio/socket.io` vs. Current `socketio-rs` Gap Audit

**Overall Progress**: **~60%–64%** of full production parity (plus **Issue #1736** empty-room leak fixed and **Sprint 7** beyond-upstream roadmap added).
**Test Suite**: **36 / 36 tests passing** across all 6 crates.

| Upstream Package / Feature | Current State in `socketio-rs` | Progress | Delta | Remaining for 100% Parity & Beyond |
| :--- | :--- | :--- | :--- | :--- |
| **`engine.io` (HTTP Polling + WS)** | Both `polling.rs` (`GET`/`POST`) and `ws.rs` dispatch `4...` -> `EngineHandler` + `on_close` | **90%** | — | Add base64 (`b...`) legacy binary polling, `EIO=3` (`v4`) flag, & `serveClient` JS bundle |
| **`engine.io` (WebTransport HTTP/3)** | `webtransport.rs` has `WebTransportConfig` & single-stream `WebTransportSessionGuard` | **20%** | — | Integrate live `wtransport` / `quinn` UDP/QUIC endpoint + `rustls` TLS 1.3 listener |
| **`engine.io-parser` / `socket.io-parser`** | `SharedBytes`, v5 encode/decode, `sonic-rs`, `rmp-serde`, `flate2` | **85%** | — | Wire `BinaryAttachmentReconstructor` into live WS binary frames; add variadic multi-arg & `zstd` delta compression |
| **`socket.io` (Server, NS, Rooms, Acks)** | `SocketIoEngineBridge` & `SocketIoLayer::with_handler` dispatch `40`, `42`, `43`, `41`; empty rooms pruned (`#1736` fixed) | **83%** | **+3%** | Add `onAny`, `disconnecting`, `fetchSockets`, `socketsJoin`/`Leave`, and native Presence |
| **`socket.io` (CSR Replay)** | Live `40{"pid":...,"offset":...}` handshake recovery restores rooms & replays missed frames | **85%** | — | Persist `RecoveryManager` snapshots into Redis Streams / NATS JetStream |
| **`socket.io-adapter` (Redis/NATS/PG)** | Async `Adapter` trait + `RedisStreams`, `NatsJetStream`, `Postgres` over in-memory bus | **30%** | — | Plug in real `redis`/`fred`, `async-nats`, and `sqlx`/`tokio-postgres` clients + binary `serverSideEmit` |
| **Adaptive `IoDriver` (`io_uring`)** | `AdaptiveDriver::probe` detects `io_uring` vs `epoll`/`kqueue`; runs on `tokio` | **35%** | — | Implement `compio`/`monoio` `io_uring` reactor (multishot + provided buffer rings) behind `IoDriver` |
| **Official E2E & Benchmarks** | `bridge_roundtrip.rs`, `e2e_compliance.rs`, `load_and_jitter.rs` pass (36/36) | **45%** | — | Run live Node.js `socket.io-client@4.x` against `--serve` + multi-node Linux `1M` benchmark |
| **`sio-migrate-cli`** | Substring scanner + `scaffold_rust_namespace` | **25%** | — | Integrate `swc_ecma_parser` / `tree-sitter` for true JS/TS AST parsing and `gateway.toml` / handler generation |
