# Upstream `socketio/socket.io` vs. Current `socketio-rs` Gap Audit

**Overall Progress**: **~70%–75%** of full production parity (Sprint 7 `S7-T01`–`S7-T04` complete; `zstd` delta compression, native Presence, `fetchSockets`/`socketsJoin`/`socketsLeave` implemented).

| Upstream Package / Feature | Current State in `socketio-rs` | Progress | Delta | Remaining for 100% Parity & Beyond |
| :--- | :--- | :--- | :--- | :--- |
| **`engine.io` (HTTP Polling + WS)** | Both `polling.rs` (`GET`/`POST`) and `ws.rs` dispatch `4...` + binary frames; `EIO=3` (`v4`) flag & `Connection: close` anti-leak guard added | **94%** | — | Add base64 (`b...`) legacy binary polling & `serveClient` JS bundle |
| **`engine.io` (WebTransport HTTP/3)** | `webtransport.rs` has `WebTransportConfig` & single-stream `WebTransportSessionGuard` | **20%** | — | Integrate live `wtransport` / `quinn` UDP/QUIC endpoint + `rustls` TLS 1.3 listener |
| **`engine.io-parser` / `socket.io-parser`** | `SharedBytes`, v5 encode/decode, `sonic-rs`, `rmp-serde`, `flate2`, variadic multi-arg parser, live binary attachments, & `zstd` dictionary delta compression | **100%** | **+5%** | Feature-complete; add `cargo-fuzz` CI runs |
| **`socket.io` (Server, NS, Rooms, Acks)** | `SocketIoEngineBridge`, `SocketIoLayer`, empty-room pruning (`#1736`), `fetchSockets`/`socketsJoin`/`socketsLeave`, & native `PresenceRegistry` + `PushFallbackHook` | **92%** | **+7%** | Add `onAny` / `disconnecting` hooks on `SocketRef` |
| **`socket.io` (CSR Replay)** | Live `40{"pid":...,"offset":...}` handshake recovery restores rooms & replays missed frames | **85%** | — | Persist `RecoveryManager` snapshots into Redis Streams / NATS JetStream |
| **`socket.io-adapter` (Redis/NATS/PG)** | Async `Adapter` trait with binary `ClusterBroadcastPacket` support over in-memory bus | **35%** | — | Plug in real `redis`/`fred`, `async-nats`, and `sqlx`/`tokio-postgres` clients |
| **Adaptive `IoDriver` (`io_uring`)** | `AdaptiveDriver::probe` detects `io_uring` vs `epoll`/`kqueue`; runs on `tokio` | **35%** | — | Implement `compio`/`monoio` `io_uring` reactor (multishot + provided buffer rings) behind `IoDriver` |
| **Official E2E & Benchmarks** | `bridge_roundtrip.rs`, `e2e_compliance.rs`, `load_and_jitter.rs` pass | **48%** | — | Run live Node.js `socket.io-client@4.x` against `--serve` + multi-node Linux `1M` benchmark |
| **`sio-migrate-cli`** | Substring scanner + `scaffold_rust_namespace` | **25%** | — | Integrate `swc_ecma_parser` / `tree-sitter` for true JS/TS AST parsing and `gateway.toml` / handler generation |
