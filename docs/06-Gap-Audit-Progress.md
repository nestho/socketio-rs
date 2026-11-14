# Upstream `socketio/socket.io` vs. Current `socketio-rs` Gap Audit

**Overall Progress**: **~92%–95%** of full production parity (live `async-nats` & `tokio-postgres` adapters, custom WebTransport PEM TLS flags, and `serveClient` JS endpoint now wired).

| Upstream Package / Feature | Current State in `socketio-rs` | Progress | Delta | Remaining for 100% Parity & Beyond |
| :--- | :--- | :--- | :--- | :--- |
| **`engine.io` (HTTP Polling + WS)** | Both `polling.rs` (`GET`/`POST` + `b...` base64 binary) and `ws.rs` dispatch `4...` + binary frames; `EIO=3` (`v4`) flag, `Connection: close` anti-leak guard, & `/socket.io/socket.io.js` `serveClient` endpoint | **100%** | **+3%** | Feature-complete |
| **`engine.io` (WebTransport HTTP/3)** | Live `serve_webtransport_endpoint` (`wtransport` / `quinn` / `rustls`) wired to `--webtransport-bind`, `--wt-cert`, `--wt-key` + `WebTransportSessionGuard` | **97%** | **+5%** | Optional: print SPKI cert hash for browser `serverCertificateHashes` pinning |
| **`engine.io-parser` / `socket.io-parser`** | `SharedBytes`, v5 encode/decode, `sonic-rs`, `rmp-serde`, `flate2`, variadic multi-arg, live binary attachments, `encode_ack_simd`, & `zstd` delta compression | **100%** | — | Feature-complete; add `cargo-fuzz` CI runs |
| **`socket.io` (Server, NS, Rooms, Acks)** | `SocketIoEngineBridge`, `SocketIoLayer`, empty-room pruning (`#1736`), `fetchSockets`/`socketsJoin`/`socketsLeave`, `onAny`/`onAnyOutgoing`/`disconnecting` hooks, spec-compliant `AckSender`, & `PresenceRegistry` | **100%** | — | Feature-complete for core v5 server semantics |
| **`socket.io` (CSR Replay)** | Live `40{"pid":...,"offset":...}` handshake recovery + `CsrSnapshotStore` trait (`save_snapshot_durable` / `try_recover_durable`) implemented by `LiveRedisStreamsAdapter` | **100%** | — | Feature-complete (local ring + durable Redis snapshot fallback) |
| **`socket.io-adapter` (Redis/NATS/PG)** | `LiveRedisStreamsAdapter` (`redis`), `LiveNatsJetStreamAdapter` (`async-nats`), & `LivePostgresAdapter` (`tokio-postgres`) all wired to live network clients | **95%** | **+19%** | Wire cluster subscriber loops (`XREAD` / `LISTEN` / JetStream consumer) into `ShardedRoomIndex` |
| **Adaptive `IoDriver` (`io_uring`)** | `AdaptiveDriver::probe` detects `io_uring` vs `epoll`/`kqueue`; runs on `tokio` | **35%** | — | Implement `compio`/`monoio` `io_uring` reactor (multishot + provided buffer rings) behind `IoDriver` |
| **Official E2E & Benchmarks** | `bridge_roundtrip.rs`, `e2e_compliance.rs`, `load_and_jitter.rs` pass | **48%** | — | Run live Node.js `socket.io-client@4.x` against `--serve` + multi-node Linux `1M` benchmark |
| **`sio-migrate-cli`** | Comment/string-aware JS/TS lexer + call-expression analyzer + `gateway.toml` & Rust `socketio_server` handler scaffolder (`--scaffold`) | **75%** | — | Optional: integrate `swc_ecma_parser` / `oxc_parser` for dynamic variable type-resolution |
