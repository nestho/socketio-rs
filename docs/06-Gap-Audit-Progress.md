# Upstream `socketio/socket.io` vs. Current `socketio-rs` Gap Audit

**Overall Progress**: **~83%–87%** of full production parity (`--webtransport-bind` CLI + Redis CSR snapshot persistence + `onAny`/`disconnecting` hooks now wired).

| Upstream Package / Feature | Current State in `socketio-rs` | Progress | Delta | Remaining for 100% Parity & Beyond |
| :--- | :--- | :--- | :--- | :--- |
| **`engine.io` (HTTP Polling + WS)** | Both `polling.rs` (`GET`/`POST`) and `ws.rs` dispatch `4...` + binary frames; `EIO=3` (`v4`) flag & `Connection: close` anti-leak guard added | **94%** | — | Add base64 (`b...`) legacy binary polling & `serveClient` JS bundle |
| **`engine.io` (WebTransport HTTP/3)** | Live `serve_webtransport_endpoint` (`wtransport` / `quinn` / `rustls`) wired to `--webtransport-bind` in `socketio-gateway` + `WebTransportSessionGuard` | **92%** | **+7%** | Add custom TLS cert/key path options & browser cert pinning |
| **`engine.io-parser` / `socket.io-parser`** | `SharedBytes`, v5 encode/decode, `sonic-rs`, `rmp-serde`, `flate2`, variadic multi-arg, live binary attachments, `encode_ack_simd`, & `zstd` delta compression | **100%** | — | Feature-complete; add `cargo-fuzz` CI runs |
| **`socket.io` (Server, NS, Rooms, Acks)** | `SocketIoEngineBridge`, `SocketIoLayer`, empty-room pruning (`#1736`), `fetchSockets`/`socketsJoin`/`socketsLeave`, `onAny`/`disconnecting` hooks, spec-compliant `AckSender`, & `PresenceRegistry` | **97%** | **+4%** | Add `onAnyOutgoing` outbound interceptor |
| **`socket.io` (CSR Replay)** | Live `40{"pid":...,"offset":...}` handshake recovery + `LiveRedisStreamsAdapter::save_csr_snapshot`/`load_csr_snapshot` (`SETEX`/`GETDEL`) | **92%** | **+7%** | Wire `RecoveryManager` trait backend directly to `LiveRedisStreamsAdapter` |
| **`socket.io-adapter` (Redis/NATS/PG)** | `LiveRedisStreamsAdapter` (`redis` crate `XADD`/`SADD`/`PUBLISH`/`SETEX`/`GETDEL`) + `NatsJetStream` / `Postgres` traits | **74%** | **+4%** | Plug in live `async-nats` and `sqlx`/`tokio-postgres` network clients |
| **Adaptive `IoDriver` (`io_uring`)** | `AdaptiveDriver::probe` detects `io_uring` vs `epoll`/`kqueue`; runs on `tokio` | **35%** | — | Implement `compio`/`monoio` `io_uring` reactor (multishot + provided buffer rings) behind `IoDriver` |
| **Official E2E & Benchmarks** | `bridge_roundtrip.rs`, `e2e_compliance.rs`, `load_and_jitter.rs` pass | **48%** | — | Run live Node.js `socket.io-client@4.x` against `--serve` + multi-node Linux `1M` benchmark |
| **`sio-migrate-cli`** | Substring scanner + `scaffold_rust_namespace` | **25%** | — | Integrate `swc_ecma_parser` / `tree-sitter` for true JS/TS AST parsing and `gateway.toml` / handler generation |
