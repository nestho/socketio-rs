# Changelog

All notable changes to `socketio-rs` are documented in this file.

## [0.1.0] — 2026-11-14 (Public Preview)

### Added
- **`engineio-core`**: Engine.IO v4 HTTP long-polling (`OPTIONS` CORS preflight + credentials, `GET`/`POST`, `b<base64>` binary), RFC 6455 WebSocket (`tokio-tungstenite` + `2probe`->`3probe`->`5` upgrade + queued packet drain), HTTP/3 WebTransport (`wtransport`/`quinn`/`rustls` + single-stream guard + SHA-256 cert hash), optional `v4` (`EIO=3`) flag, `StreamingPayloadGuard` (CVE-2026-59725), `validate_sid` (CVE-2026-59724), `WriteWatermarkController`, and `AdaptiveDriver` (`socket2` `SO_REUSEPORT` + `TCP_NODELAY`/`SO_KEEPALIVE`).
- **`socketio-parser`**: Zero-copy `SharedBytes` (`Arc<[u8]>`), Socket.IO v5 packet codec, `sonic-rs` SIMD JSON, `rmp-serde` MsgPack (`msgpack` feature), `5`/`6` binary attachment reconstruction, variadic multi-arg payloads, `flate2` `permessage-deflate` (`100:1` zip-bomb guard), and `zstd` delta compression.
- **`socketio-server`**: Static & prefix namespaces, `io.use_ns(...)` middleware (`44` `CONNECT_ERROR`), `ShardedRoomIndex` with immediate empty-room pruning (`#1736`), `fetchSockets`/`socketsJoin`/`socketsLeave`, `AckTable`/`AckSender`, `onAny`/`onAnyOutgoing`/`disconnecting` hooks, Axum-style extractors, `SocketIoLayer` (`tower::Layer`), CSWSH `Origin` validation, and `PresenceRegistry`.
- **`socketio-adapters`**: `pid`+`offset` Connection State Recovery (`RecoveryManager`, `CsrReplayLog`, `CsrSnapshotStore`) + live `LiveRedisStreamsAdapter` (`redis`), `LiveNatsJetStreamAdapter` (`async-nats`), and `LivePostgresAdapter` (`tokio-postgres` `pg_notify`/`LISTEN`).
- **`socketio-gateway`**: Standalone polyglot sidecar binary with `gateway.toml`, `/_broadcast`, `/_rooms/join`, `/_rooms/leave`, `/_disconnect`, `/_presence`, `/_stats`, `/healthz`, `/metrics`, outbound `webhook_url` event forwarder, `SIGTERM` drain, and Linux `tikv-jemallocator`.
- **`sio-migrate-cli`**: Comment/string-aware JS/TS lexical scanner (`--scan`, `--scaffold`, `--json`) generating `gateway.toml`, `migrated_handlers.rs`, and `MIGRATION_PLAN.md`.
- **Verification**: 53/53 workspace tests passing, `cargo clippy --workspace --all-targets -- -D warnings` clean (`0` warnings), and `4.38 ms` measured `--release` 10K-subscriber zero-copy fan-out.
