# `socketio-rs` — High-Performance Rust Socket.IO Engine & Polyglot Sidecar Gateway

Wire-compatible with [Socket.IO v4/v5](https://socket.io/docs/v4/how-it-works/) and [Engine.IO v4](https://socket.io/docs/v4/engine-io-protocol/), engineered in memory-safe Rust (`#![forbid(unsafe_code)]` across all 6 crates) for low tail latency, high connection density, native HTTP/3 WebTransport, durable Connection State Recovery (CSR), and zero-friction migration from Node.js [`socketio/socket.io`](https://github.com/socketio/socket.io).

> **Status (`v0.1.0` Preview)**: All **53 workspace tests** and `cargo clippy --workspace --all-targets -- -D warnings` pass (`0` warnings). Ready for staging evaluation and community testing; external Node.js `socket.io-client@4.x` compliance and 72-hour Linux soak tests are scheduled before `v1.0.0` GA.

## Architecture: Two Ways to Adopt `socketio-rs`

### Path A: Zero-Rewrite Polyglot Sidecar (`socketio-gateway`)
Keep your existing **Python, Node.js, Go, PHP, or Java** business logic while offloading persistent WebSocket, HTTP long-polling, and HTTP/3 WebTransport connections to `socketio-gateway`:

- **Inbound client events** -> forwarded via `webhook_url` (`POST` JSON) to your backend.
- **Outbound broadcasts & room control** -> your backend calls `POST /_broadcast`, `POST /_rooms/join`, `POST /_rooms/leave`, and `POST /_disconnect` on the local control listener (`127.0.0.1:3001`).

### Path B: Native Rust Embedding (`socketio-server` + `engineio-core`)
Embed `SocketIoLayer` (`tower::Layer`) and `SocketIoEngineBridge` directly into your `hyper` or `axum` service with Axum-style extractors (`Data<T>`, `TryData<T>`, `AckSender`, `Bin`, `State<S>`).

## Comparison: `socketio/socket.io` vs. `socketioxide` vs. `socketio-rs`

| Capability | [`socketio/socket.io`](https://github.com/socketio/socket.io) (Node.js) | [`Totodore/socketioxide`](https://github.com/Totodore/socketioxide) (Rust) | `socketio-rs` |
| :--- | :--- | :--- | :--- |
| **Engine.IO v4 (Polling + WS + `2probe`->`3probe`->`5`)** | Yes | Yes | **Yes** (`v4`/`EIO=3` flag + `OPTIONS` CORS credentials) |
| **HTTP/3 WebTransport (`QUIC`)** | Experimental (`@socket.io/web-transport`) | Open issue ([#261](https://github.com/Totodore/socketioxide/issues/261)) | **Built-in** (`wtransport`/`quinn` + single-stream guard) |
| **Polyglot Sidecar Binary (`/_broadcast`, `/_rooms/*`)** | No (Node.js only) | No (Rust library only) | **Built-in** (`socketio-gateway` + `gateway.toml` + `webhook_url`) |
| **JSON & Binary Parsers** | `JSON.parse` + MsgPack | `serde_json` + `rmp-serde` | **`sonic-rs` SIMD JSON** + `rmp-serde` + `SharedBytes` zero-copy |
| **Distributed Adapters** | Redis / Mongo / Postgres / GCP | Redis / Postgres | **Redis Streams (`XADD`/`XREAD`), NATS JetStream, Postgres (`LISTEN/NOTIFY`)** |
| **Connection State Recovery (CSR)** | In-memory or adapter | In-memory | **Ring buffer + durable Redis `CsrSnapshotStore` (`SETEX`/`GETDEL`)** |
| **Security & Leak Mitigations** | Manual tuning (CVE-2026-59724/59725, `#1736`, `#3477`) | Rust memory safety | **Streaming `maxPayload`, constant-time `sid`, `100:1` zip-bomb guard, CSWSH `Origin`, empty-room pruning (`#1736`), `128 KiB` watermarks** |
| **Native Presence & Delta Compression** | No | No | **Built-in `PresenceRegistry`, `PushFallbackHook`, & `zstd` delta compression** |
| **Automated Migration CLI** | N/A | N/A | **`sio-migrate`** (JS/TS lexical analyzer + `gateway.toml` & Rust scaffolder) |

## Measured Local Benchmark (`--release`)

- **10,000-Subscriber Zero-Copy Fan-Out** (Apple Silicon macOS, 8 shards, `sonic-rs` SIMD JSON + `SharedBytes`): **`4.38 ms`** (`load_and_jitter::load_10k_subscribers_zero_copy_sharded_fanout`).
- **WiFi-Jitter Recovery**: `100%` missed-frame delivery across simulated 2-second disconnects via `RecoveryManager` (`pid` + `offset`).

## Quick Start

```bash
# 1. Verify lints and all 53 workspace tests
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# 2. Build and run the standalone polyglot gateway (HTTP/WS + HTTP/3 WebTransport)
cargo build --release -p socketio-gateway -p sio-migrate-cli
./target/release/socketio-gateway --serve --bind 0.0.0.0:3000 --webtransport-bind 0.0.0.0:3443

# 3. Scan an existing Node.js Socket.IO repo and scaffold gateway.toml + Rust handlers
./target/release/sio-migrate --scan /path/to/node-app --scaffold ./migration-out
```

### Control API Endpoints (`127.0.0.1:3001`)

| Method & Path | Payload / Query | Purpose |
| :--- | :--- | :--- |
| `POST /_broadcast` | `{"namespace":"/","room":"chat:1","event":"msg","data":{...}}` | Zero-copy room broadcast (local + cluster adapter) |
| `POST /_rooms/join` | `{"sid":"<sid>","room":"chat:1"}` | Join a connected socket to a room |
| `POST /_rooms/leave` | `{"sid":"<sid>","room":"chat:1"}` | Remove a socket from a room (prunes empty rooms) |
| `POST /_disconnect` | `{"sid":"<sid>"}` | Server-initiated disconnect (`41` + cleanup) |
| `GET /_presence` | `?room=chat:1` | List tracked `PresenceMember` entries |
| `GET /_stats` | — | Live `active_sessions`, `active_rooms`, `recovered_sessions`, `broadcasts_total` |
| `GET /healthz` | — | Readiness probe (`{"status":"ok","shards":N}`) |
| `GET /metrics` | — | Prometheus metrics (`text/plain; version=0.0.4`) |

## Workspace Crates & Documentation

| Crate | Path | Description |
| :--- | :--- | :--- |
| `engineio-core` | [`crates/engineio-core`](./crates/engineio-core) | Adaptive `IoDriver`, Engine.IO v4 polling (`OPTIONS` CORS + `b...` binary), WebSocket (`2probe`->`3probe`->`5`), HTTP/3 WebTransport, streaming `maxPayload`, and write watermarks. |
| `socketio-parser` | [`crates/socketio-parser`](./crates/socketio-parser) | Zero-copy `SharedBytes` v5 codec, `sonic-rs` SIMD JSON, `rmp-serde` MsgPack, `flate2` (`100:1` zip-bomb guard), and `zstd` delta compression. |
| `socketio-server` | [`crates/socketio-server`](./crates/socketio-server) | Namespaces, `io.use_ns(...)` middleware (`44` `CONNECT_ERROR`), `ShardedRoomIndex`, `AckTable`, `SocketIoLayer`, `PresenceRegistry`, and CSWSH `Origin` guard. |
| `socketio-adapters` | [`crates/socketio-adapters`](./crates/socketio-adapters) | Durable CSR (`RecoveryManager` + `CsrSnapshotStore`) and live `LiveRedisStreamsAdapter`, `LiveNatsJetStreamAdapter`, and `LivePostgresAdapter`. |
| `socketio-gateway` | [`crates/socketio-gateway`](./crates/socketio-gateway) | Standalone polyglot edge gateway binary (`tikv-jemallocator` on Linux) with control API, webhook forwarder, and `/metrics`. |
| `sio-migrate-cli` | [`crates/sio-migrate-cli`](./crates/sio-migrate-cli) | Node.js/TS lexical analyzer and scaffolder (`gateway.toml`, `migrated_handlers.rs`, `MIGRATION_PLAN.md`). |

All deep architecture reports, HLD, roadmap, sprint backlog, and gap audits are in [`./docs/`](./docs/) and [`INSTRUCTIONS.md`](./INSTRUCTIONS.md).

## License

Dual-licensed under MIT or Apache-2.0.
