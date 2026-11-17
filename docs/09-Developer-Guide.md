# `socketio-rs` Developer Guide

This guide covers how to run `socketio-gateway` as a polyglot sidecar, embed `socketio-server` natively in Rust, configure distributed adapters and HTTP/3 WebTransport, and troubleshoot deployments.

## 1. Workspace Architecture

| Crate | Responsibility | Key Public Types |
| :--- | :--- | :--- |
| `engineio-core` | Transport & framing: HTTP long-polling (`OPTIONS` CORS + `b...` binary), WebSocket (`2probe`->`3probe`->`5`), HTTP/3 WebTransport, streaming `maxPayload`, write watermarks | `EngineConfig`, `SessionStore`, `AdaptiveDriver`, `IoDriver`, `WebTransportTlsConfig` |
| `socketio-parser` | Wire codec: Engine.IO/Socket.IO v5 frames, `SharedBytes` zero-copy buffer, `sonic-rs` SIMD JSON, `rmp-serde` MsgPack, `flate2` (`100:1` ratio cap), `zstd` delta | `SocketIoPacket`, `PacketType`, `SharedBytes`, `encode_event_simd`, `parse_dynamic_event` |
| `socketio-server` | Server core: sharded room index, `SocketIo`, namespaces, `io.use_ns(...)` middleware, Axum-style extractors, `AckTable`, `PresenceRegistry`, `SocketIoLayer`, CSWSH guard | `SocketIo`, `SocketIoEngineBridge`, `SocketRef`, `Data<T>`, `AckSender`, `PresenceRegistry`, `validate_origin` |
| `socketio-adapters` | Clustering & CSR: `RecoveryManager`, `CsrSnapshotStore`, Redis Streams (`LiveRedisStreamsAdapter`), NATS JetStream (`LiveNatsJetStreamAdapter`), Postgres (`LivePostgresAdapter`) | `Adapter`, `RecoveryManager`, `CsrSnapshotStore`, `LiveRedisStreamsAdapter` |
| `socketio-gateway` | Standalone polyglot edge binary with `gateway.toml`, HTTP control API, `webhook_url` forwarder, `/healthz`, `/_stats`, `/metrics` | CLI binary (`socketio-gateway`) |
| `sio-migrate-cli` | Comment/string-aware JS/TS scanner and scaffolder (`gateway.toml`, `migrated_handlers.rs`, `MIGRATION_PLAN.md`) | CLI binary (`sio-migrate`) |

## 2. Path A: Polyglot Sidecar (`socketio-gateway`)

Use `socketio-gateway` when your application logic lives in **Python, Node.js, Go, PHP, Ruby, or Java** and you want Rust handling persistent sockets, rooms, CSR, and backpressure.

### 2.1 Running the Gateway

```bash
cargo build --release -p socketio-gateway
./target/release/socketio-gateway --help
./target/release/socketio-gateway --serve --config ./gateway.toml --bind 0.0.0.0:3000
```

Enable HTTP/3 WebTransport on UDP `3443`:

```bash
./target/release/socketio-gateway --serve \
  --bind 0.0.0.0:3000 \
  --webtransport-bind 0.0.0.0:3443 \
  --wt-cert ./certs/fullchain.pem \
  --wt-key ./certs/privkey.pem
```

### 2.2 `gateway.toml` Reference

```toml
[server]
bind_addr = "127.0.0.1:3000"
path = "/socket.io/"
io_driver = "auto"                      # "auto" | "epoll"
worker_threads = 0                      # 0 = physical core count via SO_REUSEPORT
shutdown_drain_timeout_ms = 15000
max_connection_duration_jitter_pct = 15 # anti-thundering-herd disconnect jitter

[engine]
ping_interval_ms = 25000
ping_timeout_ms = 20000
max_payload_bytes = 1000000
write_high_watermark_bytes = 131072     # 128 KiB slow-client cap
write_low_watermark_bytes = 16384       # 16 KiB resume threshold
tcp_notsent_lowat_bytes = 16384
enable_webtransport_h3 = true

[security]
allowed_origins = ["https://app.example.com"]
require_origin_header = true
max_decompression_ratio = 100
permessage_deflate_threshold_bytes = 1024
server_max_window_bits = 10

[recovery]
enabled = true
max_disconnection_duration_ms = 120000
ring_buffer_capacity_per_room = 512
adapter = "in_memory"                   # "in_memory" | "redis" | "redis_streams" | "nats" | "nats_jetstream" | "postgres"

[bridge]
broadcast_bind_addr = "127.0.0.1:3001"
```

### 2.3 Control API (`127.0.0.1:3001`)

All control endpoints accept and return JSON over HTTP/1.1 on `bridge.broadcast_bind_addr`:

| Endpoint | Method | Request Body / Query | Response |
| :--- | :--- | :--- | :--- |
| `/_broadcast` | `POST` | `{"namespace":"/","room":"chat:1","event":"message","data":{"text":"hello"}}` | `{"delivered":N}` |
| `/_rooms/join` | `POST` | `{"sid":"<socket_id>","room":"chat:1"}` | `{"ok":true}` |
| `/_rooms/leave` | `POST` | `{"sid":"<socket_id>","room":"chat:1"}` | `{"ok":true}` |
| `/_disconnect` | `POST` | `{"sid":"<socket_id>"}` | `{"disconnected":true}` |
| `/_presence` | `GET` | `?room=chat:1` | `[{"sid":"...","user_id":"...","metadata":{...}}]` |
| `/_stats` | `GET` | — | `{"active_sessions":N,"active_rooms":N,"recovered_sessions":N,"broadcasts_total":N}` |
| `/healthz` | `GET` | — | `{"status":"ok","shards":N}` |
| `/metrics` | `GET` | — | Prometheus `text/plain; version=0.0.4` |

### 2.4 Inbound Event Webhooks (`webhook_url` / `GATEWAY_WEBHOOK_URL`)

When `bridge.webhook_url` or `GATEWAY_WEBHOOK_URL` is set, every inbound Socket.IO event triggers an async HTTP/1.1 `POST` with:

```json
{
  "sid": "session_id_here",
  "namespace": "/",
  "event": "send_message",
  "payload": "[\"send_message\",{\"room\":\"chat:1\",\"text\":\"hi\"}]"
}
```

### 2.5 Distributed Clustering Environment Variables

- **Redis Streams** (`adapter = "redis"` or `"redis_streams"`): configure `REDIS_URL`
- **NATS JetStream** (`adapter = "nats"` or `"nats_jetstream"`): configure `NATS_URL`
- **PostgreSQL `LISTEN/NOTIFY`** (`adapter = "postgres"`): configure `DATABASE_URL`

## 3. Path B: Native Rust Embedding (`socketio-server`)

Run the included examples directly:

```bash
cargo run -p socketio-server --example echo_server
cargo run -p socketio-server --example chat_rooms
cargo run -p socketio-server --example recovery_and_presence
```

### Core Patterns

- **Namespace Authentication Middleware (`io.use_ns`)**:
  Returning `Err("reason".into())` automatically emits a Socket.IO v5 `44<ns>,{"message":"reason"}` (`CONNECT_ERROR`) packet:
  ```rust
  io.use_ns("/admin", |_sid, auth| {
      if auth.get("token").is_some() { Ok(()) } else { Err("unauthorized".into()) }
  });
  ```
- **Typed Extractors & Acknowledgements**:
  Use `Data<T>::from_context(&ctx)` for deserializing event payloads and `AckSender::from_context(&ctx).send(&reply)` for callback acknowledgements.
- **Lifecycle Hooks**:
  - `bridge.register_on_any(&socket.id, ...)` — inspect all inbound events.
  - `bridge.register_on_any_outgoing(&socket.id, ...)` — inspect outbound frames.
  - `bridge.register_disconnecting(&socket.id, ...)` — observe room list before automatic room pruning.

## 4. Automated Migration (`sio-migrate`)

```bash
cargo build --release -p sio-migrate-cli
./target/release/sio-migrate --help
./target/release/sio-migrate --scan /path/to/node-socketio-app --scaffold ./migration-out
```

This generates `gateway.toml`, `migrated_handlers.rs`, and `MIGRATION_PLAN.md` while detecting `cors`, `connectionStateRecovery`, namespaces, rooms, and `socket.request` retention.

## 5. Troubleshooting

- **`403 Forbidden Origin` on handshake**: Add your frontend origin to `security.allowed_origins` in `gateway.toml`, or set `require_origin_header = false` for non-browser CLI clients.
- **Decompression ratio limit**: Payloads expanding beyond `100:1` are rejected automatically (`max_decompression_ratio`).
- **`io_uring` in Docker/seccomp**: `AdaptiveDriver::probe` falls back cleanly to `epoll`/`kqueue` if `io_uring` syscalls are blocked; set `io_driver = "epoll"` in `gateway.toml` to skip probing.
