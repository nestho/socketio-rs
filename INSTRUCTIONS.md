# `socketio-rs` — Project Engineering & Developer Instructions

This document defines the architectural invariants, build instructions, coding standards, and step-by-step developer workflow for building and maintaining `socketio-rs`.

## 1. Core Architectural Invariants (Non-Negotiable Rules)

1. **Zero Unsafe in Protocol & Parser Crates**:
   - `engineio-core`, `socketio-parser`, `socketio-server`, and `socketio-adapters` enforce `#![forbid(unsafe_code)]`.
   - Zero `.unwrap()` or `.expect()` calls are permitted on untrusted network inputs, query parameters, session IDs (`sid`), or packet payloads (preventing [CVE-2026-59724](https://nvd.nist.gov/vuln/detail/CVE-2026-59724)-class Denial-of-Service panics).
2. **Adaptive `IoDriver` Abstraction (`io_uring` + `epoll` Fallback)**:
   - Never hardcode transport state machines directly to a single OS primitive.
   - At startup, `engineio-core::driver::detect_optimal_driver()` probes whether `io_uring` is permitted by the host kernel and container `seccomp` policy ([Upwind 2025](https://www.upwind.io/feed/io_uring-linux-performance-boost-or-security-headache)). If blocked (default Docker/Kubernetes `seccomp`) or running on macOS (`kqueue`), it cleanly falls back to pinned per-core `epoll`/`kqueue` event loops.
3. **Zero-Copy Broadcast Fan-Out (`bytes::Bytes`)**:
   - Outbound broadcasts must be serialized **once** into an immutable ref-counted `bytes::Bytes` buffer and distributed across room subscribers via O(1) pointer clones—never re-serializing JSON per recipient.
4. **Bounded Backpressure & Streaming Payload Guards**:
   - Every connection enforces streaming `maxPayload` checks *before* full heap allocation (mitigating [CVE-2026-59725](https://access.redhat.com/security/cve/cve-2026-59725)).
   - Per-socket outbound queues enforce explicit high/low watermarks (`write_high_watermark_bytes = 131072` [128 KiB], `write_low_watermark_bytes = 16384` [16 KiB] per [Envoy 1.39.0](https://www.envoyproxy.io/docs/envoy/v1.40.1/version_history/v1.39/v1.39.0)).
5. **Hardware Locality & Cache-Line Separation**:
   - Hot-path metrics and cross-core ring indices use `#[repr(align(64))]` cache-line padding to prevent CPU L1/L2 false sharing ([Building a High-Throughput API Gateway in Rust 2026](https://medium.com/@koutilyae/building-a-high-throughput-api-gateway-in-rust-fc37ca34955a)).
6. **Mandatory Handshake Security**:
   - Enforce Cross-Site WebSocket Hijacking (CSWSH) `Origin` allowlists ([OWASP WebSocket Security](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html)) and a maximum `100:1` decompressed-to-compressed ratio on `permessage-deflate` payloads ([OpenAI Production Best Practices](https://developers.openai.com/api/docs/guides/production-best-practices)).

## 2. Developer Workflow & Commands

### Build & Test
```bash
# Check all crates in the workspace
cargo check --workspace --all-targets

# Run unit and integration tests across all 6 crates
cargo test --workspace

# Run clippy lints with strict warnings
cargo clippy --workspace --all-targets -- -D warnings
```

### Running the Standalone Gateway (Path A: Zero-Rewrite Sidecar)
```bash
cargo run -p socketio-gateway -- --config gateway.toml
```

### Running the Migration Assistant CLI (`sio-migrate`)
```bash
# Scan a Node.js / TypeScript Socket.IO repository and output readiness + ROI report
cargo run -p sio-migrate-cli -- scan --path /path/to/nodejs-project

# Generate a drop-in Sidecar Gateway configuration for an existing project
cargo run -p sio-migrate-cli -- init-sidecar --output ./gateway.toml
```

## 3. Linux Production Kernel Tuning Baseline (1M+ Connections)

When deploying `socketio-gateway` or a native `socketio-server` binary on Linux 6.x hosts targeting 100K–1M+ concurrent connections, apply the following `/etc/sysctl.d/99-socketio-rs.conf` parameters ([OneUptime 2026](https://oneuptime.com/blog/post/2026-01-24-websocket-max-connections-limits/view); [Cloudflare TCP Tuning](https://blog.cloudflare.com/optimizing-tcp-for-high-throughput-and-low-latency/)):

```ini
fs.file-max = 2097152
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_rmem = 4096 16384 16777216
net.ipv4.tcp_wmem = 4096 16384 16777216
net.ipv4.tcp_adv_win_scale = -2
net.ipv4.tcp_notsent_lowat = 16384
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
```

## 4. Phased Development Execution Order

1. **Phase 0 (Completed)**: Architecture suite (`docs/01..04`), workspace initialization, trait boundaries, and baseline unit tests.
2. **Phase 1 (Next Up)**: Wire `hyper` HTTP/1.1 & HTTP/2 long-polling and `tokio-tungstenite` WebSocket transport handlers into `engineio-core`, plus SIMD JSON (`sonic-rs`) and MsgPack into `socketio-parser`.
3. **Phase 2**: Complete Tower `Layer`/`Service` integration and extractor macros (`Data<T>`, `AckSender`, `Bin`, `SocketRef`) in `socketio-server` and run the official Socket.IO E2E compliance suite.
4. **Phase 3**: Integrate `wtransport` HTTP/3 WebTransport and Redis/Valkey Streams + NATS JetStream durable CSR in `socketio-adapters`.
5. **Phase 4**: Expand `sio-migrate-cli` with AST parsing (`swc`/`tree-sitter`) and continuous `cargo-fuzz` verification.
