# Security Policy

## Supported Versions

| Version | Supported | Notes |
| :--- | :--- | :--- |
| `0.1.x` (`main` / `develop`) | Yes | Active security fixes and protocol hardening |

## Built-In Security Guarantees

`socketio-rs` enforces `#![forbid(unsafe_code)]` across all 6 workspace crates and mitigates known real-time Engine.IO / Socket.IO attack vectors by default:

1. **Crafted SID Denial-of-Service (CVE-2026-59724)**: Constant-bounded `validate_sid` rejects malformed, oversized, or non-URL-safe `sid` tokens before allocating session or WebTransport state.
2. **HTTP Polling Binary POST Exhaustion (CVE-2026-59725)**: `StreamingPayloadGuard` tracks `maxPayload` per incoming `hyper::Body` chunk *before* allocating a contiguous heap buffer.
3. **Decompression Bombs**: `permessage-deflate` (`flate2`) and `zstd` delta decoders enforce a strict `100:1` maximum expansion ratio and a `1 KB` compression threshold.
4. **Cross-Site WebSocket Hijacking (CSWSH)**: `validate_origin` enforces `allowed_origins` and `require_origin_header` on `/socket.io/` handshakes (`403 Forbidden` on mismatch).
5. **Slow-Consumer Memory Exhaustion**: `WriteWatermarkController` enforces `128 KiB` high / `16 KiB` low outbound watermarks per socket.

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.
Report security findings privately to **nestho@icloud.com** with:
- Affected crate (`engineio-core`, `socketio-parser`, `socketio-server`, `socketio-adapters`, `socketio-gateway`, or `sio-migrate-cli`)
- Minimal reproduction steps or packet payload
- Impact assessment

We will acknowledge receipt within 48 hours and coordinate a fix on a `hotfix/*` branch prior to public disclosure.
