# `socketio-rs` — Sprint 7: Beyond-Upstream Hardening & Missing Capabilities

Based on a deep audit of [`socketio/socket.io`](https://github.com/socketio/socket.io) open issues and production limitations, Sprint 7 addresses structural weaknesses present in the Node.js reference implementation:

| Upstream Weakness / Limitation | Source | `socketio-rs` Architectural Solution |
| :--- | :--- | :--- |
| **`perMessageDeflate` RSS bloat & >25 KB leak** | [Issue #3477](https://github.com/socketio/socket.io/issues/3477), [Issue #3595](https://github.com/socketio/socket.io/issues/3595) | Disabled by default; 1 KB threshold + `100:1` decompression-bomb cap + Linux `tikv-jemallocator` |
| **EIO=3 / EIO=4 version-mismatch reconnect leak** | [Issue #3477](https://github.com/socketio/socket.io/issues/3477) | Strict handshake version rejection + immediate socket teardown + optional `v4` (`EIO=3`) feature flag |
| **`MaxListenersExceededWarning` & closure leaks** | [Issue #4708](https://github.com/socketio/socket.io/issues/4708), [Issue #3553](https://github.com/socketio/socket.io/issues/3553) | Fixed per-session `HashMap` dropped deterministically on `on_close` (no `EventEmitter` listener arrays) |
| **Unbounded `writeBuffer` growth on disconnect** | [Issue #3582](https://github.com/socketio/socket.io/issues/3582) | Bounded `mpsc::channel(256)` + `WriteWatermarkController` (128 KiB / 16 KiB) |
| **Empty room map leak under high room churn** | [Issue #1736](https://github.com/socketio/socket.io/issues/1736) | **Fixed in `ShardedRoomIndex`**: `leave` & `unregister_socket` prune empty rooms immediately |
| **Sticky-session lock-in & autoscaling friction** | [Ably 2026](https://ably.com/topic/scaling-socketio) | WebSocket-first direct connect mode (no polling upgrade needed) + externalized Redis/NATS CSR |
| **Variadic args & no binary `serverSideEmit`** | [Socket.IO Server API](https://socket.io/docs/v4/server-api/) | Multi-arg `Vec<Value>` parser support + binary-capable `ClusterBroadcastPacket` |
| **No Presence, Delta Compression, or Mobile Push** | [DEV Community](https://dev.to/ably/challenges-of-scaling-websockets-3493) | Native Presence state, `zstd`/JSON-diff delta compression, and APNs/FCM offline fallback hook |

## Sprint 7 Tasks (`S7-T01`–`S7-T04`)

- [x] **S7-T01** Prune empty room sets in `ShardedRoomIndex::leave` & `unregister_socket` (mitigating [Issue #1736](https://github.com/socketio/socket.io/issues/1736)).
- [x] **S7-T02** Add `EIO=3` (`v4` protocol) feature flag + anti-reconnect-flood teardown on version mismatch ([Issue #3477](https://github.com/socketio/socket.io/issues/3477)).
- [x] **S7-T03** Support variadic multi-argument event payloads (`["ev", a, b, c]`) in `socketio-parser` and binary payloads in `Adapter::server_side_emit`.
- [x] **S7-T04** Add native Presence tracking, `zstd` delta compression for slow/mobile clients, and an offline APNs/FCM push fallback webhook.
