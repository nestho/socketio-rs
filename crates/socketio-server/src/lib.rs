#![forbid(unsafe_code)]
//! `socketio-server` provides Discord-inspired sharded room fan-out,
//! `#[repr(align(64))]` cache-line padded atomic counters, Cross-Site WebSocket
//! Hijacking (CSWSH) `Origin` validation, `AckTable`, Axum-style extractors,
//! static/dynamic namespaces, and `SocketIoLayer` (`tower::Layer`).

pub mod ack;
pub mod bridge;
pub mod extract;
pub mod layer;
pub mod presence;

pub mod prelude {
    pub use crate::ack::{AckError, AckTable};
    pub use crate::bridge::SocketIoEngineBridge;
    pub use crate::extract::{AckSender, Bin, Data, EventContext, FromEventContext, State, TryData};
    pub use crate::layer::{LayerConfig, SocketIoLayer, SocketIoService};
    pub use crate::presence::{PresenceDiff, PresenceMember, PresenceRegistry, PushFallbackHook};
    pub use crate::{
        validate_origin, BroadcastOperator, CacheLinePaddedCounter, NamespaceRegistry,
        ShardedRoomIndex, SocketIo, SocketRef,
    };
}

use serde::Serialize;
use socketio_parser::{encode_event_simd, encode_v5_packet, PacketType, SharedBytes, SocketIoPacket};
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Cache-line padded atomic counter (`64 bytes`) to eliminate false sharing
/// across pinned worker cores on the broadcast hot path.
#[repr(align(64))]
#[derive(Debug, Default)]
pub struct CacheLinePaddedCounter {
    value: AtomicU64,
}

impl CacheLinePaddedCounter {
    pub const fn new(val: u64) -> Self {
        Self {
            value: AtomicU64::new(val),
        }
    }

    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Validates the HTTP `Origin` header during WebSocket / Engine.IO handshakes
/// to prevent Cross-Site WebSocket Hijacking (CSWSH) attacks.
pub fn validate_origin(
    origin_header: Option<&str>,
    allowed_origins: &[String],
    require_origin: bool,
) -> bool {
    match origin_header {
        Some(origin) => allowed_origins
            .iter()
            .any(|allowed| allowed == "*" || allowed == origin),
        None => !require_origin,
    }
}

/// A sharded room membership index that partitions subscribers and their live
/// `mpsc::Sender<SharedBytes>` outbound channels across `N` shards (`S2-T07`).
#[derive(Debug)]
pub struct ShardedRoomIndex {
    shards: Vec<RwLock<ShardState>>,
    broadcast_counter: CacheLinePaddedCounter,
}

#[derive(Debug, Default)]
struct ShardState {
    rooms: HashMap<String, HashSet<String>>,
    sockets: HashMap<String, mpsc::Sender<SharedBytes>>,
}

impl ShardedRoomIndex {
    pub fn new(shard_count: usize) -> Self {
        let count = shard_count.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(RwLock::new(ShardState::default()));
        }
        Self {
            shards,
            broadcast_counter: CacheLinePaddedCounter::new(0),
        }
    }

    fn shard_for_socket(&self, sid: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        sid.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    /// Registers a live socket's bounded outbound channel in its owning shard.
    pub fn register_socket(&self, sid: &str, outbound: mpsc::Sender<SharedBytes>) {
        let idx = self.shard_for_socket(sid);
        if let Ok(mut guard) = self.shards[idx].write() {
            guard.sockets.insert(sid.to_owned(), outbound);
        }
    }

    /// Returns all rooms currently joined by `sid`.
    pub fn rooms_for_socket(&self, sid: &str) -> Vec<String> {
        let idx = self.shard_for_socket(sid);
        let mut out = Vec::new();
        if let Ok(guard) = self.shards[idx].read() {
            for (room, members) in &guard.rooms {
                if members.contains(sid) {
                    out.push(room.clone());
                }
            }
        }
        out
    }

    /// Removes a socket from its owning shard and all joined rooms, pruning empty rooms (Issue #1736).
    pub fn unregister_socket(&self, sid: &str) {
        let idx = self.shard_for_socket(sid);
        if let Ok(mut guard) = self.shards[idx].write() {
            guard.sockets.remove(sid);
            guard.rooms.retain(|_, members| {
                members.remove(sid);
                !members.is_empty()
            });
        }
    }

    pub fn join(&self, room: &str, sid: &str) {
        let idx = self.shard_for_socket(sid);
        if let Ok(mut guard) = self.shards[idx].write() {
            guard
                .rooms
                .entry(room.to_owned())
                .or_default()
                .insert(sid.to_owned());
        }
    }

    pub fn leave(&self, room: &str, sid: &str) {
        let idx = self.shard_for_socket(sid);
        if let Ok(mut guard) = self.shards[idx].write() {
            let empty = if let Some(set) = guard.rooms.get_mut(room) {
                set.remove(sid);
                set.is_empty()
            } else {
                false
            };
            if empty {
                guard.rooms.remove(room);
            }
        }
    }

    /// Returns total active (non-empty) room entries across all shards.
    pub fn active_room_count(&self) -> usize {
        self.shards
            .iter()
            .filter_map(|s| s.read().ok().map(|g| g.rooms.len()))
            .sum()
    }

    /// Upstream `io.fetchSockets()` parity: lists all socket IDs globally or in `room`.
    pub fn fetch_sockets(&self, room: Option<&str>) -> Vec<String> {
        let mut sids = HashSet::new();
        for shard in &self.shards {
            if let Ok(guard) = shard.read() {
                match room {
                    Some(r) => {
                        if let Some(members) = guard.rooms.get(r) {
                            for sid in members {
                                sids.insert(sid.clone());
                            }
                        }
                    }
                    None => {
                        for sid in guard.sockets.keys() {
                            sids.insert(sid.clone());
                        }
                    }
                }
            }
        }
        sids.into_iter().collect()
    }

    /// Upstream `io.in(target_room).socketsJoin(join_room)` parity.
    pub fn sockets_join(&self, target_room: &str, join_room: &str) {
        let targets = self.fetch_sockets(Some(target_room));
        for sid in targets {
            self.join(join_room, &sid);
        }
    }

    /// Upstream `io.in(target_room).socketsLeave(leave_room)` parity.
    pub fn sockets_leave(&self, target_room: &str, leave_room: &str) {
        let targets = self.fetch_sockets(Some(target_room));
        for sid in targets {
            self.leave(leave_room, &sid);
        }
    }

    /// Zero-copy fan-out of an already-encoded `SharedBytes` packet across all shards,
    /// pushing `O(1)` clones into live socket outbound channels while respecting `except_sids`.
    pub fn broadcast_to_rooms(
        &self,
        target_rooms: &[String],
        except_rooms: &[String],
        except_sids: &[String],
        encoded_packet: &SharedBytes,
    ) -> usize {
        self.broadcast_counter.inc();
        let mut delivered = 0usize;

        for shard in &self.shards {
            if let Ok(guard) = shard.read() {
                let mut excluded_sids: HashSet<&str> =
                    except_sids.iter().map(String::as_str).collect();
                for er in except_rooms {
                    if let Some(members) = guard.rooms.get(er) {
                        for sid in members {
                            excluded_sids.insert(sid.as_str());
                        }
                    }
                }

                let mut seen = HashSet::new();
                for room in target_rooms {
                    if let Some(members) = guard.rooms.get(room) {
                        for sid in members {
                            if !excluded_sids.contains(sid.as_str()) && seen.insert(sid.as_str()) {
                                if let Some(tx) = guard.sockets.get(sid) {
                                    // Live channel present: non-blocking O(1) push
                                    if tx.try_send(encoded_packet.clone()).is_ok() {
                                        delivered += 1;
                                    }
                                } else {
                                    // Membership-only unit check / dry fan-out
                                    let _zero_copy = encoded_packet.clone();
                                    delivered += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        delivered
    }

    pub fn broadcast_zero_copy(&self, room: &str, encoded_packet: &SharedBytes) -> usize {
        self.broadcast_to_rooms(&[room.to_owned()], &[], &[], encoded_packet)
    }

    pub fn total_broadcasts(&self) -> u64 {
        self.broadcast_counter.get()
    }
}

/// Fluent `to` / `in` / `except` / `volatile` / `local` broadcast builder (`S2-T07`).
#[derive(Debug, Clone)]
pub struct BroadcastOperator {
    namespace: String,
    rooms: Arc<ShardedRoomIndex>,
    target_rooms: Vec<String>,
    except_rooms: Vec<String>,
    except_sids: Vec<String>,
    pub volatile: bool,
    pub local: bool,
}

impl BroadcastOperator {
    pub fn new(namespace: impl Into<String>, rooms: Arc<ShardedRoomIndex>) -> Self {
        Self {
            namespace: namespace.into(),
            rooms,
            target_rooms: Vec::new(),
            except_rooms: Vec::new(),
            except_sids: Vec::new(),
            volatile: false,
            local: false,
        }
    }

    pub fn to(mut self, room: impl Into<String>) -> Self {
        self.target_rooms.push(room.into());
        self
    }

    pub fn in_room(self, room: impl Into<String>) -> Self {
        self.to(room)
    }

    pub fn except(mut self, room: impl Into<String>) -> Self {
        self.except_rooms.push(room.into());
        self
    }

    pub fn except_sid(mut self, sid: impl Into<String>) -> Self {
        self.except_sids.push(sid.into());
        self
    }

    pub fn volatile(mut self) -> Self {
        self.volatile = true;
        self
    }

    pub fn local(mut self) -> Self {
        self.local = true;
        self
    }

    pub fn emit<T: Serialize>(&self, event: &str, data: &T) -> usize {
        let Ok(payload) = encode_event_simd(event, data) else {
            return 0;
        };
        let frame = encode_v5_packet(&SocketIoPacket {
            packet_type: PacketType::Event,
            namespace: self.namespace.clone(),
            ack_id: None,
            attachments: 0,
            payload,
        });
        self.rooms.broadcast_to_rooms(
            &self.target_rooms,
            &self.except_rooms,
            &self.except_sids,
            &frame,
        )
    }
}

/// Ergonomic handle to a connected client socket (`S2-T06`, `S2-T07`).
#[derive(Debug, Clone)]
pub struct SocketRef {
    pub id: String,
    pub ns: String,
    rooms: Arc<ShardedRoomIndex>,
    outbound: mpsc::Sender<SharedBytes>,
}

impl SocketRef {
    pub fn new(
        id: impl Into<String>,
        ns: impl Into<String>,
        rooms: Arc<ShardedRoomIndex>,
        outbound: mpsc::Sender<SharedBytes>,
    ) -> Self {
        let id = id.into();
        rooms.register_socket(&id, outbound.clone());
        Self {
            id,
            ns: ns.into(),
            rooms,
            outbound,
        }
    }

    pub fn join(&self, room: &str) {
        self.rooms.join(room, &self.id);
    }

    pub fn leave(&self, room: &str) {
        self.rooms.leave(room, &self.id);
    }

    pub fn to(&self, room: impl Into<String>) -> BroadcastOperator {
        BroadcastOperator::new(self.ns.clone(), Arc::clone(&self.rooms))
            .to(room)
            .except_sid(self.id.clone())
    }

    pub fn broadcast(&self) -> BroadcastOperator {
        BroadcastOperator::new(self.ns.clone(), Arc::clone(&self.rooms))
            .except_sid(self.id.clone())
    }

    pub fn emit<T: Serialize>(&self, event: &str, data: &T) -> bool {
        let Ok(payload) = encode_event_simd(event, data) else {
            return false;
        };
        let frame = encode_v5_packet(&SocketIoPacket {
            packet_type: PacketType::Event,
            namespace: self.ns.clone(),
            ack_id: None,
            attachments: 0,
            payload,
        });
        self.outbound.try_send(frame).is_ok()
    }
}

pub type ConnectHandler = Arc<dyn Fn(SocketRef) + Send + Sync>;

/// Static and dynamic-prefix Namespace registry (`S2-T05`).
#[derive(Default)]
pub struct NamespaceRegistry {
    exact: RwLock<HashMap<String, ConnectHandler>>,
    prefixes: RwLock<Vec<(String, ConnectHandler)>>,
}

impl NamespaceRegistry {
    pub fn register_exact(&self, ns: impl Into<String>, handler: ConnectHandler) {
        if let Ok(mut guard) = self.exact.write() {
            guard.insert(ns.into(), handler);
        }
    }

    pub fn register_prefix(&self, prefix: impl Into<String>, handler: ConnectHandler) {
        if let Ok(mut guard) = self.prefixes.write() {
            guard.push((prefix.into(), handler));
        }
    }

    pub fn resolve(&self, ns: &str) -> Option<ConnectHandler> {
        if let Ok(guard) = self.exact.read() {
            if let Some(h) = guard.get(ns) {
                return Some(Arc::clone(h));
            }
        }
        if let Ok(guard) = self.prefixes.read() {
            for (prefix, h) in guard.iter() {
                if ns.starts_with(prefix) {
                    return Some(Arc::clone(h));
                }
            }
        }
        None
    }
}

/// Top-level `SocketIo` server registry mirroring the Node.js `io.of(...)` API.
pub struct SocketIo {
    pub rooms: Arc<ShardedRoomIndex>,
    pub namespaces: Arc<NamespaceRegistry>,
}

impl SocketIo {
    pub fn new(shard_count: usize) -> Self {
        Self {
            rooms: Arc::new(ShardedRoomIndex::new(shard_count)),
            namespaces: Arc::new(NamespaceRegistry::default()),
        }
    }

    pub fn ns<F>(&self, path: &str, handler: F)
    where
        F: Fn(SocketRef) + Send + Sync + 'static,
    {
        self.namespaces.register_exact(path, Arc::new(handler));
    }

    pub fn ns_prefix<F>(&self, prefix: &str, handler: F)
    where
        F: Fn(SocketRef) + Send + Sync + 'static,
    {
        self.namespaces.register_prefix(prefix, Arc::new(handler));
    }

    pub fn to(&self, room: impl Into<String>) -> BroadcastOperator {
        BroadcastOperator::new("/", Arc::clone(&self.rooms)).to(room)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_line_alignment_is_64_bytes() {
        assert_eq!(std::mem::align_of::<CacheLinePaddedCounter>(), 64);
    }

    #[test]
    fn test_cswsh_origin_validation() {
        let allowed = vec!["https://app.example.com".to_owned()];
        assert!(validate_origin(
            Some("https://app.example.com"),
            &allowed,
            true
        ));
        assert!(!validate_origin(
            Some("https://evil.attacker.com"),
            &allowed,
            true
        ));
        assert!(!validate_origin(None, &allowed, true));
    }

    #[test]
    fn test_sharded_room_zero_copy_fanout_and_except() {
        let io = SocketIo::new(8);
        let (tx1, mut rx1) = mpsc::channel(16);
        let (tx2, mut rx2) = mpsc::channel(16);

        let s1 = SocketRef::new("sid_alpha_12345678", "/", Arc::clone(&io.rooms), tx1);
        let s2 = SocketRef::new("sid_beta_87654321", "/", Arc::clone(&io.rooms), tx2);
        s1.join("guild:1");
        s2.join("guild:1");
        s2.join("muted");

        let sent = io
            .to("guild:1")
            .except("muted")
            .emit("msg", &"hello");
        assert_eq!(sent, 1);
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_err());
    }

    #[test]
    fn test_dynamic_and_static_namespace_resolution() {
        let io = SocketIo::new(4);
        io.ns("/", |_| {});
        io.ns_prefix("/dynamic-", |_| {});

        assert!(io.namespaces.resolve("/").is_some());
        assert!(io.namespaces.resolve("/dynamic-101").is_some());
        assert!(io.namespaces.resolve("/unknown").is_none());
    }
}
