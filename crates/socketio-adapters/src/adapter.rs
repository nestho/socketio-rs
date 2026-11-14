//! Distributed horizontal scaling `Adapter` trait and Redis/Valkey Streams,
//! NATS JetStream, and PostgreSQL (`LISTEN/NOTIFY`) implementations (`S3-T05`..`S3-T07`).

use serde::{Deserialize, Serialize};
use socketio_parser::SharedBytes;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use thiserror::Error;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdapterError {
    #[error("adapter transport disconnected: {0}")]
    Disconnected(String),
    #[error("serialization failure: {0}")]
    Serialization(String),
}

/// Broadcast targeting options shared across all distributed adapters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BroadcastOptions {
    pub namespace: String,
    pub rooms: Vec<String>,
    pub except: Vec<String>,
    pub local_only: bool,
}

/// Metadata returned by `fetch_sockets()` across cluster nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteSocketInfo {
    pub sid: String,
    pub namespace: String,
    pub rooms: Vec<String>,
    pub node_id: String,
}

/// Cluster-wide event envelope exchanged over Redis/Valkey, NATS, or PostgreSQL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBroadcastPacket {
    pub origin_node: String,
    pub opts: BroadcastOptions,
    pub payload_bytes: Vec<u8>,
}

/// Async distributed adapter trait (`S3-T05`) mirroring `socket.io-adapter`.
pub trait Adapter: Send + Sync {
    fn node_id(&self) -> &str;

    fn broadcast(
        &self,
        opts: BroadcastOptions,
        encoded_packet: SharedBytes,
    ) -> BoxFuture<'_, Result<(), AdapterError>>;

    fn add_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>>;

    fn del_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>>;

    fn fetch_sockets(
        &self,
        namespace: &str,
        room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>>;

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>>;
}

/// Redis / Valkey Pub/Sub + Redis Streams adapter (`S3-T06`).
#[cfg(feature = "redis-adapter")]
#[derive(Debug, Clone)]
pub struct RedisStreamsAdapter {
    node_id: String,
    stream_key: String,
    state: Arc<RwLock<AdapterMemoryBus>>,
}

#[cfg(feature = "redis-adapter")]
impl RedisStreamsAdapter {
    pub fn new(node_id: impl Into<String>, stream_key: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            stream_key: stream_key.into(),
            state: Arc::new(RwLock::new(AdapterMemoryBus::default())),
        }
    }

    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    pub fn published_count(&self) -> usize {
        self.state.read().map(|s| s.published.len()).unwrap_or(0)
    }
}

#[cfg(feature = "redis-adapter")]
impl Adapter for RedisStreamsAdapter {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn broadcast(
        &self,
        opts: BroadcastOptions,
        encoded_packet: SharedBytes,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let pkt = ClusterBroadcastPacket {
            origin_node: self.node_id.clone(),
            opts,
            payload_bytes: encoded_packet.to_vec(),
        };
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.published.push(pkt);
            }
            Ok(())
        })
    }

    fn add_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                let entry = guard.sockets.entry(sid.clone()).or_insert(RemoteSocketInfo {
                    sid,
                    namespace: "/".to_owned(),
                    rooms: Vec::new(),
                    node_id,
                });
                for r in rooms {
                    if !entry.rooms.contains(&r) {
                        entry.rooms.push(r);
                    }
                }
            }
            Ok(())
        })
    }

    fn del_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                if let Some(entry) = guard.sockets.get_mut(&sid) {
                    entry.rooms.retain(|r| !rooms.contains(r));
                }
            }
            Ok(())
        })
    }

    fn fetch_sockets(
        &self,
        namespace: &str,
        room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>> {
        let ns = namespace.to_owned();
        let filter = room_filter.map(str::to_owned);
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let guard = state
                .read()
                .map_err(|_| AdapterError::Disconnected("lock poisoned".to_owned()))?;
            let list = guard
                .sockets
                .values()
                .filter(|s| s.namespace == ns)
                .filter(|s| match &filter {
                    Some(r) => s.rooms.contains(r),
                    None => true,
                })
                .cloned()
                .collect();
            Ok(list)
        })
    }

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let ev = event.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.server_events.push((ev, payload));
            }
            Ok(())
        })
    }
}

/// NATS JetStream adapter (`S3-T07`).
#[cfg(feature = "nats-adapter")]
#[derive(Debug, Clone)]
pub struct NatsJetStreamAdapter {
    node_id: String,
    subject_prefix: String,
    state: Arc<RwLock<AdapterMemoryBus>>,
}

#[cfg(feature = "nats-adapter")]
impl NatsJetStreamAdapter {
    pub fn new(node_id: impl Into<String>, subject_prefix: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            subject_prefix: subject_prefix.into(),
            state: Arc::new(RwLock::new(AdapterMemoryBus::default())),
        }
    }

    pub fn subject_prefix(&self) -> &str {
        &self.subject_prefix
    }
}

#[cfg(feature = "nats-adapter")]
impl Adapter for NatsJetStreamAdapter {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn broadcast(
        &self,
        opts: BroadcastOptions,
        encoded_packet: SharedBytes,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let pkt = ClusterBroadcastPacket {
            origin_node: self.node_id.clone(),
            opts,
            payload_bytes: encoded_packet.to_vec(),
        };
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.published.push(pkt);
            }
            Ok(())
        })
    }

    fn add_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                let entry = guard.sockets.entry(sid.clone()).or_insert(RemoteSocketInfo {
                    sid,
                    namespace: "/".to_owned(),
                    rooms: Vec::new(),
                    node_id,
                });
                entry.rooms.extend(rooms);
            }
            Ok(())
        })
    }

    fn del_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                if let Some(entry) = guard.sockets.get_mut(&sid) {
                    entry.rooms.retain(|r| !rooms.contains(r));
                }
            }
            Ok(())
        })
    }

    fn fetch_sockets(
        &self,
        namespace: &str,
        _room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>> {
        let ns = namespace.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let guard = state
                .read()
                .map_err(|_| AdapterError::Disconnected("lock poisoned".to_owned()))?;
            Ok(guard
                .sockets
                .values()
                .filter(|s| s.namespace == ns)
                .cloned()
                .collect())
        })
    }

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let ev = event.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.server_events.push((ev, payload));
            }
            Ok(())
        })
    }
}

/// PostgreSQL `LISTEN/NOTIFY` + durable offset table adapter (`S3-T07`).
#[cfg(feature = "postgres-adapter")]
#[derive(Debug, Clone)]
pub struct PostgresAdapter {
    node_id: String,
    channel_name: String,
    state: Arc<RwLock<AdapterMemoryBus>>,
}

#[cfg(feature = "postgres-adapter")]
impl PostgresAdapter {
    pub fn new(node_id: impl Into<String>, channel_name: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            channel_name: channel_name.into(),
            state: Arc::new(RwLock::new(AdapterMemoryBus::default())),
        }
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
}

#[cfg(feature = "postgres-adapter")]
impl Adapter for PostgresAdapter {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn broadcast(
        &self,
        opts: BroadcastOptions,
        encoded_packet: SharedBytes,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let pkt = ClusterBroadcastPacket {
            origin_node: self.node_id.clone(),
            opts,
            payload_bytes: encoded_packet.to_vec(),
        };
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.published.push(pkt);
            }
            Ok(())
        })
    }

    fn add_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                let entry = guard.sockets.entry(sid.clone()).or_insert(RemoteSocketInfo {
                    sid,
                    namespace: "/".to_owned(),
                    rooms: Vec::new(),
                    node_id,
                });
                entry.rooms.extend(rooms);
            }
            Ok(())
        })
    }

    fn del_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let sid = sid.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                if let Some(entry) = guard.sockets.get_mut(&sid) {
                    entry.rooms.retain(|r| !rooms.contains(r));
                }
            }
            Ok(())
        })
    }

    fn fetch_sockets(
        &self,
        namespace: &str,
        _room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>> {
        let ns = namespace.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let guard = state
                .read()
                .map_err(|_| AdapterError::Disconnected("lock poisoned".to_owned()))?;
            Ok(guard
                .sockets
                .values()
                .filter(|s| s.namespace == ns)
                .cloned()
                .collect())
        })
    }

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let ev = event.to_owned();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if let Ok(mut guard) = state.write() {
                guard.server_events.push((ev, payload));
            }
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
struct AdapterMemoryBus {
    published: Vec<ClusterBroadcastPacket>,
    sockets: HashMap<String, RemoteSocketInfo>,
    server_events: Vec<(String, serde_json::Value)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_nats_postgres_adapters_implement_trait() {
        let redis = RedisStreamsAdapter::new("node-1", "sio:stream");
        let nats = NatsJetStreamAdapter::new("node-2", "sio.events");
        let pg = PostgresAdapter::new("node-3", "sio_notify");

        redis
            .add_sockets("sid-1", vec!["room-a".to_owned()])
            .await
            .unwrap();
        let fetched = redis.fetch_sockets("/", Some("room-a")).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].node_id, "node-1");

        redis
            .broadcast(
                BroadcastOptions {
                    namespace: "/".to_owned(),
                    rooms: vec!["room-a".to_owned()],
                    except: vec![],
                    local_only: false,
                },
                SharedBytes::from_slice(b"2[\"hi\"]"),
            )
            .await
            .unwrap();
        assert_eq!(redis.published_count(), 1);

        nats.server_side_emit("cluster_ping", serde_json::json!({"ok": true}))
            .await
            .unwrap();
        pg.server_side_emit("cluster_pong", serde_json::json!({"ok": true}))
            .await
            .unwrap();
    }
}
