//! Live Redis / Valkey Streams network adapter using `redis` crate (`XADD` + `PUBLISH`) (`S3-T06`).

use crate::adapter::{
    Adapter, AdapterError, BoxFuture, BroadcastOptions, ClusterBroadcastPacket, RemoteSocketInfo,
};
use crate::recovery::{CsrSnapshotStore, SessionRecoverySnapshot};
use redis::AsyncCommands;
use socketio_parser::SharedBytes;

/// Production Redis / Valkey Streams adapter backed by a real `redis::Client`.
#[derive(Clone)]
pub struct LiveRedisStreamsAdapter {
    node_id: String,
    stream_key: String,
    client: redis::Client,
}

impl LiveRedisStreamsAdapter {
    pub fn connect(
        node_id: impl Into<String>,
        stream_key: impl Into<String>,
        redis_url: &str,
    ) -> Result<Self, AdapterError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        Ok(Self {
            node_id: node_id.into(),
            stream_key: stream_key.into(),
            client,
        })
    }

    pub fn csr_snapshot_key(&self, pid: &str) -> String {
        format!("{}:csr:{pid}", self.stream_key)
    }

    /// Persists a `SessionRecoverySnapshot` in Redis with TTL matching `max_disconnection_duration_ms`.
    pub async fn save_csr_snapshot(
        &self,
        snapshot: &SessionRecoverySnapshot,
        ttl_secs: u64,
    ) -> Result<(), AdapterError> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;
        let key = self.csr_snapshot_key(&snapshot.pid);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        let _: () = conn
            .set_ex(key, json, ttl_secs.max(1))
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        Ok(())
    }

    /// Atomically fetches and deletes a persisted `SessionRecoverySnapshot` from Redis.
    pub async fn load_csr_snapshot(
        &self,
        pid: &str,
    ) -> Result<Option<SessionRecoverySnapshot>, AdapterError> {
        let key = self.csr_snapshot_key(pid);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        let raw: Option<String> = conn
            .get_del(key)
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        match raw {
            Some(s) => serde_json::from_str(&s)
                .map(Some)
                .map_err(|e| AdapterError::Serialization(e.to_string())),
            None => Ok(None),
        }
    }

    /// Spawns a background `XREAD BLOCK 1000` consumer loop that forwards remote
    /// `ClusterBroadcastPacket`s (skipping `origin_node == self.node_id`) to `handler`.
    pub fn spawn_subscriber_loop(
        &self,
        handler: crate::adapter::ClusterPacketHandler,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let stream_key = self.stream_key.clone();
        let local_node = self.node_id.clone();

        tokio::spawn(async move {
            let mut last_id = "$".to_owned();
            let opts = redis::streams::StreamReadOptions::default()
                .block(1000)
                .count(64);
            loop {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                };
                let reply: redis::RedisResult<redis::streams::StreamReadReply> =
                    conn.xread_options(&[&stream_key], &[&last_id], &opts).await;
                if let Ok(stream_reply) = reply {
                    for key in stream_reply.keys {
                        for entry in key.ids {
                            last_id = entry.id.clone();
                            if let Some(redis::Value::BulkString(raw)) = entry.map.get("packet") {
                                if let Ok(pkt) =
                                    serde_json::from_slice::<ClusterBroadcastPacket>(raw)
                                {
                                    if pkt.origin_node != local_node {
                                        handler(pkt);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

impl Adapter for LiveRedisStreamsAdapter {
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
            binary_attachments: Vec::new(),
        };
        let client = self.client.clone();
        let stream_key = self.stream_key.clone();

        Box::pin(async move {
            let json = serde_json::to_string(&pkt)
                .map_err(|e| AdapterError::Serialization(e.to_string()))?;
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            let _: String = conn
                .xadd(&stream_key, "*", &[("packet", json)])
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            Ok(())
        })
    }

    fn add_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let client = self.client.clone();
        let key = format!("{}:socket:{sid}:rooms", self.stream_key);
        Box::pin(async move {
            if rooms.is_empty() {
                return Ok(());
            }
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            let _: usize = conn
                .sadd(&key, rooms)
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            Ok(())
        })
    }

    fn del_sockets(
        &self,
        sid: &str,
        rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let client = self.client.clone();
        let key = format!("{}:socket:{sid}:rooms", self.stream_key);
        Box::pin(async move {
            if rooms.is_empty() {
                return Ok(());
            }
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            let _: usize = conn
                .srem(&key, rooms)
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            Ok(())
        })
    }

    fn fetch_sockets(
        &self,
        _namespace: &str,
        _room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let client = self.client.clone();
        let channel = format!("{}:cluster", self.stream_key);
        let msg = serde_json::json!({ "event": event, "payload": payload }).to_string();
        Box::pin(async move {
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            let _: usize = conn
                .publish(&channel, msg)
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            Ok(())
        })
    }
}

impl CsrSnapshotStore for LiveRedisStreamsAdapter {
    fn save_snapshot_async(
        &self,
        snapshot: SessionRecoverySnapshot,
        ttl_secs: u64,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move { self.save_csr_snapshot(&snapshot, ttl_secs).await })
    }

    fn load_snapshot_async(
        &self,
        pid: &str,
    ) -> BoxFuture<'_, Result<Option<SessionRecoverySnapshot>, AdapterError>> {
        let pid = pid.to_owned();
        Box::pin(async move { self.load_csr_snapshot(&pid).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_redis_adapter_builds_client_handle() {
        let adapter =
            LiveRedisStreamsAdapter::connect("node-prod-1", "sio:stream", "redis://127.0.0.1:6379")
                .expect("valid redis URI");
        assert_eq!(adapter.node_id(), "node-prod-1");
        assert_eq!(
            adapter.csr_snapshot_key("pid-123"),
            "sio:stream:csr:pid-123"
        );
    }
}
