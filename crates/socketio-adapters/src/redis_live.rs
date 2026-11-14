//! Live Redis / Valkey Streams network adapter using `redis` crate (`XADD` + `PUBLISH`) (`S3-T06`).

use crate::adapter::{
    Adapter, AdapterError, BoxFuture, BroadcastOptions, ClusterBroadcastPacket, RemoteSocketInfo,
};
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_redis_adapter_builds_client_handle() {
        let adapter = LiveRedisStreamsAdapter::connect(
            "node-prod-1",
            "sio:stream",
            "redis://127.0.0.1:6379",
        )
        .expect("valid redis URI");
        assert_eq!(adapter.node_id(), "node-prod-1");
    }
}
