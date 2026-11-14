//! Live NATS Core / JetStream network adapter using `async-nats` (`S3-T06`).

use crate::adapter::{
    Adapter, AdapterError, BoxFuture, BroadcastOptions, ClusterBroadcastPacket, RemoteSocketInfo,
};
use bytes::Bytes;
use socketio_parser::SharedBytes;

/// Production NATS JetStream adapter backed by `async_nats::Client`.
#[derive(Clone)]
pub struct LiveNatsJetStreamAdapter {
    node_id: String,
    subject_prefix: String,
    nats_url: String,
    client: Option<async_nats::Client>,
}

impl LiveNatsJetStreamAdapter {
    /// Creates a configured adapter handle; call `connect_live().await` to establish the TCP/TLS link.
    pub fn new(
        node_id: impl Into<String>,
        subject_prefix: impl Into<String>,
        nats_url: impl Into<String>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            subject_prefix: subject_prefix.into(),
            nats_url: nats_url.into(),
            client: None,
        }
    }

    /// Connects to the NATS server / cluster.
    pub async fn connect_live(
        node_id: impl Into<String>,
        subject_prefix: impl Into<String>,
        nats_url: impl Into<String>,
    ) -> Result<Self, AdapterError> {
        let url = nats_url.into();
        let client = async_nats::connect(&url)
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        Ok(Self {
            node_id: node_id.into(),
            subject_prefix: subject_prefix.into(),
            nats_url: url,
            client: Some(client),
        })
    }

    pub fn broadcast_subject(&self) -> String {
        format!("{}.broadcast", self.subject_prefix)
    }

    pub fn cluster_subject(&self) -> String {
        format!("{}.cluster", self.subject_prefix)
    }

    /// Spawns a background NATS subscriber loop that forwards remote
    /// `ClusterBroadcastPacket`s (skipping `origin_node == self.node_id`) to `handler`.
    pub fn spawn_subscriber_loop(
        &self,
        handler: crate::adapter::ClusterPacketHandler,
    ) -> tokio::task::JoinHandle<()> {
        let client_opt = self.client.clone();
        let nats_url = self.nats_url.clone();
        let subject = self.broadcast_subject();
        let local_node = self.node_id.clone();

        tokio::spawn(async move {
            use futures_util::StreamExt;
            let client = match client_opt {
                Some(c) => c,
                None => match async_nats::connect(&nats_url).await {
                    Ok(c) => c,
                    Err(_) => return,
                },
            };
            let Ok(mut sub) = client.subscribe(subject).await else {
                return;
            };
            while let Some(msg) = sub.next().await {
                if let Ok(pkt) = serde_json::from_slice::<ClusterBroadcastPacket>(&msg.payload) {
                    if pkt.origin_node != local_node {
                        handler(pkt);
                    }
                }
            }
        })
    }
}

impl Adapter for LiveNatsJetStreamAdapter {
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
        let client_opt = self.client.clone();
        let nats_url = self.nats_url.clone();
        let subject = self.broadcast_subject();

        Box::pin(async move {
            let payload =
                serde_json::to_vec(&pkt).map_err(|e| AdapterError::Serialization(e.to_string()))?;
            let client = match client_opt {
                Some(c) => c,
                None => async_nats::connect(&nats_url)
                    .await
                    .map_err(|e| AdapterError::Disconnected(e.to_string()))?,
            };
            let js = async_nats::jetstream::new(client);
            let ack_fut = js
                .publish(subject, Bytes::from(payload))
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            let _ = ack_fut
                .await
                .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
            Ok(())
        })
    }

    fn add_sockets(
        &self,
        _sid: &str,
        _rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn del_sockets(
        &self,
        _sid: &str,
        _rooms: Vec<String>,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn fetch_sockets(
        &self,
        _namespace: &str,
        _room_filter: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<RemoteSocketInfo>, AdapterError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn server_side_emit(
        &self,
        event: &str,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<(), AdapterError>> {
        let client_opt = self.client.clone();
        let nats_url = self.nats_url.clone();
        let subject = self.cluster_subject();
        let msg = serde_json::json!({ "event": event, "payload": payload }).to_string();

        Box::pin(async move {
            let client = match client_opt {
                Some(c) => c,
                None => async_nats::connect(&nats_url)
                    .await
                    .map_err(|e| AdapterError::Disconnected(e.to_string()))?,
            };
            client
                .publish(subject, Bytes::from(msg))
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
    fn test_live_nats_adapter_subjects_and_node_id() {
        let adapter =
            LiveNatsJetStreamAdapter::new("node-nats-1", "sio.prod", "nats://127.0.0.1:4222");
        assert_eq!(adapter.node_id(), "node-nats-1");
        assert_eq!(adapter.broadcast_subject(), "sio.prod.broadcast");
        assert_eq!(adapter.cluster_subject(), "sio.prod.cluster");
    }
}
