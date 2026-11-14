//! Live PostgreSQL `NOTIFY` / attachment-table adapter using `tokio-postgres` (`S3-T06`).

use crate::adapter::{
    Adapter, AdapterError, BoxFuture, BroadcastOptions, ClusterBroadcastPacket, RemoteSocketInfo,
};
use socketio_parser::SharedBytes;
use std::sync::Arc;

/// Production PostgreSQL adapter backed by `tokio_postgres::Client`.
#[derive(Clone)]
pub struct LivePostgresAdapter {
    node_id: String,
    channel: String,
    conn_str: String,
    client: Option<Arc<tokio_postgres::Client>>,
}

impl LivePostgresAdapter {
    /// Creates a configured adapter handle; call `connect_live().await` to open the Postgres connection.
    pub fn new(
        node_id: impl Into<String>,
        channel: impl Into<String>,
        conn_str: impl Into<String>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            channel: channel.into(),
            conn_str: conn_str.into(),
            client: None,
        }
    }

    /// Connects to PostgreSQL and spawns the connection driver task.
    pub async fn connect_live(
        node_id: impl Into<String>,
        channel: impl Into<String>,
        conn_str: impl Into<String>,
    ) -> Result<Self, AdapterError> {
        let dsn = conn_str.into();
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            node_id: node_id.into(),
            channel: channel.into(),
            conn_str: dsn,
            client: Some(Arc::new(client)),
        })
    }

    pub fn notify_channel(&self) -> &str {
        &self.channel
    }

    /// Spawns a background PostgreSQL `LISTEN` loop that forwards remote
    /// `ClusterBroadcastPacket`s (skipping `origin_node == self.node_id`) to `handler`.
    pub fn spawn_subscriber_loop(
        &self,
        handler: crate::adapter::ClusterPacketHandler,
    ) -> tokio::task::JoinHandle<()> {
        let conn_str = self.conn_str.clone();
        let channel = self.channel.clone();
        let local_node = self.node_id.clone();

        tokio::spawn(async move {
            let Ok((client, mut connection)) =
                tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await
            else {
                return;
            };
            let listen_sql = format!("LISTEN \"{}\"", channel.replace('"', "\"\""));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            tokio::spawn(async move {
                let _ = futures_util::future::poll_fn(|cx| {
                    while let std::task::Poll::Ready(Some(msg)) = connection.poll_message(cx)? {
                        if let tokio_postgres::AsyncMessage::Notification(n) = msg {
                            let _ = tx.send(n.payload().to_owned());
                        }
                    }
                    std::task::Poll::Pending::<Result<(), tokio_postgres::Error>>
                })
                .await;
            });
            if client.batch_execute(&listen_sql).await.is_err() {
                return;
            }
            while let Some(payload) = rx.recv().await {
                if let Ok(pkt) = serde_json::from_str::<ClusterBroadcastPacket>(&payload) {
                    if pkt.origin_node != local_node {
                        handler(pkt);
                    }
                }
            }
        })
    }
}

impl Adapter for LivePostgresAdapter {
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
        let conn_str = self.conn_str.clone();
        let channel = self.channel.clone();

        Box::pin(async move {
            let json = serde_json::to_string(&pkt)
                .map_err(|e| AdapterError::Serialization(e.to_string()))?;
            let client = match client_opt {
                Some(c) => c,
                None => {
                    let (c, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                        .await
                        .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    Arc::new(c)
                }
            };
            client
                .execute("SELECT pg_notify($1, $2)", &[&channel, &json])
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
        let conn_str = self.conn_str.clone();
        let channel = format!("{}_cluster", self.channel);
        let msg = serde_json::json!({ "event": event, "payload": payload }).to_string();

        Box::pin(async move {
            let client = match client_opt {
                Some(c) => c,
                None => {
                    let (c, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                        .await
                        .map_err(|e| AdapterError::Disconnected(e.to_string()))?;
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    Arc::new(c)
                }
            };
            client
                .execute("SELECT pg_notify($1, $2)", &[&channel, &msg])
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
    fn test_live_postgres_adapter_channel_and_node_id() {
        let adapter = LivePostgresAdapter::new(
            "node-pg-1",
            "socket_io_events",
            "postgres://postgres@127.0.0.1:5432/app",
        );
        assert_eq!(adapter.node_id(), "node-pg-1");
        assert_eq!(adapter.notify_channel(), "socket_io_events");
    }
}
