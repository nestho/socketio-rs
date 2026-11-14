//! Bounded `emitWithAck` and `timeout(ms)` correlation table (`S2-T08`).

use socketio_parser::SharedBytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckError {
    Timeout,
    CapacityExceeded,
    ChannelClosed,
}

/// Bounded pending-acknowledgement correlation map preventing unbounded memory growth
/// if clients disconnect or ignore `emitWithAck` requests.
#[derive(Debug, Clone)]
pub struct AckTable {
    next_id: Arc<AtomicU64>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<SharedBytes>>>>,
    max_pending: usize,
}

impl AckTable {
    pub fn new(max_pending: usize) -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            max_pending: max_pending.max(1),
        }
    }

    /// Registers a pending acknowledgement and returns `(ack_id, receiver)`.
    pub fn register(&self) -> Result<(u64, oneshot::Receiver<SharedBytes>), AckError> {
        let mut guard = self.pending.lock().map_err(|_| AckError::ChannelClosed)?;
        if guard.len() >= self.max_pending {
            return Err(AckError::CapacityExceeded);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        guard.insert(id, tx);
        Ok((id, rx))
    }

    /// Resolves a pending acknowledgement when an inbound `PacketType::Ack` arrives.
    pub fn resolve(&self, ack_id: u64, payload: SharedBytes) -> bool {
        if let Ok(mut guard) = self.pending.lock() {
            if let Some(tx) = guard.remove(&ack_id) {
                return tx.send(payload).is_ok();
            }
        }
        false
    }

    /// Awaits an acknowledgement with a strict timeout, automatically evicting expired entries.
    pub async fn wait_with_timeout(
        &self,
        ack_id: u64,
        rx: oneshot::Receiver<SharedBytes>,
        timeout: Duration,
    ) -> Result<SharedBytes, AckError> {
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(payload)) => Ok(payload),
            Ok(Err(_)) => Err(AckError::ChannelClosed),
            Err(_) => {
                if let Ok(mut guard) = self.pending.lock() {
                    guard.remove(&ack_id);
                }
                Err(AckError::Timeout)
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().map(|g| g.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ack_table_resolve_and_timeout_cleanup() {
        let table = AckTable::new(16);
        let (id1, rx1) = table.register().expect("register 1");
        assert!(table.resolve(id1, SharedBytes::from_slice(b"[\"ok\"]")));
        let res = table
            .wait_with_timeout(id1, rx1, Duration::from_millis(100))
            .await
            .expect("resolved");
        assert_eq!(res.as_slice(), b"[\"ok\"]");

        let (id2, rx2) = table.register().expect("register 2");
        let err = table
            .wait_with_timeout(id2, rx2, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(err, AckError::Timeout);
        assert_eq!(table.pending_count(), 0);
    }
}
