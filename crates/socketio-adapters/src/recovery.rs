//! Durable Connection State Recovery (CSR) session snapshot & `pid` + `offset` manager (`S3-T03`, `S3-T04`).

use crate::adapter::{AdapterError, BoxFuture};
use crate::CsrReplayLog;
use serde::{Deserialize, Serialize};
use socketio_parser::SharedBytes;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Snapshot of a disconnected client's recoverable session state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecoverySnapshot {
    pub pid: String,
    pub sid: String,
    pub namespace: String,
    pub rooms: Vec<String>,
    pub last_offset: u64,
    pub disconnected_at_ms: u64,
}

/// Pluggable external persistence backend for CSR session snapshots (e.g. `LiveRedisStreamsAdapter`).
pub trait CsrSnapshotStore: Send + Sync {
    fn save_snapshot_async(
        &self,
        snapshot: SessionRecoverySnapshot,
        ttl_secs: u64,
    ) -> BoxFuture<'_, Result<(), AdapterError>>;

    fn load_snapshot_async(
        &self,
        pid: &str,
    ) -> BoxFuture<'_, Result<Option<SessionRecoverySnapshot>, AdapterError>>;
}

/// Result returned when a client attempts Connection State Recovery (`auth: { pid, offset }`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub recovered: bool,
    pub sid: String,
    pub rooms: Vec<String>,
    pub missed_frames: Vec<SharedBytes>,
}

/// Coordinates `pid` -> `SessionRecoverySnapshot` and `CsrReplayLog` replay (`S3-T03`, `S3-T04`).
#[derive(Clone)]
pub struct RecoveryManager {
    replay_log: Arc<RwLock<CsrReplayLog>>,
    snapshots: Arc<RwLock<HashMap<String, SessionRecoverySnapshot>>>,
    max_disconnection_duration_ms: u64,
    external_store: Option<Arc<dyn CsrSnapshotStore>>,
}

impl std::fmt::Debug for RecoveryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryManager")
            .field(
                "max_disconnection_duration_ms",
                &self.max_disconnection_duration_ms,
            )
            .field("has_external_store", &self.external_store.is_some())
            .finish()
    }
}

impl RecoveryManager {
    pub fn new(ring_capacity: usize, max_disconnection_duration_ms: u64) -> Self {
        Self {
            replay_log: Arc::new(RwLock::new(CsrReplayLog::new(
                ring_capacity,
                max_disconnection_duration_ms,
            ))),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            max_disconnection_duration_ms,
            external_store: None,
        }
    }

    pub fn with_store(
        ring_capacity: usize,
        max_disconnection_duration_ms: u64,
        store: Arc<dyn CsrSnapshotStore>,
    ) -> Self {
        Self {
            replay_log: Arc::new(RwLock::new(CsrReplayLog::new(
                ring_capacity,
                max_disconnection_duration_ms,
            ))),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            max_disconnection_duration_ms,
            external_store: Some(store),
        }
    }

    /// Records an outbound room broadcast and returns its monotonic stream `offset`.
    pub fn record_outbound(&self, room: &str, encoded_frame: SharedBytes, now_ms: u64) -> u64 {
        if let Ok(mut log) = self.replay_log.write() {
            return log.record_broadcast(room, encoded_frame, now_ms);
        }
        0
    }

    /// Saves a session's `pid`, `sid`, `rooms`, and `last_offset` upon transport disconnect.
    pub fn save_snapshot(&self, snapshot: SessionRecoverySnapshot) {
        if let Ok(mut map) = self.snapshots.write() {
            map.insert(snapshot.pid.clone(), snapshot);
        }
    }

    /// Saves a snapshot to the external `CsrSnapshotStore` (if configured) as well as local cache.
    pub async fn save_snapshot_durable(
        &self,
        snapshot: SessionRecoverySnapshot,
    ) -> Result<(), AdapterError> {
        self.save_snapshot(snapshot.clone());
        if let Some(ref store) = self.external_store {
            let ttl_secs = (self.max_disconnection_duration_ms / 1000).max(1);
            store.save_snapshot_async(snapshot, ttl_secs).await?;
        }
        Ok(())
    }

    /// Attempts to restore a session from `(pid, since_offset)` during Socket.IO handshake.
    pub fn try_recover(
        &self,
        pid: &str,
        since_offset: u64,
        now_ms: u64,
    ) -> Option<RecoveryOutcome> {
        let snapshot = {
            let mut map = self.snapshots.write().ok()?;
            map.remove(pid)?
        };

        self.recover_from_snapshot(snapshot, since_offset, now_ms)
    }

    /// Attempts durable recovery, checking local cache first and falling back to `CsrSnapshotStore`.
    pub async fn try_recover_durable(
        &self,
        pid: &str,
        since_offset: u64,
        now_ms: u64,
    ) -> Option<RecoveryOutcome> {
        if let Some(outcome) = self.try_recover(pid, since_offset, now_ms) {
            return Some(outcome);
        }
        let store = self.external_store.as_ref()?;
        let snapshot = store.load_snapshot_async(pid).await.ok()??;
        self.recover_from_snapshot(snapshot, since_offset, now_ms)
    }

    fn recover_from_snapshot(
        &self,
        snapshot: SessionRecoverySnapshot,
        since_offset: u64,
        now_ms: u64,
    ) -> Option<RecoveryOutcome> {
        if now_ms.saturating_sub(snapshot.disconnected_at_ms) > self.max_disconnection_duration_ms {
            return None;
        }

        let missed = {
            let log = self.replay_log.read().ok()?;
            log.recover_since(since_offset, &snapshot.rooms, now_ms)?
        };

        Some(RecoveryOutcome {
            recovered: true,
            sid: snapshot.sid,
            rooms: snapshot.rooms,
            missed_frames: missed.into_iter().map(|e| e.encoded_frame).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExternalStore {
        saved: RwLock<HashMap<String, SessionRecoverySnapshot>>,
    }

    impl CsrSnapshotStore for MockExternalStore {
        fn save_snapshot_async(
            &self,
            snapshot: SessionRecoverySnapshot,
            _ttl_secs: u64,
        ) -> BoxFuture<'_, Result<(), AdapterError>> {
            if let Ok(mut g) = self.saved.write() {
                g.insert(snapshot.pid.clone(), snapshot);
            }
            Box::pin(async { Ok(()) })
        }

        fn load_snapshot_async(
            &self,
            pid: &str,
        ) -> BoxFuture<'_, Result<Option<SessionRecoverySnapshot>, AdapterError>> {
            let res = self.saved.write().ok().and_then(|mut g| g.remove(pid));
            Box::pin(async move { Ok(res) })
        }
    }

    #[test]
    fn test_recovery_manager_restores_sid_rooms_and_missed_events() {
        let mgr = RecoveryManager::new(256, 120_000);
        let o1 = mgr.record_outbound("chat:42", SharedBytes::from_slice(b"2[\"msg\",1]"), 1_000);

        mgr.save_snapshot(SessionRecoverySnapshot {
            pid: "pid-secret-001".to_owned(),
            sid: "sid-original-001".to_owned(),
            namespace: "/".to_owned(),
            rooms: vec!["chat:42".to_owned()],
            last_offset: o1,
            disconnected_at_ms: 1_500,
        });

        let _o2 = mgr.record_outbound("chat:42", SharedBytes::from_slice(b"2[\"msg\",2]"), 2_000);
        let _o3 = mgr.record_outbound("chat:42", SharedBytes::from_slice(b"2[\"msg\",3]"), 2_500);

        let outcome = mgr
            .try_recover("pid-secret-001", o1, 3_000)
            .expect("should recover within window");

        assert!(outcome.recovered);
        assert_eq!(outcome.sid, "sid-original-001");
        assert_eq!(outcome.rooms, vec!["chat:42"]);
        assert_eq!(outcome.missed_frames.len(), 2);
        assert_eq!(outcome.missed_frames[0].as_slice(), b"2[\"msg\",2]");
        assert_eq!(outcome.missed_frames[1].as_slice(), b"2[\"msg\",3]");
    }

    #[tokio::test]
    async fn test_durable_recovery_falls_back_to_external_store() {
        let store = Arc::new(MockExternalStore {
            saved: RwLock::new(HashMap::new()),
        });
        let mgr = RecoveryManager::with_store(128, 120_000, store.clone());
        let o1 = mgr.record_outbound("room:1", SharedBytes::from_slice(b"2[\"a\",1]"), 1_000);

        // Save directly into external store (simulating another gateway node)
        store
            .save_snapshot_async(
                SessionRecoverySnapshot {
                    pid: "pid-cluster-9".to_owned(),
                    sid: "sid-cluster-9".to_owned(),
                    namespace: "/".to_owned(),
                    rooms: vec!["room:1".to_owned()],
                    last_offset: o1,
                    disconnected_at_ms: 1_200,
                },
                120,
            )
            .await
            .unwrap();

        let _o2 = mgr.record_outbound("room:1", SharedBytes::from_slice(b"2[\"b\",2]"), 2_000);

        let outcome = mgr
            .try_recover_durable("pid-cluster-9", o1, 2_500)
            .await
            .expect("recovered from external store");
        assert_eq!(outcome.sid, "sid-cluster-9");
        assert_eq!(outcome.missed_frames.len(), 1);
    }
}
