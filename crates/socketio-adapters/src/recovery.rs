//! Durable Connection State Recovery (CSR) session snapshot & `pid` + `offset` manager (`S3-T03`, `S3-T04`).

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

/// Result returned when a client attempts Connection State Recovery (`auth: { pid, offset }`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub recovered: bool,
    pub sid: String,
    pub rooms: Vec<String>,
    pub missed_frames: Vec<SharedBytes>,
}

/// Coordinates `pid` -> `SessionRecoverySnapshot` and `CsrReplayLog` replay (`S3-T03`, `S3-T04`).
#[derive(Debug, Clone)]
pub struct RecoveryManager {
    replay_log: Arc<RwLock<CsrReplayLog>>,
    snapshots: Arc<RwLock<HashMap<String, SessionRecoverySnapshot>>>,
    max_disconnection_duration_ms: u64,
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
        }
    }

    /// Records an outbound room broadcast and returns its monotonic stream `offset`.
    pub fn record_outbound(
        &self,
        room: &str,
        encoded_frame: SharedBytes,
        now_ms: u64,
    ) -> u64 {
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

    /// Attempts to restore a session from `(pid, since_offset)` during Socket.IO handshake.
    pub fn try_recover(&self, pid: &str, since_offset: u64, now_ms: u64) -> Option<RecoveryOutcome> {
        let snapshot = {
            let mut map = self.snapshots.write().ok()?;
            map.remove(pid)?
        };

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

    #[test]
    fn test_recovery_manager_restores_sid_rooms_and_missed_events() {
        let mgr = RecoveryManager::new(256, 120_000);
        let o1 = mgr.record_outbound("chat:42", SharedBytes::from_slice(b"2[\"msg\",1]"), 1_000);

        // Client disconnects at offset o1, then two broadcasts happen while offline
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
}
