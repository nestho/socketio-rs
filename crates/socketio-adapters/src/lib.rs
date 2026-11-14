#![forbid(unsafe_code)]
//! `socketio-adapters` implements Connection State Recovery (CSR) via monotonic
//! stream offsets (`CsrReplayLog`, `RecoveryManager`) and pluggable horizontal
//! scaling adapters (Redis/Valkey Streams, NATS JetStream, PostgreSQL).

pub mod adapter;
#[cfg(feature = "nats-adapter")]
pub mod nats_live;
#[cfg(feature = "postgres-adapter")]
pub mod postgres_live;
pub mod recovery;
#[cfg(feature = "redis-adapter")]
pub mod redis_live;

pub use adapter::{
    Adapter, AdapterError, BroadcastOptions, ClusterBroadcastPacket, RemoteSocketInfo,
};
#[cfg(feature = "nats-adapter")]
pub use adapter::NatsJetStreamAdapter;
#[cfg(feature = "postgres-adapter")]
pub use adapter::PostgresAdapter;
#[cfg(feature = "redis-adapter")]
pub use adapter::RedisStreamsAdapter;
#[cfg(feature = "nats-adapter")]
pub use nats_live::LiveNatsJetStreamAdapter;
#[cfg(feature = "postgres-adapter")]
pub use postgres_live::LivePostgresAdapter;
#[cfg(feature = "redis-adapter")]
pub use redis_live::LiveRedisStreamsAdapter;
pub use recovery::{
    CsrSnapshotStore, RecoveryManager, RecoveryOutcome, SessionRecoverySnapshot,
};

use socketio_parser::SharedBytes;
use std::collections::VecDeque;

/// A sequenced frame stored in the Connection State Recovery (CSR) log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEntry {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub room: String,
    pub encoded_frame: SharedBytes,
}

/// Bounded ring-buffer CSR store for replaying missed broadcasts when a client
/// reconnects with a valid `(pid, last_offset)` within `max_disconnection_duration_ms`.
#[derive(Debug, Clone)]
pub struct CsrReplayLog {
    capacity: usize,
    max_disconnection_duration_ms: u64,
    next_offset: u64,
    entries: VecDeque<ReplayEntry>,
}

impl CsrReplayLog {
    pub fn new(capacity: usize, max_disconnection_duration_ms: u64) -> Self {
        Self {
            capacity: capacity.max(1),
            max_disconnection_duration_ms,
            next_offset: 1,
            entries: VecDeque::with_capacity(capacity.max(1)),
        }
    }

    /// Appends a zero-copy `SharedBytes` broadcast frame and returns its assigned monotonic offset.
    pub fn record_broadcast(
        &mut self,
        room: impl Into<String>,
        encoded_frame: SharedBytes,
        now_ms: u64,
    ) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;

        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }

        self.entries.push_back(ReplayEntry {
            offset,
            timestamp_ms: now_ms,
            room: room.into(),
            encoded_frame,
        });

        offset
    }

    /// Recovers all frames strictly after `since_offset` for `subscribed_rooms` if the window has not expired.
    pub fn recover_since(
        &self,
        since_offset: u64,
        subscribed_rooms: &[String],
        now_ms: u64,
    ) -> Option<Vec<ReplayEntry>> {
        let first = self.entries.front()?;
        if since_offset + 1 < first.offset {
            return None; // Requested offset already evicted from ring buffer
        }

        let mut recovered = Vec::new();
        for entry in &self.entries {
            if entry.offset > since_offset {
                if now_ms.saturating_sub(entry.timestamp_ms) > self.max_disconnection_duration_ms {
                    return None; // Disconnection exceeded max_disconnection_duration_ms
                }
                if subscribed_rooms.iter().any(|r| r == &entry.room) {
                    recovered.push(entry.clone());
                }
            }
        }
        Some(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_replay_log_recovers_missed_wifi_jitter_frames() {
        let mut log = CsrReplayLog::new(128, 120_000);
        let o1 = log.record_broadcast("room:general", SharedBytes::from_slice(b"frame1"), 1_000);
        let _o2 = log.record_broadcast("room:general", SharedBytes::from_slice(b"frame2"), 2_000);
        let _o3 = log.record_broadcast("room:other", SharedBytes::from_slice(b"frame3"), 2_500);

        let rooms = vec!["room:general".to_owned()];
        let missed = log
            .recover_since(o1, &rooms, 3_000)
            .expect("recovery should succeed");
        assert_eq!(missed.len(), 1);
        assert_eq!(missed[0].encoded_frame, SharedBytes::from_slice(b"frame2"));
    }
}
