//! Native Presence tracking & offline APNs/FCM push fallback hook (`S7-T04`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceMember {
    pub sid: String,
    pub user_id: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceDiff {
    pub room: String,
    pub joins: Vec<PresenceMember>,
    pub leaves: Vec<String>,
}

/// Callback invoked when a targeted room/user broadcast finds 0 active sockets,
/// enabling seamless APNs/FCM mobile push fallback without draining mobile battery.
pub type PushFallbackHook = Arc<dyn Fn(&str, &str, &serde_json::Value) + Send + Sync>;

/// Thread-safe room presence registry.
#[derive(Debug, Default)]
pub struct PresenceRegistry {
    rooms: RwLock<HashMap<String, HashMap<String, PresenceMember>>>,
}

impl PresenceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn track(&self, room: &str, member: PresenceMember) -> PresenceDiff {
        if let Ok(mut guard) = self.rooms.write() {
            guard
                .entry(room.to_owned())
                .or_default()
                .insert(member.sid.clone(), member.clone());
        }
        PresenceDiff {
            room: room.to_owned(),
            joins: vec![member],
            leaves: vec![],
        }
    }

    pub fn untrack(&self, room: &str, sid: &str) -> Option<PresenceDiff> {
        let mut guard = self.rooms.write().ok()?;
        let empty = {
            let map = guard.get_mut(room)?;
            map.remove(sid)?;
            map.is_empty()
        };
        if empty {
            guard.remove(room);
        }
        Some(PresenceDiff {
            room: room.to_owned(),
            joins: vec![],
            leaves: vec![sid.to_owned()],
        })
    }

    pub fn untrack_socket_all(&self, sid: &str) -> Vec<PresenceDiff> {
        let mut diffs = Vec::new();
        if let Ok(mut guard) = self.rooms.write() {
            guard.retain(|room, members| {
                if members.remove(sid).is_some() {
                    diffs.push(PresenceDiff {
                        room: room.clone(),
                        joins: vec![],
                        leaves: vec![sid.to_owned()],
                    });
                }
                !members.is_empty()
            });
        }
        diffs
    }

    pub fn list(&self, room: &str) -> Vec<PresenceMember> {
        self.rooms
            .read()
            .ok()
            .and_then(|g| g.get(room).map(|m| m.values().cloned().collect()))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_presence_track_untrack_and_cleanup() {
        let reg = PresenceRegistry::new();
        let diff = reg.track(
            "doc:101",
            PresenceMember {
                sid: "sid-1".to_owned(),
                user_id: "user-alice".to_owned(),
                metadata: serde_json::json!({"cursor": [10, 20]}),
            },
        );
        assert_eq!(diff.joins.len(), 1);
        assert_eq!(reg.list("doc:101").len(), 1);

        let leaves = reg.untrack_socket_all("sid-1");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].leaves, vec!["sid-1"]);
        assert!(reg.list("doc:101").is_empty());
    }
}
