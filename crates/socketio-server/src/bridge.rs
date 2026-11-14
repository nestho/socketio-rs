//! Live Engine.IO v4 -> Socket.IO v5 session bridge (`SocketIoEngineBridge`)
//! with integrated `pid` + `offset` Connection State Recovery (CSR) and
//! live `BinaryAttachmentReconstructor` (`5...` / `6...` binary frames).

use crate::ack::AckTable;
use crate::extract::EventContext;
use crate::{SocketIo, SocketRef};
use bytes::Bytes;
use engineio_core::EngineHandler;
use serde::Deserialize;
use socketio_adapters::{RecoveryManager, SessionRecoverySnapshot};
use socketio_parser::{
    decode_v5_packet, parse_dynamic_event, BinaryAttachmentReconstructor, PacketType, SharedBytes,
    SocketIoPacket,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub type EventCallback = Arc<dyn Fn(SocketRef, EventContext) + Send + Sync>;

#[derive(Debug, Deserialize)]
struct ConnectAuthPayload {
    pid: Option<String>,
    offset: Option<String>,
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Bridges `engineio-core` transport sessions to `socketio-server` namespaces,
/// rooms, event callbacks, acknowledgement correlation, binary attachments, and CSR.
pub struct SocketIoEngineBridge {
    io: Arc<SocketIo>,
    acks: Arc<AckTable>,
    recovery: Option<Arc<RecoveryManager>>,
    sessions: RwLock<HashMap<String, ActiveSession>>,
}

struct ActiveSession {
    pid: String,
    raw_outbound: mpsc::Sender<String>,
    socket_outbound: mpsc::Sender<SharedBytes>,
    sockets_by_ns: HashMap<String, SocketRef>,
    event_handlers: Arc<RwLock<HashMap<String, EventCallback>>>,
    bin_reconstructor: Mutex<BinaryAttachmentReconstructor>,
}

impl SocketIoEngineBridge {
    pub fn new(io: Arc<SocketIo>) -> Arc<Self> {
        Arc::new(Self {
            io,
            acks: Arc::new(AckTable::new(4096)),
            recovery: None,
            sessions: RwLock::new(HashMap::new()),
        })
    }

    pub fn with_recovery(io: Arc<SocketIo>, recovery: Arc<RecoveryManager>) -> Arc<Self> {
        Arc::new(Self {
            io,
            acks: Arc::new(AckTable::new(4096)),
            recovery: Some(recovery),
            sessions: RwLock::new(HashMap::new()),
        })
    }

    /// Registers an event callback (`socket.on(event, ...)`) for a connected session.
    pub fn register_event_handler(&self, sid: &str, event: &str, cb: EventCallback) {
        if let Ok(guard) = self.sessions.read() {
            if let Some(sess) = guard.get(sid) {
                if let Ok(mut handlers) = sess.event_handlers.write() {
                    handlers.insert(event.to_owned(), cb);
                }
            }
        }
    }

    fn dispatch_event(&self, sid: &str, pkt: SocketIoPacket, binaries: Vec<SharedBytes>) {
        let Ok((event_name, _data)) = parse_dynamic_event(pkt.payload.as_slice()) else {
            return;
        };
        let (sock_opt, cb_opt, sock_tx_opt) = {
            let Ok(guard) = self.sessions.read() else {
                return;
            };
            let Some(sess) = guard.get(sid) else {
                return;
            };
            let sock = sess.sockets_by_ns.get(&pkt.namespace).cloned();
            let cb = sess
                .event_handlers
                .read()
                .ok()
                .and_then(|h| h.get(&event_name).cloned());
            (sock, cb, Some(sess.socket_outbound.clone()))
        };

        if let (Some(sock), Some(cb), Some(outbound)) = (sock_opt, cb_opt, sock_tx_opt) {
            let ctx = EventContext {
                namespace: pkt.namespace,
                ack_id: pkt.ack_id,
                payload: pkt.payload,
                binaries,
                outbound,
            };
            cb(sock, ctx);
        }
    }
}

impl EngineHandler for SocketIoEngineBridge {
    fn on_open(&self, sid: &str, raw_outbound: mpsc::Sender<String>) {
        let (sock_tx, mut sock_rx) = mpsc::channel::<SharedBytes>(256);
        let raw_tx_clone = raw_outbound.clone();

        tokio::spawn(async move {
            while let Some(pkt) = sock_rx.recv().await {
                if let Ok(s) = std::str::from_utf8(pkt.as_slice()) {
                    if raw_tx_clone.send(format!("4{s}")).await.is_err() {
                        break;
                    }
                }
            }
        });

        let session = ActiveSession {
            pid: format!("pid-{sid}"),
            raw_outbound,
            socket_outbound: sock_tx,
            sockets_by_ns: HashMap::new(),
            event_handlers: Arc::new(RwLock::new(HashMap::new())),
            bin_reconstructor: Mutex::new(BinaryAttachmentReconstructor::default()),
        };

        if let Ok(mut guard) = self.sessions.write() {
            guard.insert(sid.to_owned(), session);
        }
    }

    fn on_message(&self, sid: &str, payload: &str) {
        let Ok(pkt) = decode_v5_packet(SharedBytes::from_slice(payload.as_bytes())) else {
            return;
        };

        match pkt.packet_type {
            PacketType::Connect => {
                let ns = pkt.namespace.clone();
                let auth_req =
                    serde_json::from_slice::<ConnectAuthPayload>(pkt.payload.as_slice()).ok();

                let recovered_outcome = auth_req
                    .as_ref()
                    .and_then(|a| a.pid.as_deref().zip(a.offset.as_deref()))
                    .and_then(|(pid, off_str)| {
                        let off = off_str.parse::<u64>().ok()?;
                        self.recovery.as_ref()?.try_recover(pid, off, epoch_ms())
                    });

                let (socket_ref, raw_out, active_pid) = {
                    let Ok(mut guard) = self.sessions.write() else {
                        return;
                    };
                    let Some(sess) = guard.get_mut(sid) else {
                        return;
                    };
                    if let Some(ref outcome) = recovered_outcome {
                        if let Some(req_pid) = auth_req.as_ref().and_then(|a| a.pid.clone()) {
                            sess.pid = req_pid;
                        }
                        for room in &outcome.rooms {
                            self.io.rooms.join(room, sid);
                        }
                    }
                    let sock = SocketRef::new(
                        sid,
                        ns.clone(),
                        Arc::clone(&self.io.rooms),
                        sess.socket_outbound.clone(),
                    );
                    sess.sockets_by_ns.insert(ns.clone(), sock.clone());
                    (sock, sess.raw_outbound.clone(), sess.pid.clone())
                };

                let ns_prefix = if ns == "/" {
                    String::new()
                } else {
                    format!("{ns},")
                };

                let connect_ack = if self.recovery.is_some() {
                    format!("40{ns_prefix}{{\"sid\":\"{sid}\",\"pid\":\"{active_pid}\"}}")
                } else {
                    format!("40{ns_prefix}{{\"sid\":\"{sid}\"}}")
                };
                let _ = raw_out.try_send(connect_ack);

                if let Some(outcome) = recovered_outcome {
                    for frame in outcome.missed_frames {
                        if let Ok(s) = std::str::from_utf8(frame.as_slice()) {
                            let _ = raw_out.try_send(format!("4{s}"));
                        }
                    }
                }

                if let Some(handler) = self.io.namespaces.resolve(&ns) {
                    handler(socket_ref);
                }
            }
            PacketType::Event => {
                self.dispatch_event(sid, pkt, vec![]);
            }
            PacketType::BinaryEvent => {
                let completed = {
                    let Ok(guard) = self.sessions.read() else {
                        return;
                    };
                    let Some(sess) = guard.get(sid) else {
                        return;
                    };
                    let Ok(mut recon) = sess.bin_reconstructor.lock() else {
                        return;
                    };
                    recon.start(pkt)
                };
                if let Some((completed_pkt, bins)) = completed {
                    self.dispatch_event(sid, completed_pkt, bins);
                }
            }
            PacketType::Ack => {
                if let Some(ack_id) = pkt.ack_id {
                    self.acks.resolve(ack_id, pkt.payload);
                }
            }
            PacketType::Disconnect => {
                self.io.rooms.unregister_socket(sid);
            }
            _ => {}
        }
    }

    fn on_binary(&self, sid: &str, data: Bytes) {
        let completed = {
            let Ok(guard) = self.sessions.read() else {
                return;
            };
            let Some(sess) = guard.get(sid) else {
                return;
            };
            let Ok(mut recon) = sess.bin_reconstructor.lock() else {
                return;
            };
            recon.push_attachment(SharedBytes::from_slice(&data))
        };

        if let Some((completed_pkt, bins)) = completed {
            match completed_pkt.packet_type {
                PacketType::BinaryEvent => self.dispatch_event(sid, completed_pkt, bins),
                PacketType::BinaryAck => {
                    if let Some(ack_id) = completed_pkt.ack_id {
                        self.acks.resolve(ack_id, completed_pkt.payload);
                    }
                }
                _ => {}
            }
        }
    }

    fn on_close(&self, sid: &str) {
        let joined_rooms = self.io.rooms.rooms_for_socket(sid);
        self.io.rooms.unregister_socket(sid);
        if let Ok(mut guard) = self.sessions.write() {
            if let Some(sess) = guard.remove(sid) {
                if let Some(ref rec) = self.recovery {
                    rec.save_snapshot(SessionRecoverySnapshot {
                        pid: sess.pid,
                        sid: sid.to_owned(),
                        namespace: "/".to_owned(),
                        rooms: joined_rooms,
                        last_offset: 0,
                        disconnected_at_ms: epoch_ms(),
                    });
                }
            }
        }
    }
}
