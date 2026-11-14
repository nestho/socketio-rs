//! Live Engine.IO v4 -> Socket.IO v5 session bridge (`SocketIoEngineBridge`).
//! Connects incoming `4...` (`MESSAGE`) frames from `engineio-core` to
//! `socketio-parser`, `NamespaceRegistry`, `SocketRef` event handlers, and `AckTable`.

use crate::{AckTable, EventContext, SocketIo, SocketRef};
use engineio_core::EngineHandler;
use socketio_parser::{decode_v5_packet, parse_dynamic_event, PacketType, SharedBytes};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub type EventCallback = Arc<dyn Fn(SocketRef, EventContext) + Send + Sync>;

/// Bridges `engineio-core` transport sessions to `socketio-server` namespaces,
/// rooms, event callbacks, and acknowledgement correlation.
pub struct SocketIoEngineBridge {
    io: Arc<SocketIo>,
    acks: Arc<AckTable>,
    sessions: RwLock<HashMap<String, ActiveSession>>,
}

struct ActiveSession {
    raw_outbound: mpsc::Sender<String>,
    socket_outbound: mpsc::Sender<SharedBytes>,
    sockets_by_ns: HashMap<String, SocketRef>,
    event_handlers: Arc<RwLock<HashMap<String, EventCallback>>>,
}

impl SocketIoEngineBridge {
    pub fn new(io: Arc<SocketIo>) -> Arc<Self> {
        Arc::new(Self {
            io,
            acks: Arc::new(AckTable::new(4096)),
            sessions: RwLock::new(HashMap::new()),
        })
    }

    /// Registers an event callback (`socket.on(event, ...)`) for a connected session.
    pub fn register_event_handler(
        &self,
        sid: &str,
        event: &str,
        cb: EventCallback,
    ) {
        if let Ok(guard) = self.sessions.read() {
            if let Some(sess) = guard.get(sid) {
                if let Ok(mut handlers) = sess.event_handlers.write() {
                    handlers.insert(event.to_owned(), cb);
                }
            }
        }
    }
}

impl EngineHandler for SocketIoEngineBridge {
    fn on_open(&self, sid: &str, raw_outbound: mpsc::Sender<String>) {
        let (sock_tx, mut sock_rx) = mpsc::channel::<SharedBytes>(256);
        let raw_tx_clone = raw_outbound.clone();

        // Forward Socket.IO SharedBytes packets as Engine.IO MESSAGE ('4' + packet) frames
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
            raw_outbound,
            socket_outbound: sock_tx,
            sockets_by_ns: HashMap::new(),
            event_handlers: Arc::new(RwLock::new(HashMap::new())),
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
                let (socket_ref, raw_out) = {
                    let Ok(mut guard) = self.sessions.write() else {
                        return;
                    };
                    let Some(sess) = guard.get_mut(sid) else {
                        return;
                    };
                    let sock = SocketRef::new(
                        sid,
                        ns.clone(),
                        Arc::clone(&self.io.rooms),
                        sess.socket_outbound.clone(),
                    );
                    sess.sockets_by_ns.insert(ns.clone(), sock.clone());
                    (sock, sess.raw_outbound.clone())
                };

                // Send Socket.IO v5 CONNECT ACK: `40{"sid":"<sid>"}` (or `40/ns,{"sid":"<sid>"}`)
                let ns_prefix = if ns == "/" {
                    String::new()
                } else {
                    format!("{ns},")
                };
                let connect_ack = format!("40{ns_prefix}{{\"sid\":\"{sid}\"}}");
                let _ = raw_out.try_send(connect_ack);

                if let Some(handler) = self.io.namespaces.resolve(&ns) {
                    handler(socket_ref);
                }
            }
            PacketType::Event => {
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
                        binaries: vec![],
                        outbound,
                    };
                    cb(sock, ctx);
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

    fn on_close(&self, sid: &str) {
        self.io.rooms.unregister_socket(sid);
        if let Ok(mut guard) = self.sessions.write() {
            guard.remove(sid);
        }
    }
}
