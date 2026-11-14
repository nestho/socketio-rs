//! Engine.IO v4 HTTP Long-Polling transport (`GET` handshake/poll + `POST`
//! streaming `maxPayload` guard + per-SID session lifecycle & timeout reaper).

use crate::{
    validate_sid, EngineConfig, HandshakePacket, StreamingPayloadGuard, TransportKind,
    UpgradeState, WriteWatermarkController,
};
use bytes::{Buf, Bytes, BytesMut};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

/// Engine.IO v4 batch record separator (`\x1e`).
pub const ENGINEIO_RECORD_SEPARATOR: char = '\x1e';

static SID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generates a URL-safe, >=16-character Session ID (`sid`) passing `validate_sid`.
pub fn generate_sid() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = SID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("eio4-{:08x}-{:08x}", nanos ^ 0xA5A5_5A5A, seq)
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Active Engine.IO v4 HTTP long-polling session state.
#[derive(Debug)]
pub struct PollingSession {
    pub sid: String,
    pub state: UpgradeState,
    pub last_seen_ms: u64,
    pub outbound_queue: VecDeque<String>,
    pub watermarks: WriteWatermarkController,
    pub notify: Arc<Notify>,
}

/// Thread-safe Engine.IO v4 session registry with idle/aborted reaper.
#[derive(Debug, Clone)]
pub struct SessionStore {
    sessions: Arc<RwLock<HashMap<String, PollingSession>>>,
    config: EngineConfig,
}

impl SessionStore {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Creates a new polling session and returns its `HandshakePacket`.
    pub fn create_session(&self) -> HandshakePacket {
        let sid = generate_sid();
        let session = PollingSession {
            sid: sid.clone(),
            state: UpgradeState::Active(TransportKind::Polling),
            last_seen_ms: current_epoch_ms(),
            outbound_queue: VecDeque::new(),
            watermarks: WriteWatermarkController::new(
                self.config.write_high_watermark_bytes,
                self.config.write_low_watermark_bytes,
            ),
            notify: Arc::new(Notify::new()),
        };

        if let Ok(mut guard) = self.sessions.write() {
            guard.insert(sid.clone(), session);
        }

        HandshakePacket {
            sid,
            upgrades: vec![TransportKind::WebSocket],
            ping_interval: self.config.ping_interval_ms,
            ping_timeout: self.config.ping_timeout_ms,
            max_payload: self.config.max_payload_bytes,
        }
    }

    /// Enqueues an encoded Engine.IO packet for delivery on the next `GET` poll.
    pub fn enqueue_packet(&self, sid: &str, packet: impl Into<String>) -> bool {
        let pkt = packet.into();
        if let Ok(mut guard) = self.sessions.write() {
            if let Some(session) = guard.get_mut(sid) {
                if session.watermarks.enqueue(pkt.len()).is_err() {
                    return false;
                }
                session.outbound_queue.push_back(pkt);
                session.notify.notify_one();
                return true;
            }
        }
        false
    }

    /// Drains queued outbound packets joined by `\x1e` (Engine.IO v4 record separator).
    pub fn drain_outbound(&self, sid: &str) -> Option<(String, Arc<Notify>)> {
        let mut guard = self.sessions.write().ok()?;
        let session = guard.get_mut(sid)?;
        session.last_seen_ms = current_epoch_ms();

        if session.outbound_queue.is_empty() {
            return Some((String::new(), Arc::clone(&session.notify)));
        }

        let mut drained_bytes = 0usize;
        let mut parts = Vec::with_capacity(session.outbound_queue.len());
        while let Some(pkt) = session.outbound_queue.pop_front() {
            drained_bytes += pkt.len();
            parts.push(pkt);
        }
        session.watermarks.on_flushed(drained_bytes);
        Some((
            parts.join(&ENGINEIO_RECORD_SEPARATOR.to_string()),
            Arc::clone(&session.notify),
        ))
    }

    pub fn remove_session(&self, sid: &str) {
        if let Ok(mut guard) = self.sessions.write() {
            guard.remove(sid);
        }
    }

    pub fn active_count(&self) -> usize {
        self.sessions.read().map(|g| g.len()).unwrap_or(0)
    }

    /// Reaps expired or abandoned HTTP long-polling sessions (`S1-T06`).
    pub fn reap_expired(&self, now_ms: u64) -> usize {
        let ttl = self.config.ping_interval_ms + self.config.ping_timeout_ms;
        if let Ok(mut guard) = self.sessions.write() {
            let before = guard.len();
            guard.retain(|_, s| now_ms.saturating_sub(s.last_seen_ms) <= ttl);
            return before.saturating_sub(guard.len());
        }
        0
    }
}

/// Parses `sid` query parameter from a `/socket.io/?EIO=4&transport=polling&sid=...` URI.
pub fn extract_query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v);
            }
        }
    }
    None
}

fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain; charset=UTF-8")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(body.into()))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"internal error"))))
}

/// Returns true if the requested `EIO` protocol version is supported.
pub fn is_supported_eio_version(eio: Option<&str>) -> bool {
    match eio {
        Some("4") => true,
        #[cfg(feature = "v4")]
        Some("3") => true,
        _ => false,
    }
}

/// Processes an incoming Engine.IO v4 HTTP long-polling request (`GET` or `POST`).
pub async fn handle_polling_request(
    req: Request<Incoming>,
    store: SessionStore,
    handler: Option<Arc<dyn crate::EngineHandler>>,
) -> Response<Full<Bytes>> {
    let query = req.uri().query().unwrap_or("").to_owned();
    let eio = extract_query_param(&query, "EIO");
    if !is_supported_eio_version(eio) {
        // Immediate teardown + Retry-After prevents Issue #3477 version-mismatch reconnect leaks
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "application/json")
            .header("Connection", "close")
            .header("Retry-After", "5")
            .body(Full::new(Bytes::from_static(
                b"{\"code\":5,\"message\":\"Unsupported protocol version\"}",
            )))
            .unwrap_or_else(|_| text_response(StatusCode::BAD_REQUEST, b"Unsupported protocol"));
    }

    let sid_opt = extract_query_param(&query, "sid").map(str::to_owned);

    match *req.method() {
        Method::GET => match sid_opt.as_deref() {
            None => {
                let hs = store.create_session();
                if let Some(ref h) = handler {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(256);
                    let store_fwd = store.clone();
                    let sid_fwd = hs.sid.clone();
                    tokio::spawn(async move {
                        while let Some(pkt) = rx.recv().await {
                            if !store_fwd.enqueue_packet(&sid_fwd, pkt) {
                                break;
                            }
                        }
                    });
                    h.on_open(&hs.sid, tx);
                }
                text_response(StatusCode::OK, Bytes::from(hs.to_engineio_open_frame()))
            }
            Some(sid) => {
                if validate_sid(sid).is_err() {
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        Bytes::from_static(b"{\"code\":1,\"message\":\"Session ID unknown\"}"),
                    );
                }

                let Some((payload, notify)) = store.drain_outbound(sid) else {
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        Bytes::from_static(b"{\"code\":1,\"message\":\"Session ID unknown\"}"),
                    );
                };

                if !payload.is_empty() {
                    return text_response(StatusCode::OK, Bytes::from(payload));
                }

                let wait_ms = store.config().ping_interval_ms.min(25_000);
                let _ = tokio::time::timeout(Duration::from_millis(wait_ms), notify.notified()).await;

                match store.drain_outbound(sid) {
                    Some((delayed, _)) if !delayed.is_empty() => {
                        text_response(StatusCode::OK, Bytes::from(delayed))
                    }
                    Some(_) => text_response(StatusCode::OK, Bytes::from_static(b"6")), // Engine.IO NOOP
                    None => text_response(
                        StatusCode::BAD_REQUEST,
                        Bytes::from_static(b"{\"code\":1,\"message\":\"Session ID unknown\"}"),
                    ),
                }
            }
        },
        Method::POST => {
            let Some(sid) = sid_opt.as_deref() else {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    Bytes::from_static(b"{\"code\":1,\"message\":\"Session ID unknown\"}"),
                );
            };
            if validate_sid(sid).is_err() {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    Bytes::from_static(b"{\"code\":1,\"message\":\"Session ID unknown\"}"),
                );
            }

            // Stream body chunks through StreamingPayloadGuard BEFORE allocating full payload (CVE-2026-59725)
            let mut guard = StreamingPayloadGuard::new(store.config().max_payload_bytes);
            let mut body = req.into_body();
            let mut buf = BytesMut::new();

            while let Some(frame_res) = body.frame().await {
                let Ok(frame) = frame_res else {
                    return text_response(
                        StatusCode::BAD_REQUEST,
                        Bytes::from_static(b"{\"code\":3,\"message\":\"Bad request\"}"),
                    );
                };
                if let Some(chunk) = frame.data_ref() {
                    if guard.observe_chunk(chunk.len()).is_err() {
                        return text_response(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            Bytes::from_static(b"{\"code\":3,\"message\":\"Payload too large\"}"),
                        );
                    }
                    buf.extend_from_slice(chunk.chunk());
                }
            }

            let Ok(text) = std::str::from_utf8(&buf) else {
                return text_response(
                    StatusCode::BAD_REQUEST,
                    Bytes::from_static(b"{\"code\":3,\"message\":\"Invalid UTF-8 payload\"}"),
                );
            };

            for packet in text.split(ENGINEIO_RECORD_SEPARATOR) {
                if packet.is_empty() {
                    continue;
                }
                if packet.starts_with('1') {
                    if let Some(ref h) = handler {
                        h.on_close(sid);
                    }
                    store.remove_session(sid);
                } else if let Some(rest) = packet.strip_prefix('2') {
                    // Engine.IO ping -> enqueue pong ('3' + probe payload)
                    let pong = format!("3{rest}");
                    store.enqueue_packet(sid, pong);
                } else if let Some(sio_payload) = packet.strip_prefix('4') {
                    if let Some(ref h) = handler {
                        h.on_message(sid, sio_payload);
                    }
                }
            }

            text_response(StatusCode::OK, Bytes::from_static(b"ok"))
        }
        _ => text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            Bytes::from_static(b"{\"code\":2,\"message\":\"Bad handshake method\"}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polling_session_lifecycle_and_reaper() {
        let cfg = EngineConfig::default();
        let store = SessionStore::new(cfg);

        let hs = store.create_session();
        assert!(validate_sid(&hs.sid).is_ok());
        assert_eq!(store.active_count(), 1);

        assert!(store.enqueue_packet(&hs.sid, "3probe"));
        assert!(store.enqueue_packet(&hs.sid, "40"));

        let (drained, _) = store.drain_outbound(&hs.sid).expect("session exists");
        assert_eq!(drained, "3probe\x1e40");

        // Reaping far in the future cleans up expired sessions
        let reaped = store.reap_expired(u64::MAX / 2);
        assert_eq!(reaped, 1);
        assert_eq!(store.active_count(), 0);
    }
}
