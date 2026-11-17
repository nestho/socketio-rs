//! Engine.IO v4 WebSocket transport (`tokio-tungstenite` over `hyper::upgrade`),
//! `2probe` -> `3probe` -> `5` upgrade state machine, and `pingInterval` /
//! `pingTimeout` heartbeat timer with `WriteWatermarkController` backpressure.

use crate::polling::{extract_query_param, SessionStore};
use crate::{
    validate_sid, EngineConfig, EngineError, HandshakePacket, TransportKind, UpgradeState,
    WriteWatermarkController,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Explicit Engine.IO v4 WebSocket upgrade finite-state machine (`S1-T08`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeFsm {
    pub state: UpgradeState,
}

impl UpgradeFsm {
    pub fn new_direct_ws() -> Self {
        Self {
            state: UpgradeState::Active(TransportKind::WebSocket),
        }
    }

    pub fn new_upgrading_from_polling() -> Self {
        Self {
            state: UpgradeState::Probing {
                current: TransportKind::Polling,
                target: TransportKind::WebSocket,
            },
        }
    }

    /// Processes an Engine.IO control packet during upgrade (`2probe` -> `3probe`, `5` -> `Upgraded`).
    pub fn on_control_packet(&mut self, frame: &str) -> Result<Option<&'static str>, EngineError> {
        match self.state {
            UpgradeState::Probing {
                target: TransportKind::WebSocket,
                ..
            } => {
                if frame == "2probe" {
                    return Ok(Some("3probe"));
                }
                if frame == "5" {
                    self.state = UpgradeState::Upgraded(TransportKind::WebSocket);
                    return Ok(None);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

/// Returns true if the incoming HTTP request is an RFC 6455 WebSocket upgrade.
pub fn is_websocket_upgrade(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
}

fn bad_request(body: &'static [u8]) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("Content-Type", "text/plain; charset=UTF-8")
        .body(Full::new(Bytes::from_static(body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"bad request"))))
}

/// Handles an Engine.IO v4 WebSocket upgrade request (`GET /socket.io/?EIO=4&transport=websocket`).
pub async fn handle_websocket_upgrade(
    req: Request<Incoming>,
    store: SessionStore,
    handler: Option<std::sync::Arc<dyn crate::EngineHandler>>,
) -> Response<Full<Bytes>> {
    let query = req.uri().query().unwrap_or("").to_owned();
    if !crate::polling::is_supported_eio_version(extract_query_param(&query, "EIO")) {
        return bad_request(b"{\"code\":5,\"message\":\"Unsupported protocol version\"}");
    }

    let sid_opt = extract_query_param(&query, "sid").map(str::to_owned);
    if let Some(ref sid) = sid_opt {
        if validate_sid(sid).is_err() {
            return bad_request(b"{\"code\":1,\"message\":\"Session ID unknown\"}");
        }
    }

    let Some(ws_key) = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    else {
        return bad_request(b"Missing Sec-WebSocket-Key");
    };

    let accept_key = derive_accept_key(ws_key.as_bytes());
    let config = store.config().clone();

    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                let ws_stream =
                    WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None)
                        .await;
                run_ws_session(ws_stream, sid_opt, store, config, handler).await;
            }
            Err(_) => {
                // Client aborted before upgrade completed
            }
        }
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_ACCEPT, accept_key)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| bad_request(b"upgrade error"))
}

/// Runs the per-connection WebSocket event loop with `pingInterval`/`pingTimeout`
/// heartbeat monitoring and `WriteWatermarkController` backpressure (`S1-T09`).
async fn run_ws_session<S>(
    ws_stream: WebSocketStream<S>,
    sid_opt: Option<String>,
    store: SessionStore,
    config: EngineConfig,
    handler: Option<std::sync::Arc<dyn crate::EngineHandler>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_tx, mut ws_rx) = ws_stream.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    let mut watermarks = WriteWatermarkController::new(
        config.write_high_watermark_bytes,
        config.write_low_watermark_bytes,
    );

    let (sid, mut fsm) = match sid_opt {
        Some(existing_sid) => {
            // Upgrading from HTTP long-polling: wait for `2probe` -> `3probe` -> `5`
            (existing_sid, UpgradeFsm::new_upgrading_from_polling())
        }
        None => {
            // Direct WebSocket connection: create session and emit open handshake (`0{...}`)
            let mut hs: HandshakePacket = store.create_session();
            hs.upgrades.retain(|u| *u != TransportKind::WebSocket);
            let _ = out_tx.try_send(hs.to_engineio_open_frame());
            (hs.sid, UpgradeFsm::new_direct_ws())
        }
    };

    if let Some(ref h) = handler {
        h.on_open(&sid, out_tx.clone());
    }

    let ping_interval = Duration::from_millis(config.ping_interval_ms.max(100));
    let ping_timeout = Duration::from_millis(config.ping_timeout_ms.max(100));
    let mut ping_ticker = tokio::time::interval_at(Instant::now() + ping_interval, ping_interval);
    let mut awaiting_pong_since: Option<Instant> = None;

    loop {
        tokio::select! {
            Some(outbound) = out_rx.recv() => {
                let len = outbound.len();
                if watermarks.enqueue(len).is_err() {
                    // Slow-consumer backpressure exceeded 128 KiB high watermark
                    break;
                }
                if ws_tx.send(Message::Text(outbound.into())).await.is_err() {
                    break;
                }
                watermarks.on_flushed(len);
            }
            _ = ping_ticker.tick() => {
                if let Some(sent_at) = awaiting_pong_since {
                    if sent_at.elapsed() >= ping_timeout {
                        // Client failed to respond with Engine.IO PONG (`3`) within pingTimeout
                        break;
                    }
                } else {
                    if out_tx.try_send("2".to_owned()).is_err() {
                        break;
                    }
                    awaiting_pong_since = Some(Instant::now());
                }
            }
            inbound = ws_rx.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        let s: &str = text.as_ref();
                        if s.len() > config.max_payload_bytes {
                            break;
                        }
                        if s == "3" {
                            // Engine.IO PONG received
                            awaiting_pong_since = None;
                        } else if s == "1" {
                            // Engine.IO CLOSE
                            break;
                        } else if let Some(sio_payload) = s.strip_prefix('4') {
                            // Engine.IO MESSAGE ('4...') -> dispatch to Socket.IO layer
                            if let Some(ref h) = handler {
                                h.on_message(&sid, sio_payload);
                            }
                        } else if let Ok(maybe_reply) = fsm.on_control_packet(s) {
                            if let Some(reply) = maybe_reply {
                                if out_tx.try_send(reply.to_owned()).is_err() {
                                    break;
                                }
                            }
                            if matches!(fsm.state, UpgradeState::Upgraded(_)) {
                                // Drain any buffered polling packets onto WebSocket
                                if let Some((pending, _)) = store.drain_outbound(&sid) {
                                    for pkt in pending.split('\x1e') {
                                        if !pkt.is_empty() && out_tx.try_send(pkt.to_owned()).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        if bin.len() > config.max_payload_bytes {
                            break;
                        }
                        if let Some(ref h) = handler {
                            h.on_binary(&sid, bin);
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }

    if let Some(ref h) = handler {
        h.on_close(&sid);
    }
    store.remove_session(&sid);
    let _ = ws_tx.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_fsm_probe_and_commit() {
        let mut fsm = UpgradeFsm::new_upgrading_from_polling();
        assert_eq!(fsm.on_control_packet("2probe"), Ok(Some("3probe")));
        assert_eq!(fsm.on_control_packet("5"), Ok(None));
        assert_eq!(fsm.state, UpgradeState::Upgraded(TransportKind::WebSocket));
    }
}
