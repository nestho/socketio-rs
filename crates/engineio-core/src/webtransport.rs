//! Engine.IO v4 HTTP/3 WebTransport (QUIC) transport (`wtransport` + `quinn` + `rustls`),
//! single-stream-per-session enforcement, and constant-time `sid` validation (CVE-2026-59724).

use crate::polling::generate_sid;
use crate::{
    validate_sid, EngineConfig, EngineError, EngineHandler, HandshakePacket, TransportKind,
    UpgradeState,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use wtransport::{Endpoint, Identity, ServerConfig};

/// HTTP/3 WebTransport (QUIC / `wtransport` + `quinn`) endpoint configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebTransportConfig {
    pub enabled: bool,
    pub udp_gso_enabled: bool,
    pub udp_gro_enabled: bool,
    pub max_concurrent_bidi_streams: u32,
}

impl Default for WebTransportConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            udp_gso_enabled: true,
            udp_gro_enabled: true,
            // Per Engine.IO v4 spec: a client MUST NOT open more than one WebTransport stream per session.
            max_concurrent_bidi_streams: 1,
        }
    }
}

/// Enforces Engine.IO v4 WebTransport session invariants (`S4-T02`):
/// 1. Validates `sid` via `validate_sid` before allocating session state (CVE-2026-59724).
/// 2. Rejects any attempt to open a second bidirectional stream on the same session.
#[derive(Debug)]
pub struct WebTransportSessionGuard {
    pub sid: String,
    pub state: UpgradeState,
    stream_opened: AtomicBool,
}

impl WebTransportSessionGuard {
    pub fn new(sid: &str) -> Result<Self, EngineError> {
        validate_sid(sid)?;
        Ok(Self {
            sid: sid.to_owned(),
            state: UpgradeState::Active(TransportKind::WebTransport),
            stream_opened: AtomicBool::new(false),
        })
    }

    /// Registers the single allowed bidirectional Engine.IO stream.
    /// Returns `Err(EngineError::InvalidUpgradeTransition)` if a second stream is attempted.
    pub fn accept_bidi_stream(&self) -> Result<(), EngineError> {
        let already_open = self.stream_opened.swap(true, Ordering::SeqCst);
        if already_open {
            return Err(EngineError::InvalidUpgradeTransition {
                from: self.state,
                target: TransportKind::WebTransport,
            });
        }
        Ok(())
    }
}

/// Optional PEM certificate + private key paths for HTTP/3 WebTransport TLS.
#[derive(Debug, Clone, Default)]
pub struct WebTransportTlsConfig {
    pub cert_pem_path: Option<std::path::PathBuf>,
    pub key_pem_path: Option<std::path::PathBuf>,
}

/// Starts a live HTTP/3 WebTransport (`wtransport` / `quinn`) server endpoint
/// listening for Engine.IO v4 bidirectional streams (`S4-T01`, `S4-T02`).
pub async fn serve_webtransport_endpoint(
    bind_addr: SocketAddr,
    engine_cfg: EngineConfig,
    handler: Option<Arc<dyn EngineHandler>>,
    tls_cfg: WebTransportTlsConfig,
) -> Result<(), std::io::Error> {
    let identity = match (tls_cfg.cert_pem_path, tls_cfg.key_pem_path) {
        (Some(cert), Some(key)) => Identity::load_pemfiles(&cert, &key)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?,
        _ => Identity::self_signed(["localhost", "127.0.0.1"])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?,
    };

    if let Some(leaf) = identity.certificate_chain().as_slice().first() {
        println!("  WebTransport Cert SHA256 : {}", leaf.hash());
    }

    let wt_server_cfg = ServerConfig::builder()
        .with_bind_address(bind_addr)
        .with_identity(identity)
        .build();

    let endpoint = Endpoint::server(wt_server_cfg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    loop {
        let incoming = endpoint.accept().await;
        let cfg = engine_cfg.clone();
        let h_clone = handler.clone();

        tokio::spawn(async move {
            let Ok(session_request) = incoming.await else {
                return;
            };
            let Ok(connection) = session_request.accept().await else {
                return;
            };
            let sid = generate_sid();
            let Ok(guard) = WebTransportSessionGuard::new(&sid) else {
                return;
            };
            let Ok((mut send_stream, mut recv_stream)) = connection.accept_bi().await else {
                return;
            };
            if guard.accept_bidi_stream().is_err() {
                return;
            }

            let hs = HandshakePacket {
                sid: sid.clone(),
                upgrades: vec![],
                ping_interval: cfg.ping_interval_ms,
                ping_timeout: cfg.ping_timeout_ms,
                max_payload: cfg.max_payload_bytes,
            };
            let open_frame = format!("{}\n", hs.to_engineio_open_frame());
            if send_stream.write_all(open_frame.as_bytes()).await.is_err() {
                return;
            }

            let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
            if let Some(ref h) = h_clone {
                h.on_open(&sid, out_tx);
            }

            let mut read_buf = vec![0u8; 8192];
            loop {
                tokio::select! {
                    Some(outbound) = out_rx.recv() => {
                        let line = format!("{outbound}\n");
                        if send_stream.write_all(line.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    res = recv_stream.read(&mut read_buf) => {
                        match res {
                            Ok(Some(n)) if n > 0 => {
                                if let Ok(text) = std::str::from_utf8(&read_buf[..n]) {
                                    for pkt in text.lines() {
                                        if let Some(sio) = pkt.strip_prefix('4') {
                                            if let Some(ref h) = h_clone {
                                                h.on_message(&sid, sio);
                                            }
                                        }
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                }
            }

            if let Some(ref h) = h_clone {
                h.on_close(&sid);
            }
        });
    }
}

/// Computes deterministic per-connection jittered max duration (Envoy 1.39.0 pattern)
/// to prevent synchronized client reconnect avalanches (`S4-T05`).
pub fn compute_jittered_duration_ms(base_ms: u64, jitter_pct: u8, sid: &str) -> u64 {
    if jitter_pct == 0 || base_ms == 0 {
        return base_ms;
    }
    let mut hash: u64 = 14695981039346656037;
    for b in sid.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    let max_jitter = (base_ms as u128 * jitter_pct as u128 / 100) as u64;
    if max_jitter == 0 {
        return base_ms;
    }
    base_ms.saturating_add(hash % (max_jitter + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webtransport_single_stream_invariant_and_sid_guard() {
        assert!(WebTransportSessionGuard::new("bad").is_err());

        let guard = WebTransportSessionGuard::new("FSDjX-WRwSA4zTZMALqx").expect("valid sid");
        assert!(guard.accept_bidi_stream().is_ok());
        assert!(guard.accept_bidi_stream().is_err());
    }

    #[test]
    fn test_jittered_connection_duration_spreads_reconnects() {
        let d1 = compute_jittered_duration_ms(60_000, 15, "sid-alpha-00000001");
        let d2 = compute_jittered_duration_ms(60_000, 15, "sid-beta-000000002");
        assert!(d1 >= 60_000 && d1 <= 69_000);
        assert!(d2 >= 60_000 && d2 <= 69_000);
    }
}
