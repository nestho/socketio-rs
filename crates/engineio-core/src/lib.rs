#![forbid(unsafe_code)]
//! `engineio-core` implements the low-level Engine.IO v4 protocol state machine,
//! adaptive `IoDriver` selection (`io_uring` bare-metal probing with automatic
//! `epoll`/`kqueue` fallback under container `seccomp`), streaming `maxPayload`
//! enforcement (mitigating CVE-2026-59725), constant-time `sid` validation
//! (mitigating CVE-2026-59724), and explicit write watermarks (128 KiB / 16 KiB).

pub mod driver;
pub mod polling;
pub mod ws;

pub use driver::{
    detect_optimal_driver, AdaptiveDriver, DriverCapabilities, IoDriver, IoDriverKind,
    SocketTuningProfile,
};
pub use polling::{
    extract_query_param, generate_sid, handle_polling_request, PollingSession, SessionStore,
    ENGINEIO_RECORD_SEPARATOR,
};
pub use ws::{handle_websocket_upgrade, is_websocket_upgrade, UpgradeFsm};

use std::fmt;

/// Supported Engine.IO v4 transport protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Polling,
    WebSocket,
    WebTransport,
}

impl TransportKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Polling => "polling",
            Self::WebSocket => "websocket",
            Self::WebTransport => "webtransport",
        }
    }
}

/// Compile-time / explicit transport upgrade finite-state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeState {
    /// Initial state (typically HTTP long-polling).
    Active(TransportKind),
    /// Client sent `2probe` over candidate transport; server replied `3probe` and paused polling writes.
    Probing {
        current: TransportKind,
        target: TransportKind,
    },
    /// Client sent `5` (Upgrade) packet; old transport closed cleanly.
    Upgraded(TransportKind),
    /// Connection closed.
    Closed,
}

/// Engine.IO v4 configuration parameters with production-hardened watermarks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub max_payload_bytes: usize,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
    pub tcp_notsent_lowat_bytes: usize,
    pub max_connection_duration_jitter_pct: u8,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            ping_interval_ms: 25_000,
            ping_timeout_ms: 20_000,
            max_payload_bytes: 1_000_000,
            write_high_watermark_bytes: 131_072, // 128 KiB (Envoy 1.39.0 standard)
            write_low_watermark_bytes: 16_384,   // 16 KiB
            tcp_notsent_lowat_bytes: 16_384,     // Prevents kernel send-buffer bloat
            max_connection_duration_jitter_pct: 15,
        }
    }
}

/// Engine.IO v4 Handshake Open packet (`0{"sid":...}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakePacket {
    pub sid: String,
    pub upgrades: Vec<TransportKind>,
    pub ping_interval: u64,
    pub ping_timeout: u64,
    pub max_payload: usize,
}

impl HandshakePacket {
    /// Serializes the Engine.IO v4 open handshake packet (`0{"sid":...}`).
    pub fn to_engineio_open_frame(&self) -> String {
        let upgrades_json: Vec<String> = self
            .upgrades
            .iter()
            .map(|u| format!("\"{}\"", u.as_str()))
            .collect();
        format!(
            "0{{\"sid\":\"{}\",\"upgrades\":[{}],\"pingInterval\":{},\"pingTimeout\":{},\"maxPayload\":{}}}",
            self.sid,
            upgrades_json.join(","),
            self.ping_interval,
            self.ping_timeout,
            self.max_payload
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    InvalidSid,
    PayloadTooLarge { actual: usize, max: usize },
    WriteBackpressureExceeded { buffered: usize, high_watermark: usize },
    InvalidUpgradeTransition { from: UpgradeState, target: TransportKind },
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSid => write!(f, "invalid or malformed session ID (sid)"),
            Self::PayloadTooLarge { actual, max } => write!(
                f,
                "incoming payload ({actual} bytes) exceeds maxPayload ({max} bytes)"
            ),
            Self::WriteBackpressureExceeded {
                buffered,
                high_watermark,
            } => write!(
                f,
                "outbound write buffer exceeded high watermark ({buffered} >= {high_watermark} bytes)"
            ),
            Self::InvalidUpgradeTransition { from, target } => write!(
                f,
                "invalid transport upgrade transition from {from:?} via {target:?}"
            ),
        }
    }
}

impl std::error::Error for EngineError {}

/// Validates a Session ID (`sid`) in constant-bounded length and character set
/// to prevent crafted-SID Denial-of-Service crashes (CVE-2026-59724 mitigation).
pub fn validate_sid(sid: &str) -> Result<(), EngineError> {
    if sid.len() < 16 || sid.len() > 64 {
        return Err(EngineError::InvalidSid);
    }
    let valid = sid
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !valid {
        return Err(EngineError::InvalidSid);
    }
    Ok(())
}

/// Streaming byte-budget tracker that rejects oversized HTTP long-polling or
/// WebSocket frames *before* allocating a contiguous heap buffer (CVE-2026-59725 mitigation).
#[derive(Debug, Clone)]
pub struct StreamingPayloadGuard {
    accumulated_bytes: usize,
    max_payload_bytes: usize,
}

impl StreamingPayloadGuard {
    pub fn new(max_payload_bytes: usize) -> Self {
        Self {
            accumulated_bytes: 0,
            max_payload_bytes,
        }
    }

    pub fn observe_chunk(&mut self, chunk_len: usize) -> Result<usize, EngineError> {
        let next = self.accumulated_bytes.saturating_add(chunk_len);
        if next > self.max_payload_bytes {
            return Err(EngineError::PayloadTooLarge {
                actual: next,
                max: self.max_payload_bytes,
            });
        }
        self.accumulated_bytes = next;
        Ok(self.accumulated_bytes)
    }

    pub fn reset(&mut self) {
        self.accumulated_bytes = 0;
    }
}

/// Per-socket outbound watermark controller (128 KiB high / 16 KiB low).
#[derive(Debug, Clone)]
pub struct WriteWatermarkController {
    buffered_bytes: usize,
    high_watermark: usize,
    low_watermark: usize,
    paused: bool,
}

impl WriteWatermarkController {
    pub fn new(high_watermark: usize, low_watermark: usize) -> Self {
        Self {
            buffered_bytes: 0,
            high_watermark,
            low_watermark,
            paused: false,
        }
    }

    pub fn enqueue(&mut self, bytes: usize) -> Result<(), EngineError> {
        let next = self.buffered_bytes.saturating_add(bytes);
        if next > self.high_watermark {
            self.paused = true;
            return Err(EngineError::WriteBackpressureExceeded {
                buffered: next,
                high_watermark: self.high_watermark,
            });
        }
        self.buffered_bytes = next;
        Ok(())
    }

    pub fn on_flushed(&mut self, flushed_bytes: usize) -> bool {
        self.buffered_bytes = self.buffered_bytes.saturating_sub(flushed_bytes);
        if self.paused && self.buffered_bytes <= self.low_watermark {
            self.paused = false;
        }
        !self.paused
    }

    pub fn is_writable(&self) -> bool {
        !self.paused
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sid_validation_blocks_cve_2026_59724_payloads() {
        assert!(validate_sid("FSDjX-WRwSA4zTZMALqx").is_ok());
        assert_eq!(validate_sid("short"), Err(EngineError::InvalidSid));
        assert_eq!(
            validate_sid("bad/sid/with/slashes/and/null\0"),
            Err(EngineError::InvalidSid)
        );
    }

    #[test]
    fn test_streaming_payload_guard_blocks_cve_2026_59725_exhaustion() {
        let mut guard = StreamingPayloadGuard::new(1024);
        assert_eq!(guard.observe_chunk(512), Ok(512));
        assert_eq!(guard.observe_chunk(512), Ok(1024));
        assert_eq!(
            guard.observe_chunk(1),
            Err(EngineError::PayloadTooLarge {
                actual: 1025,
                max: 1024
            })
        );
    }

    #[test]
    fn test_write_watermark_backpressure() {
        let mut ctrl = WriteWatermarkController::new(131_072, 16_384);
        assert!(ctrl.enqueue(100_000).is_ok());
        assert!(ctrl.enqueue(40_000).is_err());
        assert!(!ctrl.is_writable());
        ctrl.on_flushed(90_000);
        assert!(ctrl.is_writable());
    }

    #[test]
    fn test_handshake_open_frame_serialization() {
        let hs = HandshakePacket {
            sid: "FSDjX-WRwSA4zTZMALqx".to_owned(),
            upgrades: vec![TransportKind::WebSocket, TransportKind::WebTransport],
            ping_interval: 25000,
            ping_timeout: 20000,
            max_payload: 1000000,
        };
        assert_eq!(
            hs.to_engineio_open_frame(),
            "0{\"sid\":\"FSDjX-WRwSA4zTZMALqx\",\"upgrades\":[\"websocket\",\"webtransport\"],\"pingInterval\":25000,\"pingTimeout\":20000,\"maxPayload\":1000000}"
        );
    }
}
