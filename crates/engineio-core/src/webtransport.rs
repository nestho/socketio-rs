//! Engine.IO v4 HTTP/3 WebTransport (QUIC) transport framing, single-stream-per-session
//! enforcement, and constant-time `sid` validation (mitigating CVE-2026-59724) (`S4-T01`, `S4-T02`).

use crate::{validate_sid, EngineError, TransportKind, UpgradeState};
use std::sync::atomic::{AtomicBool, Ordering};

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
        // Second bidi stream MUST be rejected per Engine.IO v4 WebTransport spec
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
