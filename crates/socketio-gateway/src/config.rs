//! TOML configuration loader for `socketio-gateway` (`S4-T03`).

use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GatewayConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub engine: EngineSection,
    #[serde(default)]
    pub security: SecuritySection,
    #[serde(default)]
    pub recovery: RecoverySection,
    #[serde(default)]
    pub bridge: BridgeSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerSection {
    pub bind_addr: String,
    pub path: String,
    pub io_driver: String,
    pub worker_threads: usize,
    pub shutdown_drain_timeout_ms: u64,
    pub max_connection_duration_jitter_pct: u8,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3000".to_owned(),
            path: "/socket.io/".to_owned(),
            io_driver: "auto".to_owned(),
            worker_threads: 0,
            shutdown_drain_timeout_ms: 15_000,
            max_connection_duration_jitter_pct: 15,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineSection {
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub max_payload_bytes: usize,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
    pub tcp_notsent_lowat_bytes: usize,
    pub enable_webtransport_h3: bool,
}

impl Default for EngineSection {
    fn default() -> Self {
        Self {
            ping_interval_ms: 25_000,
            ping_timeout_ms: 20_000,
            max_payload_bytes: 1_000_000,
            write_high_watermark_bytes: 131_072,
            write_low_watermark_bytes: 16_384,
            tcp_notsent_lowat_bytes: 16_384,
            enable_webtransport_h3: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecuritySection {
    pub allowed_origins: Vec<String>,
    pub require_origin_header: bool,
    pub max_decompression_ratio: usize,
    pub permessage_deflate_threshold_bytes: usize,
    pub server_max_window_bits: u8,
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            allowed_origins: vec!["*".to_owned()],
            require_origin_header: false,
            max_decompression_ratio: 100,
            permessage_deflate_threshold_bytes: 1024,
            server_max_window_bits: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecoverySection {
    pub enabled: bool,
    pub max_disconnection_duration_ms: u64,
    pub ring_buffer_capacity_per_room: usize,
    pub adapter: String,
}

impl Default for RecoverySection {
    fn default() -> Self {
        Self {
            enabled: true,
            max_disconnection_duration_ms: 120_000,
            ring_buffer_capacity_per_room: 512,
            adapter: "in_memory".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BridgeSection {
    pub broadcast_bind_addr: String,
}

impl Default for BridgeSection {
    fn default() -> Self {
        Self {
            broadcast_bind_addr: "127.0.0.1:3001".to_owned(),
        }
    }
}

impl GatewayConfig {
    pub fn load_or_default(path: &Path) -> Self {
        match fs::read_to_string(path) {
            Ok(raw) => toml::from_str(&raw).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gateway_toml_parse() {
        let sample = r#"
            [server]
            bind_addr = "0.0.0.0:3000"
            path = "/socket.io/"
            io_driver = "auto"
            worker_threads = 4
            shutdown_drain_timeout_ms = 10000
            max_connection_duration_jitter_pct = 20

            [bridge]
            broadcast_bind_addr = "127.0.0.1:3001"
        "#;
        let cfg: GatewayConfig = toml::from_str(sample).expect("valid gateway.toml");
        assert_eq!(cfg.server.worker_threads, 4);
        assert_eq!(cfg.server.max_connection_duration_jitter_pct, 20);
        assert_eq!(cfg.bridge.broadcast_bind_addr, "127.0.0.1:3001");
    }
}
