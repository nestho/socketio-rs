//! Prometheus `/metrics` exporter with `64-byte` cache-line padded counters (`S4-T07`).

use socketio_server::CacheLinePaddedCounter;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct GatewayMetrics {
    pub active_connections: CacheLinePaddedCounter,
    pub broadcasts_total: CacheLinePaddedCounter,
    pub dropped_slow_consumers_total: CacheLinePaddedCounter,
    pub recovered_sessions_total: CacheLinePaddedCounter,
    pub cswsh_rejections_total: CacheLinePaddedCounter,
}

impl GatewayMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP socketio_rs_active_connections Active Engine.IO/Socket.IO connections\n\
             # TYPE socketio_rs_active_connections gauge\n\
             socketio_rs_active_connections {}\n\
             # HELP socketio_rs_broadcasts_total Total zero-copy room broadcasts dispatched\n\
             # TYPE socketio_rs_broadcasts_total counter\n\
             socketio_rs_broadcasts_total {}\n\
             # HELP socketio_rs_dropped_slow_consumers_total Connections evicted due to 128 KiB write watermark\n\
             # TYPE socketio_rs_dropped_slow_consumers_total counter\n\
             socketio_rs_dropped_slow_consumers_total {}\n\
             # HELP socketio_rs_recovered_sessions_total Sessions restored via Connection State Recovery\n\
             # TYPE socketio_rs_recovered_sessions_total counter\n\
             socketio_rs_recovered_sessions_total {}\n\
             # HELP socketio_rs_cswsh_rejections_total Requests rejected by CSWSH Origin policy\n\
             # TYPE socketio_rs_cswsh_rejections_total counter\n\
             socketio_rs_cswsh_rejections_total {}\n",
            self.active_connections.get(),
            self.broadcasts_total.get(),
            self.dropped_slow_consumers_total.get(),
            self.recovered_sessions_total.get(),
            self.cswsh_rejections_total.get(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_metrics_output() {
        let m = GatewayMetrics::new();
        m.broadcasts_total.inc();
        let text = m.render_prometheus();
        assert!(text.contains("socketio_rs_broadcasts_total 1"));
    }
}
