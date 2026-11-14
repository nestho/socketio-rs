//! Trait bridging Engine.IO v4 transport sessions (`polling.rs` / `ws.rs`)
//! to the higher-level Socket.IO v5 multiplexing & event dispatch layer.

use bytes::Bytes;
use tokio::sync::mpsc;

/// Callback interface implemented by `socketio-server` to receive Engine.IO
/// `MESSAGE` (`4...`) payloads, binary attachment frames, and lifecycle events.
pub trait EngineHandler: Send + Sync + 'static {
    /// Called when a new Engine.IO session is established with an outbound frame channel.
    fn on_open(&self, sid: &str, outbound: mpsc::Sender<String>);

    /// Called when an Engine.IO `MESSAGE` (`4...`) text payload arrives (with leading `'4'` stripped).
    fn on_message(&self, sid: &str, payload: &str);

    /// Called when a raw binary WebSocket frame arrives (trailing attachment for `5...` / `6...`).
    fn on_binary(&self, sid: &str, data: Bytes) {
        let _ = (sid, data);
    }

    /// Called when an Engine.IO session closes or times out.
    fn on_close(&self, sid: &str);
}
