//! Multi-shard fan-out load test & WiFi-jitter CSR replay verification (`S6-T01`, `S6-T02`).

use socketio_adapters::{RecoveryManager, SessionRecoverySnapshot};
use socketio_parser::SharedBytes;
use socketio_server::{SocketIo, SocketRef};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

#[tokio::test]
async fn load_10k_subscribers_zero_copy_sharded_fanout() {
    let subscribers = 10_000usize;
    let io = SocketIo::new(8);
    let mut receivers = Vec::with_capacity(subscribers);

    for i in 0..subscribers {
        let (tx, rx) = mpsc::channel(8);
        let sid = format!("sid-load-{i:08}");
        let sock = SocketRef::new(sid, "/", Arc::clone(&io.rooms), tx);
        sock.join("global:feed");
        receivers.push(rx);
    }

    let t0 = Instant::now();
    let delivered = io
        .to("global:feed")
        .emit("tick", &serde_json::json!({"price": 42.5}));
    let elapsed = t0.elapsed();

    assert_eq!(delivered, subscribers);
    // Verify every subscriber received the frame in memory
    for mut rx in receivers {
        assert!(rx.try_recv().is_ok());
    }
    println!("10K zero-copy sharded fan-out completed in {:?}", elapsed);
}

#[test]
fn wifi_jitter_simulation_100_percent_delivery_via_csr() {
    let csr = RecoveryManager::new(1024, 120_000);
    let mut last_seen_offset = 0;

    // 1. Client receives 10 frames before WiFi drops
    for i in 1..=10 {
        last_seen_offset = csr.record_outbound(
            "chat:1",
            SharedBytes::from_slice(format!("2[\"msg\",{i}]").as_bytes()),
            i * 100,
        );
    }

    // 2. Client drops for 2 seconds; 15 frames are broadcast while offline
    csr.save_snapshot(SessionRecoverySnapshot {
        pid: "pid-jitter-1".to_owned(),
        sid: "sid-jitter-1".to_owned(),
        namespace: "/".to_owned(),
        rooms: vec!["chat:1".to_owned()],
        last_offset: last_seen_offset,
        disconnected_at_ms: 1_050,
    });

    for i in 11..=25 {
        csr.record_outbound(
            "chat:1",
            SharedBytes::from_slice(format!("2[\"msg\",{i}]").as_bytes()),
            i * 100,
        );
    }

    // 3. Client reconnects and recovers 100% of missed frames
    let outcome = csr.try_recover("pid-jitter-1", last_seen_offset, 3_050).unwrap();
    assert!(outcome.recovered);
    assert_eq!(outcome.missed_frames.len(), 15);
}
