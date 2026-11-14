//! Live Engine.IO v4 -> Socket.IO v5 WebSocket & CSR recovery bridge integration tests.

use engineio_core::EngineHandler;
use socketio_adapters::RecoveryManager;
use socketio_parser::SharedBytes;
use socketio_server::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn live_engineio_to_socketio_v5_bridge_roundtrip() {
    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);

    io.ns("/", move |socket| {
        let b = Arc::clone(&b_ns);
        b.register_event_handler(
            &socket.id,
            "ping",
            Arc::new(|_sock, ctx| {
                let ack = AckSender::from_context(&ctx);
                ack.send(&"pong");
            }),
        );
        let b2 = Arc::clone(&b_ns);
        b2.register_event_handler(
            &socket.id,
            "echo",
            Arc::new(|sock, ctx| {
                if let Ok((_ev, data)) =
                    socketio_parser::parse_dynamic_event(ctx.payload.as_slice())
                {
                    sock.emit("echo_reply", &data);
                }
            }),
        );
    });

    let (out_tx, mut out_rx) = mpsc::channel::<String>(32);
    let sid = "FSDjX-WRwSA4zTZMALqx";
    bridge.on_open(sid, out_tx);

    bridge.on_message(sid, "0");
    let connect_ack = out_rx.recv().await.expect("connect ack");
    assert_eq!(connect_ack, "40{\"sid\":\"FSDjX-WRwSA4zTZMALqx\"}");

    bridge.on_message(sid, "21[\"ping\",{}]");
    let ack_reply = out_rx.recv().await.expect("ack reply");
    assert_eq!(ack_reply, "431[\"pong\"]");

    bridge.on_message(sid, "2[\"echo\",{\"msg\":\"hello\"}]");
    let echo_reply = out_rx.recv().await.expect("echo reply");
    assert_eq!(echo_reply, "42[\"echo_reply\",{\"msg\":\"hello\"}]");

    bridge.on_close(sid);
}

#[tokio::test]
async fn live_csr_pid_offset_handshake_recovery_replays_missed_frames() {
    let io = Arc::new(SocketIo::new(4));
    let recovery = Arc::new(RecoveryManager::new(128, 120_000));
    let bridge = SocketIoEngineBridge::with_recovery(Arc::clone(&io), Arc::clone(&recovery));

    io.ns("/", |socket| {
        socket.join("room:alpha");
    });

    // 1. Initial connection
    let (tx1, mut rx1) = mpsc::channel::<String>(16);
    let sid1 = "FSDjX-WRwSA4zTZMALq1";
    bridge.on_open(sid1, tx1);
    bridge.on_message(sid1, "0");
    let ack1 = rx1.recv().await.unwrap();
    assert!(ack1.contains("\"pid\":\"pid-FSDjX-WRwSA4zTZMALq1\""));

    let o1 = recovery.record_outbound(
        "room:alpha",
        SharedBytes::from_slice(b"2[\"msg\",1]"),
        1_000_000_000_000,
    );

    // 2. Client disconnects; broadcast happens while offline
    bridge.on_close(sid1);
    let _o2 = recovery.record_outbound(
        "room:alpha",
        SharedBytes::from_slice(b"2[\"msg\",2]"),
        u64::MAX / 2,
    );

    // 3. Client reconnects with pid + offset in 40{...} handshake
    let (tx2, mut rx2) = mpsc::channel::<String>(16);
    let sid2 = "FSDjX-WRwSA4zTZMALq2";
    bridge.on_open(sid2, tx2);
    bridge.on_message(
        sid2,
        &format!("0{{\"pid\":\"pid-FSDjX-WRwSA4zTZMALq1\",\"offset\":\"{o1}\"}}"),
    );

    let ack2 = rx2.recv().await.unwrap();
    assert!(ack2.contains("\"pid\":\"pid-FSDjX-WRwSA4zTZMALq1\""));
    let replayed = rx2.recv().await.unwrap();
    assert_eq!(replayed, "42[\"msg\",2]");
}
