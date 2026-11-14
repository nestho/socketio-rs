//! Live Engine.IO v4 -> Socket.IO v5 WebSocket bridge integration test (Step 1 & Step 2).

use engineio_core::EngineHandler;
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

    // 1. Client sends Socket.IO v5 CONNECT ('0')
    bridge.on_message(sid, "0");
    let connect_ack = out_rx.recv().await.expect("connect ack");
    assert_eq!(connect_ack, "40{\"sid\":\"FSDjX-WRwSA4zTZMALqx\"}");

    // 2. Client sends Socket.IO v5 EVENT with Ack ID 1 ('21["ping",{}]')
    bridge.on_message(sid, "21[\"ping\",{}]");
    let ack_reply = out_rx.recv().await.expect("ack reply");
    assert_eq!(ack_reply, "431[\"ack\",\"pong\"]");

    // 3. Client sends Socket.IO v5 EVENT ('2["echo",{"msg":"hello"}]')
    bridge.on_message(sid, "2[\"echo\",{\"msg\":\"hello\"}]");
    let echo_reply = out_rx.recv().await.expect("echo reply");
    assert_eq!(echo_reply, "42[\"echo_reply\",{\"msg\":\"hello\"}]");

    bridge.on_close(sid);
}
