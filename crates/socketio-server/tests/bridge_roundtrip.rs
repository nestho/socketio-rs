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

#[tokio::test]
async fn live_on_any_and_disconnecting_hooks_observe_events_and_rooms() {
    use std::sync::Mutex;

    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);
    let observed_any: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_rooms: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let any_c = Arc::clone(&observed_any);
    let rooms_c = Arc::clone(&observed_rooms);

    io.ns("/", move |socket| {
        socket.join("lobby");
        let a = Arc::clone(&any_c);
        b_ns.register_on_any(
            &socket.id,
            Arc::new(move |_sock, event, _ctx| {
                a.lock().unwrap().push(event.to_owned());
            }),
        );
        let r = Arc::clone(&rooms_c);
        b_ns.register_disconnecting(
            &socket.id,
            Arc::new(move |_sock, rooms| {
                *r.lock().unwrap() = rooms;
            }),
        );
    });

    let (tx, _rx) = mpsc::channel::<String>(16);
    let sid = "FSDjX-WRwSA4zTZMALq9";
    bridge.on_open(sid, tx);
    bridge.on_message(sid, "0");
    bridge.on_message(sid, "2[\"custom_ev\",{\"x\":1}]");
    bridge.on_close(sid);

    assert_eq!(*observed_any.lock().unwrap(), vec!["custom_ev".to_owned()]);
    assert_eq!(*observed_rooms.lock().unwrap(), vec!["lobby".to_owned()]);
    assert_eq!(io.rooms.active_room_count(), 0);
}

#[tokio::test]
async fn live_on_any_outgoing_observes_emitted_frames() {
    use std::sync::Mutex;

    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);
    let outbound_frames: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let out_c = Arc::clone(&outbound_frames);

    io.ns("/", move |socket| {
        let o = Arc::clone(&out_c);
        b_ns.register_on_any_outgoing(
            &socket.id,
            Arc::new(move |_sid, pkt| {
                if let Ok(s) = std::str::from_utf8(pkt.as_slice()) {
                    o.lock().unwrap().push(s.to_owned());
                }
            }),
        );
        socket.emit("welcome", &"hi");
    });

    let (tx, mut rx) = mpsc::channel::<String>(16);
    let sid = "FSDjX-WRwSA4zTZMALq8";
    bridge.on_open(sid, tx);
    bridge.on_message(sid, "0");

    let _connect_ack = rx.recv().await.unwrap();
    let welcome = rx.recv().await.unwrap();
    assert_eq!(welcome, "42[\"welcome\",\"hi\"]");
    assert_eq!(
        *outbound_frames.lock().unwrap(),
        vec!["2[\"welcome\",\"hi\"]".to_owned()]
    );
}

#[tokio::test]
async fn live_namespace_middleware_rejects_unauthorized_with_44_connect_error() {
    let io = Arc::new(SocketIo::new(4));
    io.use_ns("/admin", |_sid, auth| {
        if auth.get("token").and_then(|v| v.as_str()) == Some("secret-ok") {
            Ok(())
        } else {
            Err("unauthorized".to_owned())
        }
    });
    io.ns("/admin", |socket| {
        socket.emit("admin_ready", &true);
    });

    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));

    // 1. Unauthorized attempt -> expects 44/admin,{"message":"unauthorized"}
    let (tx1, mut rx1) = mpsc::channel::<String>(8);
    let sid1 = "FSDjX-WRwSA4zTZMALa1";
    bridge.on_open(sid1, tx1);
    bridge.on_message(sid1, "0/admin,{\"token\":\"bad\"}");
    let err_frame = rx1.recv().await.unwrap();
    assert_eq!(err_frame, "44/admin,{\"message\":\"unauthorized\"}");

    // 2. Authorized attempt -> expects 40/admin,{"sid":...} + admin_ready
    let (tx2, mut rx2) = mpsc::channel::<String>(8);
    let sid2 = "FSDjX-WRwSA4zTZMALa2";
    bridge.on_open(sid2, tx2);
    bridge.on_message(sid2, "0/admin,{\"token\":\"secret-ok\"}");
    let ok_frame = rx2.recv().await.unwrap();
    assert_eq!(ok_frame, format!("40/admin,{{\"sid\":\"{sid2}\"}}"));
    let ready_frame = rx2.recv().await.unwrap();
    assert_eq!(ready_frame, "42/admin,[\"admin_ready\",true]");
}
