//! Multi-room `/chat` namespace example with `io.use_ns(...)` auth middleware,
//! acks, `on_any` inspection, and `disconnecting` cleanup hooks.
//! Run with: `cargo run -p socketio-server --example chat_rooms`

use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, EngineConfig,
    EngineHandler, SessionStore,
};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use serde::Deserialize;
use socketio_server::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
struct RoomCmd {
    room: String,
    text: Option<String>,
}

#[tokio::main]
async fn main() {
    let io = Arc::new(SocketIo::new(4));

    // Reject `/chat` connections missing `auth.token` with Socket.IO `44` CONNECT_ERROR.
    io.use_ns("/chat", |_sid, auth| {
        if auth.get("token").and_then(|v| v.as_str()).is_some() {
            Ok(())
        } else {
            Err("unauthorized: missing auth.token".to_owned())
        }
    });

    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);

    io.ns("/chat", move |socket| {
        socket.join("general");

        let b = Arc::clone(&b_ns);
        b.register_on_any(
            &socket.id,
            Arc::new(|sock, event, _ctx| {
                println!("[on_any] sid={} ns={} event={}", sock.id, sock.ns, event);
            }),
        );

        let b_join = Arc::clone(&b_ns);
        b_join.register_event_handler(
            &socket.id,
            "join_room",
            Arc::new(|sock, ctx| {
                if let Ok(Data(cmd)) = Data::<RoomCmd>::from_context(&ctx) {
                    sock.join(&cmd.room);
                    AckSender::from_context(&ctx).send(&serde_json::json!({
                        "joined": cmd.room
                    }));
                }
            }),
        );

        let b_msg = Arc::clone(&b_ns);
        b_msg.register_event_handler(
            &socket.id,
            "message",
            Arc::new(|sock, ctx| {
                if let Ok(Data(cmd)) = Data::<RoomCmd>::from_context(&ctx) {
                    let text = cmd.text.unwrap_or_default();
                    sock.to(&cmd.room).emit(
                        "message",
                        &serde_json::json!({
                            "from": sock.id,
                            "room": cmd.room,
                            "text": text
                        }),
                    );
                }
            }),
        );

        let b_disc = Arc::clone(&b_ns);
        b_disc.register_disconnecting(
            &socket.id,
            Arc::new(|sock, rooms| {
                println!("[disconnecting] sid={} leaving rooms={:?}", sock.id, rooms);
            }),
        );
    });

    let addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let store = SessionStore::new(EngineConfig::default());
    println!("Chat server listening on http://{addr}/socket.io/ (namespace: /chat)");

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let s = store.clone();
        let h: Arc<dyn EngineHandler> = Arc::clone(&bridge) as Arc<dyn EngineHandler>;
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let s_c = s.clone();
                let h_c = Arc::clone(&h);
                async move {
                    let resp = if is_websocket_upgrade(&req) {
                        handle_websocket_upgrade(req, s_c, Some(h_c)).await
                    } else {
                        handle_polling_request(req, s_c, Some(h_c)).await
                    };
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), svc)
                .await;
        });
    }
}
