//! Connection State Recovery (CSR) and Presence tracking example.
//! Run with: `cargo run -p socketio-server --example recovery_and_presence`

use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, EngineConfig,
    EngineHandler, SessionStore,
};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use socketio_adapters::RecoveryManager;
use socketio_server::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let io = Arc::new(SocketIo::new(4));
    let recovery = Arc::new(RecoveryManager::new(512, 120_000));
    let bridge = SocketIoEngineBridge::with_recovery(Arc::clone(&io), Arc::clone(&recovery));

    let io_ns = Arc::clone(&io);
    let b_ns = Arc::clone(&bridge);
    io.ns("/", move |socket| {
        socket.join("presence-room");
        io_ns.presence.track(
            "presence-room",
            PresenceMember {
                sid: socket.id.clone(),
                user_id: format!("user-{}", &socket.id[..socket.id.len().min(6)]),
                metadata: serde_json::json!({"status": "online"}),
            },
        );
        let online = io_ns.presence.list("presence-room");
        println!(
            "[connect] sid={} | presence-room count={}",
            socket.id,
            online.len()
        );

        let b_ping = Arc::clone(&b_ns);
        b_ping.register_event_handler(
            &socket.id,
            "ping",
            Arc::new(|_sock, ctx| {
                AckSender::from_context(&ctx).send(&"pong");
            }),
        );
    });

    let addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let store = SessionStore::new(EngineConfig::default());
    println!("CSR + Presence server listening on http://{addr}/socket.io/");

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
