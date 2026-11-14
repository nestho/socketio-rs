//! Native Rust Socket.IO v5 echo & room server example.
//! Run with: `cargo run -p socketio-server --example echo_server`

use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, EngineConfig,
    EngineHandler, SessionStore,
};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use socketio_server::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let io = Arc::new(SocketIo::new(4));
    io.use_ns("/chat", |_sid, auth| {
        if auth.get("token").is_some() {
            Ok(())
        } else {
            Err("missing auth token".to_owned())
        }
    });

    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);
    io.ns("/", move |socket| {
        socket.join("lobby");
        let b = Arc::clone(&b_ns);
        b.register_event_handler(
            &socket.id,
            "ping",
            Arc::new(|_sock, ctx| {
                AckSender::from_context(&ctx).send(&"pong");
            }),
        );
        let b2 = Arc::clone(&b_ns);
        b2.register_event_handler(
            &socket.id,
            "chat",
            Arc::new(|sock, ctx| {
                if let Ok((_ev, data)) =
                    socketio_parser::parse_dynamic_event(ctx.payload.as_slice())
                {
                    sock.to("lobby").emit("chat", &data);
                }
            }),
        );
    });

    let addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let store = SessionStore::new(EngineConfig::default());
    println!("Native socketio-server listening on http://{addr}/socket.io/");

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
