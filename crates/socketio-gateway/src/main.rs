#![forbid(unsafe_code)]
//! `socketio-gateway` is the standalone polyglot sidecar gateway binary.

use bytes::Bytes;
use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, AdaptiveDriver,
    EngineConfig, IoDriver, SessionStore, SocketTuningProfile,
};
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use socketio_server::ShardedRoomIndex;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let config = EngineConfig::default();
    let driver = AdaptiveDriver::probe(true, 0, SocketTuningProfile::default());
    let caps = driver.capabilities();
    let cores = driver.shard_count();
    let rooms = ShardedRoomIndex::new(cores);

    println!("==============================================================");
    println!("  socketio-rs Standalone Polyglot Gateway v0.1.0");
    println!("==============================================================");
    println!("  Selected IoDriver        : {}", driver.driver_name());
    println!(
        "  Driver Capabilities      : multishot_accept={}, buf_rings={}, notsent_lowat={}",
        caps.multishot_accept, caps.provided_buffer_rings, caps.tcp_notsent_lowat
    );
    println!("  Detected Worker Shards   : {} (64-byte cache-line aligned)", cores);
    println!(
        "  Write Watermarks         : {} KiB high / {} KiB low",
        config.write_high_watermark_bytes / 1024,
        config.write_low_watermark_bytes / 1024
    );
    println!("  Max Payload Guard        : {} bytes (streaming)", config.max_payload_bytes);
    println!("  Active Broadcast Count   : {}", rooms.total_broadcasts());
    println!("==============================================================");

    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--serve") {
        let bind_str = args
            .windows(2)
            .find(|w| w[0] == "--bind")
            .map(|w| w[1].as_str())
            .unwrap_or("127.0.0.1:3000");
        let addr: SocketAddr = bind_str.parse().expect("valid bind SocketAddr");
        let store = SessionStore::new(config);
        let listener = TcpListener::bind(addr)
            .await
            .expect("failed to bind gateway TCP listener");
        println!("  Listening for Engine.IO v4 (Polling + WebSocket) on http://{addr}/socket.io/");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let _ = stream.set_nodelay(true);
            let store_clone = store.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req| {
                    let s = store_clone.clone();
                    async move {
                        let resp = if req.uri().path().starts_with("/socket.io") {
                            if is_websocket_upgrade(&req) {
                                handle_websocket_upgrade(req, s).await
                            } else {
                                handle_polling_request(req, s).await
                            }
                        } else {
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::from_static(b"Not Found")))
                                .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
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
}
