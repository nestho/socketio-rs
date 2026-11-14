//! Full HTTP/1.1 `hyper` + `polling` + `b<base64>` binary + `SocketIoEngineBridge` E2E test
//! over `tokio::io::duplex` (works in both bare-metal and network-isolated sandboxes).

use bytes::Bytes;
use engineio_core::{handle_polling_request, EngineConfig, EngineHandler, SessionStore};
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use socketio_server::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn send_http_request(
    store: SessionStore,
    bridge: Arc<dyn EngineHandler>,
    raw_req: &str,
) -> String {
    let (mut client_io, server_io) = tokio::io::duplex(65_536);

    let server_task = tokio::spawn(async move {
        let svc = service_fn(move |req| {
            let s = store.clone();
            let b = Arc::clone(&bridge);
            async move {
                let path = req.uri().path();
                let resp = if path == "/socket.io/socket.io.js" {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/javascript; charset=utf-8")
                        .body(Full::new(Bytes::from_static(b"/* socketio-rs */")))
                        .unwrap()
                } else {
                    handle_polling_request(req, s, Some(b)).await
                };
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .serve_connection(TokioIo::new(server_io), svc)
            .await;
    });

    client_io.write_all(raw_req.as_bytes()).await.unwrap();
    client_io.flush().await.unwrap();

    let mut resp_bytes = Vec::new();
    client_io.read_to_end(&mut resp_bytes).await.unwrap();
    let _ = server_task.await;
    String::from_utf8_lossy(&resp_bytes).into_owned()
}

#[tokio::test]
async fn e2e_hyper_http_polling_handshake_ping_ack_binary_and_serve_client() {
    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let uploaded_bin: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let bin_c = Arc::clone(&uploaded_bin);
    let b_ns = Arc::clone(&bridge);

    io.ns("/", move |socket| {
        let b1 = Arc::clone(&b_ns);
        b1.register_event_handler(
            &socket.id,
            "ping",
            Arc::new(|_sock, ctx| {
                AckSender::from_context(&ctx).send(&"pong");
            }),
        );
        let b2 = Arc::clone(&b_ns);
        let bc = Arc::clone(&bin_c);
        b2.register_event_handler(
            &socket.id,
            "upload",
            Arc::new(move |_sock, ctx| {
                if let Some(first) = ctx.binaries.first() {
                    *bc.lock().unwrap() = first.as_slice().to_vec();
                }
            }),
        );
    });

    let store = SessionStore::new(EngineConfig::default());
    let h: Arc<dyn EngineHandler> = Arc::clone(&bridge) as Arc<dyn EngineHandler>;

    // 1. serveClient JS endpoint
    let js_resp = send_http_request(
        store.clone(),
        Arc::clone(&h),
        "GET /socket.io/socket.io.js HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(js_resp.contains("200 OK"));
    assert!(js_resp.contains("application/javascript"));

    // 2. Engine.IO v4 polling handshake
    let hs_resp = send_http_request(
        store.clone(),
        Arc::clone(&h),
        "GET /socket.io/?EIO=4&transport=polling HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(hs_resp.contains("200 OK"));
    let body_start = hs_resp.find("\r\n\r\n").unwrap() + 4;
    let hs_body = &hs_resp[body_start..];
    let json_part: serde_json::Value = serde_json::from_str(&hs_body[1..]).unwrap();
    let sid = json_part["sid"].as_str().unwrap().to_owned();

    // 3. Socket.IO v5 CONNECT ('40')
    let conn_post = format!(
        "POST /socket.io/?EIO=4&transport=polling&sid={sid} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n40"
    );
    let conn_resp = send_http_request(store.clone(), Arc::clone(&h), &conn_post).await;
    assert!(conn_resp.contains("200 OK"));

    // 4. Poll for 40{"sid":...}
    let poll1 = send_http_request(
        store.clone(),
        Arc::clone(&h),
        &format!("GET /socket.io/?EIO=4&transport=polling&sid={sid} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert!(poll1.contains(&format!("40{{\"sid\":\"{sid}\"}}")));

    // 5. Send ack event + binary attachment via base64 polling ('bAQID' = [1, 2, 3])
    let payload = "421[\"ping\",{}]\x1e451-[\"upload\",{\"_placeholder\":true,\"num\":0}]\x1ebAQID";
    let ev_post = format!(
        "POST /socket.io/?EIO=4&transport=polling&sid={sid} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let ev_resp = send_http_request(store.clone(), Arc::clone(&h), &ev_post).await;
    assert!(ev_resp.contains("200 OK"));

    // 6. Poll for 431["pong"]
    let poll2 = send_http_request(
        store.clone(),
        Arc::clone(&h),
        &format!("GET /socket.io/?EIO=4&transport=polling&sid={sid} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
    )
    .await;
    assert!(poll2.contains("431[\"pong\"]"));
    assert_eq!(*uploaded_bin.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn e2e_hyper_websocket_upgrade_ping_ack_and_binary() {
    use engineio_core::{handle_websocket_upgrade, is_websocket_upgrade};
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::Notify;
    use tokio_tungstenite::tungstenite::Message;

    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let b_ns = Arc::clone(&bridge);
    let uploaded_bin: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let bin_c = Arc::clone(&uploaded_bin);
    let bin_done = Arc::new(Notify::new());
    let bin_done_c = Arc::clone(&bin_done);

    io.ns("/", move |socket| {
        let b1 = Arc::clone(&b_ns);
        b1.register_event_handler(
            &socket.id,
            "ping",
            Arc::new(|_sock, ctx| {
                AckSender::from_context(&ctx).send(&"pong");
            }),
        );
        let b2 = Arc::clone(&b_ns);
        let bc = Arc::clone(&bin_c);
        let bd = Arc::clone(&bin_done_c);
        b2.register_event_handler(
            &socket.id,
            "upload",
            Arc::new(move |_sock, ctx| {
                if let Some(first) = ctx.binaries.first() {
                    *bc.lock().unwrap() = first.as_slice().to_vec();
                    bd.notify_one();
                }
            }),
        );
    });

    let store = SessionStore::new(EngineConfig::default());
    let h: Arc<dyn EngineHandler> = Arc::clone(&bridge) as Arc<dyn EngineHandler>;
    let (client_io, server_io) = tokio::io::duplex(65_536);

    let server_task = tokio::spawn(async move {
        let svc = service_fn(move |req| {
            let s = store.clone();
            let b = Arc::clone(&h);
            async move {
                let resp = if is_websocket_upgrade(&req) {
                    handle_websocket_upgrade(req, s, Some(b)).await
                } else {
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::new()))
                        .unwrap()
                };
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server_io), svc)
            .with_upgrades()
            .await;
    });

    let (mut ws, resp) = tokio_tungstenite::client_async(
        "ws://localhost/socket.io/?EIO=4&transport=websocket",
        client_io,
    )
    .await
    .expect("WebSocket handshake succeeds");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);

    // Engine.IO v4 OPEN handshake (`0{...}`)
    let open_msg = ws.next().await.unwrap().unwrap();
    assert!(open_msg.to_text().unwrap().starts_with('0'));

    // Socket.IO v5 CONNECT (`40`) -> expect `40{"sid":...}`
    ws.send(Message::Text("40".into())).await.unwrap();
    let conn_ack = ws.next().await.unwrap().unwrap();
    assert!(conn_ack.to_text().unwrap().starts_with("40{\"sid\":"));

    // Ack roundtrip: `421["ping",{}]` -> `431["pong"]`
    ws.send(Message::Text("421[\"ping\",{}]".into())).await.unwrap();
    let ack_reply = ws.next().await.unwrap().unwrap();
    assert_eq!(ack_reply.to_text().unwrap(), "431[\"pong\"]");

    // Binary event + raw RFC 6455 binary attachment frame
    ws.send(Message::Text(
        "451-[\"upload\",{\"_placeholder\":true,\"num\":0}]".into(),
    ))
    .await
    .unwrap();
    ws.send(Message::Binary(Bytes::from_static(&[9, 8, 7])))
        .await
        .unwrap();

    bin_done.notified().await;

    // Clean close
    ws.send(Message::Text("1".into())).await.unwrap();
    let _ = ws.close(None).await;
    server_task.abort();

    assert_eq!(*uploaded_bin.lock().unwrap(), vec![9, 8, 7]);
}
