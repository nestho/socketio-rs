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

#[tokio::test]
async fn e2e_polling_to_websocket_2probe_3probe_5_upgrade_and_ack() {
    use engineio_core::{handle_websocket_upgrade, is_websocket_upgrade};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
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
    });

    let store = SessionStore::new(EngineConfig::default());
    let h: Arc<dyn EngineHandler> = Arc::clone(&bridge) as Arc<dyn EngineHandler>;

    // 1. Start with HTTP polling handshake to obtain `sid`
    let hs_resp = send_http_request(
        store.clone(),
        Arc::clone(&h),
        "GET /socket.io/?EIO=4&transport=polling HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    let body_start = hs_resp.find("\r\n\r\n").unwrap() + 4;
    let hs_json: serde_json::Value = serde_json::from_str(&hs_resp[body_start + 1..]).unwrap();
    let sid = hs_json["sid"].as_str().unwrap().to_owned();

    // Enqueue a pending polling packet (`42["queued",1]`) to verify drain-on-upgrade
    assert!(store.enqueue_packet(&sid, "42[\"queued\",1]"));

    // 2. Upgrade to WebSocket with `?EIO=4&transport=websocket&sid=<sid>`
    let (client_io, server_io) = tokio::io::duplex(65_536);
    let store_ws = store.clone();
    let h_ws = Arc::clone(&h);
    let server_task = tokio::spawn(async move {
        let svc = service_fn(move |req| {
            let s = store_ws.clone();
            let b = Arc::clone(&h_ws);
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

    let ws_url = format!("ws://localhost/socket.io/?EIO=4&transport=websocket&sid={sid}");
    let (mut ws, resp) = tokio_tungstenite::client_async(ws_url, client_io)
        .await
        .expect("WS upgrade handshake succeeds");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);

    // 3. Engine.IO v4 `2probe` -> `3probe` -> `5` upgrade sequence
    ws.send(Message::Text("2probe".into())).await.unwrap();
    let probe_reply = ws.next().await.unwrap().unwrap();
    assert_eq!(probe_reply.to_text().unwrap(), "3probe");

    ws.send(Message::Text("5".into())).await.unwrap();
    let drained = ws.next().await.unwrap().unwrap();
    assert_eq!(drained.to_text().unwrap(), "42[\"queued\",1]");

    // 4. Socket.IO CONNECT (`40`) + Ack roundtrip (`421["ping",{}]` -> `431["pong"]`)
    ws.send(Message::Text("40".into())).await.unwrap();
    let conn_ack = ws.next().await.unwrap().unwrap();
    assert_eq!(conn_ack.to_text().unwrap(), format!("40{{\"sid\":\"{sid}\"}}"));

    ws.send(Message::Text("421[\"ping\",{}]".into())).await.unwrap();
    let ack_reply = ws.next().await.unwrap().unwrap();
    assert_eq!(ack_reply.to_text().unwrap(), "431[\"pong\"]");

    ws.send(Message::Text("1".into())).await.unwrap();
    let _ = ws.close(None).await;
    server_task.abort();
}

#[tokio::test]
async fn e2e_cswsh_origin_policy_rejects_disallowed_origin_with_403() {
    let allowed = vec!["https://trusted.example.com".to_owned()];
    let (mut client_io, server_io) = tokio::io::duplex(16_384);

    let server_task = tokio::spawn(async move {
        let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
            let allowed_c = allowed.clone();
            async move {
                let origin = req
                    .headers()
                    .get(http::header::ORIGIN)
                    .and_then(|v| v.to_str().ok());
                let resp = if !validate_origin(origin, &allowed_c, true) {
                    Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(Full::new(Bytes::from_static(b"Forbidden Origin")))
                        .unwrap()
                } else {
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from_static(b"ok")))
                        .unwrap()
                };
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .serve_connection(TokioIo::new(server_io), svc)
            .await;
    });

    let bad_req = "GET /socket.io/?EIO=4&transport=polling HTTP/1.1\r\nHost: localhost\r\nOrigin: https://evil.attacker.com\r\nConnection: close\r\n\r\n";
    client_io.write_all(bad_req.as_bytes()).await.unwrap();
    client_io.flush().await.unwrap();

    let mut resp_bytes = Vec::new();
    client_io.read_to_end(&mut resp_bytes).await.unwrap();
    let _ = server_task.await;
    let resp_str = String::from_utf8_lossy(&resp_bytes);
    assert!(resp_str.contains("403 Forbidden"));
}

#[tokio::test]
async fn e2e_control_broadcast_and_healthz_over_hyper() {
    use http::Method;
    use http_body_util::BodyExt;
    use socketio_parser::{encode_event_simd, encode_v5_packet, PacketType, SocketIoPacket};

    let io = Arc::new(SocketIo::new(4));
    let (sock_tx, mut sock_rx) = tokio::sync::mpsc::channel(8);
    let sock = SocketRef::new("sid_ctrl_00000001", "/", Arc::clone(&io.rooms), sock_tx);
    sock.join("ops:alerts");

    let (mut client_io, server_io) = tokio::io::duplex(32_768);
    let io_srv = Arc::clone(&io);
    let server_task = tokio::spawn(async move {
        let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
            let io_c = Arc::clone(&io_srv);
            async move {
                let resp = match (req.method(), req.uri().path()) {
                    (&Method::GET, "/healthz") => Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from_static(b"{\"status\":\"ok\"}")))
                        .unwrap(),
                    (&Method::POST, "/_broadcast") => {
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let payload = encode_event_simd(
                            v["event"].as_str().unwrap(),
                            &v["data"],
                        )
                        .unwrap();
                        let frame = encode_v5_packet(&SocketIoPacket {
                            packet_type: PacketType::Event,
                            namespace: "/".to_owned(),
                            ack_id: None,
                            attachments: 0,
                            payload,
                        });
                        let delivered = io_c
                            .rooms
                            .broadcast_zero_copy(v["room"].as_str().unwrap(), &frame);
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(format!(
                                "{{\"delivered\":{delivered}}}"
                            ))))
                            .unwrap()
                    }
                    _ => Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                };
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .serve_connection(TokioIo::new(server_io), svc)
            .await;
    });

    let body = r#"{"room":"ops:alerts","event":"alert","data":{"level":"high"}}"#;
    let req = format!(
        "POST /_broadcast HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    client_io.write_all(req.as_bytes()).await.unwrap();
    client_io.flush().await.unwrap();

    let mut resp_bytes = Vec::new();
    client_io.read_to_end(&mut resp_bytes).await.unwrap();
    let _ = server_task.await;
    let resp_str = String::from_utf8_lossy(&resp_bytes);
    assert!(resp_str.contains("{\"delivered\":1}"));
    assert_eq!(
        sock_rx.try_recv().unwrap().as_slice(),
        b"2[\"alert\",{\"level\":\"high\"}]"
    );
}

#[tokio::test]
async fn e2e_cors_options_preflight_returns_204_with_credentials() {
    let store = SessionStore::new(EngineConfig::default());
    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let h: Arc<dyn EngineHandler> = Arc::clone(&bridge) as Arc<dyn EngineHandler>;

    let resp = send_http_request(
        store,
        h,
        "OPTIONS /socket.io/?EIO=4&transport=polling HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(resp.contains("204 No Content"));
    assert!(resp.to_lowercase().contains("access-control-allow-origin: https://app.example.com"));
    assert!(resp.to_lowercase().contains("access-control-allow-credentials: true"));
}

#[tokio::test]
async fn e2e_polyglot_room_join_leave_and_disconnect_control_api() {
    let io = Arc::new(SocketIo::new(4));
    let bridge = SocketIoEngineBridge::new(Arc::clone(&io));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(16);
    let sid = "FSDjX-WRwSA4zTZMALp1";
    bridge.on_open(sid, tx);
    bridge.on_message(sid, "0");
    let _ack = rx.recv().await.unwrap();

    // Join room via ShardedRoomIndex (/_rooms/join)
    io.rooms.join("chat:room-9", sid);
    assert_eq!(io.rooms.rooms_for_socket(sid), vec!["chat:room-9".to_owned()]);

    // Leave room (/_rooms/leave) -> empty room pruned
    io.rooms.leave("chat:room-9", sid);
    assert_eq!(io.rooms.active_room_count(), 0);

    // Server-initiated disconnect (/_disconnect) -> sends 41 and cleans up session
    assert!(bridge.disconnect_socket(sid));
    let disc = rx.recv().await.unwrap();
    assert_eq!(disc, "41");
    assert_eq!(bridge.active_session_count(), 0);
}
