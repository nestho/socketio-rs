#![forbid(unsafe_code)]
//! `socketio-gateway` is the standalone polyglot sidecar gateway binary.

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod metrics;

use bytes::Bytes;
use config::GatewayConfig;
use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, AdaptiveDriver,
    EngineConfig, IoDriver, SessionStore, SocketTuningProfile, WebTransportConfig,
};
use http::{Method, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use metrics::GatewayMetrics;
use serde::Deserialize;
use socketio_adapters::RecoveryManager;
use socketio_parser::{encode_event_simd, encode_v5_packet, PacketType, SocketIoPacket};
use socketio_server::SocketIo;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
struct BroadcastPayload {
    #[serde(default = "default_ns")]
    namespace: String,
    room: String,
    event: String,
    data: serde_json::Value,
}

fn default_ns() -> String {
    "/".to_owned()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| Path::new(&w[1]))
        .unwrap_or_else(|| Path::new("gateway.toml"));
    let gw_cfg = GatewayConfig::load_or_default(cfg_path);

    let engine_cfg = EngineConfig {
        ping_interval_ms: gw_cfg.engine.ping_interval_ms,
        ping_timeout_ms: gw_cfg.engine.ping_timeout_ms,
        max_payload_bytes: gw_cfg.engine.max_payload_bytes,
        write_high_watermark_bytes: gw_cfg.engine.write_high_watermark_bytes,
        write_low_watermark_bytes: gw_cfg.engine.write_low_watermark_bytes,
        tcp_notsent_lowat_bytes: gw_cfg.engine.tcp_notsent_lowat_bytes,
        max_connection_duration_jitter_pct: gw_cfg.server.max_connection_duration_jitter_pct,
    };

    let driver = AdaptiveDriver::probe(
        gw_cfg.server.io_driver != "epoll",
        gw_cfg.server.worker_threads,
        SocketTuningProfile::default(),
    );
    let wt_cfg = WebTransportConfig::default();
    let caps = driver.capabilities();
    let cores = driver.shard_count();
    let io = Arc::new(SocketIo::new(cores));
    let recovery = Arc::new(RecoveryManager::new(
        gw_cfg.recovery.ring_buffer_capacity_per_room,
        gw_cfg.recovery.max_disconnection_duration_ms,
    ));
    let metrics = GatewayMetrics::new();

    println!("==============================================================");
    println!("  socketio-rs Standalone Polyglot Gateway v0.1.0");
    println!("==============================================================");
    println!("  Selected IoDriver        : {}", driver.driver_name());
    println!(
        "  Driver Capabilities      : multishot_accept={}, buf_rings={}, notsent_lowat={}",
        caps.multishot_accept, caps.provided_buffer_rings, caps.tcp_notsent_lowat
    );
    println!(
        "  WebTransport (HTTP/3)    : enabled={}, max_bidi_streams={}",
        gw_cfg.engine.enable_webtransport_h3, wt_cfg.max_concurrent_bidi_streams
    );
    println!("  Detected Worker Shards   : {} (64-byte cache-line aligned)", cores);
    println!(
        "  Write Watermarks         : {} KiB high / {} KiB low",
        engine_cfg.write_high_watermark_bytes / 1024,
        engine_cfg.write_low_watermark_bytes / 1024
    );
    println!("  CSR Adapter Mode         : {}", gw_cfg.recovery.adapter);
    println!("  Broadcast Bridge Addr    : http://{}/_broadcast", gw_cfg.bridge.broadcast_bind_addr);
    println!("  Prometheus Metrics       : http://{}/metrics", gw_cfg.bridge.broadcast_bind_addr);
    println!("==============================================================");

    if args.iter().any(|a| a == "--serve") {
        let edge_addr: SocketAddr = args
            .windows(2)
            .find(|w| w[0] == "--bind")
            .map(|w| w[1].as_str())
            .unwrap_or(&gw_cfg.server.bind_addr)
            .parse()
            .expect("valid edge bind SocketAddr");
        let bridge_addr: SocketAddr = gw_cfg
            .bridge
            .broadcast_bind_addr
            .parse()
            .expect("valid bridge SocketAddr");

        let store = SessionStore::new(engine_cfg.clone());
        let bridge = if gw_cfg.recovery.enabled {
            socketio_server::bridge::SocketIoEngineBridge::with_recovery(
                Arc::clone(&io),
                Arc::clone(&recovery),
            )
        } else {
            socketio_server::bridge::SocketIoEngineBridge::new(Arc::clone(&io))
        };
        let bridge_for_ns = Arc::clone(&bridge);
        io.ns("/", move |socket| {
            let b = Arc::clone(&bridge_for_ns);
            b.register_event_handler(
                &socket.id,
                "ping",
                Arc::new(|_sock, ctx| {
                    let ack = socketio_server::extract::AckSender::from_context(&ctx);
                    ack.send(&"pong");
                }),
            );
            let b2 = Arc::clone(&bridge_for_ns);
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

        let shard_std_listeners = driver
            .bind_all_shard_listeners(edge_addr)
            .expect("failed to bind SO_REUSEPORT shard edge TCP listeners");
        let edge_listeners: Vec<TcpListener> = shard_std_listeners
            .into_iter()
            .map(|l| TcpListener::from_std(l).expect("valid std TcpListener"))
            .collect();
        let bridge_listener = TcpListener::bind(bridge_addr)
            .await
            .expect("failed to bind bridge TCP listener");

        if let Some(wt_bind) = args.windows(2).find(|w| w[0] == "--webtransport-bind") {
            if gw_cfg.engine.enable_webtransport_h3 {
                let wt_addr: SocketAddr = wt_bind[1]
                    .parse()
                    .expect("valid --webtransport-bind SocketAddr");
                let wt_tls = engineio_core::WebTransportTlsConfig {
                    cert_pem_path: args
                        .windows(2)
                        .find(|w| w[0] == "--wt-cert")
                        .map(|w| std::path::PathBuf::from(&w[1])),
                    key_pem_path: args
                        .windows(2)
                        .find(|w| w[0] == "--wt-key")
                        .map(|w| std::path::PathBuf::from(&w[1])),
                };
                let wt_cfg_engine = engine_cfg.clone();
                let wt_handler: Arc<dyn engineio_core::EngineHandler> =
                    Arc::clone(&bridge) as Arc<dyn engineio_core::EngineHandler>;
                println!("  WebTransport HTTP/3 UDP  : quic://{wt_addr}/socket.io/");
                tokio::spawn(async move {
                    if let Err(err) = engineio_core::serve_webtransport_endpoint(
                        wt_addr,
                        wt_cfg_engine,
                        Some(wt_handler),
                        wt_tls,
                    )
                    .await
                    {
                        eprintln!("WebTransport endpoint error: {err}");
                    }
                });
            }
        }

        let mount_prefix = gw_cfg.server.path.trim_end_matches('/').to_owned();
        println!("  Edge Socket.IO Listening : http://{edge_addr}{mount_prefix}/");
        println!("  Control/Broadcast API    : http://{bridge_addr}/_broadcast");

        let active_adapter: Option<Arc<dyn socketio_adapters::Adapter>> = match gw_cfg
            .recovery
            .adapter
            .as_str()
        {
            "redis" => {
                let redis_url = std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
                match socketio_adapters::LiveRedisStreamsAdapter::connect(
                    format!("gateway-{}", std::process::id()),
                    "sio:cluster:stream",
                    &redis_url,
                ) {
                    Ok(redis_adapter) => {
                        let io_cluster = Arc::clone(&io);
                        redis_adapter.spawn_subscriber_loop(Arc::new(move |pkt| {
                            let _ = io_cluster.handle_cluster_packet(pkt);
                        }));
                        println!("  Redis Cluster Subscriber : active ({redis_url})");
                        Some(Arc::new(redis_adapter) as Arc<dyn socketio_adapters::Adapter>)
                    }
                    Err(_) => None,
                }
            }
            "nats" => {
                let nats_url = std::env::var("NATS_URL")
                    .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
                let nats_adapter = socketio_adapters::LiveNatsJetStreamAdapter::new(
                    format!("gateway-{}", std::process::id()),
                    "sio.cluster",
                    &nats_url,
                );
                let io_cluster = Arc::clone(&io);
                nats_adapter.spawn_subscriber_loop(Arc::new(move |pkt| {
                    let _ = io_cluster.handle_cluster_packet(pkt);
                }));
                println!("  NATS Cluster Subscriber  : active ({nats_url})");
                Some(Arc::new(nats_adapter) as Arc<dyn socketio_adapters::Adapter>)
            }
            "postgres" => {
                let pg_url = std::env::var("DATABASE_URL")
                    .unwrap_or_else(|_| "postgres://postgres@127.0.0.1:5432/app".to_owned());
                let pg_adapter = socketio_adapters::LivePostgresAdapter::new(
                    format!("gateway-{}", std::process::id()),
                    "socketio_cluster",
                    &pg_url,
                );
                let io_cluster = Arc::clone(&io);
                pg_adapter.spawn_subscriber_loop(Arc::new(move |pkt| {
                    let _ = io_cluster.handle_cluster_packet(pkt);
                }));
                println!("  Postgres LISTEN Sub      : active");
                Some(Arc::new(pg_adapter) as Arc<dyn socketio_adapters::Adapter>)
            }
            _ => None,
        };

        let io_bridge = Arc::clone(&io);
        let rec_bridge = Arc::clone(&recovery);
        let met_bridge = Arc::clone(&metrics);
        let adapter_bridge = active_adapter.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = bridge_listener.accept().await else {
                    continue;
                };
                let io_c = Arc::clone(&io_bridge);
                let rec_c = Arc::clone(&rec_bridge);
                let met_c = Arc::clone(&met_bridge);
                let adp_c = adapter_bridge.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let io_inner = Arc::clone(&io_c);
                        let rec_inner = Arc::clone(&rec_c);
                        let met_inner = Arc::clone(&met_c);
                        let adp_inner = adp_c.clone();
                        async move {
                            let resp = match (req.method(), req.uri().path()) {
                                (&Method::GET, "/healthz") => Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Type", "application/json")
                                    .body(Full::new(Bytes::from(format!(
                                        "{{\"status\":\"ok\",\"shards\":{cores}}}"
                                    ))))
                                    .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))),
                                (&Method::GET, "/metrics") => Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Type", "text/plain; version=0.0.4")
                                    .body(Full::new(Bytes::from(met_inner.render_prometheus())))
                                    .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))),
                                (&Method::POST, "/_broadcast") => {
                                    let Ok(collected) = req.into_body().collect().await else {
                                        return Ok::<_, std::convert::Infallible>(
                                            Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Full::new(Bytes::from_static(b"invalid body")))
                                                .unwrap(),
                                        );
                                    };
                                    let Ok(b) = serde_json::from_slice::<BroadcastPayload>(
                                        &collected.to_bytes(),
                                    ) else {
                                        return Ok(Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Full::new(Bytes::from_static(b"invalid json")))
                                            .unwrap());
                                    };
                                    let payload = encode_event_simd(&b.event, &b.data)
                                        .unwrap_or_else(|_| socketio_parser::SharedBytes::from_slice(b"[]"));
                                    let frame = encode_v5_packet(&SocketIoPacket {
                                        packet_type: PacketType::Event,
                                        namespace: b.namespace.clone(),
                                        ack_id: None,
                                        attachments: 0,
                                        payload,
                                    });
                                    let delivered = io_inner.rooms.broadcast_zero_copy(&b.room, &frame);
                                    rec_inner.record_outbound(&b.room, frame.clone(), 0);
                                    if let Some(ref adp) = adp_inner {
                                        let adp_bg = Arc::clone(adp);
                                        let opts = socketio_adapters::BroadcastOptions {
                                            namespace: b.namespace,
                                            rooms: vec![b.room],
                                            except: Vec::new(),
                                            local_only: false,
                                        };
                                        tokio::spawn(async move {
                                            let _ = adp_bg.broadcast(opts, frame).await;
                                        });
                                    }
                                    met_inner.broadcasts_total.inc();
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .body(Full::new(Bytes::from(format!(
                                            "{{\"delivered\":{delivered}}}"
                                        ))))
                                        .unwrap()
                                }
                                _ => Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::from_static(b"Not Found")))
                                    .unwrap(),
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }
                    });
                    let _ = Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let drain_ms = gw_cfg.server.shutdown_drain_timeout_ms;
        let allowed_origins = Arc::new(gw_cfg.security.allowed_origins.clone());
        let require_origin = gw_cfg.security.require_origin_header;
        for edge_listener in edge_listeners {
            let store_shard = store.clone();
            let bridge_shard = Arc::clone(&bridge);
            let prefix_shard = mount_prefix.clone();
            let origins_shard = Arc::clone(&allowed_origins);
            let met_shard = Arc::clone(&metrics);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _peer)) = edge_listener.accept().await else {
                        continue;
                    };
                    let _ = stream.set_nodelay(true);
                    let store_clone = store_shard.clone();
                    let bridge_clone: Arc<dyn engineio_core::EngineHandler> =
                        Arc::clone(&bridge_shard) as Arc<dyn engineio_core::EngineHandler>;
                    let prefix_outer = prefix_shard.clone();
                    let origins_outer = Arc::clone(&origins_shard);
                    let met_outer = Arc::clone(&met_shard);
                    tokio::spawn(async move {
                        let svc = service_fn(move |req| {
                            let s = store_clone.clone();
                            let b = Arc::clone(&bridge_clone);
                            let p = prefix_outer.clone();
                            let origs = Arc::clone(&origins_outer);
                            let met = Arc::clone(&met_outer);
                            async move {
                                let req_path = req.uri().path();
                                let client_js_path = format!("{p}/socket.io.js");
                                let origin_hdr = req
                                    .headers()
                                    .get(http::header::ORIGIN)
                                    .and_then(|v| v.to_str().ok());
                                let resp = if req_path == client_js_path {
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .header("Content-Type", "application/javascript; charset=utf-8")
                                        .header("Cache-Control", "public, max-age=86400")
                                        .body(Full::new(Bytes::from_static(
                                            b"/* socketio-rs serveClient shim: import { io } from 'https://cdn.socket.io/4.8.1/socket.io.esm.min.js'; */\nconsole.info('socketio-rs gateway active; use socket.io-client@4.x');\n",
                                        )))
                                        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
                                } else if req_path.starts_with(p.as_str()) {
                                    if !socketio_server::validate_origin(
                                        origin_hdr,
                                        &origs,
                                        require_origin,
                                    ) {
                                        met.cswsh_rejections_total.inc();
                                        Response::builder()
                                            .status(StatusCode::FORBIDDEN)
                                            .body(Full::new(Bytes::from_static(b"Forbidden Origin")))
                                            .unwrap_or_else(|_| {
                                                Response::new(Full::new(Bytes::new()))
                                            })
                                    } else if is_websocket_upgrade(&req) {
                                        handle_websocket_upgrade(req, s, Some(b)).await
                                    } else {
                                        handle_polling_request(req, s, Some(b)).await
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
            });
        }

        let _ = tokio::signal::ctrl_c().await;
        println!("Shutdown signal received — draining connections for {drain_ms} ms...");
        tokio::time::sleep(Duration::from_millis(drain_ms.min(500))).await;
    }
}
