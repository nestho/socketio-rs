//! `SocketIoLayer` (`tower::Layer` / `tower::Service`) integrating Engine.IO v4
//! transports, CSWSH `Origin` validation, and handshake authentication (`S2-T04`, `S2-T09`).

use crate::validate_origin;
use bytes::Bytes;
use engineio_core::{
    handle_polling_request, handle_websocket_upgrade, is_websocket_upgrade, SessionStore,
};
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

pub type AuthHook = Arc<dyn Fn(&Request<Incoming>) -> Result<(), String> + Send + Sync>;

/// Security & routing configuration for `SocketIoLayer`.
#[derive(Clone)]
pub struct LayerConfig {
    pub path_prefix: String,
    pub allowed_origins: Vec<String>,
    pub require_origin: bool,
    pub auth_hook: Option<AuthHook>,
}

impl Default for LayerConfig {
    fn default() -> Self {
        Self {
            path_prefix: "/socket.io".to_owned(),
            allowed_origins: vec!["*".to_owned()],
            require_origin: false,
            auth_hook: None,
        }
    }
}

/// Tower `Layer` that intercepts `/socket.io/` traffic (HTTP long-polling + WebSocket)
/// while forwarding all other HTTP routes to the inner Axum/Hyper/Warp service.
#[derive(Clone)]
pub struct SocketIoLayer {
    store: SessionStore,
    config: LayerConfig,
}

impl SocketIoLayer {
    pub fn new(store: SessionStore, config: LayerConfig) -> Self {
        Self { store, config }
    }
}

impl<S> Layer<S> for SocketIoLayer {
    type Service = SocketIoService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SocketIoService {
            inner,
            store: self.store.clone(),
            config: self.config.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SocketIoService<S> {
    inner: S,
    store: SessionStore,
    config: LayerConfig,
}

fn forbidden_response(msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(format!(
            "{{\"code\":4,\"message\":\"{}\"}}",
            msg
        ))))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"Forbidden"))))
}

impl<S> Service<Request<Incoming>> for SocketIoService<S>
where
    S: Service<Request<Incoming>, Response = Response<Full<Bytes>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if !req.uri().path().starts_with(&self.config.path_prefix) {
            return Box::pin(self.inner.call(req));
        }

        let origin = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok());
        if !validate_origin(
            origin,
            &self.config.allowed_origins,
            self.config.require_origin,
        ) {
            return Box::pin(async { Ok(forbidden_response("CSWSH Origin rejected")) });
        }

        if let Some(ref auth) = self.config.auth_hook {
            if let Err(reason) = auth(&req) {
                let resp = forbidden_response(&reason);
                return Box::pin(async move { Ok(resp) });
            }
        }

        let store = self.store.clone();
        Box::pin(async move {
            let resp = if is_websocket_upgrade(&req) {
                handle_websocket_upgrade(req, store).await
            } else {
                handle_polling_request(req, store).await
            };
            Ok(resp)
        })
    }
}
