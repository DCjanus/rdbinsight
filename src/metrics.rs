use std::net::SocketAddr;

use anyhow::{Context, Result};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::tokio::TokioIo;
use lazy_static::lazy_static;
use prometheus::{Encoder, Gauge, Opts, TextEncoder};
use tracing::warn;

lazy_static! {
    pub static ref BUILD_INFO: Gauge = {
        let gauge = Gauge::with_opts(
            Opts::new("rdbinsight_build_info", "Build and version information")
                .const_label("version", env!("CARGO_PKG_VERSION")),
        )
        .expect("Failed to create build info gauge");
        gauge.set(1.0);
        gauge
    };
}

pub fn init_metrics() {
    let _ = prometheus::default_registry().register(Box::new(BUILD_INFO.clone()));
}

pub async fn run_metrics_server(addr: SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| "Failed to bind metrics TCP listener")?;

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(ok) => ok,
            Err(e) => {
                warn!(operation = "metrics_accept_error", error = %e, "Failed to accept connection");
                continue;
            }
        };

        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<hyper::body::Incoming>| async move {
                if req.method() == http::Method::GET && req.uri().path() == "/metrics" {
                    let mut buffer = Vec::new();
                    let encoder = TextEncoder::new();
                    let families = prometheus::gather();
                    let body_bytes: Bytes = if encoder.encode(&families, &mut buffer).is_ok() {
                        Bytes::from(buffer)
                    } else {
                        Bytes::new()
                    };

                    let mut response: Response<Full<Bytes>> = Response::new(Full::from(body_bytes));
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        http::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
                    );
                    Ok::<_, std::convert::Infallible>(response)
                } else {
                    let mut not_found: Response<Full<Bytes>> =
                        Response::new(Full::new(Bytes::new()));
                    *not_found.status_mut() = StatusCode::NOT_FOUND;
                    Ok::<_, std::convert::Infallible>(not_found)
                }
            });

            let io = TokioIo::new(stream);
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                warn!(operation = "metrics_conn_error", error = %e, "Error serving metrics connection");
            }
        });
    }
}
