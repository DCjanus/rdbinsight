use std::{net::SocketAddr, sync::LazyLock};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    http::{HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use prometheus::{
    Encoder, IntCounterVec, IntGauge, TextEncoder, opts, register_int_counter_vec,
    register_int_gauge,
};

pub static BUILD_INFO: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = register_int_gauge!(
        opts!("rdbinsight_build_info", "Build and version information")
            .const_label("version", env!("CARGO_PKG_VERSION"))
    )
    .expect("Failed to register build info gauge");
    gauge.set(1);
    gauge
});

pub static RECORDS_PARSED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "rdbinsight_records_parsed_total",
        "Total number of records parsed per instance",
        &["cluster", "batch", "instance",]
    )
    .expect("Failed to register records parsed counter")
});

pub fn init_metrics() {
    // Eagerly initialize the gauge so it is registered in the default registry
    let _ = &*BUILD_INFO;
    // Eagerly initialize counters as well
    let _ = &*RECORDS_PARSED_TOTAL;
}

pub async fn run_metrics_server(addr: SocketAddr) -> Result<()> {
    // Build Axum router with a single `/metrics` endpoint
    let app = Router::new().route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| "Failed to bind metrics TCP listener")?;

    axum::serve(listener, app)
        .await
        .with_context(|| "Error serving metrics connection")
}

async fn metrics_handler() -> impl IntoResponse {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let body_bytes: Bytes = if encoder.encode(&families, &mut buffer).is_ok() {
        Bytes::from(buffer)
    } else {
        Bytes::new()
    };

    let mut response = (StatusCode::OK, Body::from(body_bytes)).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

pub fn inc_record(cluster: &str, batch: &str, instance: &str) {
    RECORDS_PARSED_TOTAL
        .with_label_values(&[cluster, batch, instance])
        .inc();
}
