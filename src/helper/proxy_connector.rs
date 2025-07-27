use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::bail;
use base64::Engine;
use http::Uri;
use hyper_util::client::legacy::connect::{
    HttpConnector,
    proxy::{SocksV5, Tunnel},
};
use tower_service::Service;
use url::Url;

use crate::helper::AnyResult;

#[derive(Debug, Clone)]
pub enum ProxyConnector {
    Direct(HttpConnector),
    Socks5(SocksV5<HttpConnector>),
    Http(Tunnel<HttpConnector>),
}

impl ProxyConnector {
    pub fn new(proxy_url: Option<&str>) -> AnyResult<Self> {
        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(std::time::Duration::from_secs(30)));

        let proxy_url = match proxy_url {
            None => return Ok(ProxyConnector::Direct(connector)),
            Some(proxy_url) => proxy_url,
        };

        let url: Url = proxy_url.parse()?;
        let uri: Uri = proxy_url.parse()?;

        match url.scheme() {
            "http" | "https" => {
                let mut tunnel = Tunnel::new(uri, connector);
                if let Some(password) = url.password() {
                    let username = url.username();
                    let auth = base64::engine::general_purpose::STANDARD
                        .encode([username, password].join(":"));
                    let auth = format!("Basic {auth}");
                    tunnel = tunnel.with_auth(auth.parse()?);
                }
                Ok(ProxyConnector::Http(tunnel))
            }
            "socks5" => {
                // For SOCKS5, we need to construct the URI without the scheme
                let host = url
                    .host_str()
                    .ok_or_else(|| anyhow::anyhow!("SOCKS5 proxy URL must have host"))?;
                let port = url.port().unwrap_or(1080);
                let socks_uri = format!("http://{host}:{port}").parse::<Uri>()?;

                let mut socks = SocksV5::new(socks_uri, connector);
                if let Some(password) = url.password() {
                    let username = url.username();
                    socks = socks.with_auth(username.to_string(), password.to_string());
                }
                Ok(ProxyConnector::Socks5(socks))
            }
            _ => bail!("Unsupported proxy scheme: {}", url.scheme()),
        }
    }

    /// Create with authentication for SOCKS5
    pub fn with_socks5_auth(
        proxy_url: &str,
        username: String,
        password: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(std::time::Duration::from_secs(30)));

        let parsed_url: Uri = proxy_url.parse()?;
        let socks = SocksV5::new(parsed_url, connector).with_auth(username, password);
        Ok(ProxyConnector::Socks5(socks))
    }
}

#[derive(Debug)]
pub struct ProxyConnectorError(pub Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for ProxyConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Proxy connector error: {}", self.0)
    }
}

impl std::error::Error for ProxyConnectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

impl Service<Uri> for ProxyConnector {
    type Response = <HttpConnector as Service<Uri>>::Response;
    type Error = ProxyConnectorError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            ProxyConnector::Direct(connector) => connector
                .poll_ready(cx)
                .map_err(|e| ProxyConnectorError(Box::new(e))),
            ProxyConnector::Socks5(connector) => connector
                .poll_ready(cx)
                .map_err(|e| ProxyConnectorError(Box::new(e))),
            ProxyConnector::Http(connector) => connector
                .poll_ready(cx)
                .map_err(|e| ProxyConnectorError(Box::new(e))),
        }
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        match self {
            ProxyConnector::Direct(connector) => {
                let fut = connector.call(req);
                Box::pin(async move { fut.await.map_err(|e| ProxyConnectorError(Box::new(e))) })
            }
            ProxyConnector::Socks5(connector) => {
                let fut = connector.call(req);
                Box::pin(async move { fut.await.map_err(|e| ProxyConnectorError(Box::new(e))) })
            }
            ProxyConnector::Http(connector) => {
                let fut = connector.call(req);
                Box::pin(async move { fut.await.map_err(|e| ProxyConnectorError(Box::new(e))) })
            }
        }
    }
}
