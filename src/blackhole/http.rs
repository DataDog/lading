//! The HTTP protocol speaking blackhole.

use std::{net::SocketAddr, time::Duration};

use http::{header::InvalidHeaderValue, status::InvalidStatusCode, HeaderMap};
use hyper::{
    body, header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use metrics::{register_counter, Counter};
use once_cell::{sync, unsync::OnceCell};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{debug, error, info};

use crate::signals::Shutdown;

#[allow(clippy::declare_interior_mutable_const)]
const RESPONSE: OnceCell<Vec<u8>> = OnceCell::new();

static BYTES_RECEIVED: sync::OnceCell<Counter> = sync::OnceCell::new();
#[inline]
fn bytes_received() -> &'static Counter {
    BYTES_RECEIVED.get_or_init(|| register_counter!("bytes_received"))
}

static REQUESTS_RECEIVED: sync::OnceCell<Counter> = sync::OnceCell::new();
#[inline]
fn requests_received() -> &'static Counter {
    REQUESTS_RECEIVED.get_or_init(|| register_counter!("requests_received"))
}

fn default_concurrent_requests_max() -> usize {
    100
}

/// Errors produced by [`Http`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    #[error("HTTP server error: {0}")]
    Hyper(hyper::Error),
    /// The configured content type value was not valid.
    #[error("The configured content type value was not valid: {0}")]
    InvalidContentType(InvalidHeaderValue),
    /// The configured status code was not valid.
    #[error("The configured status code was not valid: {0}")]
    InvalidStatusCode(InvalidStatusCode),
}

/// Body variant supported by this blackhole.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyVariant {
    /// All response bodies will be empty.
    Nothing,
    /// All response bodies will mimic AWS Kinesis.
    AwsKinesis,
    /// Respond with a hardcoded string value
    Static(String),
}

fn default_body_variant() -> BodyVariant {
    BodyVariant::Nothing
}

fn default_status_code() -> u16 {
    StatusCode::OK.as_u16()
}

fn default_headers() -> HeaderMap {
    let mut map = HeaderMap::new();
    map.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    map
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
/// Configuration for [`Http`]
pub struct Config {
    /// number of concurrent HTTP connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
    /// the body variant to respond with, default nothing
    #[serde(default = "default_body_variant")]
    pub body_variant: BodyVariant,
    /// Headers to include in the response; default is `Content-Type: application/json`
    #[serde(with = "http_serde::header_map", default = "default_headers")]
    pub headers: HeaderMap,
    /// the content-type header to respond with, defaults to 200
    #[serde(default = "default_status_code")]
    pub status: u16,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct KinesisPutRecordBatchResponseEntry {
    error_code: Option<String>,
    error_message: Option<String>,
    record_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct KinesisPutRecordBatchResponse {
    encrypted: Option<bool>,
    failed_put_count: u32,
    request_responses: Vec<KinesisPutRecordBatchResponseEntry>,
}

#[allow(clippy::borrow_interior_mutable_const)]
async fn srv(
    status: StatusCode,
    body_variant: BodyVariant,
    req: Request<Body>,
    headers: HeaderMap,
) -> Result<Response<Body>, hyper::Error> {
    requests_received().increment(1);

    let (parts, body) = req.into_parts();

    let bytes = body::to_bytes(body).await?;

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            bytes_received().increment(body.len() as u64);

            let mut okay = Response::default();
            *okay.status_mut() = status;

            *okay.headers_mut() = headers;

            let body_bytes = RESPONSE
                .get_or_init(|| match body_variant {
                    BodyVariant::AwsKinesis => {
                        let response = KinesisPutRecordBatchResponse {
                            encrypted: None,
                            failed_put_count: 0,
                            request_responses: vec![KinesisPutRecordBatchResponseEntry {
                                error_code: None,
                                error_message: None,
                                record_id: "foobar".to_string(),
                            }],
                        };
                        serde_json::to_vec(&response).unwrap()
                    }
                    BodyVariant::Nothing => vec![],
                    BodyVariant::Static(val) => val.as_bytes().to_vec(),
                })
                .clone();
            *okay.body_mut() = Body::from(body_bytes);
            Ok(okay)
        }
    }
}

#[derive(Debug)]
/// The HTTP blackhole.
pub struct Http {
    httpd_addr: SocketAddr,
    body_variant: BodyVariant,
    concurrency_limit: usize,
    shutdown: Shutdown,
    headers: HeaderMap,
    status: StatusCode,
}

impl Http {
    /// Create a new [`Http`] server instance
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let status = StatusCode::from_u16(config.status).map_err(Error::InvalidStatusCode)?;

        Ok(Self {
            httpd_addr: config.binding_addr,
            body_variant: config.body_variant.clone(),
            concurrency_limit: config.concurrent_requests_max,
            headers: config.headers.clone(),
            status,
            shutdown,
        })
    }

    /// Run [`Http`] to completion
    ///
    /// This function runs the HTTP server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the configuration is invalid or if
    /// receiving a packet fails.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(mut self) -> Result<(), Error> {
        let service = make_service_fn(|_: &AddrStream| {
            let body_variant = self.body_variant.clone();
            let headers = self.headers.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |request| {
                    debug!("REQUEST: {:?}", request);
                    srv(self.status, body_variant.clone(), request, headers.clone())
                }))
            }
        });
        let svc = ServiceBuilder::new()
            .load_shed()
            .concurrency_limit(self.concurrency_limit)
            .timeout(Duration::from_secs(1))
            .service(service);

        let addr = AddrIncoming::bind(&self.httpd_addr)
            .map(|mut addr| {
                addr.set_keepalive(Some(Duration::from_secs(60)));
                addr
            })
            .map_err(Error::Hyper)?;

        let server = Server::builder(addr).serve(svc);
        loop {
            tokio::select! {
                res = server => {
                    error!("server shutdown unexpectedly");
                    return res.map_err(Error::Hyper);
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
