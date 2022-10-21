//! The HTTP protocol speaking blackhole.

use std::{net::SocketAddr, str::FromStr, time::Duration};

use http::{header::InvalidHeaderValue, HeaderValue};
use hyper::{
    body, header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use once_cell::unsync::OnceCell;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{debug, error, info};

use crate::signals::Shutdown;

#[allow(clippy::declare_interior_mutable_const)]
const RESPONSE: OnceCell<Vec<u8>> = OnceCell::new();

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
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
/// Body variant supported by this blackhole.
pub enum BodyVariant {
    /// All response bodies will be empty.
    Nothing,
    /// All response bodies will mimic AWS Kinesis.
    AwsKinesis,
}

impl FromStr for BodyVariant {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nothing" => Ok(BodyVariant::Nothing),
            "aws_kinesis" | "kinesis" => Ok(BodyVariant::AwsKinesis),
            _ => Err("unknown variant"),
        }
    }
}

fn default_body_variant() -> BodyVariant {
    BodyVariant::Nothing
}

fn default_content_type() -> String {
    "application/json".to_string()
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
    /// the content-type header to respond with, defaults to application/json
    #[serde(default = "default_content_type")]
    pub content_type: String,
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
    body_variant: BodyVariant,
    req: Request<Body>,
    content_type: Option<HeaderValue>,
) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1);

    let (parts, body) = req.into_parts();

    let bytes = body::to_bytes(body).await?;

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            metrics::counter!("bytes_received", body.len() as u64);

            let mut okay = Response::default();
            *okay.status_mut() = StatusCode::OK;

            if let Some(val) = content_type {
                okay.headers_mut().insert(header::CONTENT_TYPE, val);
            }

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
    content_type_header: Option<HeaderValue>,
}

impl Http {
    /// Create a new [`Http`] server instance
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let content_type_header = if config.content_type.is_empty() {
            None
        } else {
            Some(
                header::HeaderValue::from_str(&config.content_type)
                    .map_err(Error::InvalidContentType)?,
            )
        };

        Ok(Self {
            httpd_addr: config.binding_addr,
            body_variant: config.body_variant,
            concurrency_limit: config.concurrent_requests_max,
            shutdown,
            content_type_header,
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
            let content_type = self.content_type_header.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |request| {
                    debug!("REQUEST: {:?}", request);
                    srv(self.body_variant, request, content_type.clone())
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
