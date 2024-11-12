//! The HTTP protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total requests received
//!

use std::{net::SocketAddr, time::Duration};

use http::{header::InvalidHeaderValue, status::InvalidStatusCode, HeaderMap};
use hyper::{
    body::HttpBody,
    header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{debug, error, info};

use super::General;

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
    /// Failed to deserialize the configuration.
    #[error("Failed to deserialize the configuration: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Body variant supported by this blackhole.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum BodyVariant {
    /// All response bodies will be empty.
    Nothing,
    /// All response bodies will mimic AWS Kinesis.
    AwsKinesis,
    /// Respond with a hardcoded byte slice value
    RawBytes,
    /// Respond with a hardcoded string value
    Static(String),
}

fn default_body_variant() -> BodyVariant {
    BodyVariant::Nothing
}

fn default_response_delay_millis() -> u64 {
    0
}

fn default_status_code() -> u16 {
    StatusCode::OK.as_u16()
}

fn default_headers() -> HeaderMap {
    let mut map = HeaderMap::new();
    map.insert(
        header::CONTENT_TYPE,
        "application/json"
            .parse()
            .expect("Not possible to parse into HeaderMap"),
    );
    map
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
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
    /// raw array of bytes if the `raw_bytes` body variant is selected
    #[serde(default)]
    pub raw_bytes: Vec<u8>,
    /// delay to add before making a response
    #[serde(default = "default_response_delay_millis")]
    pub response_delay_millis: u64,
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
    bytes_received: Counter,
    requests_received: Counter,
    body_bytes: Vec<u8>,
    req: Request<Body>,
    headers: HeaderMap,
    response_delay: Duration,
) -> Result<Response<Body>, hyper::Error> {
    requests_received.increment(1);

    let (parts, body) = req.into_parts();

    let bytes = body.collect().await?.to_bytes();

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            bytes_received.increment(body.len() as u64);

            tokio::time::sleep(response_delay).await;

            let mut okay = Response::default();
            *okay.status_mut() = status;
            *okay.headers_mut() = headers;
            *okay.body_mut() = Body::from(body_bytes);
            Ok(okay)
        }
    }
}

#[derive(Debug)]
/// The HTTP blackhole.
pub struct Http {
    httpd_addr: SocketAddr,
    body_bytes: Vec<u8>,
    concurrency_limit: usize,
    shutdown: lading_signal::Watcher,
    headers: HeaderMap,
    status: StatusCode,
    metric_labels: Vec<(String, String)>,
    response_delay: Duration,
}

impl Http {
    /// Create a new [`Http`] server instance
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    ///
    /// # Panics
    ///
    /// None known.
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let status = StatusCode::from_u16(config.status).map_err(Error::InvalidStatusCode)?;

        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "http".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        let body_bytes = match &config.body_variant {
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
                serde_json::to_vec(&response)?
            }
            BodyVariant::Nothing => vec![],
            BodyVariant::RawBytes => config.raw_bytes.clone(),
            BodyVariant::Static(val) => val.as_bytes().to_vec(),
        };

        Ok(Self {
            httpd_addr: config.binding_addr,
            body_bytes,
            concurrency_limit: config.concurrent_requests_max,
            headers: config.headers.clone(),
            status,
            shutdown,
            metric_labels,
            response_delay: Duration::from_millis(config.response_delay_millis),
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
    pub async fn run(self) -> Result<(), Error> {
        let bytes_received = counter!("bytes_received", &self.metric_labels);
        let requests_received = counter!("requests_received", &self.metric_labels);
        let service = make_service_fn(|_: &AddrStream| {
            let bytes_received = bytes_received.clone();
            let requests_received = requests_received.clone();
            let body_bytes = self.body_bytes.clone();
            let headers = self.headers.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |request| {
                    debug!("REQUEST: {:?}", request);
                    srv(
                        self.status,
                        bytes_received.clone(),
                        requests_received.clone(),
                        body_bytes.clone(),
                        request,
                        headers.clone(),
                        self.response_delay,
                    )
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
        tokio::select! {
            res = server => {
                error!("server shutdown unexpectedly");
                 res.map_err(Error::Hyper)
            }
            () = self.shutdown.recv() => {
                info!("shutdown signal received");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn config_deserializes_variant_nothing() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant: "nothing"
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(
            config,
            Config {
                concurrent_requests_max: default_concurrent_requests_max(),
                response_delay_millis: default_response_delay_millis(),
                binding_addr: SocketAddr::from_str("127.0.0.1:1000")
                    .expect("Not possible to parse into SocketAddr"),
                body_variant: BodyVariant::Nothing,
                headers: default_headers(),
                status: default_status_code(),
                raw_bytes: vec![],
            },
        );
    }

    #[test]
    fn config_deserializes_raw_bytes() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant: "raw_bytes"
raw_bytes: [0x01, 0x02, 0x10]
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(
            config,
            Config {
                concurrent_requests_max: default_concurrent_requests_max(),
                response_delay_millis: default_response_delay_millis(),
                binding_addr: SocketAddr::from_str("127.0.0.1:1000")
                    .expect("Not possible to parse into SocketAddr"),
                body_variant: BodyVariant::RawBytes,
                headers: default_headers(),
                status: default_status_code(),
                raw_bytes: vec![0x01, 0x02, 0x10],
            },
        );
    }
}
