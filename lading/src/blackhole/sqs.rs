//! The [SQS](https://aws.amazon.com/sqs/) protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total messages received
//!

use bytes::Bytes;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::{Request, Response, StatusCode};
use metrics::counter;
use rand::{Rng, distr::Alphanumeric};
use serde::{Deserialize, Serialize};
use std::{fmt::Write, net::SocketAddr};
use tracing::{debug, error};

use super::General;
use crate::blackhole::common;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Sqs`]
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    /// Wrapper for [`std::fmt::Error`].
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    /// Wrapper for [`serde_qs::Error`].
    #[error(transparent)]
    SerdeQs(#[from] serde_qs::Error),
    /// Wrapper for [`hyper::http::Error`].
    #[error(transparent)]
    Http(#[from] hyper::http::Error),
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrapper for [`crate::blackhole::common::Error`].
    #[error(transparent)]
    Common(#[from] crate::blackhole::common::Error),
}

fn default_concurrent_requests_max() -> usize {
    100
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Sqs`]
pub struct Config {
    /// number of concurrent HTTP connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

#[derive(Debug)]
/// The [SQS](https://aws.amazon.com/sqs/) blackhole.
pub struct Sqs {
    httpd_addr: SocketAddr,
    concurrency_limit: usize,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl Sqs {
    /// Create a new [`Sqs`] server instance
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "sqs".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            httpd_addr: config.binding_addr,
            concurrency_limit: config.concurrent_requests_max,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`Sqs`] to completion
    ///
    /// This function runs the SQS server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an if an http server error ocurrs.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(self) -> Result<(), Error> {
        common::run_httpd(
            self.httpd_addr,
            self.concurrency_limit,
            self.shutdown,
            self.metric_labels.clone(),
            move || {
                let metric_labels = self.metric_labels.clone();
                hyper::service::service_fn(move |req| srv(req, metric_labels.clone()))
            },
        )
        .await?;

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct Action {
    action: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct ReceiveMessage {
    #[allow(dead_code)]
    version: String,
    #[allow(dead_code)]
    queue_url: String,
    max_number_of_messages: u32,
    #[allow(dead_code)]
    wait_time_seconds: u32,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct DeleteMessageBatch {
    #[allow(dead_code)]
    version: String,
    #[allow(dead_code)]
    queue_url: String,

    // a Vec isn't properly deserialized here
    #[serde(rename = "DeleteMessageBatchRequestEntry.1.Id")]
    entry_1_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.2.Id")]
    entry_2_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.3.Id")]
    entry_3_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.4.Id")]
    entry_4_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.5.Id")]
    entry_5_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.6.Id")]
    entry_6_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.7.Id")]
    entry_7_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.8.Id")]
    entry_8_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.9.Id")]
    entry_9_id: Option<String>,
    #[serde(rename = "DeleteMessageBatchRequestEntry.10.Id")]
    entry_10_id: Option<String>,
}

impl DeleteMessageBatch {
    #[must_use]
    fn get_entry_ids(&self) -> Vec<&String> {
        let mut output = vec![];
        if let Some(x) = &self.entry_1_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_2_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_3_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_4_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_5_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_6_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_7_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_8_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_9_id {
            output.push(x);
        }
        if let Some(x) = &self.entry_10_id {
            output.push(x);
        }
        output
    }

    #[must_use = "This function returns a string or errors"]
    fn generate_response(&self) -> Result<String, Error> {
        let mut entries = String::new();
        for id in self.get_entry_ids() {
            write!(
                &mut entries,
                "<DeleteMessageBatchResultEntry><Id>{id}</Id></DeleteMessageBatchResultEntry>",
            )?;
        }
        Ok(format!(
            "<DeleteMessageBatchResponse><DeleteMessageBatchResult>{entries}</DeleteMessageBatchResult><ResponseMetadata><RequestId>5EH8IGJZI4VZQTZ2VQ3KQN1AOMNA81OZ3AL1S4RHKII94ZD4FPG8</RequestId></ResponseMetadata></DeleteMessageBatchResponse>"
        ))
    }
}

async fn srv(
    req: Request<hyper::body::Incoming>,
    metric_labels: Vec<(String, String)>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    counter!("requests_received", &metric_labels).increment(1);

    let (_, body) = req.into_parts();
    let bytes = body.boxed().collect().await?.to_bytes();
    counter!("bytes_received", &metric_labels).increment(bytes.len() as u64);

    let action = match serde_qs::from_bytes::<Action>(&bytes) {
        Ok(a) => a,
        Err(e) => {
            let msg = format!("SQS parse error: {e}");
            return Ok(build_response(
                StatusCode::BAD_REQUEST,
                Some("text/plain"),
                msg.into_bytes(),
            ));
        }
    };

    match action.action.as_str() {
        "ReceiveMessage" => {
            let num_messages = match serde_qs::from_bytes::<ReceiveMessage>(&bytes) {
                Ok(rm) => rm.max_number_of_messages,
                Err(e) => {
                    let msg = format!("ReceiveMessage parse error: {e}");
                    return Ok(build_response(
                        StatusCode::BAD_REQUEST,
                        Some("text/plain"),
                        msg,
                    ));
                }
            };
            let body_str = generate_receive_message_response(num_messages);
            Ok(build_response(StatusCode::OK, Some("text/html"), body_str))
        }

        "DeleteMessage" => {
            let body_str = generate_delete_message_response();
            Ok(build_response(StatusCode::OK, Some("text/html"), body_str))
        }

        "DeleteMessageBatch" => {
            let batch = match serde_qs::from_bytes::<DeleteMessageBatch>(&bytes) {
                Ok(b) => b,
                Err(e) => {
                    let msg = format!("DeleteMessageBatch parse error: {e}");
                    return Ok(build_response(
                        StatusCode::BAD_REQUEST,
                        Some("text/plain"),
                        msg,
                    ));
                }
            };
            let response_str = match batch.generate_response() {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("DeleteMessageBatch formatting error: {e}");
                    return Ok(build_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Some("text/plain"),
                        msg,
                    ));
                }
            };
            Ok(build_response(
                StatusCode::OK,
                Some("text/html"),
                response_str,
            ))
        }

        other => {
            debug!("Unknown action: {other:?}");
            Ok(build_response(
                StatusCode::NOT_IMPLEMENTED,
                None,
                Vec::new(),
            ))
        }
    }
}

fn build_response(
    status: StatusCode,
    content_type: Option<&str>,
    body: impl Into<Bytes>,
) -> Response<BoxBody<Bytes, hyper::Error>> {
    let mut builder = Response::builder().status(status);
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    match builder.body(crate::full(body)) {
        Ok(resp) => resp,
        Err(e) => {
            error!("Error building response: {e}");
            match Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "text/plain")
                .body(crate::full(b"Internal error building response".to_vec()))
            {
                Ok(r) => r,
                Err(inner_err) => {
                    // Building a fallback failed, panic.
                    panic!("Catastrophic error: {inner_err}");
                }
            }
        }
    }
}

#[must_use]
fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

fn generate_delete_message_response() -> String {
    let request_id = random_string(52);
    format!(
        "<DeleteMessageResponse><ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata></DeleteMessageResponse>"
    )
}

fn generate_receive_message_response(num_messages: u32) -> String {
    let mut messages = String::new();
    for _ in 0..num_messages {
        messages += &generate_message();
    }
    format!(
        r"<ReceiveMessageResponse><ReceiveMessageResult>{messages}</ReceiveMessageResult><ResponseMetadata><RequestId>XG851KHKN09CRHBGHVD2VHCRE0JR5AKHOPR4GVBCPZT1LYKYY606</RequestId></ResponseMetadata></ReceiveMessageResponse>"
    )
}

fn generate_message() -> String {
    let message_id = random_string(36);
    let receipt_handle = random_string(185);
    let md5 = "8de06d40d5d9a2ec1c6b9a8d80faf135";
    let body = r#"{"host":"31.16.132.149","user-identifier":"jesseddy","datetime":"04/Nov/2021:15:31:28","method":"PUT","request":"/controller/setup","protocol":"HTTP/1.1","status":"400","bytes":31776,"referer":"https://up.de/booper/bopper/mooper/mopper"}"#;
    format!(
        r"<Message><MessageId>{message_id}</MessageId><ReceiptHandle>{receipt_handle}</ReceiptHandle><MD5OfBody>{md5}</MD5OfBody><Body>{body}</Body><Attribute><Name>SenderId</Name><Value>AIDAIT2UOQQY3AUEKVGXU</Value></Attribute><Attribute><Name>SentTimestamp</Name><Value>1636054288389</Value></Attribute><Attribute><Name>ApproximateReceiveCount</Name><Value>1</Value></Attribute><Attribute><Name>ApproximateFirstReceiveTimestamp</Name><Value>1636054288391</Value></Attribute></Message>"
    )
}
