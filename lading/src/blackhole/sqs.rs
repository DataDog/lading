//! The [SQS](https://aws.amazon.com/sqs/) protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total messages received
//!

use std::{fmt::Write, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::{service::service_fn, Request, Response, StatusCode};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use metrics::counter;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::{pin, sync::Semaphore, task::JoinSet};
use tracing::{debug, error, info};

use super::General;

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
        let listener = tokio::net::TcpListener::bind(self.httpd_addr).await?;
        let sem = Arc::new(Semaphore::new(self.concurrency_limit));
        let mut join_set = JoinSet::new();

        let shutdown = self.shutdown.recv();
        pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => {
                    info!("shutdown signal received");
                    break;
                }

                incoming = listener.accept() => {
                    let (stream, addr) = match incoming {
                        Ok((s,a)) => (s,a),
                        Err(e) => {
                            error!("accept error: {e}");
                            continue;
                        }
                    };

                    let metric_labels = self.metric_labels.clone();
                    let sem = Arc::clone(&sem);

                    join_set.spawn(async move {
                        debug!("Accepted connection from {addr}");
                        let permit = match sem.acquire_owned().await {
                            Ok(p) => p,
                            Err(e) => {
                                error!("Semaphore closed: {e}");
                                return;
                            }
                        };

                        let builder = auto::Builder::new(TokioExecutor::new());
                        let serve_future = builder
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<hyper::body::Incoming>| {
                                    debug!("REQUEST: {:?}", req);
                                     srv(
                                        req,
                                        metric_labels.clone(),

                                    )
                                })
                            );

                        if let Err(e) = serve_future.await {
                            error!("Error serving {addr}: {e}");
                        }
                        drop(permit);
                    });
                }
            }
        }
        drop(listener);
        while join_set.join_next().await.is_some() {}
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
        Ok(format!("<DeleteMessageBatchResponse><DeleteMessageBatchResult>{entries}</DeleteMessageBatchResult><ResponseMetadata><RequestId>5EH8IGJZI4VZQTZ2VQ3KQN1AOMNA81OZ3AL1S4RHKII94ZD4FPG8</RequestId></ResponseMetadata></DeleteMessageBatchResponse>"))
    }
}

async fn srv(
    req: Request<hyper::body::Incoming>,
    metric_labels: Vec<(String, String)>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, Error> {
    counter!("requests_received", &metric_labels).increment(1);

    let (_, body) = req.into_parts();

    let bytes = body.boxed().collect().await?.to_bytes();
    counter!("bytes_received", &metric_labels).increment(bytes.len() as u64);

    let action: Action = serde_qs::from_bytes(&bytes)?;

    match action.action.as_str() {
        "ReceiveMessage" => {
            let action: ReceiveMessage = serde_qs::from_bytes(&bytes)?;
            let num_messages = action.max_number_of_messages;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/html")
                .body(crate::full(generate_receive_message_response(num_messages)))?)
        }
        "DeleteMessage" => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/html")
            .body(crate::full(generate_delete_message_response()))?),
        "DeleteMessageBatch" => {
            let action: DeleteMessageBatch = serde_qs::from_bytes(&bytes)?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/html")
                .body(crate::full(action.generate_response()?))?)
        }
        action => {
            debug!("Unknown action: {action:?}");
            Ok(Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(crate::full(vec![]))?)
        }
    }
}

#[must_use]
fn random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

fn generate_delete_message_response() -> String {
    let request_id = random_string(52);
    format!("<DeleteMessageResponse><ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata></DeleteMessageResponse>")
}

fn generate_receive_message_response(num_messages: u32) -> String {
    let mut messages = String::new();
    for _ in 0..num_messages {
        messages += &generate_message();
    }
    format!(
        r#"<ReceiveMessageResponse><ReceiveMessageResult>{messages}</ReceiveMessageResult><ResponseMetadata><RequestId>XG851KHKN09CRHBGHVD2VHCRE0JR5AKHOPR4GVBCPZT1LYKYY606</RequestId></ResponseMetadata></ReceiveMessageResponse>"#
    )
}

fn generate_message() -> String {
    let message_id = random_string(36);
    let receipt_handle = random_string(185);
    let md5 = "8de06d40d5d9a2ec1c6b9a8d80faf135";
    let body = r#"{"host":"31.16.132.149","user-identifier":"jesseddy","datetime":"04/Nov/2021:15:31:28","method":"PUT","request":"/controller/setup","protocol":"HTTP/1.1","status":"400","bytes":31776,"referer":"https://up.de/booper/bopper/mooper/mopper"}"#;
    format!(
        r#"<Message><MessageId>{message_id}</MessageId><ReceiptHandle>{receipt_handle}</ReceiptHandle><MD5OfBody>{md5}</MD5OfBody><Body>{body}</Body><Attribute><Name>SenderId</Name><Value>AIDAIT2UOQQY3AUEKVGXU</Value></Attribute><Attribute><Name>SentTimestamp</Name><Value>1636054288389</Value></Attribute><Attribute><Name>ApproximateReceiveCount</Name><Value>1</Value></Attribute><Attribute><Name>ApproximateFirstReceiveTimestamp</Name><Value>1636054288391</Value></Attribute></Message>"#
    )
}
