//! The [SQS](https://aws.amazon.com/sqs/) protocol speaking blackhole.

use std::net::SocketAddr;

use hyper::{
    body,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::Deserialize;
use tokio::time::Duration;
use tower::ServiceBuilder;
use tracing::{error, info};

use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`Sqs`]
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    Hyper(hyper::Error),
}

fn default_concurrent_requests_max() -> usize {
    100
}

#[derive(Debug, Deserialize, Clone, Copy)]
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
    shutdown: Shutdown,
}

impl Sqs {
    /// Create a new [`Sqs`] server instance
    #[must_use]
    pub fn new(config: &Config, shutdown: Shutdown) -> Self {
        Self {
            httpd_addr: config.binding_addr,
            concurrency_limit: config.concurrent_requests_max,
            shutdown,
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
    pub async fn run(mut self) -> Result<(), Error> {
        let service =
            make_service_fn(|_: &AddrStream| async move { Ok::<_, hyper::Error>(service_fn(srv)) });
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
            .unwrap();
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

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Action {
    action: String,
}

#[derive(Deserialize, Debug)]
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

    #[must_use]
    fn generate_response(&self) -> String {
        let mut entries = String::new();
        for id in self.get_entry_ids() {
            entries += &format!(
                "<DeleteMessageBatchResultEntry><Id>{}Id></DeleteMessageBatchResultEntry>",
                id
            );
        }
        format!("<DeleteMessageBatchResponse><DeleteMessageBatchResult>{}</DeleteMessageBatchResult><ResponseMetadata><RequestId>5EH8IGJZI4VZQTZ2VQ3KQN1AOMNA81OZ3AL1S4RHKII94ZD4FPG8</RequestId></ResponseMetadata></DeleteMessageBatchResponse>", entries)
    }
}

async fn srv(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1);

    let bytes = body::to_bytes(req).await?;
    metrics::counter!("bytes_received", bytes.len() as u64);

    let action: Action = serde_qs::from_bytes(&bytes).unwrap();

    match action.action.as_str() {
        "ReceiveMessage" => {
            let action: ReceiveMessage = serde_qs::from_bytes(&bytes).unwrap();
            let num_messages = action.max_number_of_messages;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/html")
                .body(Body::from(generate_receive_message_response(num_messages)))
                .unwrap())
        }
        "DeleteMessage" => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/html")
            .body(Body::from(generate_delete_message_response()))
            .unwrap()),
        "DeleteMessageBatch" => {
            let action: DeleteMessageBatch = serde_qs::from_bytes(&bytes).unwrap();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/html")
                .body(Body::from(action.generate_response()))
                .unwrap())
        }
        action => {
            println!("Unknown action: {:?}", action);
            Ok(Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(Body::from(vec![]))
                .unwrap())
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
    format!("<DeleteMessageResponse><ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata></DeleteMessageResponse>",
    request_id)
}

fn generate_receive_message_response(num_messages: u32) -> String {
    let mut messages = String::new();
    for _ in 0..num_messages {
        messages += &generate_message();
    }
    format!(
        r#"<ReceiveMessageResponse><ReceiveMessageResult>{}</ReceiveMessageResult><ResponseMetadata><RequestId>XG851KHKN09CRHBGHVD2VHCRE0JR5AKHOPR4GVBCPZT1LYKYY606</RequestId></ResponseMetadata></ReceiveMessageResponse>"#,
        messages
    )
}

fn generate_message() -> String {
    let message_id = random_string(36);
    let receipt_handle = random_string(185);
    let md5 = "8de06d40d5d9a2ec1c6b9a8d80faf135";
    let body = r#"{"host":"31.16.132.149","user-identifier":"jesseddy","datetime":"04/Nov/2021:15:31:28","method":"PUT","request":"/controller/setup","protocol":"HTTP/1.1","status":"400","bytes":31776,"referer":"https://up.de/booper/bopper/mooper/mopper"}"#;
    format!(
        r#"<Message><MessageId>{}</MessageId><ReceiptHandle>{}</ReceiptHandle><MD5OfBody>{}</MD5OfBody><Body>{}</Body><Attribute><Name>SenderId</Name><Value>AIDAIT2UOQQY3AUEKVGXU</Value></Attribute><Attribute><Name>SentTimestamp</Name><Value>1636054288389</Value></Attribute><Attribute><Name>ApproximateReceiveCount</Name><Value>1</Value></Attribute><Attribute><Name>ApproximateFirstReceiveTimestamp</Name><Value>1636054288391</Value></Attribute></Message>"#,
        message_id, receipt_handle, md5, body
    )
}
