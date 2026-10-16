//! Log intake HTTP server that captures Datadog Agent log payloads.
//!
//! The agent sends JSON arrays of log objects to `/api/v2/logs` or `/v1/input`.
//! This server accepts those payloads, decompresses if needed, parses the JSON,
//! and stores the individual log entries for later property assertion.

use std::borrow::Cow;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use flate2::read::GzDecoder;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, header};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::{debug, trace, warn};

/// Errors from the log intake server.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Hyper HTTP error.
    #[error("HTTP error: {0}")]
    Hyper(#[from] hyper::Error),
}

/// A single log entry as received from the Datadog Agent.
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
pub struct ReceivedLogEntry {
    /// The log message content.
    pub message: String,
    /// The log status/level (e.g., "info", "error").
    #[serde(default)]
    pub status: Option<String>,
    /// The timestamp (can be integer or string depending on agent version).
    #[serde(default)]
    pub timestamp: Option<serde_json::Value>,
    /// The hostname.
    #[serde(default)]
    pub hostname: Option<String>,
    /// The service name.
    #[serde(default)]
    pub service: Option<String>,
    /// The source identifier.
    #[serde(default)]
    pub ddsource: Option<String>,
    /// Comma-separated tags.
    #[serde(default)]
    pub ddtags: Option<String>,
}

/// Thread-safe storage for received log entries.
#[derive(Debug, Clone, Default)]
pub struct LogStore {
    entries: Arc<Mutex<Vec<ReceivedLogEntry>>>,
}

impl LogStore {
    /// Create a new empty log store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append entries to the store.
    pub fn append(&self, new_entries: Vec<ReceivedLogEntry>) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.extend(new_entries);
        }
    }

    /// Take all stored entries, leaving the store empty.
    #[must_use]
    pub fn take(&self) -> Vec<ReceivedLogEntry> {
        self.entries
            .lock()
            .map(|mut entries| std::mem::take(&mut *entries))
            .unwrap_or_default()
    }

    /// Get the current number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// Check if the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The log intake HTTP server.
#[derive(Debug)]
pub struct LogIntakeServer {
    /// The address the server is listening on.
    addr: SocketAddr,
    /// The log store shared with the request handler.
    store: LogStore,
    /// Shutdown signal sender.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Server task handle.
    handle: tokio::task::JoinHandle<()>,
}

impl LogIntakeServer {
    /// Start the log intake server on an ephemeral port.
    ///
    /// # Errors
    ///
    /// Returns error if the server cannot bind to a port.
    pub async fn start() -> Result<Self, Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let store = LogStore::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let server_store = store.clone();
        let handle = tokio::spawn(async move {
            run_server(listener, server_store, shutdown_rx).await;
        });

        debug!("log intake server listening on {addr}");

        Ok(Self {
            addr,
            store,
            shutdown_tx,
            handle,
        })
    }

    /// The port the server is listening on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// The full address the server is listening on.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The log store containing received entries.
    #[must_use]
    pub fn store(&self) -> &LogStore {
        &self.store
    }

    /// Stop the server and return all collected log entries.
    pub async fn stop(self) -> Vec<ReceivedLogEntry> {
        // Signal shutdown
        let _ = self.shutdown_tx.send(());
        // Wait for server to finish
        let _ = self.handle.await;
        self.store.take()
    }
}

async fn run_server(
    listener: TcpListener,
    store: LogStore,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer)) => {
                        trace!("accepted connection from {peer}");
                        let store = store.clone();
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req| {
                                let store = store.clone();
                                async move { handle_request(req, &store).await }
                            });
                            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                                .http1()
                                .serve_connection(io, svc)
                                .await
                            {
                                debug!("connection error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("accept error: {e}");
                    }
                }
            }
            _ = &mut shutdown_rx => {
                debug!("log intake server shutting down");
                return;
            }
        }
    }
}

async fn handle_request(
    req: Request<Incoming>,
    store: &LogStore,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    if method == Method::POST
        && (path.contains("/api/v2/logs") || path.contains("/v1/input"))
    {
        let content_encoding = req
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let body = req.collect().await?.to_bytes();

        debug!(
            path = %path,
            content_encoding = %content_encoding,
            body_size = body.len(),
            "received log payload",
        );

        match decompress_if_needed(&body, &content_encoding) {
            Ok(decompressed) => {
                // The agent may send empty objects `{}` as health/keepalive
                // pings. Skip payloads that are too small to contain log data.
                if decompressed.len() <= 2 {
                    trace!("ignoring empty payload ({} bytes)", decompressed.len());
                } else {
                    match serde_json::from_slice::<Vec<ReceivedLogEntry>>(&decompressed) {
                        Ok(entries) => {
                            debug!("parsed {} log entries", entries.len());
                            store.append(entries);
                        }
                        Err(e) => {
                            // Try parsing as a single entry (some agent versions
                            // send objects rather than arrays).
                            if let Ok(entry) =
                                serde_json::from_slice::<ReceivedLogEntry>(&decompressed)
                            {
                                store.append(vec![entry]);
                            } else {
                                warn!("failed to parse log payload ({} bytes): {e}", decompressed.len());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("failed to decompress payload: {e}");
            }
        }
    } else {
        trace!("non-log request: {method} {path}");
    }

    // Always return 200 OK — the agent may probe various health endpoints.
    Ok(Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from_static(b"{\"status\":\"ok\"}")))
        .expect("response builder should not fail"))
}

fn decompress_if_needed<'a>(body: &'a [u8], content_encoding: &str) -> Result<Cow<'a, [u8]>, io::Error> {
    match content_encoding {
        "gzip" => {
            let mut decoder = GzDecoder::new(body);
            let mut decompressed = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            Ok(Cow::Owned(decompressed))
        }
        "zstd" => {
            let decompressed =
                zstd::decode_all(body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Cow::Owned(decompressed))
        }
        _ => Ok(Cow::Borrowed(body)),
    }
}
